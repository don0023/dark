// --- START OF FILE dark-server.js ---

const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const { EventEmitter } = require('events');

class LoggingService {
  constructor(serviceName = 'ProxyServer') {
    this.serviceName = serviceName;
  }
  
  _formatMessage(level, message) {
    const timestamp = new Date().toISOString();
    return `[${level}] ${timestamp} [${this.serviceName}] - ${message}`;
  }
  
  info(message) {
    console.log(this._formatMessage('INFO', message));
  }
  
  error(message) {
    console.error(this._formatMessage('ERROR', message));
  }
  
  warn(message) {
    console.warn(this._formatMessage('WARN', message));
  }
  
  debug(message) {
    console.debug(this._formatMessage('DEBUG', message));
  }
}

class MessageQueue extends EventEmitter {
  constructor(timeoutMs = 600000) {
    super();
    this.messages = [];
    this.waitingResolvers = [];
    this.defaultTimeout = timeoutMs;
    this.closed = false;
  }
  
  enqueue(message) {
    if (this.closed) return;
    
    if (this.waitingResolvers.length > 0) {
      const resolver = this.waitingResolvers.shift();
      resolver.resolve(message);
    } else {
      this.messages.push(message);
    }
  }
  
  async dequeue(timeoutMs = this.defaultTimeout) {
    if (this.closed) {
      throw new Error('Queue is closed');
    }
    
    return new Promise((resolve, reject) => {
      if (this.messages.length > 0) {
        resolve(this.messages.shift());
        return;
      }
      
      const resolver = { resolve, reject };
      this.waitingResolvers.push(resolver);
      
      const timeoutId = setTimeout(() => {
        const index = this.waitingResolvers.indexOf(resolver);
        if (index !== -1) {
          this.waitingResolvers.splice(index, 1);
          reject(new Error('Queue timeout'));
        }
      }, timeoutMs);
      
      resolver.timeoutId = timeoutId;
    });
  }
  
  close() {
    this.closed = true;
    this.waitingResolvers.forEach(resolver => {
      clearTimeout(resolver.timeoutId);
      resolver.reject(new Error('Queue closed'));
    });
    this.waitingResolvers = [];
    this.messages = [];
  }
}

class ConnectionRegistry extends EventEmitter {
  constructor(logger) {
    super();
    this.logger = logger;
    this.connections = new Set();
    this.messageQueues = new Map();
  }
  
  addConnection(websocket, clientInfo) {
    this.connections.add(websocket);
    this.logger.info(`新客户端连接: ${clientInfo.address}`);
    
    websocket.on('message', (data) => {
      this._handleIncomingMessage(data.toString());
    });
    
    websocket.on('close', () => {
      this._removeConnection(websocket);
    });
    
    websocket.on('error', (error) => {
      this.logger.error(`WebSocket连接错误: ${error.message}`);
    });
    
    this.emit('connectionAdded', websocket);
  }
  
  _removeConnection(websocket) {
    this.connections.delete(websocket);
    this.logger.info('客户端连接断开');
    
    this.messageQueues.forEach(queue => queue.close());
    this.messageQueues.clear();
    
    this.emit('connectionRemoved', websocket);
  }
  
  _handleIncomingMessage(messageData) {
    try {
      const parsedMessage = JSON.parse(messageData);
      const requestId = parsedMessage.request_id;
      
      if (!requestId) {
        this.logger.warn('收到无效消息：缺少request_id');
        return;
      }
      
      const queue = this.messageQueues.get(requestId);
      if (queue) {
        this._routeMessage(parsedMessage, queue);
      } else {
        this.logger.warn(`收到未知请求ID的消息: ${requestId}`);
      }
    } catch (error) {
      this.logger.error('解析WebSocket消息失败');
    }
  }
  
  _routeMessage(message, queue) {
    const { event_type } = message;
    
    switch (event_type) {
      case 'response_headers':
      case 'chunk':
      case 'error':
        queue.enqueue(message);
        break;
      case 'stream_close':
        queue.enqueue({ type: 'STREAM_END' });
        break;
      default:
        this.logger.warn(`未知的事件类型: ${event_type}`);
    }
  }
  
  hasActiveConnections() {
    return this.connections.size > 0;
  }
  
  getFirstConnection() {
    return this.connections.values().next().value;
  }
  
  createMessageQueue(requestId) {
    const queue = new MessageQueue();
    this.messageQueues.set(requestId, queue);
    return queue;
  }
  
  removeMessageQueue(requestId) {
    const queue = this.messageQueues.get(requestId);
    if (queue) {
      queue.close();
      this.messageQueues.delete(requestId);
    }
  }
}

class RequestHandler {
  constructor(serverSystem, connectionRegistry, logger) {
    this.serverSystem = serverSystem;
    this.connectionRegistry = connectionRegistry;
    this.logger = logger;
  }
  
  async processRequest(req, res) {
    this.logger.info(`处理请求: ${req.method} ${req.path}`);
    
    if (!this.connectionRegistry.hasActiveConnections()) {
      return this._sendErrorResponse(res, 503, '没有可用的浏览器连接');
    }
    
    const requestId = this._generateRequestId();
    const proxyRequest = this._buildProxyRequest(req, requestId);
    
    const messageQueue = this.connectionRegistry.createMessageQueue(requestId);
    
    try {
      await this._forwardRequest(proxyRequest);
      await this._handleResponse(messageQueue, req, res);
    } catch (error) {
      this._handleRequestError(error, res);
    } finally {
      this.connectionRegistry.removeMessageQueue(requestId);
    }
  }
  
  _generateRequestId() {
    return `${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
  }
  
  _buildProxyRequest(req, requestId) {
    let requestBody = '';
    if (Buffer.isBuffer(req.body)) {
      requestBody = req.body.toString('utf-8');
    } else if (typeof req.body === 'string') {
        requestBody = req.body;
    } else if (req.body) {
      requestBody = JSON.stringify(req.body);
    }
    
    return {
      path: req.path,
      method: req.method,
      headers: req.headers,
      query_params: req.query,
      body: requestBody,
      request_id: requestId,
      streaming_mode: this.serverSystem.streamingMode
    };
  }
  
  async _forwardRequest(proxyRequest) {
    const connection = this.connectionRegistry.getFirstConnection();
    connection.send(JSON.stringify(proxyRequest));
  }
  
  async _handleResponse(messageQueue, req, res) {
    const headerMessage = await messageQueue.dequeue();
    
    if (headerMessage.event_type === 'error') {
      return this._sendErrorResponse(res, headerMessage.status || 500, headerMessage.message);
    }
    
    this._setResponseHeaders(res, headerMessage);
    
    if (this.serverSystem.streamingMode === 'fake') {
      this.logger.info('当前为假流式模式，开始处理...');
      await this._pseudoStreamResponseData(messageQueue, req, res);
    } else {
      this.logger.info('当前为真流式模式，开始处理...');
      await this._realStreamResponseData(messageQueue, res);
    }
  }
  
  _setResponseHeaders(res, headerMessage) {
    res.status(headerMessage.status || 200);
    
    const headers = headerMessage.headers || {};
    Object.entries(headers).forEach(([name, value]) => {
      const isFakeMode = this.serverSystem.streamingMode === 'fake';
      if (!isFakeMode || name.toLowerCase() !== 'content-length') {
        res.set(name, value);
      }
    });
  }

  async _pseudoStreamResponseData(messageQueue, req, res) {
    let connectionMaintainer = null;
    let fullBody = '';
    let keepAliveChunk = 'data: {}\n\n';
    let apiType = 'Unknown';

    if (req.path.includes('chat/completions')) {
      apiType = 'OpenAI';
      const fakeId = `chatcmpl-${this._generateRequestId()}`;
      const timestamp = Math.floor(Date.now() / 1000);
      const openAIKeepAlivePayload = {
        id: fakeId,
        object: "chat.completion.chunk",
        created: timestamp,
        model: "gpt-4",
        choices: [{ index: 0, delta: {}, finish_reason: null }]
      };
      keepAliveChunk = `data: ${JSON.stringify(openAIKeepAlivePayload)}\n\n`;
    } 
    else if (req.path.includes('generateContent')) {
      apiType = 'Gemini';
      const geminiKeepAlivePayload = {
          candidates: [{ content: { parts: [{ text: "" }], role: "model" }, finishReason: null, index: 0, safetyRatings: [] }]
      };
      keepAliveChunk = `data: ${JSON.stringify(geminiKeepAlivePayload)}\n\n`;
    }

    try {
      this.logger.info(`检测到 ${apiType} API，已启动假流式响应。`);

      connectionMaintainer = setInterval(() => {
        if (!res.writableEnded) { res.write(keepAliveChunk); }
      }, 1000);

      const dataMessage = await messageQueue.dequeue();

      if (dataMessage.type === 'STREAM_END') {
        this.logger.warn('在收到任何数据块前流已关闭');
      } else if (dataMessage.data) {
        fullBody = dataMessage.data;
        this.logger.info(`已收到完整响应体，长度: ${fullBody.length}`);
      }

      const endMessage = await messageQueue.dequeue();
      if (endMessage.type !== 'STREAM_END') {
        this.logger.warn('在数据块之后未收到预期的STREAM_END信号');
      }

    } catch (error) {
        this.logger.error(`假流式响应处理中发生错误: ${error.message}`);
        throw error;
    } finally {
      if (connectionMaintainer) {
        clearInterval(connectionMaintainer);
        this.logger.info('假流式响应的连接维持已停止。');
      }

      if (!res.writableEnded) {
        if (fullBody) { res.write(fullBody); }
        res.end();
        this.logger.info('完整响应已发送，连接已关闭。');
      }
    }
  }

  async _realStreamResponseData(messageQueue, res) {
    try {
        while (true) {
            const dataMessage = await messageQueue.dequeue(30000);
            
            if (dataMessage.type === 'STREAM_END') {
              this.logger.info('收到流结束信号。');
              break;
            }
            
            if (dataMessage.data) {
              res.write(dataMessage.data);
            }
        }
    } catch(error) {
        if (error.message === 'Queue timeout') {
            this.logger.warn('真流式响应超时，可能是流已正常结束但未收到结束信号。');
        } else {
            this.logger.error(`真流式响应处理中发生错误: ${error.message}`);
            throw error;
        }
    } finally {
        if(!res.writableEnded) {
            res.end();
            this.logger.info('真流式响应连接已关闭。');
        }
    }
  }
  
  _handleRequestError(error, res) {
    if (!res.headersSent) {
      if (error.message === 'Queue timeout') {
        this._sendErrorResponse(res, 504, '请求超时');
      } else {
        this.logger.error(`请求处理错误: ${error.message}`);
        this._sendErrorResponse(res, 500, `代理错误: ${error.message}`);
      }
    } else {
        this.logger.error(`请求处理错误（头已发送）: ${error.message}`);
        if(!res.writableEnded) res.end();
    }
  }
  
  _sendErrorResponse(res, status, message) {
    if (!res.headersSent) {
      res.status(status).send(message);
    }
  }
}

class ProxyServerSystem extends EventEmitter {
  constructor(config = {}) {
    super();
    this.config = {
      httpPort: 8889,
      wsPort: 9998,
      host: '127.0.0.1',
      ...config
    };
    
    this.streamingMode = 'fake';

    this.logger = new LoggingService('ProxyServer');
    this.connectionRegistry = new ConnectionRegistry(this.logger);
    this.requestHandler = new RequestHandler(this, this.connectionRegistry, this.logger);
    
    this.httpServer = null;
    this.wsServer = null;
  }
  
  async start() {
    try {
      await this._startHttpServer();
      await this._startWebSocketServer();
      
      this.logger.info(`代理服务器系统启动完成，当前模式: ${this.streamingMode}`);
      this.emit('started');
    } catch (error) {
      this.logger.error(`启动失败: ${error.message}`);
      this.emit('error', error);
      throw error;
    }
  }
  
  async _startHttpServer() {
    const app = this._createExpressApp();
    this.httpServer = http.createServer(app);
    
    return new Promise((resolve) => {
      this.httpServer.listen(this.config.httpPort, this.config.host, () => {
        this.logger.info(`HTTP服务器启动: http://${this.config.host}:${this.config.httpPort}`);
        resolve();
      });
    });
  }
  
  _createExpressApp() {
    const app = express();
    
    app.use(express.json({ limit: '100mb' }));
    app.use(express.urlencoded({ extended: true, limit: '100mb' }));
    app.use(express.raw({ type: '*/*', limit: '100mb' }));

    app.get('/admin/set-mode', (req, res) => {
        const newMode = req.query.mode;
        if (newMode === 'fake' || newMode === 'real') {
            this.streamingMode = newMode;
            const message = `流式响应模式已切换为: ${this.streamingMode}`;
            this.logger.info(message);
            res.status(200).send(message);
        } else {
            const message = '无效的模式。请使用 "fake" 或 "real"。';
            this.logger.warn(message);
            res.status(400).send(message);
        }
    });

    app.get('/admin/get-mode', (req, res) => {
        const message = `当前流式响应模式为: ${this.streamingMode}`;
        res.status(200).send(message);
    });
    
    app.all(/(.*)/, (req, res) => {
      if (req.path.startsWith('/admin/')) return;
      this.requestHandler.processRequest(req, res);
    });
    
    return app;
  }
  
  async _startWebSocketServer() {
    this.wsServer = new WebSocket.Server({
      port: this.config.wsPort,
      host: this.config.host
    });
    
    this.wsServer.on('connection', (ws, req) => {
      this.connectionRegistry.addConnection(ws, {
        address: req.socket.remoteAddress
      });
    });
    
    this.logger.info(`WebSocket服务器启动: ws://${this.config.host}:${this.config.wsPort}`);
  }
}

async function initializeServer() {
  const serverSystem = new ProxyServerSystem();
  
  try {
    await serverSystem.start();
  } catch (error) {
    console.error('服务器启动失败:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  initializeServer();
}

module.exports = { ProxyServerSystem, initializeServer };