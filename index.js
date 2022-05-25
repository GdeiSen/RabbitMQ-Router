const EventEmitter = require('events');
var amqp = require('amqplib/callback_api');
const { v4: uuidv4 } = require('uuid');
exports.ConnectionManager = class ConnectionManager {

    constructor(config) {
        this.connectionName = config.name;
        this.dispatchTo = config.dispatchTo;
        this.consumeOn = config.consumeOn;
        this.durable = config.durable || false;
        this.safeMode = config.safeMode || true;
        this.timeout = config.timeout || 8000;
        this.throwErrors = config.throwErrors || true;
        this.listeners = [];
        this.InputRequestEmitter = new EventEmitter;
        this.InputResponceEmitter = new EventEmitter;
        if (config.showInfoTable == true) console.log(`
        ========== CONNECTION MANAGER ==========
        ${fixedLengthString(`${this.connectionName}`, 40, true)}
        ========================================
        #   ${fixedLengthString(`durable = ${this.durable}`, 35)}#
        #   ${fixedLengthString(`getTimeout = ${this.timeout}`, 35)}#
        #   ${fixedLengthString(`safemode = ${this.safeMode}`, 35)}#
        #   ${fixedLengthString(`dispatchTo = ${this.dispatchTo}`, 35)}#
        #   ${fixedLengthString(`consumeOn = ${this.consumeOn}`, 35)}#
        #   ${fixedLengthString(`throwErrors = ${this.throwErrors}`, 35)}#
        ========================================
        `)
    }

    connect() {
        try {
            let self = this
            amqp.connect('amqp://localhost', function (error0, connection) {
                if (error0) { throw error0; }
                connection.createChannel(function (error1, channel) {
                    if (error1) { throw error1; }
                    self.channel = channel;
                    channel.assertQueue(self.consumeOn, { durable: self.durable }, async function (error2, q) {
                        if (error2) { throw error2; }
                        self.q = q;
                        channel.consume(self.consumeOn, function (msg) {
                            let parsedContent = JSON.parse(msg.content);
                            msg.content = parsedContent;
                            msg.correlationId = msg.properties.correlationId;
                            if (msg.properties.type == 'request' || msg.properties.type == 'post') {
                                self.InputRequestEmitter.emit(msg.content?.request?.name || msg.content?.request, msg);
                                self.InputRequestEmitter.emit('request', msg);
                                self.#emitRoute(msg.content?.request?.name || msg.content?.request, msg)
                            }
                            if (msg.properties.type == 'responce' || msg.properties.type == 'error') {
                                self.InputResponceEmitter.emit(msg.properties.correlationId, msg);
                                self.InputResponceEmitter.emit('responce', msg);
                            }
                            channel.ack(msg);
                        }, { noAck: false });
                        console.log(`⬜ Ready To Consume Messages From The App Using Queue [${self.consumeOn}]`)
                    });
                });
            });
        } catch (err) { console.log(err); return 0; };
    }

    addRoute(requestName, funcBody) {
        this.listeners[requestName] = this.listeners[requestName] || [];
        this.listeners[requestName].push(funcBody);
        return this;
    }

    #emitRoute(requestName, requestMessage) {
        try {
            let self = this
            let responce = {
                send: function (responce, params) {
                    if (!requestMessage.properties.replyTo || !requestMessage.properties.correlationId) return 0
                    self.channel.sendToQueue(requestMessage.properties.replyTo,
                        Buffer.from(JSON.stringify({ responce: responce, requestMessage: requestMessage, requestBody: requestMessage?.content?.request, request: requestMessage?.content?.request?.name })), {
                        correlationId: requestMessage.properties.correlationId,
                        type: params?.type || 'responce'
                    });
                    console.log(`[ ]=----(${requestName} responce)`);
                },
                error: function (error, params) {
                    if (!requestMessage.properties.replyTo || !requestMessage.properties.correlationId) return 0
                    self.channel.sendToQueue(requestMessage.properties.replyTo,
                        Buffer.from(JSON.stringify({ error: error, requestMessage: requestMessage, requestBody: requestMessage?.content?.request, request: requestMessage?.content?.request?.name })), {
                        correlationId: requestMessage.properties.correlationId,
                        type: params?.type || 'error'
                    });
                    console.log(`[ ]=----(${requestName} error)`);
                }
            }
            console.log(`[ ]<----(${requestName} request)`);
            let fns = this.listeners[requestName];
            if (!fns) return false;
            fns.forEach((f) => {
                f(requestMessage.content.request, responce, requestMessage);
            });
            return true;
        } catch (error) { console.log(error) }
    }

    async awaitResponce(correlationId, request, params) {
        const promise = new Promise((resolve, reject) => {
            let timeout
            if (!params || params?.timeout !== 'none') {
                timeout = setTimeout(() => {
                    resolve(undefined);
                    this.InputResponceEmitter.removeAllListeners(correlationId);
                    console.log(`Cannot Get Responce From [${params?.dispatchTo || this.dispatchTo}] Queue, Request: [${request.name}]`)
                }, params?.timeout || this.timeout);
            }
            this.InputResponceEmitter.once(correlationId, (msg) => {
                console.log(`[ ]<----(${request.name} responce)`);
                this.InputResponceEmitter.removeAllListeners(correlationId);
                clearTimeout(timeout);
                resolve(msg)
            })
        })
        return await promise;
    }

    async get(request, params) {
        try {
            if (!this.channel) { console.log('ERROR Connection Is Not Created!'); return 0 };
            let correlationId = uuidv4();
            const promise = new Promise(async (resolve, reject) => {
                if (typeof request === 'string') { request = { name: request } }
                this.channel.sendToQueue(params?.dispatchTo || request?.dispatchTo || this.dispatchTo, Buffer.from(JSON.stringify({ request: request })), {
                    correlationId: correlationId,
                    replyTo: params?.consumeOn || this.consumeOn,
                    type: 'request',
                });
                console.log(`[ ]=----(${request.name} request)`);
                let responceMessage = undefined;
                responceMessage = await this.awaitResponce(correlationId, request, params);
                if (responceMessage?.content?.error) reject(responceMessage?.content?.error)
                resolve(modeSelector(responceMessage, params));
            })
            return promise;
        } catch (err) { console.log(err) };
    }

    async post(request, params) {
        if (!this.channel) { console.log('ERROR Connection Is Not Created!'); return 0 };
        let correlationId = uuidv4();
        this.channel.sendToQueue(params?.dispatchTo || request?.dispatchTo || this.dispatchTo, Buffer.from(JSON.stringify({ request: request })), {
            correlationId: correlationId,
            type: 'post',
        });
        console.log(`[ ]=----(${request.name})`);
    }
}

function modeSelector(msg, params) {
    if (params?.output == "full") return (msg);
    if (params?.output == "body") return (msg?.content);
    if (!params?.output) return (msg?.content?.error || msg?.content?.responce);
}

function fixedLengthString(string, n, center) {
    if (string.length > n) {
        let s = string.substr(0, n);
        let words = s.split(' ');
        words[words.length - 1] = ' ';
        string = words.join(' ') + '...'
    }
    let sideMargin;
    if (center == true) {
        sideMargin = n / 2 + string.length / 2;
        return string.padStart(sideMargin, " ")
    }
    else return string.padEnd(n, " ");
}
