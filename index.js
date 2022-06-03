const EventEmitter = require('events');
var amqp = require('amqplib/callback_api');
const { v4: uuidv4 } = require('uuid');
exports.ConnectionManager = class ConnectionManager {
    /**
    * Сreates a ConnectionManager object with methods for simplified routing of function execution requests and sending them
    *
    * @param {object} config parameters for connection configuration and manager operation
    * @param {string} config.name the name of the manager for the convenience of displaying information
    * @param {string} config.consumeOn name of the queue for receiving requests by default
    * @param {string} config.dispatchTo name of the queue for sending requests by default
    * @param {boolean} config.durable the rabbitmq parameter for the queue (There may be errors when synchronizing connection parameters with the rabbitmq server. sometimes it should be enabled)
    * @param {boolean} config.showLogs parameter to enable logging display in the console when receiving and sending requests
    * @param {number} config.timeout parameter for setting the time interval for waiting for a response to a request by default
    * @param {boolean} config.throwErrors parameter that includes throwing out an error if there is one in the response by default
    */
    constructor(config) {
        this.connectionName = config.name;
        this.dispatchTo = config.dispatchTo;
        this.consumeOn = config.consumeOn;
        this.durable = config.durable || false;
        this.safeMode = config.safeMode || true;
        this.showLogs = config.showLogs || false;
        this.timeout = config.timeout || 8000;
        this.throwErrors = config.throwErrors || true;
        this.listeners = [];
        this.InputRequestEmitter = new EventEmitter;
        this.InputResponceEmitter = new EventEmitter;
        if (config.showInfoTable == true) this.#showInfoTable();
    }


    /**
    * Сonnects using rabbitmq to the specified queue and starts listening to incoming requests for the execution of functions
    */
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

    /**
    * Creates a listener for a request to execute a function, and when a request is received to launch a function by a specific name, launches it
    *
    * @param {string} requestName identifier (name) of the function to run it
    * @param {function} funcBody the body of the function that accepts two parameters when a request is received: the request object and the response object
    * @return {ConnectionManager}
    */
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
                    if (this.showLogs == true) console.log(`[ ]=----(${requestName} responce)`);
                },
                error: function (error, params) {
                    if (!requestMessage.properties.replyTo || !requestMessage.properties.correlationId) return 0
                    self.channel.sendToQueue(requestMessage.properties.replyTo,
                        Buffer.from(JSON.stringify({ error: error, requestMessage: requestMessage, requestBody: requestMessage?.content?.request, request: requestMessage?.content?.request?.name })), {
                        correlationId: requestMessage.properties.correlationId,
                        type: params?.type || 'error'
                    });
                    if (this.showLogs == true) console.log(`[ ]=----(${requestName} error)`);
                }
            }
            if (this.showLogs == true) console.log(`[ ]<----(${requestName} request)`);
            let fns = this.listeners[requestName];
            if (!fns) return false;
            fns.forEach((f) => {
                f(requestMessage.content.request, responce, requestMessage);
            });
            return true;
        } catch (error) { console.log(error) }
    }

    async #awaitResponce(correlationId, request, params) {
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
                if (this.showLogs == true) console.log(`[ ]<----(${request.name} responce)`);
                this.InputResponceEmitter.removeAllListeners(correlationId);
                clearTimeout(timeout);
                resolve(msg)
            })
        })
        return await promise;
    }

    /**
    * A function for waiting for a response to be received
    *
    * @param {object} request identifier (name) of the function to run it
    * @param {object} params an object with parameters for sending a request
    * @param {string} params.dispatchTo name of the queue for sending the request
    * @param {string} params.replyTo name of the queue to receive a response
    * @param {number} params.timeout the time interval in milliseconds after which the system stops waiting for a response
    * @return {Promise<any>} responce promise
    */
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
                if (this.showLogs == true) console.log(`[ ]=----(${request.name} request)`);
                let responceMessage = undefined;
                responceMessage = await this.#awaitResponce(correlationId, request, params);
                if (responceMessage?.content?.error) reject(responceMessage?.content?.error)
                resolve(modeSelector(responceMessage, params));
            })
            return promise;
        } catch (err) { console.log(err) };
    }
    /**
    * A function for sending a request to perform a function without waiting for a response
    *
    * @param {object} request identifier (name) of the function to run it
    * @param {object} params an object with parameters for sending a request
    * @param {string} params.dispatchTo name of the queue for sending the request
    * @param {string} params.replyTo name of the queue to receive a response
    * @param {number} params.timeout the time interval in milliseconds after which the system stops waiting for a response
    * @returns {void}
    */
    post(request, params) {
        if (!this.channel) { console.log('ERROR Connection Is Not Created!'); return 0 };
        let correlationId = uuidv4();
        this.channel.sendToQueue(params?.dispatchTo || request?.dispatchTo || this.dispatchTo, Buffer.from(JSON.stringify({ request: request })), {
            correlationId: correlationId,
            type: 'post',
        });
        if (this.showLogs == true) console.log(`[ ]=----(${request.name})`);
    }

    #showInfoTable() {
        console.log(`
            ========== CONNECTION MANAGER ==========
            ${fixedLengthString(`${this.connectionName}`, 40, true)}
            ========================================
            #   ${fixedLengthString(`durable = ${this.durable}`, 35)}#
            #   ${fixedLengthString(`getTimeout = ${this.timeout}`, 35)}#
            #   ${fixedLengthString(`safemode = ${this.safeMode}`, 35)}#
            #   ${fixedLengthString(`showLogs = ${this.showLogs}`, 35)}#
            #   ${fixedLengthString(`dispatchTo = ${this.dispatchTo}`, 35)}#
            #   ${fixedLengthString(`consumeOn = ${this.consumeOn}`, 35)}#
            #   ${fixedLengthString(`throwErrors = ${this.throwErrors}`, 35)}#
            ========================================
            `)
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

