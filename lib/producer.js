"use strict";

var NativeProducer = require('../build/Release/lmdb-queue.node').Producer;

function Producer() {
    this._nativeProducer = new NativeProducer();
}

Producer.prototype = {
    push: function (msg) {
        var nativeProducer = this._nativeProducer;

        if (Array.isArray(msg)) {
            if (typeof msg[0] === 'string') {
                return nativeProducer.pushString.apply(nativeProducer, msg);
            } else if (Buffer.isBuffer(msg[0])) {
                return nativeProducer.pushBuffer.apply(nativeProducer, msg);
            }
        } else if (typeof msg === 'string') {
            return nativeProducer.pushString(msg);
        } else if (Buffer.isBuffer(msg)) {
            return nativeProducer.pushBuffer(msg);
        }

        throw new Error('Invalid message type.');
    }
};

module.exports = Producer;
