"use strict";

var NativeProducer = require('../build/Release/lmdb-queue.node').Producer;

function Producer() {
    this._nativeProducer = new NativeProducer();
}

Producer.prototype = {
    push: function (msg) {
        if (typeof msg === 'string') {
            this._nativeProducer.pushString(msg);
        } else if (Buffer.isBuffer(msg)) {
            this._nativeProducer.pushBuffer(msg);
        } else if (Array.isArray(msg)) {
            msg.forEach(function (m) {
                this.push(m);
            });
        }
    }
};

module.exports = Producer;
