"use strict";

var nativeModule = require('../build/Release/lmdb-queue.node'),
    NativeProducer = nativeModule.Producer;

function Producer(opt) {
    this._nativeProducer = new NativeProducer(opt);
    this._pushFnName = opt.dataType === nativeModule.BUFFER_TYPE ? 'pushBuffer' : 'pushString';
}

Producer.prototype = {
    push: function (msg) {
        var nativeProducer = this._nativeProducer,
            fn = this._pushFnName;

        if (Array.isArray(msg)) {
            return nativeProducer[fn].apply(nativeProducer, msg);
        } else {
            return nativeProducer[fn](msg);
        }
    }
};

module.exports = Producer;
