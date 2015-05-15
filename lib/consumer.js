"use strict";

var nativeModule = require('../build/Release/lmdb-queue.node'),
    NativeConsumer = nativeModule.Consumer;

function Consumer(opt) {
    this._nativeConsumer = new NativeConsumer(opt);
    this._pullFnName = opt.dataType === nativeModule.BUFFER_TYPE ? 'pullBuffer' : 'pullString';
}

Consumer.prototype = {
    offset: function () {
        return this._nativeConsumer.offset();
    },

    pull: function () {
        return this._nativeConsumer[this._pullFnName]();
    }
};

module.exports = Consumer;
