"use strict";

var LmdbQueue = require('../'),
    Producer = LmdbQueue.Producer;

var producer = new Producer({ backgroundFlush: true, useCache: true, path: __dirname + '/test-data', topic: 'test', dataType: LmdbQueue.STRING_TYPE, chunkSize: 64 * 1024 * 1024, chunksToKeep: 8 }),
    start = Date.now();

console.log('Begin write to queue.');

var step = 1000;
for (var i = 0; i < 10 * 1000 * 1000; i += step) {
    //producer.push('msg' + i);
    var msg = [];
    for (var j = 0; j < step; j++) msg.push('msg' + (i + j));
    producer.push(msg);
}

console.log('Pushed 10M messages in %d ms', Date.now() - start);
setTimeout(function () {}, 12000);