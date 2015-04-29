"use strict";

var LmdbQueue = require('../'),
    Producer = LmdbQueue.Producer;

var producer = new Producer({ path: __dirname + '/test-data', topic: 'test', dataType: LmdbQueue.STRING_TYPE, chunkSize: 1024 * 1024 * 1024, keepHours: 24 * 2 }),
    start = Date.now();

console.log('Begin write to queue.');

var step = 1024;
for (var i = 0; i < 10 * 1024 * 1024; i += step) {
    //producer.push('msg' + i);
    var msg = [];
    for (var j = 0; j < step; j++) msg.push('msg' + (i + j));
    producer.push(msg);
}

console.log('Pushed 1M messages in %d ms', Date.now() - start);