"use strict";

var LmdbQueue = require('../'),
    Producer = LmdbQueue.Producer;

var producer = new Producer({ path: 'test-data', dataType: LmdbQueue.STRING_TYPE, chunkSize: 1024 * 1024 * 1024, keepHours: 24 * 2 }),
    start = Date.now();

for (var i = 0; i < 1024 * 1024; i += 10) {
    //producer.push('msg' + i);
    var msg = [];
    for (var j = 0; j < 10; j++) msg.push('msg' + (i + j));
    producer.push(msg);
}

console.log('Pushed 1M messages in %d ms', Date.now() - start);