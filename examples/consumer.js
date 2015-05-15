"use strict";

var LmdbQueue = require('../'),
    Consumer = LmdbQueue.Consumer;

var consumer = new Consumer({ path: __dirname + '/test-data', topic: 'test', name: 'test', dataType: LmdbQueue.STRING_TYPE, chunkSize: 64 * 1024 * 1024 }),
    start = Date.now();

console.log('Begin read queue.');

for (var i = 0; i < 10 * 1024 * 1024; i++) {
    consumer.pull();
    consumer.offset();
}

console.log('Read 10M messages in %d ms', Date.now() - start);