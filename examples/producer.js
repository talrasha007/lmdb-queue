"use strict";

var Producer = require('../').Producer;

var producer = new Producer('test-data', 1024 * 1024 * 1024),
    start = Date.now();

for (var i = 0; i < 1024 * 1024; i++) {
    producer.push('msg' + i);
}

console.log('Pushed 1M messages in %d ms', Date.now() - start);