"use strict";

var LmdbQueue = require('../'),
    Topic = LmdbQueue.Topic;

var topic = new Topic({ path: __dirname + '/test-data', topic: 'test' });
console.log(topic.status());