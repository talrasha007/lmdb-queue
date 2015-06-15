var native = require('../build/Release/lmdb-queue.node');

module.exports = {
    STRING_TYPE: native.STRING_TYPE,
    BUFFER_TYPE: native.BUFFER_TYPE,

    Consumer: require('./consumer.js'),
    Producer: require('./producer.js'),
    Topic: native.Topic
};