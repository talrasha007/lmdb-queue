# lmdb-queue
  A high performance & embeded queue that can process millions of messages per second. The queue store can be accessed by multi-process.
  (Now, it is node.js only, but its core is implemented by C++, it'll be easy to make it work for other languages.)
  
## Usage
- Producer
```js
var LmdbQueue = require('../'),
    Producer = LmdbQueue.Producer;

var producer = new Producer({ path: __dirname + '/test-data', topic: 'test', dataType: LmdbQueue.STRING_TYPE, chunkSize: 64 * 1024 * 1024, chunksToKeep: 8 });

/*
options:
  path: The path where to put queue files, remember to create it before open it.
  topic: Topic name.
  dataType:
    LmdbQueue.STRING_TYPE: message is treated as string.
    LmdbQueue.BUFFER_TYPE: message is treated as buffer.
  chunkSize: The size of data chunk(in bytes).
  chunksToKeep: How many chunks to keep.
*/

producer.push(['abcdeaef', 'deffdaf']);
```

- Consumer
```js
var LmdbQueue = require('../'),
    Consumer = LmdbQueue.Consumer;

var consumer = new Consumer({ path: __dirname + '/test-data', topic: 'test', name: 'test', dataType: LmdbQueue.STRING_TYPE, chunkSize: 64 * 1024 * 1024, batchSize: 1024 * 16 });
/*
options:
  path: The path where to put queue files, remember to create it before open it.
  topic: Topic name.
  dataType:
    LmdbQueue.STRING_TYPE: message is treated as string.
    LmdbQueue.BUFFER_TYPE: message is treated as buffer.
  chunkSize: The size of data chunk(in bytes).
  batchSize: The read batch size.
*/

var msg;
while (msg = consumer.pop()) {
    // ...do sth with msg...
}
```
