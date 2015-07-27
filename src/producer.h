#pragma once

#include <thread>
#include <condition_variable>
#include <chrono>
#include <vector>
#include <tuple>
#include <string>

#include <lmdb/lmdb.h>
#include "env.h"

class Topic;

class Producer {
public:
    class ItemType {
    public:
        static ItemType create(size_t len);

    public:
        ItemType(char* mem, size_t len) : _mem(mem), _len(len), _shouldDelete(false) {
        }

        ItemType(ItemType&& r) : _mem(r._mem), _len(r._len), _shouldDelete(r._shouldDelete) {
            r._mem = nullptr;
            r._len = 0;
            r._shouldDelete = false;
        }

        ~ItemType();

    public:
        inline size_t len() const { return _len; }
        inline char* data() { return _mem; }
        inline const char* data() const { return _mem; }

    private:
        ItemType(const ItemType&);
        ItemType& operator=(const ItemType&);

    private:
        char* _mem;
        size_t _len;
        bool _shouldDelete;
    };

    typedef std::vector<ItemType> BatchType;

public:
	Producer(const std::string& root, const std::string& topic, TopicOpt* opt, bool useBackgroundFlush);
	~Producer();

private:
    Producer(const Producer&);
    Producer& operator=(const Producer&);

public:
    bool push(const BatchType& batch);

    void setCacheSize(size_t sz);
    void push(ItemType&& item); // Push to cache
    void flush();

private:
    void flushWorker();
    void flushImpl();

    void openHead(Txn* txn, bool rotating = false);
    void closeCurrent();
    void rotate();

private:
    TopicOpt _opt;
    Topic* _topic;

    uint32_t _current;
    MDB_env* _env;
    MDB_dbi _db;

    bool _bgEnabled, _bgRunning;
    std::thread _bgFlush;
    std::condition_variable _bgCv;
    std::mutex _cacheMtx, _flushMtx;
    size_t _cacheMax; // Default: 100
    BatchType _cache0, _cache1, *_cacheCurrent;
};
