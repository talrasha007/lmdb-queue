#pragma once

#include <vector>
#include <tuple>

#include "env.h"

class Topic;

class Consumer {
public:
    typedef std::tuple<uint64_t, const char*, size_t> ItemType;
    typedef std::vector<ItemType> BatchType;

public:
    Consumer(const std::string& root, const std::string& topic, const std::string& name, TopicOpt* opt);
    ~Consumer();

private:
    Consumer(const Consumer&);
    Consumer& operator=(const Consumer&);

public:
    void pop(BatchType& result, size_t cnt);

private:
    void openHead(Txn* txn);
    void closeCurrent();
    void rotate();

private:
    TopicOpt _opt;
    Topic* _topic;
    std::string _name;

    uint32_t _current;
    uint64_t _lastOffset;
    MDB_env* _env;
    MDB_dbi _db;
    MDB_txn* _rtxn;
    MDBCursor* _cursor;
};
