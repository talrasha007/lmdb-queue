#pragma once

#include <vector>
#include <tuple>
#include <string>

#include <lmdb/lmdb.h>
#include "env.h"

class Topic;

class Producer {
public:
    typedef std::vector<std::tuple<const char*, size_t> > BatchType;

public:
	Producer(const std::string& root, const std::string& topic, TopicOpt* opt);
	~Producer();

private:
    Producer(const Producer&);
    Producer& operator=(const Producer&);

public:
    bool push(const BatchType& batch);

private:
    void openHead(Txn* txn, bool rotating = false);
    void closeCurrent();
    void rotate();

private:
    TopicOpt _opt;
    Topic* _topic;

    uint32_t _current;
    MDB_env* _env;
    MDB_dbi _db;
};
