#pragma once

#include <vector>
#include <tuple>
#include <string>

#include <lmdb/lmdb.h>

class Topic;

class Producer {
public:
    typedef std::vector<std::tuple<const char*, size_t> > BatchType;

public:
	Producer(const std::string& root);
	~Producer();

public:
    void stats();
    bool push(const BatchType& batch);

private:
    void rotate();

private:
	std::string _root;
    Topic* _topic;

    MDB_env* _env;
    MDB_dbi _db;
};
