#pragma once

#include <vector>
#include <tuple>
#include <string>

#include <lmdb/lmdb.h>

class Producer {
public:
    typedef std::vector<std::tuple<const char*, size_t> > BatchType;

public:
	Producer(const std::string& root);
	~Producer();

public:
    bool push(const BatchType& batch);

private:
	std::string _root;
};
