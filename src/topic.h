#pragma once

#include "env.h"

class Topic {
public:
    Topic(Env* env, const std::string& name);
    ~Topic();

public:
    inline Env* getEnv() { return _env; }

    uint64_t getProducerHead(Txn& txn);
    void setProducerHead(Txn& txn, uint64_t head);

    uint64_t getConsumerHead(Txn& txn, const std::string& name);
    void setConsumerHead(Txn& txn, const std::string& name, uint64_t head);

private:
    Env *_env;

    MDB_dbi _desc;
};