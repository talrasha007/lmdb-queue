#pragma once

#include "env.h"

class Topic {
public:
    Topic(Env* env, const std::string& name);
    ~Topic();

private:
    Topic(const Topic&);
    Topic& operator=(const Topic&);

public:
    TopicStatus status();

    inline Env* getEnv() { return _env; }
    inline const std::string& getName() { return _name; }

    uint32_t getProducerHeadFile(Txn& txn);
    void setProducerHeadFile(Txn& txn, uint32_t file, uint64_t offset);

    uint64_t getProducerHead(Txn& txn);
    void setProducerHead(Txn& txn, uint64_t head);

    uint32_t getConsumerHeadFile(Txn& txn, const std::string& name, uint32_t searchFrom);
    uint64_t getConsumerHead(Txn& txn, const std::string& name);
    uint64_t getConsumerByte(Txn& txn, const std::string& name);
    void setConsumerHead(Txn& txn, const std::string& name, uint64_t head, uint64_t byte);

    int getChunkFilePath(char* buf, uint32_t chunkSeq);
    size_t countChunks(Txn& txn);
    void removeOldestChunk(Txn& txn);

private:
    Env *_env;

    std::string _name;
    MDB_dbi _desc;
};
