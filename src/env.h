#pragma once

#include <mutex>
#include <map>
#include <memory>
#include <string>

#include "wrapper.h"

class Topic;

struct EnvOpt {
    size_t maxTopicNum;
    size_t mapSize;
};

struct TopicOpt {
    size_t chunkSize;
    size_t chunksToKeep;
};

struct ConsumeInfo {
    uint64_t head;
    uint64_t byte;
};

struct TopicStatus{
    uint64_t producerHead;
    std::map<std::string, ConsumeInfo> consumerHeads;
};

class Env {
private:
    friend class EnvManager;
    friend class Txn;

    Env(const std::string& root, EnvOpt *opt);
    Env(const Env&);
    Env& operator=(const Env&);

public:
    ~Env();

    const std::string& getRoot() { return _root; }
    MDB_env* getMdbEnv() { return _env; }

    Topic* getTopic(const std::string& name);

private:
    std::string _root;
    MDB_env *_env;

    typedef std::unique_ptr<Topic> TopicPtr;
    typedef std::map<std::string, TopicPtr> TopicMap;

    std::mutex _mtx;
    TopicMap _topics;
};

class EnvManager {
public:
    static Env* getEnv(const std::string& root, EnvOpt *opt = NULL);

private:
    std::mutex _mtx;

    typedef std::unique_ptr<Env> EnvPtr;
    typedef std::map<std::string, EnvPtr> EnvMap;
    EnvMap _envMap;
};

class Txn {
public:
    Txn(Env* env, MDB_env* consumerOrProducerEnv, bool readOnly = false) : _abort(false), _envTxn(nullptr), _cpTxn(nullptr) {
        mdb_txn_begin(env->_env, NULL, 0, &_envTxn);
        if (consumerOrProducerEnv) mdb_txn_begin(consumerOrProducerEnv, NULL, 0, &_cpTxn);
    }

    ~Txn() {
        if (_cpTxn) mdb_txn_abort(_cpTxn);
        if (_envTxn) mdb_txn_abort(_envTxn);
    }

public:
    inline MDB_txn* getEnvTxn() { return _envTxn; }
    inline MDB_txn* getTxn() { return _cpTxn; }

    void abort() {
        if (_cpTxn) mdb_txn_abort(_cpTxn);
        mdb_txn_abort(_envTxn);

        _cpTxn = _envTxn = nullptr;
    }

    int commit() {
        int rc = 0;
        if (_cpTxn) {
            rc = mdb_txn_commit(_cpTxn);
            if (rc != 0) {
                mdb_txn_abort(_envTxn);
                _cpTxn = _envTxn = nullptr;
                return rc;
            }
        }

        rc = mdb_txn_commit(_envTxn);
        _cpTxn = _envTxn = nullptr;
        return rc;
    }

private:
    Txn(const Txn&);
    Txn& operator=(const Txn&);

private:
    bool _abort;
    MDB_txn *_envTxn, *_cpTxn;
};

template<typename INT_TYPE> int mdbIntCmp(const MDB_val *a, const MDB_val *b) {
    INT_TYPE ia = *(INT_TYPE*)a->mv_data;
    INT_TYPE ib = *(INT_TYPE*)b->mv_data;
    return ia < ib ? -1 : ia > ib ? 1 : 0;
}
