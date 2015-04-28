#pragma once

#include <mutex>
#include <map>
#include <memory>
#include <string>
#include <lmdb/lmdb.h>

struct DbOpt {
    size_t maxDbs;
    size_t mapSize;
};

class Env {
private:
    friend class EnvManager;
    friend class Txn;

    Env(const std::string& root, DbOpt *opt);

public:
    ~Env();

private:
    std::string _root;
    MDB_env *_env;
};

class EnvManager {
public:
    static Env* getEnv(const std::string& root, DbOpt *opt = NULL);

private:
    std::mutex _mtx;

    typedef std::unique_ptr<Env> EnvPtr;
    typedef std::map<std::string, EnvPtr> EnvMap;
    EnvMap _envMap;
};

class Txn {
public:
    Txn(Env* env, MDB_env* consumerOrProducerEnv, bool readOnly = false) : _abort(false), _envTxn(NULL), _cpTxn(NULL) {
        mdb_txn_begin(env->_env, NULL, 0, &_envTxn);
        if (consumerOrProducerEnv) mdb_txn_begin(consumerOrProducerEnv, NULL, 0, &_cpTxn);
    }

    ~Txn() {
        if (_abort) {
            if (_cpTxn) mdb_txn_abort(_cpTxn);
            mdb_txn_abort(_envTxn);
        } else {
            if (_cpTxn) mdb_txn_commit(_cpTxn);
            mdb_txn_commit(_envTxn);
        }
    }

public:
    inline MDB_txn* getEnvTxn() { return _envTxn; }
    inline MDB_txn* getTxn() { return _cpTxn; }

    void abort() {
        _abort = true;
    }

private:
    Txn(const Txn&);
    Txn& operator=(const Txn&);

private:
    bool _abort;
    MDB_txn *_envTxn, *_cpTxn;
};
