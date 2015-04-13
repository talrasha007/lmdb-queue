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
