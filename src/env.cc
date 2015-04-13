#include "env.h"

using namespace std;

Env* EnvManager::getEnv(const string& root, DbOpt *opt) {
    static EnvManager instance;

    std::lock_guard<std::mutex> guard(instance._mtx);
    EnvPtr& ptr = instance._envMap[root];
    if (!ptr.get()) ptr.reset(new Env(root, opt));

    return ptr.get();
}

Env::Env(const string& root, DbOpt* opt) : _root(root), _env(NULL) {
    mdb_env_create(&_env);

    if (opt) {
        mdb_env_set_mapsize(_env, opt->mapSize);
        mdb_env_set_maxdbs(_env, opt->maxDbs);
    }

    int rc = mdb_env_open(_env, root.c_str(), MDB_NOSYNC, 0664);
    if (rc != 0) {
        mdb_env_close(_env);
        _env = NULL;
        printf("Env open error.\n%s\n", mdb_strerror(rc));
        return;
    }

    int cleared = 0;
    mdb_reader_check(_env, &cleared);
}

Env::~Env() {
    if (_env) {
        mdb_env_close(_env);
        _env = NULL;
    }
}
