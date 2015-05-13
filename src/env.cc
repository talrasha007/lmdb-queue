#include "topic.h"
#include "env.h"

using namespace std;

Env* EnvManager::getEnv(const string& root, EnvOpt *opt) {
    static EnvManager instance;

    std::lock_guard<std::mutex> guard(instance._mtx);
    EnvPtr& ptr = instance._envMap[root];
    if (!ptr.get()) ptr.reset(new Env(root, opt));

    return ptr.get();
}

Env::Env(const string& root, EnvOpt* opt) : _root(root), _env(nullptr) {
    mdb_env_create(&_env);

    if (opt) {
        mdb_env_set_mapsize(_env, opt->mapSize);
        mdb_env_set_maxdbs(_env, opt->maxTopicNum);
    } else {
        /* Default opt */
        mdb_env_set_mapsize(_env, 256 * 1024 * 1024);
        mdb_env_set_maxdbs(_env, 256);
    }

    string path = root + "/__meta__";
    int rc = mdb_env_open(_env, path.c_str(), MDB_NOSYNC | MDB_NOSUBDIR, 0664);
    if (rc != 0) {
        mdb_env_close(_env);
        _env = nullptr;
        printf("Env open error.\n%s\n", mdb_strerror(rc));
        return;
    }

    int cleared = 0;
    mdb_reader_check(_env, &cleared);
}

Env::~Env() {
    _topics.clear();
    if (_env) {
        mdb_env_close(_env);
        _env = nullptr;
    }
}

Topic* Env::getTopic(const string& name) {
    std::lock_guard<std::mutex> guard(_mtx);
    TopicPtr& ptr = _topics[name];
    if (!ptr.get()) ptr.reset(new Topic(this, name));

    return ptr.get();
}
