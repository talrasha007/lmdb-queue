#include "topic.h"
#include "consumer.h"

using namespace std;

Consumer::Consumer(const std::string& root, const std::string& topic, const std::string& name, TopicOpt* opt) : _topic(EnvManager::getEnv(root)->getTopic(topic)), _name(name), _current(0), _lastOffset(0), _env(nullptr), _db(0), _rtxn(nullptr), _cursor(nullptr) {
    if (opt) {
        _opt = *opt;
    } else {
        /* Default opt */
        _opt.chunkSize = 1024 * 1024 * 1024;
        _opt.chunksToKeep = 8;
    }

    Txn txn(_topic->getEnv(), NULL);
    openHead(&txn);
    txn.commit();
}

Consumer::~Consumer() {
    closeCurrent();
}

void Consumer::pull(BatchType& result, size_t cnt) {
    result.reserve(cnt);
    bool shouldRotate = false;

    {
        Txn txn(_topic->getEnv(), NULL);
        mdb_txn_renew(_rtxn);

        uint64_t head = _topic->getConsumerHead(txn, _name);
        int rc = _cursor->seek(head);

        if (rc == 0) {
            uint64_t offset = 0;
            for (; rc == 0 && cnt > 0; --cnt) {
                offset = _cursor->key<uint64_t>();
                const char* data = (const char*)_cursor->val().mv_data;
                size_t len = _cursor->val().mv_size;
                result.push_back(ItemType(offset, data, len));
                rc = _cursor->next();
            }

            if (offset > 0) {
                _topic->setConsumerHead(txn, _name, offset + 1);
                txn.commit();
            }
        } else {
            if (rc != MDB_NOTFOUND) cout << "Consumer seek error: " << mdb_strerror(rc) << endl;

            if (head < _topic->getProducerHead(txn)) {
                shouldRotate = true;
            }
        }
    }

    if (shouldRotate) {
        rotate();
        pull(result, cnt);
    }
}

void Consumer::openHead(Txn* txn) {
    _current = _topic->getConsumerHeadFile(*txn, _name, _current);

    char path[4096];
    _topic->getChunkFilePath(path, _current);

    mdb_env_create(&_env);
    mdb_env_set_mapsize(_env, _opt.chunkSize);
    int rc = mdb_env_open(_env, path, MDB_RDONLY | MDB_NOSYNC | MDB_NOSUBDIR, 0664);

    int cleared = 0;
    mdb_reader_check(_env, &cleared);

    if (rc != 0) {
        mdb_env_close(_env);
        _env = nullptr;
        printf("Producer open error.\n%s\n", mdb_strerror(rc));
        return;
    }

    MDB_txn *otxn;
    mdb_txn_begin(_env, NULL, MDB_RDONLY, &otxn);
    mdb_dbi_open(otxn, NULL, MDB_CREATE, &_db);
    mdb_set_compare(otxn, _db, mdbIntCmp<uint64_t>);
    mdb_txn_commit(otxn);

    mdb_txn_begin(_env, NULL, MDB_RDONLY, &_rtxn);
    _cursor = new MDBCursor(_db, _rtxn);
    mdb_txn_reset(_rtxn);
}

void Consumer::closeCurrent() {
    delete _cursor;
    mdb_txn_abort(_rtxn);
    mdb_dbi_close(_env, _db);
    mdb_env_close(_env);
}

void Consumer::rotate() {
    Txn txn(_topic->getEnv(), NULL);
    closeCurrent();
    openHead(&txn);
    txn.commit();
}