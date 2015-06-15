#ifdef _WIN32
#include <windows.h>
#endif

#include <stdio.h>
#include <iostream>

#include "topic.h"
#include "producer.h"

using namespace std;

Producer::Producer(const string& root, const string& topic, TopicOpt* opt) : _topic(EnvManager::getEnv(root)->getTopic(topic)), _current(-1), _env(nullptr), _db(0) {
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

Producer::~Producer() {
    closeCurrent();
}

bool Producer::push(const Producer::BatchType& batch) {
    bool isFull = false;

    {
        Txn txn(_topic->getEnv(), _env);

        uint64_t head = _topic->getProducerHead(txn);
        for (auto item : batch) {
            MDB_val key{ sizeof(head), &++head },
                    val{ std::get<1>(item), (void*)std::get<0>(item) };

            int rc = mdb_put(txn.getTxn(), _db, &key, &val, MDB_APPEND);
            if (rc == MDB_MAP_FULL) {
                txn.abort();
                isFull = true;
                break;
            }
        }

        if (!isFull) {
            _topic->setProducerHead(txn, head);
            int rc = txn.commit();
            if (rc == MDB_MAP_FULL) {
                isFull = true;
            }
        }
    }

    if (isFull) {
        rotate();
        return push(batch);
    }

    return true;
}

void Producer::closeCurrent() {
    mdb_dbi_close(_env, _db);
    mdb_env_close(_env);
}

void Producer::openHead(Txn* txn, bool rotating) {
    uint32_t headFile = _topic->getProducerHeadFile(*txn);
    if (rotating && _current == headFile) {
        _topic->setProducerHeadFile(*txn, ++headFile, _topic->getProducerHead(*txn) + 1);
    }

    _current = headFile;

    char path[4096];
    _topic->getChunkFilePath(path, headFile);

#ifdef _WIN32
    Sleep(500); // Fix error on windows when multi process rotate at same time. ("The requested operation cannot be performed on a file with a user-mapped section open.")
#endif
    mdb_env_create(&_env);
    mdb_env_set_mapsize(_env, _opt.chunkSize);
    int rc = mdb_env_open(_env, path, MDB_NOSYNC | MDB_NOSUBDIR, 0664);

    int cleared = 0;
    mdb_reader_check(_env, &cleared);

    if (rc != 0) {
        mdb_env_close(_env);
        _env = nullptr;
        printf("Producer open error.\n%s\n", mdb_strerror(rc));
        return;
    }

    MDB_txn *otxn;
    mdb_txn_begin(_env, NULL, 0, &otxn);
    mdb_dbi_open(otxn, NULL, MDB_CREATE, &_db);
    mdb_set_compare(otxn, _db, mdbIntCmp<uint64_t>);
    mdb_txn_commit(otxn);
}

void Producer::rotate() {
    Txn txn(_topic->getEnv(), NULL);

    closeCurrent();
    for (size_t chunks = _topic->countChunks(txn); chunks >= _opt.chunksToKeep; --chunks) {
        _topic->removeOldestChunk(txn);
    }

    openHead(&txn, true);
    txn.commit();
}
