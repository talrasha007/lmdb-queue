#include <string.h>

#include "topic.h"

using namespace std;

const char* keyProducerStr = "producer_head";
const char* prefixConsumerStr = "consumer_head_";
const char* keyConsumerStr = "consumer_head_%s";

int descCmp(const MDB_val *a, const MDB_val *b) {
    /* DESC order in size */
    if (a->mv_size > b->mv_size) return -1;
    if (a->mv_size < b->mv_size) return 1;

    switch (a->mv_size)
    {
    case sizeof(uint32_t) :
        return mdbIntCmp<uint32_t>(a, b);
    case sizeof(uint64_t) :
        return mdbIntCmp<uint64_t>(a, b);
    default:
        return memcmp(a->mv_data, b->mv_data, a->mv_size);
    }
}

Topic::Topic(Env* env, const string& name) : _env(env), _name(name) {
    Txn txn(env, NULL);
    int rc = mdb_dbi_open(txn.getEnvTxn(), name.c_str(), MDB_CREATE, &_desc);
    if (rc != 0) {
        printf("Topic open error.\n%s\n", mdb_strerror(rc));
        return;
    }

    mdb_set_compare(txn.getEnvTxn(), _desc, descCmp);

    MDB_val key{ 0, 0 }, val{ 0, 0 };

    uint64_t head = 0;
    key.mv_data = (void*)keyProducerStr;
    key.mv_size = strlen(keyProducerStr);
    val.mv_data = &head;
    val.mv_size = sizeof(head);
    rc = mdb_put(txn.getEnvTxn(), _desc, &key, &val, MDB_NOOVERWRITE);

    if (rc == 0) {
        uint32_t headFile = 0;
        key.mv_data = &headFile;
        key.mv_size = sizeof(headFile);
        mdb_put(txn.getEnvTxn(), _desc, &key, &val, MDB_NOOVERWRITE);
    }

    txn.commit();
}

Topic::~Topic() {
    mdb_dbi_close(_env->getMdbEnv(), _desc);
}

bool checkConsumerKeyPrefix(const MDB_val& val) {
    const char* str = (const char*)val.mv_data;
    return val.mv_size > strlen(prefixConsumerStr) && strncmp(str, prefixConsumerStr, strlen(prefixConsumerStr)) == 0;
}

TopicStatus Topic::status() {
    TopicStatus ret;

    Txn txn(_env, NULL);
    ret.producerHead = getProducerHead(txn);

    MDBCursor cur(_desc, txn.getEnvTxn());
    int rc = cur.gotoFirst();
    while (rc == 0 && checkConsumerKeyPrefix(cur.key())) {
        char name[4096];
        size_t nameLen = cur.key().mv_size - strlen(prefixConsumerStr);
        const char* namePtr = ((const char*)cur.key().mv_data) + strlen(prefixConsumerStr);
        strncpy(name, namePtr, nameLen);
        name[nameLen] = 0;

        ret.consumerHeads[name] = cur.val<uint64_t>();
        rc = cur.next();
    }

    return ret;
}

uint32_t Topic::getProducerHeadFile(Txn& txn) {
    MDBCursor cur(_desc, txn.getEnvTxn());
    cur.gotoLast();
    return cur.key<uint32_t>();
}

void Topic::setProducerHeadFile(Txn& txn, uint32_t file, uint64_t offset) {
    MDB_val key{ sizeof(file), &file},
            val{ sizeof(offset), &offset };

    mdb_put(txn.getEnvTxn(), _desc, &key, &val, 0);
}

uint64_t Topic::getProducerHead(Txn& txn) {
    MDB_val key{ strlen(keyProducerStr), (void*)keyProducerStr },
            val{ 0, 0 };

    mdb_get(txn.getEnvTxn(), _desc, &key, &val);
    return *(uint64_t*)val.mv_data;
}

void Topic::setProducerHead(Txn& txn, uint64_t head) {
    MDB_val key{ strlen(keyProducerStr), (void*)keyProducerStr },
            val{ sizeof(head), &head };

    mdb_put(txn.getEnvTxn(), _desc, &key, &val, 0);
}

uint32_t Topic::getConsumerHeadFile(Txn& txn, const std::string& name, uint32_t searchFrom) {
    uint64_t head = getConsumerHead(txn, name);

    MDBCursor cur(_desc, txn.getEnvTxn());
    int rc = cur.gte(searchFrom);
    uint32_t ret = cur.key<uint32_t>();
    uint64_t fh = cur.val<uint64_t>();
    while (rc == 0 && head >= fh) {
        rc = cur.next();
        if (rc == 0) {
            uint64_t ch = cur.val<uint64_t>();
            if (head < ch) {
                return ret;
            } else {
                ret = cur.key<uint32_t>();
                fh = ch;
            }
        }
    }

    return ret;
}

uint64_t Topic::getConsumerHead(Txn& txn, const std::string& name) {
    char keyStr[4096];
    sprintf(keyStr, keyConsumerStr, name.c_str());

    MDB_val key{ strlen(keyStr), keyStr }, val{ 0, nullptr };
    int rc = mdb_get(txn.getEnvTxn(), _desc, &key, &val);
    if (rc == 0) {
        return *(uint64_t*)val.mv_data;
    } else {
        if (rc != MDB_NOTFOUND) cout << "Consumer seek error: " << mdb_strerror(rc) << endl;

        MDBCursor cur(_desc, txn.getEnvTxn());
        cur.gte(uint32_t(0));
        return cur.val<uint64_t>();
    }
}

void Topic::setConsumerHead(Txn& txn, const std::string& name, uint64_t head) {
    char keyStr[4096];
    sprintf(keyStr, keyConsumerStr, name.c_str());

    MDB_val key{ strlen(keyStr), keyStr },
            val{ sizeof(head), &head };

    mdb_put(txn.getEnvTxn(), _desc, &key, &val, 0);
}

int Topic::getChunkFilePath(char* buf, uint32_t chunkSeq) {
    return sprintf(buf, "%s/%s.%d", getEnv()->getRoot().c_str(), getName().c_str(), chunkSeq);
}

size_t Topic::countChunks(Txn& txn) {
    MDBCursor cur(_desc, txn.getEnvTxn());

    size_t count = 0;
    uint32_t minFile = 0;
    int rc = cur.gte(minFile);

    while (rc == 0 && cur.key().mv_size == 4) {
        ++count;
        rc = cur.next();
    }

    return count;
}

void Topic::removeOldestChunk(Txn& txn) {
    MDBCursor cur(_desc, txn.getEnvTxn());

    uint32_t oldest = 0;
    int rc = cur.gte(oldest);
    if (rc == 0 && cur.key().mv_size == sizeof(oldest)) {
        oldest = cur.key<int32_t>();
        cur.del();

        char path[4096];
        getChunkFilePath(path, oldest);
        remove(path);
        strcat(path, "-lock");
        remove(path);
    }
}
