#include <string.h>

#include "topic.h"

using namespace std;

const char* keyProducerStr = "producer_head";
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
    mdb_put(txn.getEnvTxn(), _desc, &key, &val, MDB_NOOVERWRITE);

    uint32_t headFile = 0;
    key.mv_data = &headFile;
    key.mv_size = sizeof(headFile);
    mdb_put(txn.getEnvTxn(), _desc, &key, &val, MDB_NOOVERWRITE);
    txn.commit();
}

Topic::~Topic() {
    mdb_dbi_close(_env->getMdbEnv(), _desc);
}

uint32_t Topic::getProducerHeadFile(Txn& txn) {
    MDB_cursor *cur = NULL;
    mdb_cursor_open(txn.getEnvTxn(), _desc, &cur);

    MDB_val key{ 0, 0 }, val{ 0, 0 };
    mdb_cursor_get(cur, &key, &val, MDB_LAST);
    mdb_cursor_close(cur);

    return *(uint32_t*)key.mv_data;
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
