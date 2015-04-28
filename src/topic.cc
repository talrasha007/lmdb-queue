#include "topic.h"

using namespace std;

const char* keyProducerStr = "producer_head";
const char* keyConsumerStr = "consumer_head_%s";

Topic::Topic(Env* env, const string& name) : _env(env) {

}

Topic::~Topic() {

}

uint64_t Topic::getProducerHead(Txn& txn) {
    MDB_val key{ strlen(keyProducerStr), (void*)keyProducerStr },
            val{ 0 };

    int rc = mdb_get(txn.getEnvTxn(), _desc, &key, &val);
    return rc == MDB_NOTFOUND ? 0 : *(uint16_t*)val.mv_data;
}

void Topic::setProducerHead(Txn& txn, uint64_t head) {
    MDB_val key{ strlen(keyProducerStr), (void*)keyProducerStr },
            val{ sizeof(head), &head };

    mdb_put(txn.getEnvTxn(), _desc, &key, &val, 0);
}
