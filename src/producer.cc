#include <iostream>

#include "topic.h"
#include "producer.h"

using namespace std;

Producer::Producer(const string& root) : _root(root) {

}

Producer::~Producer() {

}

void Producer::stats() {

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
        }
    }

    if (isFull) {
        rotate();
        return push(batch);
    }

    return true;
}
