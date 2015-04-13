#include "topic.h"
#include "producer.h"

using namespace std;

Producer::Producer(const string& root) : _root(root) {

}

Producer::~Producer() {

}

bool Producer::push(const Producer::BatchType& batch) {
    return true;
}
