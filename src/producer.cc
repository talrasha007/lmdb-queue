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
    return true;
}
