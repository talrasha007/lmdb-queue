#include <nnu.h>

#include "topic.h"
#include "producer.h"
#include "consumer.h"

using namespace std;
using namespace v8;
using namespace node;

enum DataType {
    STRING_TYPE = 1,
    BUFFER_TYPE
};

template<DataType DT> class BatchWrap {
public:
    void push(const Local<Value>& val);

public:
    Producer::BatchType& get() {
        return _batch;
    }

    void reserve(size_t sz) {
        _batch.reserve(sz);
    }

private:
    Producer::BatchType _batch;
};

template<> void BatchWrap<STRING_TYPE>::push(const Local<Value>& val) {
    v8::Local<v8::String> toStr = val->ToString();
    size_t size = toStr->Utf8Length();
    Producer::ItemType item = Producer::ItemType::create(size + 1);
    toStr->WriteUtf8(item.data());
    _batch.push_back(std::move(item));
}

template<> void BatchWrap<BUFFER_TYPE>::push(const Local<Value>& val) {
    _batch.push_back(Producer::ItemType(node::Buffer::Data(val), node::Buffer::Length(val)));
}

class ProducerWrap : public nnu::ClassWrap<ProducerWrap> {
public:
    static const char * const CLASS_NAME; // "Producer"

    static void setupMember(v8::Local<v8::FunctionTemplate>& tpl) {
        Nan::SetPrototypeMethod(tpl, "pushString", wrapFunction<&ProducerWrap::push<STRING_TYPE> >);
        Nan::SetPrototypeMethod(tpl, "pushBuffer", wrapFunction<&ProducerWrap::push<BUFFER_TYPE> >);

        Nan::SetPrototypeMethod(tpl, "pushString2Cache", wrapFunction<&ProducerWrap::push2Cache<STRING_TYPE> >);
        Nan::SetPrototypeMethod(tpl, "pushBuffer2Cache", wrapFunction<&ProducerWrap::push2Cache<BUFFER_TYPE> >);
    }

    static NAN_METHOD(ctor) {
        Handle<Object> opt = info[0]->ToObject();

        Nan::Utf8String path(opt->Get(nnu::newString("path")));
        Nan::Utf8String topicName(opt->Get(nnu::newString("topic")));

        TopicOpt topicOpt{ 1024 * 1024 * 1024, 8 };
        Local<Value> chunkSize = opt->Get(nnu::newString("chunkSize"));
        Local<Value> chunksToKeep = opt->Get(nnu::newString("chunksToKeep"));
        Local<Value> bgFlush = opt->Get(nnu::newString("backgroundFlush"));
        if (chunkSize->IsNumber()) topicOpt.chunkSize = size_t(chunkSize->NumberValue());
        if (chunksToKeep->IsNumber()) topicOpt.chunksToKeep = size_t(chunksToKeep->NumberValue());

        ProducerWrap* ptr = new ProducerWrap(*path, *topicName, &topicOpt, bgFlush->BooleanValue());
        ptr->Wrap(info.This());
        info.GetReturnValue().Set(info.This());
    }

private:
    template<DataType DT> NAN_METHOD(push) {
        BatchWrap<DT> batch;
        batch.reserve(info.Length());
        for (int i = 0; i < info.Length(); i++) {
            batch.push(info[i]);
        }

        _handle.push(batch.get());
    }

    template<DataType DT> NAN_METHOD(push2Cache) {
        BatchWrap<DT> batch;
        batch.reserve(info.Length());
        for (int i = 0; i < info.Length(); i++) {
            batch.push(info[i]);
        }

        _handle.push2Cache(batch.get());
    }

private:
    ProducerWrap(const char* path, const char* name, TopicOpt* opt, bool bgFlush) : _handle(path, name, opt) {
        if (bgFlush) _handle.enableBackgroundFlush(chrono::milliseconds(200));
    }

    Producer _handle;
};

const char * const ProducerWrap::CLASS_NAME = "Producer";

template<DataType DT> class ReturnMaker {
public:
    Local<Value> static make(const Consumer::ItemType& item);
};

template<> Local<Value> ReturnMaker<STRING_TYPE>::make(const Consumer::ItemType& item) {
    return Nan::New(std::get<1>(item)).ToLocalChecked();
}

template<> Local<Value> ReturnMaker<BUFFER_TYPE>::make(const Consumer::ItemType& item) {
    return Nan::CopyBuffer(std::get<1>(item), std::get<2>(item)).ToLocalChecked();
}

class ConsumerWrap : public nnu::ClassWrap<ConsumerWrap> {
public:
    static const char * const CLASS_NAME; // "Consumer"

    static void setupMember(v8::Local<v8::FunctionTemplate>& tpl) {
        Nan::SetPrototypeMethod(tpl, "offset", wrapFunction<&ConsumerWrap::offset>);
        Nan::SetPrototypeMethod(tpl, "popString", wrapFunction<&ConsumerWrap::pop<STRING_TYPE> >);
        Nan::SetPrototypeMethod(tpl, "popBuffer", wrapFunction<&ConsumerWrap::pop<BUFFER_TYPE> >);
    }

    static NAN_METHOD(ctor) {
        Local<Object> opt = info[0]->ToObject();

        Nan::Utf8String path(opt->Get(nnu::newString("path")));
        Nan::Utf8String topicName(opt->Get(nnu::newString("topic")));
        Nan::Utf8String name(opt->Get(nnu::newString("name")));

        TopicOpt topicOpt{ 1024 * 1024 * 1024, 0 };
        Local<Value> chunkSize = opt->Get(nnu::newString("chunkSize"));
        if (chunkSize->IsNumber()) topicOpt.chunkSize = size_t(chunkSize->NumberValue());

        ConsumerWrap* ptr = new ConsumerWrap(*path, *topicName, *name, &topicOpt);
        Local<Value> batchSize = opt->Get(nnu::newString("batchSize"));
        if (batchSize->IsNumber()) {
           size_t bs = size_t(batchSize->NumberValue());
           if (bs > 0 && bs < 1024 * 1024) ptr->_batchSize = bs;
        }

        ptr->Wrap(info.This());
        info.GetReturnValue().Set(info.This());
    }

private:
    NAN_METHOD(offset) {
        if (_cur <= _batch.size()) {
            Local<Value> ret = Nan::New(double(std::get<0>(_batch.at(_cur - 1))));
            info.GetReturnValue().Set(ret);
        }
    }

    template<DataType DT> NAN_METHOD(pop) {
        if (_cur < _batch.size()) {
            info.GetReturnValue().Set(ReturnMaker<DT>::make(_batch.at(_cur++)));
            return ;
        }

        _batch.clear();
        _cur = 1;
        _handle.pop(_batch, _batchSize);
        if (_batch.size() > 0) {
            info.GetReturnValue().Set(ReturnMaker<DT>::make(_batch.at(0)));
            return ;
        }
    }

private:
    ConsumerWrap(const char* path, const char* topicName, const char* name, TopicOpt* opt) : _handle(path, topicName, name, opt), _cur(0), _batchSize(128) { }

    Consumer _handle;
    Consumer::BatchType _batch;
    size_t _cur, _batchSize;
};

const char * const ConsumerWrap::CLASS_NAME = "Consumer";

class TopicWrap : public nnu::ClassWrap<TopicWrap> {
public:
    static const char * const CLASS_NAME; // "Topic"

    static void setupMember(v8::Local<v8::FunctionTemplate>& tpl) {
        Nan::SetPrototypeMethod(tpl, "status", wrapFunction<&TopicWrap::status>);
    }

    static NAN_METHOD(ctor) {
        Handle<Object> opt = info[0]->ToObject();

        Nan::Utf8String path(opt->Get(nnu::newString("path")));
        Nan::Utf8String topicName(opt->Get(nnu::newString("topic")));

        TopicWrap *ptr = new TopicWrap(*path, *topicName);

        ptr->Wrap(info.This());
        info.GetReturnValue().Set(info.This());
    }

private:
    NAN_METHOD(status) {
        TopicStatus st = _handle->status();

        Local<Object> ret = Nan::New<Object>();
        ret->Set(nnu::newString("producerHead"), Nan::New<Number>(double(st.producerHead)));

        Local<Object> consumerHeads = Nan::New<Object>();
        for (auto it : st.consumerHeads) {
            consumerHeads->Set(Nan::New(it.first).ToLocalChecked(), Nan::New<Number>(double(it.second)));
        }
        ret->Set(nnu::newString("consumerHeads"), consumerHeads);

        info.GetReturnValue().Set(ret);
    }

private:
    TopicWrap(const char* path, const char* topicName) : _handle(EnvManager::getEnv(path)->getTopic(topicName)) {

    }

private:
    Topic *_handle;
};

const char * const TopicWrap::CLASS_NAME = "Topic";

NAN_MODULE_INIT(InitAll) {
    target->Set(nnu::newString("STRING_TYPE"), Nan::New(STRING_TYPE));
    target->Set(nnu::newString("BUFFER_TYPE"), Nan::New(BUFFER_TYPE));

    TopicWrap::setup(target);
    ProducerWrap::setup(target);
    ConsumerWrap::setup(target);
}

NODE_MODULE(lmdb_queue, InitAll);