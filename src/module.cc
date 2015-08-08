#include <nan.h>

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

class ProducerWrap : public ObjectWrap {
public:
    static void setup(Handle<Object>& exports) {
        const char* className = "Producer";

        Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(ProducerWrap::ctor);
        tpl->SetClassName(NanNew(className));
        tpl->InstanceTemplate()->SetInternalFieldCount(1);

        NODE_SET_PROTOTYPE_METHOD(tpl, "pushString", ProducerWrap::push<STRING_TYPE>);
        NODE_SET_PROTOTYPE_METHOD(tpl, "pushBuffer", ProducerWrap::push<BUFFER_TYPE>);

        NODE_SET_PROTOTYPE_METHOD(tpl, "pushString2Cache", ProducerWrap::push2Cache<STRING_TYPE>);
        NODE_SET_PROTOTYPE_METHOD(tpl, "pushBuffer2Cache", ProducerWrap::push2Cache<BUFFER_TYPE>);

        exports->Set(NanNew(className), tpl->GetFunction());
    }

private:
    static NAN_METHOD(ctor) {
        NanScope();

        Handle<Object> opt = args[0]->ToObject();

        NanUtf8String path(opt->Get(NanNew("path")));
        NanUtf8String topicName(opt->Get(NanNew("topic")));

        TopicOpt topicOpt{ 1024 * 1024 * 1024, 8 };
        Local<Value> chunkSize = opt->Get(NanNew("chunkSize"));
        Local<Value> chunksToKeep = opt->Get(NanNew("chunksToKeep"));
        Local<Value> bgFlush = opt->Get(NanNew("backgroundFlush"));
        if (chunkSize->IsNumber()) topicOpt.chunkSize = size_t(chunkSize->NumberValue());
        if (chunksToKeep->IsNumber()) topicOpt.chunksToKeep = size_t(chunksToKeep->NumberValue());

        ProducerWrap* ptr = new ProducerWrap(*path, *topicName, &topicOpt, bgFlush->BooleanValue());
        ptr->Wrap(args.This());
        NanReturnValue(args.This());
    }

    template<DataType DT> static NAN_METHOD(push) {
        NanScope();

        ProducerWrap* ptr = ObjectWrap::Unwrap<ProducerWrap>(args.This());

        BatchWrap<DT> batch;
        batch.reserve(args.Length());
        for (int i = 0; i < args.Length(); i++) {
            batch.push(args[i]);
        }

        ptr->_handle.push(batch.get());

        NanReturnUndefined();
    }

    template<DataType DT> static NAN_METHOD(push2Cache) {
        NanScope();

        ProducerWrap* ptr = ObjectWrap::Unwrap<ProducerWrap>(args.This());

        BatchWrap<DT> batch;
        batch.reserve(args.Length());
        for (int i = 0; i < args.Length(); i++) {
            batch.push(args[i]);
        }

        ptr->_handle.push2Cache(batch.get());

        NanReturnUndefined();
    }

private:
    ProducerWrap(const char* path, const char* name, TopicOpt* opt, bool bgFlush) : _handle(path, name, opt) {
        if (bgFlush) _handle.enableBackgroundFlush(chrono::milliseconds(200));
    }

    Producer _handle;
};

template<DataType DT> class ReturnMaker {
public:
    Local<Value> static make(const Consumer::ItemType& item);
};

template<> Local<Value> ReturnMaker<STRING_TYPE>::make(const Consumer::ItemType& item) {
    return NanNew(std::get<1>(item));
}

template<> Local<Value> ReturnMaker<BUFFER_TYPE>::make(const Consumer::ItemType& item) {
    return NanNewBufferHandle(std::get<1>(item), std::get<2>(item));
}

class ConsumerWrap : public ObjectWrap {
public:
    static void setup(Handle<Object>& exports) {
        const char* className = "Consumer";

        Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(ConsumerWrap::ctor);
        tpl->SetClassName(NanNew(className));
        tpl->InstanceTemplate()->SetInternalFieldCount(1);

        NODE_SET_PROTOTYPE_METHOD(tpl, "offset", ConsumerWrap::offset);
        NODE_SET_PROTOTYPE_METHOD(tpl, "popString", ConsumerWrap::pop<STRING_TYPE>);
        NODE_SET_PROTOTYPE_METHOD(tpl, "popBuffer", ConsumerWrap::pop<BUFFER_TYPE>);

        exports->Set(NanNew(className), tpl->GetFunction());
    }

private:
    static NAN_METHOD(ctor) {
        NanScope();

        Handle<Object> opt = args[0]->ToObject();

        NanUtf8String path(opt->Get(NanNew("path")));
        NanUtf8String topicName(opt->Get(NanNew("topic")));
        NanUtf8String name(opt->Get(NanNew("name")));

        TopicOpt topicOpt{ 1024 * 1024 * 1024, 0 };
        Local<Value> chunkSize = opt->Get(NanNew("chunkSize"));
        if (chunkSize->IsNumber()) topicOpt.chunkSize = size_t(chunkSize->NumberValue());

        ConsumerWrap* ptr = new ConsumerWrap(*path, *topicName, *name, &topicOpt);
        Local<Value> batchSize = opt->Get(NanNew("batchSize"));
        if (batchSize->IsNumber()) {
           size_t bs = size_t(batchSize->NumberValue());
           if (bs > 0 && bs < 1024 * 1024) ptr->_batchSize = bs;
        }

        ptr->Wrap(args.This());
        NanReturnValue(args.This());
    }

    static NAN_METHOD(offset) {
        NanScope();

        ConsumerWrap* ptr = ObjectWrap::Unwrap<ConsumerWrap>(args.This());

        if (ptr->_cur <= ptr->_batch.size()) {
            NanReturnValue(NanNew(double(std::get<0>(ptr->_batch.at(ptr->_cur - 1)))));
        }

        NanReturnUndefined();
    }

    template<DataType DT> static NAN_METHOD(pop) {
        NanScope();

        ConsumerWrap* ptr = ObjectWrap::Unwrap<ConsumerWrap>(args.This());

        if (ptr->_cur < ptr->_batch.size()) {
            NanReturnValue(ReturnMaker<DT>::make(ptr->_batch.at(ptr->_cur++)));
        }

        ptr->_batch.clear();
        ptr->_cur = 1;
        ptr->_handle.pop(ptr->_batch, ptr->_batchSize);
        if (ptr->_batch.size() > 0) {
            NanReturnValue(ReturnMaker<DT>::make(ptr->_batch.at(0)));
        }

        NanReturnUndefined();
    }

private:
    ConsumerWrap(const char* path, const char* topicName, const char* name, TopicOpt* opt) : _handle(path, topicName, name, opt), _cur(0), _batchSize(128) { }

    Consumer _handle;
    Consumer::BatchType _batch;
    size_t _cur, _batchSize;
};

class TopicWrap : public ObjectWrap {
public:
    static void setup(Handle<Object>& exports) {
        const char* className = "Topic";

        Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(TopicWrap::ctor);
        tpl->SetClassName(NanNew(className));
        tpl->InstanceTemplate()->SetInternalFieldCount(1);

        NODE_SET_PROTOTYPE_METHOD(tpl, "status", TopicWrap::status);

        exports->Set(NanNew(className), tpl->GetFunction());
    }

private:
    static NAN_METHOD(ctor) {
        NanScope();

        Handle<Object> opt = args[0]->ToObject();

        NanUtf8String path(opt->Get(NanNew("path")));
        NanUtf8String topicName(opt->Get(NanNew("topic")));

        TopicWrap *ptr = new TopicWrap(*path, *topicName);

        ptr->Wrap(args.This());
        NanReturnValue(args.This());
    }

    static NAN_METHOD(status) {
        NanScope();

        TopicWrap* ptr = ObjectWrap::Unwrap<TopicWrap>(args.This());
        TopicStatus st = ptr->_handle->status();

        Local<Object> ret = NanNew<Object>();
        ret->Set(NanNew("producerHead"), NanNew<Number>(double(st.producerHead)));

        Local<Object> consumerHeads = NanNew<Object>();
        Handle<Object> info = NanNew<Object>();
        for (auto it : st.consumerHeads) {
            info->Set(NanNew<String>("head"), NanNew<Number>(double(it.second.head)));
            info->Set(NanNew<String>("byte"), NanNew<Number>(double(it.second.byte)));
            consumerHeads->Set(NanNew(it.first), info);
        }
        ret->Set(NanNew("consumerHeads"), consumerHeads);

        NanReturnValue(ret);
    }

private:
    TopicWrap(const char* path, const char* topicName) : _handle(EnvManager::getEnv(path)->getTopic(topicName)) {

    }

private:
    Topic *_handle;
};

void init(v8::Handle<v8::Object> exports) {
    exports->Set(NanNew("STRING_TYPE"), NanNew(STRING_TYPE));
    exports->Set(NanNew("BUFFER_TYPE"), NanNew(BUFFER_TYPE));

    TopicWrap::setup(exports);
    ProducerWrap::setup(exports);
    ConsumerWrap::setup(exports);
}

NODE_MODULE(lmdb_queue, init);
