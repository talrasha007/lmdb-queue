#include <nan.h>

#include "producer.h"

using namespace std;
using namespace v8;
using namespace node;

enum DataType {
    STRING_TYPE = 1,
    BUFFER_TYPE
};

template<DataType DT> class BatchWrap {
public:
    ~BatchWrap();
    void push(Local<Value>& val);

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

template<> BatchWrap<STRING_TYPE>::~BatchWrap()  {
    for (Producer::BatchType::iterator it = _batch.begin(); it != _batch.end(); ++it) delete[] std::get<0>(*it);
}

template<> void BatchWrap<STRING_TYPE>::push(Local<Value>& val) {
    v8::Local<v8::String> toStr = val->ToString();
    size_t size = toStr->Utf8Length();
    char* buf = new char[size + 1];
    toStr->WriteUtf8(buf);
    _batch.push_back(make_tuple(buf, size));
}

template<> BatchWrap<BUFFER_TYPE>::~BatchWrap() {
}

template<> void BatchWrap<BUFFER_TYPE>::push(Local<Value>& val) {
    _batch.push_back(make_tuple(node::Buffer::Data(val), node::Buffer::Length(val)));
}

class ProducerWrap : public ObjectWrap {
public:
    static void setup(Handle<Object>& exports) {
        const char* className = "Producer";

        Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(ProducerWrap::ctor);
        tpl->SetClassName(NanNew(className));
        tpl->InstanceTemplate()->SetInternalFieldCount(1);

        NODE_SET_PROTOTYPE_METHOD(tpl, "stats", ProducerWrap::stats);
        NODE_SET_PROTOTYPE_METHOD(tpl, "pushString", ProducerWrap::push<STRING_TYPE>);
        NODE_SET_PROTOTYPE_METHOD(tpl, "pushBuffer", ProducerWrap::push<BUFFER_TYPE>);

        exports->Set(NanNew(className), tpl->GetFunction());
    }

private:
    static NAN_METHOD(ctor) {
        NanScope();

        NanUtf8String path(args[0]);

        ProducerWrap* ptr = new ProducerWrap(*path);
        ptr->Wrap(args.This());
        NanReturnValue(args.This());
    }

    static NAN_METHOD(stats) {
        NanScope();

        ProducerWrap* ptr = ObjectWrap::Unwrap<ProducerWrap>(args.This());
        ptr->_handle.stats();

        NanReturnUndefined();
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

private:
    ProducerWrap(const char* path) : _handle(path) { }
    Producer _handle;
};

void init(v8::Handle<v8::Object> exports) {
    exports->Set(NanNew("STRING_TYPE"), NanNew(STRING_TYPE));
    exports->Set(NanNew("BUFFER_TYPE"), NanNew(BUFFER_TYPE));

    ProducerWrap::setup(exports);
}

NODE_MODULE(lmdb_queue, init);