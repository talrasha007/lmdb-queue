#include <nan.h>

#include "producer.h"

using namespace std;
using namespace v8;
using namespace node;

class BatchWrap {
public:
    ~BatchWrap() {
        for (Producer::BatchType::iterator it = _batch.begin(); it != _batch.end(); ++it) delete[] std::get<0>(*it);
    }

    Producer::BatchType& get() {
        return _batch;
    }

    void reserve(size_t sz) {
        _batch.reserve(sz);
    }

    void pushString(Local<Value>& val) {
        v8::Local<v8::String> toStr = val->ToString();
        size_t size = toStr->Utf8Length();
        char* buf = new char[size + 1];
        toStr->WriteUtf8(buf);
        _batch.push_back(make_tuple(buf, size));
    }

    void pushBuffer(Local<Value>& val) {
        _batch.push_back(make_tuple(node::Buffer::Data(val), node::Buffer::Length(val)));
    }

private:
    Producer::BatchType _batch;
};

class ProducerWrap : public ObjectWrap {
public:
    static void setup(Handle<Object>& exports) {
        const char* className = "Producer";

        Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(ProducerWrap::ctor);
        tpl->SetClassName(NanNew(className));
        tpl->InstanceTemplate()->SetInternalFieldCount(1);

        NODE_SET_PROTOTYPE_METHOD(tpl, "pushString", ProducerWrap::pushString);
        NODE_SET_PROTOTYPE_METHOD(tpl, "pushBuffer", ProducerWrap::pushBuffer);

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

    static NAN_METHOD(pushString) {
        NanScope();

        ProducerWrap* ptr = ObjectWrap::Unwrap<ProducerWrap>(args.This());

        BatchWrap batch;
        batch.reserve(args.Length());
        for (int i = 0; i < args.Length(); i++) {
            batch.pushString(args[i]);
        }

        ptr->_handle.push(batch.get());

        NanReturnUndefined();
    }

    static NAN_METHOD(pushBuffer) {
        NanScope();

        ProducerWrap* ptr = ObjectWrap::Unwrap<ProducerWrap>(args.This());

        BatchWrap batch;
        batch.reserve(args.Length());
        for (int i = 0; i < args.Length(); i++) {
            batch.pushBuffer(args[i]);
        }

        ptr->_handle.push(batch.get());

        NanReturnUndefined();
    }

private:
    ProducerWrap(const char* path) : _handle(path) { }
    Producer _handle;
};

void init(v8::Handle<v8::Object> exports) {
}

NODE_MODULE(lmdb_queue, init);