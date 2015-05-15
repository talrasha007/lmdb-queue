#pragma once

#include <iostream>
#include <lmdb/lmdb.h>

class MDBCursor {
public:
    MDBCursor(MDB_dbi db, MDB_txn *txn) : _cursor(nullptr) {
        /* init list not supported in vs 2013 */
        _key.mv_size = _val.mv_size = 0;
        _key.mv_data = _val.mv_data = nullptr;

        int rc = mdb_cursor_open(txn, db, &_cursor);
        if (rc != 0) {
            std::cout << "Cursor open fail: " << mdb_strerror(rc) << std::endl;
        }
    }

    ~MDBCursor() {
        if (_cursor) mdb_cursor_close(_cursor);
    }

public:
    template<class INT> INT key() { return *(INT*)_key.mv_data; }
    template<class INT> INT val() { return *(INT*)_val.mv_data; }
    const MDB_val& key() { return _key; }
    const MDB_val& val() { return _val; }
    int gotoFirst() { return mdb_cursor_get(_cursor, &_key, &_val, MDB_FIRST); }
    int gotoLast() { return mdb_cursor_get(_cursor, &_key, &_val, MDB_LAST); }
    int next() { return mdb_cursor_get(_cursor, &_key, &_val, MDB_NEXT); }
    int del() { return mdb_cursor_del(_cursor, 0); }

    int seek(const MDB_val& k) {
        _key = k;
        return mdb_cursor_get(_cursor, &_key, &_val, MDB_SET);
    }

    int seek(uint32_t k) { return getInt(k, MDB_SET); }
    int seek(uint64_t k) { return getInt(k, MDB_SET); }

    int gte(const MDB_val& k) {
        _key = k;
        return mdb_cursor_get(_cursor, &_key, &_val, MDB_SET_RANGE);
    }

    int gte(uint32_t k) { return getInt(k, MDB_SET_RANGE); }
    int gte(uint64_t k) { return getInt(k, MDB_SET_RANGE); }

private:
    template<typename INT> int getInt(INT intkey, MDB_cursor_op op) {
        _key.mv_size = sizeof(intkey);
        _key.mv_data = &intkey;
        return mdb_cursor_get(_cursor, &_key, &_val, op);
    }

private:
    MDB_cursor *_cursor;
    MDB_val _key, _val;
};
