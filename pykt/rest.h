#ifndef REST_H
#define REST_H

#include "pykt.h"
#include "db.h"
#include "http.h"

typedef enum{
    MODE_SET = 0,
    MODE_ADD,
    MODE_REPLACE,
} kt_mode;

PyObject* 
rest_call_get(DBObject *db, PyObject *keyObj);

PyObject* 
rest_call_head(DBObject *db, PyObject *keyObj);

PyObject* 
rest_call_put(DBObject *db, PyObject *key, PyObject *value, int expire, kt_mode mode);

PyObject* 
rest_call_delete(DBObject *db, PyObject *keyObj);

#endif
