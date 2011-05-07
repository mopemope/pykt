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

inline PyObject* 
rest_call_get(DBObject *db, PyObject *keyObj);

inline PyObject* 
rest_call_head(DBObject *db, PyObject *keyObj);

inline PyObject* 
rest_call_put(DBObject *db, PyObject *key, PyObject *value, int expire, kt_mode mode);

inline PyObject* 
rest_call_delete(DBObject *db, PyObject *keyObj);

#endif
