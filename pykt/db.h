#ifndef DB_H
#define DB_H

#include "pykt.h"
#include "http.h"


typedef struct {
    PyObject_HEAD
    int non_blocking;
    http_connection *con;
    PyObject *dbObj;
} DBObject;

extern PyTypeObject DBObjectType;

inline int
is_opened(DBObject *self);

#endif
