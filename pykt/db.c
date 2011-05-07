#include "db.h"
#include "rpc.h"
#include "rest.h"
#include "cursor.h"

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 1978
#define DEFAULT_TIMEOUT 30

inline int
is_opened(DBObject *self)
{
    if(self->con){
        return 1;
    }
    PyErr_SetString(PyExc_IOError, "not opend database");
    return 0;
}

static inline PyObject *
DBObject_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    DBObject *self;

    self = (DBObject *)type->tp_alloc(type, 0);
    if(self == NULL){
        return NULL;
    }
    
    DEBUG("DBObject_new self %p", self);
    self->dbObj = NULL;
    self->con = NULL;
    return (PyObject *)self;
}

static inline int
DBObject_init(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *temp = NULL;

    static char *kwlist[] = {"db", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O:__init__", kwlist, &temp)){
        return -1; 
    }
    if(temp){
        if(!PyString_Check(temp)){
            PyErr_SetString(PyExc_TypeError, "database identifier must be a string");
            return -1;
        }
        self->dbObj = temp;
        Py_INCREF(self->dbObj);
    }
    return 0;
}

static inline void
DBObject_dealloc(DBObject *self)
{
    DEBUG("DBObject_dealloc self %p", self);
    if(self->con){
        close_http_connection(self->con);
        self->con = NULL;
    }
    if(self->dbObj){
        Py_DECREF(self->dbObj);
        self->dbObj = NULL;
    }
    self->ob_type->tp_free((PyObject*)self);
}

static inline PyObject* 
DBObject_open(DBObject *self, PyObject *args, PyObject *kwargs)
{
    char *host = NULL;
    int port = 0;
    int timeout = 1;
    http_connection *con;

    static char *kwlist[] = {"host", "port", "timeout", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|siii", kwlist, &host, &port, &timeout)){
        return NULL; 
    }
    
    DEBUG("DBObject_open self %p", self);
    DEBUG("DBObject_open args %s, %d", host, port);
    
    if(!host){
        host = DEFAULT_HOST;
    }
    if(port == 0){
        port = DEFAULT_PORT;
    }

    con = open_http_connection(host, port, timeout);
    if(con == NULL){
        return NULL;
    }
    //self->host = host;
    //self->port = port;
    //self->timeout = timeout;
    self->con = con;
	Py_INCREF(self);
    return (PyObject *)self;
}

static inline PyObject* 
DBObject_close(DBObject *self, PyObject *args)
{
    PyObject *result;
    result = Py_False;

    DEBUG("DBObject_close self %p", self);
    if(self->con){
        if(close_http_connection(self->con) > 0){
            self->con = NULL;
            result = Py_True;
        }
    }
    Py_INCREF(result);
    return result;

}

static inline PyObject* 
DBObject_echo(DBObject *self, PyObject *args)
{

    DEBUG("DBObject_echo self %p", self);
    if(!is_opened(self)){
        return NULL;
    }
    return rpc_call_echo(self);
}

static inline PyObject* 
DBObject_report(DBObject *self, PyObject *args)
{

    DEBUG("DBObject_report self %p", self);
    if(!is_opened(self)){
        return NULL;
    }
    return rpc_call_report(self);
}

static inline PyObject* 
DBObject_play_script(DBObject *self, PyObject *args, PyObject *kwargs)
{
    char *name = NULL;
    Py_ssize_t name_len;
    PyObject *records = NULL;

    static char *kwlist[] = {"name", "records", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#|O", kwlist, &name, &name_len,  &records)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_play_script self %p", self);
    return rpc_call_play_script(self, name, name_len, records);
}

static inline PyObject* 
DBObject_set(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *value;
    int expire = 0;

    static char *kwlist[] = {"key", "value", "expire", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|i", kwlist, &key, &value, &expire)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_set self %p", self);
    return rest_call_put(self, key, value, expire, MODE_SET);
}

static inline PyObject* 
DBObject_get(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key;

    static char *kwlist[] = {"key", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &key)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_get self %p", self);
    return rest_call_get(self, key);
}

static inline PyObject* 
DBObject_head(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key;

    static char *kwlist[] = {"key", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &key)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_head self %p", self);
    return rest_call_head(self, key);
}


static inline PyObject* 
DBObject_remove(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key;

    static char *kwlist[] = {"key", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &key)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_remove self %p", self);
    return rest_call_delete(self, key);
}


static inline PyObject* 
DBObject_status(DBObject *self, PyObject *args)
{

    DEBUG("DBObject_status self %p", self);
    if(!is_opened(self)){
        return NULL;
    }
    return rpc_call_status(self);
}

static inline PyObject* 
DBObject_clear(DBObject *self, PyObject *args)
{

    DEBUG("DBObject_clear self %p", self);
    if(!is_opened(self)){
        return NULL;
    }
    return rpc_call_clear(self);
}

static inline PyObject* 
DBObject_synchronize(DBObject *self, PyObject *args, PyObject *kwargs)
{
    char *command = NULL;
    Py_ssize_t command_len = 0;
    int hard = 0;

    static char *kwlist[] = {"hard", "command", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|is#", kwlist, &hard, &command, &command_len)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_synchronize self %p", self);
    return rpc_call_sync(self, hard, command, command_len);

}

static inline PyObject* 
DBObject_add(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *value;
    int expire = 0;

    static char *kwlist[] = {"key", "value", "expire", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|i", kwlist, &key, &value, &expire)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_add self %p", self);
    return rest_call_put(self, key, value, expire, MODE_ADD);
    /*
    PyObject *key, *value, *db_name = NULL;
    int expire = 0;

    DEBUG("DBObject_add self %p", self);
    static char *kwlist[] = {"key", "value", "xt", "db", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|iO", kwlist, &key, &value, &expire, &db_name)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    return rpc_call_add(self, key, value, db_name, expire);
    */
}

static inline PyObject* 
DBObject_replace(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *value;
    int expire = 0;

    static char *kwlist[] = {"key", "value", "expire", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|i", kwlist, &key, &value, &expire)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_replace self %p", self);
    return rest_call_put(self, key, value, expire, MODE_REPLACE);
    /*
    PyObject *key, *value, *db_name = NULL;
    int expire = 0;

    DEBUG("DBObject_replace self %p", self);
    static char *kwlist[] = {"key", "value", "xt", "db", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|iO", kwlist, &key, &value, &expire, &db_name)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    return rpc_call_replace(self, key, value, db_name, expire);
    */
}

static inline PyObject* 
DBObject_append(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *value;
    int expire = 0;

    DEBUG("DBObject_append self %p", self);
    static char *kwlist[] = {"key", "value", "expire", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|i", kwlist, &key, &value, &expire)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    return rpc_call_append(self, key, value, expire);
}

static inline PyObject* 
DBObject_increment(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key;
    int num = 1, expire = 0;

    static char *kwlist[] = {"key", "num", "expire", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|ii", kwlist, &key, &num, &expire)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_increment self %p", self);
    return rpc_call_increment(self, key, num, expire);
}

static inline PyObject* 
DBObject_increment_double(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key;
    int expire = 0;
    double num = 1.0;

    static char *kwlist[] = {"key", "num", "expire", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|di", kwlist, &key, &num, &expire)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_increment_double self %p", self);
    return rpc_call_increment_double(self, key, num, expire);
}

static inline PyObject* 
DBObject_cas(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *oval = NULL, *nval = NULL;
    int expire = 0;

    static char *kwlist[] = {"key", "oval", "nval", "expire",  NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOi", kwlist, &key, &oval, &nval, &expire)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_cas self %p", self);
    return rpc_call_cas(self, key, oval, nval, expire);
}

static inline PyObject* 
DBObject_set_bulk(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *records;
    int expire = 0, atomic = 0;

    static char *kwlist[] = {"records", "expire", "atomic", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|ii", kwlist, &records, &expire, &atomic)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_set_bulk self %p", self);
    return rpc_call_set_bulk(self, records, expire, atomic);
}

static inline PyObject* 
DBObject_remove_bulk(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *keys;
    int atomic = 0;

    static char *kwlist[] = {"keys", "atomic", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|i", kwlist, &keys, &atomic)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_remove_bulk self %p", self);
    return rpc_call_remove_bulk(self, keys, atomic);
}

static inline PyObject* 
DBObject_get_bulk(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *keys;
    int atomic = 0;

    static char *kwlist[] = {"keys", "atomic", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|i", kwlist, &keys, &atomic)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_get_bulk self %p", self);
    return rpc_call_get_bulk(self, keys, atomic);
}

static inline PyObject* 
DBObject_get_vacuum(DBObject *self, PyObject *args, PyObject *kwargs)
{
    int step = 0;

    static char *kwlist[] = {"step", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i", kwlist, &step)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_vacuum self %p", self);
    return rpc_call_vacuum(self, step);
}

static inline PyObject* 
DBObject_match_prefix(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *prefixObj;

    static char *kwlist[] = {"prefix", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &prefixObj)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_match_prefix self %p", self);
    return rpc_call_match_prefix(self, prefixObj);
}

static inline PyObject* 
DBObject_match_regex(DBObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *regexObj;

    static char *kwlist[] = {"regex", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &regexObj)){
        return NULL; 
    }
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_match_regex self %p", self);
    return rpc_call_match_regex(self, regexObj);
}

static inline PyObject* 
DBObject_cursor(DBObject *self, PyObject *args)
{
    PyObject *factory;

    if(!is_opened(self)){
        return NULL;
    }

    factory = (PyObject*)&CursorObjectType;

    return PyObject_CallFunction(factory, "O", self);
    
}

static PyMethodDef DBObject_methods[] = {
    {"open", (PyCFunction)DBObject_open, METH_VARARGS|METH_KEYWORDS, 0},
    {"close", (PyCFunction)DBObject_close, METH_NOARGS, 0},
    {"get", (PyCFunction)DBObject_get, METH_VARARGS|METH_KEYWORDS, 0},
    {"head", (PyCFunction)DBObject_head, METH_VARARGS|METH_KEYWORDS, 0},
    {"set", (PyCFunction)DBObject_set, METH_VARARGS|METH_KEYWORDS, 0},
    {"remove", (PyCFunction)DBObject_remove, METH_VARARGS|METH_KEYWORDS, 0},
    {"echo", (PyCFunction)DBObject_echo, METH_NOARGS, 0},
    {"report", (PyCFunction)DBObject_report, METH_NOARGS, 0},
    {"play_script", (PyCFunction)DBObject_play_script, METH_VARARGS|METH_KEYWORDS, 0},
    {"status", (PyCFunction)DBObject_status, METH_NOARGS, 0},
    {"clear", (PyCFunction)DBObject_clear, METH_NOARGS, 0},
    {"synchronize", (PyCFunction)DBObject_synchronize, METH_VARARGS|METH_KEYWORDS, 0},
    {"add", (PyCFunction)DBObject_add, METH_VARARGS|METH_KEYWORDS, 0},
    {"replace", (PyCFunction)DBObject_replace, METH_VARARGS|METH_KEYWORDS, 0},
    {"append", (PyCFunction)DBObject_append, METH_VARARGS|METH_KEYWORDS, 0},
    {"increment", (PyCFunction)DBObject_increment, METH_VARARGS|METH_KEYWORDS, 0},
    {"increment_double", (PyCFunction)DBObject_increment_double, METH_VARARGS|METH_KEYWORDS, 0},
    {"cas", (PyCFunction)DBObject_cas, METH_VARARGS|METH_KEYWORDS, 0},
    {"set_bulk", (PyCFunction)DBObject_set_bulk, METH_VARARGS|METH_KEYWORDS, 0},
    {"remove_bulk", (PyCFunction)DBObject_remove_bulk, METH_VARARGS|METH_KEYWORDS, 0},
    {"get_bulk", (PyCFunction)DBObject_get_bulk, METH_VARARGS|METH_KEYWORDS, 0},
    {"match_prefix", (PyCFunction)DBObject_match_prefix, METH_VARARGS|METH_KEYWORDS, 0},
    {"match_regex", (PyCFunction)DBObject_match_regex, METH_VARARGS|METH_KEYWORDS, 0},
    {"cursor", (PyCFunction)DBObject_cursor, METH_NOARGS, 0},
    {NULL, NULL}
};

static inline Py_ssize_t
DBObject_dict_length(DBObject *self)
{
    PyObject *result = NULL;
    Py_ssize_t len = -1;

    if(!is_opened(self)){
        return len;
    }

    result = rpc_call_status(self);
    if(result){
        PyObject *temp = PyDict_GetItemString(result, "count");
        if(temp){
            PyObject *i = PyNumber_Int(temp);
            len = PyNumber_AsSsize_t(i, NULL);
            Py_DECREF(i);
        }
        Py_DECREF(result);    
    }

    return len;
}

static inline PyObject *
DBObject_dict_subscript(DBObject *self, PyObject *key)
{
    if(!is_opened(self)){
        return NULL;
    }
    
    DEBUG("DBObject_dict_subscript self %p", self);
    return rest_call_get(self, key);
}

static inline int
DBObject_dict_ass_sub(DBObject *self, PyObject *key, PyObject *value)
{
    PyObject *result;

    if(!is_opened(self)){
        return -1;
    }
    
    DEBUG("DBObject_dict_ass_sub self %p", self);
    if (value == NULL){
        //DELETE
        result = rest_call_delete(self, key);
        if(result == NULL){
            return -1;
        }
        if(PyObject_IsTrue(result) < 1){
            PyErr_SetString(KtException, "no record");
            return -1;
        }
        Py_DECREF(result);
    }else{
        result = rest_call_put(self, key, value, 0, MODE_SET);
        if(result == NULL){
            return -1;
        }
        if(PyObject_IsTrue(result) < 1){
            PyErr_SetString(KtException, "do no add record");
            return -1;
        }
        Py_DECREF(result);
    }
    return 0;
}

static PyMappingMethods db_as_mapping = {
    (lenfunc)DBObject_dict_length, /*mp_length*/
    (binaryfunc)DBObject_dict_subscript, /*mp_subscript*/
    (objobjargproc)DBObject_dict_ass_sub, /*mp_ass_subscript*/
};

static inline PyObject *
DBObject_getter_db(DBObject *self, void *closure)
{
    if(!self->dbObj){
        Py_RETURN_NONE;
    }
    Py_INCREF(self->dbObj);
    return self->dbObj;
}

static int
DBObject_setter_db(DBObject *self, PyObject *value, void *closure)
{
    if (value == NULL) {
        //DELETE
        if(self->dbObj){
            Py_DECREF(self->dbObj);
            self->dbObj = NULL;
            return 0;
        }
        return -1;
    }
        
    if (!PyString_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "database identifier must be a string");
        return -1;
    }
    Py_XDECREF(self->dbObj);
    Py_INCREF(value);
    self->dbObj = value;
    return 0;
}

static PyGetSetDef DBObject_getseters[] = {
    {"db",(getter)DBObject_getter_db, (setter)DBObject_setter_db, "db identifier name"},
    {NULL}  /* Sentinel */
};

PyTypeObject DBObjectType = {
	PyObject_HEAD_INIT(&PyType_Type)
    0,
    "pykt.KyotoTycoon",             /*tp_name*/
    sizeof(DBObject), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)DBObject_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    &db_as_mapping,            /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,        /*tp_flags*/
    "KyotoTycoonObject",           /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,		               /* tp_weaklistoffset */
    0,		               /* tp_iter */
    0,		               /* tp_iternext */
    DBObject_methods,          /* tp_methods */
    0,                         /* tp_members */
    DBObject_getseters,        /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)DBObject_init,                      /* tp_init */
    0,                         /* tp_alloc */
    DBObject_new,                           /* tp_new */
};

