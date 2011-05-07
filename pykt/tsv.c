#include "tsv.h"
#include "util.h"


inline void 
on_record_keylist(void *data, const char *key, size_t key_len, const char *val, size_t val_len)
{
    PyObject *keyObj, *list;
    
    tsv_ctx *ctx = (tsv_ctx *)data;
    list = (PyObject *)ctx->user;
    if(strncmp("_", key, 1) == 0){
        //DEBUG("key %.*s", key_len, key);
        char temp[key_len-1];
        size_t temp_len = key_len -1;

        memcpy(temp, key+1, temp_len);
        int len = urldecode(temp, temp_len);
        keyObj = PyString_FromStringAndSize(temp, len);
        if(keyObj == NULL){
            ctx->error = 1;
            PyErr_NoMemory();
            return ;
        }
        if(PyList_Append(list, keyObj) < 0){
            ctx->error = 1;
        }
        Py_DECREF(keyObj);
    }
}

static inline void 
on_record_value(void *data, const char *key, size_t key_len, const char *val, size_t val_len)
{
    PyObject *keyObj, *valueObj, *dict;
    

    tsv_ctx *ctx = (tsv_ctx *)data;
    dict = (PyObject *)ctx->user;
    if(strncmp("_", key, 1) == 0){
        char tempkey[key_len-1];
        size_t tempkey_len = key_len -1;
        char tempval[val_len];
        size_t tempval_len = val_len;

        memcpy(tempkey, key+1, tempkey_len);
        int len1 = urldecode(tempkey, tempkey_len);
        keyObj = PyString_FromStringAndSize(tempkey, len1);

        //DEBUG("on_record keyObj %p ", keyObj);
        if(keyObj == NULL){
            ctx->error = 1;
            PyErr_NoMemory();
            return ;
        }
        
        if(key == val || val_len == 0){
            valueObj = PyString_FromString("");
        }else{
            memcpy(tempval, val, tempval_len);
            int len2 = urldecode(tempval, tempval_len);
            valueObj = PyString_FromStringAndSize(tempval, len2); 
        }

        //DEBUG("on_record valueObj %p ", valueObj);
        if(valueObj == NULL){
            ctx->error = 1;
            PyErr_NoMemory();
            Py_DECREF(keyObj);
            return ;
        }
        if(PyDict_SetItem(dict, keyObj, valueObj) < 0){
            ctx->error = 1;
            Py_DECREF(keyObj);
            Py_DECREF(valueObj);
            return;
        }
        Py_DECREF(keyObj);
        Py_DECREF(valueObj);
    }
}

static inline void 
on_record(void *data, const char *key, size_t key_len, const char *val, size_t val_len)
{
    PyObject *keyObj = NULL, *valueObj = NULL, *dict = NULL;
    
    //DEBUG("on_record %.*s : %.*s ", key_len, key, val_len, val);

    tsv_ctx *ctx = (tsv_ctx *)data;
    dict = (PyObject *)ctx->user;
    keyObj = PyString_FromStringAndSize(key, key_len); 
    //DEBUG("on_record keyObj %p ", keyObj);
    if(keyObj == NULL){
        ctx->error = 1;
        PyErr_NoMemory();
        return ;
    }
    
    if(key == val || val_len == 0){
        valueObj = PyString_FromString("");
    }else{
        valueObj = PyString_FromStringAndSize(val, val_len); 
    }

    //DEBUG("on_record valueObj %p ", valueObj);
    if(valueObj == NULL){
        ctx->error = 1;
        PyErr_NoMemory();
        Py_DECREF(keyObj);
        return ;
    }
    if(PyDict_SetItem(dict, keyObj, valueObj) < 0){
        ctx->error = 1;
        Py_DECREF(keyObj);
        Py_DECREF(valueObj);
        return;
    }
    Py_DECREF(keyObj);
    Py_DECREF(valueObj);
}

inline PyObject * 
convert2dict(buffer *buf)
{
    tsv_ctx *ctx;
    PyObject *dict;
    size_t nread, off = 0;

    ctx = (tsv_ctx *)PyMem_Malloc(sizeof(tsv_ctx));
    if(ctx == NULL){
        PyErr_NoMemory();
        return NULL;
    }
    dict = PyDict_New();
    if(dict == NULL){
        PyMem_Free(ctx);
        PyErr_NoMemory();
        return NULL;
    }
    memset(ctx, 0, sizeof(tsv_ctx));
    tsv_init(ctx);
    ctx->on_record = on_record;
    ctx->user = dict;
    nread = tsv_execute(ctx, buf->buf, buf->len, off);
    
    if(ctx->error){
        //TODO Error
    }

    PyMem_Free(ctx);
    return dict;
}

inline PyObject * 
convert2valuedict(buffer *buf)
{
    tsv_ctx *ctx;
    PyObject *dict;
    size_t nread, off = 0;

    ctx = (tsv_ctx *)PyMem_Malloc(sizeof(tsv_ctx));
    if(ctx == NULL){
        PyErr_NoMemory();
        return NULL;
    }
    dict = PyDict_New();
    if(dict == NULL){
        PyMem_Free(ctx);
        PyErr_NoMemory();
        return NULL;
    }
    memset(ctx, 0, sizeof(tsv_ctx));
    tsv_init(ctx);
    ctx->on_record = on_record_value;
    ctx->user = dict;
    nread = tsv_execute(ctx, buf->buf, buf->len, off);
    
    if(ctx->error){
        //TODO Error
    }

    PyMem_Free(ctx);
    return dict;
}

inline PyObject * 
convert2keylist(buffer *buf)
{
    tsv_ctx *ctx;
    PyObject *list;
    size_t nread, off = 0;

    ctx = (tsv_ctx *)PyMem_Malloc(sizeof(tsv_ctx));
    if(ctx == NULL){
        PyErr_NoMemory();
        return NULL;
    }
    
    list = PyList_New(0);
    if(list == NULL){
        PyMem_Free(ctx);
        PyErr_NoMemory();
        return NULL;
    }
    memset(ctx, 0, sizeof(tsv_ctx));
    tsv_init(ctx);
    ctx->on_record = on_record_keylist;
    ctx->user = list;
    nread = tsv_execute(ctx, buf->buf, buf->len, off);
    
    if(ctx->error){
        //TODO Error
    }

    PyMem_Free(ctx);
    return list;
}

