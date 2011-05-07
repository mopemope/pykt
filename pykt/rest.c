#include "rest.h"
#include "request.h"
#include "util.h"

static inline int
init_bucket(http_connection *con, int size)
{
    data_bucket *bucket;

    bucket = create_data_bucket(size);
    if(bucket == NULL){
        return -1;
    }
    con->bucket = bucket;
    return 1;
}

inline PyObject* 
rest_call_get(DBObject *db, PyObject *keyObj)
{
    http_connection *con;
    char *key, *encbuf;
    Py_ssize_t key_len;
    size_t encbuf_len;
    PyObject *result = NULL;

    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }

    con = db->con;
    if(init_bucket(con, 16) < 0){
        return NULL;
    }

    PyString_AsStringAndSize(keyObj, &key, &key_len);
    urlencode(key, key_len, &encbuf, &encbuf_len);

    set_request_path(con, METHOD_GET, LEN(METHOD_GET), encbuf, encbuf_len);
    end_header(con);
    
    if(request(con, 200) > 0){
        PyObject *temp;
        temp = getPyString(con->response_body);
        result = deserialize_value(temp);
        Py_DECREF(temp);
        //DEBUG("response body %s", getString(con->response_body));
    }else{
        if(con->status_code == 404){
            PyErr_Clear();
            result = Py_None;
            Py_INCREF(result);
        }
    }
    
    free_http_data(con);
    PyMem_Free(encbuf);

    return result;
}

inline PyObject* 
rest_call_head(DBObject *db, PyObject *keyObj)
{
    http_connection *con;
    char *key, *encbuf;
    Py_ssize_t key_len;
    size_t encbuf_len;
    PyObject *result = NULL;

    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }

    con = db->con;
    if(init_bucket(con, 16) < 0){
        return NULL;
    }

    PyString_AsStringAndSize(keyObj, &key, &key_len);
    urlencode(key, key_len, &encbuf, &encbuf_len);

    set_request_path(con, METHOD_HEAD, LEN(METHOD_HEAD), encbuf, encbuf_len);
    end_header(con);
    
    //head request
    con->head = 1;
    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->status_code == 404){
            PyErr_Clear();
            result = Py_False;
            Py_INCREF(result);
        }
    }
    
    free_http_data(con);
    PyMem_Free(encbuf);
    
    return result;
}

inline PyObject* 
rest_call_put(DBObject *db, PyObject *keyObj, PyObject *valueObj, int expire, kt_mode mode)
{
    http_connection *con;
    char content_length[10];
    char xt[14];
    uint64_t expire_time = 0;

    char *key, *val, *encbuf;
    Py_ssize_t key_len, val_len;
    size_t encbuf_len;
    PyObject *result = NULL, *temp_val;
    

    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }
    if(valueObj == Py_None){
        PyErr_SetString(PyExc_TypeError, "value is None");
        return NULL;
    }

    con = db->con;
    if(init_bucket(con, 16) < 0){
        return NULL;
    }

    temp_val = serialize_value(valueObj);
    
    PyString_AsStringAndSize(keyObj, &key, &key_len);
    PyString_AsStringAndSize(temp_val, &val, &val_len);

    urlencode(key, key_len, &encbuf, &encbuf_len);
    //DEBUG("urlencode key %s", encbuf);

    set_request_path(con, METHOD_PUT, LEN(METHOD_PUT), encbuf, encbuf_len);
    
    //get content-length str
    snprintf(content_length, sizeof (content_length), "%d", val_len);
    add_content_length(con, content_length, strlen(content_length));
    
    switch(mode){
        case MODE_ADD:
            add_header_oneline(con, X_KT_MODE_ADD, LEN(X_KT_MODE_ADD));
            break;
        case MODE_REPLACE:
            add_header_oneline(con, X_KT_MODE_REPLACE, LEN(X_KT_MODE_REPLACE));
            break;
        default:
            add_header_oneline(con, X_KT_MODE_SET, LEN(X_KT_MODE_SET));
    }
    if(expire > 0){
        expire_time = get_expire_time(expire);
        //set X-Kt-Kt
        snprintf(xt, sizeof (xt), "%llu", expire_time);
        //DEBUG("expire %s", xt);
        add_kt_xt(con, xt, strlen(xt));
    }
    
    end_header(con);
    add_body(con, val, val_len);
    
    if(request(con, 201) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            result = Py_False;;
            Py_INCREF(result);
        }else if(con->response_status == RES_KT_ERROR){
            switch(mode){
                case MODE_ADD:
                case MODE_REPLACE:
                    break;
                default:
                    PyErr_Clear();
                    result = Py_False;;
                    Py_INCREF(result);
            }
        }
    }
    
    free_http_data(con);
    PyMem_Free(encbuf);
    Py_DECREF(temp_val);
    return result;
}

inline PyObject* 
rest_call_delete(DBObject *db, PyObject *keyObj)
{
    http_connection *con;
    char *key, *encbuf;
    Py_ssize_t key_len;
    size_t encbuf_len;
    PyObject *result = NULL;

    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }

    con = db->con;
    if(init_bucket(con, 16) < 0){
        return NULL;
    }

    PyString_AsStringAndSize(keyObj, &key, &key_len);
    urlencode(key, key_len, &encbuf, &encbuf_len);

    set_request_path(con, METHOD_DELETE, LEN(METHOD_DELETE), encbuf, encbuf_len);
    end_header(con);
    
    if(request(con, 204) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->status_code == 404){
            PyErr_Clear();
            result = Py_False;
            Py_INCREF(result);
        }
    }
    
    free_http_data(con);
    PyMem_Free(encbuf);

    return result;
}

