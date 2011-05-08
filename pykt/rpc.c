#include "rpc.h"
#include "request.h"
#include "util.h"
#include "tsv.h"
#include "buffer.h"

#define BUF_SIZE 1024 * 8
#define LBUF_SIZE 1024 * 512

#define ECHO_URL "/rpc/echo"
#define REPORT_URL "/rpc/report"
#define PLAY_SCRIPT_URL "/rpc/play_script"
#define STATUS_URL "/rpc/status"
#define CLEAR_URL "/rpc/clear"
#define SYNC_URL "/rpc/synchronize"
#define SET_URL "/rpc/set"
#define ADD_URL "/rpc/add"
#define REPLACE_URL "/rpc/replace"
#define APPEND_URL "/rpc/append"
#define INCREMENT_URL "/rpc/increment"
#define INCREMENT_DOUBLE_URL "/rpc/increment_double"
#define CAS_URL "/rpc/cas"
#define SET_BULK_URL "/rpc/set_bulk"
#define REMOVE_BULK_URL "/rpc/remove_bulk"
#define GET_BULK_URL "/rpc/get_bulk"
#define VACUUM_URL "/rpc/vacuum"
#define MATCH_PREFIX_URL "/rpc/match_prefix"
#define MATCH_REGEX_URL "/rpc/match_regex"

#define CUR_JUMP_URL "/rpc/cur_jump"
#define CUR_JUMP_BACK_URL "/rpc/cur_jump_back"
#define CUR_STEP_URL "/rpc/cur_step"
#define CUR_STEP_BACK_URL "/rpc/cur_step_back"
#define CUR_SET_VALUE_URL "/rpc/cur_set_value"
#define CUR_REMOVE_URL "/rpc/cur_remove"
#define CUR_GET_KEY_URL "/rpc/cur_get_key"
#define CUR_GET_VALUE_URL "/rpc/cur_get_value"
#define CUR_GET_URL "/rpc/cur_get"
#define CUR_DELETE_URL "/rpc/cur_delete"


static inline int
set_error(http_connection *con)
{
    PyObject *dict, *temp;
    char *msg;
    int ret = -1;

    dict = convert2dict(con->response_body);
    if(dict){
        temp = PyDict_GetItemString(dict, "ERROR");
        if(temp){
            msg = PyString_AsString(temp);
            if(msg){
                PyErr_SetString(KtException, msg);
                ret = 1;
            }else{
                //TypeError
                ret = -1;
            }

        }
        Py_DECREF(dict);
    }else{
        PyErr_SetString(KtException, "could not set error ");
        ret = -1;
    }
    return ret;
}

static inline PyObject*
get_num(http_connection *con, uint8_t isdouble)
{
    PyObject *dict, *temp, *result = NULL;
    dict = convert2dict(con->response_body);
    if(dict){
        temp = PyDict_GetItemString(dict, "num");
        if(temp){
            if(isdouble){
                result = PyNumber_Float(temp);
            }else{
                result = PyNumber_Int(temp);
            }
        }
        Py_DECREF(dict);
    }
    return result;

}

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

static inline int 
set_param_raw(buffer *body, char *key, size_t key_len, char *val, size_t val_len)
{
    char *enc;
    size_t enc_len;
    urlencode(val, val_len, &enc, &enc_len);
    //TODO write error?
    write2buf(body, key, key_len);
    write2buf(body, enc, enc_len);
    PyMem_Free(enc);
    return 1;
}

static inline int 
set_param_db(buffer *body, PyObject *dbObj)
{
    char *db_name;
    Py_ssize_t db_name_len;

    PyString_AsStringAndSize(dbObj, &db_name, &db_name_len);
    set_param_raw(body, "DB\t", 3, db_name, db_name_len);
    return 1;
}

static inline int 
set_param_key(buffer *body, PyObject *keyObj)
{
    char *key;
    Py_ssize_t key_len;

    PyString_AsStringAndSize(keyObj, &key, &key_len);
    set_param_raw(body, "key\t", 4, key, key_len);
    return 1;
}

static inline int 
set_param(buffer *body, char *key, size_t key_len, PyObject *valueObj)
{
    char *val;
    Py_ssize_t val_len;
    
    PyString_AsStringAndSize(valueObj, &val, &val_len);
    set_param_raw(body, key, key_len, val, val_len);
    return 1;
}


static inline int 
set_param_value(buffer *body, char *key, size_t key_len, PyObject *valueObj)
{
    char *val;
    Py_ssize_t val_len;
    PyObject *temp_val;
    
    temp_val = serialize_value(valueObj);
    PyString_AsStringAndSize(temp_val, &val, &val_len);
    set_param_raw(body, key, key_len, val, val_len);
    return 1;
}

static inline int
set_param_xt(buffer *body, int expire)
{
    char xt[16];
    size_t xt_len = 0;
    snprintf(xt, sizeof (xt), "%d", expire);
    xt_len = strlen(xt);
    write2buf(body, "xt\t", 3);
    write2buf(body, xt, xt_len);
    return 1;
}

static inline int
set_param_num_int(buffer *body, int num)
{
    char addnum[32];
    size_t addnum_len;

    //num -> strnum
    snprintf(addnum, sizeof (addnum), "%d", num);
    addnum_len = strlen(addnum);
    write2buf(body, "num\t", 4);
    write2buf(body, addnum, addnum_len);
    return 1;
}

static inline int
set_param_num_double(buffer *body, double num)
{
    char addnum[32];
    size_t addnum_len;

    //num -> strnum
    snprintf(addnum, sizeof (addnum), "%f", num);
    addnum_len = strlen(addnum);
    write2buf(body, "num\t", 4);
    write2buf(body, addnum, addnum_len);
    return 1;
}

static inline int
set_param_cur(buffer *body, int cur)
{
    char c[32];
    size_t c_len = 0;

    snprintf(c, sizeof(c), "%d", cur);
    c_len = strlen(c);
    write2buf(body, "CUR\t", 4);
    write2buf(body, c, c_len);
    return 1;
}

static inline void
write_bulk_records(PyObject *dict, buffer *buf)
{
    PyObject *keyObj, *valueObj;
    Py_ssize_t size = 0, index = 1, pos = 0;

    size = PyDict_Size(dict);

    while (PyDict_Next(dict, &pos, &keyObj, &valueObj)) {
        char *key, *enckey, *val, *encval;
        Py_ssize_t key_len, val_len;
        size_t enckey_len, encval_len;
        PyObject *key_str = PyObject_Str(keyObj);
        PyObject *value_str = serialize_value(valueObj);
        
        //encode
        PyString_AsStringAndSize(key_str, &key, &key_len);
        urlencode(key, key_len, &enckey, &enckey_len);

        PyString_AsStringAndSize(value_str, &val, &val_len);
        urlencode(val, val_len, &encval, &encval_len);
        
        write2buf(buf, "_", 1);
        write2buf(buf, enckey, enckey_len);
        write2buf(buf, "\t", 1);
        write2buf(buf, encval, encval_len);
        if(index < size){
            write2buf(buf, CRLF, 2);
        }
        PyMem_Free(enckey);
        PyMem_Free(encval);

        Py_XDECREF(key_str);
        Py_XDECREF(value_str);
        index++;
    }
}

static inline void
write_bulk_keys(PyObject *list, buffer *buf)
{
    int i = 0;
    Py_ssize_t size = 0;

    size = PyList_GET_SIZE(list);

    for(i = 0; i < size; i++){
        char *key, *enckey;
        Py_ssize_t key_len ;
        size_t enckey_len;
        PyObject *keyObj = PyList_GET_ITEM(list, i);
        PyObject *key_str = PyObject_Str(keyObj);
        //encode
        PyString_AsStringAndSize(key_str, &key, &key_len);
        urlencode(key, key_len, &enckey, &enckey_len);
        write2buf(buf, "_", 1);
        write2buf(buf, enckey, enckey_len);
        write2buf(buf, "\t", 1);
        if(i < size -1){
            write2buf(buf, CRLF, 2);
        }
        PyMem_Free(enckey);

        Py_XDECREF(key_str);

    }
}

/**
static inline int
get_recods_size(PyObject *dict)
{
    PyObject *keyObj, *valueObj;
    Py_ssize_t pos = 0;

    while (PyDict_Next(dict, &pos, &keyObj, &valueObj)) {
        char *key, *enckey;
        Py_ssize_t key_len;
        size_t enckey_len;
        PyObject *key_str = PyObject_Str(keyObj);
        PyObject *value_str = PyObject_Str(valueObj);
        
        //encode
        PyString_AsStringAndSize(key_str, &key, &key_len);
        urlencode(key, key_len, &enckey, &enckey_len);
        PyMem_Free(enckey);

        Py_XDECREF(key_str);
        Py_XDECREF(value_str);
    }
    return 1;
}
*/

inline PyObject* 
rpc_call_echo(DBObject *db)
{
    http_connection *con;
    PyObject *result = NULL;

    con = db->con;
    
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    set_request_path(con, METHOD_POST, LEN(METHOD_POST), ECHO_URL, LEN(ECHO_URL));
    end_header(con);
    
    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            result = Py_False;;
            Py_INCREF(result);
        }
    }
    
    free_http_data(con);

    return result;

}

inline PyObject* 
rpc_call_report(DBObject *db)
{

    http_connection *con;
    PyObject *result = NULL;

    con = db->con;
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    set_request_path(con, METHOD_POST, LEN(METHOD_POST), REPORT_URL, LEN(REPORT_URL));
    end_header(con);
    
    if(request(con, 200) > 0){
        result = convert2dict(con->response_body);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_status(DBObject *db)
{

    http_connection *con;
    PyObject *result = NULL;
    char content_length[12];
    buffer *body;
    
    PyObject *dbObj = db->dbObj;

    con = db->con;
    body = new_buffer(128, 0);
    if(body == NULL){
        return NULL;
    }
    
    if(init_bucket(con, 16) < 0){
        return NULL;
    }

    if(dbObj){
        set_param_db(body, dbObj);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), STATUS_URL, LEN(STATUS_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    end_header(con);
    if(body->len > 0){
        add_body(con, body->buf, body->len);
    }
    
    if(request(con, 200) > 0){
        result = convert2dict(con->response_body);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_clear(DBObject *db)
{

    http_connection *con;
    PyObject *result = NULL;
    char content_length[12];
    buffer *body;
    
    PyObject *dbObj = db->dbObj;

    body = new_buffer(128, 0);
    if(body == NULL){
        return NULL;
    }

    con = db->con;
    if(init_bucket(con, 16) < 0){
        return NULL;
    }

    if(dbObj){
        set_param_db(body, dbObj);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CLEAR_URL, LEN(CLEAR_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    end_header(con);
    if(body->len > 0){
        add_body(con, body->buf, body->len);
    }
    
    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_sync(DBObject *db, int hard, char *command, Py_ssize_t command_len)
{
    http_connection *con;
    PyObject *result = NULL;
    char content_length[12];
    buffer *body;
    
    PyObject *dbObj = db->dbObj;

    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }

    con = db->con;
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    uint8_t exists = 0;

    if(dbObj){
        set_param_db(body, dbObj);
        exists = 1;
    }
    if(hard > 0){
        if(exists){
            write2buf(body, CRLF, 2);
        }
        write2buf(body, "hard\ttrue", 9);
        exists = 1;
    }
    if(command > 0){
        if(exists){
            write2buf(body, CRLF, 2);
        }
        set_param_raw(body, "command\t", 8, command, command_len); 
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), SYNC_URL, LEN(SYNC_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    end_header(con);
    if(body->len > 0){
        add_body(con, body->buf, body->len);
    }
    
    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;

}

static inline PyObject* 
add_internal(DBObject *db, char *url, size_t url_len, PyObject *keyObj, PyObject *valueObj, int expire)
{
    http_connection *con;
    char *key, *val, *enckey, *encval, *db_name = NULL;
    Py_ssize_t key_len, val_len;
    char content_length[12];
    char xt[14];
    size_t enckey_len, encval_len, db_name_len = 0, xt_len = 0;
    uint32_t body_len = 12;
    PyObject *result = NULL, *temp_val;
    
    PyObject *dbObj = db->dbObj;

    //DEBUG("rpc_call_add %p %p %d", keyObj, valueObj, expire);
    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }
    if(valueObj == Py_None){
        PyErr_SetString(PyExc_TypeError, "value is None");
        return NULL;
    }

    con = db->con;
    if(init_bucket(con, 24) < 0){
        return NULL;
    }
    
    temp_val = serialize_value(valueObj);
    
    PyString_AsStringAndSize(keyObj, &key, &key_len);
    PyString_AsStringAndSize(temp_val, &val, &val_len);
    
    urlencode(key, key_len, &enckey, &enckey_len);
    urlencode(val, val_len, &encval, &encval_len);
    body_len += enckey_len;
    body_len += encval_len;

    if(dbObj){
        char *temp;
        Py_ssize_t temp_len;
        PyString_AsStringAndSize(dbObj, &temp, &temp_len);
        urlencode(temp, temp_len, &db_name, &db_name_len);
        body_len += db_name_len;
        body_len += 5;
    }
    if(expire > 0){
        snprintf(xt, sizeof(xt), "%d", expire);
        xt_len = strlen(xt);
        body_len += xt_len;
        body_len += 5;
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), url, url_len);
    snprintf(content_length, sizeof (content_length), "%d", body_len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    if(db_name_len > 0){
        add_body(con, "DB\t", 3);
        add_body(con, db_name, db_name_len);
        add_body(con, CRLF, 2);
    }

    add_body(con, "key\t", 4);
    add_body(con, enckey, enckey_len);
    add_body(con, CRLF, 2);
    add_body(con, "value\t", 6);
    add_body(con, encval, encval_len);

    if(xt_len > 0){
        add_body(con, CRLF, 2);
        add_body(con, "xt\t", 3);
        add_body(con, xt, xt_len);
    }

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_http_data(con);
    PyMem_Free(enckey);
    PyMem_Free(encval);
    if(db_name_len > 0){
        PyMem_Free(db_name);
    }
    Py_DECREF(temp_val);

    return result;
}

/*
static inline PyObject* 
add_internal(DBObject *db, char *url, size_t url_len, PyObject *keyObj, PyObject *valueObj, PyObject *dbObj, int expire)
{
    http_connection *con;
    char content_length[12];
    buffer *body;
    PyObject *result = NULL;

    DEBUG("rpc_call_add %p %p %p %d", keyObj, valueObj, dbObj, expire);
    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }
    if(dbObj && !PyString_Check(dbObj)){
        PyErr_SetString(PyExc_TypeError, "db must be string ");
        return NULL;
    }
    if(valueObj == Py_None){
        PyErr_SetString(PyExc_TypeError, "value is None");
        return NULL;
    }

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    
    if(dbObj && dbObj != Py_None){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param_key(body, keyObj);
    write2buf(body, CRLF, 2);
    set_param(body, "value\t", 6, valueObj);
    if(expire > 0){
        write2buf(body, CRLF, 2);
        set_param_xt(body, expire); 
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), url, url_len);
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}
*/

inline PyObject* 
rpc_call_set(DBObject *db, PyObject *keyObj, PyObject *valueObj, int expire)
{
    return add_internal(db, SET_URL, LEN(SET_URL), keyObj, valueObj, expire);
}

inline PyObject* 
rpc_call_add(DBObject *db, PyObject *keyObj, PyObject *valueObj, int expire)
{
    return add_internal(db, ADD_URL, LEN(ADD_URL), keyObj, valueObj, expire);
}

inline PyObject* 
rpc_call_replace(DBObject *db, PyObject *keyObj, PyObject *valueObj, int expire)
{
    return add_internal(db, REPLACE_URL, LEN(REPLACE_URL), keyObj, valueObj, expire);
}

inline PyObject* 
rpc_call_append(DBObject *db, PyObject *keyObj, PyObject *valueObj, int expire)
{
    return add_internal(db, APPEND_URL, LEN(APPEND_URL), keyObj, valueObj, expire);
}

inline PyObject* 
rpc_call_increment(DBObject *db, PyObject *keyObj, int num, int expire)
{

    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;

    PyObject *dbObj = db->dbObj;
    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    
    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param_key(body, keyObj);
    write2buf(body, CRLF, 2);
    set_param_num_int(body, num);
    if(expire > 0){
        write2buf(body, CRLF, 2);
        set_param_xt(body, expire);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), INCREMENT_URL, LEN(INCREMENT_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = get_num(con, 0);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);
    return result;
}

inline PyObject* 
rpc_call_increment_double(DBObject *db, PyObject *keyObj, double num, int expire)
{
    http_connection *con;
    char content_length[12];
    buffer *body;
    PyObject *result = NULL;

    PyObject *dbObj = db->dbObj;
    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    
    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param_key(body, keyObj);
    write2buf(body, CRLF, 2);
    set_param_num_double(body, num);
    if(expire > 0){
        write2buf(body, CRLF, 2);
        set_param_xt(body, expire);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), INCREMENT_DOUBLE_URL, LEN(INCREMENT_DOUBLE_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = get_num(con, 1);
    }else{
        if(con->status_code == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_cas(DBObject *db, PyObject *keyObj, PyObject *ovalObj, PyObject *nvalObj, int expire)
{
    http_connection *con;
    char *key, *oval = NULL, *nval = NULL, *encbuf, *db_name = NULL;
    Py_ssize_t key_len;
    char content_length[12];
    char xt[14];
    size_t encbuf_len, oval_len = 0, nval_len = 0, xt_len = 0, db_name_len = 0;
    uint32_t body_len = 4;
    PyObject *result = NULL, *ovalS = NULL, *nvalS = NULL;

    PyObject *dbObj = db->dbObj;
    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }

    con = db->con;
    if(init_bucket(con, 24) < 0){
        return NULL;
    }
    
    if(dbObj){
        char *temp;
        Py_ssize_t temp_len;
        PyString_AsStringAndSize(dbObj, &temp, &temp_len);
        urlencode(temp, temp_len, &db_name, &db_name_len);
        body_len += db_name_len;
        body_len += 5;
    }
    
    if(ovalObj && ovalObj != Py_None){
        char *temp1;
        Py_ssize_t temp1_len;
        ovalS = serialize_value(ovalObj);
        PyString_AsStringAndSize(ovalS, &temp1, &temp1_len);
        urlencode(temp1, temp1_len, &oval, &oval_len);
        body_len += oval_len;
        body_len += 7;
    }

    if(nvalObj && nvalObj != Py_None){
        char *temp2;
        Py_ssize_t temp2_len;
        nvalS = serialize_value(nvalObj);
        PyString_AsStringAndSize(nvalS, &temp2, &temp2_len);
        urlencode(temp2, temp2_len, &nval, &nval_len);
        body_len += nval_len;
        body_len += 7;
    }
    
    PyString_AsStringAndSize(keyObj, &key, &key_len);
    
    urlencode(key, key_len, &encbuf, &encbuf_len);
    body_len += encbuf_len;

    if(expire > 0){
        snprintf(xt, sizeof (xt), "%d", expire);
        xt_len = strlen(xt);
        body_len += xt_len;
        body_len += 5;
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CAS_URL, LEN(CAS_URL));
    snprintf(content_length, sizeof (content_length), "%d", body_len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    if(db_name_len > 0){
        add_body(con, "DB\t", 3);
        add_body(con, db_name, db_name_len);
        add_body(con, CRLF, 2);
    }

    add_body(con, "key\t", 4);
    add_body(con, encbuf, encbuf_len);
    if(oval_len > 0){
        add_body(con, CRLF, 2);
        add_body(con, "oval\t", 5);
        add_body(con, oval, oval_len);
    }
    if(nval_len > 0){
        add_body(con, CRLF, 2);
        add_body(con, "nval\t", 5);
        add_body(con, nval, nval_len);
    }
    if(xt_len > 0){
        add_body(con, CRLF, 2);
        add_body(con, "xt\t", 3);
        add_body(con, xt, xt_len);
    }

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_http_data(con);
    PyMem_Free(encbuf);
    if(db_name_len > 0){
        PyMem_Free(db_name);
    }
    if(oval){
        PyMem_Free(oval);
    }
    if(nval){
        PyMem_Free(nval);
    }
    Py_XDECREF(ovalS);
    Py_XDECREF(nvalS);

    return result;

}

/* slow impl ...
inline PyObject* 
rpc_call_cas(DBObject *db, PyObject *keyObj, PyObject *dbObj, PyObject *ovalObj, PyObject *nvalObj, int expire)
{
    http_connection *con;
    char content_length[12];
    buffer *body;
    PyObject *result = NULL;

    if(!PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }
    if(dbObj && !PyString_Check(dbObj)){
        PyErr_SetString(PyExc_TypeError, "db must be string ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    
    if(dbObj && dbObj != Py_None){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param_key(body, keyObj);

    if(ovalObj && ovalObj != Py_None){
        write2buf(body, CRLF, 2);
        set_param(body, "oval\t", 5,  ovalObj);
    }

    if(nvalObj && nvalObj != Py_None){
        write2buf(body, CRLF, 2);
        set_param(body, "nval\t", 5, nvalObj);
    }

    if(expire > 0){
        write2buf(body, CRLF, 2);
        set_param_xt(body, expire);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CAS_URL, LEN(CAS_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_http_data(con);

    return result;

}*/

inline PyObject* 
rpc_call_match_prefix(DBObject *db, PyObject *prefixObj)
{
    http_connection *con;
    char content_length[12];
    buffer *body;
    PyObject *result = NULL;

    PyObject *dbObj = db->dbObj;
    if(prefixObj && !PyString_Check(prefixObj)){
        PyErr_SetString(PyExc_TypeError, "prefix must be string ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    
    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param(body, "prefix\t", 7, prefixObj);

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), MATCH_PREFIX_URL, LEN(MATCH_PREFIX_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = convert2keylist(con->response_body);
        //result = convert2dict(con->response_body);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_match_regex(DBObject *db, PyObject *regexObj)
{
    http_connection *con;
    char content_length[12];
    buffer *body;
    PyObject *result = NULL;

    PyObject *dbObj = db->dbObj;
    if(regexObj && !PyString_Check(regexObj)){
        PyErr_SetString(PyExc_TypeError, "regex must be string ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    
    if(dbObj && dbObj != Py_None){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param(body, "regex\t", 6, regexObj);

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), MATCH_REGEX_URL, LEN(MATCH_REGEX_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = convert2keylist(con->response_body);
        //result = convert2dict(con->response_body);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}


inline PyObject* 
rpc_call_set_bulk(DBObject *db, PyObject *recordObj, int expire, int atomic)
{

    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;

    PyObject *dbObj = db->dbObj;
    
    if(recordObj && !PyDict_Check(recordObj)){
        PyErr_SetString(PyExc_TypeError, "record must be dict ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(LBUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    if(expire > 0){
        set_param_xt(body, expire);
        write2buf(body, CRLF, 2);
    }
    if(atomic){
        write2buf(body, "atomic\t", 7);
        write2buf(body, "true", 4);
        write2buf(body, CRLF, 2);
    }
    write_bulk_records(recordObj, body);

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), SET_BULK_URL, LEN(SET_BULK_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = get_num(con, 0);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_remove_bulk(DBObject *db, PyObject *keysObj, int atomic)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;
    PyObject *dbObj = db->dbObj;
    
    if(keysObj && !PyList_Check(keysObj)){
        PyErr_SetString(PyExc_TypeError, "keys must be dict ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(LBUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    if(atomic){
        write2buf(body, "atomic\t", 7);
        write2buf(body, "true", 4);
        write2buf(body, CRLF, 2);
    }
    write_bulk_keys(keysObj, body);

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), REMOVE_BULK_URL, LEN(REMOVE_BULK_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = get_num(con, 0);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_get_bulk(DBObject *db, PyObject *keysObj, int atomic)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;
    PyObject *dbObj = db->dbObj;
    
    if(keysObj && !PyList_Check(keysObj)){
        PyErr_SetString(PyExc_TypeError, "keys must be dict ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(LBUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    if(atomic){
        write2buf(body, "atomic\t", 7);
        write2buf(body, "true", 4);
        write2buf(body, CRLF, 2);
    }
    write_bulk_keys(keysObj, body);

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), GET_BULK_URL, LEN(GET_BULK_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = convert2valuedict(con->response_body);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_vacuum(DBObject *db, int step)
{
    http_connection *con;
    PyObject *result = NULL;
    char content_length[12];
    buffer *body;
    
    PyObject *dbObj = db->dbObj;

    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }

    con = db->con;
    if(init_bucket(con, 16) < 0){
        return NULL;
    }
    uint8_t exists = 0;

    if(dbObj){
        set_param_db(body, dbObj);
        exists = 1;
    }
    if(step > 0){
        if(exists){
            write2buf(body, CRLF, 2);
        }
        char stepnum[16];
        size_t stepnum_len;

        snprintf(stepnum, sizeof(stepnum), "%d", step);
        stepnum_len = strlen(stepnum);
        set_param_raw(body, "step\t", 5, stepnum, stepnum_len);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), VACUUM_URL, LEN(VACUUM_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    end_header(con);
    if(body->len > 0){
        add_body(con, body->buf, body->len);
    }
    
    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;

}

inline PyObject* 
rpc_call_play_script(DBObject *db, char *name, Py_ssize_t name_len,  PyObject *recordObj)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;

    
    if(recordObj && !PyDict_Check(recordObj)){
        PyErr_SetString(PyExc_TypeError, "record must be dict ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(LBUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    set_param_raw(body, "name\t", 5, name, name_len);
    if(recordObj){
        write2buf(body, CRLF, 2);
        write_bulk_records(recordObj, body);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), PLAY_SCRIPT_URL, LEN(PLAY_SCRIPT_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = convert2valuedict(con->response_body);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;

}

inline PyObject* 
rpc_call_cur_jump(DBObject *db, int cur, PyObject *keyObj)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;
    
    PyObject *dbObj = db->dbObj;
    if(keyObj && !PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param_cur(body, cur);
    if(keyObj){
        write2buf(body, CRLF, 2);
        set_param_key(body, keyObj);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_JUMP_URL, LEN(CUR_JUMP_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_cur_jump_back(DBObject *db, int cur, PyObject *keyObj)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;
    
    PyObject *dbObj = db->dbObj;
    if(keyObj && !PyString_Check(keyObj)){
        PyErr_SetString(PyExc_TypeError, "key must be string ");
        return NULL;
    }

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param_cur(body, cur);
    if(keyObj){
        write2buf(body, CRLF, 2);
        set_param_key(body, keyObj);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_JUMP_BACK_URL, LEN(CUR_JUMP_BACK_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_cur_step(DBObject *db, int cur)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;
    
    PyObject *dbObj = db->dbObj;

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param_cur(body, cur);

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_STEP_URL, LEN(CUR_STEP_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_cur_step_back(DBObject *db, int cur)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;
    
    PyObject *dbObj = db->dbObj;

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    if(dbObj){
        set_param_db(body, dbObj);
        write2buf(body, CRLF, 2);
    }
    set_param_cur(body, cur);

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_STEP_BACK_URL, LEN(CUR_STEP_BACK_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_cur_set_value(DBObject *db, int cur, PyObject *valueObj, int step, int expire)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    set_param_cur(body, cur);
    write2buf(body, CRLF, 2);
    set_param_value(body, "value\t", 6, valueObj);
    if(step > 0){
        write2buf(body, CRLF, 2);
        write2buf(body, "step\t", 5); 
    }
    if(expire){
        write2buf(body, CRLF, 2);
        set_param_xt(body, expire);
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_SET_VALUE_URL, LEN(CUR_SET_VALUE_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;

}


inline PyObject* 
rpc_call_cur_remove(DBObject *db, int cur)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    set_param_cur(body, cur);

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_REMOVE_URL, LEN(CUR_REMOVE_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_cur_get_key(DBObject *db, int cur, int step)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    set_param_cur(body, cur);
    if(step > 0){
        write2buf(body, CRLF, 2);
        write2buf(body, "step\t", 5); 
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_GET_KEY_URL, LEN(CUR_GET_KEY_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        PyObject *temp = convert2dict(con->response_body);
        result = PyDict_GetItemString(temp, "key");
        Py_INCREF(result);
        Py_DECREF(temp);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_cur_get_value(DBObject *db, int cur, int step)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    set_param_cur(body, cur);
    if(step > 0){
        write2buf(body, CRLF, 2);
        write2buf(body, "step\t", 5); 
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_GET_VALUE_URL, LEN(CUR_GET_VALUE_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        PyObject *temp = convert2dict(con->response_body);
        result = PyDict_GetItemString(temp, "value");
        Py_INCREF(result);
        Py_DECREF(temp);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;

}

inline PyObject* 
rpc_call_cur_get(DBObject *db, int cur, int step)
{
    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    set_param_cur(body, cur);
    if(step > 0){
        write2buf(body, CRLF, 2);
        write2buf(body, "step\t", 5); 
    }

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_GET_URL, LEN(CUR_GET_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        PyObject *temp = convert2dict(con->response_body);
        PyObject *key = PyDict_GetItemString(temp, "key");
        PyObject *value = PyDict_GetItemString(temp, "value");
        result = PyTuple_Pack(2, key, value);
        Py_DECREF(temp);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

inline PyObject* 
rpc_call_cur_delete(DBObject *db, int cur)
{

    http_connection *con;
    char content_length[12];
    PyObject *result = NULL;
    buffer *body;

    con = db->con;
    body = new_buffer(BUF_SIZE, 0);
    if(body == NULL){
        return NULL;
    }
    if(init_bucket(con, 24) < 0){
        return NULL;
    }

    set_param_cur(body, cur);

    set_request_path(con, METHOD_POST, LEN(METHOD_POST), CUR_DELETE_URL, LEN(CUR_DELETE_URL));
    snprintf(content_length, sizeof (content_length), "%d", body->len);
    add_content_length(con, content_length, strlen(content_length));
    add_header_oneline(con, KT_CONTENT_TYPE, LEN(KT_CONTENT_TYPE));
    end_header(con);
    
    add_body(con, body->buf, body->len);

    if(request(con, 200) > 0){
        result = Py_True;
        Py_INCREF(result);
    }else{
        if(con->response_status == RES_SUCCESS){
            set_error(con);
        }else{
            PyErr_SetString(KtException, "could not set error ");
        }
    }
    
    free_buffer(body);
    free_http_data(con);

    return result;
}

