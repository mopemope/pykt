#include "response.h"

static inline int
message_begin_cb(http_parser *p)
{
    return 0;
}

static inline int
header_field_cb(http_parser *p, const char *buf, size_t len)
{
    http_connection *con;
    
    con = (http_connection *)p->data;
    if(strncmp("X-Kt-Error", buf, 10) == 0){
        con->have_kt_error = 1;
    }
    return 0;
}

static inline int
header_value_cb(http_parser *p, const char *buf, size_t len)
{
    http_connection *con;
    
    con = (http_connection *)p->data;
    if(con->have_kt_error){
        PyObject *value = PyString_FromStringAndSize(buf, len);
        PyErr_SetObject(KtException, value);
        Py_DECREF(value);
        con->response_status = RES_KT_ERROR;
        con->have_kt_error = 0;
        return 0;
    }
    return 0;
}

static inline int
request_path_cb(http_parser *p, const char *buf, size_t len)
{

    return 0;
}

static inline int
request_url_cb(http_parser *p, const char *buf, size_t len)
{
    return 0;
}

static inline int
query_string_cb(http_parser *p, const char *buf, size_t len)
{
    return 0;
}

static inline int
fragment_cb(http_parser *p, const char *buf, size_t len)
{
    return 0;
}


static inline int
body_cb(http_parser *p, const char *buf, size_t len)
{
    http_connection *con;
    buffer_result ret = MEMORY_ERROR;
    
    con = (http_connection *)p->data;
    ret = write2buf(con->response_body, buf, len);
    switch(ret){
        case MEMORY_ERROR:
            con->response_status = RES_MEMORY_ERROR;
            return -1;
        case LIMIT_OVER:
            con->response_status = RES_MEMORY_ERROR;
            return -1;
        default:
            break;
    }

    return 0;
}

static inline int
headers_complete_cb (http_parser *p)
{
    buffer *buf;
    http_connection *con;
    
    con = (http_connection *)p->data;

    //DEBUG("headers_complete_cb length:%d", p->content_length);
    if(p->content_length){
        buf = new_buffer(p->content_length + 1, 0);
    }else{
        buf = new_buffer(p->content_length, 0);
    }
    con->response_body = buf;
    con->status_code = p->status_code;
    if(con->head){
        if(con->response_status == RES_READY){
            con->response_status = RES_SUCCESS;
        }
    }
    return 0;
}

static inline int
message_complete_cb (http_parser *p)
{
    http_connection *con;
    
    con = (http_connection *)p->data;
    if(con->response_status == RES_READY){
        con->response_status = RES_SUCCESS;
    }
    return 0;
}

static http_parser_settings settings =
  {.on_message_begin = message_begin_cb
  ,.on_header_field = header_field_cb
  ,.on_header_value = header_value_cb
  ,.on_path = request_path_cb
  ,.on_url = request_url_cb
  ,.on_fragment = fragment_cb
  ,.on_query_string = query_string_cb
  ,.on_body = body_cb
  ,.on_headers_complete = headers_complete_cb
  ,.on_message_complete = message_complete_cb
  };

inline http_parser *  
init_parser(http_connection *con)
{
    http_parser *parser;

    parser = (http_parser *)PyMem_Malloc(sizeof(http_parser));
    if(parser == NULL){
        PyErr_NoMemory();
        return NULL;
    }

    memset(parser, 0, sizeof(http_parser));
    http_parser_init(parser, HTTP_RESPONSE);
    
    con->parser = parser;
    con->parser->data = con;
    return parser;
}

inline size_t 
execute_parse(http_connection *con, const char *buf, size_t len)
{
    size_t nparsed;
    nparsed = http_parser_execute(con->parser, &settings, buf, len);
    return nparsed;
}

inline int 
parser_finish(http_connection *con)
{
    return con->response_status == RES_SUCCESS;
}

