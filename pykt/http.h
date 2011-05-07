#ifndef HTTP_H
#define HTTP_H

#include "pykt.h"
#include "bucket.h"
#include "buffer.h"
#include "http_parser.h"

#define CRLF "\r\n"
#define DELIM ": "
#define SPACE " "

#define METHOD_GET "GET "
#define METHOD_HEAD "HEAD "
#define METHOD_POST "POST "
#define METHOD_PUT "PUT "
#define METHOD_DELETE "DELETE "

#define HTTP_10 " HTTP/1.0\r\n"
#define HTTP_11 " HTTP/1.1\r\n"

#define CONTENT_LENGTH "Content-Length: "
#define X_KT_XT "X-Kt-Xt: " 

#define CONNECTION_KEEP_ALIVE "Connection: keep-alive\r\n"

#define KT_CONTENT_TYPE "Content-Type : text/tab-separated-values; colenc=U\r\n"

#define X_KT_MODE_SET "X-Kt-Mode : set\r\n" 
#define X_KT_MODE_ADD "X-Kt-Mode : add\r\n" 
#define X_KT_MODE_REPLACE "X-Kt-Mode : replace\r\n" 

#define X_KT_FIELD_NAME "X-Kt-Xt" 


typedef enum{
    RES_INIT = 0,
    RES_READY,
    RES_SUCCESS,
    RES_MEMORY_ERROR,
    RES_HTTP_ERROR,
    RES_KT_ERROR,
} response_status_type;

typedef struct {
    char *host;
    int port;
    int timeout;

    int fd;
    data_bucket *bucket;
    http_parser *parser;
    buffer *response_body;
    int status_code;
    response_status_type response_status;
    uint8_t head;
    uint8_t have_kt_error;
} http_connection;

inline http_connection *
open_http_connection(char *host, int port, int timeout);

inline int
close_http_connection(http_connection *con);

inline void 
free_http_data(http_connection *con);

inline int  
request(http_connection *connection, int status_code);

inline int 
send_data(http_connection *con);

#endif 


