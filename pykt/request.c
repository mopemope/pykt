#include "request.h"

inline void
set_request_path(http_connection *con, char *method, size_t method_len, char *path, size_t path_len)
{
    data_bucket *bucket = con->bucket;
    
    //DEBUG("request URL %s", path);
    set2bucket(bucket, method, method_len);
    set2bucket(bucket, path, path_len);
    set2bucket(bucket, HTTP_11, LEN(HTTP_11));
    set2bucket(bucket, CONNECTION_KEEP_ALIVE, LEN(CONNECTION_KEEP_ALIVE));
}

inline void
add_content_length(http_connection *con, char *value, size_t value_len)
{
    data_bucket *bucket = con->bucket;
    set2bucket(bucket, CONTENT_LENGTH, LEN(CONTENT_LENGTH));
    set2bucket(bucket, value, value_len);
    set2bucket(bucket, CRLF, 2);
}

inline void
add_kt_xt(http_connection *con, char *value, size_t value_len)
{
    data_bucket *bucket = con->bucket;
    set2bucket(bucket, X_KT_XT, LEN(X_KT_XT));
    set2bucket(bucket, value, value_len);
    set2bucket(bucket, CRLF, 2);
}

inline void
add_crlf(http_connection *con)
{
    set2bucket(con->bucket, CRLF, 2);
}

inline void
end_header(http_connection *con)
{
    add_crlf(con);
}

inline void
add_header_oneline(http_connection *con, char *value, size_t value_len)
{
    data_bucket *bucket = con->bucket;
    set2bucket(bucket, value, value_len);
}

inline void
add_header(http_connection *con, char *name, size_t name_len, char *value, size_t value_len)
{
    data_bucket *bucket = con->bucket;
    set2bucket(bucket, name, name_len);
    set2bucket(bucket, DELIM, 2);
    set2bucket(bucket, value, value_len);
    set2bucket(bucket, CRLF, 2);
}

inline void
add_body(http_connection *con, char *value, size_t value_len)
{
    //DEBUG("HTTP BODY \n%.*s", value_len, value);
    data_bucket *bucket = con->bucket;
    set2bucket(bucket, value, value_len);
}
