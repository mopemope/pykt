#ifndef REQUEST_H
#define REQUEST_H

#include "pykt.h"
#include "http.h"

inline void
set_request_path(http_connection *connection, char *method, size_t method_len, char *path, size_t path_len);

inline void
add_header_oneline(http_connection *con, char *val, size_t val_len);

inline void
add_header(http_connection *connection, char *name, size_t name_len, char *value, size_t value_len);

inline void
add_content_length(http_connection *con, char *value, size_t value_len);

inline void
add_kt_xt(http_connection *con, char *value, size_t value_len);

inline void
end_header(http_connection *connection);

inline void
add_crlf(http_connection *connection);

inline void
add_body(http_connection *con, char *value, size_t value_len);

#endif

