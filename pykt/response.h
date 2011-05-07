#ifndef RESPONSE_H
#define RESPONSE_H

#include "pykt.h"
#include "http.h"

inline http_parser *  
init_parser(http_connection *con);

inline size_t 
execute_parse(http_connection *con, const char *buf, size_t len);

inline int 
parser_finish(http_connection *con);

#endif

