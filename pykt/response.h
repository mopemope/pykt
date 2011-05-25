#ifndef RESPONSE_H
#define RESPONSE_H

#include "pykt.h"
#include "http.h"

http_parser *  
init_parser(http_connection *con);

size_t 
execute_parse(http_connection *con, const char *buf, size_t len);

int 
parser_finish(http_connection *con);

#endif

