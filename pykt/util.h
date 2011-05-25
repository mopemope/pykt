#ifndef UTIL_C
#define UTIL_C

#include "pykt.h"

int
urldecode(char *buf, int len);

void 
urlencode(char *str, size_t len, char **s, size_t *s_len);

uint64_t
get_expire_time(int val);

#endif 
