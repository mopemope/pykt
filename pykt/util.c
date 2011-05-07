#include "util.h"

static char hex[] = "0123456789ABCDEF";

static inline int
hex2int(int i)
{
    i = toupper(i);
    i = i - '0';
    if(i > 9){ 
        i = i - 'A' + '9' + 1;
    }
    return i;
}

inline int
urldecode(char *buf, int len)
{
    int c, c1;
    char *s0, *t;
    t = s0 = buf;
    while(len > 0){
        c = *buf++;
        if(c == '%' && len > 2){
            c = *buf++;
            c1 = c;
            c = *buf++;
            c = hex2int(c1) * 16 + hex2int(c);
            len -= 2;
        }
        *t++ = c;
        len--;
    }
    *t = 0;
    return t - s0;
}

static inline char 
fromhex(char ch) 
{
  return isdigit(ch) ? ch - '0' : tolower(ch) - 'a' + 10;
}

static inline char 
tohex(char code) {
  return hex[code & 15];
}

inline void 
urlencode(char *str, size_t len, char **s, size_t *s_len)
{  
    char *pstr = str;
    char *buf = PyMem_Malloc(len * 3 + 1);
    char *pbuf = buf;

    while (*pstr) {
        if (isalnum(*pstr) || *pstr == '-' || *pstr == '_' || *pstr == '.' || *pstr == '~'){
            *pbuf++ = *pstr;
        } else if (*pstr == ' '){
            *pbuf++ = '+';
        }else{
            *pbuf++ = '%';
            *pbuf++ = tohex(*pstr >> 4);
            *pbuf++ = tohex(*pstr & 15);
        }
        pstr++;
    }
    *s_len = pbuf - buf;
    *pbuf++ = '\0';
    *s = buf;
}

inline uint64_t
get_expire_time(int val)
{
    time_t now;
    now = time(NULL);
    //DEBUG("time %d", now);
    return now + val;
}
