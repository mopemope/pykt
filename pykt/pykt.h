#ifndef PYKT_H
#define PYKT_H

#include <Python.h>
#include <structmember.h>

#include <assert.h>
#include <fcntl.h>   
#include <stddef.h> 
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <inttypes.h>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <sys/select.h>
#include <sys/time.h>

#define VERSION "0.0.1"

#ifdef DEVELOP
#define DEBUG(...) \
    do { \
        printf("DEBUG: "); \
        printf(__VA_ARGS__); \
        printf("\n"); \
    } while(0)
#define DUMP(...) \
    do { \
        printf("-------------------------------------------------------\n"); \
        printf(__VA_ARGS__); \
        printf("\n"); \
        printf("-------------------------------------------------------\n"); \
    } while(0)
#else
#define DEBUG(...) do{}while(0)
#define DUMP(...) do{}while(0)
#endif

#define LEN(x) sizeof(x) -1

#define WAIT_READ 1
#define WAIT_WRITE 2

//extern PyObject *wait_callback;
//extern PyObject *serialize_func;
//extern PyObject *deserialize_func;

extern PyObject *wait_callback;
extern PyObject *KtException;
extern PyObject *TimeoutException;

inline void
call_wait_callback(int fd, int type);

inline PyObject *
serialize_value(PyObject *obj);

inline PyObject *
deserialize_value(PyObject *obj);

#endif 
