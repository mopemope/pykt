#include "buffer.h"
#include "util.h"

#define LIMIT_MAX 1024 * 1024 * 1024

static inline buffer*
alloc_buffer(void)
{
    buffer *buf;
    buf = (buffer *)PyMem_Malloc(sizeof(buffer));
    memset(buf, 0, sizeof(buffer));
    return buf;
}

static inline void
dealloc_buffer(buffer *buf)
{
    PyMem_Free(buf);
}


inline buffer *
new_buffer(size_t buf_size, size_t limit)
{
    buffer *buf;
    
    buf = alloc_buffer();
    if(buf == NULL){
        PyErr_NoMemory();
        return NULL;
    }

    buf->buf = PyMem_Malloc(sizeof(char) * buf_size);
    if(buf->buf == NULL){
        dealloc_buffer(buf);
        PyErr_NoMemory();
        return NULL;
    }
    
    buf->buf_size = buf_size;
    if(limit){
        buf->limit = limit;
    }else{
        buf->limit = LIMIT_MAX;
    }
    DEBUG("new_buffer %p buf_size %d", buf, buf->buf_size);
    return buf;
}

inline buffer_result
write2buf(buffer *buf, const char *c, size_t  l) {
    size_t newl;
    char *newbuf;
    buffer_result ret = WRITE_OK;
    newl = buf->len + l;
    
    
    if (newl >= buf->buf_size) {
        buf->buf_size *= 2;
        if(buf->buf_size <= newl) {
            buf->buf_size = (int)(newl + 1);
        }
        if(buf->buf_size > buf->limit){
            buf->buf_size = buf->limit + 1;
        }
        DEBUG("warning !!! write2buf realloc !! %p", buf);
        newbuf = (char*)PyMem_Realloc(buf->buf, buf->buf_size);
        if (!newbuf) {
            PyErr_SetString(PyExc_MemoryError,"out of memory");
            free(buf->buf);
            buf->buf = 0;
            buf->buf_size = buf->len = 0;
            return MEMORY_ERROR;
        }
        buf->buf = newbuf;
    }
    if(newl >= buf->buf_size){
        l = buf->buf_size - buf->len -1;
        ret = LIMIT_OVER;
    }
    memcpy(buf->buf + buf->len, c , l);
    buf->len += (int)l;
    return ret;
}

inline void
free_buffer(buffer *buf)
{
    DEBUG("free_buffer %p", buf);
    PyMem_Free(buf->buf);
    dealloc_buffer(buf);
}

inline PyObject *
getPyString(buffer *buf)
{
    PyObject *o;
    o = PyString_FromStringAndSize(buf->buf, buf->len);
    //free_buffer(buf);
    //DEBUG("getPyString %p", buf);
    return o;
}

inline PyObject *
getPyStringAndDecode(buffer *buf)
{
    PyObject *o;
    int l = urldecode(buf->buf, buf->len);
    o = PyString_FromStringAndSize(buf->buf, l);
    free_buffer(buf);
    return o;
}


inline char *
getString(buffer *buf)
{
    buf->buf[buf->len] = '\0';
    return buf->buf;
}



