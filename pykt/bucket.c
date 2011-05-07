#include "bucket.h"

inline data_bucket *
create_data_bucket(int cnt)
{

    data_bucket *bucket;
    iovec_t *iov;

    bucket = PyMem_Malloc(sizeof(data_bucket));
    if(bucket == NULL){
        PyErr_NoMemory();
        return NULL;
    }
    memset(bucket, 0, sizeof(data_bucket));
    
    iov = (iovec_t *)PyMem_Malloc(sizeof(iovec_t) * cnt);
    if(iov == NULL){
        PyMem_Free(bucket);
        PyErr_NoMemory();
        return NULL;
    }
    bucket->iov = iov;
    bucket->iov_size = cnt;
    DEBUG("create_data_bucket %p", bucket);
    return bucket;
}

inline void
free_data_bucket(data_bucket *bucket)
{
    DEBUG("free_data_bucket %p", bucket);
    PyMem_Free(bucket->iov);
    PyMem_Free(bucket);
}

inline void
set2bucket(data_bucket *bucket, char *buf, size_t len)
{
    
    //DEBUG("set2bucket buf: %s, len: %d", buf, len);
    
    bucket->iov[bucket->iov_cnt].iov_base = buf;
    bucket->iov[bucket->iov_cnt].iov_len = len;
    bucket->iov_cnt++;
    bucket->total += len;
    bucket->total_size += len;
}



