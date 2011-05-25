#ifndef BUCKET_H
#define BUCKET_H

#include "pykt.h"

typedef struct iovec iovec_t;

typedef struct {
    iovec_t *iov;
    uint32_t iov_cnt;
    uint32_t iov_size;
    uint32_t total;
    uint32_t total_size;
    uint8_t sended;
} data_bucket;

data_bucket *
create_data_bucket(int cnt);

void
free_data_bucket(data_bucket *bucket);

void
set2bucket(data_bucket *bucket, char *buf, const size_t len);

//int 
//writev_bucket(data_bucket *bucket);

#endif
