#ifndef TSV_PARSER_H
#define TSV_PARSER_H

#include "pykt.h"

typedef void (*record_cb)(void *ctx, const char *key, size_t key_len, const char *val, size_t val_len);

typedef struct {
	int cs;

    size_t key_start;
    size_t key_len;
    size_t value_start;

    size_t nread;
    uint8_t error;

    record_cb on_record;

    void *user;
} tsv_ctx;

inline void 
tsv_init(tsv_ctx *ctx);

inline size_t 
tsv_execute(tsv_ctx *ctx, const char* data, size_t len, size_t off);

#endif

