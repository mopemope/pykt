#include "tsv_parser.h"
#include <assert.h>

#define MARK(M, FPC) (ctx->M = FPC - data)
#define MARK_LEN(M, FPC) (FPC - (ctx->M + data))
#define MARK_PTR(M) (ctx->M + data)

%%{
    machine tsv;

    action mark_key {
        MARK(key_start, fpc);
    }

    action key {
        ctx->key_len = MARK_LEN(key_start, fpc);
        MARK(value_start, fpc+1);
    }

    action mark_value {
        MARK(value_start, fpc);
        //printf("mark value\n");
    }

    action value {
		if(ctx->on_record){
            ctx->on_record(ctx, MARK_PTR(key_start), ctx->key_len, MARK_PTR(value_start), MARK_LEN(value_start, fpc));
        }
    }

    action integer {
        //printf("integer \n");
    }
    
    action double {
        //printf("double \n");
    }

    action error {
        ctx->error = 1;
        //printf("error \n");
        fbreak;
    }

    CRLF = ('\r\n' | '\n');
    TAB = '\t';

    keystr = ([^\r\n \t]+);
    valstr = ([^\r\n]+);
    dot = '.'; 
    integer = digit+ %integer;
    double = digit+ dot digit+ %double;
    
    val = valstr | integer | double;
    
    key = keystr $err(error) >mark_key %key;
    value = val  $err(error) >mark_value ;
    tsv = key TAB value? %value;

    main := (tsv CRLF)* tsv? $err(error);
}%%

%% write data;

inline void 
tsv_init(tsv_ctx *ctx)
{

	int cs = 0;
	%% write init;
	ctx->cs = cs;
}

inline size_t 
tsv_execute(tsv_ctx *ctx, const char* data, size_t len, size_t off)
{
    const char *p, *pe, *eof;
    int cs = ctx->cs;

    assert(off <= len && "offset past end of buffer");

    p = data + off;
    pe = data + len;
	eof = pe;

    assert(*pe == '\0' && "pointer does not end on NUL");
    assert(pe - p == len - off && "pointers aren't same distance");

	%% write exec;

    ctx->cs = cs;
    ctx->nread += p - (data + off);

    assert(p <= pe && "buffer overflow after parsing execute");
    assert(ctx->nread <= len && "nread longer than length");
    //assert(ctx->pos <= len && "body starts after buffer end");

    return(ctx->nread);
}



