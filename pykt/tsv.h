#ifndef TSV_H
#define TSV_H

#include "pykt.h"
#include "buffer.h"
#include "tsv_parser.h"

inline PyObject * 
convert2dict(buffer *buf);

inline PyObject * 
convert2valuedict(buffer *buf);

inline PyObject * 
convert2keylist(buffer *buf);

#endif

