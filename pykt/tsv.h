#ifndef TSV_H
#define TSV_H

#include "pykt.h"
#include "buffer.h"
#include "tsv_parser.h"

PyObject * 
convert2dict(buffer *buf);

PyObject * 
convert2valuedict(buffer *buf);

PyObject * 
convert2keylist(buffer *buf);

#endif

