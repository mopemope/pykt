#ifndef RPC_H
#define RPC_H

#include "pykt.h"
#include "db.h"
#include "http.h"


PyObject* 
rpc_call_echo(DBObject *db);

PyObject* 
rpc_call_report(DBObject *db);

PyObject* 
rpc_call_play_script(DBObject *db, char *name, Py_ssize_t name_len,  PyObject *recordObj);

PyObject* 
rpc_call_status(DBObject *db);

PyObject* 
rpc_call_clear(DBObject *db);

PyObject* 
rpc_call_sync(DBObject *db, int hard, char *command, Py_ssize_t command_len);
 
PyObject* 
rpc_call_set(DBObject *db, PyObject *keyObj, PyObject *valueObj, int expire);

PyObject* 
rpc_call_add(DBObject *db, PyObject *keyObj, PyObject *valueObj, int expire);

PyObject* 
rpc_call_replace(DBObject *db, PyObject *keyObj, PyObject *valueObj, int expire);

PyObject* 
rpc_call_append(DBObject *db, PyObject *keyObj, PyObject *valueObj, int expire);

PyObject* 
rpc_call_increment(DBObject *db, PyObject *keyObj, int num, int expire);

PyObject* 
rpc_call_increment_double(DBObject *db, PyObject *keyObj, double num, int expire);

PyObject* 
rpc_call_cas(DBObject *db, PyObject *keyObj, PyObject *ovalObj, PyObject *nvalObj, int expire);

PyObject* 
rpc_call_set_bulk(DBObject *db, PyObject *recordObj, int expire, int atomic);

PyObject* 
rpc_call_remove_bulk(DBObject *db, PyObject *keysObj, int atomic);

PyObject* 
rpc_call_get_bulk(DBObject *db, PyObject *keysObj, int atomic);

PyObject* 
rpc_call_vacuum(DBObject *db, int step);

PyObject* 
rpc_call_match_prefix(DBObject *db, PyObject *prefixObj);

PyObject* 
rpc_call_match_regex(DBObject *db, PyObject *regexObj);

PyObject* 
rpc_call_cur_jump(DBObject *db, int cur, PyObject *keyObj);

PyObject* 
rpc_call_cur_jump_back(DBObject *db, int cur, PyObject *keyObj);

PyObject* 
rpc_call_cur_step(DBObject *db, int cur);

PyObject* 
rpc_call_cur_step_back(DBObject *db, int cur);

PyObject* 
rpc_call_cur_set_value(DBObject *db, int cur, PyObject *valueObj, int step, int expire);

PyObject* 
rpc_call_cur_remove(DBObject *db, int cur);

PyObject* 
rpc_call_cur_get_key(DBObject *db, int cur, int step);

PyObject* 
rpc_call_cur_get_value(DBObject *db, int cur, int step);

PyObject* 
rpc_call_cur_get(DBObject *db, int cur, int step);

PyObject* 
rpc_call_cur_delete(DBObject *db, int cur);

#endif


