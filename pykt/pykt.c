#include "pykt.h"
#include "db.h"
#include "cursor.h"

PyObject *KtException;
PyObject *TimeoutException;
PyObject *wait_callback = NULL;;

static PyObject *serialize_func = NULL;
static PyObject *deserialize_func = NULL;

static inline PyObject *
clear_wait_callback(PyObject *self, PyObject *args)
{
    if(wait_callback){
        Py_CLEAR(wait_callback);
        wait_callback = NULL;
    }
    Py_RETURN_NONE;
}

static inline PyObject *
set_wait_callback(PyObject *self, PyObject *args)
{
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "O:set_wait_callback", &temp))
        return NULL;
    
    if(!PyCallable_Check(temp)){
        PyErr_SetString(PyExc_TypeError, "must be callable");
        return NULL;
    }
    if(wait_callback){
        Py_DECREF(wait_callback);
    }
    wait_callback = temp;
    Py_INCREF(wait_callback);
    Py_RETURN_NONE;
}

static inline PyObject *
set_serialize_func(PyObject *self, PyObject *args)
{
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "O:set_serializer", &temp))
        return NULL;
    
    if(!PyCallable_Check(temp)){
        PyErr_SetString(PyExc_TypeError, "must be callable");
        return NULL;
    }
    if(serialize_func){
        Py_DECREF(serialize_func);
    }
    serialize_func = temp;
    Py_INCREF(serialize_func);
    Py_RETURN_NONE;
}

static inline PyObject *
set_deserialize_func(PyObject *self, PyObject *args)
{
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "O:set_deserializer", &temp))
        return NULL;
    
    if(!PyCallable_Check(temp)){
        PyErr_SetString(PyExc_TypeError, "must be callable");
        return NULL;
    }
    if(deserialize_func){
        Py_DECREF(deserialize_func);
    }
    deserialize_func = temp;
    Py_INCREF(deserialize_func);
    Py_RETURN_NONE;
}

inline void
call_wait_callback(int fd, int type)
{
    PyObject *result = NULL, *args = NULL;
    if(wait_callback){
        args = Py_BuildValue("(ii)", fd, type);
        result = PyObject_CallObject(wait_callback, args);
        Py_DECREF(args);
        Py_XDECREF(result);
    }
}

inline PyObject *
serialize_value(PyObject *obj)
{
    if(serialize_func){
        return PyObject_CallFunctionObjArgs(serialize_func, obj, NULL);
    }else{
        return PyObject_Str(obj);
    }
}

inline PyObject *
deserialize_value(PyObject *obj)
{
    if(deserialize_func){
        return PyObject_CallFunctionObjArgs(deserialize_func, obj, NULL);
    }
    Py_INCREF(obj);
    return obj;
}

static PyMethodDef PyKtMethods[] = {
    {"set_wait_callback", set_wait_callback, METH_VARARGS, "set wait callback"},
    {"clear_wait_callback", clear_wait_callback, METH_VARARGS, "clear wait callback"},
    {"set_serializer", set_serialize_func, METH_VARARGS, "set serialize func"},
    {"set_deserializer", set_deserialize_func, METH_VARARGS, "set deserialize func"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};


PyMODINIT_FUNC
initpykt(void)
{
    PyObject *m;
    
    if(PyType_Ready(&DBObjectType) < 0){ 
        return;
    }

    CursorObjectType.tp_new = PyType_GenericNew;
    if(PyType_Ready(&CursorObjectType) < 0){ 
        return;
    }
    
    KtException = PyErr_NewException("pykt.KtException",
					  PyExc_IOError, NULL);
	if (KtException == NULL){
		return;
    }

    TimeoutException = PyErr_NewException("pykt.TimeoutException",
					  KtException, NULL);
	if (TimeoutException == NULL){
		return;
    }
    
    m = Py_InitModule3("pykt", PyKtMethods, "");
    if(m == NULL){
        return;
    }
    Py_INCREF(&DBObjectType);
    PyModule_AddObject(m, "KyotoTycoon", (PyObject *)&DBObjectType);
    Py_INCREF(&CursorObjectType);
    PyModule_AddObject(m, "Cursor", (PyObject *)&CursorObjectType);
	
    Py_INCREF(KtException);
	PyModule_AddObject(m, "KTException", KtException);
    Py_INCREF(TimeoutException);
	PyModule_AddObject(m, "TimeoutException", TimeoutException);

    PyModule_AddIntConstant(m, "WAIT_READ", WAIT_READ);
    PyModule_AddIntConstant(m, "WAIT_WRITE", WAIT_WRITE);
    PyModule_AddStringConstant(m, "__version__", VERSION);    
    
}
