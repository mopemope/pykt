#include "cursor.h"
#include "rpc.h"

static int curnum = 0;

static inline int
CursorObject_init(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    DBObject *db;
    uintptr_t i;

    if (!PyArg_ParseTuple(args, "O!", &DBObjectType, &db)){
        return -1;
    }
    if(!is_opened(db)){
        return -1;
    }

    Py_INCREF(db);
    self->db = db;
    curnum++;
    i = (uintptr_t)db >> 8;
    self->cur = (i + curnum) >> 2;
    DEBUG("CursorObject_init %p CUR: %d", self, self->cur);
    return 0;
}

static inline void
CusorObject_dealloc(CursorObject *self)
{
    
    DEBUG("CursorObject_dealloc %p CUR: %d", self, self->cur);
    /* 
    if(is_opened(self->db)){
        //Not impl ???
        PyObject *result = rpc_call_cur_delete(self->db, self->cur);
        Py_XDECREF(result);
    }*/
    Py_CLEAR(self->db);
    self->ob_type->tp_free((PyObject*)self);
}

static inline PyObject* 
CursorObject_jump(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *keyObj = NULL;

    static char *kwlist[] = {"key", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", kwlist, &keyObj)){
        return NULL; 
    }
    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_jump(self->db, self->cur, keyObj);

}

static inline PyObject* 
CursorObject_jump_back(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *keyObj = NULL;

    static char *kwlist[] = {"key", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", kwlist, &keyObj)){
        return NULL; 
    }
    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_jump_back(self->db, self->cur, keyObj);
}

static inline PyObject* 
CursorObject_step(CursorObject *self, PyObject *args)
{
    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_step(self->db, self->cur);
}

static inline PyObject* 
CursorObject_step_back(CursorObject *self, PyObject *args)
{
    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_step_back(self->db, self->cur);
}

static inline PyObject* 
CursorObject_set_value(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *valueObj = NULL;
    int step = 0, expire = 0;

    static char *kwlist[] = {"value", "step", "expire", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|ii", kwlist, &valueObj, &step, &expire)){
        return NULL; 
    }
    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_set_value(self->db, self->cur, valueObj, step, expire);
}

static inline PyObject* 
CursorObject_remove(CursorObject *self, PyObject *args)
{
    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_remove(self->db, self->cur);
}

static inline PyObject* 
CursorObject_get_key(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    int step = 0;

    static char *kwlist[] = {"step", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i", kwlist, &step)){
        return NULL; 
    }
    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_get_key(self->db, self->cur, step);
}

static inline PyObject* 
CursorObject_get_value(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    int step = 0;

    static char *kwlist[] = {"step", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i", kwlist, &step)){
        return NULL; 
    }
    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_get_value(self->db, self->cur, step);
}

static inline PyObject* 
CursorObject_get(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    int step = 0;

    static char *kwlist[] = {"step", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i", kwlist, &step)){
        return NULL; 
    }

    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_get(self->db, self->cur, step);
}

static inline PyObject* 
CursorObject_delete(CursorObject *self, PyObject *args)
{
    if(!is_opened(self->db)){
        return NULL;
    }
    return rpc_call_cur_delete(self->db, self->cur);
}

static PyMethodDef CursorObject_methods[] = {
    {"jump", (PyCFunction)CursorObject_jump, METH_VARARGS|METH_KEYWORDS, 0},
    //{"jump_back", (PyCFunction)CursorObject_jump_back, METH_VARARGS|METH_KEYWORDS, 0},
    {"step", (PyCFunction)CursorObject_step, METH_NOARGS, 0},
    //{"step_back", (PyCFunction)CursorObject_step_back, METH_NOARGS, 0},
    {"set_value", (PyCFunction)CursorObject_set_value, METH_VARARGS|METH_KEYWORDS, 0},
    {"remove", (PyCFunction)CursorObject_remove, METH_NOARGS, 0},
    {"get_key", (PyCFunction)CursorObject_get_key, METH_VARARGS|METH_KEYWORDS, 0},
    {"get_value", (PyCFunction)CursorObject_get_value, METH_VARARGS|METH_KEYWORDS, 0},
    {"get", (PyCFunction)CursorObject_get, METH_VARARGS|METH_KEYWORDS, 0},
    //{"delete", (PyCFunction)CursorObject_delete, METH_NOARGS, 0},
    {NULL, NULL}
};




PyTypeObject CursorObjectType = {
	PyObject_HEAD_INIT(&PyType_Type)
    0,
    "pykt.Cursor",             /*tp_name*/
    sizeof(CursorObject), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)CusorObject_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,        /*tp_flags*/
    "Cursor",           /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,                       /* tp_weaklistoffset */
    0,		               /* tp_iter */
    0,		               /* tp_iternext */
    CursorObject_methods,                          /* tp_methods */
    0,                         /* tp_members */
    0,                           /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)CursorObject_init,                      /* tp_init */
    0,                         /* tp_alloc */
    0,                           /* tp_new */
};

