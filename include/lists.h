#define PY_SSIZE_T_CLEAN
#include  <stdio.h>
#include  "Python.h"

// Update lists items through pointers 
static PyObject*  list_reset(PyObject* *list_col , Py_ssize_t ncols )
{    *list_col = PyList_New (ncols) ;
     return *list_col ;
}

