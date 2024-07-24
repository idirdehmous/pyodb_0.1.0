#define PY_SSIZE_T_CLEAN
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <Python.h>

//  THIS IS A DRAFT OF INIT MODULE !!!
//
//  //Init some specific python environment veriables
//  //Init some ODB envs 
//
//
//
//
//
  static PyObject* pyodbInit_method( PyObject *self, PyObject *args )
 {
  Py_GetBuildInfo();
//
//  //Py_GetCompiler()
//
//  //Py_GetCopyright()
//
//  //Py_GetPlatform()
//
//  //Py_GetVersion()
  return PyLong_FromLong(0) ;
  }




static PyMethodDef module_methods[] = {
    {"pyodbInit"  ,  (PyCFunction)(void(*)(void))    pyodbInit_method   ,
     METH_VARARGS | METH_KEYWORDS,   "pyodb init env "},

};


// Modules definition
static struct PyModuleDef   odbmodule = {
    PyModuleDef_HEAD_INIT,
    "pyodb_init",
    "Create configurtion env for used python and odb !",
    -1,
    module_methods ,
};


// Called first during python call
PyMODINIT_FUNC PyInit_pyodb_init(void) {
    PyObject*  m  ;
    char*     ModuleError ;


    m=PyModule_Create(&odbmodule);
    if ( m == NULL) {
        ModuleError = PyErr_NewException("Failed to create the module : pyodb_init", NULL, NULL);
        Py_XINCREF(ModuleError) ;
        return NULL;
}


    return m  ;
}

