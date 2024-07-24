#define PY_SSIZE_T_CLEAN
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <Python.h>
#include "odbdump.h"
#include "magicwords.h"
#include "info.h"
#include "odb.h"
#include "privpub.h"
#include "pyspam.h"
#include "macros.h"




/* ENDIANESS */
extern int ec_is_little_endian();
extern double util_walltime_();



static int maxhandle = 0;     /* Actual max no. of handles; increase/decrease via export ODB_MAXHANDLE */

//static  const int def_maxhandle = 10;
static  DB_t      *free_handles = NULL;

PUBLIC void *
ODBc_get_free_handles(int *Maxhandle)
{  if (Maxhandle) *Maxhandle = maxhandle;  return (void *)free_handles; }


static PyObject *pyodbDca_method( PyObject* Py_UNUSED(self) , PyObject *args , PyObject*  kwargs ) {
    char *path     = NULL ; 
    char *database = NULL ;

    //PyObject* ncpu  ;  // default ncpus
    Py_Initialize() ;
    int   ncpu   = 1  ; 
    char *dbname =NULL; 
    
    static char *kwlist[] = {"dbpath","db","ncpu"};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|si", kwlist, &path , &database , &ncpu))
    {
        return NULL;
    }

   database=path  ; 
 
   char * dcagen_path ; 
   char * febin  ;  
   char * sysbin ;
   
   febin      =getenv( "ODB_FEBINPATH" )  ;
   sysbin     =getenv( "ODB_SYSPATH"  )  ;
   dcagen_path=getenv( "ODB_BEBINPATH" )  ; 

    putenv(febin);
    putenv(sysbin);
    putenv(dcagen_path);
    
    char scpu[5];
    tostring(scpu, ncpu);
    char* dbname_arg ;
    char* ncpu_arg ; 
    //char* quiet  ; 
    if (dbname) { dbname_arg = concat ( " -l " , dbname   ); } else { dbname_arg=" " ;  };
    if (ncpu  ) { ncpu_arg   = concat ( " -N " , scpu     ); } else { ncpu_arg  =" " ;  };
    //if (verb  ) {  quiet = " -q "; } else { quiet =" " ;  };

    char* dca_path   = concat(dcagen_path , "/dcagen ");
    char* cma_path   = concat( " -i "     ,  database );
    char* dca_cmd    = concat(dca_path    ,  cma_path) ;
    char* dca_args   = concat(dca_cmd     ,  " -F -n -q -z " ) ; 

    char* dca_db     = concat( dca_args   ,  dbname_arg );
    char* dca_cpu    = concat( dca_db     ,  ncpu_arg  );
    //char* dca_verb   = concat(dca_cpu     ,  quiet    ) ; 

    printf ( "%s\n"  , "Creating DCA files ..." ) ; 

    int status= system ( dca_cpu ) ; 
    if ( status != 0){  
    printf("%s  %s\n" , dca_cpu, " command returned a non Zero value,  !" ) ;
   
    return PyLong_FromLong( 1 ) ;
    }else {

    free(dca_path) ; 
    free(cma_path) ;
    free(dca_cmd ) ;
    free(dca_args) ;      // deallocate the string
    }
    return PyLong_FromLong( 0 ) ; 
}


static PyMethodDef module_methods[] = {
    {"pyodbDca"  ,  (PyCFunction)(void(*)(void))    pyodbDca_method   ,
     METH_VARARGS | METH_KEYWORDS,   "Close an opened ODB "},


};


// Modules definition
static struct PyModuleDef   odbmodule = {
    PyModuleDef_HEAD_INIT,
    "pyodb_dca",
    "Create and handle DCA files ( Direct Column Access files  )!",
    -1,
    module_methods ,
     .m_slots =NULL
};


// Called first during python call
PyMODINIT_FUNC PyInit_pyodb_dca(void) {
    PyObject*  m  ;
    PyObject*  ModuleError ;


    m=PyModule_Create(&odbmodule);
    if ( m == NULL) {
        ModuleError = PyErr_NewException("Failed to create the module : pyodb_dca", NULL, NULL);
        Py_XINCREF(ModuleError) ;
        return NULL;
}


    return m  ;
}

