#define PY_SSIZE_T_CLEAN
#define CHUNK_SIZE 8192
#include <stdio.h>
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <Python.h>
#include "odbdump.h"
#include "pyspam.h"


/* ENDIANESS */
extern int ec_is_little_endian();
extern double util_walltime_();


// C wrappers

static PyObject* pyodbInit_method( PyObject *Py_UNUSED(self))
{
// INIALISATION OF PYTHON ENVIRONNEMT AND SOME SPECIFIC ODB VARIBALES !
// pyodb can run whitout , just for more rebustness 
// THIS MODDULE IS UNDER DEV !
    printf ("%s\n" ,"--------------------------------------------------" )  ;
    printf ("%s\n", "THE MODULE  pyodbInit  IS UNDER DEVELOPMENT !" )  ;
    printf ("%s\n" ,"--------------------------------------------------" )   ;
//Py_GetBuildInfo();
//Py_GetCompiler()
//Py_GetCopyright()
//Py_GetPlatform()
//Py_GetVersion()
return PyLong_FromLong(0) ;
}





// TODo 
// ADD MORE OPTIONS  --> FORMAT FLOATS 
//                   --> PRINT mdi 
//                   --> PRINT HEADER 
//                   --> CONVERT LAT,LON to DEGREES IF RADIANS 
//                   --> DEBUG ON/OFF 
static PyObject *pyodbFetch_method( PyObject* Py_UNUSED(self) , PyObject *args , PyObject*  kwargs ) {
    char *path     = NULL ;
    char *database = NULL ;
    char *sql_query= NULL;
    char *queryfile  = NULL;
    Py_Initialize() ;

    PyObject *st ,*dt , *ttm , *int4, *fl ;

    static char *kwlist[] = {"path","sql_query","queryfile"};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|ss", kwlist, &path , &sql_query , &queryfile))
    {
        return NULL;
    }

   database=path  ;

  //int i_am_little = ec_is_little_endian();
  char *poolmask  = NULL;
  char *varvalue  = NULL;
  //char *queryfile = NULL;
  Bool print_mdi  = true; /* by default prints "NULL", not value of the NULL */
  Bool raw        = true;
  int maxlines = -1;
  //char *dbl_fmt   = NULL;   /to be used later 
  void *h         = NULL;
  int maxcols     = 0;
  int rc          = 0;
  //double wlast;
  extern int optind;
 
  //Bool get_header=true ; 
  if (maxlines == 0) return PyLong_FromLong(rc );
   if ( sql_query  ) {
   printf ( "Executing query from string : %s\n" , sql_query ) ; 
   } else if ( queryfile) {
   printf ( "Executing query from file   : %s\n" , queryfile ) ;
   } ; 
  
   // open the odb 
    h     = odbdump_open(database, sql_query, queryfile, poolmask, varvalue, &maxcols);
    PyObject*  py_row    = PyList_New(0) ;
    Py_ssize_t irow      = -1  ;

  if (h && maxcols > 0) {
    
    printf( "Number of requested columns : %d \n" , maxcols  )  ; 
    int new_dataset = 0   ;
    colinfo_t *ci   = NULL;
    int nci = 0           ; // NUMBER OF COLUMNS IN QUERY 
    double *d = NULL;       // DATA VALUES C VARIABLE 
    int nd          ; 
    ll_t nrows =   0;
    ll_t nrtot =   0;
    //int  ncols =   0;  / to delete  
    Bool packed = false;
    int (*nextrow)(void *, void *, int, int *) = 
      packed ? odbdump_nextrow_packed : odbdump_nextrow;
    int dlen = packed ? maxcols * sizeof(*d) :(unsigned int) maxcols;

    ALLOCX(d, maxcols);

    while ( (nd = nextrow(h, d, dlen, &new_dataset)) > 0) {	    
      Py_ssize_t i ;
      PyObject*  py_col    = PyList_New(maxcols) ;

      char nul[5] = "NULL";
      char *pnul  = nul ; 
      if (new_dataset) { ci = odbdump_destroy_colinfo(ci, nci);	ci = odbdump_create_colinfo(h, &nci); new_dataset = 0; nrows = 0;}
      if(raw){
        Py_ssize_t  icol= -1 ; 
	for (  i=0; i<nd; i++) {
             colinfo_t *pci = &ci[i];
	     if (print_mdi && pci->dtnum != DATATYPE_STRING && ABS(d[i]) == mdi) {
	      icol++ ;
	      st=PyUnicode_FromString(  pnul ) ;
              PyList_SetItem ( py_col   , icol , st  ) ;
	      } 

	 else {
	 switch (pci->dtnum) { 
         case DATATYPE_STRING:
                {
        	//Py_ssize_t *obj;
		icol++ ; 
                char cc[sizeof(double)+1];
                char *scc = cc ;
                union {  char s[sizeof(double)] ;double d;} u ;  u.d = d[i];
                for (long unsigned int js=0; js<sizeof(double); js++) { char c = u.s[js]; *scc++ = isprint(c) ? c : '8' ; } 
                *scc = '\0';
		st=PyUnicode_FromFormat (cc )  ;
                PyList_SetItem ( py_col , icol , st  ) ; Py_INCREF(st)  ; 
               }
	      break;
         case DATATYPE_YYYYMMDD:
               icol++  ;
               dt=PyLong_FromLong((int)d[i]) ;
	       PyList_SetItem ( py_col , icol ,dt  ) ; Py_INCREF(dt) ; 
	       break;
	  case DATATYPE_HHMMSS:
	       icol++  ;
	       ttm=PyLong_FromLong((int)d[i]) ;
               PyList_SetItem ( py_col , icol ,ttm ) ; Py_INCREF(ttm); 
               break; 
          case DATATYPE_INT4:
              icol++  ;
              int4=PyLong_FromLong((int)d[i]) ; 
              PyList_SetItem ( py_col , icol , int4  ) ;Py_INCREF(int4);
              break;
	  default:
              icol++ ;
              fl=PyFloat_FromDouble((double)d[i]  ) ;
              PyList_SetItem ( py_col , icol ,   fl   ) ;Py_INCREF(fl) ; 
              break;
	    } /* switch (pci->dtnum) */			  
	 }
	} /* for (i=0; i<nd; i++) */
       } /* if (!raw_binary)*/
      ++nrows;
      ++irow  ;

     PyList_Append(py_row , py_col   ) ;
     
     if (maxlines > 0 && ++nrtot >= maxlines) break; /* while (...) */
    } /* while (...) */
    ci = odbdump_destroy_colinfo(ci, nci);
    rc = odbdump_close(h);
    FREEX(d);
 }    /* if (h && maxcols > 0) ... */
     Py_ssize_t num_rows  = PyList_Size(py_row  )  ; 
     
     PyObject*  nrows     = PyLong_FromSsize_t ( num_rows ) ;
    
     if ( nrows == PyLong_FromLong( 0 ) ){
         printf( "--The SQL request returned no data, please check the query \n" )   ;
        
     }
     //PyObject* cnt = reset_counter ( &num_rows  ) ; 


     return    py_row   ;
     Py_Finalize() ; 
}




static PyMethodDef module_methods[] = {
    {"pyodbInit"  ,  (PyCFunction)(void(*)(void))    pyodbInit_method   ,
     METH_VARARGS | METH_KEYWORDS,   "Python/ODB environement initialisation ! "},
    {"pyodbFetch",  (PyCFunction)(void(*)(void))     pyodbFetch_method ,
     METH_VARARGS | METH_KEYWORDS,   "Fetch rows from  a given ECMA or CCMA  ODB database given sql query "},

};

/*static PyTypeObject pyodbType = {
      PyVarObject_HEAD_INIT(NULL, 0)
     .tp_name = "pyodb",
     .tp_doc = PyDoc_STR("C/Python main module to access  ODB1 ECMWF database "),
     .tp_itemsize = 0,
     .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
     .tp_methods = pyodbInit_method ,  pyodbFetch_method  , 
};*/





// Modules definition
static struct PyModuleDef   odbmodule = {
    PyModuleDef_HEAD_INIT,
    "pyodb",
    "\n C/Python interface to access ODB1 ECMWF databases \n",
    -1,
    module_methods ,
     .m_slots =NULL
};



// Called first during python call
PyMODINIT_FUNC PyInit_pyodb(void) 
{
    PyObject* m;
    PyObject* ModuleError ;
    m = PyModule_Create(&odbmodule);

    m=PyModule_Create(&odbmodule);
    if ( m == NULL) { 
        ModuleError = PyErr_NewException("Failed to create the module : pyodb", NULL, NULL);
        Py_XINCREF(ModuleError) ;
        return NULL;
}

   return m;
}
