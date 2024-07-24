#define PY_SSIZE_T_CLEAN
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <Python.h>
#include "odbdump.h"
#include "pyspam.h"
//#include "lists.h"




/* ENDIANESS */
extern int ec_is_little_endian();
extern double util_walltime_();

// C wrapper 
static PyObject *pyodbFetch_method(PyObject* Py_UNUSED(self)  , PyObject *args) {
    Py_Initialize() ;
    char *database  = NULL;
    char *sql_query = NULL;
    char *fmt = "%.14g" ; 


    PyObject *st ,*dt , *ttm , *int4, *fl ; 

    
    if(!PyArg_ParseTuple(args, "ss", &database, &sql_query )) {
        return NULL;
    }


  char *poolmask  = NULL;
  char *varvalue  = NULL;
  char *queryfile = NULL;
  Bool print_mdi  = true; /* by default prints "NULL", not value of the NULL */
  Bool raw        = true;
  int maxlines = -1;
  void *h         = NULL;
  int maxcols     = 0;
  int rc          = 0;
  //double wlast;
  extern int optind;
 
 // Bool get_header=false ; 


  if (maxlines == 0) return PyLong_FromLong(rc);

  printf ("Executing query           :  %s \n",   sql_query ) ;


  //wlast = util_walltime_(); to use later for time ellapsed !

  // open the odb 
  h     = odbdump_open(database, sql_query, queryfile, poolmask, varvalue, &maxcols);
 
  if (h && maxcols > 0) {
    
    PyObject*  py_hdr    = PyList_New(maxcols) ;  
    PyObject*  py_row    = PyList_New(0) ;
    Py_ssize_t irow      = -1  ;

    printf( "Number of fetched columns : %d \n" , maxcols  )  ; 

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
    int dlen = packed ? maxcols * sizeof(*d) : maxcols;


    char nul[5] = "NULL";
    char *pnul  = nul ;

    ALLOCX(d, maxcols);
                      
    while ( (nd = nextrow(h, d, dlen, &new_dataset)) > 0) {
      Py_ssize_t i ;
      PyObject*  py_col    = PyList_New(maxcols) ;
      if (new_dataset)       {
	/* New query ? */
	//CLEAN STRUCTURES 
	ci = odbdump_destroy_colinfo(ci, nci);
	ci = odbdump_create_colinfo(h, &nci);
       new_dataset = 0;
       nrows = 0;
      
      }  /* if  NEW DATASET */


      if(raw){
	Py_ssize_t  icol= -1 ; 
	char nul[5] = "NULL";
	char *pnul  = nul ; 
	for (Py_ssize_t i=0; i<nd; i++) {
            colinfo_t *pci = &ci[i];
	  if (print_mdi && pci->dtnum != DATATYPE_STRING && ABS(d[i]) == mdi) {
	      icol++ ; 
	      st=PyUnicode_FromString(  pnul ) ; 
              PyList_SetItem ( py_col   , icol , st  ) ;
	      Py_XINCREF (st ) ;  // Could be a null pointer , use    XINCREF!
	  }
	  else {

	    switch (pci->dtnum) {

	    case DATATYPE_STRING:
	      {
		int js  ;
		char cc[sizeof(double)+1];
		char *scc = cc ;
		union {
		  char s[sizeof(double)] ;
		  double d;
		} u;
	        
                u.d = d[i];
                for (js=0; js<sizeof(double); js++) {
                  char c = u.s[js];

               *scc++ = isprint(c) ? c : '8' ; /* unprintables as blanks  REPLACE BY 8 arbitrary choice , ( juste for the col source@hdr)    */
                } 
                *scc = '\0';
		icol ++  ;  
		printf( "%s\n"  , cc ) ; 
		if (PyLong_Check( PyLong_FromLong(icol) ) ){
         		st=PyUnicode_FromFormat (cc )  ;
		     if (st  ){
			if ( PyUnicode_Check(st) ) {
		            PyList_SetItem ( py_col , icol , st  ) ;
			} else {
                   		st=PyUnicode_FromString(  pnul ) ;
			    PyList_SetItem ( py_col , icol , st ) ;
                            //Py_DECREF(py_col);
                            Py_DECREF(st);
			}

			}
		} else {  
		     PyErr_SetString ( PyExc_IndexError, "The column index may not be a 4 bytes integer"  )       ; 
		}


	      }
	      break;
         case DATATYPE_YYYYMMDD:
               icol++  ;
               if (PyLong_Check( PyLong_FromLong(icol) ) ){
                    dt= PyLong_FromLong((int)d[i])     ;
		    if (dt)  {
                                PyList_SetItem(py_col, icol , dt  ) ;
		    }
		    else if (!dt){ 
	            st=PyUnicode_FromString(  pnul ) ;
		    PyList_SetItem ( py_col , icol , st ) ;
	           // Py_DECREF(py_col);   Don't need to DECREF the reference of py_col , it will hold the 'NULL' value
                    Py_DECREF(st);
                     } 
               
                    else {
                    PyErr_SetString ( PyExc_IndexError, "The column index may not be a 4 bytes integer"  )       ;
	       
	       }
	       }
	       break;

	  case DATATYPE_HHMMSS:
                icol++  ;
               if (PyLong_Check( PyLong_FromLong(icol) ) ){
                    ttm= PyLong_FromLong((int)d[i])     ;
                    if (ttm)  {
                                PyList_SetItem(py_col, icol , ttm  ) ;
                    }
                    else if (!ttm){
                    st=PyUnicode_FromString(  pnul ) ;
                    PyList_SetItem ( py_col , icol , st ) ;
                   // Py_DECREF(py_col);   Don't need to DECREF the reference of py_col , it will hold the 'NULL' value
                    Py_DECREF(st);
                     }

                    else {
                    PyErr_SetString ( PyExc_IndexError, "The column index may not be a 4 bytes integer"  )       ;

               }
               }

               break; 
          case DATATYPE_INT4:
               icol++  ;
               if (PyLong_Check( PyLong_FromLong(icol) ) ){
                    int4= PyLong_FromLong((int)d[i])     ;
                    if (int4)  {
                       PyList_SetItem(py_col, icol , int4  ) ;
                    }
                    else if (!int4){
                    st=PyUnicode_FromString(  pnul ) ;
                    PyList_SetItem ( py_col , icol , st ) ;
                   // Py_DECREF(py_col);   Don't need to DECREF the reference of py_col , it will hold the 'NULL' value
                    Py_DECREF(st);
                     }

                    else {
                    PyErr_SetString ( PyExc_IndexError, "The column index may not be a 4 bytes integer"  )       ;

               }
               }
               break;

	  default:
              icol++  ;
              printf( "%lf\n" , d[i] ) ;
               if (PyLong_Check( PyLong_FromLong(icol) ) ){
                    fl= PyFloat_FromDouble( (double) d[i]   ) ;
                    if (fl)  {
                                PyList_SetItem(py_col, icol , fl  ) ;
                    }
                    else if (!fl){
                    st=PyUnicode_FromString(  pnul ) ;
                    PyList_SetItem ( py_col , icol , st ) ;
                   // Py_DECREF(py_col);   Don't need to DECREF the reference of py_col, it may hold a 'NULL' value
                    Py_DECREF(st);
                     }

                    else {
                    PyErr_SetString ( PyExc_IndexError, "The column index may not be a 4 bytes integer"  )       ;

               }
               }

               break;



	    } /* switch pci   - type of column */
	  }
	} 
	
       } 

      ++nrows;
      ++irow ; 
     //if ( ihd == 1 ) {
     //   if (irow == 0){          // Force the header to be in the index 0 of the returned list 
     //     PyList_Append(py_row ,   py_hdr   ) ;
    //	}
    //  }
     
     PyList_Append(py_row , py_col   ) ;
     
     //PyList_SetItem(  py_row  , irow, py_col  ) ; 
     Py_ssize_t nbcols = PyList_Size(py_col);

     // Reset list references !
     py_col=list_reset (&py_col, nbcols ) ;  
    
     //if ( nrows > 5000 )   break ; 
     if (maxlines > 0 && ++nrtot >= maxlines) break; /* while (...) */

    } 

    ci = odbdump_destroy_colinfo(ci, nci);
    rc = odbdump_close(h);

    FREEX(d);
    return    py_row   ;
    Py_DECREF ( py_row )     ;  // AVOID MEMORY GARBAGE !
    Py_Finalize() ;
  }   
  /*else {
    rc = -1;
  }*/

     Py_Finalize() ;
     return PyLong_FromLong(0) ; 
}







static PyMethodDef module_methods[] = {
    {"pyodbFetch",  (PyCFunction)(void(*)(void))   pyodbFetch_method ,
     METH_VARARGS | METH_KEYWORDS,   "odb interface "},

};


// Modules definition
static struct PyModuleDef   odbmodule = {
    PyModuleDef_HEAD_INIT,
    "pyodb",
    "Python interface for odb C ",
    -1,
    module_methods ,
};


// Called first during python call
PyMODINIT_FUNC PyInit_pyodb(void) {
    return PyModule_Create(&odbmodule);
};
