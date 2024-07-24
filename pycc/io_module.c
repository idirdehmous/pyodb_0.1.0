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
#include "iostuff.h"



/* ENDIANESS */
extern int ec_is_little_endian();
extern double util_walltime_();


static int maxhandle = 0;     /* Actual max no. of handles; increase/decrease via export ODB_MAXHANDLE */

static  const int def_maxhandle = 10;
static  DB_t      *free_handles = NULL;

PUBLIC void *
ODBc_get_free_handles(int *Maxhandle)
{  if (Maxhandle) *Maxhandle = maxhandle;  return (void *)free_handles; }



static PUBLIC   PyObject*   odbConnect_method ( PyObject *Py_UNUSED(self) , PyObject* args ,  PyObject *kwargs)  {
        //char* odbdir = NULL ;  
	char *dbname =NULL  ; 
        char *mode   =NULL  ;
        int  *npools =NULL  ;
        int  *ntables=NULL  ; 
        char *poolmask=NULL ;
 
        if(dbname  ) FREE(dbname)   ;
        if(mode    ) FREE(mode)     ;
        if(npools  ) FREE(npools )  ;
        if(ntables ) FREE(ntables)  ;
        if(poolmask) FREE(poolmask) ;

	Py_Initialize() ;
        static char *kwlist[] = {"odbdir","mode","npools","ntables","poolmask"};

       if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss|iii", 
             			    kwlist,
                                            &dbname , 
                                            &mode   ,
                                            &npools,
                                            &ntables,
                                            &poolmask ))
       {
        return NULL;
       }
  printf ( "%s\n" , dbname ) ; 
  Bool error = false;
  int handle = -1 ; 
  
  if (dbname) {
    int j;
    int Npools     = 0;
    int Ntables    = 0;
    DB_t *ph       = NULL;
    char *p_dbname = NULL;
    char *srcpath_dbname  = NULL;
    char *datapath_dbname = NULL;
    char *idxpath_dbname  = NULL;
    char *env ;
    Bool first_time = false;
    
    if (!free_handles) {
     char *  MAXH= "ODB_MAXHANDLE"   ; 
     env = getenv(MAXH );
      if (env) maxhandle = atoi(env);
      if (maxhandle <= 0) maxhandle = def_maxhandle;
      CALLOC(free_handles, maxhandle);
      
      /* Other first time setups */
      {
	extern Boolean iostuff_debug;
	iostuff_debug = false;
      }

      env = STRDUP("ODB_IO_METHOD=5"); /* Remains allocated ; cannot be free'd due to putenv() below */
      putenv(env);
      first_time = true;
    }
    
    for ( j=0; j  <maxhandle  ; j++) {
      if (free_handles[j].h == 0) {
	handle = j+1;
	ph = &free_handles[j];
	break;
      }
    }

    if (!ph) {
      
      FILE *pstd  ;
      pstd = fopen("pyodb.stderr",    "w") ;

      fprintf( pstd ,
	      "***Error: Unable to open ODB-database '%s'  : too many opened databases\n",
	      dbname);
      fprintf(pstd ,
	      "\tMax. no. of open databases currently = %d. Increase via export ODB_MAXHANDLE\n",
	      maxhandle);
      handle = -1;
      goto finish;
    }

    { /* Check if database name contains an '/' */
      char *pdot   = NULL;
      char *slash  = NULL;
      char *p = STRDUP(dbname);
      char *pdollar = strchr(p, '$');

      if (pdollar) {
	/* Resolve possible environment variable(s) in the "dbname" i.e. "p" */
	char *tmp = IOresolve_env(p);
	FREE(p);
	p = tmp;
      }

      if (strequ(p,".")) {
	/* "dbname" points to the current directory ? */
	char curpath[4096];
	FREE(p);
	p = STRDUP(getcwd(curpath, sizeof(curpath)));
      }

      {
	/* If last char(s) is/are '/', remove it/them from "dbname" i.e. "p" */
	int len = STRLEN(p);
	while (len >= 0 && p[len-1] == '/') {
	  p[len-1] = '\0';
	  --len;
	}
      }

      {
	/* Is "dbname" i.e. "p" a (schema-)file or a directory ? */
	struct stat buf;
	int    exist   = (stat(p, &buf) == -1) ? 0 : 1;
	Bool   is_dir  = (exist && S_ISDIR(buf.st_mode)) ? true : false;
	if (is_dir) {
	  /* Directory --> Append <db>.sch to "p" */
	  int len;
	  char *tmp = STRDUP(p);
	  char *db  = NULL;

	  slash     = strrchr(tmp, '/');
	  if (slash) {
	    db = STRDUP(slash+1);
	  }
	  else {
	    db = STRDUP(tmp);
	  }
	  pdot = strchr(db, '.');
	  if (pdot) *pdot = '\0';
	     const char suffix[] = ".sch";

	     len = STRLEN(tmp) + 1 + STRLEN(db) + STRLEN(suffix ) + 1;
	    
	     FREE(p);
	     ALLOC(p, len);
	     snprintf(p, len, "%s/%s%s", tmp, db, suffix);
	  FREE(tmp);
	} /* if (is_dir) */
      }

      slash = strrchr(p, '/');
      if (slash) {
	*slash = '\0';
	 p_dbname = STRDUP(slash+1);
	 pdot = strchr(p_dbname, '.');
	if (pdot) *pdot = '\0';
	   srcpath_dbname  = STRDUP(p); 
	   datapath_dbname = STRDUP(p); 
	   idxpath_dbname  = STRDUP(p);
        }
      else {
	   pdot = strchr(p, '.');
	   if (pdot) *pdot = '\0';
	   p_dbname = STRDUP(p);
      }
      FREE(p);
    }

    ph->h      = handle;
    ph->dbname = p_dbname;

    SET_PATH(srcpath  , SRCPATH ,  NULL);
    SET_PATH(datapath , DATAPATH,  NULL);
    SET_PATH(idxpath  , IDXPATH , "/idx");

    ph->srcpath  = srcpath_dbname ;
    ph->datapath = datapath_dbname;
    ph->idxpath  = idxpath_dbname ;


    if (first_time) { 
      // The following is done once only, since currently IOASSIGN is upon the first cma_open() 
      // Check existence of IOASSIGN and if not defined, then define it 
      env = getenv("IOASSIGN");
      if (!env) {
	int len = 2*strlen("IOASSIGN=") + strlen(srcpath_dbname) + 1 + strlen(p_dbname) + 1;
	ALLOC(env, len);
	snprintf(env, len, "IOASSIGN=%s/%s.IOASSIGN", srcpath_dbname, p_dbname);
	putenv(env); 
      }
      env = getenv("IOASSIGN");

      codb_init_(NULL, NULL); 
      codb_init_omp_locks_();
      codb_trace_init_(NULL); 

      // Let poolmasking know what's the maxhandle 
      codb_alloc_poolmask_(&maxhandle);
      }  //if (first_time) 

    if (poolmask) { /* Set poolmask explicitly for this database */
      char        *env = NULL;
      const char str[] = "ODB_PERMANENT_POOLMASK";
      
      int len          = STRLEN(str) + 1 + STRLEN(p_dbname) + 1 + STRLEN(poolmask) + 1;
      ALLOC(env,len);
      snprintf(env, len, "%s_%s=%s", str, p_dbname, poolmask);
      putenv(env);
      /* FREE(env); (cannot be freed due to putenv()) */ 
    } /* if (poolmask) */

    ph->tblname = NULL;

    { /* Read primary metadata (i.e. usually the $ODB_SRCPATH_<dbname>/<dbname>.dd -file) */
      int iret = 0;
      int iounit = -1;
      cma_open_(&iounit, p_dbname , "r", &iret, strlen(p_dbname), 1);
      if (iret >= 1) {
	FILE *fp = CMA_get_fp(&iounit);
	codb_read_metadata_(&handle,
			    &iounit,
			    &Npools,
			    NULL,
			    NULL,
			    NULL,
			    NULL,
			    NULL,
			    &Ntables);
	if (fp && Ntables > 0) {
	  int i;
	  char tname[4096];
	  CALLOC(ph->tblname, Ntables);
	  for (i=0; i<Ntables; i++) {
	    int id, fsize;
	    Bool found = false;
	    if (fscanf(fp,"%d %s %d",&id,tname,&fsize) == 3) {
	      char *ptbl = tname;
	      /* if (*ptbl == '@') ptbl++; */
	      if (id >= 1 && id <= Ntables) {
		ph->tblname[id-1] = STRDUP(ptbl);
		found = true;
	      }
	    }
	    if (!found) ph->tblname[i] = STRDUP("???unknown_table???");
	  } /* for (i=0; i<Ntables; i++) */
	}
      }
      cma_close_(&iounit, &iret);
    }

    /* Initialize poolmask for this database */    
    codb_init_poolmask_(&handle, p_dbname, &Npools, strlen(p_dbname));
    ph->npools  = Npools;
    ph->ntables = Ntables;
    if (npools) *npools   = Npools;
    if (ntables) *ntables = Ntables;
  }
 finish:
  // SUCCESSFULLY PARSED & CONNECTED  return 1 else 0  or -1 
  
  if (error && handle >= 1) {
       handle = ODBc_close(handle);
  return PyLong_FromLong( 1 );
  } else {
  return PyLong_FromLong( 0 );
  }
}




static PUBLIC   PyObject*   odbClose_method (PyObject *Py_UNUSED(self), PyObject *args)
     {

    int  handle          ;
    char *verbose =NULL  ;   // OPTIONAL
    Py_Initialize() ;
    if(!PyArg_ParseTuple(args, "i|i", &handle , &verbose  )) {
        return NULL;
    }

  if ( handle == 0 )  { handle =1 ;  }  ;   // Means that odbConnect returned 0 in python call (Success !)
  int  rc = -1 ;

  if (free_handles && handle >= 1 && handle <= maxhandle) {
    DB_t *ph = &free_handles[handle-1];
    if (ph->h == handle) {
      DCA_free(handle);
      codb_end_poolmask_(&handle);
      ph->h = 0;
      //FREE(ph->dbname);
      FREE(ph->srcpath);
      FREE(ph->datapath);
      if (ph->tblname && ph->ntables > 0) {
        
        for (unsigned int i=0;  (int) i< ph->ntables; i++) FREE(ph->tblname[i]);
        FREE(ph->tblname);
      }
      ph->tblname = NULL;
      if ( verbose != NULL ) {
	 printf( "--ODB %s has been closed\n.",  ph->dbname    ) ; 
         rc = 0   ; /* ok */
      }else {   rc =0 ; } ; 
      
    }
  }
  return  PyLong_FromLong(rc) ;
  Py_Finalize () ; 
  //Py_DECREF (rc) ; 

}

static PyMethodDef module_methods[] = {
    {"odbConnect",  (PyCFunction)(void(*)(void))   odbConnect_method ,
     METH_VARARGS | METH_KEYWORDS,   "Create odb connection   "},
    {"odbClose"  ,  (PyCFunction)(void(*)(void))    odbClose_method  ,
     METH_VARARGS | METH_KEYWORDS,   "Close an opened ODB "},


};


// Modules definition
static struct PyModuleDef   odbmodule = {
    PyModuleDef_HEAD_INIT,
    "pyodb_io",
    "C/Python interface for ODB I/O  connection , close !",
    -1,
    module_methods ,
     .m_slots =NULL
};


// Called first during python call
PyMODINIT_FUNC PyInit_pyodb_io (void) {
    PyObject*  m  ;
    PyObject* ModuleError ;  
    m=PyModule_Create(&odbmodule);
    if ( m == NULL) {
        ModuleError = PyErr_NewException("Failed to create the module : pyodb_io", NULL, NULL);
        Py_XINCREF(ModuleError) ;
        return NULL;
}
    return m  ; 
}

