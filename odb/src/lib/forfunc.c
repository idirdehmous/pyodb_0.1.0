
/* forfunc.c */

#include "odb.h"

/* Routines to ensure faster access to triplet (handle, poolno, "dataname") */

/*--- Defines functions: "DBNAME" there only for diagnostix

  Registration: [returns hash index]
  put_forfunc(funcs, handle, "DBNAME", poolno, it, "dataname");

  Get funcs pointer: do abort, if not found
  funcs = get_forfunc(handle, "DBNAME", poolno, it, "dataname", 1);

  Get funcs pointer: do *not* abort, if not found
  funcs = get_forfunc(handle, "DBNAME", poolno, it, "dataname", 0);

  Disable particular funcs at triplet: [returns nothing; may become active again (dyn.linking)]
  nullify_forfunc(handle, "DBNAME",poolno, it, "dataname");
  
  Print contents of hash-tables: (is also Fortran-callable)
  integer enforce
  character(len=*),parameter :: msg = 'Print message'
  enforce = 1
  CALL print_forfunc(enforce, msg)

  ---*/

#ifndef FORFUNC_HASHSIZE
#define FORFUNC_HASHSIZE 1021U
#endif

PRIVATE uint FORFUNC_hashsize = 0;

typedef struct _FORFUNC_Hash_Table {
  int handle;
  int poolno;
  char *dataname;
  ODB_Funcs *key;
  struct _FORFUNC_Hash_Table *collision;
} FORFUNC_Hash_Table;

PRIVATE FORFUNC_Hash_Table **FORFUNC_hashtable = NULL;

PRIVATE uint
Hash(int handle, int poolno, const char *s)
{ 
  uint hashval = 100 * poolno + handle; /* handle typically << 100 */
  for (; *s ; s++) {
    hashval = (*s) + 31U * hashval;
  }
  hashval = hashval % FORFUNC_hashsize;
  return hashval;
}


PRIVATE void
HashInit()
{
  if (!FORFUNC_hashtable) {
    FORFUNC_Hash_Table **tmp_FORFUNC_hashtable = NULL;
    int inumt = get_max_threads_();
    int it;
    char *p = getenv("ODB_FORFUNC_HASHSIZE");
    if (p) {
      FORFUNC_hashsize = atoi(p);
      if (FORFUNC_hashsize <= 0) FORFUNC_hashsize = FORFUNC_HASHSIZE;
    }
    else
      FORFUNC_hashsize = FORFUNC_HASHSIZE;
    ALLOC(tmp_FORFUNC_hashtable, inumt + 1); /* entries 0 for TABLEs, 1..inumt for VIEWs */
    for (it = 0; it <= inumt ; it++) {
      CALLOC(tmp_FORFUNC_hashtable[it], FORFUNC_hashsize);
    }
    FORFUNC_hashtable = tmp_FORFUNC_hashtable;
  }
}

PRIVATE FORFUNC_Hash_Table *
NewEntry(void)
{
  FORFUNC_Hash_Table *p;
  ALLOC(p, 1);
  p->handle    = 0;
  p->poolno    = 0;
  p->dataname  = NULL;
  p->key       = NULL;
  p->collision = NULL;
  return p;
}

PUBLIC void
init_FORFUNC_lock()
{
  HashInit();
}

/*---
  Registration: [returns hash index]
  put_forfunc(funcs, handle, "DBNAME", poolno, it, "dataname");
  ---*/

PUBLIC int
put_forfunc(ODB_Funcs *funcs,
            int handle,
	    const char *dbname,
            int poolno, 
	    int it,
            const char *dataname)
{
  int rc = 0;
  uint index;
  FORFUNC_Hash_Table *p;

  rc = index = Hash(handle, poolno, dataname);
  if (IS_TABLE(dataname)) it = 0;
  p = &FORFUNC_hashtable[it][index];

  while (p && p->dataname) {
    if (p->handle == handle &&
	p->poolno == poolno &&
	strequ(p->dataname, dataname)) {
      extern int ODBstatic_mode;
      if (p->key && ODBstatic_mode) {
	/* Do not allow reassignment in static mode; be more relaxed in dynamic one */
	if (p->key != funcs) {
	  fprintf(stderr,
		  "put_forfunc: (handle,dbname,poolno,it,dataname)="
		  "(%d,%s,%d,%d,'%s') already assigned to address=%p, funcs=%p\n",
		  p->handle, dbname, p->poolno, it, p->dataname, p->key, funcs);
	  rc = -1;
	  RAISE(SIGABRT);
	}
	goto finish; /* Already in list */
      }
      else {
	/* otherwise: Re-use this entry (had been disabled) */
	FREE(p->dataname);
	break;
      }
    }
    if (!p->collision) p->collision = NewEntry();
    p = p->collision;
  } /* while (p && p->dataname) */

  p->handle   = handle;
  p->poolno   = poolno;
  p->dataname = STRDUP(dataname);
  p->key      = funcs;

 finish:
  return rc;
}


/*---
  Get funcs pointer:
  funcs = get_forfunc(handle, "DBNAME", poolno, it, "dataname", do_abort);
  ---*/


PUBLIC ODB_Funcs *
get_forfunc(int handle,
	    const char *dbname,
            int poolno, 
            int it, 
            const char *dataname,
	    int do_abort)
{
  ODB_Funcs *retfunc = NULL;
  uint index;
  FORFUNC_Hash_Table *p;

  index = Hash(handle, poolno, dataname);
  if (IS_TABLE(dataname)) it = 0;
  p = &FORFUNC_hashtable[it][index];

  if (!p->dataname) {
    if (do_abort) {
      fprintf(stderr,
	      "get_forfunc: Unregistered (handle,dbname,poolno,it,dataname)="
	      "(%d,%s,%d,%d,'%s')\n",
	      handle,dbname,poolno,it,dataname);
      RAISE(SIGABRT);
    }
  }
  else {
#ifndef NECSX
    while (p && p->dataname) {
#else
    while (p!=NULL && p->dataname!=NULL) {
#endif
      if (p->handle == handle &&
	  p->poolno == poolno &&
#ifndef NECSX
	  strequ(p->dataname, dataname)) {
#else
          /* strequ(p->dataname, dataname) */
          p->dataname!=0 &&
          dataname!=0 &&
          strcmp(p->dataname,dataname) == 0
                                         ) {
#endif
	if (!p->key) {
	  if (do_abort) {
	    fprintf(stderr,
		    "get_forfunc: In (handle,dbname,poolno,it,dataname)="
		    "(%d,%s,%d,%d,'%s') key-address was NULL\n",
		    handle,dbname,poolno,it,dataname);
	    RAISE(SIGABRT);
	  }
	}
	else {
	  /* Valid and found */
	  retfunc = p->key;
	}
	break;
      }
      if (p->collision) 
	p = p->collision;
      else {
	if (do_abort) {
	  fprintf(stderr,
		  "get_forfunc: (handle,dbname,poolno,it,dataname)="
		  "(%d,%s,%d,%d,'%s') unregistered\n",
		  handle,dbname,poolno,it,dataname);
	  RAISE(SIGABRT);
	}
	break;
      }
    } /* while (p && p->dataname) */
  }

  return retfunc;
}

/*---
  Disable particular funcs at triplet: [returns nothing; may become active again (dyn.linking)]
  nullify_forfunc(handle, "DBNAME", poolno, it, "dataname");
  ---*/

PUBLIC void
nullify_forfunc(int handle,
		const char *dbname,
		int poolno, 
		int it,
		const char *dataname)
{
  uint index;
  FORFUNC_Hash_Table *p;

  index = Hash(handle, poolno, dataname);
  if (IS_TABLE(dataname)) it = 0;
  p = &FORFUNC_hashtable[it][index];

  while (p && p->dataname) {
    if (p->handle == handle &&
	p->poolno == poolno &&
	strequ(p->dataname, dataname)) {
      p->key = NULL; /* Disable (this entry should be the only one) */
      break;
    }
    if (p->collision) 
      p = p->collision;
    else {
      break;
    }
  } /* while (p && p->dataname) */
}

/*---
  Print contents of hash-tables: (is also Fortran-callable)
  CALL print_forfunc(enforce, msg)
  print_forfunc_(&enforce, msg, strlen(msg));
  ---*/

PUBLIC void
print_forfunc_(const int *enforce,
	       const char *msg,
	       /* Hidden arguments */
	       int msg_len)
{
  DECL_FTN_CHAR(msg);
  int print_it = *enforce;
  int j;
  int entries=0;
  int hits=0;
  char *env = getenv("ODB_FORFUNC_DEBUG");
  
  if (env) {
    int ienv = atoi(env);
    ienv = MAX(0,MIN(1,ienv));
    print_it |= ienv;
  }

  if (!print_it) return;

  ALLOC_FTN_CHAR(msg);

  if (msg_len > 0) fprintf(stderr,"print_forfunc_: %s\n",p_msg);
  fprintf(stderr,
	  "print_forfunc_: hashsize=%u, hashtable at %p\n",
	  FORFUNC_hashsize, FORFUNC_hashtable);

  if (FORFUNC_hashtable) {
    int inumt = get_max_threads_();
    int it;
    for (it = 0; it <= inumt ; it++) {
      for (j=0; j<FORFUNC_hashsize; j++) {
	FORFUNC_Hash_Table *p = &FORFUNC_hashtable[it][j];
	int k=1;
	if (p->dataname) entries++;
	while (p && p->dataname) {
	  fprintf(stderr,
		  "hash#%d [hit#%d] : (hdle=%d,pool=%d,it=%d,'%s') maps to address=%p\n",
		  j,k,
		  p->handle, 
		  p->poolno, 
		  it, 
		  p->dataname ? p->dataname : NIL, 
		  p->key);
	  p = p->collision;
	  k++;
	  hits++;
	} /* while (p) */
      } /* for (j=0; j<FORFUNC_hashsize; j++) */
    } /* if (FORFUNC_hashtable) */
  }

  fprintf(stderr,"print_forfunc_: Total %d hits in %d hash-entries\n",hits,entries);

  FREE_FTN_CHAR(msg);
}
