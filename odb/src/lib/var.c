
/* var.c */

#include "odb.h"
#include "cdrhook.h"

/* Routines to maintain SET-variables via hashing */

/*--- Defines functions: (per database, and (possibly) per view)

  * Add/register variable
  * Change the value of variable
  * Get the current address of variable
  * Get the current value of variable
  * Delete variables
  * Print variables (debug)
  * Transfer list of variables back to calling program

  ---*/

#ifndef VAR_HASHSIZE
#define VAR_HASHSIZE 1031U
#endif

PRIVATE o_lock_t VAR_mylock = 0; /* A specific OMP-lock; initialized only once in
				    odb/lib/codb.c, routine codb_init_omp_locks_() */

PRIVATE uint VAR_hashsize = 0;

typedef struct _VAR_Hash_Table {
  char *dbname;
  char *symbol;
  char *viewname;
  double value;
  struct _VAR_Hash_Table *collision;
} VAR_Hash_Table;

PRIVATE VAR_Hash_Table **VAR_hashtable = NULL;

PRIVATE uint
Hash(const char *dbname, 
     const char *symbol, 
     const char *viewname)
{ 
  uint hashval = 0;
  for (; *dbname ; dbname++) {
    hashval = (*dbname) + 31U * hashval;
  }
  for (; *symbol ; symbol++) {
    hashval = (*symbol) + 31U * hashval;
  }
  if (viewname) {
    for (; *viewname ; viewname++) {
      hashval = (*viewname) + 31U * hashval;
    }
  }
  hashval = hashval % VAR_hashsize;
  return hashval;
}


PRIVATE void
HashInit()
{
  if (!VAR_hashtable) {
    coml_set_lockid_(&VAR_mylock);
    if (!VAR_hashtable) {
      VAR_Hash_Table **tmp_VAR_hashtable = NULL;
      int inumt = get_max_threads_();
      int it;
      char *p = getenv("ODB_VAR_HASHSIZE");
      if (p) {
	VAR_hashsize = atoi(p);
	if (VAR_hashsize <= 0) VAR_hashsize = VAR_HASHSIZE;
      }
      else
	VAR_hashsize = VAR_HASHSIZE;
      ALLOC(tmp_VAR_hashtable, inumt + 1); /* entries 0 for TABLEs, 1..inumt for VIEWs */
      for (it = 0; it <= inumt ; it++) {
	CALLOC(tmp_VAR_hashtable[it], VAR_hashsize);
      }
      VAR_hashtable = tmp_VAR_hashtable;
    }
    coml_unset_lockid_(&VAR_mylock);
  }
}

PRIVATE VAR_Hash_Table *
NewEntry(void)
{
  VAR_Hash_Table *p;
  ALLOC(p, 1);
  p->dbname = NULL;
  p->symbol = NULL;
  p->viewname = NULL;
  p->value = 0;
  p->collision = NULL;
  return p;
}

PUBLIC void
init_VAR_lock()
{
  INIT_LOCKID_WITH_NAME(&VAR_mylock,"var.c:VAR_mylock");
  HashInit();
}

/*--- Add/register variable ---*/

PUBLIC void
ODB_add_var(const char *dbname, 
            const char *symbol, 
            const char *viewname,
	    int it,
            double value)
{
  int found = 0;
  uint index;
  VAR_Hash_Table *p;
  DRHOOK_START(ODB_add_var);

  if (!dbname || !symbol) goto finish;

  if (!viewname) it = 0; /* make sure TABLEs are "non-threaded" */

  index = Hash(dbname,symbol,viewname);
  p = &VAR_hashtable[it][index];

  found = 0;
  while (p && p->symbol) {
    if (strequ(p->symbol, symbol) && strequ(p->dbname, dbname) &&
	(!viewname || strequ(p->viewname, viewname))) {
      p->value = value;
      ++found;
      break;
    }

    if (p->collision) 
      p = p->collision;
    else {
      p->collision = NewEntry();
      p = p->collision;
      break;
    }
  } /* while (p && p->symbol) */

  if (!found && p) {
    FILE *do_trace = ODB_trace_fp();
    p->dbname = STRDUP(dbname);
    p->symbol = STRDUP(symbol);
    p->viewname = viewname ? STRDUP(viewname) : NULL;
    p->value = value;
    if (do_trace) {
      ODB_Trace TracE;
      TracE.handle = -1;
      TracE.msglen = 512;
      TracE.msg = NULL;
      ALLOC(TracE.msg, TracE.msglen);
      TracE.numargs = 0; 
      TracE.mode = -1;
      if (p->viewname) {
	snprintf(TracE.msg, TracE.msglen,
		"Added '%s', value=%.14g, db=%s, [%s], it=%d : hash#%d",
		p->symbol, p->value,
		p->dbname, p->viewname, it, index);
      }
      else {
	snprintf(TracE.msg, TracE.msglen,
		"Added '%s', value=%.14g, db=%s, it=%d : hash#%d",
		p->symbol, p->value,
		p->dbname, it, index);
      }

      codb_trace_(&TracE.handle, &TracE.mode,
		  TracE.msg, TracE.args, 
		  &TracE.numargs, TracE.msglen);
      FREE(TracE.msg);
    } /* if (do_trace) */
  }

 finish:
  DRHOOK_END(0);
}

/*--- Change the value of variable ---*/

PUBLIC double *
ODB_alter_var(const char   *dbname, 
	      const char   *symbol,
	      const char   *viewname,
	      int it,
	      const double *newvalue,
	      double       *oldvalue,
	      int           recur)
{
  double *addr = NULL;
  uint index;
  VAR_Hash_Table *p;
  FILE *do_trace = ODB_trace_fp();
  ODB_Trace TracE;
  DRHOOK_START(ODB_alter_var);

  if (do_trace) {
    TracE.handle = -1;
    TracE.msglen = 512;
    TracE.msg = NULL;
    ALLOC(TracE.msg, TracE.msglen);
    TracE.numargs = 0; 
    TracE.mode = -1;
  }

  if (!dbname || !symbol) goto finish;

  if (!viewname) it = 0; /* make sure TABLEs are "non-threaded" */

  index = Hash(dbname,symbol,viewname);
  p = &VAR_hashtable[it][index];

  while (p && p->symbol) {
    if (strequ(p->symbol, symbol) && strequ(p->dbname, dbname) &&
	(!viewname || strequ(p->viewname, viewname))) {
      if (oldvalue) *oldvalue = p->value;
      if (newvalue) {
	if (do_trace) {
	  if (p->viewname) {
	    snprintf(TracE.msg, TracE.msglen,
		    "'%s', db=%s, (old,new)=(%.14g,%.14g) [%s] it=%d : hash#%d",
		    p->symbol, p->dbname, 
		    p->value, *newvalue,
		    p->viewname, it, index);
	  }
	  else {
	    snprintf(TracE.msg, TracE.msglen,
		    "'%s', db=%s, (old,new)=(%.14g,%.14g) it=%d : hash#%d",
		    p->symbol, p->dbname, 
		    p->value, *newvalue,
		    it, index);
	  }
	  codb_trace_(&TracE.handle, &TracE.mode,
		      TracE.msg, TracE.args, 
		      &TracE.numargs, TracE.msglen);
	} /* if (do_trace) */
	p->value = *newvalue;
      }
      if (!addr) {
	if (!viewname) {
	  addr = &p->value;
	}
	else if (strequ(p->viewname, viewname)) {
	  addr = &p->value;
	}
	/* One appropriately found address is enough
	   No coherence across views (with the same variable name) is needed */
	if (addr) break;
      }
    } /* if (strequ(p->symbol, symbol) ... */
    p = p->collision;
  } /* while (p && p->symbol) */

  if (!addr && *symbol == '$' && recur == 0) {
    /* still no address ? perhaps a completely new variable ! */
    /* note: new symbol will be added only for symbols starting with '$' */
    coml_set_lockid_(&VAR_mylock);
    if (viewname) {
	   ODB_add_var(dbname, symbol, viewname, it, *newvalue);
    }
    else {
      ODB_add_var(dbname, symbol, NULL, 0, *newvalue);
    }
    addr = ODB_alter_var(dbname, symbol, viewname, it, newvalue, oldvalue, ++recur);
    coml_unset_lockid_(&VAR_mylock);
  } /* if (!addr && *symbol == '$' && recur == 0) */

 finish:
  if (do_trace) FREE(TracE.msg);

  DRHOOK_END(0);
  return addr;
}

/*--- Get the current address of a variable ---*/

PUBLIC double *
ODB_getaddr_var(const char   *dbname, 
		const char   *symbol,
		const char   *viewname,
		int it)
{
  double *addr = NULL;
  uint index;
  VAR_Hash_Table *p;
  DRHOOK_START(ODB_getaddr_var);

  if (!dbname || !symbol) goto finish;

  if (!viewname) it = 0; /* make sure TABLEs are "non-threaded" */

  index = Hash(dbname,symbol,viewname);
  p = &VAR_hashtable[it][index];

  while (p && p->symbol) {
    if (strequ(p->symbol, symbol) && strequ(p->dbname, dbname) &&
	(!viewname || strequ(p->viewname, viewname))) {
      if (!addr) {
	if (!viewname) {
	  addr = &p->value;
	}
	else if (strequ(p->viewname, viewname)) {
	  addr = &p->value;
	}
      }
      /* One appropriately found address is enough. 
         No coherence is needed across the views (over the same symbol name) */
      if (addr) break;
    }
    p = p->collision;
  } /* while (p && p->symbol) */

 finish:
  DRHOOK_END(0);
  return addr;
}

/*--- Get the current value of variable ---*/

PUBLIC double
ODB_getval_var(const char   *dbname, 
	       const char   *symbol,
	       const char   *viewname,
	       int it)
{
  double value = 0; /* Note: the default value is zero */
  double *addr = ODB_getaddr_var(dbname, symbol, viewname, it);
  if (addr) value = *addr;
  return value;
}


/*--- Delete variables ---*/

PUBLIC void
ODB_del_vars(const char *dbname,
	     const char *viewname,
	     int it)
{
  if (!dbname) return;

  if (VAR_hashtable) {
    int j;
    if (!viewname) it = 0; /* make sure TABLEs are "non-threaded" */
    for (j=0; j<VAR_hashsize; j++) {
      VAR_Hash_Table *p = &VAR_hashtable[it][j];
      while (p && p->symbol) {
	if (strequ(p->dbname, dbname) &&
	    (!viewname || strequ(p->viewname, viewname))) {
	  p->value = 0;
	}
	p = p->collision;
      } /* while (p && p->symbol) */
    } /* for (j=0; j<VAR_hashsize; j++) */
  } /* if (VAR_hashtable) */
}

/*--- Print variables (debug) ---*/

/* Can be called from Fortran, too */

PUBLIC void
codb_print_vars_(const int *enforce,
		 const char *msg,
		 /* Hidden arguments */
		 int msg_len)
{
  DECL_FTN_CHAR(msg);
  int print_it = *enforce;
  int j;
  int entries=0;
  int hits=0;
  char *env = getenv("ODB_VAR_DEBUG");
  
  if (env) {
    int ienv = atoi(env);
    ienv = MAX(0,MIN(1,ienv));
    print_it |= ienv;
  }

  if (!print_it) return;

  ALLOC_FTN_CHAR(msg);

  if (msg_len > 0) fprintf(stderr,"codb_print_vars_: %s\n",p_msg);
  fprintf(stderr,
	  "codb_print_vars_: hashsize=%u, hashtable at %p\n",
	  VAR_hashsize, VAR_hashtable);

  if (VAR_hashtable) {
    int inumt = get_max_threads_();
    int it;
    for (it = 0; it <= inumt ; it++) {
      for (j=0; j<VAR_hashsize; j++) {
	VAR_Hash_Table *p = &VAR_hashtable[it][j];
	int k=1;
	if (p->symbol) entries++;
	while (p && p->symbol) {
	  fprintf(stderr,
		  "hash#%d [hit#%d] : db='%s', symbol='%s', view='%s', it=%d : value=%.14g (at %p)\n",
		  j,k,
		  p->dbname ? p->dbname : NIL, 
		  p->symbol ? p->symbol : NIL, 
		  p->viewname ? p->viewname : NIL,
		  it,
		  p->value,
		  &p->value);
	  p = p->collision;
	  k++;
	  hits++;
	} /* while (p) */
      } /* for (j=0; j<VAR_hashsize; j++) */
    } /* for (it = 0; it <= inumt ; it++) */
  } /* if (VAR_hashtable) */

  fprintf(stderr,"codb_print_vars_: Total %d hits in %d hash-entries\n",hits,entries);

  FREE_FTN_CHAR(msg);
}

/*--- Transfer variables and their values back to calling program ---*/

PUBLIC int
ODB_get_vars(const char *dbname,
	     const char *viewname,
	     int it,
	     int nsetvar,
	     ODB_Setvar setvar[])
{
  int count = 0;

  if (VAR_hashtable) {
    int j;
    if (!viewname) it = 0; /* make sure TABLEs are "non-threaded" */
    for (j=0; j<VAR_hashsize; j++) {
      VAR_Hash_Table *p = &VAR_hashtable[it][j];
      while (p && p->symbol) {
	if ((!dbname || strequ(dbname,p->dbname)) &&
	    (!viewname || strequ(viewname, p->viewname))) {
	  /*
	  fprintf(stderr,"ODB_get_vars(dbname='%s', nsetvar=%d): p->symbol='%s', value=%g\n",
		  dbname, nsetvar, p->symbol, p->value);
		  */
	  if (setvar && count < nsetvar) {
	    setvar[count].symbol = STRDUP(p->symbol);
	    setvar[count].value = p->value;
	  }
	  count++;
	}
	p = p->collision;
      } /* while (p) */
    } /* for (j=0; j<VAR_hashsize; j++) */
  } /* if (VAR_hashtable) */

  return count;
}
