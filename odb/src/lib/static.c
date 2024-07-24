#define STATIC_C
#include "static.h"

/* Note: Static linking is now on by default (12/2/2003 by SS) */

#ifdef DYNAMIC_LINKING
PUBLIC int ODBstatic_mode = 0;
#else
PUBLIC int ODBstatic_mode = 1;
#endif

#define BIGNUM 999999

/* Statically supported databases and their initiators */

typedef struct _static_func_t_ {
  char *dbname;
  void (*initfunc)();
  void (*flagsfilefunc)();
  struct _static_func_t_ *next;
} Static_func_t;

PRIVATE Static_func_t *f = NULL;
PRIVATE Static_func_t *flast = NULL;

PUBLIC void
ODB_add2funclist(const char *dbname, 
		 void (*func)(),
		 int ifuncno)
{
  Static_func_t *pfound = NULL;
  Static_func_t *p = f;

  for (;p;) {
    if (strequ(dbname,p->dbname)) {
      pfound = p;
      break;
    }
    p = p->next;
  }

  if (!pfound) {
    ALLOC(p, 1);
    p->dbname = STRDUP(dbname);
    p->initfunc = NULL;
    p->flagsfilefunc = NULL;
    p->next = NULL;
    if (!f) {
      f = flast = p;
    }
    else {
      flast->next = p;
      flast = flast->next;
    }
    pfound = p;
  }

  if (pfound) {
    p = pfound;
    if (ifuncno == 0) p->initfunc = func;
    else if (ifuncno == 1) p->flagsfilefunc = func;
  }
}


PRIVATE void
ODB_print_flags_file(const char *dbname,
		     int *retcode,
		     /* Hidden arguments */
		     int dbname_len)
{
  int rc = 0;
  int j = 0;
  Static_func_t *p = f;
  DECL_FTN_CHAR(dbname);
  ALLOC_FTN_CHAR(dbname);
  rc = -1;
  j = 0;
  for (;p;) {
    if (strequ(p_dbname,p->dbname)) {
      if (p->flagsfilefunc) p->flagsfilefunc();
      rc = j;
      break;
    }
    j++;
    p = p->next;
  }
  FREE_FTN_CHAR(dbname);
  *retcode = rc;
}


PUBLIC void
codb_print_flags_file_(const char *dbname,
		       const int *myproc,
		       int *retcode,
		       /* Hidden arguments */
		       int dbname_len)
{
  *retcode = BIGNUM;
  if (*myproc == 1) {
    ODB_print_flags_file(dbname, retcode, dbname_len);
  }
}


PRIVATE void
ODB_static_init(const char *dbname,
		int *retcode,
		/* Hidden arguments */
		int dbname_len)
{
  int rc = 0;
  int j = 0;
  Static_func_t *p = f;
  DECL_FTN_CHAR(dbname);
  ALLOC_FTN_CHAR(dbname);
  rc = -1;
  j = 0;
  for (;p;) {
    if (strequ(p_dbname,p->dbname)) {
      if (p->initfunc) {
	p->initfunc();
      }
      else {
	fprintf(stderr,
		"ODB_static_init(): Undefined or invalid init-function for db=\"%s\"\n",
		p_dbname);
	RAISE(SIGABRT);
      }
      rc = j;
      break;
    }
    j++;
    p = p->next;
  }
  FREE_FTN_CHAR(dbname);
  *retcode = rc;
}


PUBLIC void
codb_static_init_(const char *a_out,
		  const char *dbname,
		  const int *myproc,
		  int *retcode,
		  /* Hidden arguments */
		  int a_out_len,
		  int dbname_len)
{
  char *env = getenv("ODB_STATIC_LINKING");
  boolean is_static = env ? atoi(env) : ODBstatic_mode;
  static boolean first_time = 1;
  DECL_FTN_CHAR(dbname);
  ALLOC_FTN_CHAR(dbname);

  *retcode = BIGNUM;
  if (is_static) { /* static mode */
    if (first_time && *myproc == 1) {
      fprintf(stderr,">>> Assuming static linking mode\n");
    }
    {
      char *p = p_dbname;
      int plen = dbname_len;
      while (plen > 0) { /* strip leading blanks */
	if (*p != ' ') break;
	p++; plen--;
      }
      codb_set_entrypoint_(p, plen);
      ODB_static_init(p, retcode, plen);
    }
  }
  else { /* dynamic mode */
    if (first_time && *myproc == 1) {
      fprintf(stderr,">>> Assuming dynamic linking mode\n");
    }
  }
  first_time = 0;

  ODBstatic_mode = is_static;
  FREE_FTN_CHAR(dbname);
}
