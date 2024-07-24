
/* generic.c */

#include "odb.h"
#include "odb_macros.h"
#include "dca.h"
#include "evaluate.h"
#include "info.h"
#include "iostuff.h"
#include "cdrhook.h"
#include "magicwords.h"
#include "bits.h"
#include "idx.h"
#include <sys/types.h>
#include <sys/stat.h>

static int maxhandle = 0;     /* Actual max no. of handles; increase/decrease via export ODB_MAXHANDLE */

static  const int def_maxhandle = 10;
static  DB_t      *free_handles = NULL;

PUBLIC void *
ODBc_get_free_handles(int *Maxhandle)
{
  if (Maxhandle) *Maxhandle = maxhandle;
  return (void *)free_handles;
}


#define SET_PATH(x,X,subdir) \
{ \
  char *add_subdir_if_not_defined = subdir; \
  char *x = getenv("ODB_" #X); \
  int len; \
  char *xdb = NULL; \
  len = strlen("ODB_" #X) + 1 + strlen(p_dbname) + 1; \
  ALLOC(xdb, len); \
  snprintf(xdb,len,"ODB_" #X "_%s",p_dbname); \
  env = getenv(xdb); \
  FREE(xdb); \
  if (!env) env = x; \
  if (env) { \
    xdb = STRDUP(env); \
    add_subdir_if_not_defined = NULL; \
  } \
  else if (!x##_dbname) { \
    char curpath[4096]; \
    xdb = STRDUP(getcwd(curpath, sizeof(curpath))); \
  } \
  if (xdb) { FREE(x##_dbname); x##_dbname = STRDUP(xdb); } \
  FREE(xdb); \
  len = strlen("ODB_" #X) + 1 + strlen(p_dbname) + 1 + strlen(x##_dbname) + 1; \
  if (add_subdir_if_not_defined) len += strlen(add_subdir_if_not_defined); \
  if (add_subdir_if_not_defined) len += strlen(add_subdir_if_not_defined); \
  ALLOC(env, len); /* Remains allocated ; cannot be free'd */ \
  snprintf(env,len,"ODB_" #X "_%s=%s%s",p_dbname,x##_dbname,\
           add_subdir_if_not_defined?add_subdir_if_not_defined:""); \
  putenv(env); \
  { \
    char *penv = STRDUP(env); \
    char *eq = strchr(penv,'='); \
    if (eq) { \
      *eq = '\0'; \
      env = getenv(penv); \
      fprintf(stderr,"%s=%s\n",penv,env?env:NIL); \
      if (env) { FREE(x##_dbname); x##_dbname = STRDUP(env); } \
    } \
    FREE(penv); \
  } \
}


PUBLIC int 
ODBc_open(const char *dbname, const char *mode, int *npools, int *ntables, const char *poolmask)
{
  Bool on_error = false;
  int handle = -1;
  DRHOOK_START(ODBc_open);
  if (dbname) {
    int j;
    int Npools = 0;
    int Ntables = 0;
    DB_t *ph = NULL;
    char *p_dbname = NULL;
    char *srcpath_dbname = NULL;
    char *datapath_dbname = NULL;
    char *idxpath_dbname = NULL;
    char *env;
    Bool first_time = false;
    
    if (!free_handles) {
      env = getenv("ODB_MAXHANDLE");
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
    
    for (j=0; j<maxhandle; j++) {
      if (free_handles[j].h == 0) {
	handle = j+1;
	ph = &free_handles[j];
	break;
      }
    }

    if (!ph) {
      fprintf(stderr,
	      "***Error: Unable to open ODB-database '%s' : too many opened databases\n",
	      dbname);
      fprintf(stderr,
	      "\tMax. no. of open databases currently = %d. Increase via export ODB_MAXHANDLE\n",
	      maxhandle);
      handle = -1;
      goto finish;
    }

    { /* Check if database name contains an '/' */
      char *pdot = NULL;
      char *slash = NULL;
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
	int exist = (stat(p, &buf) == -1) ? 0 : 1;
	Bool is_dir = (exist && S_ISDIR(buf.st_mode)) ? true : false;
	if (is_dir) {
	  /* Directory --> Append <db>.sch to "p" */
	  int len;
	  char *tmp = STRDUP(p);
	  char *db = NULL;
	  const char suffix[] = ".sch";
	  slash = strrchr(tmp, '/');
	  if (slash) {
	    db = STRDUP(slash+1);
	  }
	  else {
	    db = STRDUP(tmp);
	  }
	  pdot = strchr(db, '.');
	  if (pdot) *pdot = '\0';
	  len = STRLEN(tmp) + 1 + STRLEN(db) + STRLEN(suffix) + 1;
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
	srcpath_dbname = STRDUP(p); 
	datapath_dbname = STRDUP(p); 
	idxpath_dbname = STRDUP(p);
      }
      else {
	pdot = strchr(p, '.');
	if (pdot) *pdot = '\0';
	p_dbname = STRDUP(p);
      }
      FREE(p);
    }

    ph->h = handle;
    ph->dbname = p_dbname;

    SET_PATH(srcpath, SRCPATH, NULL);
    SET_PATH(datapath, DATAPATH, NULL);
    SET_PATH(idxpath, IDXPATH, "/idx");

    ph->srcpath = srcpath_dbname;
    ph->datapath = datapath_dbname;
    ph->idxpath = idxpath_dbname;

    if (first_time) { 
      /* The following is done once only, since currently IOASSIGN is upon the first cma_open() */

      /* Check existence of IOASSIGN and if not defined, then define it */
      env = getenv("IOASSIGN");
      if (!env) {
	int len = 2*strlen("IOASSIGN=") + strlen(srcpath_dbname) + 1 + strlen(p_dbname) + 1;
	ALLOC(env, len);
	snprintf(env, len, "IOASSIGN=%s/%s.IOASSIGN", srcpath_dbname, p_dbname);
	putenv(env); /* Remains allocated ; cannot be free'd */
      }
      env = getenv("IOASSIGN");

      codb_init_(NULL, NULL); /* lib/errtarp.c : the same as myproc=1, nproc=1 */
      codb_init_omp_locks_(); /* lib/codb.c    */
      codb_trace_init_(NULL); /* lib/tracing.g */

      /* Let poolmasking know what's the maxhandle */
      codb_alloc_poolmask_(&maxhandle);
    } /* if (first_time) */

    if (poolmask) { /* Set poolmask explicitly for this database */
      char *env = NULL;
      const char str[] = "ODB_PERMANENT_POOLMASK";
      int len = STRLEN(str) + 1 + STRLEN(p_dbname) + 1 + STRLEN(poolmask) + 1;
      ALLOC(env,len);
      snprintf(env, len, "%s_%s=%s", str, p_dbname, poolmask);
      putenv(env);
      /* FREE(env); (cannot be freed due to putenv()) */ 
    } /* if (poolmask) */

    ph->tblname = NULL;

    { /* Read primary metadata (i.e. usually the $ODB_SRCPATH_<dbname>/<dbname>.dd -file) */
      int iret = 0;
      int iounit = -1;
      cma_open_(&iounit, p_dbname, "r", &iret, strlen(p_dbname), 1);
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

    { /* Check whether $ODB_SRCPATH_<dbname>/dca -directory exists;
	 if not, try to run "dcagen"-tool from within this code ;
         Note: the srcpath must be writable by you !! */
      if (access(ph->srcpath, W_OK | R_OK | X_OK) == 0) {
	struct stat buf;
	int exist;
	Bool is_dir;
	char *dcadir = NULL;
	int len = STRLEN(ph->srcpath) + STRLEN("/dca") + 1;
	ALLOCX(dcadir, len);
	snprintf(dcadir, len, "%s/dca", ph->srcpath);
	exist = (stat(dcadir, &buf) == -1) ? 0 : 1;
	is_dir = (exist && S_ISDIR(buf.st_mode)) ? true : false;
	if (!is_dir) {
	  char *binpath = getenv("ODB_FEBINPATH");
	  if (binpath) {
	    const char dcagen[] = "dcagen";
	    char *dcagen_cmd = NULL;
	    len = STRLEN(binpath) + 1 + STRLEN(dcagen) + 1;
	    ALLOCX(dcagen_cmd, len);
	    snprintf(dcagen_cmd, len, "%s/%s", binpath, dcagen);
	    if (access(dcagen_cmd, X_OK | R_OK) == 0) {
	      int iret = 0;
	      extern Bool odbdump_on;
	      const char *dcagen_fmt = 
		odbdump_on ? "%s -i %s -F -n -q -N 1 -z -E pyodb.stderr" : "%s -i %s -F -n -q -N 1 -z";
	      char *cmd = NULL;
	      len = STRLEN(dcagen_fmt) + STRLEN(dcagen_cmd) + STRLEN(ph->srcpath) + 1;
	      ALLOCX(cmd, len);
	      snprintf(cmd, len, dcagen_fmt, dcagen_cmd, ph->srcpath);
	      fprintf(stderr,"***Warning: Generating dca-indices into directory '%s' ...\n", dcadir);
	      fprintf(stderr,"***Warning: The command used is : %s\n", cmd);
	      if (odbdump_on) fflush(stderr);
	      iret = system(cmd);
	      if (iret != 0) {
		fprintf(stderr,
			"***Error [iret=%d]: Unable to create dca-indices !!\n",
			iret);
		on_error = true;
	      }
	      FREEX(cmd);
	    }
	    FREEX(dcagen_cmd);
	  } /* if (binpath) */
	} /* if (!is_dir) */
	FREEX(dcadir);
      } /* if (access(ph->srcpath, ... ) */
    }

    /* Initialize poolmask for this database */
    
    codb_init_poolmask_(&handle, p_dbname, &Npools, strlen(p_dbname));

    ph->npools = Npools;
    ph->ntables = Ntables;

    if (npools) *npools = Npools;
    if (ntables) *ntables = Ntables;
  }
 finish:
  if (on_error && handle >= 1) handle = ODBc_close(handle);
  DRHOOK_END(0);
  return handle;
}


PUBLIC int
ODBc_get_handle(const char *db)
{
  int handle = -1;
  DRHOOK_START(ODBc_get_handle);
  if (free_handles) {
    int j;
    for (j=0; j<maxhandle; j++) {
      DB_t *ph = &free_handles[j];
      if (ph->h > 0 && (!db || strequ(db,ph->dbname))) {
	handle = ph->h;
	break; /* for (j=0; j<maxhandle; j++) */
      }
    }
  }
  DRHOOK_END(0);
  return handle;
}


PUBLIC int
ODBc_close(int handle)
{
  int rc = -1;
  DRHOOK_START(ODBc_close);
  if (free_handles && handle >= 1 && handle <= maxhandle) {
    DB_t *ph = &free_handles[handle-1];
    if (ph->h == handle) {
      DCA_free(handle);
      codb_end_poolmask_(&handle);
      ph->h = 0;
      FREE(ph->dbname);
      FREE(ph->srcpath);
      FREE(ph->datapath);
      if (ph->tblname && ph->ntables > 0) {
	int i;
	for (i=0; i<ph->ntables; i++) FREE(ph->tblname[i]);
	FREE(ph->tblname);
      }
      ph->tblname = NULL;
      rc = 0; /* ok */
    }
  }
  DRHOOK_END(0);
  return rc;
}


PUBLIC const char **
ODBc_get_tablenames(int handle, int *ntables)
{
  const char **rc = NULL;
  DRHOOK_START(ODBc_get_tablenames);
  if (ntables) *ntables = 0;
  if (free_handles && handle >= 1 && handle <= maxhandle) {
    DB_t *ph = &free_handles[handle-1];
    if (ph->h == handle) {
      if (ntables) *ntables = ph->ntables;
      if (ph->tblname) rc = (const char **)ph->tblname;
    }
  }
  DRHOOK_END(0);
  return rc;
}


PUBLIC void *
ODBc_sql_prepare_via_sqlfile(int handle, const char *sql_query_file, const set_t *setvar, int nsetvar)
{
  void *Info = NULL;
  DRHOOK_START(ODBc_sql_prepare_via_sqlfile);
  if (sql_query_file) {
    if (free_handles && handle >= 1 && handle <= maxhandle) {
      DB_t *ph = &free_handles[handle-1];
      if (ph->h == handle) {
	char *dbname = ph->dbname;
	if (dbname) {
	  info_t *info = NULL;
	  char *odb98_info = getenv("ODB98_INFO");
#ifdef NECSX
	  char *binpath = getenv("ODB_BINPATH");
	  const char odb98x[] = "odb98be.x";
#else
	  char *binpath = getenv("ODB_FEBINPATH");
	  const char odb98x[] = "odb98.x";
#endif
	  char *cmd = NULL;

	  if (odb98_info && STRLEN(odb98_info) > 0) {
	    /* Info was extracted in advance and put into file $ODB98_INFO */
	    cmd = STRDUP(odb98_info);
	    putenv("ODB98_INFO="); /* Discard after it was used once */
	  }
	  else {
	    char *srcpath = ph->srcpath;
	    char *exe = NULL;
	    int len, lenexe;
	    if (!binpath) binpath = ".";
	    if (!srcpath) srcpath = ".";
	    lenexe = STRLEN(binpath) + STRLEN(odb98x) + 2;
	    ALLOC(exe, lenexe);
	    snprintf(exe, lenexe, "%s/%s", binpath, odb98x);
	    if (access(exe, X_OK) != 0) {
	      fprintf(stderr,"***Error: Unable to execute '%s'\n",exe);
	      FREE(exe);
	      Info = NULL;
	      goto finish;
	    }
	    len = 2*STRLEN(srcpath) + STRLEN(exe) + STRLEN(dbname) + STRLEN(sql_query_file) + 256;
	    ALLOC(cmd, len);
	    snprintf(cmd,len,
		     "| %s -F2 -Q %s %s/%s.sch"
		     "; \\echo '/dir=%s'"
		     "; \\echo '/host='`\\hostname`"
		     "; \\echo '/tstamp='`\\date`"
		     ,exe, sql_query_file, srcpath, dbname
		     ,srcpath);
	    FREE(exe);
	  }
	  Info = info = ODBc_get_info(cmd, setvar, nsetvar);
	  FREE(cmd);

	  /* Patch up */
	  {
	    while (info) {
	      info->npools = ph->npools;
	      info->ph = ph;
	      if (info->use_indices) {
		/* Fetch stored indices for each (table,wherecond)-pair */
		table_t *t = info->t;
		while (t) {
		  t->stored_idx_count = ODBc_fetch_indices(t, info);
		  t = t->next;
		} /* while (t) */
	      }
	      info = info->next;
	    }
	  }

	} /* if (dbname) */
      }
    }
  } 
  else { /* if (sql_query_file) ... else ... */
    fprintf(stderr,
	    "***Error in ODBc_sql_prepare_via_sqlfile(): The ODB/SQL-query file is not given !!\n");
    RAISE(SIGABRT);
  }
 finish:
  DRHOOK_END(0);
  return Info;
}


PUBLIC void *
ODBc_sql_prepare(int handle, const char *sql_query, const set_t *setvar, int nsetvar)
{
  void *Info = NULL;
  DRHOOK_START(ODBc_sql_prepare);
  if (sql_query) {
    FILE *fp;
    int pid = (int)getpid();
    int len = 80;
    char *tmpfile;
    char *tmpdir = getenv("TMPDIR");
    if (!tmpdir) tmpdir = "/tmp";
    len += STRLEN(tmpdir);
    ALLOCX(tmpfile,len);
    snprintf(tmpfile,len,"%s/myview_%d.sql",tmpdir,pid);
    (void) remove_file_(tmpfile);
    fp = fopen(tmpfile,"w");
    if (fp) {
      fprintf(fp,"%s\n;\n",sql_query);
      fclose(fp);
      Info = ODBc_sql_prepare_via_sqlfile(handle, tmpfile, setvar, nsetvar);
      (void) remove_file_(tmpfile);
    }
    else {
      fprintf(stderr,
	      "***Error in ODBc_sql_prepare(): Unable to create temporary ODB/SQL-file '%s'\n",
	      tmpfile);
      RAISE(SIGABRT);
    }
  } 
  else { /* if (sql_query) ... else ... */
    fprintf(stderr,
	    "***Error in ODBc_sql_prepare(): The query string is missing !!\n");
    RAISE(SIGABRT);
  }
 finish:
  DRHOOK_END(0);
  return Info;
}


PUBLIC char *
S2D_fix(const void *Info, const char *s)
{
  char *rc = NULL;
  if (Info && s) {
    const info_t *info = Info;
    char *pwherecond = STRDUP(s);
    if (info->ns2d > 0 && strstr(pwherecond, S2D)) {
      /* Strings involved in f.ex. WHERE-stmt */
      int j, ns2d = info->ns2d;
      for (j=ns2d-1; j>=0; j--) {
	/* Must go reverse to avoid problems with S2D_1 and S2D_10 for example */
	char *new = NULL;
	const str_t *s2d = &info->s2d[j];
	char *s2dname = STRDUP(s2d->name); /* Now still "s2d_<number>" */
	char *ps = s2dname;
	char *s2dvalue = NULL;
	int len_s2dvalue = 1 + STRLEN(s2d->value) + 2;
	ALLOC(s2dvalue,len_s2dvalue);
	snprintf(s2dvalue,len_s2dvalue,"\"%s\"",s2d->value);
	while (*ps) { *ps = ToUpper(*ps); ++ps; } /* "s2d_<number>" --> "S2D_<number>" */
	new = ReplaceSubStrings(pwherecond,          /* 'haystack' */
				s2dname,             /* 'needle' */
				s2dvalue,            /* 'repl_with' */
				true,                /* 'all_occurences' */
				false,               /* 'remove_white_space' */
				false);              /* 'ignore_case' */
	FREE(pwherecond);
	pwherecond = new;
	FREE(s2dvalue);
	FREE(s2dname);
      } /* for (j=ns2d-1; j>=0; j--) */
    }
    rc = pwherecond;
  }
  return rc; /* Watch for memory leakages here */
}


PUBLIC double
ODB_maxrows()
{
  double *testvalue = getsymaddr(_ROWNUM); /* current thread's "info->idxlen" */
  return testvalue ? *testvalue : (double)0;
}


PUBLIC double
ODB_maxcount(double x)
{
  return (ODB_maxrows() < x) ? (double)1 : (double)0;
}


#define CASE_GET_BITS(x, pos, len) \
  case len: for (j=0; j<nx; j++) x[j] = GET_BITS(x[j], pos, len); break

PUBLIC void
ODBc_vget_bits(double x[], int nx, int pos, int len)
{
  int j;
  DRHOOK_START(ODBc_vget_bits);
  switch (len) {
    CASE_GET_BITS(x, pos, 1);
    CASE_GET_BITS(x, pos, 2);
    CASE_GET_BITS(x, pos, 3);
    CASE_GET_BITS(x, pos, 4);
    CASE_GET_BITS(x, pos, 5);
    CASE_GET_BITS(x, pos, 6);
    CASE_GET_BITS(x, pos, 7);
    CASE_GET_BITS(x, pos, 8);
    CASE_GET_BITS(x, pos, 9);
    CASE_GET_BITS(x, pos,10);
    CASE_GET_BITS(x, pos,11);
    CASE_GET_BITS(x, pos,12);
    CASE_GET_BITS(x, pos,13);
    CASE_GET_BITS(x, pos,14);
    CASE_GET_BITS(x, pos,15);
    CASE_GET_BITS(x, pos,16);
    CASE_GET_BITS(x, pos,17);
    CASE_GET_BITS(x, pos,18);
    CASE_GET_BITS(x, pos,19);
    CASE_GET_BITS(x, pos,20);
    CASE_GET_BITS(x, pos,21);
    CASE_GET_BITS(x, pos,22);
    CASE_GET_BITS(x, pos,23);
    CASE_GET_BITS(x, pos,24);
    CASE_GET_BITS(x, pos,25);
    CASE_GET_BITS(x, pos,26);
    CASE_GET_BITS(x, pos,27);
    CASE_GET_BITS(x, pos,28);
    CASE_GET_BITS(x, pos,29);
    CASE_GET_BITS(x, pos,30);
    CASE_GET_BITS(x, pos,31);
    CASE_GET_BITS(x, pos,32);
  default:
    for (j=0; j<nx; j++) x[j] = 0;
    break;
  }
  DRHOOK_END(0);
}


PRIVATE void
InitNrowsNcols(const DB_t *ph, info_t *info, table_t *t, int poolno)
{
  DRHOOK_START(InitNrowsNcols);
  if (ph && info && t && t->ncols == 0) {
    DCA_getsize(ph->h, ph->dbname, t->name, poolno, &t->nrows, &t->ncols);
    ODB_fprintf(ODBc_get_debug_fp(),"InitNrowsNcols[poolno#%d]: table='%s', nrows=%d, ncols=%d\n",
		poolno, t->name, t->nrows, t->ncols);
  }
  DRHOOK_END(0);
}


#define DEPTH_TABS(fp,num) ODB_fprintf(fp,"%*.*s[%d]",2*(num),2*(num),"  ",num)

#define LINK_FETCH_ERROR(what) \
fprintf(stderr, \
	"***Error in %s:%d: Unable to fetch %s '%s' for pool#%d. " \
	"Expecting %d (= # of rows in table '%s'), but got %d rows. Link-case = %d.\n", \
	__FILE__, __LINE__, #what, \
	t->what, poolno, tp_off->nrows, tp_off->name, Nrows, t->linkcase)


#define COLUMN_FETCH_ERROR() \
fprintf(stderr, \
	"***Error: Unable to fetch column '%s' (fetch_name='%s') for pool#%d. " \
	"Expecting %d (= # of rows in table '%s'), but got %d rows.\n", \
	s, sf ? sf : NIL, poolno, tc->nrows, tc->name, Nrows)

#define COLUMN_FETCH_WARNING() \
fprintf(stderr, \
	"***Warning: Unable to fetch column '%s' (fetch_name='%s') for pool#%d. " \
	"Expecting %d (= # of rows in table '%s'), but got %d rows.\n", \
	s, sf ? sf : NIL, poolno, tc->nrows, tc->name, Nrows)


PRIVATE int
PrefetchData(const DB_t *ph, col_t *colthis, int poolno, const int depth)
{
  int errflg = 0;
  DRHOOK_START(PrefetchData);
  if (colthis) {
    const char *s = colthis->name;
    table_t *tc = colthis->t;
    const char *tblname = tc->name;
    
    if (!colthis->dsym) {
      /* 
	 For the moment accept (and expect) only ordinary columns 'colname@table'
	 and '#table' -variables
         (See also info.c, routine HasSimpleWHEREcond(): 
	 "For the moment ignore all kind of '$...#'-variables and '#table'
	 variables that are not referring to the current table")
      */
      if (IS_HASH(s)) {
	/* '#table_name' == ROW-number [1..Nrows] */
	if (strequ(s+1, tblname)) {
	  colthis->dsym = putsym(s, mdi);
	}
	else {
	  errflg++;
	}
      }
      else {
	const char *sf = colthis->fetch_name;
	int Nrows;
	colthis->dinp = DCA_fetch_double(ph->h, ph->dbname, tc->name, sf,
					 poolno, NULL, 0, &Nrows);
	if (Nrows != tc->nrows) {
	  /* Don't abort */
	  COLUMN_FETCH_WARNING();
	  colthis->dsym = putsym(s, mdi);
	  errflg++;
	}
	else {
	  Bool thesame = strequ(s,sf) ? true : false;
	  FILE *fp_echo = ODBc_get_debug_fp();
	  DEPTH_TABS(fp_echo, depth);
	  ODB_fprintf(fp_echo,
		      "%s:%d: Successfully fetched (double) %s%s%s (Nrows=%d/%d) : poolno#%d\n",
		      __FILE__, __LINE__,
		      thesame ? s : sf,
		      thesame ? "" : " alias ",
		      thesame ? (const char *)"" : s,
		      Nrows, tc->nrows, poolno);
	  colthis->dinp_len = Nrows;
	  colthis->dinp_alloc = true;
	  if (colthis->dtnum == DATATYPE_INT4 && 
	      colthis->bitpos >= 0 && colthis->bitpos <  MAXBITS &&
	      colthis->bitlen >= 1 && colthis->bitlen <= MAXBITS) {
	    ODBc_vget_bits(colthis->dinp, colthis->dinp_len, colthis->bitpos, colthis->bitlen);
	  }
	  colthis->dsym = putsymvec(s, mdi, colthis->dinp, Nrows);
	}
      }
    }
    else {
      *colthis->dsym = mdi;
    } /* if (!colthis->dsym) ... else ... */
  } /* if (colthis) */
  DRHOOK_END(0);
  return errflg;
}

#define INIT_SKIPROW(x) if (t->skiprow) memset(t->skiprow, (x), Nrows * sizeof(*t->skiprow))
#define FILL_SKIPROW(OP) for (j=0; j<Nrows; j++) t->skiprow[j] = (lhs[j] OP rhs) ? 0 : 1
#define FILL_SKIPROW_USING_ROWID(OP) for (j=0; j<Nrows; j++) t->skiprow[j] = ((j+1) OP rhs) ? 0 : 1

#define FETCH_ME(what) \
{ \
  int Nrows = 0; \
  t->what = DCA_fetch_int(ph->h, ph->dbname, tp_off->name, t->link##what, \
			  poolno, NULL, 0, &Nrows); \
  if (Nrows != tp_off->nrows) { \
    LINK_FETCH_ERROR(link##what); \
    RAISE(SIGABRT); \
  } \
  else if (fp_echo) { \
    if (!done) { ODB_fprintf(fp_echo,"\n"); done++; } \
    DEPTH_TABS(fp_echo, depth); \
    ODB_fprintf(fp_echo, \
		"%s:%d: Successfully fetched (int) %s (Nrows=%d/%d) [linkcase=%d, '%s'] : poolno#%d\n", \
		__FILE__, __LINE__, \
		t->link##what, Nrows, tp_off->nrows, t->linkcase, \
		(t->linkcase >= 1 && t->linkcase <= NLINKCASES) ? linkcase_name[t->linkcase] : NIL, \
		poolno); \
  } \
}

PRIVATE int /* Recursive */
HSL(const DB_t *ph, info_t *info, table_t *t, int poolno, Bool *immed_return, const int depth)
{ /* HSL = HierarchicalSearchLoop */
  int rc = 0;
  if (t && ph && info) {
    FILE *fp_echo = ODBc_get_debug_fp();
    table_t *tp_link = t->linkparent;
    table_t *tp_off = t->offset_parent;
    int jr, lo=0, hi=0;
    int done = 0;
    if (fp_echo) {
      DEPTH_TABS(fp_echo, depth);
      ODB_fprintf(fp_echo, "%s:%d: HSL-depth#%d :",__FILE__, __LINE__, depth);
      if (t->name) {
	ODB_fprintf(fp_echo, " t=['%s' #%d lo=%d ob=%d jr=%d]",
		    t->name,t->nrows,t->lo,t->ob,t->jr);
      }
      if (tp_link && tp_link->name) {
	ODB_fprintf(fp_echo, " tp_link=['%s' #%d lo=%d ob=%d jr=%d]",
		    tp_link->name,tp_link->nrows,tp_link->lo,tp_link->ob,tp_link->jr);
      }
      if (tp_off && tp_off->name) {
	ODB_fprintf(fp_echo, " tp_off=['%s' #%d lo=%d ob=%d jr=%d]",
		    tp_off->name,tp_off->nrows,tp_off->lo,tp_off->ob,tp_off->jr);
      }
      fflush(fp_echo);
    }

    if (t->linkcase == 1 || t->linkcase == 4 || t->linkcase == 2) {
      if (!t->offset) FETCH_ME(offset);
      if (!t->len) FETCH_ME(len);
    }

    switch (t->linkcase) {
    case 1: /* ONELOOPER - link */
    case 4: /* A regular link */
      if (tp_off->jr >= 0 && tp_off->jr < tp_off->nrows) {
	/* Extra safeguard : Make sure we stay within the array bounds */
	if (tp_off->jr != t->ob) {
	  t->ob = tp_off->jr;
	  t->lo = t->offset[tp_off->jr];
	}
	lo = t->offset[tp_off->jr];
	if (t->linkcase == 1) {
	  /* Remember safeguarding */
	  hi = lo + ((t->len[tp_off->jr] == 1) ? 1 : 0);
	}
	else {
	  hi = lo + t->len[tp_off->jr];
	}
	if (lo < 0 || lo >= t->nrows || hi < 0 || hi > t->nrows) {
	  /* Disallow insane values */
	  lo = hi = 0;
	}
      }
      else {
	lo = hi = 0;
      }
      break;

    case 2: /* ALIGNed table */
      {
	Bool range_ok = false;

	if (tp_off->jr >= 0 && tp_off->jr < tp_off->nrows) {
	  /* Extra safeguard : Make sure we stay within the array bounds */
	  if (tp_off->jr != t->ob) {
	    t->ob = tp_off->jr;
	    t->lo = t->offset[tp_off->jr];
	  }
	  lo = t->offset[tp_off->jr] + (tp_link->jr - tp_link->lo);
	  range_ok = true;
	}
	else {
	  lo = -1; /* Out of bounds */
	}

	if (lo < 0 || lo >= t->nrows) {
	  /* Disallow insane values straight away */
	  lo = hi = 0;
	  break;
	}
	
	if (range_ok) {
	  /* Extra safeguard : Make sure we stay within the array bounds */
	  /* Remember safeguarding */
	  int add = ((t->len[tp_off->jr] > 0) ? 1 : 0); /* One-by-one */
	  hi = lo + add;
	}
	else {
	  /* Disallow insane values */
	  lo = hi = 0;
	}

	if (lo < 0 || lo >= t->nrows || hi < 0 || hi > t->nrows) {
	  /* Disallow insane values */
	  lo = hi = 0;
	}
      }
      break;

    case 3: /* ALIGNed, but orphaned table (i.e. no link-parent available/specified) */
      lo = tp_link ? tp_link->jr : 0;
      hi = lo + 1;
      if (lo < 0 || lo >= t->nrows || hi < 0 || hi > t->nrows) {
	/* Disallow insane values */
	lo = hi = 0;
      }
      break;

    case 5: /* The very first table in FROM-stmt */
    case 6: /* A completely disconnected/isolated table w.r.t. other tables in FROM-stmt */
      lo = 0;
      hi = t->nrows;
      break;

    default:
      fprintf(stderr,
	      "***Error: Fatal programming error : Invalid linkcase #%d."
	      " Must be one between %d and %d, inclusive\n",t->linkcase,1,NLINKCASES);
      RAISE(SIGABRT);
      break;
    } /* switch (t->linkcase) */

    if (fp_echo) {
      if (!done) { ODB_fprintf(fp_echo,"\n"); done++; }
      DEPTH_TABS(fp_echo, depth);
      ODB_fprintf(fp_echo, "%s:%d:",__FILE__,__LINE__);
      ODB_fprintf(fp_echo, " ==> tbl='%s' @ jr=%d; lo=%d; ob=%d [lo=%d, hi=%d)", 
		  t->name, t->jr, t->lo, t->ob, lo, hi);
      if (tp_link) ODB_fprintf(fp_echo, ", linkparent '%s' @ jr=%d", 
			  tp_link->name, tp_link->jr);
      if (tp_off) ODB_fprintf(fp_echo, ", offset_parent '%s' @ jr=%d", 
			      tp_off->name, tp_off->jr);
      ODB_fprintf(fp_echo, "\n");
    }

    if (info && info == t->info && lo >= 0 && hi > lo) {
      int maxfrom = t->table_id; /* The same meaning as in compiler/genc.c */

      if (t->simple_wherecond && !t->skiprow) {
	/*   
	     WHERE-condition for this table t was simple enough and thus
	   we generate exclusion vector skiprow[] in order to skip those
	   rows that will definitely be ruled out.
	     This optimization should speed-up satellite data search, when
	   (say) particular channel numbers (press@body) are to be filtered 
	*/

	int i;
	int errflg = 0;
	int nwl = t->nwl;
	int nwl_index = 0; /* Number of "wl"'s with stored_idx ... used later ... */
	simple_where_t *wl = t->wl;
	int Nrows = t->nrows;

	for (i=0; i<nwl && errflg == 0; i++) {
	  odbidx_pp_t *pp = NULL;

	  if (wl->stored_idx) {
	    pp = codb_IDXF_unpack(wl->stored_idx, poolno, 0);

	    if (!pp || (pp && (pp->nrows != Nrows))) {
	      /* Table no. of rows & the nrows seen by the Index
		 disagree ==> the index [for this poolno] is probably useless */
	      pp = NULL;
	    }
	  }

	  if (!pp) {
	    errflg = PrefetchData(ph, wl->wcol, poolno, depth);
	  }
	  else {
	    nwl_index++;
	  }

	  wl++;
	} /* for (i=0; i<nwl && errflg == 0; i++) */

	if (errflg == 0) {
	  int j;
	  ALLOC(t->skiprow, Nrows);
	  wl = t->wl;

	  if (wl->stored_idx && nwl == nwl_index) {
	    /* IDXF available : nwl can be > 1 i.e. more than one AND'ies possible */
	    unsigned char *lc_picked = NULL;
	    unsigned char *picked = NULL;
	    int min_picked = INT_MAX;
	    int max_picked = -1;

	    CALLOC(picked, Nrows);

	    INIT_SKIPROW(1); /* By default skip all rows */

	    for (i=0; i<nwl; i++) {
	      odbidx_pp_t *pp = codb_IDXF_locate_pp(wl->stored_idx, poolno, 0);
	      int js, nsets = pp->nsets;
	      odbidx_set_t *pidxset = pp->idxset;
	      if (i == 0) {
		for (js=0; js<nsets; js++) {
		  /* Please note that these "idxsets" are considered to be mutually exclusive */
		  int jr, ndata = pidxset->ndata;
		  for (jr=0; jr<ndata; jr++) {
		    int curidx = (int)pidxset->idxdata[jr];
		    picked[curidx] = 1;
		    min_picked = MIN(min_picked, curidx);
		    max_picked = MAX(max_picked, curidx);
		  } /* for (jr=0; jr<ndata; jr++) */
		  ++pidxset;
		} /* for (js=0; js<nsets; js++) */
	      }
	      else { /* i > 0 */
		/* Here we choose only those which are still in "picked[]"-list ;
		   This is due to the AND'ing of the WHERE-stmts */
		int lc_min_picked = INT_MAX;
		int lc_max_picked = -1;
		Bool found_any = false;

		if (!lc_picked) ALLOC(lc_picked, Nrows);
		memset(lc_picked, 0, Nrows * sizeof(*lc_picked));

		for (js=0; js<nsets; js++) {
		  int jr, ndata = pidxset->ndata;
		  for (jr=0; jr<ndata; jr++) {
		    int curidx = (int)pidxset->idxdata[jr];
		    if (curidx >= min_picked && 
			curidx <= max_picked &&
			picked[curidx] == 1) {
		      lc_picked[curidx] = 1;
		      lc_min_picked = MIN(lc_min_picked, curidx);
		      lc_max_picked = MAX(lc_max_picked, curidx);
		      found_any = true;
		    }
		  } /* for (jr=0; jr<ndata; jr++) */
		  ++pidxset;
		} /* for (js=0; js<nsets; js++) */

		if (found_any) {
		  min_picked = MAX(min_picked, lc_min_picked);
		  max_picked = MIN(max_picked, lc_max_picked);
		}
		else {
		  min_picked = Nrows;
		  max_picked = -1;
		  goto stop_search_now;
		}

		/* for (j=0; j<min_picked; j++) picked[j] = 0; */
		for (j=min_picked; j<=max_picked; j++) picked[j] = lc_picked[j];
		/* for (j=max_picked+1; j<Nrows; j++) picked[j] = 0; */
	      }
	      wl++;
	    } /* for (i=0; i<nwl; i++) */

	  stop_search_now:
	    FREE(lc_picked);

	    if (min_picked <= max_picked) {
	      for (j=min_picked; j<=max_picked; j++) {
		if (picked[j]) t->skiprow[j] = 2; /* 2 == do *NOT* skip this row, found via IDXF */
	      }
	    }
	    
	    FREE(picked);
	  }
	  else if (nwl == 1) {
	    /* Just one simple WHERE-condition ; no IDXF */
	    double rhs = wl->rhs;
	    if (IS_HASH(wl->lhs)) {
	      switch (wl->oper) {
	      case EQ: FILL_SKIPROW_USING_ROWID(==); break;
	      case NE: FILL_SKIPROW_USING_ROWID(!=); break;
	      case GT: FILL_SKIPROW_USING_ROWID(> ); break;
	      case GE: FILL_SKIPROW_USING_ROWID(>=); break;
	      case LT: FILL_SKIPROW_USING_ROWID(< ); break;
	      case LE: FILL_SKIPROW_USING_ROWID(<=); break;
	      default: INIT_SKIPROW(1); break;
	      } /* switch (wl->oper) */
	    }
	    else {
	      double *lhs = wl->wcol->dinp;
	      switch (wl->oper) {
	      case EQ: FILL_SKIPROW(==); break;
	      case NE: FILL_SKIPROW(!=); break;
	      case GT: FILL_SKIPROW(> ); break;
	      case GE: FILL_SKIPROW(>=); break;
	      case LT: FILL_SKIPROW(< ); break;
	      case LE: FILL_SKIPROW(<=); break;
	      default: INIT_SKIPROW(1); break;
	      } /* switch (wl->oper) */
	    }
	  }
	  else {
	    /* More than one AND'ed WHERE-conditions ; no IDXF */
	    for (j=0; j<Nrows; j++) {
	      Bool and_cond = true;
	      wl = t->wl;
	      for (i=0; i<nwl && and_cond; i++) {
		double lhs = IS_HASH(wl->lhs) ? (double)(j+1) : wl->wcol->dinp[j];
		double rhs = wl->rhs;
		switch (wl->oper) {
		case EQ: and_cond = (lhs == rhs) ? true : false; break;
		case NE: and_cond = (lhs != rhs) ? true : false; break;
		case GT: and_cond = (lhs >  rhs) ? true : false; break;
		case GE: and_cond = (lhs >= rhs) ? true : false; break;
		case LT: and_cond = (lhs <  rhs) ? true : false; break;
		case LE: and_cond = (lhs <= rhs) ? true : false; break;
		default: and_cond = false; break;
		} /* switch (wl->oper) */
		wl++;
	      } /* for (i=0; i<nwl && and_cond; i++) */
	      t->skiprow[j] = and_cond ? 0 : 1;
	    } /* for (j=0; j<Nrows; j++) */
	  } /* if (wl->stored_idx) ... else if (nwl == 1) ... else ... */
	}
	else {
	  /* Switch off the simple WHERE-cond tracking */
	  t->simple_wherecond = false;
	} /* if (errlfg == 0) ... else ... */
      }

      for (jr=lo; jr<hi; jr++) {
	double val = 1; /* The same as if WHERE was empty i.e. "true" all the time */
	t->jr = jr;

	if (t->skiprow) {
	  if (t->skiprow[jr] == 1) {
	    /* Reject this row */
	    val = 0;
	    continue; /* for (jr=lo; jr<hi; jr++) */
	  }
	  else if (t->skiprow[jr] == 2) {
	    /* This row selected due to IDXF-data */
	    val = 1;
	    goto go_deeper;
	  }
	}
	else if (t->wherecond_and_ptree) {
	  int j, iret = 0;
	  int nwhere = info->nwhere;
	  for (j=0; j<nwhere; j++) {
	    col_t *colthis = &info->w[j];
	    const char *s = colthis->name;
	    table_t *tc = colthis->t;
	    if (!tc) continue;
	    if (tc->table_id > maxfrom) continue;
	    if (!colthis->dsym) {
	      if (IS_HASH(s)) {
		/* '#table_name' == ROW-number [1..Nrows] */
		colthis->dsym = putsym(s, tc->jr + 1);
	      }
	      else {
		/* Ordinary columns 'colname@table' */
		const char *sf = colthis->fetch_name;
		int Nrows;
		colthis->dinp = DCA_fetch_double(ph->h, ph->dbname, tc->name, sf,
						 poolno, NULL, 0, &Nrows);
		if (Nrows != tc->nrows) {
		  COLUMN_FETCH_ERROR();
		  RAISE(SIGABRT);
		}
		else {
		  Bool thesame = strequ(s,sf) ? true : false;
		  DEPTH_TABS(fp_echo, depth);
		  ODB_fprintf(fp_echo,
			      "%s:%d: Successfully fetched (double) %s%s%s (Nrows=%d/%d) : poolno#%d\n",
			      __FILE__, __LINE__,
			      thesame ? s : sf,
			      thesame ? "" : " alias ",
			      thesame ? (const char *)"" : s,
			      Nrows, tc->nrows, poolno);
		  colthis->dinp_len = Nrows;
		  colthis->dinp_alloc = true;
		  if (colthis->dtnum == DATATYPE_INT4 && 
		      colthis->bitpos >= 0 && colthis->bitpos <  MAXBITS &&
		      colthis->bitlen >= 1 && colthis->bitlen <= MAXBITS) {
		    ODBc_vget_bits(colthis->dinp, colthis->dinp_len, colthis->bitpos, colthis->bitlen);
		  }
		}
		if (colthis->dinp && tc->jr >= 0 && tc->jr < tc->nrows) {
		  /* Extra safeguarding */
		  colthis->dsym = putsymvec(s, colthis->dinp[tc->jr], colthis->dinp, Nrows);
		}
		else {
		  colthis->dsym = putsym(s, mdi);
		}
	      }
	    }
	    else {
	      if (IS_HASH(s)) {
		*colthis->dsym = tc->jr + 1;
	      }
	      else {
		if (colthis->dinp && tc->jr >= 0 && tc->jr < tc->nrows) {
		  /* Extra safeguarding */
		  *colthis->dsym = colthis->dinp[tc->jr];
		}
		else {
		  *colthis->dsym = mdi;
		}
	      }
	    }
	    
	  } /* for (j=0; j<nwhere; j++) */

	  if (!t->skiprow) {
	    val = RunTree(t->wherecond_and_ptree, NULL, &iret);
	    if (iret != 0) {
	      fprintf(stderr,
		      "***Error: Unable to evaluate the WHERE-conditions AND-part '%s'\n",
		      t->wherecond_and);
	      RAISE(SIGABRT);
	    }
	  }
	  /* otherwise val = 1, since t->skiprow && t->skiprow[jr] == 0 or 2 */
	} /* if (t->wherecond_and_ptree) */
	
      go_deeper:
	if (val != 0) {
	  /* Go deeper ... */
	  DEPTH_TABS(fp_echo, depth);
	  ODB_fprintf(fp_echo, 
		      "%s:%d: val=%.14g, tbl=[%s #%d lo=%d ob=%d jr=%d]\n",
		      __FILE__, __LINE__,
		      val, t->name, t->nrows, t->lo, t->ob, t->jr);
	  rc += HSL(ph, info, t->next, poolno, immed_return, depth+1);
	}
	if (immed_return && *immed_return) break; /* Stop searching now !! */
      } /* for (jr=lo; jr<hi; jr++) */
    }
    else {
      rc = 0; /* Don't go for this at all ; probably "insane" */
    }
  }
  /* if (t && ph && info) ... */
  else {
    /* (usually) take this row & update all indices */
    if (info) {
      const int ichunk = 65536; /* A fixed idx[] (re-)alloc chunk increment for now */
      t = info->t;
      if (t) {
	FILE *fp_echo = ODBc_get_debug_fp();
	int icnt = 0;
	int idxlen = info->idxlen;
	int idxalloc = info->idxalloc;
	while (t) {
	  if (t->in_select_clause) {
	    if (idxalloc <= idxlen) REALLOC(t->idx, idxalloc + ichunk);
	    t->idx[idxlen] = t->jr;
	  }
	  if (fp_echo) {
	    DEPTH_TABS(fp_echo, depth);
	    ODB_fprintf(fp_echo, 
			"%s:%d: #<%d> tbl(idx=%d)=[%s #%d lo=%d ob=%d jr=%d]\n",
			__FILE__, __LINE__, icnt,
			t->in_select_clause ? idxlen : -1,
			t->name, t->nrows, t->lo, t->ob, t->jr);
	  }
	  t = t->next;
	  if (fp_echo) ++icnt;
	} /* while (t) */
	if (idxalloc <= idxlen) info->idxalloc += ichunk;
	rc = 1;
      }
      else if (info->wherecond) { /* No tables, but may still have a WHERE-condition */
	int iret = 0;
	double val = RunTree(info->wherecond_ptree, NULL, &iret);
	if (iret != 0) {
	  fprintf(stderr,
		  "***Error: Unable to evaluate the WHERE-condition '%s'\n",
		  info->wherecond);
	  RAISE(SIGABRT);
	}
	rc = (val != 0) ? 1 : 0;
      }
      else { /* No tables, no WHERE-condition (implies "true") */
	rc = 1; /* Take this -- unconditionally */
      }
      if (rc == 1) {
	*info->idxlen_dsym = ++(info->idxlen); /* For use by ODB_maxrows/ODB_maxcount */
      }
    }
    else {
      rc = 0; /* Do *NOT* take this */
    }
  }
  if (immed_return && info && info->__maxcount__) {
    /* This will effectively stop searching right now and there !! */
    if (info->idxlen >= *info->__maxcount__) *immed_return = true;
  }
  return rc;
}


PRIVATE void
InitCols(col_t *c)
{
  DRHOOK_START(InitCols);
  if (c) {
    DEF_IT;
    (void) delsym(c->name, it);
    if (c->dinp_alloc) FREE(c->dinp);
    c->dinp = NULL;
    c->dinp_len = 0;
    c->dinp_alloc = false;
    c->dsym = NULL;
  }
  DRHOOK_END(0);
}


PRIVATE void
InitTables(table_t *t)
{
  DRHOOK_START(InitTables);
  while (t) {
    t->jr = -1;
    t->lo =  0;
    t->ob = -1;
    t->nrows = 0;
    t->ncols = 0;
    FREE(t->offset);
    FREE(t->len);
    FREE(t->idx);
    FREE(t->skiprow);
    t = t->next;
  } /* while (t) */
  DRHOOK_END(0);
}


PUBLIC void *
ODBc_reset_info(void *Info)
{
  DRHOOK_START(ODBc_reset_info);
  if (Info) {
    info_t *info = Info;
    int j;
    for (j=0; j<info->ncols; j++) {
      col_t *colthis = &info->c[j];
      InitCols(colthis);
    }
    for (j=0; j<info->nwhere; j++) {
      col_t *colthis = &info->w[j];
      InitCols(colthis);
    }
    InitTables(info->t);
    info->idxalloc = 0;
    info->idxlen = 0;
    info->idxlen_dsym = putsym(_ROWNUM, info->idxlen);
    if (info->dyn && info->ndyn > 0) {
      dyn_t *dyn = info->dyn;
      int j, ndyn = info->ndyn;
      for (j=0; j<ndyn; j++) {
	FREE(dyn->data);
	dyn->ndata = 0;
	putsym(dyn->name, 0);
	dyn++;
      } /* for (j=0; j<ndyn; j++) */
    }
  }
  DRHOOK_END(0);
  return Info;
}


PRIVATE void
GetMinMax(const double d[], int n, double *Min, double *Max)
{
  DRHOOK_START(GetMinMax);
  if (Min && Max) {
    *Min = mdi; /* Note: from info.h ==> const double mdi = ABS(RMDI); */
    *Max = 0;
    if (d && n > 0) {
      int j;
      *Min = d[0];
      *Max = d[0];
      for (j=1; j<n; j++) {
	if (ABS(d[j]) != mdi) {
	  if (*Min > d[j]) *Min = d[j];
	  if (*Max < d[j]) *Max = d[j];
	}
      } /* for (j=1; j<n; j++) */
    }
  }
  DRHOOK_END(0);
}


PUBLIC int
ODBc_sql_exec(int handle, void *Info, int poolno, int *Begin_row, int *End_row)
{
  int nrows = -1;
  info_t *info = Info;
  DRHOOK_START(ODBc_sql_exec);
  if (info) {
    FILE *fp_echo = ODBc_get_debug_fp();
    if (free_handles && handle >= 1 && handle <= maxhandle) {
      DB_t *ph = &free_handles[handle-1];
      if (ph->h == handle) {
	int ntables = ph->ntables;
	int begin_row = Begin_row ? *Begin_row : 0;
	int end_row = End_row ? *End_row : -1;
	Bool immed_return = false;
	Bool poolno_okay;
	int npools = ph->npools;
	if (poolno <= 0 || poolno > npools) {
	  ODB_fprintf(stderr, 
		      "***Error in ODBc_sql_exec() of %s : poolno (%d) must be between"
		      " %d and %d\n",
		      info->view, poolno, 1, npools);
	  nrows = -2;
	  goto finish;
	}

	poolno_okay = ODB_in_permanent_poolmask(handle, poolno) ? true : false;
	if (!poolno_okay) {
	  ODB_fprintf(fp_echo,
		      "***Warning in ODBc_sql_exec() of %s : Pool number %d not in poolmask\n",
		      info->view, poolno);
	  nrows = 0; /* Not an error */
	  goto finish;
	}

	if (!info->dca_alloc) {
	  const table_t *t = info->t;
	  while (t) {
	    int iret = 0;
	    (void) DCA_alloc(handle, ph->dbname, t->name, &iret);
	    t = t->next;
	  }
	  info->dca_alloc = true;

	  {
	    DEF_IT; /* see privpub.h; defines "it" as current (my) YOMOML thread id >= 1 && <= inumt */
	    delallsym(&it); /* Clean-up symbol table for this thread only */
	  }

	  info->__maxcount__ = putsym("$__maxcount__", mdi);  /* ODBTk's $__maxcount__ */
	  if (info->s && info->nset > 0) {
	    /* Set values of $-variables */
	    int j;
	    for (j=0; j<info->nset; j++) {
	      /* One of these may override "$__maxcount__" as well */
	      (void) putsym(info->s[j].name, info->s[j].value);
	    }
	  }

	  if (info->s2d && info->ns2d > 0) {
	    /* Install Hollerith-strings */
	    int j;
	    for (j=0; j<info->ns2d; j++) {
	      (void) putsym(info->s2d[j].name, info->s2d[j].u.dval);
	    }
	  }

	  if (!info->has_ll && info->odb_lat->t && info->odb_lon->t) {
	    info->has_ll = true;
	  }

	  if (info->has_ll /* && (info->latlon_rad == -1 || !getenv("ODB_LATLON_RAD")) */) {
	    /* Handle setup for use of lldegrees()/llradians() */
	    if (info->odb_lat->t && info->odb_lon->t) {
	      /* References to $ODB_LAT & $ODB_LON indeed exists;
		 Now poll their raw value range (for this poolno only) and (reset) latlon_rad */
	      int j, errflg = 0;
	      const double abs_range[2] = { half_pi, pi }; /* [0] for lat & [1] for lon, respectively */
	      const double eps = (double)0.01e0;
	      int rad_score = 0;
	      int deg_score = 0;
	      col_t *cc[2];
	      cc[0] = info->odb_lat;
	      cc[1] = info->odb_lon;

	      for (j=0; j<2; j++) {
		int Nrows;
		col_t *colthis = cc[j];
		const char *s = colthis->name;
		const char *sf = colthis->fetch_name ? colthis->fetch_name : s;
		table_t *tc = colthis->t;
		InitNrowsNcols(ph, info, tc, poolno);
		colthis->dinp = DCA_fetch_double(ph->h, ph->dbname, tc->name, sf,
						 poolno, NULL, 0, &Nrows);
		if (Nrows != tc->nrows) {
		  /* Don't abort */
		  COLUMN_FETCH_WARNING();
		  errflg++;
		  break; /* for (j=0; j<2; j++) */
		}
		else {
		  double Min, Max;
		  Bool thesame = strequ(s,sf) ? true : false;
		  ODB_fprintf(fp_echo,
			      "%s:%d: Successfully fetched (double) %s%s%s (Nrows=%d/%d) : poolno#%d\n",
			      __FILE__, __LINE__,
			      thesame ? s : sf,
			      thesame ? "" : " alias ",
			      thesame ? (const char *)"" : s,
			      Nrows, tc->nrows, poolno);
		  colthis->dinp_len = Nrows;
		  colthis->dinp_alloc = true;
		  colthis->dsym = putsymvec(s, colthis->dinp ? *colthis->dinp : mdi, colthis->dinp, Nrows);
		  if (Nrows > 0) {
		    GetMinMax(colthis->dinp, Nrows, &Min, &Max);
		    if (Min > Max) {
		      /* Values do not make sense */
		      errflg++;
		      break; /* for (j=0; j<2; j++) */
		    }
		    if (ABS(Min) < abs_range[j] && ABS(Max) < abs_range[j]) {
		      /* We have radians */
		      rad_score++;
		    }
		    else {
		      deg_score++;
		    }
		  } /* if (Nrows > 0) */
		}
	      } /* for (j=0; j<2; j++) */

	      if (!errflg) {
		if (rad_score == 2) {
		  info->latlon_rad = 1;
		}
		else if (deg_score == 2) {
		  info->latlon_rad = 0;
		}
		else {
		  info->latlon_rad = -1;
		}
		codb_change_latlon_rad_(&info->latlon_rad);
	      }
	      else {
		fprintf(stderr,
			"***Warning: Unable to determine whether"
			" latitudes/longitudes were in degrees or radians\n");
	      }
	    } /* if (info->odb_lat->t && info->odb_lon->t) */
	  } /* if (info->has_ll && !getenv("ODB_LATLON_RAD")) */
	  if (info->has_ll) {
	    ODB_fprintf(fp_echo,"ODBc_sql_exec() : info->latlon_rad = %d\n",info->latlon_rad);
	  }
	} /* if (!info->dca_alloc) */

	info = ODBc_reset_info(info);

	if (begin_row >= 1) {
	  (void) putsym(BEGIN_ROW, begin_row);
	}

	if (info->__maxcount__ && *info->__maxcount__ >= 0 && end_row < 0) {
	  end_row = *info->__maxcount__;
	  if (End_row) *End_row = end_row;
	}

	if (end_row >= 0) {
	  /* Override the current value of $__maxcount__ */
	  info->__maxcount__ = putsym("$__maxcount__", end_row);
	  (void) putsym(END_ROW, end_row);
	}

	ODB_fprintf(fp_echo, 
		    "ODBc_sql_exec() : Poolno#%d : info->__maxcount__ = %p, "
		    "*info->__maxcount__ = %.14g, end_row = %d, *End_row = %.14g\n",
		    poolno, info->__maxcount__, 
		    info->__maxcount__ ? *info->__maxcount__ : mdi,
		    end_row, End_row ? (double) *End_row : mdi);

	(void) putsym("$#", poolno); /* POOLNO */
	(void) putsym(_POOLNO, poolno); /* POOLNO as well (an alias) */

	(void) putsym(_NPOOLS, npools); /* NPOOLS */
	(void) putsym(_NTABLES, ntables); /* NTABLES */

	(void) putsym(_UNIQNUM, 0);

	/*
	(void) putsym(_ROWNUM, 0);
	(void) putsym(_COLNUM, 0);
	(void) putsym(_NROWS, 0);
	*/

	(void) putsym(_NCOLS, info->ncols_true);

	{
	  table_t *t = info->t;
	  while (t) {
	    InitNrowsNcols(ph, info, t, poolno);
	    t = t->next;
	  }
	}

	if ((info->optflags & 0x1) == 0x1) {
	  /* A special case : SELECT count(*) FROM table */
	  nrows = 1;
	}
	else if (info->t) {
	  table_t *t = info->t;
	  while (t) {
	    /* 
	       Look for the first table in FROM-hierarchy which has nrows > 0.
	       This becomes the starting table for this "poolno".
	       Note: The generated C-code -approach currently differs and it bases
	       its search from full FROM-hierarchy i.e. also for tables nrows == 0
	    */
	    if (t->nrows > 0) break;
	    /* Since t->nrows == 0 ... */
	    t->jr = 0;
	    t->lo = 0;
	    t = t->next;
	  }
	  immed_return = false;
	  nrows = t ? HSL(ph, info, t, poolno, &immed_return, 0) : 0;
	}
	else {
	  /* TABLE-less query */
	  immed_return = false;
	  nrows = HSL(ph, info, NULL, poolno, &immed_return, 0);
	}

	ODB_fprintf(fp_echo,
		    "ODBc_sql_exec() : Poolno#%d : nrows = %d : immed_return = %s\n",
		    poolno,nrows,immed_return ? "true" : "false");
      }
      else {
	/* Handle not in use */
	ODB_fprintf(stderr, "***Error in ODBc_sql_exec() : Handle %d not in use\n",handle); 
	nrows = -4;
      }
    }
    else {
      /* Invalid handle */
      ODB_fprintf(stderr, "***Error in ODBc_sql_exec() : Invalid handle %d\n",handle); 
      nrows = -3;
    }
  }
 finish:
  DRHOOK_END(nrows);
  return nrows;
}
