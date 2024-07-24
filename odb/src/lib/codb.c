#ifdef RS6K
#pragma options opt=0
#endif

/* codb.c: 

   Fortran callable C-interface layer to the ODB-software:

   Layers: (DDL = Data Definition Language)

   Fortran-Application
      --> Fortran modules (odb.F)
      (*)--> C-interface layer (codb.c)
	    --> Low level link between C-interface and DDL (odbpools.c)
               --> Automatically generated C-code(s) from DDL(s)

*/
       
#define ODB_POOLS 1

#include "odb.h"
#include "evaluate.h"
#include "idx.h"
#include "regcache.h"
#include "cdrhook.h"
#include "pcma_extern.h"

PRIVATE int max_omp_threads = 0; /* Will be updated (once) in codb_init_omp_locks_() */

#define USING_IT \
(using_it && *using_it >= 1 && *using_it <= max_omp_threads) ? *using_it : get_thread_id_()

/* #define IT (it-1) */ /* Now defined in privpub.h, included via odb.h -> alloc.h */

#define POOLMASK_SET (p->pm && p->pm->poolmask[IT][0] == 1)

#define MATCHING \
  (p->inuse \
   && p->handle == Handle \
   && (AnyPool || Poolno == p->poolno) \
   && (!POOLMASK_SET || (p->pm && p->pm->poolmask[IT][p->poolno])) \
   )

#define DELETE_INTERMED(x) \
  if (x) { \
    /* Delete any existing intermediate results */ \
    int jjc, nnc = x->nc; \
    for (jjc=0; jjc<nnc; jjc++) { \
      double *din = x->d[jjc]; \
      FREE(din); \
    } \
    FREE(x); \
  }

PRIVATE o_lock_t HASHING_mylock = 0; /* A specific OMP-lock; initialized only once in
					odb/lib/codb.c, routine codb_init_omp_locks_() */

PUBLIC void 
init_HASHING_lock()
{ 
  INIT_LOCKID_WITH_NAME(&HASHING_mylock,"codb.c:HASHING_mylock");
}

PUBLIC void
codb_hash_set_lock_()
{
  coml_set_lockid_(&HASHING_mylock);
}

PUBLIC void
codb_hash_unset_lock_()
{
  coml_unset_lockid_(&HASHING_mylock);
}

PUBLIC void
codb_init_omp_locks_()
{
  /* Initializes all OMP-locks */
  static Bool first_time = true;
  if (first_time) {
    char *env = getenv("ODB_OPENMP_LOCK_DEBUG");
    int debug_on = env ? atoi(env) : 0;
    int j, iret;
    max_omp_threads = get_max_threads_();
    coml_set_debug_(&debug_on, &iret);
    coml_init_lock_(); /* initialize master lock (i.e. M_LOCK in ifsaux/module/yomoml.F90) */
    /* global locks specific to generated C-codes */
    for (j=0; j<MAX_ODB_GLOBAL_LOCKS_OPENMP; j++) {
      char lockname[80];
      snprintf(lockname,80,"codb.c:global_mylock[%d]",j);
      INIT_LOCKID_WITH_NAME(&ODB_global_mylock[j],lockname);
    }
    /* specific locks and/or first-time initializations */
    {
      extern void init_VAR_lock();
      init_VAR_lock();         /* odb/lib/var.c         */
    }
    {
      extern void init_HASHING_lock();
      init_HASHING_lock();     /* odb/lib/codb.c        */
    }
    {
      extern void init_FORFUNC_lock(); /* -- no locks were required -- */
      init_FORFUNC_lock();     /* odb/lib/forfunc.c     */
    }
    {
      extern void init_POOLREG_lock(); /* -- no locks were required -- */
      init_POOLREG_lock();     /* odb/lib/poolreg.c     */
    }
    {
      extern void init_CMDBKEYS_lock(); /* -- no locks were required -- */
      init_CMDBKEYS_lock();    /* odb/lib/cmdbkeys.c    */
    }
    {
      extern void init_CTX_lock(); /* -- no locks were required -- */
      init_CTX_lock();         /* odb/lib/ctx.c         */
    }
    {
      extern void init_PEINFO_lock(); /* -- no locks were required -- */
      init_PEINFO_lock();      /* odb/lib/peinfo.c      */
    }
    {
      extern void init_CMAIO_lock();
      init_CMAIO_lock();       /* odb/aux/cma_open.c    */
    }
    {
      extern void init_NEWIO_lock();
      init_NEWIO_lock();       /* odb/aux/newio.c       */
    }
    {
      extern void init_DCA_lock();
      init_DCA_lock();         /* odb/aux/dca.c         */
    }
    {
      extern void init_PBAR_lock();
      init_PBAR_lock();        /* odb/lib/tracing.c     */
    }
    {
      /* -- no locks were required -- */
      /* for protos: see include/regcache.h */
      (void) ODBc_init_eq_cache();         /* odb/lib/eq_reqions.c     */
      (void) ODBc_init_rgg_cache();        /* odb/lib/rgg_reqions.c     */
    }
    {
      extern void init_inside();
      init_inside(); /* odb/lib/inside.c */
    }
    /* Possible "first time"-calls on these routine */
    (void) odb_datetime_(NULL, NULL);              /* odb/lib/funcs.c       */
    (void) codb_versions_(NULL, NULL, NULL, NULL); /* odb/lib/versions.c    */
    codb_thin_init_();                             /* odb/lib/orlist.c      */
    codb_init_latlon_rad_();                       /* odb/lib/funcs.c       */
    (void) TableIsConsidered(NULL);                /* odb/lib/codb.c        */
    (void) TableIsWritable(NULL);                   /* odb/lib/codb.c        */
    init_RANDOM();                                 /* odb/lib/random.c      */
    (void) Run(NULL,NULL,NULL,NULL,NULL,false);    /* odb/lib/evaluate.c    */
    extern void InitCard();
    InitCard();                                    /* odb/aux/cardinality.c */
    (void) odb_resource_stamp_(NULL);              /* odb/lib/codb.c        */
    init_proc_funcs();                             /* odb/lib/funcs.c       */
    codb_init_twindow_();                          /* odb/lib/twindow.c     */
    first_time = false;
  }
}

PUBLIC void 
ODB_iolock(int onoff) {
  extern void iolockdb_(int *onoff); /* Fortran-routine */
  iolockdb_(&onoff);
}

PUBLIC const char *
odb_resource_stamp_(const char *label)
{ /* Now thread safe */
  char *s = NULL;
  int len = 0;
  int it = get_thread_id_();
  static struct {
    char *s;
    int  len;
  } *ps = NULL;
  if (!ps) {
    /* First time only */
    CALLOC(ps, max_omp_threads);
  }
  s = ps[--it].s;
  FREE(s);
  if (label) {
    const char *dtbuf = odb_datetime_(NULL,NULL); /* ptr to a static variable; do not FREE */
    extern long long int gethwm_(), getrss_();
    long long int mem = gethwm_()/1048576;
    long long int rss = getrss_()/1048576;
    len = STRLEN(label) + STRLEN(dtbuf) + 100;
    ALLOC(s,len);
    snprintf(s,len,"%s %s (%lld/%lldMB)",label,dtbuf,mem,rss);
  }
  else {
    s = STRDUP("");
    len = 0;
  }
  ps[it].s = s;
  ps[it].len = len;
  return s;
}


PUBLIC void
codb_datetime_(int *date_out, int *time_out)
{
  (void) odb_datetime_(date_out, time_out);
}


PUBLIC void
codb_analysis_datetime_(int *date_out, int *time_out)
{
  int Date=19700101, Time=000000;
  char *env_date, *env_time;
  boolean valid_date = 0;
  
  env_date = getenv("ODB_ANALYSIS_DATE");
  if (env_date) Date = atoi(env_date);

  env_time = getenv("ODB_ANALYSIS_TIME");
  if (env_time) Time = atoi(env_time);

  if (!env_date && !env_time) {
    /* Last resort: Try from the BASETIME environment variable */
    char *basetime = getenv("BASETIME");
    int len = basetime ? strlen(basetime) : 0;
    if (len >= 10) { /* Some hope now ... */
      int d,t;
      int nel = sscanf(basetime, "%8d%2d", &d, &t);
      if (nel == 2) {
	Date = d; /* YYYYMMDD */
	Time = t * 10000; /* HH0000 */
      }
    }
  }
  
  if (date_out) *date_out = Date;
  if (time_out) *time_out = Time;

  valid_date = ODB_twindow(Date, Time,
			   Date, Time,
			   0, 0);

  if (!valid_date) {
    fprintf(stderr,
	    "*** Warning: Invalid analysis date=%8.8d and/or time=%6.6d\n",
	    Date, Time);
  }
}


PUBLIC int
ODB_min_alloc()
{
  static int min_alloc = 0;
  if (min_alloc == 0) {
    char *p = getenv("ODB_MIN_ALLOC");
    if (p) min_alloc = atoi(p);
    if (min_alloc < 1) min_alloc = MIN_ALLOC;
  }
  return min_alloc;
}


PUBLIC int
ODB_inc_alloc(int nalloc, int min_alloc)
{
  static int inc_alloc = 0;
  int rc = 0;
  if (inc_alloc == 0) {
    char *p = getenv("ODB_INC_ALLOC");
    if (p) inc_alloc = atoi(p);
    if (inc_alloc < 1) inc_alloc = INC_ALLOC;
  }
  if (nalloc > min_alloc) rc = inc_alloc;
  return rc;
}


PUBLIC int
ODB_error(const char *where,
	  int how_much,
	  const char *what,
	  const char *srcfile,
	  int srcline,
	  int perform_abort)
{
  int rc = errno;
  
  fprintf(stderr,
          "ODB_error: %s has failed to allocate %d bytes for '%s' (in \"%s\":%d)\n",
          where, how_much, what,
          srcfile, srcline);

  if (perform_abort) {
    char msg[] = "ODB_error: Aborting ...";
    codb_abort_func_(msg, strlen(msg));
    exit(rc);
  }

  return rc;
}


PUBLIC void
codb_write_metadata_(const int  *handle,
		     const int  *iounit,
		     const int  *npools,
		     const int   CreationDT[],
		     const int   AnalysisDT[],
		     const double  *major,
		     const double  *minor,
		     const int  *io_method,
		     int *retcode)
{
  int rc = 0;
  double Version_Major;
  double Version_Minor;
  int Handle = *handle;
  int Npools = *npools;
  int IO_method = *io_method;
  FILE *fp = CMA_get_fp(iounit);
  ODB_Pool *p;
  DRHOOK_START(codb_write_metadata_);

  if (!fp) { rc = -1; goto finish; }

  (void) codb_versions_(&Version_Major, &Version_Minor, NULL, NULL); /* preset */
  if (major && *major >= 0) Version_Major = *major;
  if (minor && *minor >= 0) Version_Minor = *minor;

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      int Ntables = 0;
      int CrDate = CreationDT[0];
      int CrTime = CreationDT[1];
      int UDate, UTime;
      int AnDate = AnalysisDT[0];
      int AnTime = AnalysisDT[1];
      ODB_Funcs *pf;

      FORFUNC { if (PFCOM->is_table) Ntables++; }

      codb_datetime_(&UDate, &UTime);

      fprintf(fp,"%.0f %.3f %d\n",Version_Major, Version_Minor, IO_method);
      fprintf(fp,"%8.8d %6.6d\n",CrDate, CrTime); /* Date created */
      fprintf(fp,"%8.8d %6.6d\n",UDate, UTime);   /* Date last updated */
      fprintf(fp,"%8.8d %6.6d\n",AnDate, AnTime); /* Analysis date */
      fprintf(fp,"%d\n",Npools);
      fprintf(fp,"%d\n",Ntables);

      rc = Ntables;

      break; /* The first occurence would do */
    }
  } /* FORPOOL */

 finish:
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_write_metadata2_(const int  *handle,
		      const int  *iounit,
		      const int  *npools,
		      const int  *ntables,
		      /* const int   fsize[], */ /* Npools x Ntables */
		      int *retcode)
{
  int rc = 0;
  int Handle = *handle;
  int Npools = *npools;
  /* int Ntables = *ntables; */
  FILE *fp = CMA_get_fp(iounit);
  ODB_Pool *p;
  DRHOOK_START(codb_write_metadata2_);

  if (!fp) { rc = -1; goto finish; }

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      ODB_Funcs *pf;
      int j, k = 0;

      FORFUNC {
	if (PFCOM->is_table) {
	  char *name = PFCOM->name;
	  k++;
	  /*
	  fprintf(fp, "%d %s %d", k, name, Npools);
	  for (j=0; j<Npools; j++) {
	    fprintf(fp, " %d",fsize[(k-1)*Npools + j]); 
	  }
	  fprintf(fp, "\n"); 
	  */
	  /* We don't care about file sizes anymore */
	  fprintf(fp, "%d %s -1\n", k, name);
	}
      }

      rc = k;

      FORFUNC {
	if (PFCOM->is_table) {
	  int ncols = PFCOM->ncols;
	  char *name = PFCOM->name;
	  const ODB_Tags *tags = PFCOM->tags;
	  fprintf(fp, "%s %d\n", name, ncols);
	  for (k=0; k<ncols; k++) {
	    int i, nmem = tags[k].nmem;
	    if (nmem > 0) {
	      const char *m = tags[k].memb[nmem-1];
	      if (strequ(m,UNUSED)) nmem--;
	    }
	    fprintf(fp,"  %s  %d\n", tags[k].name, nmem);
	    for (i=0; i<nmem; i++) fprintf(fp,"    %s\n",tags[k].memb[i]);
	  }
	}
      }

      break; /* The first occurence would do */
    }
  } /* FORPOOL */

 finish:
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_write_metadata3_(const int  *handle,
		      const int  *iounit,
		      int *retcode)
     /* Write SET-information */
{
  int rc = 0;
  int Handle = *handle;
  FILE *fp = CMA_get_fp(iounit);
  ODB_Pool *p;
  DRHOOK_START(codb_write_metadata3_);

  if (!fp) { rc = -1; goto finish; }

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      char *dbname = p->dbname;
      ODB_Setvar *setvar = NULL;
      int nsetvar = ODB_get_vars(dbname, NULL, 0, 0, NULL);
      fprintf(fp,"%d\n",nsetvar);
      if (nsetvar > 0) {
	ALLOC(setvar, nsetvar);
	rc = ODB_get_vars(dbname, NULL, 0, nsetvar, setvar);
	if (rc == nsetvar) {
	  int j;
	  for (j=0; j<nsetvar; j++) {
	    fprintf(fp,"%s %.14g\n",setvar[j].symbol,setvar[j].value);
	    FREE(setvar[j].symbol);
	  }
	}
	else {
	  rc = -2; 
	}
	FREE(setvar);
	if (rc < 0) goto finish; 
      }

      break; /* The first occurence would do */
    }
  } /* FORPOOL */

 finish:
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_read_metadata_(const int  *handle,
		    const int  *iounit,
		    int  *npools,
		    int   CreationDT[],
		    int   AnalysisDT[],
		    double  *major,
		    double  *minor,
		    int  *io_method,
		    int  *retcode)
{
  int rc = 0;
  /* int Handle = *handle; */
  double Version_Major, Def_Version_Major;
  double Version_Minor, Def_Version_Minor;
  int IO_method = 1;
  int Npools = 0;
  int Ntables = 0;
  int CrDate, CrTime;
  int UDate, UTime;
  int AnDate, AnTime;
  int nel;
  char line[256];
  FILE *fp = CMA_get_fp(iounit);
  DRHOOK_START(codb_read_metadata_);

  if (!fp) { rc = -1; goto finish; }

  (void) codb_versions_(&Def_Version_Major, &Def_Version_Minor, NULL, NULL); /* preset */

  if (fgets(line, sizeof(line), fp)) {
    nel = sscanf(line,"%lf %lf %d",&Version_Major, &Version_Minor, &IO_method);
    if (io_method) {
      if (nel >= 3) { /* since-25r4 */
	*io_method = IO_method;
      }
      else {
	*io_method = 1; /* the only I/O-method prior to 25r4 */
      }
    }
  }
  else { rc = -2; goto finish; }
/*
  if (Version_Major != Def_Version_Major ||
      Version_Minor != Def_Version_Minor) {
       fprintf(stderr,
	    "***Warning: Recovering from software version mismatch: Expected CY%.0fR%.3f, found=CY%.0fR%.3f\n",
	    Def_Version_Major, Def_Version_Minor,
	    Version_Major, Version_Minor);
  }
*/
  fscanf(fp,"%d %d\n",&CrDate, &CrTime); /* Date created */
  fscanf(fp,"%d %d\n",&UDate, &UTime);   /* Date last updated/accessed */
  fscanf(fp,"%d %d\n",&AnDate, &AnTime); /* Analysis date */
  fscanf(fp,"%d\n",&Npools);
  fscanf(fp,"%d\n",&Ntables);

  if (CreationDT) {
    CreationDT[0] = CrDate;
    CreationDT[1] = CrTime;
  }

  if (AnalysisDT) {
    AnalysisDT[0] = AnDate;
    AnalysisDT[1] = AnTime;
  }

  if (major) *major = Version_Major;
  if (minor) *minor = Version_Minor;

  if (npools) *npools = Npools;
  rc = Ntables;

 finish:
  if (retcode) *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_read_metadata2_(const int  *handle,
		     const int  *iounit,
		     const int  *npools,
		     const int  *ntables,
		     /*      int   fsize[], *//* Npools x Ntables */
		     int *retcode)
{
  int rc = 0;
  int Handle = *handle;
  int Npools = *npools;
  /* int Ntables = *ntables; */
  FILE *fp = CMA_get_fp(iounit);
  ODB_Pool *p;
  DRHOOK_START(codb_read_metadata2_);

  if (!fp) { rc = -1; goto finish; }

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      ODB_Funcs *pf;
      char tname[MAXVARLEN+1];
      int j, k = 0;

      FORFUNC {
	if (PFCOM->is_table) {
	  const char *name = PFCOM->name;
	  int kchk, npchk;
	  k++;
	  fscanf(fp, "%d %s %d", &kchk, tname, &npchk);
	  if (kchk != k || !strequ(tname,name)) {
	    /* Error */
	    rc = -2;
	    goto finish;
	  }
	  /*
	  for (j=0; j<Npools; j++) {
	    fscanf(fp, "%d",&fsize[(k-1)*Npools + j]); 
	  }
	  */
	}
      }

      rc = k;

      break; /* The first occurence would do */
    }
  } /* FORPOOL */

 finish:
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_envtransf_(const char *in,
		      char *out,
		       int *retcode,
		/* Hidden arguments */
		      int  in_len,
		const int  out_len)
{
  int rc = 0;
  DECL_FTN_CHAR(in);
  DECL_FTN_CHAR(out);
  DRHOOK_START(codb_envtransf_);

  if (out_len > 0) {
    extern char *IOresolve_env(const char *str); /* from libioassign.a */
    char *s;
    ALLOC_FTN_CHAR(in);
    ALLOC_OUTPUT_FTN_CHAR(out);

    s = IOresolve_env(p_in);
    if (!s) s = STRDUP(p_in);

    rc = strlen(s);
    rc = MIN(rc, out_len);
    strncpy(p_out, s, rc);
    p_out[rc] = '\0';
    FREE(s);

    COPY_2_FTN_CHAR(out);
    FREE_FTN_CHAR(out);
    FREE_FTN_CHAR(in);
  }

  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_pc_filter_(const char *in,
		char *out,
		const int *procid,
		int *retcode,
		/* Hidden arguments */
		int in_len,
		const int out_len)
{
  int rc = 0;
  DRHOOK_START(codb_pc_filter_);

  if (out_len > 0) {
    char *s;
    DECL_FTN_CHAR(in);
    DECL_FTN_CHAR(out);

    ALLOC_FTN_CHAR(in);
    ALLOC_OUTPUT_FTN_CHAR(out);

    if (strchr(p_in,'%')) {
      rc = in_len + 50;
      ALLOC(s,rc);
      sprintf(s,p_in,*procid);
    }
    else {
      s = STRDUP(p_in);
    }

    rc = strlen(s);
    rc = MIN(rc, out_len);
    strncpy(p_out, s, rc);
    p_out[rc] = '\0';
    FREE(s);

    COPY_2_FTN_CHAR(out);
    FREE_FTN_CHAR(out);
    FREE_FTN_CHAR(in);
  }

  *retcode = rc;
  DRHOOK_END(0);
}

/* An error message print-out (printed from many places; unified here) */

PUBLIC void
ODB_check_considereness(int is_considered,
			const char *tblname)
{
  if (!is_considered) { 
    /* msgpass_loaddata() didn't load the table and yet it is now requested ==> abort */
    fprintf(stderr,
	    "***Error: Table '%s' has not been considered. Please check your ODB_CONSIDER_TABLES\n",
	    tblname);
    RAISE(SIGABRT); 
  }
}


PUBLIC void
codb_consider_table_(const int *handle,
		     const char *tblname, /* Must begin with '@' in front of */
		     int *option_code,
		     int *retcode,
		     /* Hidden arguments */
		     int tblname_len)
{
  int rc = -1;
  int Handle = *handle;
  int Opcode = *option_code; /* -1=get info, 1/0=set consider/not_consider'ness */
  DECL_FTN_CHAR(tblname);
  DRHOOK_START(codb_consider_table_);

  ALLOC_FTN_CHAR(tblname);

  if (IS_TABLE(p_tblname) && (Opcode == -1 || Opcode == 0 || Opcode == 1)) {
    boolean all_done = 0;
    int Poolno = -1;
    boolean AnyPool = (Poolno == -1);
    int it = get_thread_id_();
    ODB_Pool *p;
    const int off = 0;
    int oldvalue;

    /* switch off poolmasking [for this thread only] temporarely */
    codb_toggle_poolmask_(&Handle, &off, &oldvalue);

    FORPOOL {
      if (MATCHING) {
	ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_tblname, 1);

	if (pf && PFCOM->is_table && strequ(PFCOM->name, p_tblname)) {
	  rc = PFCOM->is_considered; /* get existing value for rc */
	  if (Opcode == -1) { /* get info */
	    all_done = 1;
	  }
	  else if (Opcode == 0 || Opcode == 1) { /* set value */
	    PFCOM->is_considered = Opcode; /* Update */
	  }
	} /* if (pf && PFCOM->is_table && strequ(PFCOM->name, p_tblname)) */

	if (all_done) break; /* FORPOOL */
      } /* if (MATCHING) */
    } /* FORPOOL */

    /* reset poolmasking status to whatever it was before */
    codb_toggle_poolmask_(&Handle, &oldvalue, NULL);

  } /* if (IS_TABLE(p_tblname) && (Opcode == ... */

  FREE_FTN_CHAR(tblname);
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC int
TableIsConsidered(const char *tblname)
{ /* Generally a non-thread safe code, 
     but the non-thread safe part called upon codb_init_omp_locks() */
  static char *env = NULL;
  static int envlen = 0;
  int rc = 0;
  if (!env) { /* the non-thread safe part */
    char *p = getenv("ODB_CONSIDER_TABLES");
    /* Skip leading blanks */
    if (p) {
      while (*p) {
	if (!isspace(*p)) break;
	++p;
      }
    }
    env = p ? STRDUP(p) : STRDUP("*");
    p = env;
    while (*p) {
      if (isupper(*p)) tolower(*p);
      ++p;
    }
    envlen = strlen(env);
    /* Ignore trailing blanks */
    p = &env[envlen-1];
    while (envlen > 0) {
      if (!isspace(*p)) break;
      --p;
      --envlen;
    }
    env[envlen] = '\0';
    {
      int myproc = 0, it = 0;
      codb_procdata_(&myproc, NULL, NULL, &it, NULL);
      if (myproc == 1 && it == 1) {
	fprintf(stderr,"ODB_CONSIDER_TABLES=%s\n",env);
      }
    }
  }
  if (tblname) {
    if (*tblname == '@') tblname++;
    if (strequ(env,"*")) {
      rc = 1;
    }
    else {
      int len = strlen(tblname);
      char *teststr = NULL;
      ALLOC(teststr,len + 3);
      snprintf(teststr,len+3,"/%s/",tblname);
      if (*env == '*' && envlen > 1) { /* "* except /tb1/tb2/" or "* /tb1/tb2/" or "* but /tb1/tb2/" */
	if (!strstr(env,teststr)) rc = 1;
      }
      else {
	if (strstr(env,teststr)) rc = 1;
      }
      FREE(teststr);
    }
  }
  return rc;
}

PUBLIC void
codb_table_is_considered_(const char *tblname,
			  int *retcode,
			  /* Hidden arguments */
			  int tblname_len)
{
  if (retcode) {
    DECL_FTN_CHAR(tblname);
    ALLOC_FTN_CHAR(tblname);
    *retcode = TableIsConsidered(p_tblname);
    FREE_FTN_CHAR(tblname);
  }
}

/* Anne Fouilloux 23/02/10 add possibility to write only a subset of tables via ODB_WRITE_TABLES */


PUBLIC void
codb_write_table_(const int *handle,
		     const char *tblname, /* Must begin with '@' in front of */
		     int *option_code,
		     int *retcode,
		     /* Hidden arguments */
		     int tblname_len)
{
  int rc = -1;
  int Handle = *handle;
  int Opcode = *option_code; /* -1=get info, 1/0=set write/not_write'ness */
  DECL_FTN_CHAR(tblname);
  DRHOOK_START(codb_consider_table_);

  ALLOC_FTN_CHAR(tblname);

  if (IS_TABLE(p_tblname) && (Opcode == -1 || Opcode == 0 || Opcode == 1)) {
    boolean all_done = 0;
    int Poolno = -1;
    boolean AnyPool = (Poolno == -1);
    int it = get_thread_id_();
    ODB_Pool *p;
    const int off = 0;
    int oldvalue;

    /* switch off poolmasking [for this thread only] temporarely */
    codb_toggle_poolmask_(&Handle, &off, &oldvalue);

    FORPOOL {
      if (MATCHING) {
	ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_tblname, 1);

	if (pf && PFCOM->is_table && strequ(PFCOM->name, p_tblname)) {
	  rc = PFCOM->is_writable; /* get existing value for rc */
	  if (Opcode == -1) { /* get info */
	    all_done = 1;
	  }
	  else if (Opcode == 0 || Opcode == 1) { /* set value */
	    PFCOM->is_writable = Opcode; /* Update */
	  }
	} /* if (pf && PFCOM->is_table && strequ(PFCOM->name, p_tblname)) */

	if (all_done) break; /* FORPOOL */
      } /* if (MATCHING) */
    } /* FORPOOL */

    /* reset poolmasking status to whatever it was before */
    codb_toggle_poolmask_(&Handle, &oldvalue, NULL);

  } /* if (IS_TABLE(p_tblname) && (Opcode == ... */

  FREE_FTN_CHAR(tblname);
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC int
TableIsWritable(const char *tblname)
{ /* Generally a non-thread safe code, 
     but the non-thread safe part called upon codb_init_omp_locks() */
  static char *env = NULL;
  static int envlen = 0;
  int rc = 0;
  if (!env) { /* the non-thread safe part */
    char *p = getenv("ODB_WRITE_TABLES");
    /* Skip leading blanks */
    if (p) {
      while (*p) {
	if (!isspace(*p)) break;
	++p;
      }
    }
    env = p ? STRDUP(p) : STRDUP("*");
    p = env;
    while (*p) {
      if (isupper(*p)) tolower(*p);
      ++p;
    }
    envlen = strlen(env);
    /* Ignore trailing blanks */
    p = &env[envlen-1];
    while (envlen > 0) {
      if (!isspace(*p)) break;
      --p;
      --envlen;
    }
    env[envlen] = '\0';
    {
      int myproc = 0, it = 0;
      codb_procdata_(&myproc, NULL, NULL, &it, NULL);
      if (myproc == 1 && it == 1) {
	fprintf(stderr,"ODB_WRITE_TABLES=%s\n",env);
      }
    }
  }
  if (tblname) {
    if (*tblname == '@') tblname++;
    if (strequ(env,"*")) {
      rc = 1;
    }
    else {
      int len = strlen(tblname);
      char *teststr = NULL;
      ALLOC(teststr,len + 3);
      snprintf(teststr,len+3,"/%s/",tblname);
      if (*env == '*' && envlen > 1) { /* "* except /tb1/tb2/" or "* /tb1/tb2/" or "* but /tb1/tb2/" */
	if (!strstr(env,teststr)) rc = 1;
      }
      else {
	if (strstr(env,teststr)) rc = 1;
      }
      FREE(teststr);
    }
  }
  return rc;
}

PUBLIC void
codb_table_is_writable_(const char *tblname,
			  int *retcode,
			  /* Hidden arguments */
			  int tblname_len)
{
  if (retcode) {
    DECL_FTN_CHAR(tblname);
    ALLOC_FTN_CHAR(tblname);
    *retcode = TableIsWritable(p_tblname);
    FREE_FTN_CHAR(tblname);
  }
}

/* End Anne fouilloux 23/02/10 */

PUBLIC void 
codb_create_pool_(const int  *handle,
		  const char *dbname,
		  const int  *poolno,
		  const int  *is_new,
		  const int  *io_method,
		  const int  *add_vars,
		  int *retcode,
		  /* Hidden arguments */
		  int dbname_len)
{
  int rc = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  int Is_new = *is_new;
  int IO_method = *io_method;
  int Add_vars = *add_vars;
  DECL_FTN_CHAR(dbname);
  DRHOOK_START(codb_create_pool_);

  ALLOC_FTN_CHAR(dbname);

  if (Poolno > 0) { 
    ODB_Pool *pool = ODB_create_pool(Handle, p_dbname, Poolno, Is_new, IO_method, Add_vars);
    if (!pool) { 
      rc = -Poolno; 
      goto finish; 
    }
    rc = Poolno;
  }

 finish:
  FREE_FTN_CHAR(dbname);

  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_close_(const int *handle,
	    const int *save,
	          int *retcode)
{
  int rc = 0;
  int nbytes = 0;
  int Handle = *handle;
  int Save = *save;
  char *dbname = NULL;
  ODB_Pool *p;
  int trace_on = 0;
  DRHOOK_START(codb_close_);

  trace_on = FREEPTR_TRACE(stdout, codb_close_, 1); /* PTR-trace ON, if ODB_MEMORY_FREEPTR_TRACE=1 */

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      ODB_Funcs *pf;

      if (Save) {
	rc = p->store(p, 0);
	if (rc < 0) goto finish;
	nbytes += rc;
      }

      FORFUNC { /* correct coding (as of 9-Jan-2004/SS) */
	PFCOM->swapout(pf->data);
	DELETE_INTERMED(pf->tmp);
	/* if (trace_on) */ FREE(pf->data);
      }

      /* Remove the funcs-chain */
      {
	for (pf = p->funcs; pf; ) {
	  ODB_Funcs *next_pf = pf->next;
	  nullify_forfunc(p->handle, p->dbname, p->poolno, pf->it, PFCOM->name);
	  FREE(pf);
	  pf = next_pf;
	} /* for (pf = p->funcs; pf; ) */
	p->funcs = NULL;
      }

      if (!dbname) dbname = STRDUP(p->dbname);

      FREE(p->dbname);
      FREE(p->srcpath);

      p->inuse = 0;

      put_poolreg(p->handle, p->poolno, NULL);
    }
  } /* FORPOOL */

  trace_on = FREEPTR_TRACE(stdout, codb_close_, 0); /* PTR-trace OFF */

  if (dbname) {
    /* Unlink from dynamic views */
    rc = ODB_undo_dynlink(dbname, 1);
    if (rc < 0) goto finish;

    /* Unlink from DBase itself */
    rc = ODB_undo_dynlink(dbname, 0);
    if (rc < 0) goto finish;

    FREE(dbname);
  }

  (void) DCA_free(Handle);
 finish:
  *retcode = (rc < 0) ? rc : nbytes;
  DRHOOK_END(0);
}


PUBLIC void
codb_swapout_(const int  *handle,
	      const int  *poolno,
	      const char *dataname,
	      const int  *save,
	      int  *retcode,
	      const int  *delete_intermed,
	      const int  *using_it,
	      /* Hidden arguments */
	      int dataname_len)
{
  int rc = 0;
  int nbytes = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  int DeleteIntermed = *delete_intermed;
  boolean AnyPool = (Poolno == -1);
  boolean Save = *save;
  POOLREG_DEF;
  int it = USING_IT;
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_swapout_);

  ALLOC_FASTFTN_CHAR(dataname);

  Save &= IS_TABLE(p_dataname);

  POOLREG_FOR {
    if (MATCHING) { 
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);
      
      if (pf) {
	void *data = pf->data;
	  
	if (Save) {
	  rc = PFCOM->store(data);
	  if (rc < 0) goto finish;
	  nbytes += rc;
	}
	
	PFCOM->swapout(pf->data); /* Swap out */
	if (DeleteIntermed) DELETE_INTERMED(pf->tmp);
      } /* if (pf) */
    }
    POOLREG_BREAK;
  } /* POOLREG_FOR */

 finish:  
  FREE_FASTFTN_CHAR(dataname);

  *retcode = (rc < 0) ? rc : nbytes;
  DRHOOK_END(0);
}


PUBLIC void
codb_remove_(const int  *handle,
	     const int  *poolno,
	     const char *dataname,
	     int  *retcode,
	     /* Hidden arguments */
	     int dataname_len)
{
  int rc = 0;
  int nbytes = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  boolean AnyPool = (Poolno == -1);
  POOLREG_DEF;
  int it = get_thread_id_();
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_remove_);

  ALLOC_FASTFTN_CHAR(dataname);

  if (!IS_TABLE(p_dataname)) goto finish;

  POOLREG_FOR {
    if (MATCHING) { 
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);
      
      if (pf) {
	void *data = pf->data;
	rc = PFCOM->remove(data);  /* Remove data table and fill it with an empty dataset */
	if (rc < 0) goto finish;
	nbytes += rc;
      } /* if (pf) */
    }
    POOLREG_BREAK;
  } /* POOLREG_FOR */

 finish:  
  FREE_FASTFTN_CHAR(dataname);

  *retcode = (rc < 0) ? rc : nbytes;
  DRHOOK_END(0);
}


PUBLIC void
codb_store_(const int *handle,
	    const int *poolno,
	    const int *io_method,
	    int *retcode)
{
  int rc = 0;
  int nbytes = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  int IO_method = *io_method;
  boolean AnyPool = (Poolno == -1);
  POOLREG_DEF;
  int it = get_thread_id_();
  DRHOOK_START(codb_store_);

  POOLREG_FOR {
    if (MATCHING) {
      rc = p->store(p, IO_method);
      if (rc < 0) goto finish;
      nbytes += rc;
    }
    POOLREG_BREAK;
  } /* POOLREG_FOR */

 finish:
  *retcode = (rc < 0) ? rc : nbytes;
  DRHOOK_END(0);
}
	
  
PUBLIC void
codb_load_(const int *handle,
	   const int *poolno,
	   const int *io_method,
	   int *retcode)
{
  int rc = 0;
  int nbytes = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  int IO_method = *io_method;
  boolean AnyPool = (Poolno == -1);
  POOLREG_DEF;
  int it = get_thread_id_();
  DRHOOK_START(codb_load_);

  POOLREG_FOR {
    if (MATCHING) {
      rc = p->load(p, IO_method);
      if (rc < 0) goto finish;
      nbytes += rc;
    }
    POOLREG_BREAK;
  } /* POOLREG_FOR */

 finish:
  *retcode = (rc < 0) ? rc : nbytes;
  DRHOOK_END(0);
}
	
  

PUBLIC void
codb_select_(const int *handle,
             const int *poolno,
	     const char *dataname,
	     int *nrows,
	     int *ncols,
	     int *retcode,
	     const int *inform_progress,
	     const int *using_it,
	     /* Hidden arguments */
	     int dataname_len)
{
  int rc = 0;
  int Handle = *handle;
  int Nrows = 0;
  int Ncols = 0;
  int Pbar = *inform_progress; /* whether to invoke codb_progress_bar_() or not */
  int Poolno = *poolno;
  boolean AnyPool = (Poolno == -1);
  POOLREG_DEF;
  int it = USING_IT;
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_select_);

  ALLOC_FASTFTN_CHAR(dataname);

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);

      if(pf) {
	void *data = pf->data;
	int nc = PFCOM->ncols;
	extern double util_walltime_();
	double zwall[2];

	if (Pbar) zwall[0] = util_walltime_();
	
	rc = PFCOM->select(data, NULL, -1, NULL);
	
	if (rc < 0) goto finish;
	
	Nrows += rc;
	Ncols  = nc;

	if (Pbar) {
	  double wtime;
	  zwall[1] = util_walltime_();
	  wtime = zwall[1] - zwall[0];
	  codb_progress_bar_(NULL,
			     p_dataname,
			     &p->poolno,
			     NULL,
			     &rc,
			     NULL,
			     &wtime,
			     NULL,
			     strlen(p_dataname));
	}
      } /* if (pf) */
    } /* if (MATCHING) */
    POOLREG_BREAK;
  } /* POOLREG_FOR */

 finish:

  if (rc >=0 && Ncols == 0) { 
    /* If no match so far, then must get the correct Ncols */
    const int some_pool = -1;
    int dummy;
    codb_getsize_(handle,
		  &some_pool,
		  p_dataname,
		  &dummy,
		  &Ncols,
		  NULL,
		  NULL,
		  &it,
		  /* Hidden arguments */
#ifdef USE_CTRIM
		  strlen(p_dataname)+1
#else
		  strlen(p_dataname)
#endif
		  );
  }

  FREE_FASTFTN_CHAR(dataname);

  *nrows = Nrows;
  *ncols = Ncols;

  *retcode = (rc >= 0) ? Nrows : rc;
  DRHOOK_END(Nrows);
}


void
codb_mp_select_(const int *handle,
		const int *poolno,
		const char *dataname,
		int nrows[],
		int *ncols,
		int *retcode,
		const char *pevar,
		const int *npes,
		const int *replicate_PE,
		const int *using_it,
		/* Hidden arguments */
		int dataname_len,
		int pevar_len)
{
  int rc = 0;
  int Handle = *handle;
  int Nrows = 0;
  int Ncols = 0;
  int PE, Npes;
  int Poolno = *poolno;
  boolean is_table = 0;
  boolean AnyPool = (Poolno == -1);
  double oldvalue = 0;
  char *dbname = NULL;
  ODB_PE_Info PEinfo;
  POOLREG_DEF;
  int it = USING_IT;
  DECL_FTN_CHAR(dataname);
  DECL_FTN_CHAR(pevar);
  DRHOOK_START(codb_mp_select_);

  ALLOC_FASTFTN_CHAR(dataname);
  ALLOC_FASTFTN_CHAR(pevar);

  Npes = *npes;
  for (PE=1; PE<=Npes; PE++) nrows[PE-1] = 0;

  is_table = IS_TABLE(p_dataname);

  dbname = NULL;
  PEinfo.npes = Npes;
  ALLOC(PEinfo.nrowvec, PEinfo.npes);
  for (PE=1; PE<=Npes; PE++) PEinfo.nrowvec[PE-1] = 0;
  PEinfo.varname = p_pevar;
  PEinfo.varname_len = strlen(p_pevar);
  PEinfo.addr = NULL;
  PEinfo.replicate_PE = *replicate_PE;

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);
	
      if (pf) {
	void *data = pf->data;
	int nc;
	
	dbname = STRDUP(p->dbname);
	PEinfo.addr = ODB_alter_var(dbname, p_pevar, 
				    is_table ? NULL : p_dataname,
				    it,
				    NULL, &oldvalue,
				    0);

	rc = PFCOM->select(data, &PEinfo, -1, NULL);

	if (rc < 0) goto finish;
	
	Nrows += rc;
	
	nc = PFCOM->ncols;
	Ncols = nc;

	if (PEinfo.addr) {
	  (void) ODB_alter_var(dbname, p_pevar, 
			       is_table ? NULL : p_dataname,
			       it,
			       &oldvalue, NULL,
			       0);
	}
      } /* if (pf) */
    } /* if (MATCHING) */
    POOLREG_BREAK;
  } /* POOLREG_FOR */
    
 finish:
  if (rc >= 0) {
    Npes = PEinfo.npes;
    for (PE=1; PE<=Npes; PE++) nrows[PE-1] = PEinfo.nrowvec[PE-1];
    FREE(PEinfo.nrowvec);
    FREE(dbname);
  }
  else {
    nrows[0] = Nrows;
  }

  if (rc >=0 && Ncols == 0) { 
    /* If no match so far, then must get the correct Ncols */
    const int some_pool = -1;
    int dummy;
    codb_getsize_(handle,
		  &some_pool,
		  p_dataname,
		  &dummy,
		  &Ncols,
		  NULL,
		  NULL,
		  &it,
		  /* Hidden arguments */
#ifdef USE_CTRIM
		  strlen(p_dataname)+1
#else
		  strlen(p_dataname)
#endif
		  );
  }

  *ncols = Ncols;

  FREE_FASTFTN_CHAR(dataname);
  FREE_FASTFTN_CHAR(pevar);
    
  *retcode = (rc >= 0) ? Nrows : rc;
  DRHOOK_END(Nrows);
}


PUBLIC void
codb_get_npes_(const int *handle,
	       const int *poolno,
	       const char *dataname,
	       int *replicate_PE,
	       int *retcode,
	       const int *using_it,
	       /* Hidden arguments */
	       int dataname_len)
{
  int rc = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  int Npes = 0;
  int Replicate_PE = 0;
  boolean AnyPool = (Poolno == -1);
  POOLREG_DEF;
  int it = USING_IT;
  DECL_FTN_CHAR(dataname);

  ALLOC_FASTFTN_CHAR(dataname);

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);

      if (pf) {
	void *data = pf->data;
	ODB_PE_Info PEinfo;
	PFCOM->peinfo(data, &PEinfo);
	Npes = MAX(Npes, PEinfo.npes);
	Replicate_PE = PEinfo.replicate_PE;
      } /* if (pf) */
    } /* MATCHING */
    POOLREG_BREAK;
  } /* POOLREG_FOR */

  /* finish: */
  FREE_FASTFTN_CHAR(dataname);

  *replicate_PE = Replicate_PE;

  *retcode = (rc >= 0) ? Npes : rc;
}


PUBLIC void
codb_get_rowvec_(const int *handle,
		 const int *poolno,
		 const char *dataname,
		 int  nrows[],
		 const int *npes,
		 int *retcode,
		 const int *using_it,
		 /* Hidden arguments */
		 int dataname_len)
{
  int rc = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  int Npes = *npes;
  int PE;
  boolean AnyPool = (Poolno == -1);
  POOLREG_DEF;
  int it = USING_IT;
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_get_rowvec_);

  ALLOC_FASTFTN_CHAR(dataname);

  for (PE=1; PE<=Npes; PE++) nrows[PE-1] = 0;

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);

      if (pf) {
	void *data = pf->data;
	ODB_PE_Info PEinfo;
	PFCOM->peinfo(data, &PEinfo);
	if (PEinfo.npes > 0 && PEinfo.nrowvec) {
	  Npes = MIN(*npes, PEinfo.npes);
	  for (PE=1; PE<=Npes; PE++) {
	    nrows[PE-1] += PEinfo.nrowvec[PE-1];
	  }
	} /* if (PEinfo.npes > 0 && PEinfo.nrowvec) */
      } /* if (pf) */
    } /* MATCHING */
    POOLREG_BREAK;
  } /* POOLREG_FOR */

  /* finish: */
  FREE_FASTFTN_CHAR(dataname);

  *retcode = (rc >= 0) ? *npes : rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_cancel_(const int  *handle,
             const int  *poolno,
	     const char *dataname,
	     int *retcode,
	     const int  *delete_intermed,
	     const int *using_it,
	     /* Hidden arguments */
	     int dataname_len)
{
  int rc = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  int DeleteIntermed = *delete_intermed;
  int is_table;
  boolean AnyPool = (Poolno == -1);
  POOLREG_DEF;
  int it = USING_IT;
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_cancel_);

  ALLOC_FASTFTN_CHAR(dataname);

  is_table = IS_TABLE(p_dataname);

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);

      if (pf) {
	void *data = pf->data;
	if (PFCOM->cancel) PFCOM->cancel(data);
	if (DeleteIntermed) DELETE_INTERMED(pf->tmp);
      } /* if (pf) */
    } /* if (MATCHING) */
    POOLREG_BREAK;
  } /* POOLREG_FOR */

  /* finish: */
  FREE_FASTFTN_CHAR(dataname);

  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_getindex_(const int *handle,
	       const int  *poolno,
	       const char *viewname,
	       const char *tablename,
	       const int *idxlen,
	       int idx[],
	       int *retcode,
	       const int *using_it,
	       /* Hidden arguments */
	       int viewname_len,
	       int tablename_len)
{
  int rc = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  boolean AnyPool = (Poolno == -1); /* Actually works only for one pool at a time */
  POOLREG_DEF;
  int it = USING_IT;
  DECL_FTN_CHAR(viewname);
  DECL_FTN_CHAR(tablename);
  DRHOOK_START(codb_getindex_);

  ALLOC_FASTFTN_CHAR(viewname);
  ALLOC_FASTFTN_CHAR(tablename);

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_viewname, 1);

      if (pf) {
	void *data = pf->data;
	int *this_idx = PFCOM->getindex(data, p_tablename, &rc);
	if (rc <= *idxlen && this_idx) {
	  int i;
	  for (i=0; i<rc; i++) {
	    idx[i] = this_idx[i];
	  }
	}
	else if (!this_idx) {
	  rc = -2; /* No index vector was not found */
	  goto finish;
	}
	else {
	  rc = -1; /* Not enough space left for outgoing index-vector */
	  goto finish;
	}
	break;  /* for now (... since works only for one pool at a time) */
      } /* if (pf) */
    } /* if (MATCHING) */
    POOLREG_BREAK;
  } /* POOLREG_FOR */

 finish:
  FREE_FASTFTN_CHAR(viewname);
  FREE_FASTFTN_CHAR(tablename);

  *retcode = rc;
  DRHOOK_END(0);
}

/* 
   A very dangerous routine: 
   == With the following routine you can easily destroy/crash
   your database handling software 

 */

PUBLIC void
codb_putindex_(const int *handle,
	       const int  *poolno,
	       const char *viewname,
	       const char *tablename,
	       const int *idxlen,
	       const int idx[],
	       int *retcode,
	       const int *using_it,
	       /* Hidden arguments */
	       int viewname_len,
	       int tablename_len)
{
  int rc = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  boolean AnyPool = (Poolno == -1); /* Actually works only for one pool at a time */
  POOLREG_DEF;
  int it = USING_IT;
  DECL_FTN_CHAR(viewname);
  DECL_FTN_CHAR(tablename);
  DRHOOK_START(codb_putindex_);

  ALLOC_FASTFTN_CHAR(viewname);
  ALLOC_FASTFTN_CHAR(tablename);

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_viewname, 1);

      if (pf) {
	if (*idxlen > 0) {
	  void *data = pf->data;
	  rc = PFCOM->putindex(data, p_tablename, *idxlen, (int *)idx, 0);
	  break; /* for now (... since works only for one pool at a time) */
	}
      } /* if (pf) */
    } /* if (MATCHING) */
    POOLREG_BREAK;
  } /* POOLREG_FOR */

  /* finish: */
  FREE_FASTFTN_CHAR(viewname);
  FREE_FASTFTN_CHAR(tablename);

  *retcode = rc;
  DRHOOK_END(0);
}

/* Put & Get functions */

#undef REAL_VERSION

#define REAL_VERSION 8
#include "codb.h"
#undef REAL_VERSION


PUBLIC void
codb_getsize_(const int *handle,
              const int *poolno,
	      const char *dataname,
	      int *nrows,
	      int *ncols,
	      const int *recur,
	      int *retcode,
	      const int *using_it,
	      /* Hidden arguments */
	      int dataname_len)
{
  int rc = 0;
  int Recur = recur ? *recur : 0;
  int Handle = *handle;
  int Poolno = *poolno;
  boolean AnyPool = (Poolno == -1);
  POOLREG_DEF;
  int it = USING_IT;
  int Nrows = 0;
  int Ncols = 0;
  DECL_FTN_CHAR(dataname);
  DRHOOK_START_RECUR(codb_getsize_,Recur);

  ALLOC_FASTFTN_CHAR(dataname);

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);

      if (pf) {
	int HasInterMed = pf->tmp ? 1 : 0;
	void *data = pf->data;
	int nc = PFCOM->ncols;

	if (!HasInterMed) {
	  nc = PFCOM->ncols;
	}
	else {
	  nc = pf->tmp->nc;
	}

	if (nrows) {
	  int nr;
	  if (!HasInterMed) {
	    PFCOM->dim(data, &nr, NULL, NULL, -1);
	  }
	  else {
	    nr = pf->tmp->nr;
	  }
	  Nrows += nr;
	}

	Ncols  = nc;
	if (!nrows) break;
      } /* if (pf) */
    } /* if (MATCHING) */
    POOLREG_BREAK;
  } /* POOLREG_FOR */

  /* finish: */

  if (rc >=0 && Ncols == 0 && ncols && Recur == 0) { 
    /* If no match so far, then must get the correct Ncols at least */
    const int off = 0;
    int oldvalue;
    const int some_pool = -1;

    /* switch poolmasking temporarely off [for this thread only] */
    codb_toggle_poolmask_(&Handle, &off, &oldvalue);

    ++Recur;
    codb_getsize_(handle,
		  &some_pool,
		  p_dataname,
		  NULL,
		  &Ncols,
		  &Recur,
		  NULL,
		  &it,
		  /* Hidden arguments */
#ifdef USE_CTRIM
		  strlen(p_dataname)+1
#else
		  strlen(p_dataname)
#endif
		  );
    --Recur;

    /* reset poolmasking status to whatever it was before */
    codb_toggle_poolmask_(&Handle, &oldvalue, NULL);
  }

  FREE_FASTFTN_CHAR(dataname);

  if (nrows) *nrows = Nrows;
  if (ncols) *ncols = Ncols;

  if (retcode) *retcode = Nrows;
  DRHOOK_END_RECUR(0,Recur);
}


PUBLIC void
codb_getsize_aux_(const int *handle,
		  const int  *poolno,
		  const char *dataname,
		  const int *ncols,
		  int *ncols_aux,
		  int  colaux[],
		  const int *colaux_len,
		  int *retcode,
		  const int  *using_it,
		  /* Hidden arguments */
		  int dataname_len)
{
  int Handle = *handle;
  int Poolno = *poolno;
  boolean AnyPool = (Poolno == -1);
  POOLREG_DEF;
  int it = USING_IT;
  int Ncols = *ncols;
  int Ncols_aux = 0;
  int Colaux_len = *colaux_len;
  int filled = 0;
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_getsize_aux_);

  ALLOC_FASTFTN_CHAR(dataname);

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);

      if (pf) {
	void *data = pf->data;
	Ncols_aux = PFCOM->ncols_aux;
	if (PFCOM->colaux) filled = PFCOM->colaux(data, colaux, Colaux_len);
	break; /* One result suffice */
      } /* if (pf) */
    } /* if (MATCHING) */
    POOLREG_BREAK;
  } /* POOLREG_FOR */

  /* finish: */

  if (!filled && Colaux_len > 0 && Ncols > 0) {
    int j, n = MIN(Ncols, Colaux_len);
    for (j=0; j<n; j++) colaux[j] = j+1; /* Fortran-index */
    filled = n;
  }

  if (ncols_aux) *ncols_aux = Ncols_aux;

#if 0
  {
    int j, n = filled;
    fprintf(stderr,
	    "codb_getsize_aux_(%s): filled = %d, (Ncols=%d, Ncols_aux=%d) and colaux[] to follow ...:\n",
	    p_dataname,filled,Ncols,Ncols_aux);
    for (j=0; j<n; j++) {
      fprintf(stderr," (%d)=%d",j+1,colaux[j]);
    }
    fprintf(stderr,"\n");
  }
#endif

  if (filled > 0) { /* Check that every filled colaux[j] has value >= 1 && <= Ncols or zero */
    int j, n = filled;
    boolean do_abort = 0;
    for (j=0; j<n; j++) {
      if (colaux[j] == 0) continue; /* Ignore zeros */
      if (colaux[j] < 1 || colaux[j] > Ncols) {
	fprintf(stderr,
		"codb_getsize_aux_(%s): COLAUX(Fortran-index j = %d) = %d is not between column range [ %d .. %d ]\n",
		p_dataname, j+1, colaux[j], 1, Ncols);
	do_abort = 1;
      }
    }
    if (do_abort) RAISE(SIGABRT);
  }

  FREE_FASTFTN_CHAR(dataname);

  *retcode = filled; /* Ret.code indicates how many elements in colaux[] were actually filled */
  DRHOOK_END(0);
}


PRIVATE char *
map2ftntype(const char *s, boolean *is_flp, int *maxbits)
{
  char *p = NULL;
  char *cmp;
  boolean Flpnt = 0;
  int Maxbits = 0;
  DRHOOK_START(map2ftntype);

  ALLOC(cmp,strlen(s) + 3);
  sprintf(cmp,"/%s/",s);

  if (strstr("/int/uint/integer4/pk1int/pk2int/pk3int/pk4int/pk5int/pk9int/",cmp)) {
    p = STRDUP("INTEGER(4)");
    Flpnt = 0;
    Maxbits = 32;
  }
  else if (strstr("/linkoffset_t/linklen_t/",cmp)) {
    p = STRDUP("INTEGER(4)");
    Flpnt = 0;
    Maxbits = 32;
  }
  else if (strstr("/double/real8/pk2real/pk3real/pk4real/pk5real/pk9real/real/Formula/",cmp)) {
    p = STRDUP("REAL(8)");
    Flpnt = 1;
    Maxbits = 64;
  }
  else if (strstr("/string/",cmp)) {
    p = STRDUP("REAL(8)");
    Flpnt = 1;
    Maxbits = 64;
  }
  else if (strstr("/pk11real/pk12real/pk13real/pk14real/pk15real/pk16real/pk17real/pk18real/pk19real/",cmp)) {
    p = STRDUP("REAL(8)");
    Flpnt = 1;
    Maxbits = 64;
  }
  else if (strstr("/pk21real/pk22real/pk23real/pk24real/pk25real/pk26real/pk27real/pk28real/pk29real/",cmp)) {
    p = STRDUP("REAL(8)");
    Flpnt = 1;
    Maxbits = 64;
  }
  else if (strstr("/pk31real/pk32real/pk33real/pk34real/pk35real/pk36real/pk37real/pk38real/pk39real/",cmp)) {
    p = STRDUP("REAL(8)");
    Flpnt = 1;
    Maxbits = 64;
  }
  else if (strstr("/Bitfield/hex4/yyyymmdd/hhmmss/integer/bufr/grib/",cmp)) {
    p = STRDUP("INTEGER(4)");
    Flpnt = 0;
    Maxbits = 32;
  }
  else if (strstr("/integer2/short/ushort/",cmp)) {
    p = STRDUP("INTEGER(2)");
    Flpnt = 0;
    Maxbits = 32;
  }
  else if (strstr("/byte/integer1/uchar/char/boolean/",cmp)) {
    p = STRDUP("INTEGER(1)");
    Flpnt = 0;
    Maxbits = 32;
  }
  else if (strstr("/float/real4/",cmp)) {
    p = STRDUP("REAL(4)");
    Flpnt = 1;
    Maxbits = 32;
  }
  else {
    ALLOC(p, 20);
    strcpy(p,"UNKNOWN");
    Flpnt = 0;
    Maxbits = 0;
  }

  FREE(cmp);

  if (is_flp)   *is_flp  = Flpnt;
  if (maxbits)  *maxbits = Maxbits;

  DRHOOK_END(0);
  return p;
}


PRIVATE char *
parse_name(int what, const char *in, boolean *is_flp, int *maxbits, const char *varname)
{
  int a_what = what%100;
  char *s = STRDUP(in);
  char *pstart = s;
  char *x = NULL;
  char *ftntype = NULL;
  boolean found = 0;
  DRHOOK_START(parse_name);

  if (a_what == 1 || a_what == 11) { /* get datatype or ftntype */
    if (what == a_what) {
      char *pend = pstart;
      
      while (*pend) {
	if (*pend == ':') {
	  *pend = '\0';
	  break;
	}
	pend++;
      } /* while (*pend) */
      
      found = 1;
      
      if (a_what == 11) { /* Map type to FORTRAN-type */
	pstart = ftntype = map2ftntype(pstart, is_flp, maxbits);
      }
    }
    else { /* a bit-member */
      int nbits = 0;
      int n = sscanf(in,"%s %d",pstart,&nbits);
      if (n == 2 && nbits > 0 && nbits <= MAXBITS) {
	if (a_what == 11) { /* Map type to FORTRAN-type */
	  FREE(s);
	  s = STRDUP("INTEGER(1)"); /* The smallest type we can offer */
	}
	else { /* ODB (sub-)type */
	  FREE(s);
	  s = STRDUP("bitxxx");
	  sprintf(s,"bit%d",nbits);
	}
	pstart = s;
	found = 1;
      }
    }
  }
  else if (a_what == 2) { /* get varname */
    if (what == a_what) {
      char *pend = strchr(pstart, ':');
      if (pend) {
	pstart = ++pend;
	found = 1;
      } /* if (pend) */
    }
    else { /* a bit-member */
      int nbits = 0;
      int n = sscanf(in,"%s %d",pstart,&nbits);
      if (n == 2 && nbits > 0 && nbits <= MAXBITS) {
	char *save = s;
	char *v = STRDUP(varname);
	int slen = strlen(v) + strlen(pstart) + 3;
	char *colname = v;
	char *tblname = strchr(v,'@');
	ALLOC(s,slen);
	if (tblname) { *tblname++ = '\0'; }
	sprintf(s,"%s.%s%s%s",colname,pstart,
		tblname ? "@" : "",
		tblname ? tblname : "");
	pstart = s;
	FREE(save);
	FREE(v);
	found = 1;
      }
    }
  }
  else if (what == 3 || what == 33) { /* get tablename */
    /* obsolete */
    char *pend = strchr(pstart, '@');
    if (pend) {
      pstart = pend;
      found = 1;
    } /* if (pend) */
  }

  x = (found) ? STRDUP(pstart) : STRDUP(in);

  FREE(s);
  FREE(ftntype);

  DRHOOK_END(0);
  return x;
}


PRIVATE const ODB_PrepTags *Get_PrepTags(const ODB_Funcs *pf, int What)
{
  const ODB_PrepTags *preptag = NULL;
  if (pf && PFCOM->preptags && PFCOM->npreptag > 0) {
    int andy = 0;
    switch (What) {
    case   1: andy = preptag_type    ; break;
    case   2: andy = preptag_name    ; break;
    case 101: andy = preptag_exttype ; break;
    case 102: andy = preptag_extname ; break;
    case  33: andy = preptag_tblname ; break;
    } /* switch (What) */
    if (andy > 0) {
      int j, n = PFCOM->npreptag;
      for (j=0; j<n; j++) { 
	/* always a short loop; length 3 for views, and 2 or 4 for tables */
	if ((PFCOM->preptags[j].tagtype & andy) == andy) { 
	  /* Found ! */
	  preptag = &PFCOM->preptags[j];
	  break;
	}
      }
    } /* if (andy > 0) */
  }
  return preptag;
}


PUBLIC void
codb_tag_delim_(char *output
		/* Hidden arguments */
		, const int output_len)
{ /* This routine makes sure that ODB_tag_delim from privpub.h is also used in F90-side */
  const char *tag_delim = ODB_tag_delim;
  if (tag_delim) {
    if (output && output_len >= 1) {
      *output = *tag_delim;
      if (output_len > 1) memset(output+1,' ',output_len-1);
    }
  }
}


PUBLIC void 
codb_getnames_(const int *handle,
	       const char *dataname,
	       const int *what, /* 1 = datatype, 
				   2 = varname, 
				   3 = tablename,
				   4 = viewname,
				  11 = ftntype, 
				 101 = exttype,    (same as datatype, but adds bit<#> for bitfield members)
				 102 = extname,    (same as varname, but gets bitfield members)
				 111 = extftntype, (same as ftntype, but handles bitfield members)
				*/
	       char *output,    /* Output string: ";xxx;yyy;zzz;" (see ";" for ODB_tag_delim in privpub.h) */
	       int *actual_output_len, /* # of bytes written into *output, excluding '\0' */
	       int *retcode,    /* # of cols resolved */
	       /* Hidden arguments */
	       int dataname_len,
	       const int output_len)
{
  int rc = 0;
  int len = 0;
  int Handle = *handle;
  int is_table;
  ODB_Pool *p;
  int aWhat, What = *what;
  DECL_FTN_CHAR(dataname);
  DECL_FTN_CHAR(output);
  DRHOOK_START(codb_getnames_);

  ALLOC_FASTFTN_CHAR(dataname);
  ALLOC_OUTPUT_FTN_CHAR(output);

  is_table = IS_TABLE(p_dataname);

  if (What == 3 && (*p_dataname != '*' && !is_table)) {
    /* Get table names associated with this view */
    What = 33;
  }

  aWhat = What%100;

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      ODB_Funcs *pf;

      if (What == 3 || What == 4) {
	/* Get all table or view names available in the DB */
	/* Tables always start with '@' */

	int kcols = 0;

	len = 1;
	FORFUNC {
	  char *s = PFCOM->name;
	  int lens = strlen(s);
	  if ((What == 3 &&  PFCOM->is_table)||
	      (What == 4 && !PFCOM->is_table && pf->it == 1)) {
	    len += lens + 1;
	    kcols++;
	  }
	} /* FORFUNC */

	if (output_len <= 0) {
	  rc = kcols;
	  len = 0;
	  goto finish;
	}

	if (output_len > len) {
	  strcpy(p_output, ODB_tag_delim);
	  FORFUNC {
	    char *s = PFCOM->name;
	    if ((What == 3 &&  PFCOM->is_table)||
		(What == 4 && !PFCOM->is_table && pf->it == 1)) {
	      strcat(p_output,s);
	      strcat(p_output,ODB_tag_delim);
	    }
	  } /* FORFUNC */
	  rc = kcols;
	}
	else {
	  rc = output_len - len; /* This many chars too short */
	  len = 0;
	}

	goto finish;
      }
      else if (What == 33) {
	FORFUNC {
	  if (strequ(PFCOM->name, p_dataname)) {
	    const ODB_PrepTags *preptags = Get_PrepTags(pf, What);
	    int ntot = PFCOM->ntables;
	    
	    if (output_len <= 0) {
	      rc = ntot;
	      len = 0;
	      goto finish;
	    }

	    len = preptags->longname_len;

	    if (output_len > len) {
	      strcpy(p_output, preptags->longname);
	      rc = ntot;
	    }
	    else {
	      rc = output_len - len; /* This many chars too short */
	      len = 0;
	    }
	    
	    goto finish;
	  }
	} /* FORFUNC */
      } 
      else if (What == 111) {
	FORFUNC {
	  if (strequ(PFCOM->name, p_dataname)) {
	    int k;
	    const ODB_Tags *tags = PFCOM->tags;
	    int ncols = PFCOM->ncols - PFCOM->ncols_aux;
	    int nmem = PFCOM->nmem;
	    int ntot = ncols + nmem;

	    if (output_len <= 0) {
	      rc = ntot;
	      len = 0;
	      goto finish;
	    }

	    len = 1;

	    for (k=0; k<ncols; k++) {
	      char *ptag = parse_name(aWhat, tags[k].name, NULL, NULL, NULL);
	      int lens = strlen(ptag);
	      len += lens + 1;
	      FREE(ptag);
	    } /* for (k=0; k<ncols; k++) */

	    if (nmem > 0) {
	      for (k=0; k<ncols; k++) {
		int n = tags[k].nmem;
		if (n > 0) {
		  char *ptag = (aWhat == 2) ? parse_name(aWhat, tags[k].name, NULL, NULL, NULL) : NULL;
		  char *varname = (aWhat == 2) ? STRDUP(ptag) : NULL;
		  int i, lens;
		  FREE(ptag);
		  for (i=0; i<n; i++) {
		    ptag = parse_name(What, tags[k].memb[i], NULL, NULL, varname);
		    lens = strlen(ptag);
		    len += lens + 1;
		    FREE(ptag);
		  } /* for (i=0; i<n; i++) */
		  FREE(varname);
		} /* if (n > 0) */
	      } /* for (k=0; k<ncols; k++) */
	    } /* if (nmem > 0) */
	    
	    if (output_len > len) {
	      strcpy(p_output, ODB_tag_delim);
	      for (k=0; k<ncols; k++) {
		char *ptag = parse_name(aWhat, tags[k].name, NULL, NULL, NULL);
		strcat(p_output,ptag);
		strcat(p_output,ODB_tag_delim);
		FREE(ptag);
	      } /* for (k=0; k<ncols; k++) */

	      if (nmem > 0) {
		for (k=0; k<ncols; k++) {
		  int n = tags[k].nmem;
		  if (n > 0) {
		    char *ptag = (aWhat == 2) ? parse_name(aWhat, tags[k].name, NULL, NULL, NULL) : NULL;
		    char *varname = (aWhat == 2) ? STRDUP(ptag) : NULL;
		    int i;
		    FREE(ptag);
		    for (i=0; i<n; i++) {
		      ptag = parse_name(What, tags[k].memb[i], NULL, NULL, varname);
		      strcat(p_output,ptag);
		      strcat(p_output,ODB_tag_delim);
		      FREE(ptag);
		    } /* for (i=0; i<n; i++) */
		    FREE(varname);
		  } /* if (n > 0) */
		} /* for (k=0; k<ncols; k++) */
	      } /* if (nmem > 0) */

	      rc = ntot;
	    }
	    else {
	      rc = output_len - len; /* This many chars too short */
	      len = 0;
	    }
	    
	    goto finish;
	  }
	} /* FORFUNC */
      }
      else if (aWhat == 1 || aWhat == 2) {
	FORFUNC {
	  if (strequ(PFCOM->name, p_dataname)) {
	    const ODB_PrepTags *preptags = Get_PrepTags(pf, What);
	    int ncols = PFCOM->ncols - PFCOM->ncols_aux;
	    int nmem = (What == aWhat) ? 0 : PFCOM->nmem;
	    int ntot = ncols + nmem;

	    if (output_len <= 0) {
	      rc = ntot;
	      len = 0;
	      goto finish;
	    }

	    len = preptags->longname_len;

	    if (output_len > len) {
	      strcpy(p_output, preptags->longname);
	      rc = ntot;
	    }
	    else {
	      rc = output_len - len; /* This many chars too short */
	      len = 0;
	    }

	    goto finish;
	  }
	}
      }
      else {
	FORFUNC {
	  if (strequ(PFCOM->name, p_dataname)) {
	    int k;
	    const ODB_Tags *tags = PFCOM->tags;
	    int ncols = PFCOM->ncols - PFCOM->ncols_aux;
	    
	    if (output_len <= 0) {
	      rc = ncols;
	      goto finish;
	    }

	    len = 1;
	    for (k=0; k<ncols; k++) {
	      char *ptag = parse_name(What, tags[k].name, NULL, NULL, NULL);
	      int lens = strlen(ptag);
	      len += lens + 1;
	      FREE(ptag);
	    }
	    
	    if (output_len > len) {
	      strcpy(p_output, ODB_tag_delim);
	      for (k=0; k<ncols; k++) {
		char *ptag = parse_name(What, tags[k].name, NULL, NULL, NULL);
		strcat(p_output,ptag);
		strcat(p_output,ODB_tag_delim);
		FREE(ptag);
	      }
	      rc = ncols;
	    }
	    else {
	      rc = output_len - len; /* This many chars too short */
	      len = 0;
	    }
	    
	    goto finish;
	  }
	} /* FORFUNC */
      } /* if (What == 3 || What == 4) ... else if's ... else ... */
    } /* if (p->inuse && p->handle == Handle) */
  } /* FORPOOL */

 finish:
  FREE_FASTFTN_CHAR(dataname);
  if (output_len > 0) {
    if (actual_output_len && len > 0) {
      COPY_2_FTN_CHARv2(output, len);
    }
    else {
      COPY_2_FTN_CHAR(output);
    }
  }
  FREE_FTN_CHAR(output);

  if (actual_output_len) *actual_output_len = len;

  *retcode = rc;
  DRHOOK_END(0);
}


PRIVATE boolean
Digital(const char *s)
{
  int len = strlen(s);
  int count = 0;
  DRHOOK_START(Digital);

  while ( *s ) {
    if (isdigit(*s)) count++;
    s++;
  }

  DRHOOK_END(0);
  return (count == len);
}



PUBLIC void 
codb_getprecision_(const int *handle,
		   const char *dataname,
		   int *maxbits,
		   int *anyflp,
		   int *retcode,
		   /* Hidden arguments */
		   int dataname_len)
{
  int rc = 0;
  int Handle = *handle;
  ODB_Pool *p;
  int it = get_thread_id_(); /* Any thread id >= 1 will do -> no messing with "using_it" */
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_getprecision_);

  ALLOC_FASTFTN_CHAR(dataname);

  *maxbits = 0;
  *anyflp = 0;

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);

      if (pf) {
	void *data = pf->data;
	int k;
	boolean Anyflp;
	int Maxbits;
	const ODB_Tags *tags = PFCOM->tags;
	int ncols = PFCOM->ncols - PFCOM->ncols_aux;
	
	Anyflp = 0;
	Maxbits = 0;
	
	for (k=0; k<ncols; k++) {
	  boolean xAnyflp;
	  int xMaxbits;
	  int What = 11;
	  char *ftntype = parse_name(What, tags[k].name, &xAnyflp, &xMaxbits, NULL);
	  
	  Anyflp |= xAnyflp;
	  Maxbits = MAX(Maxbits, xMaxbits);
	  FREE(ftntype);
	}
	
	*maxbits = Maxbits;
	*anyflp = Anyflp;
	
	rc = ncols;
	  
	goto finish;
      } /* if (pf) */
    } /* if (p->inuse && p->handle == Handle) */
  } /* FORPOOL */

 finish:
  FREE_FASTFTN_CHAR(dataname);

  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_getval_(const char   *dbname,
	     const char   *varname,
	     const char   *viewname,
	     double *value,
	     const int *using_it,
	     /* Hidden arguments */
	     int dbname_len,
	     int varname_len,
	     int viewname_len)
{
  DRHOOK_START(codb_getval_);
  if (value) {
    int vlen;
    int it = USING_IT;
    DECL_FTN_CHAR(dbname);
    DECL_FTN_CHAR(varname);
    DECL_FTN_CHAR(viewname);
    
    ALLOC_FASTFTN_CHAR(dbname);
    ALLOC_FASTFTN_CHAR(varname);
    ALLOC_FASTFTN_CHAR(viewname);
    
    vlen = p_viewname ? strlen(p_viewname) : 0;
    *value = ODB_getval_var(p_dbname, 
			    p_varname, 
			    (vlen > 0) ? p_viewname : NULL,
			    it);
    
    FREE_FASTFTN_CHAR(dbname);
    FREE_FASTFTN_CHAR(varname);
    FREE_FASTFTN_CHAR(viewname);
  }
  DRHOOK_END(0);
}


PUBLIC void
codb_setval_(const char   *dbname,
	     const char   *varname,
	     const char   *viewname,
	     const double *newvalue,
	           double *oldvalue,
	     const int *using_it,
	     /* Hidden arguments */
	     int dbname_len,
	     int varname_len,
	     int viewname_len)
{
  int vlen;
  int it = using_it ? *using_it : get_thread_id_();
  int lo, hi;
  DECL_FTN_CHAR(dbname);
  DECL_FTN_CHAR(varname);
  DECL_FTN_CHAR(viewname);
  DRHOOK_START(codb_setval_);

  ALLOC_FASTFTN_CHAR(dbname);
  ALLOC_FASTFTN_CHAR(varname);
  ALLOC_FASTFTN_CHAR(viewname);

  vlen = STRLEN(p_viewname);
  if (it == -1) {
    lo = 1;
    hi = max_omp_threads;
  }
  else if (it == 0) {
    lo = hi = USING_IT;
  }
  else {
    lo = hi = it;
  }
  for (it=hi; it>=lo; it--) {
    (void) ODB_alter_var(p_dbname, 
			 p_varname, 
			 (vlen > 0) ? p_viewname : NULL, 
			 it,
			 newvalue,
			 oldvalue,
			 0);
  }

  FREE_FASTFTN_CHAR(dbname);
  FREE_FASTFTN_CHAR(varname);
  FREE_FASTFTN_CHAR(viewname);
  DRHOOK_END(0);
}


PUBLIC void	     
codb_packer_(const int  *handle,
             const int  *poolno,
	     const char *dataname,
	     const int  *pack_it,
	     int *retcode,
	     /* Hidden arguments */
	     int dataname_len)
{
  int rc = 0;
  int Poolno = *poolno;
  boolean AnyPool = (Poolno == -1);
  int it = get_thread_id_();
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_packer_);

  ALLOC_FASTFTN_CHAR(dataname);

  if (IS_TABLE(p_dataname)) {
    int Handle = *handle;
    boolean Pack = (*pack_it > 0) ? 1 : 0;
    POOLREG_DEF;
    int Nbytes = 0;

    POOLREG_FOR {
      if (MATCHING) {
	ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);
	
	if (pf) {
	  void *data = pf->data;  
	  int (*func)(void *p) = Pack ? PFCOM->pack : PFCOM->unpack;
	  rc = func(data);
	  if (rc < 0) goto finish; /* Error */
	  Nbytes += rc;
	} /* if (pf) */
      } /* if (MATCHING) */
      POOLREG_BREAK;
    } /* POOLREG_FOR */

    rc = Nbytes;
  } /* if (IS_TABLE(p_dataname)) */

 finish:
  FREE_FASTFTN_CHAR(dataname);

  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC int
ODB_add_funcs(ODB_Pool *p, ODB_Funcs *funcs, int it)
{
  int nfuncs = 0;
  DRHOOK_START(ODB_add_funcs);

  if (p) {
    nfuncs = p->nfuncs;

    if (funcs) {
      boolean is_table = IS_TABLE(funcs->common->name);
      ODB_Funcs *plast  = NULL;
      ODB_Funcs *pf, *this = NULL, *next;
      int ret = 0;

      FORFUNC {
	if (strequ(PFCOM->name,funcs->common->name) &&
	    (is_table || (!is_table && pf->it == it))) {
	  this = pf;
	  break;
	}
	plast = pf;
      }

      funcs->it = it;

      if (!this) { /* Create a new entry */
	this = funcs;
	next = NULL;
	nfuncs++;
      }
      else { /* Replace an existing entry */
	next = this->next;
	this = funcs;
      }

      ret = put_forfunc(this, p->handle, p->dbname, p->poolno, funcs->it, funcs->common->name);
      if (ret < 0) {
	char msg[] = "put_forfunc failed";
	codb_abort_func_(msg, strlen(msg));
      }

      this->next = next;

      if (plast) plast->next = this;
      
      if (!p->funcs) p->funcs = this; /* Start chaining now */
	
      p->nfuncs = nfuncs;
    } /* if (funcs) */
  } /* if (p) */

  DRHOOK_END(0);
  return nfuncs;
}


PUBLIC void 
codb_gethandle_(const int *handle,
		const char *dataname,
		int *retcode,
		const int *using_it,
		/* Hidden arguments */
		int dataname_len)
{
  int rc = -1;
  int Handle = *handle;
  ODB_Pool *p;
  int it = USING_IT;
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_gethandle_);

  ALLOC_FASTFTN_CHAR(dataname);
  
  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 0);

      if (pf) {
#define odbrand(x,it) ((((x) & 0x007fffff) << 8) + (it))
	typedef union _ODB_odbrand {
	  unsigned long long int i;
	  void (*f)();
	  struct {
	    unsigned int lo;
	    unsigned int hi;
	  } s;
	} ODB_odbrand;
	ODB_odbrand jack;
	jack.i = 0; /* ... in case pointers were just 32-bit */
	jack.f = (void (*)())PFCOM->init;
	/* This used to be "vhandle" */
	rc = odbrand(jack.s.lo|jack.s.hi,it); /* Return VIEW or TABLE handle ; different for each thread (it) */
	goto finish;
      } /* if (pf) */
    } /* if (p->inuse && p->handle == Handle) */
  } /* FORPOOL */
  
 finish:
  FREE_FASTFTN_CHAR(dataname);
  
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void 
codb_get_view_info_(const int *handle,
		    const char *dataname,
		    int view_info[],
		    const int *nview_info,
		    int *retcode,
		    /* Hidden arguments */
		    int dataname_len)
{
  int rc = 0;
  int Nview_info = *nview_info;
  int Handle = *handle;
  ODB_Pool *p;
  int it = get_thread_id_(); /* Any thread id >= 1 will do -> no messing with "using_it" */
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_get_view_info_);

  ALLOC_FASTFTN_CHAR(dataname);

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);
	
      if (pf) {
	if (Nview_info >= 1) {
	  view_info[0] = PFCOM->has_select_distinct; /* Fortran index#1 */
	  rc++;
	}
	goto finish;
      } /* if (pf) */
    } /* if (p->inuse && p->handle == Handle) */
  } /* FORPOOL */
  
 finish:
  FREE_FASTFTN_CHAR(dataname);
  
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void 
codb_sortkeys_(const int *handle,
	       const char *dataname,
	       int *nkeys,
	       int  mkeys[],
	       int *retcode,
	       /* Hidden arguments */
	       int dataname_len)
{
  int rc = 0;
  int Nkeys = *nkeys;
  int Handle = *handle;
  ODB_Pool *p;
  int it = get_thread_id_(); /* Any thread id >= 1 will do -> no messing with "using_it" */
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_sortkeys_);

  ALLOC_FASTFTN_CHAR(dataname);

  if (!IS_TABLE(p_dataname)) {
    FORPOOL {
      if (p->inuse && p->handle == Handle) {
	ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);
	
	if (pf) {
	  void *data = pf->data;
	  int *keys = PFCOM->sortkeys ? PFCOM->sortkeys(data, &rc) : NULL;
	  if (keys && rc > 0) {
	    int j, minkeys = MIN(rc, Nkeys);
	    for (j=0; j<minkeys; j++) {
	      /* Usually short(ish) loop */
	      mkeys[j] = keys[j];
	    } /* for (j=0; j<minkeys; j++) */
	  }
	  goto finish;
	} /* if (pf) */

      } /* if (p->inuse && p->handle == Handle) */
    } /* FORPOOL */
  }
  
 finish:
  FREE_FASTFTN_CHAR(dataname);
  
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void 
codb_update_info_(const int *handle,
		  const char *dataname,
		  const int *ncols,
		  int  can_UPDATE[],
		  int *retcode,
		  /* Hidden arguments */
		  int dataname_len)
{
  int rc = 0;
  int Ncols = *ncols;
  int Handle = *handle;
  ODB_Pool *p;
  int it = get_thread_id_(); /* Any thread id >= 1 will do -> no messing with "using_it" */
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_update_info_);

  ALLOC_FASTFTN_CHAR(dataname);

  if (!IS_TABLE(p_dataname)) {
    FORPOOL {
      if (p->inuse && p->handle == Handle) {
	ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);
	if (pf) {
	  void *data = pf->data;
	  rc = PFCOM->update_info(data, Ncols, can_UPDATE);
	  goto finish;
	} /* if (pf) */
      } /* if (p->inuse && p->handle == Handle) */
    } /* FORPOOL */
  }
  else {
    int j;
    for (j=0; j<Ncols; j++) can_UPDATE[j] = 1; /* All updatable */
    rc = Ncols;
  }
  
 finish:
  FREE_FASTFTN_CHAR(dataname);
  
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void 
codb_aggr_info_(const int  *handle,
		const int  *poolno,
		const char *dataname,
		const int  *ncols,
		int  aggr_func_flag[],
		int *phase_id,
		const int *using_it,
		int *retcode,
		/* Hidden arguments */
		int dataname_len)
{
  int rc = 0;
  int Poolno = *poolno;
  boolean AnyPool = (Poolno == -1);
  int Ncols = *ncols;
  int Phase_Id = -1;
  boolean zero_all = 0;
  boolean check_env = 0;
  int it = USING_IT;
  DECL_FTN_CHAR(dataname);
  DRHOOK_START(codb_aggr_info_);

  ALLOC_FASTFTN_CHAR(dataname);

  if (!IS_TABLE(p_dataname)) {
    int Handle = *handle;
    POOLREG_DEF;

    POOLREG_FOR {
      if (MATCHING) {
	ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);
	if (pf) {
	  void *data = pf->data;
	  boolean finito = 0;
	  if (PFCOM->aggr_info) {
	    rc = PFCOM->aggr_info(data, Ncols, (Ncols > 0) ? aggr_func_flag : NULL);
	    if (pf->tmp) {
	      Phase_Id = 1;
	      check_env = 0;
	      finito = 1;
	    }
	    else if (!AnyPool) {
	      check_env = 1;
	      finito = 1;
	    }
	  }
	  else {
	    zero_all = 1;
	    finito = 1;
	  }
	  if (finito) goto finish;
	} /* if (pf) */
      } /* if (MATCHING) */
      POOLREG_BREAK;
    } /* POOLREG_FOR */
  }
  else {
    zero_all = 1;
  }

 finish:

  if (zero_all) {
    int j;
    for (j=0; j<Ncols; j++) aggr_func_flag[j] = ODB_AGGR_NONE; /* All non-aggregate functions */
    rc = 0;
  }
  else if (check_env || (AnyPool && Phase_Id == -1)) {
    char *env = getenv("CODB_AGGR_PHASE_ID");
    Phase_Id = 0;
    if (env) {
      int val = atoi(env);
      if (val == -1) Phase_Id = -1;
    }
  }

  FREE_FASTFTN_CHAR(dataname);
  
  *retcode = rc;
  *phase_id = Phase_Id;
  DRHOOK_END(0);
}


/* codb_calc_aggr_(....) moved to aggr.c on 18-Aug-2006/SS */


PUBLIC void
codb_sqlprint_(const int *handle,
	       const char *dataname,
	       int *retcode,
	       /* Hidden arguments */
	       int dataname_len)
{
  int rc = 0;
  char *env = getenv("ODB_PRINT_SQL");
  int print_sql = 1;
  DRHOOK_START(codb_sqlprint_);

  if (env) print_sql = atoi(env);
  
  if (print_sql == 1) {
    FILE *fp = ODB_getprt_FP();

    if (fp) {
      int Handle = *handle;
      int Poolno = -1;
      boolean AnyPool = 1;
      ODB_Pool *p;
      int it = get_thread_id_();
      DECL_FTN_CHAR(dataname);
      
      ALLOC_FTN_CHAR(dataname);
      
      FORPOOL {
	if (MATCHING) {
	  ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);
	  
	  if (pf) {
	    rc = PFCOM->sql(fp, 0, ": ", NULL, NULL);
	    goto finish;
	  } /* if (pf) */
	} /* if (MATCHING) */
      } /* FORPOOL */
      
    finish:
      FREE_FTN_CHAR(dataname);
    } /* if (fp) */
  } /* if (print_sql == 1) */

  *retcode = rc;
  DRHOOK_END(0);
}

PUBLIC void
codb_filesize_(const char *filename,
	       int *retcode,
	       /* Hidden arguments */
	       int filename_len)
{
  /* from libioassign.a */
  extern char *IOtruename(const char *name, const int *len_str);
  extern int   IOgetsize(const char *path, int *filesize, int *blksize);

  char *p;
  int size = 0;
  DRHOOK_START(codb_filesize_);

  p = IOtruename(filename, &filename_len); 
  (void) IOgetsize(p, &size, NULL); 
  FREE(p);
  
  *retcode = size;
  DRHOOK_END(0);
}

PUBLIC void
codb_remove_file_(const char *filename,
		  int *retcode,
                  /* Hidden arguments */
                  int filename_len)
{
  int rc;
  DECL_FTN_CHAR(filename);
  DRHOOK_START(codb_remove_file_);

  ALLOC_FTN_CHAR(filename);

  rc = remove_file_(p_filename);
  if (retcode) *retcode = rc;

  FREE_FTN_CHAR(filename);
  DRHOOK_END(0);
}

PUBLIC void
codb_rename_file_(const char *oldfile,
		  const char *newfile,
		  int *retcode,
                  /* Hidden arguments */
                  int oldfile_len,
                  int newfile_len)
{
  int rc;
  DECL_FTN_CHAR(oldfile);
  DECL_FTN_CHAR(newfile);
  DRHOOK_START(codb_rename_file_);

  ALLOC_FTN_CHAR(oldfile);
  ALLOC_FTN_CHAR(newfile);

  rc = rename_file_(p_oldfile, p_newfile);
  if (retcode) *retcode = rc;

  FREE_FTN_CHAR(oldfile);
  FREE_FTN_CHAR(newfile);
  DRHOOK_END(0);
}

PUBLIC void
codb_remove_tablefile_(const char *dbname,
		       const char *table,
		       const int  *poolno,
		       int *retcode,
		       /* Hidden arguments */
		       int dbname_len,
		       int table_len)
{
  char *name;
  int rc, Poolno = *poolno;
  DECL_FTN_CHAR(dbname);
  DECL_FTN_CHAR(table);
  DRHOOK_START(codb_remove_tablefile_);

  ALLOC_FTN_CHAR(dbname);
  ALLOC_FTN_CHAR(table);

  MakeFileName(name, p_dbname, p_table, Poolno); /* A macro */

  FREE_FTN_CHAR(dbname);
  FREE_FTN_CHAR(table);

  rc = remove_file_(name);

  FREEX(name);
  *retcode = rc;
  DRHOOK_END(0);
}

PUBLIC void
codb_tablesize_(const char *dbname,
		const char *table,
		const int  *poolno,
		int *retcode,
		/* Hidden arguments */
		int dbname_len,
		int table_len)
{
  char *name;
  int size;
  int Poolno = *poolno;
  DECL_FTN_CHAR(dbname);
  DECL_FTN_CHAR(table);
  DRHOOK_START(codb_tablesize_);

  ALLOC_FTN_CHAR(dbname);
  ALLOC_FTN_CHAR(table);

  MakeFileName(name, p_dbname, p_table, Poolno); /* A macro */

  FREE_FTN_CHAR(dbname);
  FREE_FTN_CHAR(table);

  codb_filesize_(name, &size, strlen(name));

  FREEX(name);
  *retcode = size;
  DRHOOK_END(0);
}


PUBLIC void
codb_d2u_(const double *d, uint *u) 
{ 
  /* RS6K does the following wrong for negative *d's : 
   *u = (uint) *d; ==> need an alternative */
  int tmp = *d;
  /* DRHOOK_START(codb_d2u_); */
  *u = tmp;
  /* DRHOOK_END(0); */
}

PUBLIC double
ODB_duint(double d)
{
  uint u;
  d = trunc(d);
  if (d >= -INT_MAX && d <= INT_MAX) {
    int tmp = d;
    uint u = tmp;
    d = u;
  }
  else if (d < 0) {
    /* Not correct ? */
    d = -d;
  }
  return d;
}

PUBLIC double
ODB_dfloat(double d)
{
  float f = d;
  d = f;
  return d;
}

/* Byte stream distribution */

#ifdef INCLUDE_THIS
PUBLIC void
codb_dist_(char s[],
	   void (*func)(char *s, const int *len, const int *with, int *rc),
	   const int *with,
	   const int *dim2len,
	   int *retcode,
	   /* Hidden arguments */
	   const int s_len)
{
  int rc = 0;
  if (func) {
    int len = s_len * (*dim2len);
    func(s, &len, with, &rc);
  }
  else {
    rc = -1; /* Error */
  }
  *retcode = rc;
}
#endif

