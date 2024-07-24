#include "odb.h"

/* To activate poolmasking, set export ODB_POOLMASKING=1 (valid for ALL databases)
   or set ODB_PERMANENT_POOLMASK          (valid for ALL databases; see below)
   or set ODB_PERMANENT_POOLMASK_<dbname> (see below) */

#define IT (it-1)

/* Handle (i.e. database) specific permanent poolmask items */

typedef struct db_perm_t_ {
 int npermcnt;
 int nperm;
 unsigned char *perm_list;
} db_perm_t;

PRIVATE int MaxHandle = 0;
PRIVATE db_perm_t *db = NULL;
PRIVATE ODB_PoolMask *db_pm = NULL;

static int
raise_signal(int sig, const char *msg, int code, const char *file, int linenum)
{
  fprintf(stderr,"\n***Error in %s:%6.6d (signal#%d): %s (code=%d)\n",file,linenum,sig,msg,code);
  RAISE(sig);
  /* Should never end up here */
  _exit(1);
  return sig;
}

#define RAISE_SIGNAL(sig,msg,code) raise_signal(sig, msg, code, __FILE__, __LINE__)

#define SET_PDB(h) \
db_perm_t *pdb = (db && (h) >= 1 && (h) <= MaxHandle) ? &db[(h)-1] : \
( RAISE_SIGNAL(SIGABRT, "In SET_PDB-macro : handle out of range, thus pdb not allocated", h), \
 (db_perm_t *)NULL )

#define TEST_PM(h) (db_pm && (h) >= 1 && (h) <= MaxHandle)

#define GET_PM(h) (TEST_PM(h) && db_pm[(h)-1].handle == (h)) ? &db_pm[(h)-1] : NULL

#define SETPM(poolno, onoff) \
( (pdb->perm_list && (poolno >= 1 && poolno <= pdb->nperm)) ? \
  (pdb->perm_list[poolno] & onoff) : ((pdb->nperm > 0) ? 0 : onoff ) )

#define NEWPERM(j) { maxval = MAX(maxval, j); n++; \
/*fprintf(stderr,">>NEWPERM: j=%d, maxval=%d, n=%d\n",j,maxval,n);*/ }
#define SETPERM(j) { if (j > 0 && j <= pdb->nperm) { pdb->perm_list[j] = 1; n++; \
/*fprintf(stderr,">>SETPERM: j=%d, n=%d\n",j,n);*/ }}

#define WHILE_BLOCK(makro) \
while (*c) { \
  int cnt, val, lo, hi; \
  while (*c == ' ') c++; \
  if (!*c) break; \
  cnt = sscanf(c, "%s", tmpstr); \
  if (cnt != 1) break; \
  cnt = sscanf(tmpstr, "%d-%d", &lo, &hi); \
  if (cnt == 2) { \
    lo = MIN(lo, npools); \
    hi = MIN(hi, npools); \
    if (lo <= hi) { \
      int j; \
      for (j=lo; j<=hi; j++) makro(j); \
    } \
    cnt = 1; \
  } \
  else { \
    cnt = sscanf(tmpstr, "%d", &val); \
    if (cnt == 1) { val = MIN(val, npools); makro(val); } \
  } \
  if (cnt != 1) break; \
  c += strlen(tmpstr); \
  if (!*c || (int)(c - ca) >= ca_len) break; \
} /* while (*c) */

PRIVATE int
permanent_poolmask(const int *handle,
		   const char *dbname,
		   int npools)
{
  /* 
     This routine will enable to set the PERMANENT poolmask for the given execution;
     Set f.ex. export ODB_PERMANENT_POOLMASK="1 7 8 10"
     or range  export ODB_PERMANENT_POOLMASK="11-15"
     or both   export ODB_PERMANENT_POOLMASK="1,7,8,10 11:15 20-35"
     and in the given run you will NEVER access any other pool than specified in the list ;
     Applies to ALL opened databases at the same time ;

     *** Per database is fully implemented now *** :

     export ODB_PERMANENT_POOLMASK_ECMA="1 7 8 10"
     export ODB_PERMANENT_POOLMASK_CCMA="11-15"

   */

  int maxval = 0;
  int len;
  char *envname = NULL;
  const char prefix[] = "ODB_PERMANENT_POOLMASK";
  char *perm_pm;
  int myproc = 0;
  codb_procdata_(&myproc, NULL, NULL, NULL, NULL);

  len = strlen(prefix) + strlen(dbname) + 2;
  ALLOC(envname,len);
  sprintf(envname,"%s_%s",prefix,dbname);
  perm_pm = getenv(envname);
  if (!perm_pm) perm_pm = getenv(prefix);

  if (perm_pm && !strequ(perm_pm,"-1")) {
    SET_PDB(*handle);
    int n = 0;
    char *ca = STRDUP(perm_pm);
    int ca_len = strlen(ca);
    char *tmpstr = NULL;
    char *c;

    ALLOC(tmpstr,ca_len+1);

    c = ca;
    if (c) {
      while (*c) {
	if (isspace(*c) || *c == ',') *c = ' ';
	if (*c == ':') *c = '-';
	c++;
      }
    }

    c = ca;
    if (c) {
      WHILE_BLOCK(NEWPERM);
    }

    pdb->nperm = MIN(maxval, npools);
    pdb->nperm = MAX(pdb->nperm, 0);

    FREE(pdb->perm_list);
    CALLOC(pdb->perm_list, pdb->nperm+1);

    if (n > 0) {
      n = 0;
      c = ca;
      WHILE_BLOCK(SETPERM);

      if (myproc == 1) {
	int j;
	fprintf(stderr,
	"***INFO: Only the following pools will be accessed (dbname='%s', count=%d, nperm=%d)\n", 
	dbname, n, pdb->nperm);
	fprintf(stderr,
		"            ODB_PERMAMENT_POOLMASK_%s=%s\n==>",dbname,perm_pm);
	for (j=1; j<=pdb->nperm; j++) {
	  if (pdb->perm_list[j]) fprintf(stderr,"%d ", j);
	} /* for (j=0; j<pdb->nperm; j++) */
	fprintf(stderr,"\n");
      }
    } /* if (n > 0) */

    FREE(tmpstr);
    FREE(ca);

    { /* calculate "pdb->npermcnt" i.e. number of active pools permanently poolmasked */
      int j;
      pdb->npermcnt = 0;
      for (j=1; j<=pdb->nperm; j++) {
	if (pdb->perm_list[j]) pdb->npermcnt++;
      }
    }
  } /* if (perm_pm) */

  FREE(envname);
  return maxval;
}

PUBLIC ODB_PoolMask *
ODB_get_poolmask_by_handle(int handle)
{
  ODB_PoolMask *pm = GET_PM(handle);
  return pm;
}


PUBLIC void
codb_toggle_poolmask_(const int *handle, const int *onoff, int *oldvalue)
{
  /* Disable/enable poolmask testing w/o touching poolmask[] itself */
  /* Note: thread specific, thus thread safe */
  ODB_PoolMask *pm = GET_PM(*handle);
  if (oldvalue) *oldvalue = -1; /* undefined */
  if (pm) {
    DEF_IT;
    if (oldvalue) *oldvalue = pm->poolmask[IT][0]; /* old value */
    pm->poolmask[IT][0] = (*onoff > 0) ? 1 : 0;
  }
}

PUBLIC void
codb_print_poolmask_(const int *handle, const int *ithread)
{
  ODB_PoolMask *pm = GET_PM(*handle);
  if (pm) {
    SET_PDB(*handle);
    char *buf;
    int buflen;
    int it = *ithread;
    int inumt = 0;
    int j, npools;
    int j1, j2;
    int myproc = 0;
    codb_procdata_(&myproc, NULL, NULL, NULL, &inumt);
    npools = pm->npools;
    if (it >= 1 && it <= inumt) { j1 = it; j2 = it;    }
    else                        { j1 =  1; j2 = inumt; }
    fprintf(stderr,
	    "%d: codb_print_poolmask_() for h=%d, thread range [%d..%d]; pm=%p; nperm=%d\n",
	    myproc, *handle, j1, j2, pm, pdb->nperm);
    buflen = (1 + npools) * 12 + 200;
    ALLOC(buf, buflen);
    for (it=j1; it<=j2; it++) {
      char *c = buf;
      int inc = sprintf(c,
			"%d: it=%d: pm[0]=%d --> pm[1..%d]=\n%d:  ",
			myproc,it,pm->poolmask[IT][0],npools,myproc);
      c += inc;
      for (j=1; j<=npools; j++) {
	inc = sprintf(c,"%d ",pm->poolmask[IT][j]);
	c += inc;
	if (j%32 == 0 && j<npools) {
	  inc = sprintf(c,"\n%d:  ",myproc);
	  c += inc;
	}
      }
      fprintf(stderr,"%s\n", buf);
    }
    FREE(buf);
  }
}

PUBLIC void
codb_reset_poolmask_(const int *handle, const int *ithread)
{
  ODB_PoolMask *pm = GET_PM(*handle);
  if (pm) {
    int it = *ithread;
    int j, npools;
    npools = pm->npools;
    if (pm->poolmask[IT][0] != 0) {
      /* Poolmasking is OFF by default i.e. poolmask is NOT checked, 
	 unless permanent poolmask has been employed */
      SET_PDB(*handle);
      int set = (pdb->nperm > 0) ? 1 : 0;
      pm->poolmask[IT][0] = set;
      for (j=1; j<=npools; j++) {
	/* If Poolmasking was ON, then EXCLUDE all pools by default */
	/* Pool number j will NOT be processed, unless poolmasking is OFF */
	pm->poolmask[IT][j] = SETPM(j,set);
      }
    } /* if (pm->poolmask[IT][0] != 0) */
  }
}

PUBLIC void
codb_end_poolmask_(const int *handle)
{
  ODB_PoolMask *pm = GET_PM(*handle);
  if (pm) {
    const int version = 0;
    const int idummy = -1;

    /* Release pool_list-space(s), if allocated, with version == 0 */

    apply_poolmasking_(handle, &version, 
		       "", &idummy,
                       &idummy, &idummy, &idummy, &idummy,
		       &idummy, &idummy, &idummy
		       ,0);

    FREE(pm->dbname);
    pm->handle = -pm->handle;
  }
  {
    SET_PDB(*handle);
    pdb->nperm = 0;
    pdb->npermcnt = 0;
    FREE(pdb->perm_list);
  }
}

PUBLIC void
codb_init_poolmask_(const int *handle,
		    const char *dbname,
		    const int *num_poolnos_in
		    /* Hidden arguments */
		    ,int dbname_len)
{
  int it, inumt, j, npools, maxval;
  ODB_PoolMask *pm = NULL;
  char *p = getenv("ODB_POOLMASKING");
  int value = p ? atoi(p) : 0;
  int poolmasking = (value == 1) ? 1 : 0;
  DECL_FTN_CHAR(dbname);

  ALLOC_FTN_CHAR(dbname);

  npools = *num_poolnos_in;

  maxval = permanent_poolmask(handle, p_dbname, npools);

  if (!poolmasking && maxval == 0) {
      int myproc = 0;
      codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
      if (myproc == 1) 
	fprintf(stderr,
		"***INFO: Poolmasking ignored altogether for database '%s'\n",
		p_dbname);
  }
  else if (maxval > 0) {
    poolmasking = 1;
  }

  /* Pointer pm doesn't get initialized at all 
     ==> behaves as if poolmasking doesn't exist at all */

  if (!poolmasking) goto finish;

  if (TEST_PM(*handle)) {
    pm = &db_pm[(*handle)-1];
  }
  else { /* Bailing out */
    goto finish;
  }

  inumt = get_max_threads_();

  pm->handle = *handle;
  pm->npools = npools;
  pm->inumt = inumt;
  if (pm->poolmask) {
    for (it=1; it<=inumt; it++) {
      FREE(pm->poolmask[IT]);
    }
    FREE(pm->poolmask);
  }
  FREE(pm->dbname);
  pm->dbname = STRDUP(p_dbname);

  ALLOC(pm->poolmask, inumt);
  for (it=1; it<=inumt; it++) {
    ALLOC(pm->poolmask[IT], 1 + npools);
    /* Poolmasking is still uninitialized, but the
       subsequent codb_reset_poolmask_() will do the job */
    pm->poolmask[IT][0] = -1;
    codb_reset_poolmask_(handle, &it);
  }

 finish:
  FREE_FTN_CHAR(dbname);
}

PUBLIC void
codb_get_permanent_poolmask_(const int *handle,
			     const int *num_poolnos_in,
			     int  poolnos[],
			     int *num_poolnos_out)
{
  int n = 0;
  SET_PDB(*handle);
  if (*num_poolnos_in >= pdb->npermcnt) {
    int i=0, j;
    for (j=1; j<=pdb->nperm; j++) {
      if (pdb->perm_list[j]) poolnos[i++] = j;
    }
    if (pdb->npermcnt - i != 0) {
      RAISE_SIGNAL(SIGABRT, "Programming error: pdb->npermcnt - i != 0", pdb->npermcnt - i);
    }
    *num_poolnos_out = pdb->npermcnt;
  }
  else {
    *num_poolnos_out = -pdb->npermcnt; /* *num_poolnos_in needs to be bigger i.e. ABS(-npermcnt) */
  }
}


PUBLIC int *
ODB_get_permanent_poolmask(int handle, int *npools)
{
  int n = -1;
  int *poolnos = NULL;
  ODB_PoolMask *pm = GET_PM(handle);
  if (pm) {
    int nout = 0;
    SET_PDB(handle);
    n = pdb->npermcnt;
    CALLOC(poolnos, n);
    codb_get_permanent_poolmask_(&handle, &n, poolnos, &nout);
    if (npools) *npools = nout;
  }
  else if (npools) {
    int j;
    n = *npools;
    ALLOC(poolnos, n);
    for (j=1; j<=n; j++) poolnos[j-1] = j;
  }
  return poolnos; /* Watch for memory leaks here */
}


PUBLIC void
codb_in_permanent_poolmask_(const int *handle,
			    const int *poolno,
			    int *retcode)
{ 
  /* 
     Returns 1 (=true), the Poolno is in valid range and if

     1) it is found from the permanent poolmask list
     2) if the permanent poolmask is not defined at all
     3) if the poolmasking is off

     Otherwise returns 0 (=false)
  */

  int rc = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  ODB_PoolMask *pm = GET_PM(Handle);
  if (pm) {
    SET_PDB(*handle);
    if (pdb->npermcnt > 0 && pdb->perm_list &&
	Poolno >= 1 && Poolno <= pdb->nperm && 
	pdb->perm_list[Poolno]) rc = 1;
  }
  else { 
    /* Poolmasking and/or permanent poolmask was off for this database
       Note: Upper bound range could not be checked; assumed ok */
    if (Poolno >= 1) rc = 1;
  }
  *retcode = rc;
}
			    
PUBLIC int
ODB_in_permanent_poolmask(int handle, int poolno)
{
  int rc = 0;
  codb_in_permanent_poolmask_(&handle, &poolno, &rc);
  return rc;
}

PUBLIC void
codb_get_poolmask_(const int *handle,
		   const int *num_poolnos_in,
		         int  poolnos[],
		         int *poolmask_set,
		         int *num_poolnos_out)
{
  ODB_PoolMask *pm = GET_PM(*handle);
  *poolmask_set = -1;
  *num_poolnos_out = 0;
  if (pm) {
    DEF_IT;
    int j, npools;
    npools = MIN(*num_poolnos_in, pm->npools);
    for (j=1; j<=npools; j++) {
      poolnos[j-1] = pm->poolmask[IT][j];
    }
    *poolmask_set = pm->poolmask[IT][0];
    *num_poolnos_out = npools;
  }
}


PUBLIC void
codb_set_poolmask_(const int *handle,
		   const int *num_poolnos_in,
		   const int  poolnos[],
		   const int *onoff)
{
  ODB_PoolMask *pm = GET_PM(*handle);
  if (pm) {
    DEF_IT;
    int OnOff = *onoff;
    int j, npools;
    SET_PDB(*handle);
    codb_reset_poolmask_(handle, &it);
    npools = *num_poolnos_in;
    for (j=0; j<npools; j++) {
      int poolno = poolnos[j];
      if (poolno >= 1 && poolno <= pm->npools) {
	/* include (= On i.e. 1) / exclude (= Off i.e. 0) 
	   the pool number "poolno" from the execution */
	pm->poolmask[IT][poolno] = SETPM(poolno,OnOff); 
      }
    }
    pm->poolmask[IT][0] = 1;
  }
}

PUBLIC void
codb_poolmasking_status_(const int *handle,
			 int *retcode)
{
  ODB_PoolMask *pm = GET_PM(*handle);
  int rc = pm ? 1 : 0;
  *retcode = rc;
}

PUBLIC void
codb_alloc_poolmask_(const int *maxhandle)
{
  if (db || !maxhandle) return;
  MaxHandle = *maxhandle;
  CALLOC(db, MaxHandle);
  CALLOC(db_pm, MaxHandle);
}
