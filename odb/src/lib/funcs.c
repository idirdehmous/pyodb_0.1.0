
/* funcs.c */

#include "odb.h"
#include "evaluate.h"
#include "node.h"
#include "pcma_extern.h"

#define FUNCS_C 1
#include "funcs.h"

#define CHASE_A_BUG 0

#if CHASE_A_BUG == 1
/* export from ../include/info.h */
extern FILE *ODBc_get_debug_fp(); /* Returns current debug file pointer */
#else
#undef CHASE_A_BUG
#endif

PRIVATE int
cmpstr(const void *A, const void *B)
{
  const funcs_t *a = A;
  const funcs_t *b = B;
  return strcasecmp(a->name, b->name);
}


PRIVATE int
print1func(void *fp, const funcs_t *pf)
{
  int len = 0;
  if (fp && pf) {
    DEF_IT;
    const thsafe_parse_t *thsp = GetTHSP();
    const char *s = pf->name;
    int j, nargs = pf->numargs;
#if 0
    FprintF(fp,"@%p>> %s ; nargs=%d\n", pf, s, nargs);
#else
    len += FprintF(fp,"   %s",s);
    if (nargs > 0) {
      for (j=1; j<=nargs; j++) {
	len += FprintF(fp,"%carg#%d",(j==1)?'(':',',j);
      }
      len += FprintF(fp,")");
    }
    else if (nargs == 0) {
      len += FprintF(fp,"()");
    }
    else {
      len += FprintF(fp,"(<var.args>)");
    }
#endif
  }
  return len;
}


PUBLIC void
printfunc(void *fp)
{
  if (fp) {
    DEF_IT;
    const thsafe_parse_t *thsp = GetTHSP();
    int totlen = 0;
    int j;
    if (NfuncS == 0) initfunc(); /* Normally not needed */
    FprintF(fp,"=== Currently available functions:\n");
    for (j=0; j<NfuncS; j++) {
      const funcs_t *pf = &Func[j];
      totlen += print1func(fp, pf);
      if (totlen > 60) {
	FprintF(fp,"\n");
	totlen = 0;
      }
    } /* for (j=0; j<NfuncS; j++) */
    if (totlen > 0) FprintF(fp,"\n");
  }
}


PUBLIC void
initfunc()
{
  if (NfuncS == 0) {
    DEF_IT;
    const thsafe_parse_t *thsp = GetTHSP();
    funcs_t *pf = Func;
    while (pf->name) {
      if (!pf->u.compile_time) pf->u.compile_time = pf->f.run_time;
      NfuncS++;
      pf++;
    }
    qsort(Func, NfuncS, sizeof(funcs_t), cmpstr);
#if 0
    printfunc(StderR);
#endif
  } /* if (NfuncS == 0) */
}


PUBLIC const void *
getfunc(const char *s, int numargs)
{
  funcs_t *pf = NULL;
  funcs_t key;
#if 0
  if (s && *s == '_') s++; /* Was an aggregate function */
#endif
  key.name = (char *)s;
  pf = bsearch(&key, Func, NfuncS, sizeof(funcs_t), cmpstr);
  if (!(pf && (pf->numargs == numargs || pf->numargs == -1))) pf = NULL;
  return pf;
}


PUBLIC Bool
checkfunc(const char *s, int numargs)
{
  const funcs_t *p = getfunc(s, numargs);
  return p ? true : false;
}


PUBLIC double
callfunc(void *pnode, int *retcode)
{
  DEF_IT;
  const thsafe_parse_t *thsp = GetTHSP();
  int rc = 0;
  double value = RMDI;
  double *d = NULL;
  Node_t *p = pnode;
  if (p) {
#if defined(CHASE_A_BUG)
    FILE *fp_echo = ODBc_get_debug_fp();
#endif
    const char *s = p->name;
    int numargs = p->numargs;
    const funcs_t *pf = p->funcptr ? p->funcptr : getfunc(s, numargs);
#if defined(CHASE_A_BUG)
    ODB_fprintf(fp_echo, "callfunc: '%s' %d pf=%p\n",s,numargs,pf);
#endif
    if (pf) {
      Bool funcresult_to_degrees = (pf->deg2rad > 0 && ((pf->deg2rad & 0x2) == 0x2)) ? true : false;
      int j, nargs = (pf->numargs == -1) ? numargs : pf->numargs;
      Bool use_vararg = (pf->numargs == -1) ? true : false;
#if defined(CHASE_A_BUG)
      ODB_fprintf(fp_echo, "callfunc('%s', numargs=%d) : expected # of args = %d\n",
		  s, numargs, nargs);
#endif
      if (numargs != nargs) {
	ERROR1("callfunc() : # of args mismatch in function %s",-1,s);
      }
#if defined(CHASE_A_BUG)
      ODB_fprintf(fp_echo, "callfunc: --> %s(",s);
#endif
      if (nargs > 0) {
	Bool arg_to_radians = (pf->deg2rad > 0 && ((pf->deg2rad & 0x1) == 0x1)) ? true : false;
	CALLOC(d,nargs);
	for (j=0; j<nargs; j++) {
	  d[j] = RunNode(p->args[j], &rc);
	  if (rc != 0) goto finish;
	  if (arg_to_radians) d[j] = D2R(d[j]);
	  else if (pf->deg2rad < 0 && ABS(pf->deg2rad) != j+1) { /* [hack] */
	    d[j] = D2R(d[j]);
	  }
#if defined(CHASE_A_BUG)
	  ODB_fprintf(fp_echo, "%s%.14g",(j>0) ? "," : "",d[j]); 
#endif
	}
      }
#if defined(CHASE_A_BUG)
      ODB_fprintf(fp_echo, ")");
#endif
      if (use_vararg) {
	value = pf->u.vararg(nargs, d);
      }
      else {
	switch (nargs) {
	case 0: value = pf->u.zeroarg (); break;
	case 1: value = pf->u.onearg  (d[0]); break;
	case 2: value = pf->u.twoarg  (d[0], d[1]); break;
	case 3: value = pf->u.threearg(d[0], d[1], d[2]); break;
	case 4:	value = pf->u.fourarg (d[0], d[1], d[2], d[3]); break;
	case 5:	value = pf->u.fivearg (d[0], d[1], d[2], d[3], d[4]); break;
	case 6:	value = pf->u.sixarg  (d[0], d[1], d[2], d[3], d[4], d[5]); break;
	default: value = pf->u.vararg (nargs, d); break;
	} /* switch (nargs) */
      }
      if (funcresult_to_degrees) value = R2D(value);
#if defined(CHASE_A_BUG)
      ODB_fprintf(fp_echo, " = %.14g\n",value);
#endif
    }
    else {
      ERROR1("callfunc(): Unable to locate function %s",-2,s);
    } /* if (pf) else ... */
  } /* if (p) */
  else {
    ERROR("callfunc(): Nothing to run ???",-3);
  }
 finish:
  FREE(d);
  if (retcode) *retcode = rc;
  return value;
}


PUBLIC const char *
odb_datetime_(int *date_out, int *time_out)
{ /* Thread safe if initialized from within codb_init_omp_locks_() */
  int Date=0, Time=0;
  time_t tp;
  const int bufsize = 80;
  static char **buf = NULL;
  int it = get_thread_id_();

  if (!buf) {
    int j, inumt = get_max_threads_();
    CALLOC(buf,inumt);
    for (j=0; j<inumt; j++) {
      CALLOC(buf[j],bufsize);
    }
  }

  /* Are the next two lines re-entrant & thread safe ?? */
  time(&tp);
  strftime(buf[--it], bufsize, "%Y%m%d %H%M%S", localtime(&tp));

  if (date_out || time_out) {
    sscanf(buf[it],"%d %d", &Date, &Time);
    if (date_out) *date_out = Date;
    if (time_out) *time_out = Time;
  }

  return buf[it];
}


PRIVATE int latlon_rad = -2; /* Not initialized */

PUBLIC int
codb_change_latlon_rad_(const int *newvalue)
{ /* Not thread safe */
  int oldvalue = latlon_rad;
  if (newvalue) {
    latlon_rad = *newvalue;
    if (latlon_rad != -1 &&
	latlon_rad !=  0 && 
	latlon_rad !=  1) latlon_rad = -1; /* i.e. as if undefined */
  }
  return oldvalue;
}

PUBLIC void
codb_init_latlon_rad_()
{ /* Not thread safe, but executed once only */
  if (latlon_rad == -2) {
    /* The same as is ../compiler/odb98.c */
    char *p = getenv("ODB_LATLON_RAD"); /* 1 = radians, 0 = not-radians i.e. degrees, -1 = undefined */
    if (p) {
      latlon_rad = atoi(p);
      if (latlon_rad != -1 &&
	  latlon_rad !=  0 && 
	  latlon_rad !=  1) latlon_rad = -1; /* i.e. as if undefined */
    }
    else
      latlon_rad = -1; /* i.e. as if undefined */
  }
}

PUBLIC double
ODB_lldegrees(double d)
{
  /* Convert argument 'd' to degrees, unless 
     the environment variable $ODB_LATLON_RAD was set 0
     indicating that no conversion is needed, since 'd' 
     was already supplied in degrees */
  if (latlon_rad != 0) d = R2D(d);
  /* if (d > 180) d -= 360; */
  return d;
}

PUBLIC double
ODB_degrees(double d)
{
  /* Convert argument 'd' to degrees, unconditionally */
  d = R2D(d);
  return d;
}

PUBLIC double
ODB_llradians(double d)
{
  /* Convert argument 'd' to radians, unless 
     the environment variable $ODB_LATLON_RAD was set 1
     indicating that no conversion is needed, since 'd'
     was already supplied in radians */
  if (latlon_rad != 1) d = D2R(d);
  /* if (d > pi) d -= 2*pi; */
  return d;
}

PUBLIC double
ODB_radians(double d)
{
  /* Convert argument 'd' to radians, unconditionally */
  d = D2R(d);
  return d;
}

PUBLIC double
ODB_timestamp(double indate, double intime)
{ /* Merge "YYYYMMDD" and "HHMMSS" into "YYYYMMDDHHMMSS" */
  double outstamp = RMDI; /* Initialized to missing data indicator ; indicates error */
  if (indate >= 0 && indate <= INT_MAX &&
      intime >= 0 && intime <= 240000) {
    long long int lldate = (long long int)indate;
    long long int lltime = (long long int)intime;
    long long int tstamp = lldate * 1000000ll + lltime;
    outstamp = tstamp;
    outstamp = trunc(outstamp);
  }
  return outstamp;
}

PUBLIC double
ODB_basetime(double indate, double intime)
{ /* Merge "YYYYMMDD" and "HHMMSS" into "YYYYMMDDHH" */
  double outstamp = RMDI; /* Initialized to missing data indicator ; indicates error */
  if (indate >= 0 && indate <= INT_MAX &&
      intime >= 0 && intime <= 240000) {
    long long int lldate = (long long int)indate;
    long long int lltime = (long long int)intime;
    long long int tstamp = lldate * 1000000ll + (lltime/10000ll);
    outstamp = tstamp;
    outstamp = trunc(outstamp);
  }
  return outstamp;
}

PUBLIC double
ODB_now()
{
  int indate, intime;
  (void) odb_datetime_(&indate, &intime);
  return ODB_timestamp(indate, intime);
}

PUBLIC double
ODB_date_now()
{
  int indate;
  (void) odb_datetime_(&indate, NULL);
  return (double)indate;
}

PUBLIC double
ODB_time_now()
{
  int intime;
  (void) odb_datetime_(NULL, &intime);
  return (double)intime;
}

PUBLIC double
ODB_binlo(double x, double deltax)
{
  return binlo(x, deltax);
}

PUBLIC double
ODB_binhi(double x, double deltax)
{
  return binhi(x, deltax);
}

PRIVATE Boolean
InGenList(double target, double begin, double end, double step)
{
  if (begin > end) {
    /* Swap begin with end and negate step */
    double tmp = begin;
    begin = end;
    end = tmp;
    step = -step;
  }
  if (target < begin || target > end) return false; /* definitely not inside */
  {
    double j;
    for (j=begin; j<=end; j += step) {
      if (target == j) return true; /* found exact match */
      if (step <= 0) break; /* Illegal step; ABS(step) must be > 0 */
    }
  }
  return false; /* not inside */
}

PUBLIC double
ODB_InGenList(double target, double begin, double end, double step)
{
  return (double) InGenList(target, begin, end, step);
}

PUBLIC double
ODB_pi()
{
  return pi; /* A macro #define from privpub.h */
}

PUBLIC double
ODB_speed(double u, double v)
{
  return Speed(u,v);
}

PUBLIC double
ODB_dir(double u, double v)
{
  return Dir(u,v);
}

PUBLIC double
ODB_ucom(double dd, double ff)
{
  return Ucom(dd,ff);
}

PUBLIC double
ODB_vcom(double dd, double ff)
{
  return Vcom(dd,ff);
}

PUBLIC double
ODB_myproc()
{ /* Thread-safe, since static variable initialized once, outside the OpenMP parallel region */
  static int myproc = -1;
  if (myproc == -1) codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
  return myproc;
}

PUBLIC double
ODB_nproc()
{ /* Thread-safe, since static variable initialized once, outside the OpenMP parallel region */
  static int nproc = -1;
  if (nproc == -1) codb_procdata_(NULL, &nproc, NULL, NULL, NULL);
  return nproc;
}

PUBLIC double
ODB_pid()
{ /* Thread-safe, since static variable initialized once, outside the OpenMP parallel region */
  static int pid = -1;
  if (pid == -1) codb_procdata_(NULL, NULL, &pid, NULL, NULL);
  return pid;
}

PUBLIC double
ODB_tid()
{ /* Thread-safe */
  int tid = get_thread_id_();
  return tid;
}

PUBLIC double
ODB_nthreads()
{ /* Thread-safe */
  int nthreads = get_max_threads_();
  return nthreads;
}

PUBLIC void
init_proc_funcs()
{
  (void) ODB_myproc();
  (void) ODB_nproc();
  (void) ODB_pid();
}
