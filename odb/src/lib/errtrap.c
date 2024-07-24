#include "odb.h"
#include "pcma_extern.h"

#ifdef VPP
#include <ucontext.h>
#endif

#ifdef RS6K
extern void xl__trbk_();
extern void xl__trce_(int sig, siginfo_t *sigcode, void *sigcontextptr);
#endif

/* Error trapping */

PRIVATE char *abfunc_name = NULL;

extern void errtra_();

PRIVATE int Myproc = 0;
PRIVATE int Nproc = 0;
PRIVATE int Pid = 0;

PUBLIC void
ODB_packing_trace(int mode,
		  const char *type, const char *name, const char *table,
		  int pmethod, int pmethod_from_datatype,
		  int dlen, int sizeof_dlen, int pdlen, uint datatype)
{
  FILE *fp = ODB_trace_fp();
  if (fp) {
    int packed = pdlen;
    int unpacked = dlen * sizeof_dlen;
    double ratio;
    ODB_Trace TracE;

    TracE.handle = -1; /* Irrelevant and/or unknown */
    TracE.mode = mode;
    TracE.msg = NULL;
    TracE.msglen = 64;
    TracE.numargs = 7;

    TracE.args[0] = pmethod;
    TracE.args[1] = pmethod_from_datatype;
    TracE.args[2] = dlen;
    TracE.args[3] = sizeof_dlen;
    TracE.args[4] = packed;
    TracE.args[5] = unpacked;
    TracE.args[6] = datatype;
    
    ratio = PKRATIO(unpacked, packed);
    
    if (TracE.mode == 0) {
      TracE.msglen += strlen(type) + strlen(name) + strlen(table);
      ALLOC(TracE.msg, TracE.msglen);
      sprintf(TracE.msg, "Unpacking(%s:%s@%s,%.2f%%)", type, name, table, ratio);
    }
    else if (TracE.mode == 1) {
      TracE.msglen += strlen(type) + strlen(name) + strlen(table);
      ALLOC(TracE.msg, TracE.msglen);
      sprintf(TracE.msg, "Packing(%s:%s@%s,%.2f%%)", type, name, table, ratio);
    }
    else if (TracE.mode == 2) {
      TracE.msglen += strlen(type) + strlen(name) + strlen(table);
      ALLOC(TracE.msg, TracE.msglen);
      sprintf(TracE.msg, "Enforced packing(%s:%s@%s,%.2f%%)", type, name, table, ratio);
    }
    else {
      return;
    }

    codb_trace_(&TracE.handle, &TracE.mode,
		TracE.msg, TracE.args, &TracE.numargs, TracE.msglen);
    
    FREE(TracE.msg);
  }
}

/* ODB_packing() moved here to ../aux/pcma_9.c on 09/03/2005 by SS */

PUBLIC void
ODB_packing_setup(int *Prtmsg, int *Threshold, double *Factor)
{
  static int prtmsg = 0;
  static int threshold = 8192;
  static double factor = 2.0;
  static boolean first_time = 0;
  if (!first_time) {
    char *p;
    p = getenv("ODB_PACK_PRTMSG");
    if (p) {
      prtmsg = atoi(p);
      prtmsg = MAX(0,prtmsg);
    }
    p = getenv("ODB_PACK_THRESHOLD");
    if (p) {
      threshold = atoi(p);
      threshold = MAX(0,threshold);
    }
    p = getenv("ODB_PACK_FACTOR");
    if (p) {
      factor = atof(p);
      factor = MAX(0,factor);
    }
    first_time = 1;
  }
  if (Prtmsg) *Prtmsg = prtmsg;
  if (Threshold) *Threshold = threshold;
  if (Factor)    *Factor = factor;
}

PUBLIC void
codb_procdata_(int *myproc,
	       int *nproc,
	       int *pid,
	       int *it,
	       int *inumt)
{
  if (myproc) *myproc = Myproc;
  if (nproc)  *nproc  = Nproc;
  if (pid)    *pid    = Pid;
  if (it)     *it     = get_thread_id_();
  if (inumt)  *inumt  = get_max_threads_();
}

PUBLIC void
codb_test_index_range_(const int vec[],
		       const int *veclen,
		       const int *low,
		       const int *high,
		       int *retcode)
{
  /* Count how many values in vec[] are OUTSIDE of the valid range [low:high], both inclusive */

  int rc = 0;
  int j;
  int N = *veclen;
  int Lo = *low;
  int Hi = *high;
  static int do_test = 0;
  static int first_time = 1;

  if (first_time) {
    /* ODB_TEST_INDEX_RANGE : 0 = no test, procid [1..NPES] or -1 (=all procs) test(s) */
    char *env = getenv("ODB_TEST_INDEX_RANGE"); 
    if (env) {
      int myproc = 0;
      int value = atoi(env);
      codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
      do_test = (value == -1 || value == myproc) ? 1 : 0;
    }
    first_time = 0;
  }

  if (do_test) {
    for (j=0; j<N; j++) {
      if (vec[j] < Lo || vec[j] > Hi) rc++;
    }
    if (rc > 0) {
      const int maxcount = 10;
      int cnt = 0;
      int it = 0;
      int myproc = 0;
      codb_procdata_(&myproc, NULL, NULL, &it, NULL);
      fprintf(stderr,
	      "\n*** Error: Total %d incides (out of %d) are out of bounds [%d,%d]"
	      " on thread@task = %d@%d. For example:\n",
	      rc, N, Lo, Hi, it, myproc);
      for (j=0; j<N; j++) {
	if (vec[j] < Lo || vec[j] > Hi) {
	  fprintf(stderr,
		  ">>> %d@%d: index[%d] = %d\n",
		  it, myproc,j,vec[j]);
	  if (++cnt >= maxcount) break;
	}
      }
      RAISE(SIGABRT);
    }
  }

  *retcode = rc;
}

PUBLIC void
codb_init_(const int *myproc, 
	   const int *nproc)
{
  Myproc = myproc ? *myproc : 1;
  Nproc  = nproc ? *nproc : 1;
  Pid = getpid();
  if (Nproc > 1) {
    /* Line-bufferize stderr when Nproc > 1 to avoid messy outputs */
    int rc = 0;
    char *buf = NULL;
    ALLOC(buf, BUFSIZ+8);
    rc = setvbuf(stderr, buf, _IOLBF, BUFSIZ);
    if (rc != 0) {
      /* Report problem; do not abort for this */
      perror("codb_init_(): setvbuf(stderr) failed");
    }
  }
  setup_sort_(NULL, NULL);
}


PUBLIC void 
codb_register_abort_func_(const char *name,
                          /* Hidden arguments */
                          int name_len)
{
  DECL_FTN_CHAR(name);
  ALLOC_FTN_CHAR(name);
  if (abfunc_name) FREE(abfunc_name);
  ALLOC(abfunc_name,strlen(p_name)+1);
  strcpy(abfunc_name,p_name);
  FREE_FTN_CHAR(name);
}


#if defined(SGI) || defined(RS6K_OLD)
PUBLIC void 
errtra_()
{
  char *p = getenv("ODB_ERRTRA");
  int value = p ? atoi(p) : 0;
  if (value > 0) {
    char cmd[1024];
#ifdef SGI
    static char def[] = "/usr/bin/echo where | /usr/bin/dbx -c /dev/null -p";
#endif
#ifdef RS6K_OLD
    static char def[] = "/usr/bin/echo where | /usr/bin/dbx -c /dev/null -F -x -a";
#endif
    char *x = getenv("ODB_ERRTRA_CMD");
    boolean x_given = 0;
    if (!x) x = def;
    else x_given = 1;
    sprintf(cmd,"%s %d",x,getpid());
    if (x_given) fprintf(stderr,"\nerrtra_(): Executing '%s' ...\n",cmd);
    fflush(stderr);
    fflush(stdout);
    system(cmd);
    fflush(stderr);
    fflush(stdout);
  }
}
#endif

#ifdef RS6K
PUBLIC void 
errtra_()
{
  xl__trbk_();
}
#endif


PUBLIC void
codb_abort_func_(const char *msg,
		 /* Hidden arguments */
		 int msg_len)
{
  char *messy_output = getenv("ODB_MESSY_OUTPUT");
  int enforce = 1;
  static boolean trace_done = 0;
  ODB_iolock(-1); /* To avoid messy tracebacks */
  if (messy_output) {
    int imessy = atoi(messy_output);
    if (imessy > 0) {
      print_forfunc_(&enforce, msg, msg_len);
      ctxdebug_(&enforce, msg, msg_len);
      cmdb_debug_(&enforce, msg, msg_len);
      codb_print_vars_(&enforce, msg, msg_len);
    }
  }
  
  if (!trace_done) {
#if defined(SGI) || defined(RS6K)
    errtra_();
#endif
#ifdef VPP
    _TraceCalls(NULL);
#endif
    trace_done = 1;
  }

  ODB_iolock(-1);
  if (abfunc_name && strequ(abfunc_name,"cmpl_abort")) {
    extern void cmpl_abort_(const char s[], 
			    /* Hidden arguments */
			    int slen);
    fprintf(stderr,
	    "%s: Calling abort function '%s' on PE#%d (pid=%d)\n",
	    "codb_abort_func_()", abfunc_name, Myproc, Pid);
#ifdef VPP
    sleep(5);
#endif

    if (msg && msg_len > 0) {
      fprintf(stderr,"%*.*s\n",msg_len,msg_len,msg);
    }

    /* sometimes gets stuck --> commented out */
    /* cmpl_abort_(msg, msg_len); */

    /* instead the following should work */
    RAISE(SIGABRT);
  }
  abort(); /* No bail outs ! */
}

#ifdef RS6K
PRIVATE void
trce_alarm(int sig)
{
  if (sig == SIGALRM) {
    fflush(stdout);
    fflush(stderr);
    RAISE(SIGKILL);
  }
}

PUBLIC void 
my_xl__trce_(int sig, siginfo_t *si, void *uctext)
{
  extern long long int gethwm_();
  long long int maxmem = gethwm_();
  int tid = get_thread_id_();

  maxmem /= 1048576U;
  fprintf(stderr,
	  "my_xl__trce_(): Received signal#%d (PE#%d, tid#%d). Maxmem: %d MB. Aborting ...\n",
	  sig,Myproc,tid,(int)maxmem);
  signal(SIGALRM, trce_alarm);
  alarm(5);
  xl__trce_(sig, si, uctext);
  raise(SIGKILL); /* you are not supposed to get here */
}

PUBLIC void 
my_xl__trce(int sig, siginfo_t *si, void *uctext)
{
  my_xl__trce_(sig, si, uctext); /* for non -qextname cases in Fortran */
}
#endif

PUBLIC void
codb_set_signals_()
{
  static int done = 0;
  if (!done) {
    FILE *fp = ODB_trace_fp();
    /* Original codehas been removed and replaced with a call to Dr.Hook */
    extern void c_drhook_init_signals_(const int *enforce);
    const int enforce = 1;
    c_drhook_init_signals_(&enforce);
    if (fp) {
      fprintf(fp,">> Library compiled on %s  %s\n",__DATE__,__TIME__);
      fprintf(fp,">> Signals maybe caught via Dr.Hook\n");
    }
    done = 1;
  }
}

PUBLIC void
codb_ignore_alarm_()
{
  signal(SIGALRM, SIG_IGN);
}

PRIVATE void
catch_alarm(int sig)
{
  if (sig == SIGALRM) codb_ignore_alarm_();
}

PUBLIC void
codb_catch_alarm_(int *sig)
{
  catch_alarm(*sig);
}

PUBLIC void
codb_set_alarm_()
{
  signal(SIGALRM, catch_alarm);
}

PUBLIC void
codb_send_alarm_(int *pid)
{
  kill(*pid, SIGALRM);
}


#ifdef DYNAMIC_LINKING
/* Make certain ($#*@?!) dynamic linkers happy ! */

void
exec_dummy(int dummy, ...) { }

void 
cdummy_load_()
{
  int dummy = 0;
  exec_dummy(dummy, ODB_iolock);
  exec_dummy(dummy, ODB_unpack_UINT);
  exec_dummy(dummy, ODB_pack_DBL);
  exec_dummy(dummy, ODB_packing);
  exec_dummy(dummy, ODB_add_funcs);
  exec_dummy(dummy, ODB_get_packing_consts);
  exec_dummy(dummy, ODB_pack_INT);
  exec_dummy(dummy, ODB_error);
  exec_dummy(dummy, ODB_packing_setup);
  exec_dummy(dummy, ODB_packing_trace);
  exec_dummy(dummy, ODB_pack_UINT);
  exec_dummy(dummy, ODB_min_alloc);
  exec_dummy(dummy, ODB_inc_alloc);
  exec_dummy(dummy, ODB_unpack_INT);
  exec_dummy(dummy, ODB_trace_fp);
  exec_dummy(dummy, ODB_unpack_DBL);
  exec_dummy(dummy, ODB_getval_var);
  exec_dummy(dummy, ODB_WildCard);
}
#endif

/* Some dirty hacks to get the F90-level working (needed at MF!!)*/

#if defined(VPP5000) || defined(HPPA)
void 
putenv_(const char *s, 
	int slen) 
{
  char *p = NULL;
  ALLOC(p, slen+1);
  strncpy(p,s,slen); 
  p[slen]='\0';
  putenv(p); 
  /* FREE(p); */
}
#endif

