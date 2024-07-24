#include "odb.h"

/* Run-time tracing stuff */

PRIVATE FILE *fp_trace = NULL;
PRIVATE boolean trace_atexit_done = 0;
PRIVATE int trace_flush_freq = 0;
PRIVATE int trace_proc = -1;

PRIVATE o_lock_t PBAR_mylock = 0; /* A specific OMP-lock; initialized only once in
				    odb/lib/codb.c, routine codb_init_omp_locks_() */

/* from libifsaux.a */
extern double util_walltime_();        /* Wall clock time in seconds */
extern double util_cputime_();         /* CPU-times (user+system+child(s)) in seconds */
extern int util_ihpstat_(int *option); /* Peak memory usage in 8 byte words */

#define KILO 1024.0
#define MEGA 1048576.0
#define GIGA 1073741824.0

#ifdef VPP
PRIVATE long long
getstackusage_odb()
{
  extern long long getstackusage_();
  long long stack_now;
  static long long init_stack = 0;
  static int first_time = 1;
  if (first_time) {
    init_stack = getstackusage_();
    first_time = 0;
  }
  stack_now = getstackusage_() - init_stack;
  return stack_now;
}
#endif

PRIVATE char *
trace_stat(boolean final_call)
{
  static char buf[1024];
#ifdef VPP
  extern long long int gethwm_();
#elif defined(SGI)
  PRIVATE long long int gethwm();
  PRIVATE void pr_infos(double *size, double *rssize);
#elif defined(RS6K)
  extern long long int getstk_();
  extern long long int gethwm_();
  extern long long int getrss_();
  extern long long int getcurheap_();
  extern long long int getpag_();
#elif defined(LINUX)
  extern long long int gethwm_();
  extern long long int getrss_();
#else
  int option = 1;
#endif
  double hwm, maxrss, rssnow, stack, paging;
  double wall, cpu;

#ifdef VPP
  hwm = gethwm_();
  maxrss = hwm;
  rssnow = maxrss;
  stack = getstackusage_odb();
  paging = 0;
#elif defined(SGI)
  pr_infos(&hwm, &stack); /* stack is misleading; is actually maxrss for SGI */
  maxrss = stack;
  rssnow = maxrss;
  paging = 0;
#elif defined(RS6K)
  hwm = gethwm_(); 
  maxrss = getrss_();
  rssnow = getcurheap_();
  stack = getstk_();
  paging = getpag_();
#elif defined(LINUX)
  hwm = gethwm_(); 
  maxrss = getrss_();
  rssnow = getrss_();
  stack = 0;
  paging = 0;
#else
  hwm = 8.0 * util_ihpstat_(&option);
  maxrss = hwm;
  rssnow = maxrss;
  stack = 0;
  paging = 0;
#endif
  wall = util_walltime_();
  cpu = util_cputime_();

  if (hwm > GIGA || maxrss > GIGA || rssnow > GIGA || stack > GIGA) {
    sprintf(buf,"%.1fG\t%.1fG\t%.1fG\t%.1fG\t%.0f\t%.2f\t%.2f",
	    hwm/GIGA, 
	    maxrss/GIGA, 
	    rssnow/GIGA, 
	    stack/GIGA,
	    paging,
	    wall,
	    cpu);
  }
  else if (hwm > MEGA || maxrss > MEGA || rssnow > MEGA || stack > MEGA) {
    sprintf(buf,"%.1fM\t%.1fM\t%.1fM\t%.1fM\t%.0f\t%.2f\t%.2f",
	    hwm/MEGA, 
	    maxrss/MEGA, 
	    rssnow/MEGA, 
	    stack/MEGA,
	    paging,
	    wall,
	    cpu);
  }
  else {
    sprintf(buf,"%.1fK\t%.1fK\t%.1fK\t%.1fK\t%.0f\t%.2f\t%.2f",
	    hwm/KILO, 
	    maxrss/KILO, 
	    rssnow/KILO, 
	    stack/KILO,
	    paging,
	    wall,
	    cpu);
  }

  return buf;
}

PUBLIC void
codb_trace_begin_()
{
  if (fp_trace) {
    coml_set_lockid_(&PBAR_mylock);
    {
      const char *dtbuf = odb_datetime_(NULL, NULL); /* ptr to a static variable; do NOT free */
      int myproc, tid, pid;
      codb_procdata_(&myproc, NULL, &pid, &tid, NULL);
      fprintf(fp_trace,"%s: ",dtbuf);
      fprintf(fp_trace, "%s : Tracing started on PE#%d, pid=%d [thread#%d].\n", 
	      trace_stat(0), myproc, pid, tid);
    }
    coml_unset_lockid_(&PBAR_mylock);
  }
}

PUBLIC void
codb_trace_end_()
{
  if (fp_trace) {
    coml_set_lockid_(&PBAR_mylock);
    {
      const char *dtbuf = odb_datetime_(NULL, NULL); /* ptr to a static variable; do NOT free */
      int myproc, tid, pid;
      codb_procdata_(&myproc, NULL, &pid, &tid, NULL);
      fprintf(fp_trace,"%s: ",dtbuf);
      fprintf(fp_trace, "%s : Tracing finished on PE#%d, pid=%d [thread#%d].\n",
	      trace_stat(1), myproc, pid, tid);
      fflush(fp_trace);
      if (fp_trace != stderr && fp_trace != stdout) fclose(fp_trace);
      fp_trace = NULL;
    }
    coml_unset_lockid_(&PBAR_mylock);
  }
}


PUBLIC void
codb_trace_(const int  *handle,
	    const int  *mode,
	    const char *msg,
	    const int   args[],
	    const int  *numargs,
	    /* Hidden arguments */
	    int msg_len)
{
  if (fp_trace) {
    coml_set_lockid_(&PBAR_mylock);
    {
      const char *dtbuf = odb_datetime_(NULL, NULL); /* ptr to a static variable; do NOT free */
      int tid = get_thread_id_();
      int Handle = *handle;
      int Mode = *mode;
      int j, Numargs = MIN(*numargs, MAXTRACEARGS);
      unsigned char ch;
      DECL_FTN_CHAR(msg);
      
      ALLOC_FTN_CHAR(msg);
      
      fprintf(fp_trace,"%s: [#%d] ",dtbuf, tid);
      
      ch = ':';
      if (Mode == 1) ch = '>';
      else if (Mode == 0) ch = '<';
      
      if (Handle >= 0) {
	fprintf(fp_trace,
		"%s %c %d %s", 
		trace_stat(0), 
		ch, Handle, p_msg);
      }
      else {
	fprintf(fp_trace,
		"%s %c %s", 
		trace_stat(0), 
		ch, p_msg);
      }

      for (j=0; j<Numargs; j++) fprintf(fp_trace," %d",args[j]);
      
      fprintf(fp_trace,"\n");

      FREE_FTN_CHAR(msg);

      if (trace_flush_freq > 0) {
	static uint callno = 0;
	callno++;
	if (callno%trace_flush_freq == 0) fflush(fp_trace);
      }
    }
    coml_unset_lockid_(&PBAR_mylock);
  }
}


PUBLIC void
codb_trace_init_(int *trace_on)
{
  boolean proceed = 0;
  int myproc;
  char *proc = getenv("ODB_TRACE_PROC");

  codb_trace_end_();
  if (trace_on) *trace_on = 0;

  codb_procdata_(&myproc, NULL, NULL, NULL, NULL);

  if (proc) {
    trace_proc = atoi(proc);
    proceed = (trace_proc == -1 || trace_proc == myproc);
  }

  if (proceed) {
    char *p;
    int rc, unit;
    char *pfile = getenv("ODB_TRACE_FILE");
    if (!pfile) pfile = "stdout"; /* std-output, not std-error to get line buffering */

    if (strequ(pfile,"stderr") || strequ(pfile,"stdout")) {
      fp_trace = strequ(pfile,"stderr") ? stderr : stdout;
    }
    else {
      if (strchr(pfile,'%')) {
	ALLOC(p,strlen(pfile) + 20);
	sprintf(p,pfile,myproc);
      }
      else {
	p = STRDUP(pfile);
      }
      cma_open_(&unit, p, "w", &rc, strlen(p), 1);
      if (rc < 1) {
	perror(p);
	FREE(p);
	return;
      }
      FREE(p);
      
      fp_trace = CMA_get_fp(&unit);
    }
    
    if (fp_trace) {
      codb_trace_begin_();
      if (!trace_atexit_done) {
	atexit(codb_trace_end_);
	trace_atexit_done = 1;
      }
      if (trace_on) *trace_on = 1;
      
      {
	char *x = getenv("ODB_TRACE_FLUSH_FREQ");
	if (x) trace_flush_freq = atoi(x);
      }
    } /* if (fp_trace) */
  } /* if (proceed) */
}


PUBLIC FILE *
ODB_trace_fp()
{
  return fp_trace;
}


PUBLIC void
ODB_debug_print_index(FILE *fp, const char *View, int poolno, int n, int ntbl, ...)
{
  static int rc = 0;
  static int first_time = 1;

  if (first_time) {
    char *p = getenv("ODB_DEBUG_PRINT_INDEX");
    if (p) rc = atoi(p);
    first_time = 0;
  }

  if (!rc) 
    return;
  else if (fp && n > 0) {
    int jtbl;
    int myproc = 0;
    int it = 0;
    va_list ap;
    va_start(ap, ntbl);
    
    codb_procdata_(&myproc, NULL, NULL, &it, NULL);
    for (jtbl=0; jtbl<ntbl; jtbl++) {
      const char *Table = va_arg(ap, const char *);
      const int *Index_Table = va_arg(ap, const int *);
      const void *Table_addr = va_arg(ap, const void *);
      int Nrows = va_arg(ap, int);
      int j, jnext, newline;
      fprintf(fp,
	      "\nDEBUG_PRINT :: myproc,it,poolno=<%d;%d;%d> ; View=%s ;"
	      " Index_%s[0:n=%d) at %p ;"
	      " Nrows(table %s at %p)=%d\n",
	      myproc, it, poolno, View, 
	      Table, n, Index_Table, 
	      Table, Table_addr, Nrows);
      fflush(fp);
      for (j=0; j<n; j++) {
	if (j%10 == 0) fprintf(fp,"<%d;%d;%d>", myproc, it, poolno);
	jnext = j + 1;
	newline = (jnext%10 == 0 || jnext == n) ? 1 : 0;
	fprintf(fp,"%12d%s", Index_Table[j], newline ? "\n" : "");
	if (newline) fflush(fp);
      }
      fflush(fp);
    }
    va_end(ap);
  } /* if (rc && fp && n > 0) */
}

static int PBAR_iounit = 0;
static int PBAR_maxpool = 0;
static int PBAR_totalrows = 0;
static double PBAR_totwtime = 0;

PUBLIC void init_PBAR_lock()
{
  INIT_LOCKID_WITH_NAME(&PBAR_mylock,"tracing.c:PBAR_mylock");
  PBAR_totwtime = util_walltime_(); /* Initialize wall clock timer */
}


PUBLIC void
codb_set_progress_bar_(const char *param,
		       const int *value
		       /* Hidden arguments */
		       , int param_len)
{
  /* Preset semi-permanent values for use by codb_progress_bar_() */
  if (param && value) {
    coml_set_lockid_(&PBAR_mylock);
    if (strncaseequ(param,"iounit",param_len)) {
      PBAR_iounit = *value;
    }
    else if (strncaseequ(param,"maxpool",param_len)) {
      PBAR_maxpool = *value;
    }
    else if (strncaseequ(param,"totalrows",param_len)) {
      PBAR_totalrows = *value;
    }
    coml_unset_lockid_(&PBAR_mylock);
  }
}

PUBLIC void
codb_get_progress_bar_(const char *param,
		       int *value
		       /* Hidden arguments */
		       , int param_len)
{
  /* Get current semi-permanent values for use by codb_progress_bar_() */
  if (param && value) {
    coml_set_lockid_(&PBAR_mylock);
    if (strncaseequ(param,"iounit",param_len)) {
      *value = PBAR_iounit;
    }
    else if (strncaseequ(param,"maxpool",param_len)) {
      *value = PBAR_maxpool;
    }
    else if (strncaseequ(param,"totalrows",param_len)) {
      *value = PBAR_totalrows;
    }
    coml_unset_lockid_(&PBAR_mylock);
  }
}

PUBLIC void
codb_progress_nl_(const int *iounit)
{
  FILE *fp = stderr;
  int Iounit = iounit ? *iounit : PBAR_iounit;
  if (Iounit == 6) fp = stdout;
  if (fp) {
    fprintf(fp, "\n");
    fflush(fp);
  }
}

PUBLIC void
codb_progress_bar_(const int *iounit,
		   const char *dtname,
		   const int *curpool,
		   const int *maxpool,
		   const int *currows,
		   const int *totalrows,
		   const double *wtime,
		   const int *newline
		   /* Hidden arguments */
		   , int dtname_len)
{
  coml_set_lockid_(&PBAR_mylock);
  {
    int tid = get_thread_id_();
    extern long long int gethwm_();
    double hwm = gethwm_();
    FILE *fp = stderr;
    int Iounit = iounit ? *iounit : PBAR_iounit;
    int Maxpool = maxpool ? *maxpool : PBAR_maxpool;
    int Totalrows = totalrows ? *totalrows : PBAR_totalrows + (*currows);
    static double maxhwm = 0;
    double totwtime;
    double pc = (*curpool > 0 && Maxpool > 0) ? (double)(*curpool)/(Maxpool) : 0;
    int j, ipc;
    static const int maxipc = 20;
    static const char propel[4] = { '-', '\\', '|', '/' };
    static int npropel = 0;
    static int maxchar = 0;
    int nchar = 0;
    if (Iounit == 6) fp = stdout;
    if (hwm > maxhwm) maxhwm = hwm;
    /* if (tid != 1) fp = NULL; */
    pc *= 100;
    pc = MAX(0,pc);
    pc = MIN(pc, 100);
    ipc = maxipc * (pc/100);
    if (newline && *newline) { ipc = maxipc; pc = 100; }
    ipc = MIN(ipc,maxipc);
    pc = MIN(pc,100);
    if (fp) {
      nchar += fprintf(fp,"%*.*s #%d: %d/%d rows, %d/%d pools : %c",
		       dtname_len, dtname_len, dtname,
		       tid,
		       *currows, Totalrows, *curpool, Maxpool, 
		       ((ipc == maxipc) || (newline && *newline)) ? '+'  : propel[npropel]);
      if (++npropel >= 4) npropel = 0;
      for (j=0; j<ipc; j++) nchar += fprintf(fp,"#");
      for (j=ipc; j<maxipc; j++) nchar += fprintf(fp,".");
    }
    totwtime = util_walltime_() - PBAR_totwtime;
    if (fp) {
      double Tremains = (pc > 0) ? ((100 - pc) * totwtime / pc) : 0;
      nchar += fprintf(fp,": %5.1f%% (%.3f/%.3f/%.3fs,%.0f/%.0fMB)",
		       pc,*wtime,totwtime,Tremains,hwm/MEGA,maxhwm/MEGA);
      if (maxchar > nchar) {
	int nblanks = maxchar - nchar;
	for (j=0; j<nblanks; j++) fprintf(fp," ");
      }
      else {
	maxchar = nchar;
      }
      fprintf(fp,"%c",(newline && *newline) ? '\n' : '\r');
      fflush(fp);
    }
    if (!totalrows) PBAR_totalrows = Totalrows;
  }
  coml_unset_lockid_(&PBAR_mylock);
}


#ifdef SGI
  /* A few hacks for SGI, since getrusage() does not work ... sigh !! */

PRIVATE int
gethwm_()
{
  extern void *sbrk(int);
  extern int _etext[];
  int *p = sbrk(0);
  int rc = (p - _etext)*sizeof(int);
  return rc;
}

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/signal.h>
#include <sys/fault.h>
#include <sys/syscall.h>
#include <sys/procfs.h>
#include <fcntl.h>

PRIVATE void
pr_infos(double *size, double *rssize)
{
  static int pid = -1;
  static int fildes = -1;
  static int pagesize = 1;
  static prpsinfo_t psinfo;

  if (size)   *size = 0;
  if (rssize) *rssize = 0;

  if (pid == -1) {
    char procfile[40];
    pid = getpid();
    sprintf(procfile,"/proc/%10.10d",pid);
    fildes = open(procfile, O_RDONLY);
    pagesize = getpagesize();
  }

  if (fildes == -1) return;

  if (ioctl(fildes, PIOCPSINFO, &psinfo) == -1) return;
  
  if (size  ) *size   = psinfo.pr_size * pagesize;
  if (rssize) *rssize = psinfo.pr_rssize * pagesize;
}

#endif
