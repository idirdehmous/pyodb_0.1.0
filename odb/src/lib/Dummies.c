#include <stdio.h>
#include <string.h>
#ifdef USE_MALLOC_H
#include <malloc.h>
#endif
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>

/* General do-nothing routine */

void 
Dummies() { }

#ifdef NEED_PB_ROUTINES
/* In case PB-routines are still involved ... */

FILE *
pbfp()
{
  fprintf(stderr,"pbfp()@%s: You were not supposed to call this routine\n",__FILE__);
  raise(SIGABRT);
  return NULL;
}

void
pbopen_()
{
  fprintf(stderr,"pbopen_()@%s: You were not supposed to call this routine\n",__FILE__);
  raise(SIGABRT);
}

void
pbclose_()
{
  fprintf(stderr,"pbclose_()@%s: You were not supposed to call this routine\n",__FILE__);
  raise(SIGABRT);
}

void
pbread_()
{
  fprintf(stderr,"pbread_()@%s: You were not supposed to call this routine\n",__FILE__);
  raise(SIGABRT);
}

void
pbread2_()
{
  fprintf(stderr,"pbread2_()@%s: You were not supposed to call this routine\n",__FILE__);
  raise(SIGABRT);
}

void
pbwrite_()
{
  fprintf(stderr,"pbwrite_()@%s: You were not supposed to call this routine\n",__FILE__);
  raise(SIGABRT);
}

void
pbbufr_()
{
  fprintf(stderr,"pbbufr_()@%s: You were not supposed to call this routine\n",__FILE__);
  raise(SIGABRT);
}
#endif /* NEED_PB_ROUTINES */

/* Some dirty hacks to get the F90-level working */

void 
codb_getenv_(const char *s, 
	     char *value,
	     /* Hidden arguments */
	     int slen,
	     const int valuelen) 
{
  char *env = NULL;
  char *p = malloc(slen+1);
  if (!p) {
    fprintf(stderr,"codb_getenv_(): Unable to allocate %d bytes of memory\n", slen+1);
    raise(SIGABRT);
  }
  memcpy(p,s,slen); 
  p[slen]='\0';
  memset(value, ' ', valuelen);
  env = getenv(p);
  if (env) {
    int len = strlen(env);
    if (valuelen < len) len = valuelen;
    memcpy(value,env,len); 
  }
  free(p);
}

void
codb_getcwd_(char *s, 
	     /* Hidden arguments */
	     int slen)
{ /* Get current working directory */
  if (!getcwd(s,slen)) {
    perror("getcwd");
    memset(s,' ',slen);
  }
  else {
    int len = strlen(s);
    if (len < slen) memset(&s[len],' ',slen-len);
  }
}

void 
codb_putenv_(const char *s, 
	     /* Hidden argument */
	     int slen) 
{
  const char *x = &s[slen-1];
  /* strip trailing blanks first */
  while (slen > 0 && *x-- == ' ') { slen--; }
  /* now go ahead */
  {
    char *p = malloc(slen+1);
    if (!p) {
      fprintf(stderr,"codb_putenv_(): Unable to allocate %d bytes of memory\n", slen+1);
      raise(SIGABRT);
    }
    memcpy(p,s,slen); 
    p[slen]='\0';
    putenv(p); 
    /* Cannot free(p); , since putenv() uses this memory area for good ;-( */
  }
}

void
codb_pause_() 
{ 
#if !defined(CRAYXT)
  (void) pause(); 
#endif
}

void 
codb_raise_(const int *sig)
{
  raise(*sig);
}

void 
codb_sleep_(const int *seconds)
{
  (void) sleep(*seconds);
}

void
codb_usleep_(const int *usecs)
{
  if (usecs && *usecs > 0) {
    struct timeval t;
    t.tv_sec =  (*usecs)/1000000;
    t.tv_usec = (*usecs)%1000000;
    (void) select(0, NULL, NULL, NULL, &t);
  }
}

void
codb_wait_(const int *pid, const int *nohang, int *rc)
{
  /* CALL CODB_WAIT(-1, 1, iret)   ! Wait any (child) process-id to finish, but do not hang/block */
  /* CALL CODB_WAIT(ipid, 0, iret) ! Wait until process-id ipid to finished */
  /* 
   *rc contains the process-id that has finished, 
   or -1 in error 
   or 0 if no-hang was used but process has not yet finished
  */
  int status;
#ifdef CRAYXT
  *rc = -1;
#else
  if (*nohang) {
    *rc = waitpid(*pid, &status, WNOHANG);
  }
  else {
    *rc = waitpid(*pid, &status, 0);
  }
#endif
  if (*rc < 0) {
    fprintf(stderr,
	    "***Error in codb_wait_(pid=%d, nohang=%d) --> retcode = %d\n",
	    *pid, *nohang, *rc);
  }
}

void 
codb_subshell_(const char *cmd, int *pid
	       /* Hidden argument */
	       ,int cmdlen) 
{
#ifdef CRAYXT
  *pid = -1;
#else
  *pid = fork();
  if (*pid == 0) { /* slave process */
    char *s = malloc(cmdlen+1);
    int rc;
    memcpy(s,cmd,cmdlen);
    s[cmdlen] = '\0';
    rc = system(s);
    fflush(stdout);
    fflush(stderr);
    free(s);
    _exit(rc);
    /* end of slave process */
  }
#endif
  else if (*pid < 0) {
    /* parent only process : unable to fcuk */
    fprintf(stderr,
	    "***Error in codb_subshell_('%*.*s'): Unable to fork --> retcode = %d\n",
	    cmdlen,cmdlen,cmd,*pid);
  }
  else {
    /* parent only process : check if exited already ; try to gather the exit-code (usually non-zero) */
#if defined(WIFEXITED) && defined(WEXITSTATUS)
    /* Check if near-immediate abort has occurred */
    int ret, status = 0;
    const int usecs = 200000; /* 0.2sec i.e. 200,000 microsecs nap */
    codb_usleep_(&usecs);
    ret = waitpid(*pid, &status, WNOHANG);
    if (ret == *pid) {
      if (WIFEXITED(status)) {
	ret = WEXITSTATUS(status);
	if (ret != 0) {
	  fprintf(stderr,
		  "***Error in codb_subshell_('%*.*s') : exited nearly immediately --> retcode = %d (status = %d)\n",
		  cmdlen,cmdlen,cmd,ret,status);
	  *pid = -abs(ret);
	}
	else
	  *pid = 0;
      } /* if (WIFEXITED(status)) */
    } /* if (ret == *pid) */
#endif
  }
}

void
codb_system_(const char *s
	     /* Hidden argument */
	     ,int slen) 
{
  char *p = malloc(slen+1);
  if (!p) {
    fprintf(stderr,"codb_system_(): Unable to allocate %d bytes of memory\n", slen+1);
    raise(SIGABRT);
  }
  memcpy(p,s,slen); 
  p[slen]='\0';
#ifdef CRAYXT
  fprintf(stderr,"codb_system_(): Not implemented system(%s)\n",p);
#else
  (void) system(p); 
#endif
  free(p);
}

#if defined(HPPA)
int kill_(int *pid, int *sig) { return kill(*pid,*sig); }
#endif

#if defined(HPPA)
int getpid_() { return getpid(); }
#endif

#if defined(HPPA)
int signal_(int *sig, void (*func)(int), int *mode)
{ 
  return (func != NULL) ? 
    signal(*sig, func) : 
      signal(*sig, (void (*)(int))(*mode));
}
#endif

#if defined(HPPA)
void 
getenv_(const char *s, 
	char *value,
	/* Hidden arguments */
	int slen,
	const int valuelen) 
{
  codb_getenv_(s,value,slen,valuelen);
}
#endif


#if defined(VPP_SYSTEM_call)
int 
system(const char *string)  
{
  extern int system_(const char *, int);
  return system_(string, strlen(string)); /* Use Fortran system_ */
}
#endif

void 
codb_getpid_(pid_t *pid)
{
  if (pid) *pid = getpid();
}

#ifdef RS6K
/* IBM RS/6000 pmapi.h dummies needed in drhook.c (IFSAUX) */
/* function prototypes */

double   pm_cycles(void) { return 0; }
void     pm_error(char *where, int error) 
{ 
  fprintf(stderr,"***Error in dummy pm_error(%s): error=%d\n",where,error);
}
int      pm_init(int filter, void *pminfo, void *pmgroups) { return 0; }
int      pm_initialize(int filter, void *pminfo, void *pmgroups, int flag) { return 0; }

int      pm_set_program_mythread(void *prog) { return 0; }
int      pm_start_mythread(void) { return 0; }
int      pm_stop_mythread(void) { return 0; }
int      pm_get_data_mythread(void *data) { return 0; }

#endif /* RS6K */

#if !defined(HAS_LAPACK)

/* 
   Unless you have LAPACK, use these second() and secondr() functions --
   both meant to be REAL*8 functions in our environment 

   See also util_cputime_() in ifsaux/support/drhook.c
   and ifsaux/support/cptime.F, where the mess starts off ;-)
*/

#include <unistd.h>
#include <sys/types.h>
#include <sys/times.h>
#undef MIN
#undef MAX
#include <sys/param.h>
#include <time.h>

#ifdef CRAYXT
/* Cray XT3/XT4 with catamount microkernel */
#include <catamount/dclock.h>
#endif

double second_()
{
  double res = 0;
#if !defined(CRAYXT)
  extern clock_t times (struct tms *buffer);
  struct tms tbuf;
  static double clock_ticks = 0;
  static int first_time = 1;

  (void) times(&tbuf);

  if (first_time) {
    clock_ticks = (double) sysconf(_SC_CLK_TCK);
    first_time = 0;
  }

  res = (tbuf.tms_utime + tbuf.tms_stime +
	 tbuf.tms_cutime + tbuf.tms_cstime) / clock_ticks;
#else
  res = dclock();
#endif
  return res;
}

double second() { return second_(); }

double secondr_() { return second_(); }

double secondr() { return second_(); }

#endif
