#include "iostuff.h"

#include <sys/types.h>
#include <sys/times.h>
#include <time.h>
#include <sys/time.h>

static real8 
walltime()
{
  static real8 time_init = 0;
  real8 time_in_secs;
  struct timeval tbuf;
  if (gettimeofday(&tbuf,NULL) == -1) PERROR("walltime()");

  if (time_init == 0) time_init = 
    (real8) tbuf.tv_sec + (tbuf.tv_usec / 1000000.0);

  time_in_secs = 
  (real8) tbuf.tv_sec + (tbuf.tv_usec / 1000000.0) - time_init;

  return time_in_secs;
}


#if defined(CRAYXT)
/* Cray XT3/XT4 with catamount microkernel */

static real8 
cputime(real8 *user, real8 *sys)
{
  real8 w = walltime(); /* In absence of anything better */
  if (user) *user = w;
  if (sys)  *sys = 0;
  return w;
}

#else
extern clock_t times (struct tms *buffer);
#define clock_ticks ( (real8) sysconf(_SC_CLK_TCK) )


static real8 
cputime(real8 *user, real8 *sys)
{
  struct tms tbuf;
  (void) times(&tbuf);
  
  if (user) *user = (tbuf.tms_utime + tbuf.tms_cutime) / clock_ticks;
  if (sys)  *sys  = (tbuf.tms_stime + tbuf.tms_cstime) / clock_ticks;
  
  return (tbuf.tms_utime + tbuf.tms_stime +
          tbuf.tms_cutime + tbuf.tms_cstime) / clock_ticks; 
}
#endif

void 
IOtimes(IOtime *p)
{
  static real8 w1=0, u1=0, s1=0;

  if (p) {
    real8 w2, u2, s2;

    cputime(&u2, &s2);
    w2 = walltime();

    p->usercpu  += (u2 - u1);
    p->syscpu   += (s2 - s1);
    p->walltime += (w2 - w1);
  }
  else {
    cputime(&u1, &s1);
    w1 = walltime();
  }
}



