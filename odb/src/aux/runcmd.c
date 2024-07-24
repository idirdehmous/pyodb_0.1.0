
/* Author: Sami Saarinen, ECMWF, 26-Feb-2002 */

#include "odb.h"

PUBLIC void
runcmd_(int *myproc,
	int *nproc,
	const char *cmd,
	int *retcode
	/* Hidden arguments */
	,int  cmd_len)
{
  int Myproc = *myproc; /* Assumed to be numbered from 1 to Nproc, inclusive */
  int Nproc = *nproc;
  char *env;
  int ignore_errors = 0;
  int myproc_offset = 0;
  int last_myproc = Nproc;
  int verbose = 0;
  int i, rc = 0;
  int old;
  int dcount = 0; /* count of %d's or -- say -- count of %5.5d's */
  char *c;
  DECL_FTN_CHAR(cmd);

  old = ODB_std_mem_alloc(1); /* Always enforce standard memory alloc
				 since we intend *not* to rely on libodb.a's memory alloc */

  ALLOC_FASTFTN_CHAR(cmd);

  env = getenv("MPI_SCHEDULER_IGNORE_ERRORS");
  if (env) ignore_errors = atoi(env);
  env = getenv("MPI_SCHEDULER_MYPROC_OFFSET");
  if (env) myproc_offset = atoi(env);
  env = getenv("MPI_SCHEDULER_LAST_MYPROC");
  if (env) last_myproc = atoi(env);
  env = getenv("MPI_SCHEDULER_VERBOSE");
  if (env) verbose = atoi(env);

  i = Myproc + myproc_offset;
  if (i > last_myproc) goto finish;

  c = p_cmd;
  while (*c) {
    if (*c++ == '%') dcount++;
  }

  ALLOC(c, strlen(p_cmd) + 20 * dcount + 1);
  sprintf(c,p_cmd,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,
	          i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i); /* Enough i's ?? ; must be >= dcount */

  if (verbose) {
    printf("(PE#%d): Executing '%s'\n", Myproc, c);
    fflush(stdout);
  }

  rc = system(c);

  if (ignore_errors) rc = 0;

  if (rc != 0) {
    printf("***Errors found on PE#%d : rc=%d\n", Myproc, rc);
    if (!verbose) printf("Command was on PE#%d : '%s'\n",Myproc,c);
    fflush(stdout);
  }
  FREE(c);

 finish:

  FREE_FASTFTN_CHAR(cmd);

  (void) ODB_std_mem_alloc(old); /* Restore old settings */
  *retcode = rc;
}
