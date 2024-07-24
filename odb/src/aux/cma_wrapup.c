#include "cmaio.h"

/* Close any files that have been opened via CMA-lib */

FORTRAN_CALL void
cma_wrapup_(integer4  *retcode)
{
  int rc = 0;
  int i, maxcmaio = CMA_get_MAXCMAIO();
  int tid = get_thread_id_();

  if (iostuff_debug) fprintf(stderr,"cma_wrapup\n");

  for (i=0; i<maxcmaio; i++) {
    IOcma *pcmaio = &cmaio[tid][i];
    if (pcmaio->io.is_inuse) {
      integer4 unit = i;
      integer4 retc;
      cma_close_(&unit, &retc);
      rc++;
    }
  }

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
}
