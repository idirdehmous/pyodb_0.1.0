#include "cmaio.h"

FORTRAN_CALL void 
cma_detach_(const integer4 *unit, integer4 *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);

  if (iostuff_debug) fprintf(stderr,"cma_detach(%d) ",index);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.is_inuse) {
      if (!pcmaio->io.read_only) {
	/* If WRITE-ONLY -file, then flush the current contents of the I/O-buf */
	int numbins = pcmaio->io.numbins;
	int j;

	for (j=0; j<numbins; j++) {
	  IObin *pbin = &pcmaio->io.bin[j];
	  FILE  *fp   =  pbin->fp;

	  /* fflush() */
	  if (iostuff_stat) IOtimes(NULL);
	  (void)fflush(fp);
	  if (iostuff_stat) {
	    IOtimes(&pbin->stat.t);
	    pbin->stat.num_trans++;
	  }

	} /* for (j=0; j<numbins; j++) */
      }

      pcmaio->io.is_detached = true;

      if (iostuff_debug)
	fprintf(stderr," file='%s', alias='%s%s', numbins=%d\n",
		pcmaio->io.logical_name, 
		pcmaio->io.leading_part,
		(pcmaio->io.numbins == 1) ? "\0" : ",*",
		pcmaio->io.numbins);
      rc = 0;
    }
    else {
      /* Error : File not in use i.e. reference not found */
      rc = -1;
    }
  }
  else {
    /* Error: Invalid internal file unit */
    rc = -2;
  }

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
}
