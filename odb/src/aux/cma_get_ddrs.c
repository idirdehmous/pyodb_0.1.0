#include "cmaio.h"

FORTRAN_CALL void
cma_get_ddrs_(const integer4 *unit,
	      real8           ddrs[],
	      const integer4 *lenddrs,
	      integer4       *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);
  /* int bin   = IOBIN(*unit); */

  if (iostuff_debug)  fprintf(stderr,"cma_get_ddrs(%d)",*unit);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.is_inuse && pcmaio->io.read_only) {
      if (!pcmaio->zinfo) {
	/* Perform the very first cma_info()-gathering if not done already */
	/* The following used to be in cma_open() ... */
	/* bin#0: Get info-vector and DDRs for subsequent use; ignore errors */
	integer4 *info = NULL;
	integer4 infolen = 0;
	integer4 retc;
	cma_info_(unit, info, &infolen, &retc);
      }
      else if (pcmaio->io.is_mrfs2disk && pcmaio->ddr_read_counter > 0) {
	/* Get the next chunk of DDRs from the concatenated file */ 

	rc = CMA_fetch_ddrs(unit, pcmaio);
	if (rc < 0) goto finish;
      }
      
      if (iostuff_debug)  
	fprintf(stderr,": ddr_read_counter=%d ",pcmaio->ddr_read_counter);
    
      if (pcmaio->ddrs) {
	int minlen = MIN(*lenddrs, pcmaio->lenddrs);
	IOrcopy(ddrs, pcmaio->ddrs, minlen);
	pcmaio->ddr_read_counter++;
	rc = minlen;
      }
      else
	rc = 0;

      if (iostuff_debug)  
	fprintf(stderr,": now=%d ",pcmaio->ddr_read_counter);

    }
    else {
      /* Error : File was not in use or was not open for read only */
      rc = -4;
    }
  }
  else {
    /* Error : Invalid internal file unit  */
    rc = -2;
  }

 finish:

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
}
