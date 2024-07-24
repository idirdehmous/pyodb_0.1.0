#include "cmaio.h"

FORTRAN_CALL void
cma_bin_info_(const integer4 *unit,
	      integer4       *numbins,
	      integer4       *binfactor)
{
  int index = IOINDEX(*unit);

  if (iostuff_debug) fprintf(stderr,"cma_bin_info(%d)\n", *unit);

  *binfactor = IOBINFACTOR;

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];
    *numbins = pcmaio->io.is_inuse ? pcmaio->io.numbins : 0;
  }
  else {
    *numbins = -1;
  }
}
