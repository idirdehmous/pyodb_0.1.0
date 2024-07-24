#include "cmaio.h"

FORTRAN_CALL void
cma_get_byteswap_(const integer4 *unit, integer4 *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];
    if (pcmaio->io.is_inuse) {
      rc = pcmaio->io.req_byteswap;
    }
  }

  *retcode = rc;
}

FORTRAN_CALL void
cma_set_byteswap_(const integer4 *unit, 
		  const integer4 *toggle,
		  integer4 *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];
    if (pcmaio->io.is_inuse) {
      pcmaio->io.req_byteswap = *toggle;
    }
  }

  *retcode = rc;
}
