#include "cmaio.h"

void 
cma_on_mrfs_(const integer4 *unit,
	     integer4 *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    rc = pcmaio->io.on_mrfs;
  }
  else {
    /* Error : Invalid internal file unit  */
    rc = -2;
  }

  *retcode = rc;
}

