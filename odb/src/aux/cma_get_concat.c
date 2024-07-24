#include "cmaio.h"

void 
cma_get_concat_(const integer4 *unit,
		integer4 *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.read_only) {
      rc = pcmaio->io.is_mrfs2disk ? pcmaio->io.concat : 0;
    }
    else 
      rc = pcmaio->io.concat;
  }
  else {
    /* Error : Invalid internal file unit  */
    rc = -2;
  }

  *retcode = rc;
}

		    
