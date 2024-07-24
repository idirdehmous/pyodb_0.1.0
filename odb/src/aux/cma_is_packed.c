#include "cmaio.h"

void 
cma_is_packed_(const integer4 *unit,
	       integer4 *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    rc = pcmaio->io.packmethod;
  }
  else {
    /* Error : Invalid internal file unit  */
    rc = -2;
  }

  *retcode = rc;
}

		    
