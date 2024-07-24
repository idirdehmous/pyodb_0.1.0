#include "cmaio.h"
#include "cdrhook.h"

FORTRAN_CALL void 
cma_rewind_(integer4 *unit,
	    integer4 *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);
  /* int bin = IOBIN(*unit); */ /* Ignored, since ALL will be rewound */
  DRHOOK_START(cma_rewind_);

  if (iostuff_debug) fprintf(stderr,"cma_rewind(%d) ",*unit);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.is_inuse) {
      int j, numbins = pcmaio->io.numbins;

      for (j=0; j<numbins; j++) {
	IObin *pbin = &pcmaio->io.bin[j];
	
	if ( ((pcmaio->io.scheme & none) == none) || 
	     ((pcmaio->io.scheme & internal) == internal) ) {
	  FILE *fp = pbin->fp;
	  
	  /* Safe to seek at the beginning since regular, non-piped I/O */
	  rewind(fp);
	  
	  pbin->filepos = 0;
	  rc = 0;
	}
	else {
	  integer4 retc;
	  char *file = IOstrdup(pcmaio->io.logical_name, NULL);
	  char mode = pcmaio->io.read_only ? 'r' : 'w';
	  int swp = pcmaio->io.req_byteswap;
	  
#if 0
	  fprintf(stderr,"< cma_rewind(): pcmaio->io.req_byteswap = %d [before cma_close_]\n",
		  pcmaio->io.req_byteswap);
#endif

	  cma_close_(unit, &retc);
	  rc = retc;
	  
	  if (rc == 0) {
	    cma_open_(unit, file, &mode, &retc, strlen(file), 1);
	    rc = retc;
	    if (rc == 0) {
	      FREE(file);
	      break; /* for (j=0; j<numbins; j++) */
	    }
	  }
	  FREE(file);
	  pcmaio->io.req_byteswap = swp; /* Restored */
#if 0
	  fprintf(stderr,"> cma_rewind(): pcmaio->io.req_byteswap = %d\n",
		  pcmaio->io.req_byteswap);
#endif
	}
      } /* for (j=0; j<numbins; j++) */
    }
    else {   
      /* Error : File was not in use */
      rc = -4;
    }
  }
  else {
    /* Error : Invalid internal file unit  */
    rc = -2;
  }

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
  DRHOOK_END(0);
}
