#include "cmaio.h"

void 
cma_bin_file_(const integer4 *unit,
	      char *filename,
	      integer4 *retcode,
	      int len_filename)
{
  int rc = 0;
  int index = IOINDEX(*unit);
  int bin   = IOBIN(*unit);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (bin >= 0 && bin < pcmaio->io.numbins) {
      IObin *pbin = &pcmaio->io.bin[bin];
      int len = MIN(len_filename, strlen(pbin->true_name));
      int i;

      strncpy(filename, pbin->true_name, len);
      for (i=len; i<len_filename; i++) 
	filename[i] = ' ';

      rc = len;
    }
    else {
      /* Error : Attempt to use an invalid bin */
      rc = -5;
    }
  }
  else {
    /* Error : Invalid internal file unit  */
    rc = -2;
  }

  *retcode = rc;
}
