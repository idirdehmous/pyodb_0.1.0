#if TYPE_ID == real_8
#include "pcma_extern.h"
#endif

#include "cdrhook.h"

FORTRAN_CALL void 
FUNC_NAME(const integer4 *unit,
	  const DATA_TYPE pbuf[],
	  const integer4 *kbufsize,
	  integer4       *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);
  int bin = IOBIN(*unit);
  int sz = 0;
  DRHOOK_START_BY_STRING(FUNC_STR);

  if (iostuff_debug) fprintf(stderr,"%s(%d,w=%d) ", FUNC_STR, *unit, *kbufsize);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.is_inuse) {
      if (pcmaio->io.read_only) {
	/* Error : File was open for read only */
	rc = -3;
	goto finish;
      }

      if (bin >= 0 && bin < pcmaio->io.numbins) {
	IObin *pbin = &pcmaio->io.bin[bin];
	FILE *fp = pbin->fp;
	int bufsize = *kbufsize;
	
	/* fwrite() */
	if (iostuff_stat) IOtimes(NULL);
#if TYPE_ID == real_8
	{
	  static volatile Boolean check_it = true;
	  cma_set_lock(2);
	  if (check_it) {
	    Boolean tsek = true;
	    char *filename = pbin->true_name;
	    rc = bufsize;
	    CMA_flpchecker(FUNC_STR, 2, filename,
			   pbuf, pbin->filepos, &rc,
			   &tsek);
	    check_it = tsek;
	    if (rc == -7) {
	      /* Error : Invalid flp values were detected */
	      goto finish;
	    }
	  }
	  cma_unset_lock(2);
	}

	if ((pcmaio->io.scheme & internal) == internal) {
	  int bytes_in, bytes_out;

	  cma_set_lock(2);
	  rc = pcma(NULL, fp,
		    pcmaio->io.packmethod,
		    pbuf, bufsize,
		    NULL,
		    &bytes_in, &bytes_out);	
	  cma_unset_lock(2);

	  sz += bytes_out;
	  if (rc >= 0) {
	    rc = bytes_in/sizeof(DATA_TYPE);
	  }
	}
	else {
	  rc = fwrite(pbuf, sizeof(DATA_TYPE), bufsize, fp);
	  if (rc > 0) sz += rc * sizeof(DATA_TYPE);
	}
#else
	rc = fwrite(pbuf, sizeof(DATA_TYPE), bufsize, fp);
	if (rc > 0) sz += rc * sizeof(DATA_TYPE);
#endif
	if (iostuff_stat) IOtimes(&pbin->stat.t);
	
	if (iostuff_debug) fprintf(stderr," : fwrite(%d:%s)",rc,DATA_STR);
	
	pbin->filepos += rc * sizeof(DATA_TYPE);
	
	if (iostuff_stat) {
	  pbin->stat.bytes += rc * sizeof(DATA_TYPE);
	  pbin->stat.num_trans++;
	}
	
	if (rc != bufsize) {
	  /* Error while writing data */
	  rc = -1;
	}
      }  
      else {
	/* Error : Attempt to use an invalid bin */
	rc = -5;
      }
     }
    else {
      /* Error : File was not in use */
      rc = -4;
    }
  }
  else {
    /* Error : Invalid internal file unit */
    rc = -2;
  }

 finish:

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
  DRHOOK_END(sz);
}
