#include "privpub.h"
#include "cdrhook.h"

FORTRAN_CALL void 
FUNC_NAME(const integer4 *unit,
	  const integer4 *koffset, /* 1 unit == sizeof(DATA_TYPE) */
	  const integer4 *kwhence, /* 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END */
	  integer4       *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);
  int bin = IOBIN(*unit);
  int sz = 0;
  DRHOOK_START_BY_STRING(FUNC_STR);

  if (iostuff_debug) 
    fprintf(stderr,"%s(%d,off=%d,wh=%d) ", FUNC_STR, *unit, *koffset, *kwhence);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.is_inuse) {
      if ((pcmaio->io.scheme & external) == external) {
	/* Handle special case by reading data in */
	if (*koffset >= 0 && *kwhence == 1 &&
	    bin >= 0 && bin < pcmaio->io.numbins) {
	  IObin *pbin = &pcmaio->io.bin[bin];
	  int buflen = pbin->readbuf.len;
	  int len = (*koffset) * sizeof(DATA_TYPE); 
	  int nread;
	  byte1 *buf = NULL;
	  buflen = MAX(buflen, IO_BUFSIZE_DEFAULT); /* IO_BUFSIZE_DEFAULT from privpub.h */
	  ALLOC(buf, buflen);
	  nread = 0;
	  while (nread < len) {
	    int n = MIN(buflen, len - nread);
	    cma_readb_(unit, buf, &n, &rc);
	    if (rc != n) break; /* Error */
	    nread += n;
	  }
	  sz = nread;
	  if (nread == len) rc = 0; /* All was okay from "simulated" fseek() point of view */
	  else rc = -7;
	  FREE(buf);
	}
	else {
	  /* Error : File was opened via pipe and seek was not from current position & forward */
	  rc = -1;
	}
	goto finish;
      }

      if (bin >= 0 && bin < pcmaio->io.numbins) {
	IObin *pbin = &pcmaio->io.bin[bin];
	FILE *fp = pbin->fp;
	int offset = (*koffset) * sizeof(DATA_TYPE);
	int whence = *kwhence;
	int oldpos = pbin->filepos;
	int newpos;

	/* fseek() */
	if (iostuff_stat) IOtimes(NULL);
	rc = fseek(fp, offset, whence);
	if (iostuff_stat) IOtimes(&pbin->stat.t);
	
	if (iostuff_debug) fprintf(stderr," : fseek(%d:%s)",rc,DATA_STR);

	if (rc < 0 || ferror(fp)) {
	  /* Error while seeking */
	  rc = -6;
	  goto finish;
	}

	newpos = pbin->filepos = ftell(fp);
	sz = ABS(newpos - oldpos);
	
	if (iostuff_stat) {
	  pbin->stat.num_trans++;
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
    /* Error : Invalid internal file unit  */
    rc = -2;
  }

 finish:

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
  DRHOOK_END(sz);
}
