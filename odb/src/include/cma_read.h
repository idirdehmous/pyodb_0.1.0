#if TYPE_ID == real_8
#include "pcma_extern.h"
#endif

#include "swapbytes.h"
#include "cdrhook.h"

FORTRAN_CALL void 
FUNC_NAME(const integer4 *unit,
	  DATA_TYPE       pbuf[],
	  const integer4 *kbufsize,
	  integer4       *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);
  int bin = IOBIN(*unit);
  int sz = 0;
  DRHOOK_START_BY_STRING(FUNC_STR);

  if (iostuff_debug) 
    fprintf(stderr,"%s(%d,w=%d) ", FUNC_STR, *unit, *kbufsize);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.is_inuse) {
      if (!pcmaio->io.read_only) {
	/* Error : File was open for write only */
	rc = -3;
	goto finish;
      }

      if (bin >= 0 && bin < pcmaio->io.numbins) {
	IObin *pbin = &pcmaio->io.bin[bin];
	FILE *fp = pbin->fp;
	int bufsize = *kbufsize;

	/* fread() */
	if (iostuff_stat) IOtimes(NULL);
#if TYPE_ID == real_8
	if ((pcmaio->io.scheme & internal) == internal) {
	  int can_swp_data = pcmaio->io.req_byteswap; /* in absence any better information ;-( */

	  if (bufsize == 1 && pcmaio->chunk == 0) {
	    /* A frequently used special case where only 
	       the report length is requested */
	    int method, chunk, msgbytes;
	    double nmdi, rmdi;
	    int new_version = 0;
	    int swp = 0;

	    rc = upcma_hdr(fp, &swp,
			   pcmaio->hdr, 1,
			   &method, &chunk, &msgbytes,
			   &nmdi, &rmdi,
			   &new_version);

	    if ((!new_version && rc == HDRLEN) ||
		( new_version && rc == PCMA_HDRLEN)) {
	      pcmaio->io.packmethod = method;
	      pcmaio->chunk = chunk;
	      pbuf[0] = chunk;
	      rc = 1;
	    }
	  }
	  else if (pcmaio->chunk > 0) {
	    int bytes_in, bytes_out;
	    int chunk = pcmaio->chunk;
	    real8 *tmpbuf = NULL;

	    ALLOC(tmpbuf, chunk);
	    cma_set_lock(1);
	    rc = upcma(can_swp_data, fp, NULL,
		       NULL, 0, 1,
		       tmpbuf, chunk,
		       pcmaio->hdr,
		       NULL,
		       &bytes_in, &bytes_out);
	    cma_unset_lock(1);
	    sz += bytes_in;
	    if (rc >= 0) {
	      rc = MIN(chunk - 1, bufsize);
	      IOrcopy(pbuf, &tmpbuf[1], rc);
	    }
    	    FREE(tmpbuf);

	    pcmaio->chunk = 0;
	  }
	  else {
	    int bytes_in, bytes_out;
	    cma_set_lock(1);
	    rc = upcma(can_swp_data, fp, NULL,
		       NULL, 0, 1,
		       pbuf, bufsize,
		       NULL,
		       NULL,
		       &bytes_in, &bytes_out);
	    cma_unset_lock(1);
	    sz += bytes_in;
	    if (rc >= 0) {
	      rc = bytes_out/sizeof(DATA_TYPE);
	    }
	  }
	}
	else {
	  rc = fread(pbuf, sizeof(DATA_TYPE), bufsize, fp);
	  if (rc > 0 && pcmaio->io.req_byteswap) swap8bytes_(pbuf, &rc); /* double */
	  if (rc > 0) sz += rc * sizeof(DATA_TYPE);
	}
#else
	rc = fread(pbuf, sizeof(DATA_TYPE), bufsize, fp);
	if (rc > 0) sz += rc * sizeof(DATA_TYPE);
#endif
#if (TYPE_ID == integer_4) || (TYPE_ID == real_4)
	if (rc > 0 && pcmaio->io.req_byteswap) swap4bytes_(pbuf, &rc); /* int */
#endif
	if (iostuff_stat) IOtimes(&pbin->stat.t);
	
	if (iostuff_debug) fprintf(stderr," : fread(%d:%s)",rc,DATA_STR);
	
	if (rc == 0 && feof(fp)) {
	  /* EOF */
	  rc = -1;
	  goto finish;
	}

	if (rc < 0 || ferror(fp)) {
	  /* Error while reading data */
	  rc = -6;
	  goto finish;
	}

	pbin->filepos += rc * sizeof(DATA_TYPE);      
	
	if (iostuff_stat) {
	  pbin->stat.bytes += rc * sizeof(DATA_TYPE);
	  pbin->stat.num_trans++;
	}
#if TYPE_ID == real_8
	{
	  static volatile Boolean check_it = true;
	  cma_set_lock(1);
	  if (check_it) {
	    Boolean tsek = true;
	    char *filename = pbin->true_name;
	    CMA_flpchecker(FUNC_STR, 1, filename,
			   pbuf, pbin->filepos, &rc,
			   &tsek);
	    check_it = tsek;
	  }
	  cma_unset_lock(1);
	}
#endif
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
