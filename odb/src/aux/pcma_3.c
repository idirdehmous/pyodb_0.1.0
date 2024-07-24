
/* === CMA-packing method#3 === */

#include "pcma.h"

int
pcma_3_driver(int method, /* ignored */
	      FILE *fp_out,
	      const double  cma[],
	               int  lencma,
	      double nmdi,
	      double rmdi,
	           Packbuf *pbuf)
{
  int rc = 0;
  int count, nbytes, chunk, replen;
  int total_count = 0;
  int lenactive;
  unsigned int *packed_data = NULL;
  int hdrlen = PCMA_HDRLEN;
  DRHOOK_START(pcma_3_driver);

  replen = lencma;
  chunk = replen - 1; /* Report w/o length information */

  lenactive = chunk;      /* Active packed uint(s); a first guess */

  do {
    count  = hdrlen;      /* 'PCMA' + method & 3 zero bytes + no_of_packed_bytes + no_of_unpacked + 2 x double MDIs */
    count += lenactive;   /* No. of uint words reserved for packed buffer */
    
    REALLOC(packed_data, count);
      
    packed_data[0] = PCMA;
    packed_data[1] = 3 * MAXSHIFT + 0; /* zero no_of_unpacked means that 4th word, packed_data[3] contains
					  report length; ==> no_of_unpacked can now be over 16megawords */
    packed_data[2] = 0; /* no. of bytes packed; for the moment equal to zero */
    packed_data[3] = replen;
    memcpy(&packed_data[4],&nmdi,sizeof(nmdi));
    memcpy(&packed_data[6],&rmdi,sizeof(rmdi));

    if (replen > 1) {
      int Chunk = chunk * sizeof(double);
      int Lenactive = lenactive * sizeof(*packed_data);

      pcma_zero_uint(&packed_data[hdrlen], lenactive);

      lzw_pack_((const unsigned char *)&cma[1], &Chunk,
		(unsigned char *)&packed_data[hdrlen], &Lenactive,
		&nbytes);
    }
    else {
      nbytes = 0;
    }

    /* If didn't fit, then adjust length "lenactive" */
    if (nbytes < 0) {
      int nb = -nbytes;
      lenactive = RNDUP(nb,b4)/b4;
    }

  } while (nbytes < 0);

  count          = hdrlen + RNDUP(nbytes,b4)/b4;    /* Actual count of uint words */
  packed_data[2] = nbytes; /* True no. of bytes packed */

  if (fp_out) (void) fwrite(packed_data, sizeof(*packed_data), count, fp_out);

  if (pbuf) {
    unsigned int *tmp = pcma_alloc(pbuf, count);
    pcma_copy_uint(tmp, packed_data, count);
  }

  total_count += count;

  rc = total_count *  sizeof(*packed_data);

  /* finish: */
  FREE(packed_data);

  DRHOOK_END(0);
  return rc;
}


/* ======================================================================================== */

int
upcma_3_driver(int method,
	       int swp, int can_swp_data,
	       int new_version,
	       const unsigned int packed_data[],
	       int len_packed_data,
	       int msgbytes,
	       double nmdi,
	       double rmdi,
	       FILE *fp_out, 
	       const int idx[], int idxlen,
	       int fill_zeroth_cma,
	       double cma[], int lencma)
{
  int rc = 0;
  int replen;
  DRHOOK_START(upcma_3_driver);

  replen = lencma;
  if (fp_out) { /* backward compatibility */
    idxlen = 0;
    fill_zeroth_cma = 1;
  }
  if (fill_zeroth_cma) {
    cma[0] = replen;
    fill_zeroth_cma = 1;
  }
  rc = 1;

  if (replen > 1) {
    int chunk = replen - 1;
    int endcma = chunk;
    double *cma_addr = &cma[fill_zeroth_cma];

    chunk = endcma * sizeof(double);
    lzw_unpack_((const unsigned char *)packed_data, &msgbytes,
		(unsigned char *)cma_addr, &chunk,
		&rc);

    if (rc != chunk) {
      /* Error : Not all data was unpacked */
      goto finish;
    }

    rc /= sizeof(double);
    rc = (rc == endcma) ? replen : rc;

    if (swp && can_swp_data) {
      int len = endcma;
      swap8bytes_(cma_addr, &len);
    }
  }

  if (fp_out) (void) fwrite(cma, sizeof(double), lencma, fp_out);

 finish:
  DRHOOK_END(0);
  return rc;
}
