
/* === CMA-packing methods#31 to 39 === */

#include "pcma.h"
#include "bits.h"

#include <signal.h>
#include <math.h>

#ifdef VPP
#pragma global noalias
#pragma global novrec
#elif defined(NECSX)
#pragma cdir options -pvctl,nodep
#endif

#define MAXINT   2147483647
#define MININT  (-MAXINT-1)
#define TRUNC(x) ((int)(x))

#define IS_INT32(x) ((x) <= MAXINT && (x) >= MININT && (x) == TRUNC((x)))

PRIVATE int
pcma_31to39_prepare(const double d[], int n, int flag[],
		    double auxint[], double auxdbl[])
{
  int j, kr = 0, ki = 0;
  for (j=0; j<n; j++) {
    double x = d[j];
    if (IS_INT32(x)) {
      auxint[ki++] = TRUNC(x);
      flag[j] = 1;
    }
    else {
      auxdbl[kr++] = x;
      flag[j] = 0;
    }
  } /* for (j=0; j<n; j++) */
  return ki;
}

int
pcma_31to39_driver(int method,
		   FILE *fp_out,
		   const double  cma[],
		   int  lencma,
		   double nmdi,
		   double rmdi,
		   Packbuf *pbuf)
{
  int rc = 0;
  int replen, chunk;
  int count, nw, nwrt;
  int total_count = 0;
  unsigned int *packed_data = NULL;
  unsigned int *bitmap = NULL;
  int hdrlen = PCMA_HDRLEN;
  int prec = method - 30;
  int lenextra, lenbitmap, lenint, lendbl;
  int intbytes, dblbytes;
  int num_ints = 0;
  int num_dbls = 0;
  Packbuf intbuf, dblbuf;
  DRHOOK_START(pcma_31to39_driver);

  if (prec < 1) {
    prec = 1;
  }
  else if (prec > 9) {
    prec = 9;
  }

  nwrt = 0;

  replen = lencma;
  chunk = replen-1;  /* Report w/o length information */

  count = hdrlen;   /* 'PCMA' + method & 3 zero bytes + no_of_packed_bytes + 
		       no_of_unpacked + 2 x double MDIs */

#ifdef DEBUG
  fprintf(stderr,
	  "pcma_31to39_driver(method=%d, prec=%d): hdrlen=%d, replen=%d\n",
	  method, prec, hdrlen, replen);
#endif

  if (replen > 1) { 
    double *auxint, *auxdbl;
    int *flag;

    ALLOC(flag, chunk);
    ALLOC(auxint, replen);
    ALLOC(auxdbl, replen);

#ifdef DEBUG
    {
	int j;
	fprintf(stderr,"Numbers to pack (replen = %d, chunk= %d)\n",replen,chunk);
	for (j=0; j<replen; j++) {
	    fprintf(stderr," %.12g", cma[j]);
	    if ((j+1)%10 == 0 || j == replen-1) fprintf(stderr,"\n");
	}
    }
#endif

    num_ints = pcma_31to39_prepare(&cma[1], chunk, flag,
				   &auxint[1], &auxdbl[1]);
    num_dbls = chunk - num_ints;

#ifdef DEBUG
    fprintf(stderr,"# of ints = %d, doubles = %d\n", num_ints, num_dbls);
#endif
    
    auxint[0] = 1 + num_ints;
    auxdbl[0] = 1 + num_dbls;

    if (num_ints > 0) {
      intbuf.counter = 0;
      intbuf.maxalloc = 0;
      intbuf.len = 0;
      intbuf.p = NULL;
      intbuf.allocatable = 1;
      intbytes = pcma_1_driver(1, NULL, auxint, 1 + num_ints, nmdi, rmdi, &intbuf);
      lenint = intbuf.len - hdrlen; /* Header is excluded */
      FREE(auxint);
      if (intbuf.len < 0) {
	rc = -1; /* Unable to pack integer part with method#1 */
	goto finish;
      }
    }
    else {
      intbytes = 0;
      lenint = 0;
      FREE(auxint);
    }

    if (num_dbls > 0) {
      dblbuf.counter = 0;
      dblbuf.maxalloc = 0;
      dblbuf.len = 0;
      dblbuf.p = NULL;
      dblbuf.allocatable = 1;
      dblbytes = pcma_21to29_driver(20+prec, NULL, auxdbl, 1 + num_dbls, nmdi, rmdi, &dblbuf);
      lendbl = dblbuf.len - hdrlen; /* Header is excluded */
      FREE(auxdbl);
      if (dblbuf.len < 0) {
	rc = -2; /* Unable to pack double part with method#21..29 */
	goto finish;
      }
    }
    else {
      dblbytes = 0;
      lendbl = 0;
      FREE(auxdbl);
    }

    if (num_ints > 0 && num_dbls > 0) { 
      /* Note: An optimization : The bitmap is stored only if there exists BOTH ints & doubles !! */
      int j;
      lenbitmap = RNDUP(chunk,N32BITS)/N32BITS;
      CALLOC(bitmap, lenbitmap);
      for (j=0; j<chunk; j++) { /* More "modern" way */
	if (flag[j]) ODBIT_set(bitmap, chunk, N32BITS, j, j); 
      }

#ifdef DEBUG
      fprintf(stderr,"flags=");
      for (j=0; j<chunk; j++) {
	  fprintf(stderr," %d",flag[j]);
      }
      fprintf(stderr,"\n");
#endif

    }
    else {
      lenbitmap = 0;
      bitmap = NULL;
    }
    FREE(flag);
    
    lenextra = 1 + 1 + 1; /* For lenint, lendbl, num_ints */
  }
  else {
    lenextra = lenbitmap = lenint = lendbl = 0;
    bitmap = NULL;
  }

#ifdef DEBUG
  fprintf(stderr,
	  "lenextra = %d, lenbitmap = %d, lenint = %d, lendbl = %d, num_ints = %d, num_dbls = %d\n",
	  lenextra, lenbitmap, lenint, lendbl, num_ints, num_dbls);
#endif

  count += (lenextra + lenbitmap + lenint + lendbl);
    
  packed_data = pcma_alloc(pbuf, count);
  
  packed_data[0] = PCMA;
  packed_data[1] = (30 + prec) * MAXSHIFT + 0; /* zero no_of_unpacked means that 4th word, 
						  packed_data[3] contains report length; 
						  ==> no_of_unpacked can now be over 16megawords */
  packed_data[2] = 
    (lenextra + lenbitmap + lenint + lendbl)
      * sizeof(*packed_data); /* packed_data length in bytes */
  packed_data[3] = replen;
  memcpy(&packed_data[4],&nmdi,sizeof(nmdi));
  memcpy(&packed_data[6],&rmdi,sizeof(rmdi));

  if (replen > 1) {
    unsigned int *extra    = &packed_data[hdrlen];
    unsigned int *bitdata  = (lenbitmap > 0) ? &packed_data[hdrlen + lenextra] : NULL;
    unsigned int *intdata  = (lenint    > 0) ? &packed_data[hdrlen + lenextra + lenbitmap] : NULL;
    unsigned int *dbldata  = (lendbl    > 0) ? &packed_data[hdrlen + lenextra + lenbitmap + lenint] : NULL;

    /* Note: No need to store lenbitmap, since it can be figured out "chunk" upon unpacking */
    /*       We also do not need to stored num_dbls, since it is "chunk - num_ints" by definition */

    extra[0] = lenint;
    extra[1] = lendbl;
    extra[2] = num_ints;

    /* Bitmap */
    if (lenbitmap > 0) {
      pcma_copy_uint(bitdata, bitmap, lenbitmap);
      FREE(bitmap);
    }

    /* Packed integer part */
    if (lenint > 0) {
      pcma_copy_uint(intdata, &intbuf.p[hdrlen], lenint);
      FREE(intbuf.p);
    }

    /* Packed double part */
    if (lendbl  > 0) {
      pcma_copy_uint(dbldata, &dblbuf.p[hdrlen], lendbl);
      FREE(dblbuf.p);
    }
  }

  if (fp_out) nwrt = fwrite(packed_data, sizeof(*packed_data), count, fp_out);

  total_count += count;

  rc = total_count *  sizeof(*packed_data);

 finish:
  if (!pbuf) FREE(packed_data);

  DRHOOK_END(0);
  return rc;
}


/* ======================================================================================== */

int
upcma_31to39_driver(int method,
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
  int nwrt = 0;
  int replen;
  DRHOOK_START(upcma_31to39_driver);

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
    int can_swp_data_here = 1;
    int chunk = replen - 1;
    int stacma = 0;
    int endcma = chunk;
    int j;
    int lenextra  = 1 + 1 + 1;
    const unsigned int *extra = &packed_data[0];
    int lenbitmap = 0;
    int lenint = extra[0];
    int lendbl = extra[1];
    int num_ints = extra[2];
    int num_dbls = 0;
    int hdrlen = new_version ? PCMA_HDRLEN : HDRLEN;
    double *auxint = NULL;
    double *auxdbl = NULL;
    int auxint_allocated = 0;
    int auxdbl_allocated = 0;
    int *flag = NULL;

    if (swp) {
      const int one = 1;
      swap4bytes_(&lenint, &one);
      swap4bytes_(&lendbl, &one);
      swap4bytes_(&num_ints, &one);
    }
    num_dbls = chunk - num_ints;

    if (num_ints > 0 && num_dbls > 0) {
      lenbitmap = RNDUP(chunk,N32BITS)/N32BITS;
    }
    else { /* Otherwise no bitmap stored !! */
      lenbitmap = 0;
    }

#ifdef DEBUG
    fprintf(stderr,
	    "upcma_31to39_driver(method=%d): lenbitmap=%d lenint=%d, lendbl=%d, "
	    "len_packed_data=%d, hdrlen=%d, msgbytes=%d, v=%d, num_ints=%d, num_dbls=%d\n",
	    method, lenbitmap, lenint, lendbl, 
	    len_packed_data, hdrlen, msgbytes, new_version,
	    num_ints, num_dbls);
#endif

    /* STAEND_FIX(); not applicable */

    if (lenbitmap > 0) { /* bitmap part */
      const unsigned int *bitmap = &packed_data[lenextra];
      if (swp) { /* Enforce to swap bytes */
	swap4bytes_((unsigned int *)bitmap, &lenbitmap);
      }
      CALLOC(flag, chunk);
      for (j=0; j<chunk; j++) {
	flag[j] = ODBIT_get(bitmap, chunk, N32BITS, j);
      }
      if (swp) { /* Restore : Swap bytes back */
	swap4bytes_((unsigned int *)bitmap, &lenbitmap);
      }
    }
    else if (!fill_zeroth_cma) { /* A rare possibility ... */
      if (num_ints > 0 && num_dbls == 0) {
	ALLOC(flag, chunk);
	for (j=0; j<chunk; j++) flag[j] = 1;
      }
      else if (num_dbls > 0 && num_ints == 0) {
	CALLOC(flag, chunk); /* All flag[]'s now zero */
      }
    }

    if (lenint > 0) { /* Integer part */
      const unsigned int *int_packed;
      int rc1;

      if (flag) {
	ALLOC(auxint, 1 + num_ints);
	auxint_allocated = 1;
      }
      else { /* ALL ints --> a direct copy to cma[] */
	auxint = cma;
	auxint_allocated = 0;
      }

      int_packed = &packed_data[lenextra + lenbitmap];
      rc1 = upcma_1_driver(method,
			   swp, can_swp_data_here,
			   new_version,
			   int_packed,
			   lenint,
			   (lenint - hdrlen) * sizeof(*int_packed),
			   nmdi,
			   rmdi,
			   NULL, 
			   NULL, 0,
			   1,
			   auxint, 1 + num_ints);
      
      if (rc1 != 1 + num_ints) {
#ifdef DEBUG
	fprintf(stderr,"(rc1 != 1 + num_ints) : rc1=%d, 1 + num_ints = %d\n",rc1,1 + num_ints);
#endif
	rc = -1;
	goto finish;
      }
      rc += rc1 - 1;
    }

    if (lendbl > 0) { /* Double part (with truncated precision, that is) */
      const unsigned int *dbl_packed;
      int rc2;

      if (flag) {
	ALLOC(auxdbl, 1 + num_dbls);
	auxdbl_allocated = 1;
      }
      else { /* ALL dbls --> a direct copy to cma[] */
	auxdbl = cma;
	auxdbl_allocated = 0;
      }

      dbl_packed = &packed_data[lenextra + lenbitmap + lenint];
      rc2 = upcma_21to29_driver(method,
				swp, can_swp_data_here,
				new_version,
				dbl_packed,
				lendbl,
				(lendbl - hdrlen) * sizeof(*dbl_packed),
				nmdi,
				rmdi,
				NULL, 
				NULL, 0,
				1,
				auxdbl, 1 + num_dbls);

      if (rc2 != 1 + num_dbls) {
#ifdef DEBUG
	fprintf(stderr,"(rc2 != 1 + num_dbls) : rc2=%d, 1 + num_dbls = %d\n",rc2,1 + num_dbls);
#endif
	rc = -2;
	goto finish;
      }
      rc += rc2 - 1;
    }

    /* Finally ... */

    if (flag) {
      int ki = 1;
      int kr = 1;
      int k = 1;
      if (!fill_zeroth_cma) --k;
      if (auxint && auxdbl) {
	for (j=0; j<chunk; j++) {
	  if (flag[j]) { /* IS_INT32 */
	    cma[k] = auxint[ki++];
	  }
	  else {
	    cma[k] = auxdbl[kr++];
	  }
	  k++;
	}
      }
      else if (auxint && !auxdbl) {
	for (j=0; j<chunk; j++) cma[k++] = auxint[ki++];
      }
      else if (!auxint && auxdbl) {
	for (j=0; j<chunk; j++) cma[k++] = auxdbl[kr++];
      }
    }

  finish:
    FREE(flag);
    if (auxint_allocated) FREE(auxint);
    if (auxdbl_allocated) FREE(auxdbl);

    rc = (rc == replen) ? replen : rc;
  }

  if (fp_out) nwrt = fwrite(cma, sizeof(double), lencma, fp_out);

  DRHOOK_END(0);
  return rc;
}
