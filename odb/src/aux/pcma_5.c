
/* === CMA-packing method#5 === */

#include "pcma.h"

#include <signal.h>
#include <math.h>

#ifdef VPP
#pragma global noalias
#elif defined(NECSX)
#pragma cdir options -pvctl,nodep
#endif

#ifdef VPP
/* Alters vectorization pattern to prevent flp exception in certain loops */
PRIVATE int kdummy = 0; 

#endif

#define LOW    1U
#define HIGH 255U

/* #define IS_DBL_IN_RANGE(d,low,high) (((d) >= (low)) && ((d) <= (high)) && ((d) == (int)(d))) */
PRIVATE double one = 1;
#define IS_DBL_IN_RANGE(d,low,high) (((d) >= (low)) && ((d) <= (high)) && (fmod(d,one) == 0))

#define DEFMASK(x) unsigned int mask##x = ((IS_DBL_IN_RANGE(cma[i+(x)],LOW,HIGH)) << (x))


/* pcma_5_dbls: Count no. of words that are *not* between [1..255] and creates auxcma[] */

PRIVATE int
pcma_5_dbls(const double cma[],    /* double CMA[] */
                     int lencma,   /* length of the double CMA[] */
	          double auxcma[]
	     )
{
  int rc = 0;
  int i;
  int count = 0;

  for (i=0; i<lencma; i++) {
    if (!IS_DBL_IN_RANGE(cma[i], LOW, HIGH)) {
      auxcma[count] = cma[i];
      count++;
    }
  }

  rc = count;

  /* finish: */
  return rc;
}

PRIVATE int
pcma_5(const double cma[], 
                int lencma,
       unsigned int bitmap[], 
       unsigned int small_data[], 
                int len_tmpbuf)
{
  int rc = 0;
  int i, j, k, n;
  int istart, iend;
  unsigned int *tmpbuf = NULL;

  istart = 0;
  iend   = lencma - lencma%b32;

  for (i=istart; i<iend; i+=b32) {
    int offset = i/b32;

    DEFMASK(0) ; DEFMASK(1) ; DEFMASK(2) ; DEFMASK(3) ;
    DEFMASK(4) ; DEFMASK(5) ; DEFMASK(6) ; DEFMASK(7) ;
    DEFMASK(8) ; DEFMASK(9) ; DEFMASK(10); DEFMASK(11);
    DEFMASK(12); DEFMASK(13); DEFMASK(14); DEFMASK(15);
    DEFMASK(16); DEFMASK(17); DEFMASK(18); DEFMASK(19);
    DEFMASK(20); DEFMASK(21); DEFMASK(22); DEFMASK(23);
    DEFMASK(24); DEFMASK(25); DEFMASK(26); DEFMASK(27);
    DEFMASK(28); DEFMASK(29); DEFMASK(30); DEFMASK(31);

    bitmap[offset] =   
      mask0  +  mask1 +  mask2 +  mask3 + 
      mask4  +  mask5 +  mask6 +  mask7 +
      mask8  +  mask9 + mask10 + mask11 +
      mask12 + mask13 + mask14 + mask15 +
      mask16 + mask17 + mask18 + mask19 +
      mask20 + mask21 + mask22 + mask23 +
      mask24 + mask25 + mask26 + mask27 +
      mask28 + mask29 + mask30 + mask31;
  }

  if (iend < lencma) {
    int offset = lencma/b32;
    unsigned int sum = 0;

    for (i=iend; i<lencma; i++) {
      int shift = (i - iend);
      unsigned int mask = (IS_DBL_IN_RANGE(cma[i], LOW, HIGH));
      /* sum += (mask << shift); */
      sum += mask * PWR2(shift);
    }

    bitmap[offset] = sum;
  }

  /* Copy small data to tmpbuf[] */

  ALLOC(tmpbuf, len_tmpbuf);

#ifdef VPP
  kdummy = 0;
#endif

  k = 0;
  for (i=0; i<lencma; i++) {
    int offset = i/b32;
    int shift  = i%N32BITS;
    unsigned int bitset = ((bitmap[offset] & PWR2(shift)) / PWR2(shift));

    if (bitset) {
      tmpbuf[k] = cma[i];
      k++;
    }
#ifdef VPP
    else {
      /* A real trick: Prevents flp exception by disabling
	 any attempts to copy too large doubles to the VECTOR REGISTER
	 as unsigned ints */
      kdummy++;
    }
#endif
  }

  for (i=k; i<len_tmpbuf; i++)
    tmpbuf[i] = 0;

  /* Fill in with small data */

  k = 0;
  n = len_tmpbuf/b4;

  for (j=0; j<n; j++) {
    small_data[j] = 
      (((tmpbuf[k]*256U + tmpbuf[k+1])*256U + tmpbuf[k+2])*256U + tmpbuf[k+3]);
    k += 4;
  }

  FREE(tmpbuf);

  rc = lencma;

  return rc;
}


int
pcma_5_driver(int method, /* ignored */
	      FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	      double nmdi,
	      double rmdi,
	           Packbuf *pbuf)
{
  int rc = 0;
  int count, nw, nwrt, chunk, replen;
  int total_count = 0;
  int lenbitmap, lenactive;
  int numsmall, numsmall_packed;
  unsigned int *packed_data = NULL;
  Packbuf  auxpbuf;
  Packbuf *pauxpbuf = NULL;
  int hdrlen = PCMA_HDRLEN;
  int lenextra = 0;
  int lenm2 = 0;
  int len_auxcma = 0;
  DRHOOK_START(pcma_5_driver);

  nwrt = 0;

  replen = lencma;
  chunk = replen-1;  /* Report w/o length information */

  count  = hdrlen;   /* 'PCMA' + method & 3 zero bytes + no_of_packed_bytes + no_of_unpacked + 2 x double MDIs */

  if (replen > 1) {
    double *auxcma;
    int auxbytes;
    int numdbls;

    ALLOC(auxcma, replen);

    lenbitmap = RNDUP(chunk,b32)/b32; /* Bitmap (roundep up and truncated) */
    numdbls  = pcma_5_dbls(&cma[1], chunk, &auxcma[1]);  /* Count no. of genuine doubles */
    numsmall = chunk - numdbls;               /* No. of smallish [1..255] numbers */
    numsmall_packed = RNDUP(numsmall,b4)/b4;

    len_auxcma = 1 + numdbls; /* True length; and +1 comes from "report" length */
    auxcma[0] = len_auxcma;   /* "report" length */

    pauxpbuf = &auxpbuf;
    pauxpbuf->counter = 0;
    pauxpbuf->maxalloc = 0;
    pauxpbuf->len = 0;
    pauxpbuf->p = NULL;
    pauxpbuf->allocatable = 1;

    auxbytes = pcma_2_driver(2, NULL, auxcma, len_auxcma, nmdi, rmdi, pauxpbuf);
    FREE(auxcma);

    if (pauxpbuf->len < 0) {
      /* Error : pcma_2_driver() was unable to deliver a packed buffer */
      rc = -1;
      goto finish;
    }

    /* Active set is:
       small numbers (rounded up) +
       output from pcma_2_driver() excluding header (new_version only) */
       
    lenm2 = pauxpbuf->len - hdrlen;

    lenactive = numsmall_packed + lenm2;

    lenextra = 1 + 1 + 1; /* no. of smallish numbers +
			     no. of packed words with method#2 (excluding header) +
			     length of auxiliary CMA */

    count += (lenextra + lenbitmap + lenactive); /* One word for no. of smallish numbers */
  }
  else {
    lenextra = lenbitmap = lenactive = lenm2 = 0;
  }

  packed_data = pcma_alloc(pbuf, count);

  packed_data[0] = PCMA;
  packed_data[1] = 5 * MAXSHIFT + 0; /* zero no_of_unpacked means that 4th word, packed_data[3] contains
					report length; ==> no_of_unpacked can now be over 16megawords */

  packed_data[2] = (lenextra + lenbitmap + lenactive) * sizeof(*packed_data); /* bitmap + packed_data */
  packed_data[3] = replen;
  memcpy(&packed_data[4],&nmdi,sizeof(nmdi));
  memcpy(&packed_data[6],&rmdi,sizeof(rmdi));

  if (replen > 1) {
    unsigned int *extra      = &packed_data[hdrlen];
    unsigned int *bitmap     = &packed_data[hdrlen + lenextra];
    unsigned int *small_data = &packed_data[hdrlen + lenextra + lenbitmap];
    unsigned int *pcma2_data = (lenm2 > 0) ? &packed_data[hdrlen + lenextra + lenbitmap + numsmall_packed] : NULL;

    extra[0] = numsmall;
    extra[1] = lenm2;
    extra[2] = len_auxcma;
    {
      /* Handle smallish numbers */
      int len_tmpbuf = numsmall_packed * b4;
      nw = pcma_5(&cma[1], chunk, bitmap, small_data, len_tmpbuf);
    }

    if (lenm2 > 0) {
      /* Append the already packed stuff with method#2 */
      pcma_copy_uint(pcma2_data, &pauxpbuf->p[hdrlen], lenm2);
    }

    FREE(pauxpbuf->p);
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

/* upcma_5: perform the actual unpacking of a buffer */

PRIVATE int 
upcma_5(int swp,
	double  cma[],         /* output : double CMA[] */
	int  stacma,           /* starting address of the double CMA[] */
	int  endcma,           /* ending address of the double CMA[] */
        const unsigned int  bitmap[],      /* bitmap: 0=raw double CMA-data; 1=dbl */
	int lenbitmap,
	const double  indbl[],             /* Packed data: double input part; after upcma_2 */
	const unsigned int  small_data[],  /* Packed data: small data input part */
	int  len_tmpbuf
	)
{
  int rc = 0;
  int i, j, kdbl, k, n;
  unsigned int *tmpbuf = NULL; /* Temporary buffer for expanded small data input */
 
  /* Expand small_data[] into the tmpbuf[] */

  if (len_tmpbuf%b4 != 0) {
    fprintf(stderr,"upcma_5: len_tmpbuf%%b4 != 0 (len_tmpbuf=%d, b4=%d)\n",
	    len_tmpbuf, b4);
    RAISE(SIGABRT);
    exit(1);
  }

  ALLOC(tmpbuf, len_tmpbuf);

  k = 0;
  n = len_tmpbuf/b4;

  if (swp) {
    swap4bytes_((unsigned int *)small_data, &n);
    swap4bytes_((unsigned int *)bitmap,&lenbitmap);
  }

  for (j=0; j<n; j++) {
    tmpbuf[k  ] =  small_data[j] / 16777216U;
    tmpbuf[k+1] = (small_data[j] % 16777216U) / 65536U;
    tmpbuf[k+2] = (small_data[j] % 65536U) / 256U;
    tmpbuf[k+3] =  small_data[j] % 256U;
    k += 4;
  }

  /* Expand from indbl[] and tmpbuf[] to output CMA-array cma[]
     with bitmap[] as a driver */

  k = 0;
  kdbl = 0;
  for (i=0; i<stacma; i++) { /* get the right starting point for "k" and "kdb" */
    int offset = i/b32;
    int shift  = i%N32BITS;
    unsigned int bitset = ((bitmap[offset] & PWR2(shift)) / PWR2(shift));
    if (bitset) k++;
    else kdbl++;
  }

  for (i=stacma; i<endcma; i++) {
    int offset = i/b32;
    int shift  = i%N32BITS;
    unsigned int bitset = ((bitmap[offset] & PWR2(shift)) / PWR2(shift));
    if (bitset) {
      cma[i] = tmpbuf[k];
      k++;
    }
  }

  if (k > len_tmpbuf) {
    fprintf(stderr,"upcma_5: endcma=%d, stacma=%d, k != len_tmpbuf (k=%d, len_tmpbuf=%d)\n",
	    endcma, stacma, k, len_tmpbuf);
    RAISE(SIGABRT);
    exit(1);
  }

  FREE(tmpbuf);

  for (i=stacma; i<endcma; i++) {
    int offset = i/b32;
    int shift  = i%N32BITS;
    unsigned int bitset = ((bitmap[offset] & PWR2(shift)) / PWR2(shift));
    if (!bitset) {
      cma[i] = indbl[kdbl]; /* already in right endian format due to job done by upcma_2() !! */
      kdbl++;
    }
  }

  if (k + kdbl != endcma) {
    fprintf(stderr,"upcma_5: k + kdbl != endcma (k=%d, kdbl=%d, endcma=%d, stacma=%d)\n",
	    k, kdbl, endcma, stacma);
    RAISE(SIGABRT);
    exit(1);
  }

  rc = endcma-stacma;

  /* finish: */

  /* Upon successful completion returns no. of uint CMA-words unpacked */

  if (swp && pcma_restore_packed) {
    swap4bytes_((unsigned int *)small_data, &n);
    swap4bytes_((unsigned int *)bitmap,&lenbitmap);
  }

  return rc; 
}

int
upcma_5_driver(int method,
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
  const int one = 1;
  int numsmall = 0;
  DRHOOK_START(upcma_5_driver);

  replen = lencma;
  if (fp_out) { /* backward compatibility */
    idxlen = 0;
    fill_zeroth_cma = 1;
  }
  if (fill_zeroth_cma) {
    cma[0] = replen;
    fill_zeroth_cma = 1;
  }

  if (replen > 1) {
    numsmall = packed_data[0];
    if (swp) swap4bytes_(&numsmall, &one);
  }

  rc = 1;

  if (replen > 1) {
    int chunk  = replen - 1;
    int lenbitmap = RNDUP(chunk,b32)/b32;

    int numsmall_packed = RNDUP(numsmall,b4)/b4;
    int len_tmpbuf = numsmall_packed * b4;

    int lenextra = new_version ? 3 : 1;

    const unsigned int *extra             = &packed_data[0];
    const unsigned int *bitmap            = &packed_data[lenextra];
    const unsigned int *small_data        = &packed_data[lenextra + lenbitmap];
    const unsigned int *pcma_auxpack_data = &packed_data[lenextra + lenbitmap + numsmall_packed];

    unsigned int aux0 = pcma_auxpack_data[0];

    double *auxcma = NULL;
    int auxreplen; 

    if (swp) {
      swap4bytes_((unsigned int *)extra, &lenextra);
      swap4bytes_(&aux0, &one);
    }

    auxreplen = new_version ? extra[2] : aux0%MAXSHIFT;

    ALLOC(auxcma, auxreplen);

    if (new_version) {
      int hdrlen = PCMA_HDRLEN;
      int lenm2 = extra[1];
      
      if (lenm2 > 0) {
	int can_swp_data_here = 1; /* to avoid undoing of byte swapping twice */
	rc = upcma_2_driver(2,
			    swp, can_swp_data_here,
			    new_version,
			    pcma_auxpack_data,
			    lenm2,
			    (lenm2 - hdrlen) * sizeof(*pcma_auxpack_data),
			    nmdi,
			    rmdi,
			    NULL, 
			    NULL, 0, 
			    fill_zeroth_cma,
			    auxcma, auxreplen);
	
	if (rc != auxreplen) {
	  rc = -1;
	  goto finish;
	}
      }
    }
    else {
      /* Old version only */

      /* Handle the second level of unpacking first */

      unsigned int hdr[HDRLEN];

      hdr[0] = swp ? AMCP : PCMA;
      hdr[1] = pcma_auxpack_data[0]; /* must be w/o byte swapping */
      hdr[2] = pcma_auxpack_data[1]; /* must be w/o byte swapping */

      {
	int can_swp_data_here = 1; /* to avoid undoing of byte swapping twice */
	upcmaTOcma(&can_swp_data_here,
		   (const unsigned int *)hdr, 
		   &pcma_auxpack_data[HDRLEN-1], 
		   NULL, 0, fill_zeroth_cma,
		   auxcma, &auxreplen, &rc);
      }
	
      if (rc != auxreplen) {
	/* Error: Unable to unpack data into the auxcma[] */
	rc = -2;
	goto finish;
      }
    }

    /* Handle the first level here */

    {
      int stacma = 0;
      int endcma = chunk;
      double *cma_addr = &cma[fill_zeroth_cma];

      STAEND_FIX();

      rc = upcma_5(swp,
		   cma_addr,
		   stacma,endcma,
		   bitmap, lenbitmap,
		   &auxcma[fill_zeroth_cma],
		   small_data,
		   len_tmpbuf);

      SWAP_DATA_BACKv2(swp,can_swap_data,cma,stacma,endcma);

      rc = (rc == endcma-stacma) ? replen : rc;
    }
    
  finish:
    FREE(auxcma);

    if (swp && pcma_restore_packed) {
      swap4bytes_((unsigned int *)extra, &lenextra);
    }
  }

  if (fp_out) nwrt = fwrite(cma, sizeof(double), lencma, fp_out);

  DRHOOK_END(0);
  return rc;
}

