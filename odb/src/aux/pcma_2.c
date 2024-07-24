
/* === CMA-packing method#2 === */

/* Thanks to Bob Carruthers from Cray for supplying Cray X1 (SV2) 
   directives & mods for better vectorization (Aug-Sep/2004) */

#include "pcma.h"

#ifdef VPP
#pragma global noalias
#elif defined(NECSX)
#pragma cdir options -pvctl,nodep
#endif

#ifdef LITTLE
#define RMDI_1 4294959041U
#define RMDI_2      49407U
#else
#define RMDI_1 3252682751U
#define RMDI_2 4290772992U
#endif

#define IS_ONE_OF(u) ( 3U*((u) == ZERO) + 2U*((u) == i_rmdi_1) + ((u) == i_rmdi_2) )

#define DEFMASK(x) unsigned int mask##x = ( IS_ONE_OF(icma[i+(x)]) << ((N32BITS/b16)*(x)) )

/* pcma_2_count: Count no. of words that are not one of {ZERO, RMDI_1, RMDI_2}'s */

static int
pcma_2_count(unsigned int i_rmdi_1,
	     unsigned int i_rmdi_2,
	     const unsigned int  icma[],    /* uint representation of the double CMA[] */
    	                    int  lenicma    /* 2 x length of the double CMA[] */
	     )
{
  int rc = 0;
  int i;
  int zero_count   = 0;
  int rmdi_1_count = 0;
  int rmdi_2_count = 0;

#if defined(SV2)
#pragma _CRI ivdep
#endif
  for (i=0; i<lenicma; i++) {
    /* Check if an uint CMA-word (a half of the double CMA-words) 
       contains any of the {ZERO, RMDI_1, RMDI_2} */

    if (icma[i] == ZERO) {
      zero_count++;
    }
    else if (icma[i] == i_rmdi_1) {
      rmdi_1_count++;
    }
    else if (icma[i] == i_rmdi_2) {
      rmdi_2_count++;
    }
  }

  rc = lenicma - (zero_count + rmdi_1_count + rmdi_2_count);

  /* finish: */
  return rc;
}

/* pcma_2: perform the actual packing of a buffer */

static int 
pcma_2(unsigned int i_rmdi_1,
       unsigned int i_rmdi_2,
       const unsigned int  icma[],    /* uint representation of the double CMA[] */
                      int  lenicma,   /* 2 x length of the double CMA[] */
             unsigned int  bitmap[],  /* bitmap: 0=raw uint CMA-data; 1=RMDI_2; 2=RMDI_1; 3=ZERO */
             unsigned int  outbuf[]   /* Effective packed output; w/o bitmap though */
       )
{
  int rc = 0;
  int i, k;
  int istart, iend;

  istart = 0;
  iend   = lenicma - lenicma%b16;

  /*
#if defined(SV2)
#pragma _CRI ivdep
  for (i=istart; i<iend; i+=b16) {
    int offset = (i >> 4) & 0x07FFFFFF;

    bitmap[offset]=0u;
  }
#pragma _CRI unroll
  for (k=0; k<16; k++) {
    int mask_size = (N32BITS >> 4);
    int mask_shift = (mask_size*k);
#pragma _CRI concurrent
    for (i=istart; i<iend; i+=b16) {
      int offset = (i >> 4) & 0x07FFFFFF;

      bitmap[offset] = bitmap[offset] | (IS_ONE_OF(icma[i+k]) << mask_shift);
    }
  }

#else
  */

#if defined(SV2)
#pragma _CRI ivdep
#endif
  for (i=istart; i<iend; i+=b16) {
#if defined(SV2)
    int offset = i >> 4;
#else
    int offset = i/b16;
#endif

    DEFMASK(0) ; DEFMASK(1) ; DEFMASK(2) ; DEFMASK(3) ;
    DEFMASK(4) ; DEFMASK(5) ; DEFMASK(6) ; DEFMASK(7) ;
    DEFMASK(8) ; DEFMASK(9) ; DEFMASK(10); DEFMASK(11);
    DEFMASK(12); DEFMASK(13); DEFMASK(14); DEFMASK(15);

#if defined(SV2)
    bitmap[offset] =   
      mask0  |  mask1 |  mask2 |  mask3 | 
      mask4  |  mask5 |  mask6 |  mask7 |
      mask8  |  mask9 | mask10 | mask11 |
      mask12 | mask13 | mask14 | mask15;
#else
    bitmap[offset] =   
      mask0  +  mask1 +  mask2 +  mask3 + 
      mask4  +  mask5 +  mask6 +  mask7 +
      mask8  +  mask9 + mask10 + mask11 +
      mask12 + mask13 + mask14 + mask15;
#endif
  }
  /*
#endif
  */

  if (iend < lenicma) {
    unsigned int sum = 0;
    int offset = lenicma/b16;

#if defined(SV2)
#pragma _CRI ivdep
#endif
    for (i=iend; i<lenicma; i++) {
      int shift = (N32BITS/b16)*(i-iend);
      unsigned int mask = IS_ONE_OF(icma[i]);
      /* sum += (mask << shift); */
      sum += mask * PWR2(shift);
    }

    bitmap[offset] = sum;
  }

  k = 0;
#if defined(SV2)
#pragma _CRI ivdep
#endif
  for (i=0; i<lenicma; i++) {
    if (! IS_ONE_OF(icma[i]) ) {
      outbuf[k] = icma[i];
      k++;
    }
  }

  rc = lenicma;

  /* finish: */

  /* Upon successful completion returns no. of uint CMA-words packed */

  return rc; 
}


int
pcma_2_driver(int method, /* ignored */
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
  unsigned int *packed_data = NULL;
  int hdrlen = PCMA_HDRLEN;
  unsigned int *i_rmdi = (unsigned int *)&rmdi;
  DRHOOK_START(pcma_2_driver);

  nwrt = 0;

  replen = lencma;
  chunk = replen-1;  /* Report w/o length information */

  count  = hdrlen;   /* 'PCMA' + method & 3 zero bytes + no_of_packed_bytes + no_of_unpacked + 2 x double MDIs */

  if (replen > 1) {
    lenbitmap = RNDUP(2*chunk,b16)/b16;  /* Bitmap (roundep up and truncated) */
    lenactive = pcma_2_count(i_rmdi[0], i_rmdi[1], 
			     (unsigned int *)&cma[1], 2*chunk); /* Active packed uint(s) */
  }
  else {
    lenbitmap = lenactive = 0;
  }
  count += (lenbitmap + lenactive);
    
  packed_data = pcma_alloc(pbuf, count);

  packed_data[0] = PCMA;
  packed_data[1] = 2 * MAXSHIFT + 0; /* zero no_of_unpacked means that 4th word, packed_data[3] contains
					report length; ==> no_of_unpacked can now be over 16megawords */
  packed_data[2] = (lenbitmap + lenactive) * sizeof(*packed_data); /* bitmap + packed_data */
  packed_data[3] = replen;
  memcpy(&packed_data[4],&nmdi,sizeof(nmdi));
  memcpy(&packed_data[6],&rmdi,sizeof(rmdi));

  if (replen > 1) {
    unsigned int *bitmap = &packed_data[hdrlen];
    unsigned int *data   = &packed_data[hdrlen + lenbitmap];
    nw = pcma_2(i_rmdi[0], i_rmdi[1],
		(unsigned int *)&cma[1], 2*chunk, 
		bitmap, data);
  }

  if (fp_out) nwrt = fwrite(packed_data, sizeof(*packed_data), count, fp_out);

  total_count += count;

  rc = total_count *  sizeof(*packed_data);

  /* finish: */
  if (!pbuf) FREE(packed_data);

  DRHOOK_END(0);
  return rc;
}


/* ======================================================================================== */

#define ZERO_BITSET   3
#define RMDI_1_BITSET 2
#define RMDI_2_BITSET 1

/* upcma_2: perform the actual unpacking of a buffer */

static int 
upcma_2(int swp,
	unsigned int i_rmdi_1,
	unsigned int i_rmdi_2,
	unsigned int  icma[],    /* output : uint representation of the double CMA[] */
	int  staicma,            /* 2 x stacma of the double CMA[] */
	int  endicma,            /* 2 x endcma of the double CMA[] */
        const unsigned int  bitmap[],  /* bitmap: 0=raw uint CMA-data; 1=RMDI_2; 2=RMDI_1; 3=ZERO */
	int lenbitmap,
        const unsigned int  inbuf[],   /* Packed input; w/o bitmap though */
	int leninbuf
	)
{
  int rc = 0;
  int i, k;

  if (swp) {
    int len = leninbuf/2;
    swap4bytes_((unsigned int *)bitmap,&lenbitmap);
    /* swap8bytes_((double *)inbuf,&len); */
  }

  k = 0;
  for (i=0; i<staicma; i++) { /* get the right starting point for "k" */
    int offset = i/b16;
    int shift  = ((N32BITS/b16)*i)%N32BITS;
    unsigned int bitset = ((bitmap[offset] & (3U * PWR2(shift))) / PWR2(shift));
    if (bitset == 0) k++;
  }

#if defined(SV2)
#pragma _CRI ivdep
#endif
  for (i=staicma; i<endicma; i++) {
#if defined(SV2)
    int mask_size = (N32BITS >> 4);
    int offset;
    int shift;
    unsigned int bitset;

    offset = (i >> 4) & 0x07FFFFFF;
    shift = mask_size*i;
    shift = (((shift >> 5) & 0x03FFFFFF) << 5) ^ shift;
    bitset = ((bitmap[offset] & (3U << shift)) >> shift) & 0x3;
#else
    int offset = i/b16;
    int shift  = ((N32BITS/b16)*i)%N32BITS;
    unsigned int bitset = ((bitmap[offset] & (3U * PWR2(shift))) / PWR2(shift));
#endif

         if (bitset ==   ZERO_BITSET) icma[i] = ZERO;
    else if (bitset == RMDI_1_BITSET) icma[i] = i_rmdi_1;
    else if (bitset == RMDI_2_BITSET) icma[i] = i_rmdi_2;
    else {
      icma[i] = inbuf[k];
      k++;
    }
  }

  rc = endicma-staicma;

  if (swp) {
    int len = (endicma-staicma)/2;
    swap8bytes_((double *)&icma[staicma], &len);
  }

  /* finish: */

  /* Upon successful completion returns no. of uint CMA-words unpacked */

  if (swp && pcma_restore_packed) {
    int len = leninbuf/2;
    swap4bytes_((unsigned int *)bitmap,&lenbitmap);
    /* swap8bytes_((double *)inbuf,&len); */
  }

  return rc; 
}

int
upcma_2_driver(int method,
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
  DRHOOK_START(upcma_2_driver);

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
    int chunk  = replen - 1;
    int stacma = 0;
    int endcma = chunk;
    int lenbitmap = RNDUP(2*chunk,b16)/b16;
    int lenactive = len_packed_data - lenbitmap;

    const unsigned int *bitmap = &packed_data[0];
    const unsigned int *data   = &packed_data[lenbitmap];

    unsigned int *i_rmdi = (unsigned int *)&rmdi;
    const int one = 1;
    double *cma_addr = &cma[fill_zeroth_cma];

#ifdef DEBUG
    fprintf(stderr,"upcma_2_driver(swp=%d, new_version=%d, can_swp_data=%d) : chunk=%d\n",
	    swp,new_version,can_swp_data,chunk);
    fprintf(stderr,">rmdi=%.16g (nmdi=%.16g)\n",rmdi,nmdi);
    fprintf(stderr,">i_rmdi[0]=%u, RMDI_1=%u\n",i_rmdi[0],RMDI_1);
    fprintf(stderr,">i_rmdi[1]=%u, RMDI_2=%u\n",i_rmdi[1],RMDI_2);
#endif

    if (swp) {
      swap8bytes_(&rmdi, &one); /* note: swapped the i_rmdi[0] & i_rmdi[1], too */
#ifdef DEBUG
      fprintf(stderr,"<i_rmdi[0]=%u, RMDI_1=%u\n",i_rmdi[0],RMDI_1);
      fprintf(stderr,"<i_rmdi[1]=%u, RMDI_2=%u\n",i_rmdi[1],RMDI_2);
      fprintf(stderr,"<rmdi=%.16g (nmdi=%.16g)\n",rmdi,nmdi);
#endif
    }

    STAEND_FIX();

    rc = upcma_2(swp,
		 i_rmdi[0], i_rmdi[1],
		 (unsigned int *)cma_addr,
		 2*stacma, 2*endcma,
		 bitmap, lenbitmap,
		 data, lenactive);
    rc /= 2;

    SWAP_DATA_BACKv2(swp,can_swap_data,cma,stacma,endcma);

    rc = (rc == endcma-stacma) ? replen : rc;
  }

  if (fp_out) nwrt = fwrite(cma, sizeof(double), lencma, fp_out);

  DRHOOK_END(0);
  return rc;
}
