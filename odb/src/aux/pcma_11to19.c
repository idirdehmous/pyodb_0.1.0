
/* === CMA-packing methods#11 to 19 === */

#include "pcma.h"

#include <signal.h>
#include <math.h>

#ifdef VPP
#pragma global noalias
#pragma global novrec
#elif defined(NECSX)
#pragma cdir options -pvctl,nodep
#endif

#define IS_RMDI(u) ((u) == rmdi)

#define DEFMASK(x) unsigned int mask##x = ( IS_RMDI(cma[i+(x)]) << (x) )

PRIVATE int Nbits[10] = { 0, 4, 7, 10, 14, 17, 20, 24, 27, 30 };

/* pcma_11to19_prepare: Prepare for compression */

PRIVATE unsigned int *
pcma_11to19_prepare(double rmdi,
		    const double cma[], /* double CMA[] */
		    int  lencma,        /* length of the double CMA[] */
		    int *Nbits,         /* No. of bits requested */
		    int *Noffset,       /* Length of offset array <all non-negative> */
		    int *Npacked,       /* Minimum length for output packed array */
		    double *Vmax        /* Value of the absolute maximum */
		    )
{
  double vmax = -INT_MAX;
  double vmin =  INT_MAX;
  int nbits = *Nbits;
  int i, j, n;
  unsigned int *Offset = NULL;
  
  n = 0;
  for (i=0; i<lencma; i++) {
    if (!IS_RMDI(cma[i])) {
      n++;
      if (vmax < cma[i]) vmax = cma[i];
      if (vmin > cma[i]) vmin = cma[i];
    }
  }

  if (vmin >= 0) nbits--; /* Still always > 0 */
  if (vmin == vmax) nbits = 0; /* Special case: all non-RMDI's are the same */

#ifdef DEBUG
  fprintf(stderr,"vmin=%f, vmax=%f, *Nbits=%d, nbits=%d\n",vmin,vmax,*Nbits,nbits);
#endif

  vmin = ABS(vmin);
  vmax = MAX(vmax, vmin);

  if (nbits > 0) {
    ALLOC(Offset, n);
    if (n > 0) {
      unsigned int p2 = PWR2(nbits)-1;
      double mult = (0.5 * p2)/vmax;

#ifdef DEBUG
      fprintf(stderr,"mult=%f, p2=%u 0x%8.8x\n",mult,p2,p2);
#endif

      j = 0;
      for (i=0; i<lencma; i++) {
	if (!IS_RMDI(cma[i])) {
	  double value = (vmax - cma[i]) * mult;
	  Offset[j] = value;
#ifdef DEBUG
	  if (j<20) {
	    fprintf(stderr,
		    "value=%f, cma[%d]=%f, Offset[%d]=%u\n",
		    value, i, cma[i], j, Offset[j]);
	  }
#endif
	  j++;
	}
      }
    } /* if (n > 0) */
  }
  else {
    n = 0;
  }

  *Noffset = n;
  *Npacked = RNDUP(n * nbits,N32BITS)/N32BITS;
  *Nbits = nbits;
  *Vmax = vmax;

  return Offset;
}

/* pcma_11to19: perform the actual packing of a buffer */

PRIVATE int 
pcma_11to19(double vmax,
	    double rmdi,
	    const double  cma[],   /* double CMA[] */
	    int    lencma,   /* length of the double CMA[] */
	    const unsigned int Offset[],    /* preprocessed offset data */
	    int noffset,     /* no. of elements in offset data */
	    int nbits,       /* no. of bits involved */
	    unsigned int    bitmap[], /* bitmap: 0=positive values, 1=negative, 2=nmdi, 3=rmdi */
	    int lenbitmap,  /* length of bitmap[] */
	    unsigned int    outbuf[], /* Effective packed output; w/o bitmap though */
	    int noutbuf              /* No. of elements in outbuf[] */
	    )
{
  int rc = 0;
  int i;
  int istart, iend;

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
    unsigned int sum = 0;
    int offset = lencma/b32;

    for (i=iend; i<lencma; i++) {
      int shift = (i-iend);
      unsigned int mask = IS_RMDI(cma[i]);
      sum += mask * PWR2(shift);
    }

    bitmap[offset] = sum;
  }

  if (nbits > 0) {
    /* Pack offsets */
    vpack_bits_(&nbits,
		Offset,
		&noffset,
		outbuf,
		&noutbuf,
		&rc);
  }
  else
    rc = 0;

  rc = (rc == noutbuf) ? lencma : -1;

  /* finish: */

  /* Upon successful completion returns no. of CMA-words packed */

  if (rc != lencma) {
    perror("pcma_11to19: (rc != lencma)");
  }

  return rc; 
}


int
pcma_11to19_driver(int method,
		   FILE *fp_out,
		   const double  cma[],
		   int  lencma,
		   double nmdi,
		   double rmdi,
		   Packbuf *pbuf)
{
  int rc = 0;
  int replen, chunk;
  int count, nw, nwrt, nbits;
  int total_count = 0;
  int lenextra, lenbitmap, lenactive;
  unsigned int *packed_data = NULL;
  int hdrlen = PCMA_HDRLEN;
  double vmax = 0;
  int noffset = 0;
  unsigned int *Offset = NULL;
  int prec = method - 10;
  DRHOOK_START(pcma_11to19_driver);

  if (prec < 1) {
    prec = 1;
  }
  else if (prec > 9) {
    prec = 9;
  }
  nbits = Nbits[prec];

  nwrt = 0;

  replen = lencma;
  chunk = replen-1;  /* Report w/o length information */

  count = hdrlen;   /* 'PCMA' + method & 3 zero bytes + no_of_packed_bytes + 
		       no_of_unpacked + 2 x double MDIs */

  if (replen > 1) { 
    Offset = pcma_11to19_prepare(rmdi, 
				 &cma[1], chunk, 
				 &nbits, &noffset, 
				 &lenactive, /* Active no. of words that are non-RMDIs */
				 &vmax); 
    
    lenbitmap = RNDUP(chunk,b32)/b32;  /* Bitmap (roundep up and truncated) */
    lenextra = 1 + 1 + 1 * (sizeof(double)/sizeof(int)); /* nbits, noffset, vmax */
  }
  else {
    lenextra = lenbitmap = lenactive = 0;
  }
  count += (lenextra + lenbitmap + lenactive);
    
  packed_data = pcma_alloc(pbuf, count);

  packed_data[0] = PCMA;
  packed_data[1] = (10 + prec) * MAXSHIFT + 0; /* zero no_of_unpacked means that 4th word, 
						  packed_data[3] contains report length; 
						  ==> no_of_unpacked can now be over 16megawords */
  packed_data[2] = 
    (lenextra + lenbitmap + lenactive) 
      * sizeof(*packed_data); /* extra + bitmap + packed_data */
  packed_data[3] = replen;
  memcpy(&packed_data[4],&nmdi,sizeof(nmdi));
  memcpy(&packed_data[6],&rmdi,sizeof(rmdi));

  if (replen > 1) {
    unsigned int *extra  = &packed_data[hdrlen];
    unsigned int *bitmap = &packed_data[hdrlen + lenextra];
    unsigned int *data   = (nbits > 0) ? &packed_data[hdrlen + lenextra + lenbitmap] : NULL;
    extra[0] = nbits;
    extra[1] = noffset;
    memcpy(&extra[2],&vmax,sizeof(vmax));
    nw = pcma_11to19(vmax, rmdi,
		     &cma[1], chunk,
		     Offset, noffset, nbits,
		     bitmap, lenbitmap,
		     data, lenactive);
    FREE(Offset);
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


/* upcma_1: perform the actual unpacking of a buffer */

PRIVATE int 
upcma_11to19(int swp,
	     double vmax,
	     double rmdi,
	     double  cma[],   /* double CMA[] */
	     int    stacma,   /* starting address of the double CMA[] */
	     int    endcma,   /* ending address of the double CMA[] */
	     const int idx[],
	     int       idxlen,
	     unsigned int Offset[],    /* preprocessed offset data */
	     int noffset,     /* no. of elements in offset data */
	     int nbits,       /* no. of bits involved */
	     const unsigned int    bitmap[], /* bitmap: 0=normal values, 1=RMDI */
	     int lenbitmap,  /* length of bitmap[] */
	     const unsigned int    outbuf[], /* Effective packed output; w/o bitmap though */
	     int noutbuf              /* No. of elements in outbuf[] */
	     )
{
  int rc = 0;
  int i;

  if (swp) { /* swap bytes */
    swap4bytes_((unsigned int *)bitmap,&lenbitmap);
    swap4bytes_((unsigned int *)outbuf,&noutbuf);
  }

  if (nbits > 0) {
    /* Unpack offsets */
    vunpack_bits_(&nbits,
		  outbuf,
		  &noutbuf,
		  Offset,
		  &noffset,
		  &rc);
  }
  else
    rc = 0;

  if (rc == noffset) {
    int j = 0;

    if (nbits > 0) {
      unsigned int p2 = PWR2(nbits)-1;
      double mult = (2 * vmax)/p2;

#ifdef DEBUG
      fprintf(stderr,"vmax=%f, mult=%f, p2=%u 0x%8.8x, nbits=%d\n",vmax,mult,p2,p2,nbits);
#endif

      for (i=0; i<stacma; i++) { /* get the right starting point for "j" */
	int offset = i/b32;
	int shift  = i%N32BITS;
	unsigned int bitset = (bitmap[offset] & PWR2(shift));
	if (!bitset) j++;
      }

      for (i=stacma; i<endcma; i++) {
	int offset = i/b32;
	int shift  = i%N32BITS;
	unsigned int bitset = (bitmap[offset] & PWR2(shift));
	if (bitset) {
	  cma[i] = rmdi;
	}
	else {
	  double value = Offset[j];
	  cma[i] = vmax - value * mult;
#ifdef DEBUG
	  if (j<20) {
	    fprintf(stderr,
		    "value=%f, value * mult=%f, cma[%d]=%f, Offset[%d]=%u\n",
		    value, value * mult, i, cma[i], j, Offset[j]);
	  }
#endif
	  j++;
	}
      }
    }
    else { /* All non-RMDIs must be the same */
      if (idx && idxlen > 0 && idxlen < endcma-stacma) {
	int ii;
	for (i=0; i<idxlen; i++) { /* ultra-fast :-) */
	  ii = idx[i];
	  if (ii >= stacma && ii < endcma) {
	    int offset = ii/b32;
	    int shift  = ii%N32BITS;
	    unsigned int bitset = (bitmap[offset] & PWR2(shift));
	    if (bitset) {
	      cma[ii] = rmdi;
	    }
	    else {
	      cma[ii] = vmax;
	    }
	  } /* if (ii >= stacma && ii < endcma) */
	} /* for (i=0; i<idxlen; i++) */
      }
      else {
	for (i=stacma; i<endcma; i++) {
	  int offset = i/b32;
	  int shift  = i%N32BITS;
	  unsigned int bitset = (bitmap[offset] & PWR2(shift));
	  if (bitset) {
	    cma[i] = rmdi;
	  }
	  else {
	    cma[i] = vmax;
	  }
	}
      }
    }
    
    rc = endcma-stacma;
  }
  else
    rc = -1;

  /* finish: */

  /* Upon successful completion returns no. of uint CMA-words unpacked */

  if (rc != endcma-stacma) {
    perror("upcma_11to19: (rc != endcma-stacma)");
  }

  if (rc == endcma-stacma && swp && pcma_restore_packed) { /* restore */
    swap4bytes_((unsigned int *)bitmap,&lenbitmap);
    swap4bytes_((unsigned int *)outbuf,&noutbuf);
  }

  return rc; 
}

int
upcma_11to19_driver(int method,
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
  DRHOOK_START(upcma_11to19_driver);

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
    int lenextra  = 1 + 1 + 1 * (sizeof(double)/sizeof(int)); /* i.e. 4 */
    int lenbitmap =  RNDUP(chunk,b32)/b32;
    int lenactive = len_packed_data - lenbitmap - lenextra;

    const unsigned int *extra  = &packed_data[0];
    const unsigned int *bitmap = &packed_data[lenextra];
    const unsigned int *data;

    int nbits = extra[0];
    int noffset = extra[1];
    unsigned int *Offset = NULL;
    double vmax;

    const int one = 1;
    const int two = 2;

    memcpy(&vmax,&extra[2],sizeof(vmax));

    if (swp) {
      swap4bytes_(&nbits, &one);
      swap4bytes_(&noffset, &one);
      swap8bytes_(&vmax, &one);
    }

#ifdef DEBUG
    fprintf(stderr,
	    "upcma_11to19_driver(method=%d): vmax=%g, nbits=%d, noffset=%d\n",
	    method, vmax, nbits, noffset);
#endif

    if (nbits > 0) {
      ALLOC(Offset, noffset);
      data = &packed_data[lenextra + lenbitmap];
    }
    else {
      Offset = NULL;
      data = NULL;
    }

    STAEND_FIX();

    {
      double *cma_addr = &cma[fill_zeroth_cma];
      rc = upcma_11to19(swp,
			vmax,rmdi,
			cma_addr, 
			stacma, endcma,
			idx, idxlen,
			Offset, noffset, nbits,
			bitmap, lenbitmap,
			data, lenactive);
    }

    FREE(Offset);
    
    rc = (rc == endcma-stacma) ? replen : rc;
  }

  if (fp_out) nwrt = fwrite(cma, sizeof(double), lencma, fp_out);

  DRHOOK_END(0);
  return rc;
}
