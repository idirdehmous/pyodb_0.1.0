
/* === Vectorizable bit-packing (resembles GBYTES) === */

/* Author: Sami Saarinen, ECMWF, sometime in 2001 meant mainly for packing method = 1 */

/* Thanks to John Chambers (ECMWF) for discussions & program extracts on how GRIB-packing works */

/* == Check for thread-safeness of this utility ; looks bad says author ;-( == */

#include "odb.h"

#include "pcma.h"

#include <signal.h>

#ifdef VPP
#pragma global noalias
#pragma global novrec
#elif defined(NECSX)
#pragma cdir options -pvctl,nodep
#endif

/* these below are "brilliant" for thread-safeness ... sigh! */

PRIVATE int maxbits = 0;
PRIVATE int I_inc[N32BITS];
PRIVATE int J_inc[N32BITS];
PRIVATE int Mask[N32BITS];

PRIVATE void init();

PUBLIC char *
bit_pattern(unsigned int mask)
{
  unsigned int one = 1;
  int j;
  char c[N32BITS+1];
  if (maxbits == 0) init();
  c[maxbits] = '\0';
  for (j=maxbits-1; j>=0; j--) {
    c[j] = (mask & one) ? '1' : '0';
    one <<= 1;
  }
  return STRDUP(c);
}

PRIVATE void
get_incs(int nbits, int *i_inc, int *j_inc)
{
  int inc = maxbits/nbits;
  int inc_nbits = inc * nbits;
  int gap = maxbits - inc_nbits;
  int ncycles = 0;

  if (gap > 0) {
    int spill = 0;
    do {
      ncycles++;
      gap = maxbits - inc_nbits - spill;
      if (gap >= 0) {
	spill = (nbits - gap)%nbits;
      }
      else {
	spill = -gap;
      }
    } while (spill != 0);
    *i_inc = ncycles;
    *j_inc = maxbits/(nbits/ncycles);
  }
  else {
    ncycles = maxbits/nbits;
    *i_inc = 1;
    *j_inc = ncycles;
  }
}

PRIVATE void
init()
{
  int j;
  unsigned int mask = 0xFFFFFFFFU;

  /* no. of bits per word */
  maxbits = 0;
  while (maxbits++, mask >>= 1);

  if (maxbits != N32BITS) {
    fprintf(stderr,
	    "***Error in init()@%s: maxbits = %d not equal to %d\n",
	    __FILE__,maxbits,N32BITS);
    RAISE(SIGABRT);
  }

  for (j=1; j<=maxbits; j++) {
    get_incs(j, &I_inc[j-1], &J_inc[j-1]);
    Mask[j-1] = (~0U) >> (maxbits-j);
  } /* for (j=1; j<=maxbits; j++) */

#ifdef DEBUG
  fprintf(stderr,"init(): I_inc, J_inc, Mask follow ...\n");
  for (j=1; j<=maxbits; j++) {
    extern char *bit_pattern(unsigned int mask);
    char *c = bit_pattern(Mask[j-1]);
    fprintf(stderr,"(%2d): I_inc = %4d, J_inc = %4d, Mask = %12u [0x%8.8x] %s\n",
	    j, I_inc[j-1], J_inc[j-1], Mask[j-1], Mask[j-1], c);
    FREE(c);
  }
#endif
}


/* packing */

void
vpack_bits_(const int *N_bits,
	    const unsigned int    unpacked[],
	             const int *N_unpacked,
	          unsigned int    packed[],
	             const int *N_packed,
	          int *retcode)
{
  int rc = 0;
  int nbits = *N_bits;
  int nupk = (unpacked && (*N_unpacked > 0)) ? *N_unpacked : 0;
  int npk  = (packed   && (*N_packed   > 0)) ? *N_packed   : 0;
  int shift;
  int i, i_inc, i_start, i_max; /* for packed data */
  int j, j_inc, j_start, j_max; /* for unpacked data */

  if (maxbits == 0) init();

  if (nbits < 1 || nbits > maxbits) {
    fprintf(stderr,
	    "vpack_bits_(): Invalid # of bits (=%d); must be between [1..%d]\n",
	    nbits,maxbits);
    goto finish;
  }

  i_start = 0;
  j_start = 0;

  i_inc = I_inc[nbits-1];
  j_inc = J_inc[nbits-1];

  j_max = nupk;
  i_max = RNDUP(j_max * nbits,maxbits)/maxbits;

  if (i_max > npk) {
    rc = -i_max;
    goto finish;
  }

  for (i=0; i<i_max; i++) packed[i] = 0;

  shift = maxbits - nbits;

  for (;;) {
    int k, k_max;
    unsigned int mask, notmask, negshift;

    j = j_start;
    if (j >= j_inc) break;
    i = i_start;

    k_max = (j_max-j)/j_inc + 1;
    if ((k_max-1)*j_inc + j >= j_max) k_max--;

    if (shift >= 0) {
      mask = Mask[nbits-1] << shift;
      notmask = ~mask;
      for (k=0; k<k_max; k++) {
	packed[i] = (packed[i] & notmask) | ((unpacked[j] << shift) & mask);
	i += i_inc;
	j += j_inc;
      } /* for (k=0; k<k_max; k++) */
    }
    else { /* shift < 0 */
      mask = Mask[nbits+shift-1];
      notmask = ~mask;
      negshift = -shift;
      for (k=0; k<k_max; k++) {
	packed[i] = (packed[i] & notmask) | ((unpacked[j] >> negshift) & mask);
	i += i_inc;
	j += j_inc;
      } /* for (k=0; k<k_max; k++) */
    }

    if (shift == 0) {
      break;
    }
    else if (shift > 0) {
      shift -= nbits;
      j_start++;
    }
    else { /* shift < 0 */
      shift += maxbits;
      i_start++;
    }

  } /* for (;;) */

  rc = i_max;

#ifdef DEBUG
  {
    char *env = getenv("KKMAX");
    int kkmax = env ? atoi(env) : 3;
    int k, kmax = MIN(i_max,kkmax);
    fprintf(stderr,"vpack_bits_: in j_max=%d, out i_max=%d words [kmax=%d]\n",j_max,i_max,kmax);
    for (k=0; k<kmax; k++) {
      extern char *bit_pattern(unsigned int mask);
      char *c = bit_pattern(packed[k]);
      fprintf(stderr,"\tunpacked[%4d] = %12u, packed[%4d] = %12u : %s\n",k,unpacked[k],k,packed[k],c);
      FREE(c);
    }
  }
#endif

 finish:
  *retcode = rc;
}


/* unpacking */

void
vunpack_bits_(const int *N_bits,
	      const unsigned int    packed[],
	               const int *N_packed,
	            unsigned int    unpacked[],
	               const int *N_unpacked,
	            int *retcode)
	      
{
  int rc = 0;
  int nbits = *N_bits;
  int nupk = (unpacked && (*N_unpacked > 0)) ? *N_unpacked : 0;
  int npk  = (packed   && (*N_packed   > 0)) ? *N_packed   : 0;
  int shift;
  int i, i_inc, i_start, i_max; /* for packed data */
  int j, j_inc, j_start, j_max; /* for unpacked data */

  if (maxbits == 0) init();

  if (nbits < 1 || nbits > maxbits) {
    fprintf(stderr,
	    "vunpack_bits_(): Invalid # of bits (=%d); must be between [1..%d]\n",
	    nbits,maxbits);
    goto finish;
  }

  i_start = 0;
  j_start = 0;

  i_inc = I_inc[nbits-1];
  j_inc = J_inc[nbits-1];

  i_max = npk;
  j_max = RNDUP(i_max * maxbits, nbits)/nbits;
  if (nupk > 0 && j_max > nupk) j_max = nupk;
  
  if (nupk == 0) {
    rc = -j_max;
    goto finish;
  }
	      
  for (j=0; j<j_max; j++) unpacked[j] = 0;

  shift = maxbits - nbits;

  for (;;) {
    int k, k_max;
    unsigned int mask, notshift;

    j = j_start;
    if (j >= j_inc) break;
    i = i_start;

    k_max = (j_max-j)/j_inc + 1;
    if ((k_max-1)*j_inc + j >= j_max) k_max--;

    if (shift >= 0) {
      mask = Mask[nbits-1] << shift;
      for (k=0; k<k_max; k++) {
	unpacked[j] |= ((packed[i] & mask) >> shift);
	i += i_inc;
	j += j_inc;
      } /* for (k=0; k<k_max; k++) */
    }
    else { /* shift < 0 */
      mask = Mask[nbits+shift-1];
      notshift = -shift;
      for (k=0; k<k_max; k++) {
	unpacked[j] |= ((packed[i] & mask) << notshift);
	i += i_inc;
	j += j_inc;
      } /* for (k=0; k<k_max; k++) */
    }

    if (shift == 0) {
      break;
    }
    else if (shift > 0) {
      shift -= nbits;
      j_start++;
    }
    else { /* shift < 0 */
      shift += maxbits;
      i_start++;
    }

  } /* for (;;) */

  rc = j_max;

#ifdef DEBUG
  {
    char *env = getenv("KKMAX");
    int kkmax = env ? atoi(env) : 3;
    int k, kmax = MIN(i_max,kkmax);
    fprintf(stderr,"vUNpack_bits_: in j_max=%d, out i_max=%d words [kmax=%d]\n",j_max,i_max,kmax);
    for (k=0; k<kmax; k++) {
      extern char *bit_pattern(unsigned int mask);
      char *c = bit_pattern(packed[k]);
      fprintf(stderr,"\tunpacked[%4d] = %12u, packed[%4d] = %12u : %s\n",k,unpacked[k],k,packed[k],c);
      FREE(c);
    }
  }
#endif

 finish:
  *retcode = rc;
}
