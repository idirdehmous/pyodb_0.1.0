/* bits.c */

#include <stdio.h>
#include "alloc.h"
#include "bits.h"
#include "swapbytes.h"

/* direct bit stream manipulation */

/* 
   The following array bitcnt[] contains the number of bits set (=1) 
   for each unsigned char (=byte) in range between 0 and 255.
   For reference, the following piece of (Fortran90!) code was used to generated this table :

   program bitcnt
   implicit none
   integer j
   integer numbits
   do j=0,255
     if (mod(j,16) == 0) print *,' '
     write(*,'(i2,",")',advance='no') numbits(j)
   enddo
   end program bitcnt
   
   function numbits(j)
   implicit none
   integer i,j,tmp,numbits
   numbits = 0
   tmp = j
   do i=1,8
     if (iand(tmp,1) == 1) numbits = numbits + 1
     tmp = tmp/2
   enddo
   end function numbits

*/

#if defined(VPP) || defined(NECSX)
/* Preserve chances for vectorization */
static const int 
#else
static const unsigned char
#endif
bitcnt[256] = {
  0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};

#define GETCOUNT(t, Nbw, ptr, offset, Nbits) \
  const t *ww = ptr; \
  int Offset = offset; \
  const t one = 0x1; \
  int j; \
  for (j=0; j<Nbits; j++) { \
    int wordno = (j + Nbw)/Nbw - 1; \
    int shft  = Nbw - 1 - j%Nbw; \
    if ((ww[Offset+wordno] >> shft) & one) cnt++; \
  } /* for (j=0; j<Nbits; j++) */ \

#define GETCOUNT_PRE(t, Nbw, ptr, Nbits) \
  const t *w = ptr; \
  const t one = 0x1; \
  int j; \
  int modulo = Nbits%Nbw; \
  for (j=0; j<Nbits-modulo; j+=Nbw) { \
    int wordno = (j + Nbw)/Nbw - 1; \
    int shft  = Nbw - 1 - j%Nbw

/* CNT8OPT works *ONLY* with byte-data or big-endian non-byte data */
#define CNT8OPT cnt += bitcnt[w[wordno]]

#define CNT8 \
    if ((w[wordno] >> shft--) & one) cnt++; if ((w[wordno] >> shft--) & one) cnt++; \
    if ((w[wordno] >> shft--) & one) cnt++; if ((w[wordno] >> shft--) & one) cnt++; \
    if ((w[wordno] >> shft--) & one) cnt++; if ((w[wordno] >> shft--) & one) cnt++; \
    if ((w[wordno] >> shft--) & one) cnt++; if ((w[wordno] >> shft--) & one) cnt++

#define CNT16 CNT8; CNT8
#define CNT32 CNT16; CNT16
#define CNT64 CNT32; CNT32

#define GETCOUNT_POST(t, Nbw, ptr, Nbits) \
  } /* for (j=0; j<Nbits-modulo; j+=Nbw) */ \
  if (modulo > 0) { GETCOUNT(t, Nbw, w, (Nbits-modulo)/Nbw, modulo); }

#define GETCOUNT_08(t, Nbw, ptr, Nbits) \
        GETCOUNT_PRE(t, Nbw, ptr, Nbits); \
        CNT8OPT; GETCOUNT_POST(t, Nbw, ptr, Nbits)

#define GETCOUNT_16(t, Nbw, ptr, Nbits) \
        GETCOUNT_PRE(t, Nbw, ptr, Nbits); \
        CNT16; GETCOUNT_POST(t, Nbw, ptr, Nbits)

#define GETCOUNT_32(t, Nbw, ptr, Nbits) \
        GETCOUNT_PRE(t, Nbw, ptr, Nbits); \
        CNT32; GETCOUNT_POST(t, Nbw, ptr, Nbits)

#define GETCOUNT_64(t, Nbw, ptr, Nbits) \
        GETCOUNT_PRE(t, Nbw, ptr, Nbits); \
        CNT64; GETCOUNT_POST(t, Nbw, ptr, Nbits)


int
ODBIT_get_count(const void *data, int nbits, int nbw)
{
  int cnt = 0;
  if (nbits > 0) {
    switch (nbw) {
    case  8: { GETCOUNT_08(unsigned char         , 8,data,nbits) ; } break;
    case 16: { GETCOUNT_16(unsigned short int    ,16,data,nbits); } break;
    case 32: { GETCOUNT_32(unsigned int          ,32,data,nbits); } break;
    case 64: { GETCOUNT_64(unsigned long long int,64,data,nbits); } break;
    }
  }
  return cnt;
}


void 
codbit_get_count_(const void *data,
		  const int *nbits,
		  const int *nbw,
		  int *rc)
{ /* Get the count of number of bits set to 1 */
  *rc = ODBIT_get_count(data, *nbits, *nbw);
}


#define GETIDX(t, Nbw) \
case Nbw: { \
  const t *w = data; \
  const t one = 0x1; \
  int j, Nbits = *nbits; \
  for (j=0; j<Nbits; j++) { \
    int wordno = (j + Nbw)/Nbw - 1; \
    int shft  = Nbw - 1 - j%Nbw; \
    /* No array bound checks in idx[] below; make sure *nidx is sufficient */ \
    if ((w[wordno] >> shft) & one) idx[cnt++] = j; /* C-indexing : [0 .. *nbits - 1] */ \
  } /* for (j=0; j<Nbits; j++) */ \
} break


void
codbit_getidx_(const void *data, 
	       const int *nbits,
	       const int *nbw,
	       unsigned int idx[], /* bit numbers [0 .. *nbits - 1] that are set to 1 */
	       const int *nidx, /* length of idx[], potentially >= *nbits */
	       int *rc) /* no. of bits set to 1 */
{ /* gets indices [0 .. *nbits - 1] of bits that are set to 1 */
  int cnt = 0;
  if (idx && *nidx > 0) {
    switch (*nbw) {
      GETIDX(unsigned char         ,  8);
      GETIDX(unsigned short int    , 16);
      GETIDX(unsigned int          , 32);
      GETIDX(unsigned long long int, 64);
    }
  }
  else {
    /* idx[] NULL-pointer or *nidx <= 0 ==> return count, as negative number */
    cnt = -ODBIT_get_count(data, *nbits, *nbw);
  }
  *rc = cnt;
}


unsigned int *
ODBIT_getidx(const void *data, int nbits, int nbw, int *idxlen)
{
  int cnt = ODBIT_get_count(data, nbits, nbw);
  unsigned int *idx = NULL;
  ALLOC(idx, cnt);
  codbit_getidx_(data, &nbits, &nbw, idx, &cnt, idxlen);
  return idx; /* A potential memory leakage; must be released via ODBIT_free(idx) */
}


void
ODBIT_free(void *v)
{
  FREE(v); /* just to use consistent set of ALLOC & FREE */
}


#define GETBIT(t, Nbw) \
case Nbw: { \
  const t *w = data; \
  int wordno = (bitno + Nbw)/Nbw - 1; \
  int shft  = Nbw - bitno%Nbw - 1; \
  rc = (w[wordno] >> shft) & 0x1; \
} break


int
ODBIT_get(const void *data, int nbits, int nbw, int bitno)
{
  int rc = -1;
  if (nbits > 0 && bitno >= 0 && bitno < nbits) {
    switch (nbw) {
      GETBIT(unsigned char         ,  8);
      GETBIT(unsigned short int    , 16);
      GETBIT(unsigned int          , 32);
      GETBIT(unsigned long long int, 64);
    }
  }
  return rc; /* Successful returns are 0 or 1 ; out of bit-range returns -1 */
}


void 
codbit_get_(const void *data, 
	    const int *nbits,
	    const int *nbw,
	    const int *bitno, /* C-indexing : between [0 .. *nbits - 1] */
	    int *rc)
{
  *rc = ODBIT_get(data, *nbits, *nbw, *bitno);
}


int
ODBIT_test(const void *data, int nbits, int nbw, int bitno1, int bitno2)
{ /* 
     Returns  1, if ALL bits in range [bitno1,bitno2] are set 
     Returns  0, if ALL bits in range [bitno1,bitno2] are NOT set
     Returns -1, if when the first bit in the range happens to go out of bounds
   */
  int j, rctot = 0, n = bitno2 - bitno1 + 1;
  int rc = 0;
  for (j=bitno1; j<=bitno2; j++) {
    rc = ODBIT_get(data, nbits, nbw, j); /* Can be 1, 0 or -1 */
    if (rc == 1) {
      rctot++; 
    }
    else {
      /* Note: rc == -1 counts as out-of-bounds */
      break;
    }
  } 
  return (n > 0 && rctot == n) ? 1 : rc;
}


char *
ODBIT_getmap(const void *data, int nbits, int nbw, char *s)
{ /* 
     Returns a character-string s[] of length nbits+1 (= its minimum length)
     containing a stream of 0's and 1's (left-to-right) for printing etc. purposes 
     '-' means bit tested was out of its range
  */
  if (!s) ALLOC(s, nbits+1); /* A potential memory leak; use ODBIT_free(s) to release the space */
  if (s) {
    char *p = s;
    int j;
    for (j=0; j<nbits; j++) {
      int test = ODBIT_test(data, nbits, nbw, j, j);
      *p++ = (test == 1) ? '1' : ((test == 0) ? '0' : '-');
    }
    *p = '\0';
  }
  return s; 
}


#define SETBIT(t, Nbw, One) \
case Nbw: { \
  int j; \
  t *w = data; \
  for (j=bitno1; j<=bitno2; j++) { \
    int wordno = (j + Nbw)/Nbw - 1; \
    int shft  = Nbw - j%Nbw - 1; \
    t mask = (One << shft); \
    w[wordno] |= mask; \
  } \
} break


void
ODBIT_set(void *data, int nbits, int nbw, int bitno1, int bitno2)
{ /* sets bit number(s) in range [bitno1,bitno2] to 1 */
  if ( nbits >  0 && bitno1 <= bitno2 &&
      bitno1 >= 0 && bitno1 <  nbits &&
      bitno2 >= 0 && bitno2 <  nbits) {
    switch (nbw) {
      SETBIT(unsigned char         ,  8, 1U);
      SETBIT(unsigned short int    , 16, 1U);
      SETBIT(unsigned int          , 32, 1U);
      SETBIT(unsigned long long int, 64, 1ull);
    }
  }
}


void 
codbit_set_(void *data, 
	    const int *nbits,
	    const int *nbw,
	    const int *bitno1,
	    const int *bitno2) /* C-indexing : between [0 .. *nbits - 1] */
{
  ODBIT_set(data, *nbits, *nbw, *bitno1, *bitno2);
}


void
codbit_setidx_(void *data, 
	       const int *nbits,
	       const int *nbw,
	       const unsigned int idx[], /* the bit numbers [0 .. *nbits - 1] that will be set to 1 */
	       const int *nidx) /* length of idx[], potentially >= *nbits */
{
  if (idx && *nidx > 0) {
    int j, Nidx = *nidx;
    for (j=0; j<Nidx; j++) ODBIT_set(data, *nbits, *nbw, idx[j], idx[j]);
  }
}

#define UNSETBIT(t, Nbw, One) \
case Nbw: { \
  int j; \
  t *w = data; \
  for (j=bitno1; j<=bitno2; j++) { \
    int wordno = (j + Nbw)/Nbw - 1; \
    int shft  = Nbw - j%Nbw - 1; \
    t mask = (One << shft); \
    w[wordno] &= ~mask; \
  } \
} break


void
ODBIT_unset(void *data, int nbits, int nbw, int bitno1, int bitno2)
{ /* sets bit number(s) in range [bitno1,bitno2] to 0 */
  if ( nbits >  0 && bitno1 <= bitno2 &&
      bitno1 >= 0 && bitno1 <  nbits &&
      bitno2 >= 0 && bitno2 <  nbits) {
    switch (nbw) {
      UNSETBIT(unsigned char         ,  8, 1U);
      UNSETBIT(unsigned short int    , 16, 1U);
      UNSETBIT(unsigned int          , 32, 1U);
      UNSETBIT(unsigned long long int, 64, 1ull);
    }
  }
}


void 
codbit_unset_(void *data, 
	      const int *nbits,
	      const int *nbw,
	      const int *bitno1,
	      const int *bitno2) /* C-indexing : between [0 .. *nbits - 1] */
{
  ODBIT_unset(data, *nbits, *nbw, *bitno1, *bitno2);
}


void
codbit_unsetidx_(void *data, 
		 const int *nbits,
		 const int *nbw,
		 const unsigned int idx[], /* the bit numbers [0 .. *nbits - 1] that will be set to 0 */
		 const int *nidx) /* length of idx[], potentially >= *nbits */
{
  if (idx && *nidx > 0) {
    int j, Nidx = *nidx;
    for (j=0; j<Nidx; j++) ODBIT_unset(data, *nbits, *nbw, idx[j], idx[j]);
  }
}


#define AND_OR(t,Nbw,One,op1,op2) \
case Nbw: { \
  t *wout = out_data; \
  const t *win = in_data; \
  int nw = (*nbits)/Nbw; \
  int nrembits = (*nbits)%Nbw; \
  int j; \
  /* Full words first */ \
  for (j=0; j<nw; j++) wout[j] op1 win[j]; /* Nbw-bits at a time */ \
  if (nrembits > 0) { /* Remainder of the bits */ \
    t *last_word_out = &wout[nw]; \
    const t *last_word_in = &win[nw]; \
    t value = 0; \
    const t one = 0x1; \
    for (j=0; j<nrembits; j++) { \
      int shft = Nbw - 1 - j%Nbw; \
      if ( (((*last_word_out) >> shft) & 0x1) op2 \
	   (((*last_word_in ) >> shft) & 0x1) ) { \
	t mask = (One << shft); \
	value |= mask; \
      } \
    } /* for (j=0; j<nrembits; j++) */ \
    for (j=nrembits; j<Nbw; j++) { \
      /* Keep bits between [nrembits .. Nbw-1] unchanged in the last word */ \
      int shft = Nbw - 1 - j%Nbw; \
      t mask = (One << shft); \
      value |= ((*last_word_out) & mask); \
    } \
    *last_word_out = value; \
  } /* if (nrembits > 0) */ \
} break


void 
codbit_and_(void *out_data, 
	    const void *in_data,
	    const int *nbits,
	    const int *nbw)
{ /* BITAND of two bit-vectors of *nbits bit-elements */
  if (*nbits > 0) {
    switch (*nbw) {
      AND_OR(unsigned char         , 8,1U  ,&=,&&);      
      AND_OR(unsigned short int    ,16,1U  ,&=,&&);      
      AND_OR(unsigned int          ,32,1U  ,&=,&&);      
      AND_OR(unsigned long long int,64,1ull,&=,&&);      
    }
  }
}


void 
codbit_or_(void *out_data, 
	   const void *in_data,
	   const int *nbits,
	   const int *nbw)
{ /* BITOR of two bit-vectors of *nbits bit-elements */
  if (*nbits > 0) {
    switch (*nbw) {
      AND_OR(unsigned char         , 8,1U  ,|=,||);      
      AND_OR(unsigned short int    ,16,1U  ,|=,||);      
      AND_OR(unsigned int          ,32,1U  ,|=,||);      
      AND_OR(unsigned long long int,64,1ull,|=,||);      
    }
  }
}

/* Leading zero counts */

/*
  The following table contains the count of leading 
  zeros in a 4-bit word
*/

#if defined(VPP) || defined(NECSX)
/* Preserve chances for vectorization */
static const int 
#else
static const unsigned char
#endif
lzctbl[16] = {
  4, 3, 2, 2, 1, 1, 1, 1,
  0, 0, 0, 0, 0, 0, 0, 0
};


#define DO_LZC(type, n, byteswap_macro) \
case n: \
{ \
  const type *u = data; \
  type x = iam_little_endian ? byteswap_macro(*u) : (*u); \
  int shift = n - 4; \
  for (; shift >= 0; shift -= 4) { \
    int cnt = lzctbl[(x>>shift) & 0xF]; \
    lzc += cnt; \
    if (cnt != 4) break; \
  } \
} break


int
ODBIT_lzc(const void *data, int nbw)
{ /* Counts the number of leading zero bits in a {8,16,32,64}-bit word*/
  int lzc = 0;
  const unsigned int ulbtest = 0x12345678;
  const unsigned char *clbtest = (const unsigned char *)&ulbtest;
  int iam_little_endian = (*clbtest == 0x78);
  switch (nbw) {
    DO_LZC(unsigned char         ,  8, bswap8bits );
    DO_LZC(unsigned short int    , 16, bswap16bits);
    DO_LZC(unsigned int          , 32, bswap32bits);
    DO_LZC(unsigned long long int, 64, bswap64bits);
  }
  return lzc;
}

void
codbit_lzc_(const void *data, 
	    const int *nbw, 
	    int *lzc)
{
  /* Fortran callable lzc */
  if (lzc) {
    *lzc = (data && nbw) ? ODBIT_lzc(data, *nbw) : 0;
  }
}
