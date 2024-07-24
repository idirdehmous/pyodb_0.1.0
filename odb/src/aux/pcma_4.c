#ifdef RS6K
/* Until we solve packing method#4 problems with -O2, disable optimization on IBMs */
#pragma options noopt
#endif

/*=== Packing method#4 ===*/

#include "pcma.h"

#define REPBITS_DEFAULT      7
#define REPLIM(nb)        (1<<(nb))

#define MASK64(x,nbits)  \
  ( ((nbits) < 64) ? ((x) & ((1ull << (nbits)) - 1)) : ((x) & 0xffffffffffffffffull) )

#define SIGNBIT64(x)  ( ((x)>>63) & 0x1 )
#define SIGN2LSB64(x) ( ((x)<<1) | SIGNBIT64(x) )
#define LSB642SIGN(x) ( ((x)>>1) | (((x) & 0x1)<<63) )

#define SIGNBIT32(x)  ( ((x)>>31) & 0x1 )
#define SIGN2LSB32(x) ( ((x)<<1) | SIGNBIT32(x) )

#define MAXINT   2147483647
#define MININT  (-MAXINT-1)

#define TRUNC(x) ((int)(x))

#define PACK64(u,nb)   pack64bits(u,nb,packbuf,packbuf_maxlen,&packbuf_offset,&bitpos,__LINE__)
#define UNPACK64(u,nb) unpack64bits(&u,nb,packbuf,packbuf_maxlen,&packbuf_offset,&bitpos,1,swp)
#define UNPACK32(u,nb) unpack32bits(&u,nb,packbuf,packbuf_maxlen,&packbuf_offset,&bitpos,swp)

typedef union {
    double d;
    u_ll_t llu;
} alias_t;

#define GENBITS(nb,utype) \
static char * \
genbits##nb(utype z, int maxlsbbits) \
{ \
  static char s[nb+1]; \
  int j; \
  s[nb] = 0; \
  for (j=nb-1; j>=0; j--) { \
    s[j] = (z & 0x1) ? '1' : '0'; \
    z >>= 1; \
  } \
  if (maxlsbbits < nb) { \
    int offset = nb-maxlsbbits; \
    for (j=0; j<offset; j++) s[j] = ' '; \
    for (j=0; j<maxlsbbits; j++) s[j] = s[j+offset]; \
    s[maxlsbbits] = 0; \
  } \
  return s; \
}

GENBITS(64,u_ll_t)

static int
pack64bits(u_ll_t u, int nb,
	   u_ll_t packbuf[], int packbuf_maxlen,
	   int *packbuf_offset, int *bitpos,
	   int line)
{
#if 0
  char *s;
#endif
  int bpos = *bitpos;
  u_ll_t *cur = &packbuf[*packbuf_offset]; /* Add array bound checks later on */
  int spill = 0;
  if (*packbuf_offset == 0 && bpos == 0) {
    *cur = 0; /* Add array bound checks later on */
  }
  if (bpos == 64) {
    *++cur = 0; /* Add array bound checks later on */
    bpos = 0;
  }
#if 0
  s = genbits64(u,nb);
  fprintf(stderr,"%4d) pk64<%d>: %20llu : %*s%s : nb=%d, bpos=%d, spill=%d\n",
	  __LINE__,line,u,64-nb,"",s,nb,bpos,spill);
#endif
  if (nb < 64) u = MASK64(u,nb); /* Just a precaution */
  spill = bpos + nb - 64;
#if 0
  s = genbits64(u,nb);
  fprintf(stderr,"%4d) pk64(): %20llu : %*s%s : nb=%d, bpos=%d, spill=%d\n",
	  __LINE__,u,64-nb,"",s,nb,bpos,spill);
#endif
  if (spill > 0) { 
    /* u has to be split between cur[0] & cur[1] */
    u_ll_t umsb = (u >> spill);
    u_ll_t ulsb = MASK64(u,spill);

#if 0
    s = genbits64(umsb,nb-spill);
    fprintf(stderr,"%4d)  umsb: %20llu : %*s%s : nb-spill=%d\n",
	    __LINE__,umsb,64-(nb-spill),"",s,nb-spill);
#endif

    *cur |= umsb; /* cur[0] */

#if 0
    s = genbits64(*cur,64);
    fprintf(stderr,"%4d) cur[0]: %20llu : %s :\n",
	    __LINE__,*cur,s);
    s = genbits64(ulsb,spill);
    fprintf(stderr,"%4d)  ulsb: %20llu : %s%*s :\n",
	    __LINE__,umsb,s,64-spill,"");
#endif

    *++cur = (ulsb << (64-spill)); /* cur[1]; Add array bound checks later on */
    bpos = spill;

#if 0
    s = genbits64(*cur,64);
    { int j; for (j=bpos; j<64; j++) s[j] = '-'; }
    fprintf(stderr,"%4d) cur[1]: %20llu : %s : bpos=%d\n",
	    __LINE__,*cur,s,bpos);
#endif
  }
  else {
    /* u fully fits into current cur[0] */
    *cur |= (u << (-spill));
    bpos += nb;

#if 0
    s = genbits64(*cur,64);
    { int j; for (j=bpos; j<64; j++) s[j] = '-'; }
    fprintf(stderr,"%4d)  *cur: %20llu : %s : nb=%d, bpos=%d\n",
	    __LINE__,*cur,s,nb,bpos);
#endif
  }
  *packbuf_offset = cur - packbuf;
  *bitpos = bpos;
  return nb;
}

static int
zc64(u_ll_t u, int limit)
{
  int lzc = 0;
  u_ll_t mask = 0x8000000000000000ull; /* 1000 0000 0000 0000 0000 0000 0000 0000 
					  0000 0000 0000 0000 0000 0000 0000 0000 */
  if (SIGNBIT64(u)) u = ~u; /* Reverse bits */
  lzc = 0;
  while (lzc < limit && u < mask) {
    lzc++;
    mask >>= 1;
  }
  if (lzc < 7 || lzc > 21) lzc = 1;
  return lzc;
}

static int
zc32(unsigned int u, int limit)
{
  int lzc = 0;
  unsigned int mask = 0x80000000U; /* 1000 0000 0000 0000 0000 0000 0000 0000 */
  if (SIGNBIT32(u)) u = ~u; /* Reverse bits */
  lzc = 0;
  while (lzc < limit && u < mask) {
    lzc++;
    mask >>= 1;
  }
  return lzc;
}

static int 
opt_repbits(const u_ll_t u[], int n)
{
  int repbits = 1;
  if (n > 0) {
    int j;
    int repmax = 0;
    int repcnt = 0;
    u_ll_t lastu = 0;
    int replim = n;
    for (j=0; j<n; j++) {
      if (u[j] == lastu) {
	if (++repcnt == replim) { /* limit reached */
	  if (repcnt > repmax) repmax = repcnt;
	  repcnt = 0;
	}
      }
      else {
	if (repcnt > 0) {
	  if (repcnt > repmax) repmax = repcnt;
	  repcnt = 0;
	}
      }
      lastu = u[j];
    }
    if (repcnt > 0) {
      if (repcnt > repmax) repmax = repcnt;
      repcnt = 0;
    }
    if (repmax > 1) {
      while (REPLIM(repbits) < repmax) repbits++;
    }
  }
  return repbits;
}

static u_ll_t *
pcma_4(const double x[], int xmax, int *packbuf_len)
{
  int m = xmax;
  int packbuf_maxlen = 1 + 2 * m;
  int packbuf_offset = 0;
  u_ll_t *packbuf = NULL;
  int bitpos = 0;
  alias_t pk;
  u_ll_t u;
  int thesame, winner, lzc, nbw, numbits;
  u_ll_t ulast, lastu, pkword;
  int repcnt = 0;
  int repbits = REPBITS_DEFAULT;
  int replim = REPLIM(repbits);
  int repmax = 0;
  int j;
  u_ll_t outbits = 0;

  ALLOC(packbuf, packbuf_maxlen);
  if (packbuf_len) *packbuf_len = 0;

  pk.d = x[0];
  u = pk.llu;
  repbits = opt_repbits(&u, MIN(m, replim));
  replim = REPLIM(repbits);

  { /* Pack input data block length (m) & no. of replication bits */
    u_ll_t v = m;
    v <<= 1;
    if (m < 0) v++; /* Never true though; just for consistency */
    lzc = zc32(v,32);
    nbw = 32 - lzc + 1;
    numbits = 1 + 1 + 5 + nbw + 6;
    thesame = 0;
    winner = 1;
    pkword = (((u_ll_t)winner  << (5+nbw+6)) | 
	      ((u_ll_t)(lzc-1) << (  nbw+6)) | 
	      (MASK64(v,nbw)   <<        6 ) | 
	      (repbits-1));
#if 0
    {
      char *s = genbits64(pkword,numbits);
      fprintf(stderr,"%4d) pcma_4(xmax=%d, m=%d, winner=%d, lzc=%d, v=%llu, repbits=%d)\n",
	      __LINE__,xmax, m, winner, lzc, v, repbits);
      fprintf(stderr,"\tnbw=%d, numbits=%d\n",nbw,numbits);
      fprintf(stderr,"pkword=%20llu : %s\n",pkword,s);
    }
#endif
    /* No. of bits (=numbits) guaranteed to be <= 64 */
    outbits += PACK64(pkword,numbits);
  }

  ulast = 0; /* Last u with sign-bit moved to least significant */
  lastu = 0; /* True last u */
  repcnt = 0;

  for (j=0; j<m; j++) {
    double d = (x[j] == 0) ? 0 : x[j]; /* Since sign-bit may have been set */
    alias_t xx;
    u_ll_t uj;

    xx.d = d;
    uj = SIGN2LSB64(xx.llu);

#if 0
    fprintf(stderr,"%4d) x[%d] = %.20g ; d = %.20g, %.20g ; uj=%llu ; xx.llu=%llu\n",
	    __LINE__,j,x[j],d,xx.d,uj,xx.llu);
#endif
    
    if (xx.llu == lastu) {
      /* Replicated word */
      if (++repcnt == 1) {
	thesame = 1;
	outbits += PACK64(thesame,1);
	numbits = 1;
      }
      else if (repcnt == replim) { /* i.e. 2^repbits-bits reached */
	outbits += PACK64(repcnt-1,repbits);
	if (repcnt > repmax) repmax = repcnt;
	numbits = repbits;
	repcnt = 0;
      }
      else {
	numbits = 0;
      }
    }
    else {
      if (repcnt > 0) { /* Flush replication counter */
	outbits += PACK64(repcnt-1,repbits);
	if (repcnt > repmax) repmax = repcnt;
	repcnt = 0;
      }

      if (d <= MAXINT && d >= MININT && d == TRUNC(d)) {
	/* Word is a 32-bit integer */
	u_ll_t v = ABS(d);
	v <<= 1;
	if (d < 0) v++;
	lzc = zc32(v,32);
	nbw = 32 - lzc + 1;
	numbits = 1 + 1 + 5 + nbw;
	thesame = 0;
	winner = 1;
	pkword = (((u_ll_t)winner << (5+nbw)) | (((u_ll_t)lzc-1) << nbw) | MASK64(v,nbw));
	/* No. of bits (= numbits) guaranteed to be <= 64 */
#if 0
	fprintf(stderr,"%4d) --> winner=%d: numbits=%d, nbw=%d, lzc_eff=%d\n", 
		__LINE__,winner, numbits, nbw, lzc-1);
#endif
	outbits += PACK64(pkword,numbits);
      }
      else {
	int lzc_eff;
	u_ll_t diff = uj ^ ulast;
	lzc = zc64(diff,32);
	nbw = 64 - lzc + 1;
	numbits = 1 + 1 + 4 + nbw;
	lzc_eff = (lzc == 1) ? 0 : lzc-6;
	thesame = 0;
	winner = 0;
#if 0
	fprintf(stderr,"%4d) --> winner=%d: numbits=%d, nbw=%d, lzc_eff=%d, uj=%llu, ulast=%llu\n", 
		__LINE__, winner, numbits, nbw, lzc_eff, uj, ulast);
#endif
	/* May need two packs, since no. of bits (= numbits) may go beyond 64 */
	if (numbits > 64) {
	  outbits += PACK64(lzc_eff,6); /* thesame=0 + winner=0 + 4-bits of lzc_eff == 6 bits */


	  outbits += PACK64(diff,nbw);
	}
	else {
	  pkword = ( ((u_ll_t)lzc_eff << nbw) | MASK64(diff,nbw));
	  outbits += PACK64(pkword,numbits);
	}
      }
    }
      
    ulast = uj;
    lastu = xx.llu;
  } /* for (j=0; j<m; j++) */
    
  if (repcnt > 0) {  /* Flush replication counter */
    outbits += PACK64(repcnt-1,repbits);
    if (repcnt > repmax) repmax = repcnt;
    repcnt = 0;
  }

  {
    u_ll_t *cur = &packbuf[packbuf_offset]; /* Add array bound checks later on */
    if (bitpos > 0) {
      cur++;
      packbuf_offset = cur - packbuf;
    }
  }

  if (packbuf_len) *packbuf_len = packbuf_offset;
  return packbuf;
}

static int
unpack64bits(u_ll_t *u, int nb,
	     const u_ll_t packbuf[], int packbuf_maxlen,
	     int *packbuf_offset, int *bitpos,
	     int fillprefix, int swp)
{
  int bpos = *bitpos;
  const u_ll_t *cur = &packbuf[*packbuf_offset]; /* Add array bound checks later on */
  int spill;
  u_ll_t cur0;
  if (bpos == 64) {
    ++cur; /* Add array bound checks later on */
    bpos = 0;
  }
  *u = 0;
  cur0 = swp ? bswap64bits(cur[0]) : cur[0];
  if (fillprefix && nb > 0 && nb < 64) {
    u_ll_t sign = (((cur0) << bpos) >> 63) & 0x1;
    if (sign) *u = (0xffffffffffffffffull << (nb-1));
  }
  spill = bpos + nb - 64;
  if (spill > 0) { 
    /* bit pattern has been split between cur[0] & cur[1] */
    /* Add array bound checks later on */
    u_ll_t umsb = MASK64(cur0,nb-spill);
    u_ll_t cur1 = swp ? bswap64bits(cur[1]) : cur[1];
    u_ll_t ulsb = (cur1 >> (64-spill));
    *u |= ((umsb << spill) | ulsb);
#if 0
    {
      char *s = genbits64(cur0,64);
     { int j; for (j=0; j<bpos; j++) s[j] = '-'; }
     fprintf(stderr,"%4d)  cur0: %20llu : %s : nb=%d, bpos=%d, spill=%d\n",
	     __LINE__,cur0,s,nb,bpos,spill);
    }
    {
      char *s = genbits64(cur1,64);
      { int j; for (j=spill; j<64; j++) s[j] = '-'; }
      fprintf(stderr,"%4d)  cur1: %20llu : %s : nb=%d, bpos=%d, spill=%d\n",
	      __LINE__,cur1,s,nb,bpos,spill);
    }
#endif
    bpos = spill;
    ++cur; /* Add array bound checks later on */
  }
  else {
    /* the current cur[0] fully accomocates u */
    u_ll_t utmp = (cur0 >> (-spill));
    *u |= MASK64(utmp,nb);
#if 0
    {
      char *s = genbits64(*cur,64);
      { int j; for (j=0; j<bpos; j++) s[j] = '-'; }
      { int j; for (j=bpos+nb; j<64; j++) s[j] = '-'; }
      fprintf(stderr,"%4d)  *cur: %20llu : %s : nb=%d, bpos=%d, spill=%d\n",
	      __LINE__,*cur,s,nb,bpos,spill);
    }
#endif
    bpos += nb;
  }
  *packbuf_offset = cur - packbuf;
  *bitpos = bpos;
#if 0
  {
    char *s = genbits64(*u,64);
    { int j; for (j=0; j<64-nb; j++) s[j] = '-'; }
    fprintf(stderr,"%4d)  *u  > %20llu : %s : nb=%d  (bpos now = %d)\n",
	    __LINE__,*u,s,nb,bpos);
  }
#endif
  return nb;
}

static int
unpack32bits(int *i, int nb,
	     const u_ll_t packbuf[], int packbuf_maxlen,
	     int *packbuf_offset, int *bitpos,
	     int swp)
{
  u_ll_t u;
  int numbits;
  numbits = unpack64bits(&u, nb,
			 packbuf, packbuf_maxlen,
			 packbuf_offset, bitpos, 0, swp);
  *i = u;
  return numbits;
}

static int
upcma_4(double x[], int xmax,
	const u_ll_t packbuf[], int packbuf_maxlen,
	int swp)
{
  int j;
  int m = xmax;
  int packbuf_offset = 0;
  int bitpos = 0;
  u_ll_t ulast = 0;
  double lastdbl = 0;
  u_ll_t *u = (u_ll_t *)x;
  int repcnt = 0;
  int repbits = REPBITS_DEFAULT;
  u_ll_t inbits = 0;

#if 0
  fprintf(stderr,"%4d) upcma_4(xmax=m=%d, packbuf_maxlen=%d, swp=%d)\n",
	  __LINE__,xmax,packbuf_maxlen,swp);
#endif

  for (j = -1; j<m; j++) {
    int winner, lzc, nbw, thesame;
    if (repcnt > 0) {
      thesame = 1;
    }
    else {
      inbits += UNPACK32(thesame,1);
#if 0
      fprintf(stderr,"%4d) j=%d ; thesame = %d\n",__LINE__,j,thesame);
#endif
    }
    if (thesame == 0) {
      u_ll_t z;
      inbits += UNPACK32(winner,1);
#if 0
      fprintf(stderr,"%4d) --> winner = %d\n",__LINE__,winner);
#endif
      if (winner == 0) {
	inbits += UNPACK32(lzc,4);
#if 0
	fprintf(stderr,"\t%4d) --> lzc = %d\n",__LINE__,lzc);
#endif
	lzc = (lzc == 0) ? lzc+1 : lzc+6;
	nbw = 64 - lzc + 1;
	inbits += UNPACK64(z,nbw);
#if 0
	{
	  char *s = genbits64(z,nbw);
	  fprintf(stderr,"\t%4d) --> lzc = %d , nbw = %d : %20llu : %s\n",
		  __LINE__,lzc, nbw, z, s);
	}
#endif
	z ^= ulast;
	ulast = z;
      }
      else {
	inbits += UNPACK32(lzc,5);
#if 0
	fprintf(stderr,"\t%4d) --> lzc = %d\n",__LINE__,lzc);
#endif
	lzc++;
	nbw = 32 - lzc + 1;
	inbits += UNPACK64(z,nbw);
#if 0
	{
	  char *s = genbits64(z,nbw);
	  fprintf(stderr,"\t%4d) --> lzc = %d, nbw = %d : %20llu : %s\n",
		  __LINE__, lzc, nbw, z, s);
	}
#endif
	z = MASK64(z,32);
	ulast = z;
      }
      if (j == -1) { /* Determine "m" */
	m = (ulast >> 1);
	if (ulast & 0x1) m = -m;
#if 0
	fprintf(stderr,"%4d) ==> ulast = %llu, m=%d, xmax=%d\n",
		__LINE__,ulast,m,xmax);
#endif
	m = MIN(m,xmax);
	ulast = 0; /* Important !! */
	/* no. of replication bits in this dataset */
	inbits += UNPACK32(repbits,6);
	repbits++;
#if 0
	fprintf(stderr,"%4d) ==> repbits = %d\n", __LINE__, repbits);
#endif
      }
      else  {
	if (winner == 0) { /* Value was a xor-red "difference" */
	  u[j] = LSB642SIGN(ulast); /* Implicitly alters x[j], too */
	}
	else { /* Value is a 32-bit integer */
	  if (ulast == 1) 
	    x[j] = MININT; /* A special case */
	  else {
	    x[j] = (ulast >> 1);
	    if (ulast & 0x1) x[j] = -x[j];
	  }
	  ulast = SIGN2LSB64(u[j]);
	}
	lastdbl = x[j];
      }
    }
    else {
      if (repcnt > 0) {
	repcnt--;
      }
      else {
	UNPACK32(repcnt, repbits);
      }
      x[j] = lastdbl;
    }

#if 0
    if (j >= 0) {
      fprintf(stderr,"%4d) x[%d] = %.20g, lastdbl = %.20g : Remaining repcnt now %d\n",
	      __LINE__, j,x[j],lastdbl,repcnt);
    }
#endif
  }
  
#if 0
  if (repcnt > 0) fprintf(stderr,"REPCNT error !!!\n"); /* Should abort ??? (what if xmax < m ?) */
#endif
  
  return m;
}


int
pcma_4_driver(int method, /* ignored */
	      FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	      double nmdi,
	      double rmdi,
	           Packbuf *pbuf)
{
  int rc = 0;
  int count, nw, nwrt, chunk, replen;
  int total_count;
  int lenactive;
  unsigned int *packed_data = NULL;
  u_ll_t *pkbuf = NULL;
  int pkbuf_bytes = 0;
  int hdrlen = PCMA_HDRLEN;
  DRHOOK_START(pcma_4_driver);

  nwrt = 0;

  replen = lencma;
  chunk  = replen-1; /* Report w/o length information */

  count = hdrlen; /* 'PCMA' + method & 3 zero bytes + no_of_packed_bytes + 
		     no_of_unpacked  + 2 x double MDIs */

  if (replen > 1) {
    const int ratio = sizeof(*pkbuf)/sizeof(*packed_data);
    int pkbuf_len = 0;
    pkbuf = pcma_4(&cma[1], chunk, &pkbuf_len);
    pkbuf_bytes = pkbuf_len * sizeof(*pkbuf);
    count += pkbuf_len * ratio;
  }

  packed_data = pcma_alloc(pbuf, count);
  if (pkbuf && pkbuf_bytes > 0) {
    memcpy(&packed_data[hdrlen],pkbuf,pkbuf_bytes);
  }
  FREE(pkbuf);

  packed_data[0] = PCMA;
  packed_data[1] = 4 * MAXSHIFT + 0; /* zero no_of_unpacked means that 4th word, packed_data[3] contains
					report length; ==> no_of_unpacked can now be over 16megawords */
  packed_data[2] = pkbuf_bytes;
  packed_data[3] = replen;
  memcpy(&packed_data[4],&nmdi,sizeof(nmdi));
  memcpy(&packed_data[6],&rmdi,sizeof(rmdi));

  if (fp_out) nwrt = fwrite(packed_data, sizeof(*packed_data), count, fp_out);

  total_count = count;

  rc = total_count * sizeof(*packed_data);

  /* finish: */
  if (!pbuf) FREE(packed_data);

  DRHOOK_END(0);
  return rc;
}

int
upcma_4_driver(int method,
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
  DRHOOK_START(upcma_4_driver);

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

#if 0
  {
    fprintf(stderr,"\fupcma_4_driver(method=%d, swp=%d, can_swp_data=%d, new_version=%d, ...\n",
	   method, swp, can_swp_data, new_version);
    fprintf(stderr,"\tlen_packed_data=%d, msgbytes=%d, nmdi=%.20g, rmdi=%.20g, idxlen=%d, ...\n",
	   len_packed_data, msgbytes, nmdi, rmdi, idxlen);
    fprintf(stderr,"\tlencma=%d, fill_zeroth_cma=%d)\n",lencma, fill_zeroth_cma);
  }
#endif

  if (replen > 1) {
    int chunk  = replen - 1;
    int stacma = 0;
    int endcma = chunk;
    const u_ll_t *pkbuf = (const u_ll_t *)packed_data;
    int pkbuf_bytes = msgbytes;
    int pkbuf_len = pkbuf_bytes/sizeof(*pkbuf);
    double *cma_addr = &cma[fill_zeroth_cma];

#if 0
    fprintf(stderr,
	    "\t>chunk=%d, stacma=%d, endcma=%d, len_packed_data=%d, pkbuf_bytes=%d, pkbuf_len=%d\n",
	    chunk, stacma, endcma, len_packed_data, pkbuf_bytes, pkbuf_len);
#endif

    END_FIX();

#if 0
    fprintf(stderr,
	    "\t<chunk=%d, stacma=%d, endcma=%d, len_packed_data=%d, pkbuf_bytes=%d, pkbuf_len=%d\n",
	    chunk, stacma, endcma, len_packed_data, pkbuf_bytes, pkbuf_len);
#endif

    rc = upcma_4(cma_addr, endcma, pkbuf, pkbuf_len, swp);

    SWAP_DATA_BACKv2(swp,can_swap_data,cma,stacma,endcma);

    rc = (rc == endcma-stacma) ? replen : rc;
  }

  if (fp_out) nwrt = fwrite(cma, sizeof(double), lencma, fp_out);

  DRHOOK_END(0);
  return rc;
}
