#ifdef RS6K
#pragma options noextchk
#endif

#include "odb.h"


PUBLIC void 
codb_mask_control_word_(double vec[],
			const int *k1, /* Note : C-indexing */
			const int *k2, /* Note : C-indexing */
			const int *doffset,
			const int poolno[])
{
  ll_t *llvec = (ll_t *)vec;
  int k;
  int K1 = *k1;
  int K2 = *k2;
  int Doffset = *doffset;
  int kk = 0;
  
  for (k=K1; k<K2; k++) {
    if (poolno[kk] < 0) {
      llvec[Doffset + k] = 0x7fffffff7fffffffull; /* 0th & 32nd bit set to 0 */
    }
    kk++;
  }
}


PUBLIC void 
codb_put_control_word_(double vec[],
		       const int *k1, /* Note : C-indexing */
		       const int *k2, /* Note : C-indexing */
		       const int *doffset,
		       const int *poolno,
		       const int *noffset)
{
  ll_t *llvec = (ll_t *)vec;
  int k;
  int K1 = *k1;
  int K2 = *k2;
  int Doffset = *doffset;
  ll_t Poolno = *poolno;
  ll_t Noffset = *noffset;

  for (k=K1; k<K2; k++) {
    llvec[Doffset + k] = ((Poolno & 0x000000007fffffffull) << 32) + (k - K1) + Noffset;
  }
}

PUBLIC double
ODB_put_one_control_word(int k, int poolno)
{
  double ctrlw = 0;
  int k1 = k;
  int k2 = k+1;
  int doffset = -k;
  int noffset = k;
  codb_put_control_word_(&ctrlw, &k1, &k2, &doffset, &poolno, &noffset);
  return ctrlw;
}


PUBLIC void 
codb_get_control_word_(const double vec[],
		       const int *k1, /* Note : C-indexing */
		       const int *k2, /* Note : C-indexing */
		       const int *doffset,
		       ll_t ctrlw[])
{
  const ll_t *llvec = (const ll_t *)vec;
  int k;
  int K1 = *k1;
  int K2 = *k2;
  int Doffset = *doffset;
  int kk = 0;

  for (k=K1; k<K2; k++) {
    ctrlw[kk++] = llvec[Doffset + k];
  }
}


PUBLIC void
codb_get_pool_count_(const double vec[],
		     const int *k1, /* Note : C-indexing */
		     const int *k2, /* Note : C-indexing */
		     const int *doffset,
		     const int *npools,
		     const int poolno[],
		     int *retcode)
{
  const ll_t *llvec = (const ll_t *)vec;
  int k, jp;
  int K1 = *k1;
  int K2 = *k2;
  int Doffset = *doffset;
  int Npools = *npools;
  int count = 0;

  for (jp=0; jp<Npools; jp++) {
    ll_t ref_poolno = poolno[jp];
    for (k=K1; k<K2; k++) {
      ll_t this_poolno = ((llvec[Doffset+k] >> 32) & 0x000000007fffffffull);
      if (this_poolno == ref_poolno) {
	/* Count those which belong to this pool */
	count++;
      }
    } /* for (k=K1; k<K2; k++) */
  } /* for (jp=0; jp<Npools; jp++) */

  *retcode = count;
}


PUBLIC void
codb_get_poolnos_(const double vec[],
		  const int *k1, /* Note : C-indexing */
		  const int *k2, /* Note : C-indexing */
		  const int *doffset,
		  int poolno_out[])
{
  const ll_t *llvec = (const ll_t *)vec;
  int k;
  int K1 = *k1;
  int K2 = *k2;
  int Doffset = *doffset;
  int jp = 0;

  for (k=K1; k<K2; k++) {
    ll_t this_poolno = ((llvec[Doffset+k] >> 32) & 0x000000007fffffffull);
    poolno_out[jp++] = this_poolno;
  }
}


PUBLIC void
codb_get_rownum_(const double vec[],
		 const int *k1, /* Note : C-indexing */
		 const int *k2, /* Note : C-indexing */
		 const int *doffset,
		 const int *noffset,
		 int rownum[])
{
  const ll_t *llvec = (const ll_t *)vec;
  int k;
  int K1 = *k1;
  int K2 = *k2;
  int Doffset = *doffset;
  ll_t Noffset = *noffset;
  int jp = 0;

  for (k=K1; k<K2; k++) {
    ll_t this_rownum = (llvec[Doffset+k] & 0x000000007fffffffull);
    rownum[jp++] = this_rownum - Noffset;
  }
}

PUBLIC void 
cmask32bits_(uint u[], const int *n, const int *nbits)
{
  int N = *n;
  int Nbits = *nbits; /* if > 0 mask-out last Nbits, else < 0 mask-out -Nbits first bits */
  int j;
  if (Nbits > 0 && Nbits <= 32) {
    for (j=0; j<N; j++) {
      u[j] >>= Nbits;
      u[j] <<= Nbits;
    }
  }
  else if (Nbits >= -32 && Nbits < 0) {
    Nbits = -Nbits;
    for (j=0; j<N; j++) {
      u[j] <<= Nbits;
      u[j] >>= Nbits;
    }
  }
}

PUBLIC void 
cmask64bits_(u_ll_t u[], const int *n, const int *nbits)
{
  int N = *n;
  int Nbits = *nbits; /* if > 0 mask-out last Nbits, else < 0 mask-out -Nbits first bits */
  int j;
  if (Nbits > 0 && Nbits <= 64) {
    for (j=0; j<N; j++) {
      u[j] >>= Nbits;
      u[j] <<= Nbits;
    }
  }
  else if (Nbits >= -64 && Nbits < 0) {
    Nbits = -Nbits;
    for (j=0; j<N; j++) {
      u[j] <<= Nbits;
      u[j] >>= Nbits;
    }
  }
}

PUBLIC void
ODB_Update_Hashmaxmin(int n, const uint h[])
{
  extern int ODB_hashmin, ODB_hashmax;
  {
    uint tmin = ODB_hashmin;
    uint tmax = ODB_hashmax;
    int i;
    for (i=0; i<n; i++) {
      if (tmin > h[i]) tmin = h[i];
      if (tmax < h[i]) tmax = h[i];
    }
    ODB_hashmin = tmin;
    ODB_hashmax = tmax;
  }
}

PUBLIC void
codb_vechash_(const int *nval,
	      const int *nlda,      /* the leading dimension of u ; >= Nval */
	      const int *nelem, 
	      const uint u[], /* actual size : Nlda x  Nelem */
	            uint h[])
{ 
  int i, j, k; 
  int Nval = *nval;
  int Nlda = *nlda;
  int Nelem = *nelem;
  uint Hashsize = ODB_Hashsize();

  if (Nelem <= 0) return;

  for (i=0; i<Nelem; i++) h[i] = 0;

  for (j=0; j<Nval; j++) {
    k = j;
    for (i=0; i<Nelem; i++) {
      h[i] = HASHFUNC(h[i], u[k], Hashsize);
      k += Nlda;
    }
  }

  for (i=0; i<Nelem; i++) h[i] %= Hashsize;

  ODB_Update_Hashmaxmin(Nelem, h);
}

PUBLIC boolean
ODB_uint_equal(const uint u1[], const uint u2[], int n)
{
  int j;
#if defined(VPP) || defined(NECSX)
  /* Vectorized */
  int cnt=0;
  for (j=0; j<n; j++) if (u1[j] != u2[j]) cnt++;
  return (cnt == 0) ? 1 : 0;
#else
  for (j=0; j<n; j++) if (u1[j] != u2[j]) return 0;
  return 1;
#endif
}


PUBLIC boolean
ODB_str_equal(const uchar u1[], const uchar u2[], int n)
{
  int j;
  /* Will not vectorize; character (1-byte) data type */
  for (j=0; j<n; j++) if (u1[j] != u2[j]) return 0;
  return 1;
}

/* Generic packing stuff */

#include "pcma_extern.h"

#define DECL_Type2DBL(visibility, func, type, usecast) \
visibility void func(double to[], const type from[], int n) \
{ int j; for (j=0; j<n; j++) to[j] = (usecast)from[j]; }

#define DECL_DBL2Type(visibility, func, type, usecast) \
visibility void func(type to[], const double from[], int n) \
{ int j; for (j=0; j<n; j++) to[j] = (usecast)from[j]; }

DECL_Type2DBL(PUBLIC, ODB_dbl2dbl, double, double)
DECL_Type2DBL(PUBLIC, ODB_uint2dbl, uint, double)
DECL_Type2DBL(PUBLIC, ODB_float2dbl, float, double)

DECL_Type2DBL(PRIVATE, dbl2dbl, double, double)

DECL_Type2DBL(PRIVATE, int2dbl, int, double)
DECL_DBL2Type(PRIVATE, dbl2int, int, int)

DECL_Type2DBL(PRIVATE, uint2dbl, uint, double)
DECL_DBL2Type(PRIVATE, dbl2uint, uint, int)

void
ODB_get_packing_consts(uint *magic, uint *hdrlen, uint *maxshift,
		       double *nmdi, double*rmdi, int *new_version)
{
  if (magic)    *magic  = PCMA;
  if (hdrlen)   *hdrlen = PCMA_HDRLEN;
  if (maxshift) *maxshift = MAXSHIFT;
  pcma_get_mdis_(nmdi, rmdi);
  if (new_version) *new_version = 1;
}


double *
ODB_unpack_DBL(const uint pd[],
               const int  pdlen,
	       int *dlen,
	       int *method_used,
	       uint  datatype)
{
  int rc = 0;
  const uint *hdr  = pd;
  double *cma = NULL;
  double *d = NULL;
  int method = 0;
  int lenbytes = 0;
  int lencma = 0;
  int totalpkwords = 0;
  const int b4 = sizeof(*hdr);
  double nmdi, rmdi;
  int new_version = 0;
  int swp = 0;
  uint dtnum = (datatype << 8);
  int can_swp_data = EXTRACT_SWAPPABLE(dtnum);
  const int idxlen = 0;
  const int fill_zeroth_cma = 0;

  *dlen = 0;

  rc = upcma_hdr(NULL, &swp,
		 (unsigned int *)hdr, 0, 
		 &method, &lencma, &lenbytes,
		 &nmdi, &rmdi,
		 &new_version);

  *method_used = method;

  totalpkwords = rc + RNDUP(lenbytes,b4)/b4;

  /*
  fprintf(stderr,
	  "ODB_unpack_DBL(HDRLEN=%d; rc=%d): method=%d, lencma=%d, lenbytes=%d, pdlen=%d, totalpkwords=%d\n",
	  HDRLEN, rc, method, lencma, lenbytes, pdlen, totalpkwords);
	  */

  if ((!new_version && rc != HDRLEN) ||
      ( new_version && rc != PCMA_HDRLEN)) {
    rc = -1; /* Error */
    goto finish;
  }

  if (rc != HDRLEN && !new_version) {
    rc = -1; /* Error */
    goto finish;
  }

  if (pdlen > 0 && totalpkwords != pdlen) {
    rc = -2; /* Error */
    goto finish;
  }

  if ((!new_version && pdlen > HDRLEN) ||
      ( new_version && pdlen > PCMA_HDRLEN)) {
    int count = 0;

    ALLOC(cma, lencma);

    pcma2cma_(&can_swp_data, pd, &pdlen, 
	      NULL, &idxlen, &fill_zeroth_cma,
	      cma, &lencma, &count, &rc);
    if (rc != lencma) {
      rc = -3; /* Error */
      goto finish;
    }

    if (count != pdlen) {
      rc = -4; /* Error */
      goto finish;
    }
    *dlen = lencma - 1;
  }
  else
    *dlen = 0;

  if (*dlen > 0) {
    /*
    ALLOC(d, *dlen);
    dbl2dbl(d, &cma[1], *dlen);
    */
    d = cma; /* due to fill_zeroth_cma == 0 */
  }
  else {
    CALLOC(d, *dlen);
  }

 finish:
  if (d != cma) FREE(cma);

  if (!d || rc < 0) {
    fprintf(stderr,
	    "***Error in ODB_unpack_DBL[method=%d;%s:%d]: rc=%d, *dlen=%d, pdlen=%d, lencma=%d, totalpkwords=%d\n",
	    method, __FILE__, __LINE__, rc, *dlen, pdlen, lencma, totalpkwords);
    RAISE(SIGABRT); /* Abort */
  }

  if (rc < 0) FREE(d);
  *dlen = (rc < 0) ? rc : *dlen;

  return d;
}


uint *
ODB_pack_DBL(const double d[],
	     const int  dlen,
	     int *method,
	     int *pdlen,
	     Bool avoid_copy)
{
  int rc = 0;
  Packbuf pbuf;
  int bytes_in, bytes_out;
  int Method = *method;
  double *cma = avoid_copy ? (double *)&d[-1] : NULL; /* A trique */
  int lencma = dlen + 1;

  if (dlen <= 0) { 
    /* Empty column */
    uint *oneword;
    CALLOC(oneword, 1);
    if (pdlen) *pdlen = 0;
    if (method) *method = 0;
    return oneword;
  }

  if (!avoid_copy) {
    ALLOC(cma,lencma);
    cma[0] = lencma;
    dbl2dbl(&cma[1],d,dlen);
  }

  pbuf.counter = 0;
  pbuf.maxalloc = 0;
  pbuf.len = 0;
  pbuf.p = NULL;
  pbuf.allocatable = 1;

  /*
  fprintf(stderr,
	  "ODB_pack_DBL(method=%d, dlen=%d, lencma=%d, addr=%p)\n",
	  Method, dlen, lencma, cma);
	  */

  rc = pcma(NULL, NULL, Method, cma, lencma, &pbuf,
	    &bytes_in, &bytes_out);

  if (rc < 0) FREE(pbuf.p);

  *pdlen = (rc < 0) ? rc : pbuf.len;

  /*
  fprintf(stderr,
	  "ODB_pack_DBL(rc=%d): method=%d, lencma=%d, bytes_in=%d, bytes_out=%d, *pdlen=%d, p=%p\n",
	  rc, method, lencma, bytes_in, bytes_out, *pdlen, pbuf.p);
	  */

  if (!avoid_copy) FREE(cma);

  if (rc == 0) {
    /* Extract true method from the packed stream */

    const uint *hdr = pbuf.p;
    int lenbytes;
    double nmdi, rmdi;
    int new_version;
    int swp = 0;

    /*
    fprintf(stderr,"ODB_pack_DBL(rc=%d, lencma=%d): method in = %d ... ",rc,lencma,*method);
    */

    (void) upcma_hdr(NULL, &swp,
		     (unsigned int *)hdr, 0, 
		     method, &lencma, &lenbytes,
		     &nmdi, &rmdi,
		     &new_version);

    /*
    fprintf(stderr,"out = %d : lencma = %d, lenbytes = %d\n",*method,lencma,lenbytes);
    */
  }

  return pbuf.p;
}


int *
ODB_unpack_INT(const uint pd[],
               const int  pdlen,
                     int *dlen,
	             int *method_used,
	            uint  datatype)
{
  double *tmp = ODB_unpack_DBL(pd, pdlen, dlen, method_used, datatype);
  int *p = NULL;
  int len = *dlen;

  ALLOC(p, len);
  if (len > 0) dbl2int(p, tmp, len);

  FREE(tmp);

  return p;
}


uint *
ODB_pack_INT(const int  d[],
	     const int  dlen,
                   int *method,
	           int *pdlen,
                   Bool avoid_copy)
{
  uint *p = NULL;

  if (dlen <= 0) { 
    /* An empty column */
    CALLOC(p, 1);
    if (pdlen) *pdlen = 0;
    if (method) *method = 0;
  }
  else if (dlen > 0) {
    double *tmp = NULL;
    
    ALLOC(tmp, dlen + 1);
    tmp[0] = dlen + 1;
    int2dbl(&tmp[1], d, dlen);
    
    p = ODB_pack_DBL(&tmp[1], dlen, method, pdlen, true);
    
    FREE(tmp);
  }

  return p;
}


uint *
ODB_unpack_UINT(const uint pd[],
                const int  pdlen,
                      int *dlen,
	              int *method_used,
	             uint  datatype)
{
  double *tmp = ODB_unpack_DBL(pd, pdlen, dlen, method_used, datatype);
  uint *p = NULL;
  int len = *dlen;

  ALLOC(p, len);
  if (len > 0) dbl2uint(p, tmp, len);

  FREE(tmp);

  return p;
}


uint *
ODB_pack_UINT(const uint  d[],
	      const int   dlen,
                    int  *method,
                    int  *pdlen,
	            Bool  avoid_copy)
{
  uint *p;

  if (dlen <= 0) { 
    /* Empty column */
    CALLOC(p, 1);
    if (pdlen) *pdlen = 0;
    if (method) *method = 0;
  }
  else if (dlen > 0) {
    double *tmp = NULL;

    ALLOC(tmp, dlen + 1);
    tmp[0] = dlen + 1;
    uint2dbl(&tmp[1], d, dlen);
    
    p = ODB_pack_DBL(&tmp[1], dlen, method, pdlen, true);

    FREE(tmp);
  }

  return p;
}
