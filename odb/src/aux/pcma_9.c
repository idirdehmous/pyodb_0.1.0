
/* === CMA-packing method#9 === */

#include "pcma.h"

#ifdef VPP
#pragma global noalias
#elif defined(NECSX)
#pragma cdir options -pvctl,nodep
#endif

/* ODB_packing() moved here from ../lib/errtrap.c on 09/03/2005 by SS */

int
ODB_packing(const int *packing_method)
{
  static boolean first_time = 1;
  static int packing = -1;
  static int done[1+MAXPACKINGMETHOD];
  static int mapped2[1+MAXPACKINGMETHOD];
  int rc;

  if (first_time) {
    int j;
    char *p = getenv("ODB_PACKING");
    int value = p ? atoi(p) : packing;
    packing = (value == 0) ? 0 : packing;
    for (j=0; j<=MAXPACKINGMETHOD; j++) {
      done[j] = 0;
      /* 
	 By default the method given in *packing_method will map to the same number/pmethod ;

	 Can be altered via environment setting "ODB_PACKING_MAP_<method>=<alternate_method>",
	 where the <method> is the default method >0 mapped to use <alternate_method>, >=0;

	 Note that the no-packing situation (method=0) can *NOT* be altered afterwards,
	 since it will lead into unstable database management
       */
      mapped2[j] = j; 
    }
    first_time = 0;
  }

  rc = packing;

  if (packing_method) {
    int pmethod = *packing_method;
    pmethod = MAX(0,pmethod);
    pmethod = MIN(pmethod,MAXPACKINGMETHOD);
    rc = pmethod;
    if (pmethod > 0 && pmethod <= MAXPACKINGMETHOD) {
      if (!done[pmethod]) {
	char env[64];
	char *p;
	int alternate_method = pmethod;
	sprintf(env,"ODB_PACKING_MAP_%d",pmethod);
	p = getenv(env);
	if (p) alternate_method = atoi(p);
	alternate_method = MAX(0,alternate_method);
	alternate_method = MIN(alternate_method,MAXPACKINGMETHOD);
	rc = mapped2[pmethod] = alternate_method;
	done[pmethod] = 1;
#if 0
	/* No access to Myproc at the moment ; hmm ... thinking ... okay, may take years to solve ;-) */
	if (Myproc == 1) {
#endif
	  if (p) {
	    fprintf(stderr,
		    "***Warning: Packing method#%d maps to method#%d, since environment variable %s=%s\n",
		    pmethod, alternate_method, env, p);
	  }
#if 0
	} /* if (Myproc == 1) */
#endif
      }
      else {
	rc = mapped2[pmethod];
      } /* if (!done[pmethod]) else ... */
    } /* if (pmethod > 0 && pmethod <= MAXPACKINGMETHOD) */
  } /* if (packing_method) */

  return rc;
}

static int
pcma_9_similar(const double  cma[],    /* double CMA[] */
	                int  lencma    /* length of the double CMA[] */
	       )
{
  int rc = 0;
  int i;
  double refval = cma[0];
  int count = 1;

#if defined(VPP) || defined(NECSX)
  for (i=1; i<lencma; i++) { /* Vectorizable */
    if (cma[i] == refval) count++;
  }
#else
  for (i=1; i<lencma; i++) { /* Scalar machines : not vectorizable */
    if (cma[i] != refval) break; /* Get out immediately when false */
    count++;
  }
#endif

  rc = (count == lencma);

  return rc;
}
	       

int
pcma_9_driver(int method, /* ignored */
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
  double refval;
  int lenactive = 2;
  int dbl_align = 0;
  unsigned int *packed_data = NULL;
  int similar = 0;
  int hdrlen = PCMA_HDRLEN;
  DRHOOK_START(pcma_9_driver);

  nwrt = 0;

  replen = lencma;
  chunk  = replen-1; /* Report w/o length information */

  if (replen >= 1) {
    similar = (replen == 1) ? 1 : pcma_9_similar(&cma[1], chunk);
  }

  if (!similar) {
    /*** This is where we use the filler/pseudo-method 94 ***/

    /* Switch for more effective packing */

    /* 
       For better performance, change on vector machines ("VPP") to

         export ODB_PACKING_MAP_94=2

       This is also compatible with previous releases.
       Databases can be 30% larger than with method#3 or #4,
       but at least packing fully vectorizes with method#2.

       In order to be compatible on scalar machines, use the following:

         export ODB_PACKING_MAP_94=3

    */

    const int method = 94;
    int mapped_method = ODB_packing(&method);

    /* If no change method, then ... */
#if defined(VPP) || defined(NECSX)
    if (mapped_method == method) mapped_method = 2; /* Vectorizable */
#else
    if (mapped_method == method) mapped_method = 3; /* Scalar */
#endif

    /* Allow only these 3 methods : 2, 3 or 4 (= future default ?) */
    if (mapped_method == 4) { /* Packs often a mixture of integers & real numbers well ; New */
      rc = pcma_4_driver(4, fp_out, cma, lencma, nmdi, rmdi, pbuf);
    }
    else if (mapped_method == 2) { /* The oldest packing method in ODB ? */
      rc = pcma_2_driver(2, fp_out, cma, lencma, nmdi, rmdi, pbuf);
    }
    else { /* a Lemper-Ziv-Welch -variation */
      rc = pcma_3_driver(3, fp_out, cma, lencma, nmdi, rmdi, pbuf);
    }
    goto finish;
  }

  /* Otherwise replication is okay! */

  refval = (replen == 1) ? 0 : cma[1];

  count = hdrlen; /* 'PCMA' + method & 3 zero bytes + no_of_packed_bytes + no_of_unpacked  + 2 x double MDIs */
  count += (dbl_align + lenactive);

  packed_data = pcma_alloc(pbuf, count);

  packed_data[0] = PCMA;
  packed_data[1] = 9 * MAXSHIFT + 0; /* zero no_of_unpacked means that 4th word, packed_data[3] contains
					report length; ==> no_of_unpacked can now be over 16megawords */
  packed_data[2] = (dbl_align + lenactive) * sizeof(*packed_data);
  packed_data[3] = replen;
  memcpy(&packed_data[4],&nmdi,sizeof(nmdi));
  memcpy(&packed_data[6],&rmdi,sizeof(rmdi));

  if (!dbl_align) {
    memcpy(&packed_data[hdrlen], &refval, sizeof(refval));
  }
  else {
    packed_data[hdrlen] = ALGN;
    memcpy(&packed_data[hdrlen+1], &refval, sizeof(refval));
  }

  if (fp_out) nwrt = fwrite(packed_data, sizeof(*packed_data), count, fp_out);

  total_count = count;

  rc = total_count *  sizeof(*packed_data);

  /*  finish: */
  if (!pbuf) FREE(packed_data);

 finish:
  DRHOOK_END(0);
  return rc;
}

/* ======================================================================================== */

/* upcma_9: perform the actual unpacking of a buffer */

static int 
upcma_9(double  cma[],     /* output :  double CMA[] */
	int     stacma,    /* starting address of CMA[] */
	int     endcma,    /* ending address of CMA[] */
	const int idx[],
	int       idxlen,
	double  refval
	)
{
  int rc = 0;
  int i;

  if (idx && idxlen > 0 && idxlen < endcma-stacma) {
    int ii;
    for (i=0; i<idxlen; i++) { /* ultra-fast :-) */
      ii = idx[i];
      if (ii >= stacma && ii < endcma) {
	cma[ii] = refval;
      }
    }
  }
  else {
    for (i=stacma; i<endcma; i++) {
      cma[i] = refval;
    }
  }

  rc = endcma-stacma;

  /*  finish: */

  /* Upon successful completion returns no. of uint CMA-words unpacked */

  return rc; 
}

int
upcma_9_driver(int method,
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
  DRHOOK_START(upcma_9_driver);

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
    int dbl_align = new_version ? 0 : 1;
    int lenactive = len_packed_data - dbl_align;
    const double *data = (const double *)&packed_data[dbl_align];
    double refval = *data;
    const int one = 1;
    double *cma_addr = &cma[fill_zeroth_cma];

    if (swp && can_swp_data) swap8bytes_(&refval, &one);
    
    STAEND_FIX();

    rc = upcma_9(cma_addr,
		 stacma, endcma,
		 idx, idxlen,
		 refval);

    rc = (rc == endcma-stacma) ? replen : rc;
  }

  if (fp_out) nwrt = fwrite(cma, sizeof(double), lencma, fp_out);

  DRHOOK_END(0);
  return rc;
}
