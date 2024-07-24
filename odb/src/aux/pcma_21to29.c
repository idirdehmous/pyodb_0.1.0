
/* === CMA-packing methods#21 to 29 === */

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

#define LARGEPOW 305
#define POW10LEN (2*LARGEPOW+1)

PRIVATE int pow10_created = 0;
PRIVATE double pow10[POW10LEN];

#define INRANGE(x) ((int)(x) >= -LARGEPOW && (int)(x) <= LARGEPOW)

#ifdef DEBUG
#define POW10(x) (INRANGE(x) ? pow10[(int)(x) + LARGEPOW] : pow10abort((int)(x)))
#else
#ifdef NOT_CACHED_MODE
#define POW10(x) pow(10,x)
#else
/* Cached mode (the default) */
#define POW10(x) pow10[(int)(x) + LARGEPOW]
#endif
#endif

PRIVATE void /* NOT THREAD SAFE */
pow10_init()
{
  if (!pow10_created) {
    /* Cache pow(10,x)'s */
    int j;
    int n = POW10LEN;
    for (j=0; j<n; j++) {
      pow10[j] = pow(10,j-LARGEPOW);
    }
    pow10_created = 1;
  }
}

PRIVATE int
pow10abort(int p10)
{
  fprintf(stderr,
	  "***Error: power in POW10-macro (obtaining cached 10^%d) out of range [%d,%d]\n",
	  p10, -LARGEPOW, LARGEPOW);
  RAISE(SIGABRT);
  return 0;
}

PRIVATE int
pcma_21to29_prepare(double rmdi, 
		    const double d[], int n,
		    double power[], double frac[])
{
  int j;
  unsigned int large = 0;
  int num_mdis = 0;

  for (j=0; j<n; j++) {
    double x = d[j];

    if (IS_RMDI(x)) {
      power[j] = rmdi;
      num_mdis++;
    }
    else if (x==0) {
      power[j] = 0;
    }
    else {
      if (x<0) x = -x;
      power[j] = (int)log10(x);
    }
    if (!IS_RMDI(power[j]) && 
	(power[j] > LARGEPOW || power[j] < -LARGEPOW)) large++;
  } /* for (j=0; j<n; j++) */

  if (num_mdis < n) {
    /* Do only if at least one non-MDI */

    if (large > 0) {
      /* Slower due to division & explicit pow()-function invocation */
      for (j=0; j<n; j++) {
	double x = d[j];
	if (IS_RMDI(x)) {
	  frac[j] = rmdi;
	}
	else {
	  frac[j] = x / pow(10,power[j]);
	}
      } /* for (j=0; j<n; j++) */
    }
    else {
      /* Faster (the normal case) */

      if (!pow10_created) pow10_init();

      for (j=0; j<n; j++) {
	double x = d[j];
	if (IS_RMDI(x)) {
	  frac[j] = rmdi;
	}
	else {
	  frac[j] = x * POW10(-power[j]);
	  if (frac[j] != 0 && frac[j] > -1 && frac[j] < 1) {
	    /* Get an extra digit for fractional part */
	    frac[j] *= 10;
	    power[j]--;
	  }
	}
      } /* for (j=0; j<n; j++) */
    }
  } /* if (num_mdis < n) */

  return num_mdis;
}

int
pcma_21to29_driver(int method,
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
  int hdrlen = PCMA_HDRLEN;
  int prec = method - 20;
  int lenextra, lenpower, lenfrac;  
  Packbuf powerbuf, fracbuf;
  DRHOOK_START(pcma_21to29_driver);

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
	  "pcma_21to29_driver(method=%d, prec=%d): hdrlen=%d, replen=%d\n",
	  method, prec, hdrlen, replen);
#endif

  if (replen > 1) { 
    double *power, *frac;
    int num_mdis;

    ALLOC(power, replen);
    ALLOC(frac, replen);

    power[0] = replen;
    frac[0] = replen;

    num_mdis = pcma_21to29_prepare(rmdi, 
				   &cma[1], chunk,
				   &power[1], &frac[1]);

    if (num_mdis < chunk) {
      int powerbytes, fracbytes;

      powerbuf.counter = 0;
      powerbuf.maxalloc = 0;
      powerbuf.len = 0;
      powerbuf.p = NULL;
      powerbuf.allocatable = 1;
      powerbytes = pcma_1_driver(1, NULL, power, replen, nmdi, rmdi, &powerbuf);
      lenpower = powerbuf.len - hdrlen; /* Header is excluded */
#ifdef DEBUG
      fprintf(stderr,
	      "powerbytes=%d, powerbuf.len=%d, lenpower=%d\n",
	      powerbytes, powerbuf.len, lenpower);
#endif
      FREE(power);
      if (powerbuf.len < 0) {
	rc = -1; /* Unable to pack power parts with method#1 */
	goto finish;
      }

      fracbuf.counter = 0;
      fracbuf.maxalloc = 0;
      fracbuf.len = 0;
      fracbuf.p = NULL;
      fracbuf.allocatable = 1;
      fracbytes = pcma_11to19_driver(10+prec, NULL, frac, replen, nmdi, rmdi, &fracbuf);
      lenfrac = fracbuf.len - hdrlen; /* Header is excluded */
#ifdef DEBUG
      fprintf(stderr,
	      "fracbytes=%d, fracbuf.len=%d, lenfrac=%d\n",
	      fracbytes, fracbuf.len, lenfrac);
#endif
      FREE(frac);
      if (fracbuf.len < 0) {
	rc = -2; /* Unable to pack fractional parts */
	goto finish;
      }
    }
    else {
      /* All MDIs ==> don't call those auxiliary drivers */
      lenpower = 0;
      lenfrac = 0;
    }

    lenextra = 1 + 1; /* For lenpower & lenfrac */
  }
  else {
    lenextra = lenpower = lenfrac = 0;
  }

#ifdef DEBUG
  fprintf(stderr,
	  "lenextra = %d, lenpower = %d,  lenfrac = %d\n",
	  lenextra, lenpower, lenfrac);
#endif

  count += (lenextra + lenpower + lenfrac);
    
  packed_data = pcma_alloc(pbuf, count);
  
  packed_data[0] = PCMA;
  packed_data[1] = (20 + prec) * MAXSHIFT + 0; /* zero no_of_unpacked means that 4th word, 
						  packed_data[3] contains report length; 
						  ==> no_of_unpacked can now be over 16megawords */
  packed_data[2] = 
    (lenextra + lenpower + lenfrac)
      * sizeof(*packed_data); /* packed_data */
  packed_data[3] = replen;
  memcpy(&packed_data[4],&nmdi,sizeof(nmdi));
  memcpy(&packed_data[6],&rmdi,sizeof(rmdi));

  if (replen > 1) {
    unsigned int *extra     = &packed_data[hdrlen];
    unsigned int *powerdata = (lenpower > 0) ? &packed_data[hdrlen + lenextra] : NULL;
    unsigned int *fracdata  = (lenfrac  > 0) ? &packed_data[hdrlen + lenextra + lenpower] : NULL;
    extra[0] = lenpower;
    extra[1] = lenfrac;

    /* Packed power part */
    if (lenpower > 0) {
      pcma_copy_uint(powerdata, &powerbuf.p[hdrlen], lenpower);
      FREE(powerbuf.p);
    }

    /* Packed fractional part */
    if (lenfrac  > 0) {
      pcma_copy_uint(fracdata, &fracbuf.p[hdrlen], lenfrac);
      FREE(fracbuf.p);
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
upcma_21to29_driver(int method,
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
  DRHOOK_START(upcma_21to29_driver);

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
    int lenextra  = 1 + 1;
    double *power = NULL;
    const unsigned int *extra  = &packed_data[0];
    int lenpower = extra[0];
    int lenfrac = extra[1];
    int hdrlen = new_version ? PCMA_HDRLEN : HDRLEN;
    const int one = 1;

#ifdef DEBUG
    fprintf(stderr,
"upcma_21to29_driver(method=%d): lenpower=%d, lenfrac=%d, len_packed_data=%d, hdrlen=%d, msgbytes=%d, v=%d\n",
	    method, lenpower, lenfrac, len_packed_data, hdrlen, msgbytes, new_version);
#endif

    if (swp) {
      swap4bytes_(&lenpower, &one);
      swap4bytes_(&lenfrac, &one);
    }

    STAEND_FIX();

    if (lenfrac > 0) {
      const unsigned int *frac_packed;

      frac_packed = &packed_data[lenextra + lenpower];
      rc = upcma_11to19_driver(method,
			       swp, can_swp_data_here,
			       new_version,
			       frac_packed,
			       lenfrac,
			       (lenfrac - hdrlen) * sizeof(*frac_packed),
			       nmdi,
			       rmdi,
			       NULL, 
			       idx, idxlen, 
			       fill_zeroth_cma,
			       cma, replen);

      if (rc != replen) {
	rc = -2;
	goto finish;
      }

      if (lenpower > 0) {
	const unsigned int *power_packed;

	ALLOC(power, replen);

	power_packed = &packed_data[lenextra];
	rc = upcma_1_driver(method,
			    swp, can_swp_data_here,
			    new_version,
			    power_packed,
			    lenpower,
			    (lenpower - hdrlen) * sizeof(*power_packed),
			    nmdi,
			    rmdi,
			    NULL, 
			    idx, idxlen, 
			    fill_zeroth_cma,
			    power, replen);

	if (rc != replen) {
	  rc = -1;
	  goto finish;
	}

	if (!pow10_created) pow10_init();

	{
	  int jj;
	  for (jj=stacma; jj<endcma; jj++) {
	    j = fill_zeroth_cma + jj;
	    if (IS_RMDI(cma[j])) {
	      cma[j] = rmdi;
	    }
	    else if (INRANGE(power[j])) {
	      cma[j] *= POW10(power[j]); /* Use the cached 10^power's */
	    }
	    else {
	      cma[j] *= pow(10,power[j]); /* Calculate explicitly the 10^power */
	    }
	  } /* for (jj=stacma; jj<endcma; jj++) */
	}
      } /* if (lenpower > 0) */
    }
    else {
      int jj;
      if (idx && idxlen > 0 && idxlen < endcma-stacma) {
	for (jj=0; jj<idxlen; jj++) { /* ultra-fast :-) */
	  j = idx[jj];
	  if (j >= stacma && j < endcma) {
	    cma[fill_zeroth_cma+j] = rmdi;
	  }
	} /* for (jj=0; jj<idxlen; jj++) */
      }
      else {
	for (jj=stacma; jj<endcma; jj++) {
	  j = fill_zeroth_cma + jj;
	  cma[j] = rmdi;
	} /* for (jj=stacma; jj<endcma; jj++) */
      }
      rc = replen;
    }

  finish:
    FREE(power);

    rc = (rc == replen) ? replen : rc;
  }

  if (fp_out) nwrt = fwrite(cma, sizeof(double), lencma, fp_out);

  DRHOOK_END(0);
  return rc;
}
