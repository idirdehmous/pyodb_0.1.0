#define PCMA_C 1

#include "pcma.h"

int pcma_blocksize = MINBLOCK;
int pcma_verbose   = 0;
int pcma_restore_packed = 0;

/* These two following lines moved to pcma_mdis.c on 23-Nov-2004 by SS
double pcma_nmdi = NMDI;
double pcma_rmdi = RMDI;
 */

static 
int (*pack_func_list[MAXPACKFUNC])
(int method,
 FILE *fp_out, 
 const double cma[], int lencma, 
 double nmdi,
 double rmdi,
 Packbuf *pbuf) = 
{
  pcma_1_driver,  /* Method 1 : adaptive, averaged */
  pcma_2_driver,  /* Method 2 : Integer ZERO, RMDI_1 & RMDI_2 packing */
  pcma_3_driver,  /* Method 3 : Lempel-Ziv-Welch  (does not vectorize) */
  pcma_5_driver,  /* Method 5 : Same as 2, but also compresses small numbers [1..255] */
  pcma_9_driver,  /* Method 9 : repeated word */
  /* Methods 11..19 : 10 + # of signicant digits ; loss of flp. pnt. accurracy */
  pcma_11to19_driver, 
  /* Methods 21..29 : 20 + # of signicant digits ; loss of flp. pnt. accurracy ;
     with 10^exponent scaling (a sort of merger of method#1 and methods#11..19) */
  pcma_21to29_driver,
  pcma_4_driver,   /* Method  4 : Delta packing (does not vectorize) */
  pcma_4_driver,   /* Method 94 : Just a filler/pseudo-method for use by pcma_9.c */
  pcma_31to39_driver, /* Methods 31..39 : The same as 21..29, but preserves 32-bit ints */
};


PUBLIC int
pcma(        FILE *fp_in,
             FILE *fp_out,
              int  method,
     const double  cma[],
              int  lencma,
	  Packbuf *pbuf,
              int *bytes_in,
              int *bytes_out)
{
  int rc = 0;
  int (*pack_func)(int method,
		   FILE *fp_out, 
		   const double cma[], int lencma, 
		   double nmdi, 
		   double rmdi,
		   Packbuf *pbuf) = NULL;
  double nmdi = pcma_nmdi;
  double rmdi = pcma_rmdi;

  *bytes_in  = 0;
  *bytes_out = 0;

  switch (method) {
    /* CMA-specific bitmap methods */
  case 1: /* Adaptive, averaged */
  case 2:
  case 5:
  case 9:
  case 11: case 12: case 13: case 14: case 15: case 16: case 17: case 18: case 19:
  case 21: case 22: case 23: case 24: case 25: case 26: case 27: case 28: case 29:
  case 31: case 32: case 33: case 34: case 35: case 36: case 37: case 38: case 39:
  case 4:  case 94:
    if (sizeof(double)/sizeof(unsigned int) != 2) {
      fprintf(stderr,
	      "pcma(#%d): There ain't exactly two uint words per double prec. word\n",
	      method);
      rc = -2;
      goto finish;
    }
    if (method == 4) {
      if (sizeof(u_ll_t)/sizeof(unsigned int) != 2) {
      fprintf(stderr,
	      "pcma(#%d): There ain't exactly two uint words per unsigned long long int\n",
	      method);
	rc = -7;
	goto finish;
      }
    }
    break;
 
  case 3: /* ECMWF's internal LZW-packing (used with the MARS as well) */
    break;

  default:
    /* Error : Unrecognized packing method */
    fprintf(stderr,"pcma(#%d): Invalid packing method\n",method);
    rc = -1;
    goto finish;
  } /* switch (method) */
  
  pack_func = pack_func_list[look_up[method]];  

  if (!fp_in) {
    /* CMA-array is assumed to be provided */

    int j, replen;
    j=0;

    while (j<lencma) { 
      /* No PCMA-check is needed since a genuine CMA comes now from an application */
      replen = cma[j];

      *bytes_in += replen * sizeof(double);

      rc = pack_func(method, fp_out, &cma[j], replen, nmdi, rmdi, pbuf);
      if (rc >= 0) {
	*bytes_out += rc;
	rc = 0;
      }
      else {
	/* Error */
	break;
      }

      j += replen;
    }
  }

  else {  
    /* Read one report at a time from CMA-file and pack it */
    int maxalloc;
    int n, minlen = MINLEN;
    double oneword;
    double *report = NULL;
    int replen;
    union {
      double d;
      int    PCMA_word;
    } jack;
    int done = 0;

    rc = 0;
    ALLOC(report, pcma_blocksize);
    if (!report) {
      fprintf(stderr,
	      "pcma(#%d): Unable to allocate initial space (%d DP-words) for a CMA-data\n",
	      method, pcma_blocksize);
      rc = errno;
      goto finish;
    }
    maxalloc = pcma_blocksize;

    for (;;) {
      n = fread(&oneword, sizeof(double), 1, fp_in);
      if (n != 1) {
	if (!feof(fp_in)) rc = errno; /* Error : EOF was expected ... */
	break;
      }

      if (!done) {
	jack.d = oneword;
	if (jack.PCMA_word == PCMA) {
	  /* Sorry, this ain't going to work; file is PCMA-packed already */
	  fprintf(stderr,"pcma(#%d): File has already been packed\n",method);
	  rc = -3;
	  break;
	}
	done = 1; /* Do it once only to save some time */
      }
      
      replen = oneword;
      if (replen > maxalloc) {
	REALLOC(report, replen);
	maxalloc = replen;
      }

      if (!report) {
	fprintf(stderr,
		"pcma(#%d): Unable to allocate space (%d DP-words) for a CMA-report\n",
		method, replen);
	rc = errno;
	break;
      }
      
      report[0] = oneword;
      n = fread(&report[1], sizeof(double), replen-1, fp_in);
      if (n != (replen-1)) {
	rc = errno; /* Error : Probably an unexpected EOF */
	break;
      }

      *bytes_in  += replen * sizeof(double);
      n = pack_func(method, fp_out, report, replen, nmdi, rmdi, pbuf);

      if (n < minlen) {
	/* Error : Invalid no. of packed bytes */
	rc = n;
	break;
      }

      *bytes_out += n;
    };

    FREE(report);    
  }

 finish:

  return rc;
}

PUBLIC void *
PackDoubles(const double v[],
	    const int nv,
	    int *nbytes)
{
  const int min_bytes = 2 * sizeof(uint);
  uint *pk = NULL;
  int Nbytes = 0;

  ALLOC(pk, 2);

  if (nv > 0) {
    int nv_bytes = nv * sizeof(double);
    int npk = nv_bytes / sizeof(uint); /* First guess */
    do { /* See how it's done in aux/pcma_3.c */
      int npk_bytes = npk * sizeof(uint);
      REALLOC(pk, 2 + npk);
      lzw_pack_((const unsigned char *)v, &nv_bytes,
		(unsigned char *)&pk[2], &npk_bytes,
		&Nbytes);
      if (Nbytes < 0) {
	int nb = -Nbytes;
	npk = RNDUP(nb,b4)/b4;
      }
    } while (Nbytes < 0);
  }

  pk[0] = (uint)Nbytes;     /* No of packed bytes */
  pk[1] = (uint)MAX(nv,0);  /* Length of the double vector just packed (in double-words) */
  Nbytes += min_bytes;      /* Total no. of bytes in "pk" */

  if (nbytes) *nbytes = Nbytes;
  return pk;
}


/*============= Intended for Fortran-access, but rather general ===============*/

PUBLIC void
cma2pcma_(const    int *method,
          const double  cma[],
	  const    int *lencma,
	  unsigned int  packed_stream[],
	  const    int *lenpacked_stream,
                   int *bytes_in,
                   int *bytes_out,
	           int *retcode)
{
  int rc = 0;
  int packingmethod = *method;
  int cmalen = *lencma;
  Packbuf pbuf;

  pbuf.counter = 0;
  pbuf.maxalloc = *lenpacked_stream;
  pbuf.len = 0;
  pbuf.p = packed_stream;
  pbuf.allocatable = 0;

  if (packingmethod == 0) {
    *bytes_in  = cmalen * sizeof(double);
    *bytes_out = *bytes_in; 

    if (pbuf.maxalloc * sizeof(unsigned int) >= *bytes_out) {
      pbuf.len = (*bytes_out)/sizeof(unsigned int);

      pcma_copy_uint((unsigned int *)packed_stream, 
		     (const unsigned int *)cma, pbuf.len);

      /* The old coding with memcpy() did not necessarely vectorize:
	 memcpy(packed_stream, cma, *bytes_out); */
    }
    else {
      /* Error: packed_stream is this many words too short */
      rc = (pbuf.maxalloc * sizeof(unsigned int) - *bytes_out) / sizeof(unsigned int);
    }
  }
  else {
    rc = pcma(NULL,  /* CMA-input channel ; N/A */
	      NULL,  /* Packed output channel ; N/A */
	      packingmethod, /* Packing method */
	      cma, cmalen, /* In-core CMA is available */
	      &pbuf,  /* Buffer of concatenated packed messages */
	      bytes_in, bytes_out); /* Bytes in/out count */

    if (*bytes_in != cmalen * sizeof(double)) {
      fprintf(stderr,
	      "cma2pcma(): Inconsistent 'bytes_in'; Not equal to 'cmalen x %d' : bytes_in=%d, cmalen=%d\n",
	      (int)sizeof(double),*bytes_in,cmalen);
      rc = -1;
    }
  }

  *retcode = rc;
}
