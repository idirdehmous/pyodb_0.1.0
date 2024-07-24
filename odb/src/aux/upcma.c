#define UPCMA_C 1

#include "pcma.h"

#define UPCMA_INFOLEN (PCMA_HDRLEN + 1)

static 
int (*unpack_func_list[MAXPACKFUNC])
     (int method,
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
      double cma[], int lencma) = 
{
  upcma_1_driver,  /* Method 1 : adaptive, averaged */
  upcma_2_driver,  /* Method 2 : Integer ZERO, RMDI_1 & RMDI_2 packing */
  upcma_3_driver,  /* Method 3 : Lempel-Ziv-Welch (does not vectorize) */
  upcma_5_driver,  /* Method 5 : Same as 2, but also compresses small numbers [1..255] */
  upcma_9_driver,  /* Method 9 : repeated word */
  /* Methods 11..19 : 10 + # of signicant digits ; loss of flp. pnt. accurracy */
  upcma_11to19_driver, 
  /* Methods 21..29 : 20 + # of signicant digits ; loss of flp. pnt. accurracy ;
     with 10^exponent scaling (a sort of merger of method#1 and methods#11..19) */
  upcma_21to29_driver, 
  upcma_4_driver,  /* Method 4 : Delta packing (does not vectorize) */
  upcma_4_driver,  /* Method 94 : Just a filler/pseudo-method for use by pcma_9.c */
  upcma_31to39_driver, /* Methods 31..39 : The same as 21..29, but preserves 32-bit ints */
};


PUBLIC int
upcma_hdr(FILE *fp_in, int *swp,
	  unsigned int *hdr, int read_hdr,
	  int *method, int *replen, int *msgbytes,
	  double *nmdi, double *rmdi,
	  int *new_version)
{
  int rc = 0;
  int errflg = 0;
  unsigned int word4 = 0;
  unsigned int undef =  0x8B8B8B8B;

  *swp = 0;
  *nmdi = undef;
  *rmdi = undef;
  *new_version = 0;

#ifdef DEBUG
  fprintf(stderr,
	  "upcma_hdr(fp_in=%p, read_hdr=%d) : HDRLEN=%d\n",
	  fp_in, read_hdr, HDRLEN);
#endif

  rc = (fp_in && read_hdr) ? fread(hdr, sizeof(*hdr), HDRLEN, fp_in) : HDRLEN;

  if (rc != HDRLEN) {
    if (fp_in && feof(fp_in)) rc = 0;
  }
  else {
    const int hdrlen = HDRLEN;
    *swp = (hdr[0] == AMCP) ? 1 : 0;

#ifdef DEBUG
    fprintf(stderr,"upcma_hdr(0): swap bytes (*swp) is %d\n",*swp);
#endif

    if (*swp) swap4bytes_(hdr,&hdrlen); /* swap bytes */

#ifdef DEBUG
    fprintf(stderr,"upcma_hdr(1): hdr[0]=%u, PCMA=%u\n",hdr[0],PCMA);
#endif

    if (hdr[0] == PCMA) {
      *new_version = PCMA_NEW_VERSION(hdr); /* Also the same as PCMA_HDRLEN - HDRLEN, if new */
      *method   = hdr[1]/MAXSHIFT;
      *replen   = hdr[1]%MAXSHIFT;
      *msgbytes = hdr[2];
      if (*swp) swap4bytes_(hdr,&hdrlen); /* restore */

#ifdef DEBUG
      fprintf(stderr,
	      "upcma_hdr(2): *new_version=%d, *method=%d, *replen=%d, *msgbytes=%d\n",
	      *new_version, *method, *replen, *msgbytes);
#endif

      if (*new_version) {
	int rc2 = (fp_in && read_hdr) ? 
	  fread(&hdr[3], sizeof(*hdr), *new_version, fp_in) : *new_version;
	if (rc2 != *new_version) {
	 if (fp_in && feof(fp_in)) 
	   rc = 0;
	 else 
	   errflg++;
	}
	else {
	  const int one = 1;
	  const int two = 2;
	  *replen = hdr[3];
	  if (*swp) swap4bytes_(replen,&one); /* swap bytes */
	  word4 = *replen;
	  if (*swp) swap8bytes_(&hdr[4],&two); /* swap bytes */
	  memcpy(nmdi, &hdr[4], sizeof(*nmdi));
	  memcpy(rmdi, &hdr[6], sizeof(*rmdi));
	  if (*swp) swap8bytes_(&hdr[4],&two); /* restore */
	  rc += *new_version;
	}
      }
      else {
	/* old version */
	*nmdi = NMDI;
	*rmdi = RMDI;
	*new_version = 0;
      } /* if (*new_version) ... else */
    }
    else {
      errflg++;
    }

    if (errflg) {
      perror("upcma_hdr(unrecognized msg)"); /* Remove this later on ... */
      fprintf(stderr,"hdr[0] = %u : Should be = %u (PCMA)\n",hdr[0],PCMA);
      fprintf(stderr,"hdr[1] = %u : Should be = method & {report length or zero}\n",hdr[1]);
      fprintf(stderr,"hdr[2] = %u : Should be = No. of packed bytes\n",hdr[2]);
      fprintf(stderr,"The following are present only if the report length in hdr[1] is zero:\n");
      fprintf(stderr,"word4  = %u ; Should be = report length (in new version)\n",word4);
      fprintf(stderr,"nmdi   = %.20g ; Should be = NMDI used in the msg\n",*nmdi);
      fprintf(stderr,"rmdi   = %.20g ; Should be = RMDI used in the msg\n",*rmdi);
      rc = -2; /* Unrecognized message */
    }

  }

  return rc;
}


PUBLIC int 
upcma_data(FILE *fp_in, const unsigned int *hdr, 
	   int new_version,
	   unsigned int *packed_data, int count)
{
  int rc = 0;
  int hdrlen = HDRLEN;

  if (hdr) {
    new_version = PCMA_NEW_VERSION(hdr);
    hdrlen += new_version;
    memcpy(packed_data, hdr, hdrlen);
  }
  else
    hdrlen += new_version;

  count -= hdrlen;
  packed_data += hdrlen;

  rc = fread(packed_data, sizeof(*packed_data), count, fp_in);

  if (rc != count) {
    perror("upcma_data(rc != count)"); /* Remove this later on ... */
    rc = -4; /* Not all data was read */
    if (feof(fp_in)) rc = 0; /* Unexpected EOF */
  }
  else
    rc += hdrlen;

  return rc;
}



PUBLIC int
upcma(          int can_swp_data,
              FILE *fp_in,
              FILE *fp_out,
	 const int  idx[], int  idxlen,
	       int  fill_zeroth_cma,
            double  cma[],
              int   lencma,
     unsigned int  *hdr_in,
	  Packbuf  *pbuf,
              int  *bytes_in,
              int  *bytes_out
      )
{
  int rc = 0;
  unsigned int hdr[PCMA_HDRLEN];
  double *ptrcma = cma;
  double *cmabuf = NULL;
  int maxalloc_cma = 0;
  int maxalloc_pcma = 0;
  unsigned int *packed_data = NULL;

  *bytes_in  = 0;
  *bytes_out = 0;

  if (fp_out || lencma > 0) {
    if (fp_out) {
      ALLOC(cmabuf, pcma_blocksize);
      maxalloc_cma = pcma_blocksize;
    }

    ALLOC(packed_data, pcma_blocksize);
    maxalloc_pcma = pcma_blocksize;
  }

  while (fp_out || lencma > 0) {
    int hdrlen = HDRLEN;
    int swp, new_version;
    int method, replen, msgbytes;
    double nmdi, rmdi;
    int count;
    unsigned int *phdr = hdr_in ? hdr_in : hdr;

    rc = upcma_hdr(fp_in, &swp,
		   phdr, (hdr_in == NULL),
		   &method, &replen, &msgbytes,
		   &nmdi, &rmdi,
		   &new_version);
    hdr_in = NULL;

    hdrlen += new_version;

    if (rc == hdrlen) {
      switch (method) {

      case 1: 
      case 2:
      case 5:
      case 9:
      case 11: case 12: case 13: case 14: case 15: case 16: case 17: case 18: case 19:
      case 21: case 22: case 23: case 24: case 25: case 26: case 27: case 28: case 29:
      case 31: case 32: case 33: case 34: case 35: case 36: case 37: case 38: case 39:
      case 4:  case 94:
	if (sizeof(double)/sizeof(unsigned int) != 2) {
	  fprintf(stderr,
		  "upcma(#%d): There ain't exactly two uint words per double prec. word\n",
		  method);
	  rc = -2;
	  goto finish;
	}
	if (method == 4) {
	  if (sizeof(u_ll_t)/sizeof(unsigned int) != 2) {
	    fprintf(stderr,
		    "upcma(#%d): There ain't exactly two uint words per unsigned long long int\n",
		    method);
	    rc = -7;
	    goto finish;
	  }
	}
	break;
      case 3:
	break;
      default:
	/* Unpacking not implemented yet for this method */
	fprintf(stderr,"upcma(#%d): Unpacking method not implemented\n",method);
	rc = -3;
	goto finish;
      } /* switch (method) */

      if (fp_out) {
	if (replen > maxalloc_cma) {
	  REALLOC(cmabuf, replen);
	  maxalloc_cma = replen;
	}
	ptrcma = cmabuf;
      }
      else {
	if (replen > lencma) {
	  fprintf(stderr,
	  "upcma(#%d): Not enough space reserved for (external) CMA-array. Short %d words\n",
		  method, replen - lencma);
	  rc = -5;
	  goto finish;
	}
      }
      
      count = hdrlen + RNDUP(msgbytes,b4)/b4; /* Total no. of packed uint words in msg */

      if (count > maxalloc_pcma) {
	REALLOC(packed_data, count);
	maxalloc_pcma = count;
      } 

      rc = upcma_data(fp_in, NULL, new_version, packed_data, count);
      if (rc != count) goto finish;

      if (pbuf) { 
	/* Return packed data to the caller */
	pbuf->counter = 0;
	pbuf->maxalloc = count;
	pbuf->len = count;
	ALLOC(pbuf->p,count); /* Beware of dangling pointers; 
				 doesn't check whether pbuf->p was already allocated !! */
	/* Note: packed_data did NOT contain the 'hdr' */
	memcpy(pbuf->p, phdr, hdrlen * sizeof(*phdr));
	/* And now the rest */
	memcpy(&pbuf->p[hdrlen], &packed_data[hdrlen], (count-hdrlen) * sizeof(*packed_data));
	pbuf->allocatable = 1;
      }
      
      rc = unpack_func_list[look_up[method]](method,
					     swp, can_swp_data,
					     new_version,
					     &packed_data[hdrlen],
					     count-hdrlen,         /* excluding header */
					     msgbytes,
					     nmdi, rmdi,
					     fp_out, 
					     idx, idxlen,
					     fill_zeroth_cma,
					     ptrcma, replen);

      if (rc != replen) {
	fprintf(stderr,"upcma(#%d): rc=%d, replen=%d\n", method, rc, replen);
	goto finish;
      }

      if (!fp_out) {
	ptrcma += replen;
	lencma -= replen;
      }

      *bytes_in  += count * b4;
      *bytes_out += replen * sizeof(double);
    }
    else {
      if (feof(fp_in))
	rc = 0;  /* EOF => no error */
      else
	rc = -6; /* Error */
      goto finish;
    }
  } /* while (fp_out || lencma > 0) */

 finish:
  FREE(packed_data);
  FREE(cmabuf);

  return rc;
}


PUBLIC void
upcmaTOcma(const          int *can_swp_data,
	   const unsigned int  packed_hdr[],
           const unsigned int  packed_stream[],
	            const int  idx[], int  idxlen,
	                  int  fill_zeroth_cma,
                       double  cma[],
                    const int *lencma,
                          int *retcode)
{
  int rc = 0;
  int hdrlen = HDRLEN;
  int swp, new_version;
  const unsigned int *hdr  = &packed_hdr[0];
  const unsigned int *data = &packed_stream[0];
  int method, replen, msgbytes;
  double nmdi, rmdi;

  rc = upcma_hdr(NULL, &swp,
		 (unsigned int *)hdr, 0,
		 &method, &replen, &msgbytes,
		 &nmdi, &rmdi,
		 &new_version);

#ifdef DEBUG
  fprintf(stderr,"upcmaTOcma(): swp=%d, *can_swp_data=%d\n",swp,*can_swp_data);
#endif

  hdrlen += new_version;

  if (rc != hdrlen) {
    /* Error : Possibly not a packed CMA-report at all */
    rc = -1;
  }
  else if (replen > *lencma) {
    /* Error : Not enough room reserved for unpacked data in cma[] -array */
    rc = -2;
  }
  else {
    int count   = hdrlen + RNDUP(msgbytes,b4)/b4; /* Total no. of packed uint words in msg */
    int datalen = count - hdrlen;

    if (   method == 2
	|| method == 3
	|| method == 5
	|| method == 9
	|| method == 1
	|| (method >= 11 && method <= 19)
	|| (method >= 21 && method <= 29)
	|| (method >= 31 && method <= 39)
	|| method == 4
	|| method == 94
	) {
      rc = unpack_func_list[look_up[method]](method,
					     swp, *can_swp_data,
					     new_version,
					     data,
					     datalen,
					     msgbytes,
					     nmdi, rmdi,
					     NULL,
					     idx, idxlen,
					     fill_zeroth_cma,
					     cma, replen);
      if (rc != replen) {
	/* Error: Unable to decode exactly the replen no. of CMA-words */
	rc = -3;
      }
    }
    else {
      /* Error: Invalid packing method; or at least no unpacking available */
      rc = -4;
    }
  }

  *retcode = rc;
}


PUBLIC double *
UnPackDoubles(const void *pk,
	      const int nbytes,
	      int *nv)
{
  const int min_bytes = 2 * sizeof(uint);
  double *v = NULL;
  int Nv = 0;
  if (pk && nbytes >= min_bytes) {
    const uint *Pk = pk;
    int Nbytes = (int)Pk[0];
    Nv = (int)Pk[1];
    ALLOC(v, Nv);
    if (Nbytes == -1) {
      /* Not packed at all */
      int nv_bytes = Nv * sizeof(double);
      memcpy(v, &Pk[2], nv_bytes);
    }
    else if (Nv > 0) {
      int rc = 0;
      int nv_bytes = Nv * sizeof(double);
      lzw_unpack_((const unsigned char *)&Pk[2], &Nbytes,
                (unsigned char *)v, &nv_bytes,
                &rc);
      if (rc == nv_bytes) {
	rc /= sizeof(double);
	rc = (rc == Nv) ? Nv : -ABS(rc);
      }
      else {
	rc = -ABS(rc);
      }
      if (rc /= Nv) {
	FREE(v);
	Nv = rc;
      }
    } /* if (Nv > 0) */
  }
  if (nv) *nv = Nv;
  return v;
}

/*============= Intended for Fortran-access, but rather general ===============*/

void 
upcma_info_(const unsigned int  packed_hdr[],
	                   int  info[],
	             const int *infolen,
	                   int *retcode)
     /* Intended for Fortran-access, but rather general 
	NOTE: Data assumed to be in packed_stream and thus
	      no I/O is performed to get it there */
{
  int rc = 0;
  int hdrlen = HDRLEN;
  int swp, new_version;
  const unsigned int *hdr  = &packed_hdr[0];
  int method, replen, msgbytes;
  double nmdi, rmdi;

  rc = upcma_hdr(NULL, &swp,
		 (unsigned int *)hdr, 0,
		 &method, &replen, &msgbytes,
		 &nmdi, &rmdi,
		 &new_version);

  hdrlen += new_version;

  if (rc != hdrlen) {
    /* Error : Possibly not a packed CMA at all */
    rc = -1;
  }
  else {
    int jlen, j;
    int Info[UPCMA_INFOLEN];
    int count   = hdrlen + RNDUP(msgbytes,b4)/b4; /* Total no. of packed uint words in msg */
    int datalen = count - hdrlen;
    
    Info[1-1] = replen;   /* CMA report length (when unpacked) */
    Info[2-1] = method;   /* Packing method */
    Info[3-1] = hdrlen;   /* Length of packed header, in unsigned ints */
    Info[4-1] = datalen;  /* Length of packed data (after hdr), in unsigned ints */
    Info[5-1] = msgbytes; /* Exact no. of bytes packed in data part (i.e. msgbytes <= 4*datalen) */
    memcpy(&Info[6-1], &nmdi, sizeof(nmdi));
    memcpy(&Info[8-1], &rmdi, sizeof(rmdi));
    
    jlen = MIN(*infolen, UPCMA_INFOLEN);
    jlen = MAX(0, jlen);
    for (j=0; j<jlen; j++) {
      info[j] = Info[j];
    }

    rc = jlen;
  }

  *retcode = rc;
}


void
pcma2cma_(const          int *can_swp_data,
	  const unsigned int  packed_data[],
	           const int *packed_len,
	           const int  idx[], const int *idxlen,
	           const int *fill_zeroth_cma,
                      double  cma[],
                   const int *lencma,
                         int *packed_count,
                         int *retcode)
{
  int rc = 0;
  int pklen = *packed_len;
  int cmalen = *lencma;
  const unsigned int *pdata = packed_data;
  double *ptr_cma = cma;

  *packed_count = 0;
  *retcode = 0;

  while (rc >= 0 && pklen > HDRLEN && cmalen > 0) { /* yes: HDRLEN, *not* PCMA_HDRLEN */
    int hdrlen = HDRLEN;
    int swp, new_version;
    const unsigned int *hdr  = pdata;
    const unsigned int *data;
    int method, replen, msgbytes;
    double nmdi, rmdi;

    rc = upcma_hdr(NULL, &swp,
		   (unsigned int *)hdr, 0,
		   &method, &replen, &msgbytes,
		   &nmdi, &rmdi,
		   &new_version);

    hdrlen += new_version;
    data = pdata + hdrlen;

    /*
    fprintf(stderr,
	    "pcma2cma_(): method=%d, replen=%d, msgbytes=%d, swp=%d, can_swp=%d, new_version=%d\n",
	    method, replen, msgbytes, swp, *can_swp_data, new_version);
    */

    if (rc != hdrlen) {
      /* Error : Possibly not a packed CMA-report at all */
      rc = -1;
    }
    else if (replen > cmalen) {
      /* NOT an error: Not enough room left for unpacked data in cma[] -array */
      break;
    }
    else {
      int count   = hdrlen + RNDUP(msgbytes,b4)/b4; /* Total no. of packed uint words in msg */
      int datalen = count - hdrlen;
      
      if (   method == 2
	  || method == 3
	  || method == 5
	  || method == 9
	  || method == 1
	  || (method >= 11 && method <= 19)
	  || (method >= 21 && method <= 29)
	  || (method >= 31 && method <= 39)
	  || method == 4
	  || method == 94
	  ) {
	rc = unpack_func_list[look_up[method]](method,
					       swp, *can_swp_data,
					       new_version,
					       data,
					       datalen,
					       msgbytes,
					       nmdi, rmdi,
					       NULL,
					       idx, *idxlen,
					       *fill_zeroth_cma,
					       ptr_cma, replen);

	/*
	if (replen >= 3) {
	  fprintf(stderr,
		  "\tunpack: rc=%d; replen=%d, cma[0]=%.20g; cma[1]=%.20g .. cma[last]=%.20g\n",
		  rc, replen, ptr_cma[0], ptr_cma[1], ptr_cma[replen-1]);
	}
	*/

	/* printf("rc=%d, count=%d, datalen=%d\n",rc, count, datalen); */

	if (rc != replen) {
	  /* Error: Unable to decode exactly the replen no. of CMA-words */
	  rc = -3;
	}
      }
      else {
	/* Error: Invalid packing method; or at least no unpacking available */
	rc = -4;
      }

      pklen  -= count;
      cmalen -= replen;
      *packed_count += count;
      *retcode += replen;
      pdata  += count;
      ptr_cma += replen;
    } /* else */

  } /* while (rc >= 0 && pklen > HDRLEN && cmalen > 0) */


  if (rc < 0) *retcode = rc;
}

