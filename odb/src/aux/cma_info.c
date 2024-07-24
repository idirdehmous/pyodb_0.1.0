#include "cmaio.h"

#ifndef MaxNPES
#define MaxNPES   256
#endif

PRIVATE int ZINFOMIN = 8;
PRIVATE int ZINFOLEN = (32 + 1 + 1 + 2 * MaxNPES);

/* Info-vector :

   1) No. of DDRs
   2) Total length of DDRs
   3) No. of CMA-reports
   4) Total length of report data
   5) Max. CMA-report length
   6) Max. obs. type
   7) =1 if the actual file on memory resident file system
   8) >0 if internally packed, =0 otherwise
   9-32) Reserved
  33) =1 if the file is MR2D-type concatenated file
  34) No. of chunks (normally equals to no. of PEs) concatenated into the file
  Followed by no. of chunk -word pairs (35,36), (37,38), ...
  35) First chunk id (=IFS PE no. that wrote the chunk)
  36) Size of the first chunk in bytes
  37) Second chunk id
  38) Size of the second chunk in bytes
  ...

*/

/* Borrowed from "/cc/rd/ifs/common/yomcmhdr.h" */

#define NCD1LN    1 /* FIRST DDR LEN. */
#define NCD1NL    8 /* LEN. OF NEXT DDR */
#define NCD1ND   18 /* NO. OF DDRS */
#define NCD1RM   22 /* MAX. LEN. OF ECMA REPORT */
#define NCD1MOT  26 /* MAX. NO. OF OBS. TYPES */
#define NCD1TO   27 /* TOTAL NO. OF OBS. */
#define NCD1AL   51 /* TOTAL LENGTH OF ECMA */

#define NCD2LN    1 /* LEN. OF SECOND DDR */
#define NCD2LND   8 /* LEN. OF NEXT DDR */


int
CMA_fetch_ddrs(const integer4 *unit,
	       IOcma *pcmaio)
{
  int rc = 0;

  real8 *zinfo = NULL;
  int zinfolen = 0;
  int j;
  int numddrs = 0;
  int lenddrs = 0;
  int ddrno = 0;
  Boolean ddrs_read = false;

  if (iostuff_debug) 
    fprintf(stderr,"CMA_fetch_ddrs(%d,read_counter=%d)\n",
	    *unit, pcmaio->ddr_read_counter);

  FREE(pcmaio->zinfo);
  pcmaio->zinfolen = 0;
  FREE(pcmaio->ddrs);
  pcmaio->lenddrs = 0;
  pcmaio->numddrs = 0;
  
  while (!ddrs_read) {
    integer4 retc;
    real8 oneword;
    const integer4 onewordlen = 1;
    real8 *ddr;
    integer4 ddrlen;
    
    cma_read_(unit, &oneword, &onewordlen, &retc);
    rc = retc;
    
    if (rc != onewordlen) goto finish;
    
    ddrlen = oneword;
    
    if (iostuff_debug) {
      fprintf(stderr," ddrlen=%d, oneword=%f\n",ddrlen,oneword);
    }
    
    ALLOC(ddr, ddrlen);
    ddr[0] = oneword;
    
    ddrlen--;
    cma_read_(unit, ddr+1, &ddrlen, &retc);
    rc = retc;
    
    if (rc != ddrlen) goto finish;
    ddrlen++;
    
    ddrno++;
	  
    if (ddrno == 1) {
      if (pcmaio->io.is_mrfs2disk) {
	int nchunk = MAX(0,pcmaio->io.concat);
	zinfolen = MIN(ZINFOLEN, 32 + 1 + 1 + 2 * nchunk);
      }
      else {
	zinfolen = ZINFOMIN;
      }

      ALLOC(zinfo, zinfolen);
      for (j=0; j<zinfolen; j++) zinfo[j] = 0;

      numddrs = ddr[NCD1ND-1];     /* No. of DDRs */
      zinfo[1-1] = numddrs;
      zinfo[2-1] = 0;
      zinfo[3-1] = ddr[NCD1TO-1];  /* No. of observations/reports */
      zinfo[4-1] = ddr[NCD1AL-1];  /* Total length of report data */
      zinfo[5-1] = ddr[NCD1RM-1];  /* Max. CMA-report length */
      zinfo[6-1] = ddr[NCD1MOT-1]; /* Max. obs. type */
      zinfo[7-1] = pcmaio->io.on_mrfs;    /* =1 if file on memory resident file system */
      zinfo[8-1] = MAX(0,pcmaio->io.packmethod); /* >0 if internally packed, =0 otherwise */
      
      pcmaio->numobs    = ddr[NCD1TO-1];
      pcmaio->cmalen    = ddr[NCD1AL-1];
      pcmaio->maxreplen = ddr[NCD1RM-1];
    }
    
    lenddrs += ddrlen;
    
    REALLOC(pcmaio->ddrs, lenddrs);
    IOrcopy(&pcmaio->ddrs[pcmaio->lenddrs], ddr, ddrlen);
    
    pcmaio->lenddrs = lenddrs;
    pcmaio->numddrs++;
    
    FREE(ddr);
    
    ddrs_read = (ddrno == numddrs) ? true : false;
  } /* while (!ddrs_read) */

  if (zinfo) {
    zinfo[2-1] = lenddrs; /* Total length of DDRs */

    if (pcmaio->io.is_mrfs2disk) {
      /* Handle info-words above 32+ */
      IObin *pbin = &pcmaio->io.bin[0];

      int nchunks = pbin->mr2d_info[0];
      zinfo[33-1] = pcmaio->io.is_mrfs2disk;
      
      IOrucopy(&zinfo[34-1], pbin->mr2d_info, 1 + 2 * nchunks);
    }

    ALLOC(pcmaio->zinfo, zinfolen);
    IOrcopy(pcmaio->zinfo, zinfo, zinfolen);
    
    pcmaio->zinfolen = zinfolen;

    FREE(zinfo);

    rc = zinfolen;
  }
  else {
    rc = 0;
  }

 finish:
  if (iostuff_debug) fprintf(stderr," : rc=%d\n", rc);

  return rc;
}


FORTRAN_CALL void 
cma_info_(const integer4 *unit, 
	  integer4        info[],
	  const integer4 *infolen,
	  integer4       *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);
  int bin = IOBIN(*unit);

  if (iostuff_debug) fprintf(stderr,"cma_info(%d)\n",index);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.is_inuse && pcmaio->io.read_only) {
      /* Only bin#0 matters currently */

      IObin *pbin = &pcmaio->io.bin[0];
      int mininfo;
      
      if (pbin->filepos == pbin->begin_filepos) {
	/* int leninfo = 0; */

	/* The very first time */
	pcmaio->ddr_read_counter = 0;
	rc = CMA_fetch_ddrs(unit, pcmaio);

	if (rc < 0) goto finish;
      }

      mininfo = pcmaio->zinfo ?  MIN(*infolen, pcmaio->zinfolen) : 0;
	
      if (iostuff_debug && pcmaio->zinfo) {
	int j;
	fprintf(stderr," : zinfo[0..%d] =",mininfo-1);
	for (j=0; j<mininfo; j++) fprintf(stderr," %g",pcmaio->zinfo[j]);
      }

      if (pcmaio->zinfo) IOircopy(info, pcmaio->zinfo, mininfo);

      if (mininfo < *infolen) {
	int j, n = *infolen;
	for (j=mininfo; j<n; j++) info[j] = 0;
      }
      
      rc = *infolen;
    }
    else if (pcmaio->io.is_inuse && !pcmaio->io.read_only) {
      /* Write only files: Use info-words above 32+ to write MR2D-information */

      if (bin >= 0 && bin < pcmaio->io.numbins) {
	IObin *pbin = &pcmaio->io.bin[bin];
	/* FILE *fp = pbin->fp; */
	int mininfo = *infolen;
	Boolean is_mr2d = false;

	if (mininfo >= 33 && pbin->filepos == pbin->begin_filepos) {
	  is_mr2d = (info[33-1] == 1) ? true : false;

	  if (is_mr2d) {
	    /* The file going to be an MR2D concatenated file */
	    int len;
	    int retcode;
	    int nchunks;
	    int *mr2d_data;

	    nchunks = info[34-1];
	    len = 1 + 1 + 2 * nchunks;

	    if (mininfo >= 32 + len) {
	      ALLOC(mr2d_data, len);

	      mr2d_data[0] = MR2D;
	      mr2d_data[1] = nchunks; /* Note: chunk length in bytes */
	      IOicopy(&mr2d_data[2], &info[35-1], len - 2);

	      cma_writei_(unit, mr2d_data, &len, &retcode);
	      
	      FREE(mr2d_data);
	    }
	    else {
	      retcode = -7; /* Error: Info vector too short */
	    }

	    rc = retcode; /* May contain a failure code, too */

	  } /* if (is_mr2d) */

	} /* if (mininfo >= 33 && pbin->filepos == 0) */
      }
      else {
	/* Error : Attempt to use an invalid bin */
	rc = -5;
      }
    }
    else {
      /* Error: File not in use i.e. reference not found */
      rc = -1;
    }
  }
  else {
    /* Error : Invalid internal file unit  */
    rc = -2;
  }

 finish:

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
  
}
