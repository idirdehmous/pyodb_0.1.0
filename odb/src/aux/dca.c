
/* dca.c */

#include "odb.h"
#include "odb_macros.h"
#include "dca.h"
#include "alloc.h"
#include "cmaio.h"
#include "magicwords.h"
#include "swapbytes.h"
#include "pcma_extern.h"
#include "cdrhook.h"

#define NIBUF 14  /* Do not change and keep consistent with tools/dcagen.c's value */
#define NDBUF_MAX 50

PRIVATE int dca_debug = -1;

#define DCA_DEBUG(hex) ((dca_debug > 0) && ((dca_debug & (hex)) == hex))

PRIVATE dca_chain_t *dca_chain = NULL;
PRIVATE dca_chain_t *dca_chain_last = NULL;
  
#define fast_physproc(poolno,nproc) (((poolno)-1)%nproc + 1)

extern void ODB_monitor_memory_addr(const void *p, const char *pwhat, const char *file, int linenum);
#define SETMEMOWR(p,pwhat) ODB_monitor_memory_addr(p,pwhat,__FILE__,__LINE__)

extern void ODB_check_memory_overwrite(const char *file, int linenum);
#define CHKMEMOWR() ODB_check_memory_overwrite(__FILE__, __LINE__)

PRIVATE o_lock_t DCA_mylock = 0; /* A specific OMP-lock; initialized only once in
				    odb/lib/codb.c, routine codb_init_omp_locks_() */

PUBLIC void
init_DCA_lock()
{
  INIT_LOCKID_WITH_NAME(&DCA_mylock,"dca.c:DCA_mylock");
}


PRIVATE uint 
HashIs(const char *s)
{
  const uint hashsize = 1031U;
  uint hashval = 0;
  for (; s && *s ; s++) {
    hashval = (*s) + 31U * hashval;
  }
  hashval = hashval % hashsize;
  return hashval;
}


PRIVATE void
DCA_Trace(int mode,
	  const char *name,
	  const dca_t *pdca,
	  const dca_chain_t *pchain,
	  int misc)
{
  if (pdca && pchain) {
    FILE *do_trace = ODB_trace_fp();
    coml_set_lockid_(&DCA_mylock);
    if (do_trace) {
      char msg[65536]; /* Enough ? */
      ODB_Trace TracE;
      TracE.handle = pchain->handle;
      TracE.msglen = sizeof(msg);
      TracE.msg = msg;
      TracE.args[0] = pdca->poolno;
      TracE.args[1] = pdca->colnum;
      TracE.args[2] = pdca->nrows;
      TracE.args[3] = pdca->nmdis;
      TracE.args[4] = pdca->offset;
      TracE.args[5] = pdca->length;
      TracE.args[6] = pdca->dtnum;
      TracE.args[7] = pdca->pmethod;
      TracE.args[8] = pdca->pmethod_actual;
      TracE.args[9] = pdca->is_little;
      TracE.args[10] = pdca->hash;
      TracE.args[11] = misc;
      TracE.numargs = 12; 
      TracE.mode = mode;
      sprintf(TracE.msg,
	      "%s(%s)[it#%d]: %s:%s@%s, (min,max,avg,card)=(%.14g,%.14g,%.14g,%d)",
	      name, pdca->filename, pchain->it,
	      pdca->dtname, pdca->colname, pchain->tblname,
	      pdca->min,pdca->max,pdca->avg,pdca->ncard);
      codb_trace_(&TracE.handle, &TracE.mode,
		  TracE.msg, TracE.args, 
		  &TracE.numargs, TracE.msglen);
    } /* if (do_trace) */
    coml_unset_lockid_(&DCA_mylock);
  }
}
  

PRIVATE void
IODCA_close(iodca_t *iodca)
{
  DRHOOK_START(IODCA_close);
  if (iodca) {
    if (iodca->unit != -1) {
      int iret;
      cma_close_(&iodca->unit, &iret);
      if (DCA_DEBUG(0x1)) {
	fprintf(stderr,
		"IODCA_close[file='%s', unit=%d, rewind=%d, offset=%d : iret=%d]\n",
		iodca->file, iodca->unit, iodca->can_rewind, iodca->offset, iret);
      }
    }
    iodca->unit = -1;
    iodca->offset = 0;
    iodca->can_rewind = 0;
    FREE(iodca->file);
  }
  DRHOOK_END(0);
}


PRIVATE int
IODCA_open(const char *file,
	   iodca_t *iodca)
{
  int rc = 0;
  DRHOOK_START(IODCA_open);
  IODCA_close(iodca);
  if (file) {
    int unit,iret;
    cma_open_(&unit, file, "r", &iret, strlen(file), 1);
    if (iret != 1) {
      fprintf(stderr,"***Error in IODCA_open: Unable to open file '%s'\n",file);
      unit = -1;
      rc = -1;
    }
    else {
      rc = iret;
      if (iodca) {
	int index = IOINDEX(unit);
	int tid = get_thread_id_();
	IOcma *pcmaio = &cmaio[tid][index];
	if (pcmaio && pcmaio->io.is_inuse) {
	  iodca->can_rewind = ((pcmaio->io.scheme & external) == external) ? 0 : 1;
	}
	else {
	  iodca->can_rewind = 0;
	}
	iodca->unit = unit;
	iodca->offset = 0;
	iodca->file = STRDUP(file);
      }
    }
  }
  DRHOOK_END(0);
  return rc;
}


PRIVATE int 
IODCA_getpos(const char *file,
	     int offset,
	     int length,
	     iodca_t *iodca,
	     const char *colname,
	     const char *tblname,
	     int poolno,
	     dca_chain_t *pchain)
{
  int rc = 0;
  if (file && iodca) {
    if (iodca->unit == -1 || 
	(!iodca->can_rewind && offset < iodca->offset) ||
	!strequ(iodca->file,file)) {
      int thesame = strequ(iodca->file,file);
      if (DCA_DEBUG(0x1)) {
	fprintf(stderr,
		"IODCA_getpos[file='%s' for %s@%s, pool#%d, cur.off=%d, off:len=%d:%d, chain=%p]\n",
		file,
		colname,tblname,poolno,iodca->offset,
		offset,length,pchain);
      }
      rc = IODCA_open(file,iodca);
      if (DCA_DEBUG(0x4)) {
	fprintf(stderr,
		">IODCA_getpos[Just %sopened='%s', unit=%d, rewindable=%d : rc=%d]\n",
		thesame ? "re-" : "",
		iodca->file, iodca->unit, iodca->can_rewind, rc);
      }
    }
  }
  return rc;
}


PRIVATE dca_chain_t *
Next_Free()
{
  dca_chain_t *pchain = NULL;
  coml_set_lockid_(&DCA_mylock);
  {
    pchain = dca_chain;
    while (pchain) {
      if (pchain->handle < 1) {
	break; /* Found an unused slot */
      }
      pchain = pchain->next;
    } /* while (pchain) */
    if (!pchain) {
      CALLOC(pchain,1);
      if (!dca_chain) {
	dca_chain = dca_chain_last = pchain;
      }
      else {
	dca_chain_last->next = pchain;
	dca_chain_last = dca_chain_last->next;
      }
      pchain->next = NULL;
    }
  }
  coml_unset_lockid_(&DCA_mylock);
  return pchain;
}


PUBLIC dca_chain_t *
DCA_alloc(int handle,
	  const char *dbname,
	  const char *tblname,
	  int *retcode)
     /* Read & allocate a DCA-file into internal data structures */
{
  int rc = 0;
  dca_chain_t *pchain = NULL;
  char *dcafile = NULL;
  DRHOOK_START(DCA_alloc);
  if (tblname && *tblname == '@') tblname++;
  if (handle > 0 && dbname && tblname) {
    int dcafile_len = strlen(dbname) + 1 + strlen(tblname) + 1 + 3 + 1;
    if (dca_debug < 0) {
      char *env = getenv("ODB_DCA_DEBUG");
      if (env) dca_debug = atoi(env);
      if (dca_debug < 0) dca_debug = 0;
    }
    pchain = Next_Free();
    ALLOC(pchain->iodca,1);
    pchain->iodca->unit = -1;
    pchain->iodca->offset = 0;
    pchain->iodca->can_rewind = 0;
    pchain->iodca->file = NULL;
    pchain->handle = 0; /* Not yet allocated (but ok'ayed) */
    pchain->it = get_thread_id_();
    pchain->dbname = NULL;
    pchain->tblname = NULL;
    ALLOC(dcafile,dcafile_len);
    sprintf(dcafile,"%s.%s.dca",dbname,tblname);
    pchain->dcafile = NULL;
    pchain->dca = NULL;
    pchain->dcalen = 0;
    pchain->ncols = 0;
    pchain->min_poolno = 2147483647;
    pchain->max_poolno = 0;
    pchain->min_hash = 2147483647U;
    pchain->max_hash = 0;
  } /* if (handle > 0 && dbname && tblname) */

  if (DCA_DEBUG(0x1)) {
    fprintf(stderr, 
	    "DCA_alloc(pchain=%p, handle=%d, dbname='%s', tblname='%s' : dcafile='%s')\n",
	    pchain, handle, 
	    dbname ? dbname : NIL,
	    tblname ? tblname : NIL,
	    dcafile ? dcafile : NIL);
  }

  if (pchain) {
    int unit, iret;
    FILE *fp = NULL;

    cma_open_(&unit, dcafile, "r", &iret,
	      strlen(dcafile), 1);
    fp = (iret == 1) ? CMA_get_fp(&unit) : NULL;

    if (fp) {
      dca_t this_dca;
      char colname[MAXLINE+1];
      char dtname[MAXLINE+1];
      char filename[MAXLINE+1];
      char line[MAXLINE+1];
      int iter = 1;
      int is_binary = 0;
      unsigned int magic_word;
      const int magic_word_len = sizeof(magic_word);
      int swap_dca = 0;
      short int nbuf[2];
      const int nbuf_len = sizeof(nbuf);
      int ibuf[NIBUF];  /* fixed for life, but ... */
      double dbuf[NDBUF_MAX]; /* ... this permits any future expansions up to NDBUF_MAX cells */
      int ilen, nibuf, ndbuf;
      int j = 0;
      int myproc = 1, nproc = 1;
      int errflg = 0;
      codb_procdata_(&myproc, &nproc, NULL, NULL, NULL);

      cma_readb_(&unit, (byte1 *)&magic_word, &magic_word_len, &iret);
      if (iret == magic_word_len) {
	is_binary = (magic_word == DCA2 || magic_word == _2ACD);
	if (is_binary) {
	  swap_dca = (magic_word == _2ACD);
	  cma_readb_(&unit, (byte1 *)&nbuf, &nbuf_len, &iret);
	  if (iret == nbuf_len) {
	    ilen = 2;
	    if (swap_dca) swap2bytes_(&nbuf, &ilen);
	    nibuf = nbuf[0]; ndbuf = nbuf[1];
	    if (nibuf != NIBUF) {
	      fprintf(stderr,
		      "***Error: Invalid value for nibuf = %d in presumably binary DCA-file '%s'."
		      " Must always be equal to %d\n",
		      nibuf,dcafile,NIBUF);
	      errflg++;
	    }
	    if (ndbuf < 0 || ndbuf > NDBUF_MAX) {
	      fprintf(stderr,
		      "***Error: Invalid value for ndbuf = %d in presumably binary DCA-file '%s'."
		      " Must be between 0 and %d\n",
		      ndbuf,dcafile,NDBUF_MAX);
	      errflg++;
	    }
	  }
	  else {
	    fprintf(stderr,
		    "***Error: Unable to read 'nbuf'-infos from presumably binary DCA-file '%s'\n",
		    dcafile);
	    errflg++;
	  }
	}
	if (!is_binary) {
	  cma_rewind_(&unit, &iret);
	  fp = CMA_get_fp(&unit); /* need this, if file io_scheme was external */
	}
      }
      else if (iret != -1) { /* Exclude EOF from errors, though */
	fprintf(stderr,
		"***Error: The DCA-file '%s' has illegal text/binary format\n",
		dcafile);
	errflg++;
      }

      if (errflg) RAISE(SIGABRT);

      do {
	if (iter == 2) {
	  cma_rewind_(&unit, &iret);
	  fp = CMA_get_fp(&unit); /* need this, if file io_scheme was external */
	  pchain->dcalen = rc;
	  CALLOC(pchain->dca,rc);
	  j = 0;
	}

	if (is_binary) {
	  int poolno, *lens;
	  const int binwhence = 1; /* SEEK_CUR (= seek from the current pos) */
	  int binoff = magic_word_len + nbuf_len;
	  if (iter == 2) cma_seekb_(&unit, &binoff, &binwhence, &iret);
	  for (;;) {
	    dca_t *pdca = NULL;
	    ilen = nibuf * sizeof(*ibuf);
	    cma_readb_(&unit, (byte1 *)ibuf, &ilen, &iret);
	    if (iret != ilen) {
	      if (iret == -1) break; /* EOF */
	      fprintf(stderr,
		      "***Error: Unable to read the first %d ibuf-words from DCA-file"
		      " '%s' (iter=%d) : iret=%d\n", 
		      nibuf, dcafile, iter, iret);
	      RAISE(SIGABRT);
	    }
	    if (swap_dca) swap4bytes_(ibuf, &nibuf);
	    poolno = ibuf[0];
	    lens = &ibuf[1];
	    if (poolno > 0 &&
		fast_physproc(poolno,nproc) == myproc &&
		ODB_in_permanent_poolmask(handle,poolno)) {
	      if (iter == 1) rc++; else pdca = &pchain->dca[j];
	    }

	    binoff = ndbuf * sizeof(*dbuf) + lens[0] + lens[1] + lens[2];

	    if (pdca) {
	      binoff = 0; /* We do not want to skip anything over */
	      ilen = ndbuf * sizeof(*dbuf);
	      cma_readb_(&unit, (byte1 *)dbuf, &ilen, &iret);
	      if (iret != ilen) {
		fprintf(stderr,
			"***Error: Unable to read the last %d dbuf-words from DCA-file"
			" '%s' (iter=%d) : iret=%d\n", 
			ndbuf, dcafile, iter, iret);
		RAISE(SIGABRT);
	      }
	      if (swap_dca) swap8bytes_(&dbuf, &ndbuf);

	      pdca->poolno = poolno;

	      if (nibuf >  4) pdca->dtnum = ibuf[4];
	      if (nibuf >  5) pdca->colnum = ibuf[5];
	      if (nibuf >  6) pdca->length = ibuf[6];
	      if (nibuf >  7) pdca->pmethod = ibuf[7];
	      if (nibuf >  8) pdca->pmethod_actual = ibuf[8];
	      if (nibuf >  9) pdca->nrows = ibuf[9];
	      if (nibuf > 10) pdca->nmdis = ibuf[10];
	      if (nibuf > 11) pdca->cr = ibuf[11];
	      if (nibuf > 12) pdca->ncard = ibuf[12];
	      if (nibuf > 13) pdca->is_little = ibuf[13];
	      
	      if (ndbuf >  0) pdca->offset = dbuf[0];
	      if (ndbuf >  1) pdca->avg = dbuf[1];
	      if (ndbuf >  2) pdca->min = dbuf[2];
	      if (ndbuf >  3) pdca->max = dbuf[3];

	      ALLOC(pdca->colname, lens[0] + 1);
	      cma_readb_(&unit, (byte1 *)pdca->colname, &lens[0],  &iret);
	      if (iret != lens[0]) {
		fprintf(stderr,
			"***Error: Unable to read column name (%d bytes) from DCA-file"
			" '%s' (iter=%d) : iret=%d\n", lens[0], dcafile, iter, iret);
		RAISE(SIGABRT);
	      }
	      pdca->colname[lens[0]] = '\0';
	      {
		char *at = strchr(pdca->colname,'@');
		if (at) *at = '\0';
	      }
	      pdca->hash = HashIs(pdca->colname);

	      ALLOC(pdca->dtname, lens[1] + 1);
	      cma_readb_(&unit, (byte1 *)pdca->dtname, &lens[1],  &iret);
	      if (iret != lens[1]) {
		fprintf(stderr,
			"***Error: Unable to read column data type (%d bytes) from DCA-file"
			" '%s' (iter=%d) : iret=%d\n", lens[1], dcafile, iter, iret);
		RAISE(SIGABRT);
	      }
	      pdca->dtname[lens[1]] = '\0';
	      
	      ALLOC(pdca->filename, lens[2] + 1);
	      cma_readb_(&unit, (byte1 *)pdca->filename, &lens[2],  &iret);
	      if (iret != lens[2]) {
		fprintf(stderr,
			"***Error: Unable to read column data file name (%d bytes) from DCA-file"
			" '%s' (iter=%d) : iret=%d\n", lens[2], dcafile, iter, iret);
		RAISE(SIGABRT);
	      }
	      pdca->filename[lens[2]] = '\0';
	      
	      if (pdca->colnum + 1 > pchain->ncols) pchain->ncols = pdca->colnum + 1;
	      if (pdca->poolno < pchain->min_poolno) pchain->min_poolno = pdca->poolno;
	      if (pdca->poolno > pchain->max_poolno) pchain->max_poolno = pdca->poolno;
	      if (pdca->hash < pchain->min_hash) pchain->min_hash = pdca->hash;
	      if (pdca->hash > pchain->max_hash) pchain->max_hash = pdca->hash;
	      if (++j >= pchain->dcalen) break; /* for (;;) */
	    } /* if (pdca) */

	    if (binoff > 0) {
	      cma_seekb_(&unit, &binoff, &binwhence, &iret);
	    }
	  } /* for (;;) */
	}
	else {
	  while (!feof(fp) && fgets(line, MAXLINE, fp)) {
	    const char *p = line;
	    if (*p != '#') {
	      dca_t *pdca = (iter == 1) ? &this_dca : &pchain->dca[j];
	      int nelem = sscanf(line, 
				 "%d %s %s %u %s %d %llu %d %d %d %d %d %lf %lf %lf %lf %d %d",
				 &pdca->colnum, colname, dtname, &pdca->dtnum,
				 filename, &pdca->poolno, &pdca->offset, &pdca->length,
				 &pdca->pmethod, &pdca->pmethod_actual,
				 &pdca->nrows, &pdca->nmdis,
				 &pdca->avg, &pdca->min, &pdca->max,
				 &pdca->cr, &pdca->ncard, &pdca->is_little);
	      if (iter == 1) {
		if (nelem == 18 && 
		    pdca->poolno > 0 &&
		    fast_physproc(pdca->poolno,nproc) == myproc &&
		    ODB_in_permanent_poolmask(handle,pdca->poolno)) rc++;
	      }
	      else { /* iter == 2 */
		if (nelem == 18 && 
		    pdca->poolno > 0 &&
		    fast_physproc(pdca->poolno,nproc) == myproc &&
		    ODB_in_permanent_poolmask(handle,pdca->poolno)) {
		  char *at = strchr(colname,'@');
		  if (at) *at = '\0';
		  pdca->hash     = HashIs(colname);
		  pdca->colname  = STRDUP(colname);
		  pdca->dtname   = STRDUP(dtname);
		  pdca->filename = STRDUP(filename);
		  if (pdca->colnum + 1 > pchain->ncols) pchain->ncols = pdca->colnum + 1;
		  if (pdca->poolno < pchain->min_poolno) pchain->min_poolno = pdca->poolno;
		  if (pdca->poolno > pchain->max_poolno) pchain->max_poolno = pdca->poolno;
		  if (pdca->hash < pchain->min_hash) pchain->min_hash = pdca->hash;
		  if (pdca->hash > pchain->max_hash) pchain->max_hash = pdca->hash;
		  if (++j >= pchain->dcalen) break; /* while (!feof(fp) && fgets(line, MAXLINE, fp)) */
		}
	      }
	    } /* if (*p != '#') */
	  } /* while (!feof(fp) && fgets(line, MAXLINE, fp)) */
	} /* if (is_binary) ... else ... */

	iter++;
	clearerr(fp);
      } while (rc > 0 && iter <= 2);

      cma_close_(&unit, &iret);

      if (rc > 0 && j == rc) { /* now all ok */
	dca_t *pdca = pchain->dca;
	pchain->handle = handle;
	pchain->it = get_thread_id_();
	pchain->dbname = STRDUP(dbname);
	pchain->tblname = STRDUP(tblname);
	pchain->dcafile = STRDUP(dcafile);
	if (DCA_DEBUG(0x16)) {
	  int poolno = -1;
	  char *env = getenv("ODB_DCA_DEBUG_POOLNO");
	  if (env) poolno = atoi(env);
	  DCA_debugprt(stderr, handle, dbname, tblname, NULL, poolno);
	}
      }
      else if (pchain->dcalen > 0) { /* not ok, but try to reclaim some space anyway */
	dca_t *pdca = pchain->dca;
	for (j=0; j<pchain->dcalen; j++) {
	  FREE(pdca->colname);
	  FREE(pdca->dtname);
	  FREE(pdca->filename);
	  pdca++;
	}
      }
    } /* if (fp) */
  } /* if (pchain) */
  FREE(dcafile);

  if (pchain && rc == 0) { /* still nothing found ; 
			      perhaps DCA-file not there or 
			      no data available for this tblname ? */
    /* Find at least number of column -- in a hard way then 
       (the info is indeed buried in each table's data struc) */
    int Ncols = 0;
    int poolno = -1;
    int len;
    char *at_tblname;
    if (*tblname == '@') tblname++;
    len = strlen(tblname) + 1;
    ALLOCX(at_tblname,len+1);
    sprintf(at_tblname,"@%s",tblname);
    codb_getsize_(&handle,
		  &poolno,
		  at_tblname,
		  NULL,
		  &Ncols,
		  NULL,
		  NULL,
		  NULL,
		  /* Hidden arguments */
		  len);
    FREEX(at_tblname);
    if (DCA_DEBUG(0x1)) {
      fprintf(stderr, ">DCA_alloc(pchain=%p : Ncols=%d)\n", pchain, Ncols);
    }
    if (Ncols > 0) {
      pchain->handle = handle;
      pchain->it = get_thread_id_();
      pchain->dbname = STRDUP(dbname);
      pchain->tblname = STRDUP(tblname);
      pchain->dcafile = STRDUP(dcafile);
      pchain->ncols = Ncols;
      rc = 1;
    }
  }
  if (retcode) *retcode = rc;
  if (DCA_DEBUG(0x1)) {
    fprintf(stderr, ">DCA_alloc(pchain=%p : rc=%d)\n", pchain, rc);
  }
  DRHOOK_END(0);
  return (rc > 0) ? pchain : NULL;
}


PUBLIC int
DCA_free(int handle)
{
  int num_freed = 0;
  DRHOOK_START(DCA_free);
  coml_set_lockid_(&DCA_mylock);
  if (dca_chain) {
    if (DCA_DEBUG(0x8)) {
      fprintf(stderr,"DCA_free: handle=%d\n",handle);
    }
    if (handle > 0) {
      dca_chain_t *pchain = dca_chain;
      while (pchain) {
	if (pchain->handle == handle) {
	  if (DCA_DEBUG(0x8)) {
	    fprintf(stderr,"DCA_free: Freeing handle=%d, chain=%p\n",handle,pchain);
	  }
	  IODCA_close(pchain->iodca);
	  FREE(pchain->iodca);
	  pchain->handle = -handle; /* Free'd handle */
	  FREE(pchain->dbname);
	  FREE(pchain->tblname);
	  FREE(pchain->dcafile);
	  if (pchain->dca && pchain->dcalen > 0) {
	    dca_t *pdca = pchain->dca;
	    int j;
	    for (j=0; j<pchain->dcalen; j++) {
	      FREE(pdca->colname);
	      FREE(pdca->dtname);
	      FREE(pdca->filename);
	      pdca++;
	    }
	  }
	  FREE(pchain->dca);
	  pchain->dcalen = 0;
	  num_freed++;
	} /* if (pchain->handle == handle && pchain->it == it) */
	pchain = pchain->next;
      } /* while (pchain) */
    } /* if (handle > 0) */
  }
  coml_unset_lockid_(&DCA_mylock);
  DRHOOK_END(0);
  return num_freed;
}


PUBLIC int
DCA_getsize(int handle,
	    const char *dbname,
	    const char *tblname,
	    int poolno,
	    int *nrows,
	    int *ncols)
{
  int found = 0;
  int Nrows = 0;
  int Ncols = 0;
  DRHOOK_START(DCA_getsize);
  if ((nrows || ncols) && handle > 0 && dbname && tblname) {
    int it = get_thread_id_();
    dca_chain_t *pchain = NULL;
    if (*tblname == '@') tblname++;
    coml_set_lockid_(&DCA_mylock);
    pchain = dca_chain;
    while (pchain) {
      if (pchain->handle == handle &&
	  pchain->it == it &&
	  strequ(pchain->dbname, dbname) &&
	  strequ(pchain->tblname, tblname)) {
	break;
      }
      pchain = pchain->next;
    }
    if (DCA_DEBUG(0x64)) {
      fprintf(stderr, 
	      "DCA_getsize(handle=%d,dbname='%s',tblname='%s',poolno=%d) : pchain=%p\n",
	      handle, dbname?dbname:NIL, tblname?tblname:NIL,
	      poolno, pchain);
    }
    coml_unset_lockid_(&DCA_mylock);
    if (pchain && 
	poolno >= pchain->min_poolno && poolno <= pchain->max_poolno) { /* found */
      dca_t *pdca = pchain->dca;
      int j;
      for (j=0; j<pchain->dcalen; j++) {
	if (pdca->poolno == poolno) { /* The first match is already sufficient */
	  Nrows  = pdca->nrows;
	  found = 1;
	  break;
	}
	pdca++;
      } /* for (j=0; j<pchain->dcalen; j++) */
    }
    if (pchain) {
      Ncols = pchain->ncols; /* Note: given independently of the pool range */
      if (!found) found = 2;
    }
    if (DCA_DEBUG(0x64)) {
      fprintf(stderr, 
	      ">DCA_getsize : Nrows=%d, Ncols=%d, found=%d\n",
	      Nrows, Ncols, found);
    }
  }
  if (nrows) *nrows = Nrows;
  if (ncols) *ncols = Ncols;
  DRHOOK_END(0);
  return found;
}


PRIVATE int
Fetch_Raw_Data(const char *dbname,
	       void *ptr,
	       dca_t *pdca,
	       dca_chain_t *pchain)
{
  int rc = 0;
  DRHOOK_START(Fetch_Raw_Data);
  if (dbname && ptr && pchain && pdca &&
      pdca->filename && pdca->length > 0) {
    char *filename = NULL;
    int allocated_filename;

    if (*pdca->filename == '$' || 
	*pdca->filename == '.' ||
	*pdca->filename == '/') {
      filename = pdca->filename;
      allocated_filename = 0;
    }
    else {
      char *env;
      char *s = NULL;
      int slen = strlen("ODB_DATAPATH_") + strlen(dbname) + 1;
      ALLOC(s,slen);
      sprintf(s,"%s%s","ODB_DATAPATH_",dbname);
      env = getenv(s);
      FREE(s);
      if (env) {
	int len = strlen("$ODB_DATAPATH_") + strlen(dbname) + 1 + strlen(pdca->filename) + 1;
	ALLOC(filename,len);
	sprintf(filename,"%s%s/%s","$ODB_DATAPATH_",dbname,pdca->filename);
      }
      else {
	env = getenv("ODB_DATAPATH");
	if (env) {
	  int len = strlen("$ODB_DATAPATH") + 1 + strlen(pdca->filename) + 1;
	  ALLOC(filename,len);
	  sprintf(filename,"%s/%s","$ODB_DATAPATH",pdca->filename);
	}
	else {
	  int len = 2 + strlen(pdca->filename) + 1;
	  ALLOC(filename,len);
	  sprintf(filename,"./%s",pdca->filename);
	}
      }
      allocated_filename = 1;
    }

    if (pchain->iodca && filename) {
      int errflg = 0;
      int iret;
      u_ll_t offset = pdca->offset;

      if (DCA_DEBUG(0x2)) {
	fprintf(stderr,
		"Fetch_Raw_Data[db=%s, var=%s:%s%s%s, pool#%d, file=%s, "
		"off=%llu, len=%d, nrows=%d]\n",
		dbname,
		pdca->dtname, pdca->colname,
		(pchain && pchain->tblname) ? "@" : "",
		(pchain && pchain->tblname) ? pchain->tblname : "",
		pdca->poolno, filename, pdca->offset, pdca->length, pdca->nrows);
      }

      iret = IODCA_getpos(filename, offset, pdca->length, pchain->iodca, 
			  pdca->colname, pchain->tblname, pdca->poolno, pchain);
      if (iret >= 0) {
	const int whence = 1; /* SEEK_CUR */
	int ioff = offset - pchain->iodca->offset;

	cma_seekb_(&pchain->iodca->unit, &ioff, &whence, &iret);
	if (iret < 0) {
	  errflg = -3;
	  goto error_processing;
	}

	cma_readb_(&pchain->iodca->unit, ptr, &pdca->length, &iret);
	if (iret != pdca->length) {
	  errflg = -4;
	  goto error_processing;
	}

	pchain->iodca->offset = offset + pdca->length; /* current data position */
	rc = pdca->length;
      } 
      else {
	errflg = -1;
	goto error_processing;
      }

    error_processing:
      if (errflg) {
	fprintf(stderr,
		"***Error: Unable to fetch data column='%s%s%s'"
		" of pool#%d from file='%s' : last iret=%d, errflg=%d\n",
		pdca->colname, 
		(pchain && pchain->tblname) ? "@" : "",
		(pchain && pchain->tblname) ? pchain->tblname : "",
		pdca->poolno, filename, iret, errflg);
	rc = errflg;
	goto finish;
      }
    }
    if (allocated_filename) { /* for the next time */
      char *refname = STRDUP(pdca->filename);
      /*
      fprintf(stderr,"Fetch_Raw_Data(pchain=%p,pdca=%p):",pchain,pdca);
      fprintf(stderr," filename (truename) ='%s', refname='%s'\n",filename,refname);
      */
      /* do the favor for every file that has the same char. string in pdca->filename */
      if (pchain) {
	dca_t *Pdca = pchain->dca;
	int j;
	for (j=0; j<pchain->dcalen; j++) {
	  if (strequ(Pdca->filename,refname)) {
	    FREE(Pdca->filename);
	    Pdca->filename = STRDUP(filename);
	  }
	  Pdca++;
	}
      }
      FREE(refname);
      FREE(filename);
    } /* if (allocated_filename) */
  }
 finish:
  DRHOOK_END(0);
  return rc;
}

PRIVATE void
swap_nothing(void *v, const int *vlen) { }

#define SWAPIT(type, nonfpe_type, swapfunc) \
{ \
  if (idx && idxlen > 0) { \
    int j; \
    /* Using types that do not trigger SIGFPEs when doing d[j] = vdata[idx[j]] */ \
    /* SIGFPEs are possible especially when getting data from different endian */ \
    /* Please note that sizeof(type) must be the the same as sizeof(nonfpe_type) !! */ \
    const nonfpe_type *vdata = data; \
    nonfpe_type *d = NULL; \
    if (sizeof(type) != sizeof(nonfpe_type)) { \
      fprintf(stderr,"***Programming error: sizeof(%s) = %d not equal to sizeof(%s) = %d\n", \
	      #type, (int)sizeof(type), #nonfpe_type, (int)sizeof(nonfpe_type)); \
      RAISE(SIGABRT); \
    } \
    ALLOC(d,idxlen); \
    for (j=0; j<idxlen; j++) { \
      d[j] = vdata[idx[j]]; \
    } \
    data = d; \
    nrows = idxlen; \
  } \
  if (swp) swapfunc(data, &nrows); \
}

PRIVATE void *
Byte_Swap(void *data, 
	  int nrows, 
	  uint dtnum,
	  int swp,
	  const int idx[],
	  int idxlen,
	  const char *dtname,
	  const char *colname,
	  const char *tblname)
{
  if (nrows > 0) {
    if (dtnum == DATATYPE_REAL8) {
      SWAPIT(double, u_ll_t, swap8bytes_);
    }
    else if (dtnum == DATATYPE_INT4) {
      SWAPIT(int, int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_LINKOFFSET) {
      SWAPIT(linkoffset_t, linkoffset_t, swap4bytes_);
    }
    else if (dtnum == DATATYPE_LINKLEN) {
      SWAPIT(linklen_t, linklen_t, swap4bytes_);
    }
    else if (dtnum == DATATYPE_YYYYMMDD) {
      SWAPIT(yyyymmdd, yyyymmdd, swap4bytes_);
    }
    else if (dtnum == DATATYPE_HHMMSS) {
      SWAPIT(hhmmss, hhmmss, swap4bytes_);
    }
    else if (dtnum == DATATYPE_BITFIELD) {
      SWAPIT(int, int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_STRING) {
      SWAPIT(string, u_ll_t, swap8bytes_);
    }
    else if (dtnum == DATATYPE_BUFR) {
      SWAPIT(bufr, bufr, swap4bytes_);
    }
    else if (dtnum == DATATYPE_GRIB) {
      SWAPIT(grib, grib, swap4bytes_);
    }
    else if (dtnum == DATATYPE_UINT4) {
      SWAPIT(unsigned int, unsigned int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_REAL4) {
      SWAPIT(float, unsigned int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_INT2) {
      SWAPIT(short int, short int, swap2bytes_);
    }
    else if (dtnum == DATATYPE_UINT2) {
      SWAPIT(unsigned short int, unsigned short int, swap2bytes_);
    }
    else if (dtnum == DATATYPE_INT1) {
      SWAPIT(char, char, swap_nothing);
    }
    else if (dtnum == DATATYPE_UINT1) {
      SWAPIT(unsigned char, unsigned char, swap_nothing);
    }
    else {
      fprintf(stderr,
	      "***Fatal error in Byte_Swap[%s:%d]: Unsupported type; dtnum=0x%x for %s:%s@%s\n",
	      __FILE__,__LINE__,dtnum,
	      dtname,colname,tblname);
      RAISE(SIGABRT);
    }
  }
  return data;
}

#define CONVIT(type) \
{ \
  type *d; \
  if (idx && idxlen > 0) { \
    ALLOC(d,idxlen); \
    if (const_value) { \
      type c = *const_value; \
      for (j=0; j<idxlen; j++) d[j] = c; \
    } \
    else { \
      for (j=0; j<idxlen; j++) d[j] = data[idx[j]]; \
    } \
    nrows = idxlen; \
  } \
  else { \
    if (const_value) { \
      type c = *const_value; \
      ALLOC(d,nrows); \
      for (j=0; j<nrows; j++) d[j] = c; \
    } \
    else { \
      if (!fill_zeroth_cma && \
          (dtnum == DATATYPE_REAL8  || dtnum == DATATYPE_STRING)) { \
        d = (type *)data; \
      } \
      else { \
        ALLOC(d,nrows); \
        for (j=0; j<nrows; j++) d[j] = data[j]; \
      } \
    } \
  } \
  out_ptr = d; \
}

PRIVATE void *
Data_Conv(int fill_zeroth_cma,
	  double *Data, 
	  int nrows,
	  uint dtnum,
	  const double *const_value,
	  const int idx[],
	  int idxlen,
	  const char *dtname,
	  const char *colname,
	  const char *tblname)
{
  void *out_ptr = NULL;
  DRHOOK_START(Data_Conv);
  if (nrows > 0) {
    double *data = Data ? &Data[fill_zeroth_cma] : NULL;
    int j;
    if (dtnum == DATATYPE_REAL8) {
      CONVIT(double);
    }
    else if (dtnum == DATATYPE_INT4) {
      CONVIT(int);
    }
    else if (dtnum == DATATYPE_LINKOFFSET) {
      CONVIT(linkoffset_t);
    }
    else if (dtnum == DATATYPE_LINKLEN) {
      CONVIT(linklen_t);
    }
    else if (dtnum == DATATYPE_YYYYMMDD) {
      CONVIT(int);
    }
    else if (dtnum == DATATYPE_HHMMSS) {
      CONVIT(int);
    }
    else if (dtnum == DATATYPE_BITFIELD) {
      CONVIT(int);
    }
    else if (dtnum == DATATYPE_STRING) {
      CONVIT(string);
    }
    else if (dtnum == DATATYPE_BUFR) {
      CONVIT(bufr);
    }
    else if (dtnum == DATATYPE_GRIB) {
      CONVIT(grib);
    }
    else if (dtnum == DATATYPE_UINT4) {
      CONVIT(unsigned int);
    }
    else if (dtnum == DATATYPE_REAL4) {
      CONVIT(float);
    }
    else if (dtnum == DATATYPE_INT2) {
      CONVIT(short int);
    }
    else if (dtnum == DATATYPE_UINT2) {
      CONVIT(unsigned short int);
    }
    else if (dtnum == DATATYPE_INT1) {
      CONVIT(char);
    }
    else if (dtnum == DATATYPE_UINT1) {
      CONVIT(unsigned char);
    }
    else {
      fprintf(stderr,
	      "***Fatal error in Data_Conv[%s:%d]: Unsupported"
	      " datatype; dtnum=0x%x for %s:%s@%s\n",
	      __FILE__,__LINE__,dtnum,
	      dtname,colname,tblname);
      RAISE(SIGABRT);
    }
  }
  DRHOOK_END(nrows);
  return out_ptr;
}


PRIVATE int
IsConst(const dca_t *pdca,
	const dca_chain_t *pchain,
	double *value)
{
  int rc = 0;
  double Value = 0;
  DRHOOK_START(IsConst);
  if (pdca && pchain) {
    if (pdca->nrows > 0) {
      FILE *do_trace = ODB_trace_fp();
      if (pdca->nrows == pdca->nmdis) { 
	/* all values are (some sort of) missing data */
	int basetype = EXTRACT_BASETYPE(pdca->dtnum);
	if (basetype == 1) { /* an integer of some kind ==> NMDI */
	  pcma_get_mdis_(&Value, NULL); /* NMDI */
	  rc = 1;
	  if (do_trace) DCA_Trace(-1,"IsConst: all NMDI",pdca,pchain,rc);
	}
	else if (basetype == 2) { /* a real of some kind ==> RMDI */
	  pcma_get_mdis_(NULL, &Value); /* RMDI */
	  rc = 2;
	  if (do_trace) DCA_Trace(-1,"IsConst: all RMDI",pdca,pchain,rc);
	}
      }
      else if (pdca->min == pdca->max && pdca->ncard == 1) { 
	/* all values are the same */
	Value = pdca->min;
	rc = 3;
	if (do_trace) DCA_Trace(-1,"IsConst: min == max and cardinality == 1",pdca,pchain,rc);
      }
    } /* if (pdca->nrows > 0) */
  }
  if (value) *value = Value;
  DRHOOK_END(0);
  return rc;
}


PUBLIC void *
DCA_fetch(int handle,
	  const char *dbname,
	  const char *tblname,
	  const char *colname,
	  int poolno,
	  int unpack,
	  const int idx[],
	  int idxlen,
	  int *nrows,
	  int *nbytes,
	  uint *datatype,
	  int *offset)
{
  void *ptr = NULL;
  int Nrows = 0;
  int Nbytes = 0;
  uint Datatype = 0;
  int ioffset = 0;
  DRHOOK_START(DCA_fetch);
  if (!tblname && colname) tblname = strchr(colname, '@');
  if (handle > 0 && dbname && tblname && colname) {
    int it = get_thread_id_();
    dca_chain_t *pchain = NULL;
    char *p_colname = STRDUP(colname);
    char *p_at = strchr(p_colname, '@');
    char *poffset = GET_OFFSET(p_colname);
    if (p_at) *p_at = '\0';
    if (poffset) {
      int sign = 1;
      int inc = 0;
      *poffset++ = '\0';
      if (*poffset == '_' || *poffset == '-') { sign = -1; inc = 1; }
      else if (*poffset == '+') { sign = 1; inc = 1; }
      ioffset = sign * atoi(poffset+inc);
    }
    if (*tblname == '@') tblname++;
    coml_set_lockid_(&DCA_mylock);
    pchain = dca_chain;
    while (pchain) {
      if (pchain->handle == handle &&
	  pchain->it == it &&
	  strequ(pchain->dbname, dbname) &&
	  strequ(pchain->tblname, tblname)) {
	break;
      }
      pchain = pchain->next;
    }
    coml_unset_lockid_(&DCA_mylock);
    if (pchain && 
	poolno >= pchain->min_poolno && 
	poolno <= pchain->max_poolno) { /* poolno range found */
      uint hash = HashIs(p_colname);
      if (pchain->dca && pchain->dcalen > 0 &&
	  hash >= pchain->min_hash && hash <= pchain->max_hash) { /* even better! */
	FILE *do_trace = ODB_trace_fp();
	int errflg = 0;
	dca_t *pdca = pchain->dca;
	int j;
	for (j=0; j<pchain->dcalen; j++) {
	  if (pdca->poolno == poolno &&
	      pdca->hash == hash &&
	      strequ(pdca->colname, p_colname)) { /* ... and this is the exact match */
	    double value = 0;
	    int is_const = IsConst(pdca, pchain, &value);
	    Nrows = pdca->nrows;
	    Datatype = pdca->dtnum;
	    if (is_const) {
	      /* Constant data is found based on DCA-record */
	      Nbytes = 0; /* indicates that no I/O was performed */
	      ptr = Data_Conv(0,NULL,Nrows,pdca->dtnum,&value,idx,idxlen,
			      pdca->dtname,pdca->colname,pchain->tblname);
	      if (idx && idxlen > 0) Nrows = idxlen;
	      if (DCA_DEBUG(0x2)) {
		fprintf(stderr,
			"IsConst[db=%s, var=%s:%s%s%s, pool#%d, value=%.14g, "
			"nrows=%d, idx=%p, idxlen=%d] : ptr=%p (dtnum=0x%x)\n",
			dbname,
			pdca->dtname, pdca->colname,
			(pchain && pchain->tblname) ? "@" : "",
			(pchain && pchain->tblname) ? pchain->tblname : "",
			pdca->poolno, value, pdca->nrows,
			idx, idxlen,
			ptr, pdca->dtnum);
	      }
	    }
	    else {
	      char *chptr = NULL;
	      ALLOC(chptr,pdca->length);
	      if (do_trace) DCA_Trace(1,"Fetch_Raw_Data",pdca,pchain,pdca->length);
	      Nbytes = Fetch_Raw_Data(dbname,chptr,pdca,pchain);
	      if (do_trace) DCA_Trace(0,"Fetch_Raw_Data",pdca,pchain,Nbytes);
	      if (Nbytes != pdca->length) {
		/* error */
		Nbytes = -pdca->length;
		FREE(chptr);
		errflg = -1;
		goto error_processing;
	      }
	      if (chptr && Nrows > 0) {
		extern int ec_is_little_endian();
		int i_am_little = ec_is_little_endian();
		if (unpack) { /* unpack data into type-specific array */
		  /* the "logic" borrowed from ../tools/dcagen.c */
		  void *vptr = NULL;
		  uint pmethod = pdca->pmethod_actual;
		  uint dtnum = pdca->dtnum;
		  uint word = pmethod + 256U * dtnum;
		  int can_swp_data = EXTRACT_SWAPPABLE(word);
		  int column_is_big_endian = (pdca->is_little == 0) ? 1 : 0;
		  int column_is_little_endian = (pdca->is_little == 1) ? 1 : 0;
		  int byteswap_needed = ((i_am_little && column_is_big_endian) || 
					 (!i_am_little && column_is_little_endian));
		  int swp = byteswap_needed;
		  if (swp) swp = can_swp_data;
		  if (pmethod == 0) { /* data is not packed */
		    if (swp || (idx && idxlen > 0)) {
		      if (do_trace) DCA_Trace(1,"Byte_Swap",pdca,pchain,idxlen);
		      vptr = Byte_Swap(chptr,Nrows,dtnum,swp,idx,idxlen,
				       pdca->dtname,pdca->colname,pchain->tblname);
		      if (idx && idxlen > 0) {
			Nrows = idxlen;
			FREE(chptr);
		      }
		      if (do_trace) DCA_Trace(0,"Byte_Swap",pdca,pchain,swp);
		    }
		    else {
		      vptr = chptr;
		    }
		  }
		  else { /* data is indeed packed */
		    int iret, packed_count;
		    double *cma = NULL;
		    int lencma = Nrows + 1;
		    const int fill_zeroth_cma = 0;
		    ALLOC(cma,lencma);
		    if (do_trace) DCA_Trace(1,"pcma2cma_",pdca,pchain,lencma);
		    {
		      DRHOOK_START(DCA_fetch:pcma2cma_);
		      coml_set_lockid_(&DCA_mylock);
		      pcma2cma_(&can_swp_data,
				(const unsigned int *)chptr,
				&Nbytes,
				idx, &idxlen, &fill_zeroth_cma,
				cma,
				&lencma,
				&packed_count,
				&iret);
		      coml_unset_lockid_(&DCA_mylock);
		      DRHOOK_END(Nbytes);
		    }
		    if (do_trace) DCA_Trace(0,"pcma2cma_",pdca,pchain,packed_count);
		    FREE(chptr);
		    if (iret < 0) {
		      /* error */
		      fprintf(stderr,
			      "***Error: Cannot unpack column='%s%s%s' : iret=%d, errflg=%d\n",
			      pdca->colname,
			      pchain->tblname ? "@" : "",
			      pchain->tblname ? pchain->tblname : "",
			      iret, errflg);
		      errflg = -2;
		      FREE(cma);
		      goto error_processing;
		    }
		    swp = 0; /* All byteswapping already done */
		    if (DCA_DEBUG(0x32)) {
		      int jj, jjmax = MIN(3,Nrows);
		      fprintf(stderr,
			      "Start of cma[] follows for %s:%s@%s at pool#%d "
			      "(dtnum=0x%x): nrows=%d, lencma=%d\n\tcma[jj]/swap=",
			      pdca->dtname,pdca->colname,pchain->tblname,pdca->poolno,
			      pdca->dtnum,Nrows,lencma);
		      for (jj=0; jj<=jjmax; jj++) {
			const int one = 1;
			double cmaswap = cma[jj];
			swap8bytes_(&cmaswap, &one);
			fprintf(stderr," [jj=%d:%.20g/%.20g]",jj,cma[jj],cmaswap);
		      }
		      fprintf(stderr,"\n");
		    } /* if (DCA_DEBUG(0x32)) */
		    if (do_trace) DCA_Trace(1,"Data_Conv",pdca,pchain,idxlen);
		    vptr = Data_Conv(fill_zeroth_cma,cma,Nrows,dtnum,NULL,idx,idxlen,
				     pdca->dtname,pdca->colname,pchain->tblname);
		    if (idx && idxlen > 0) Nrows = idxlen;
		    if (do_trace) DCA_Trace(0,"Data_Conv",pdca,pchain,can_swp_data);
		    if (DCA_DEBUG(0x32)) {
		      if (pdca->dtnum == DATATYPE_REAL8) {
			double *dptr = vptr;
			int jj, jjmax = MIN(3,Nrows);
			fprintf(stderr,
				"Start of vptr[]@%p (cma@%p) follows for %s:%s@%s at pool#%d "
				"(REAL8:dtnum=0x%x): nrows=%d, idxlen=%d\n\tvptr[jj]/swap=",
				vptr, cma,
				pdca->dtname,pdca->colname,pchain->tblname,pdca->poolno,
				pdca->dtnum,Nrows,idxlen);
			for (jj=0; jj<jjmax; jj++) {
			  const int one = 1;
			  double dswap = dptr[jj];
			  swap8bytes_(&dswap, &one);
			  fprintf(stderr," [jj=%d:%.20g/%.20g]",jj,dptr[jj],dswap);
			}
			fprintf(stderr,"\n");
		      }
		      else if (pdca->dtnum == DATATYPE_INT4) {
			int *dptr = vptr;
			int jj, jjmax = MIN(3,Nrows);
			fprintf(stderr,
				"Start of vptr[]@%p (cma@%p) follows for %s:%s@%s at pool#%d "
				"(INT4:dtnum=0x%x): nrows=%d, idxlen=%d\n\tvptr[jj]/swap=",
				vptr, cma,
				pdca->dtname,pdca->colname,pchain->tblname,pdca->poolno,
				pdca->dtnum,Nrows,idxlen);
			for (jj=0; jj<jjmax; jj++) {
			  const int one = 1;
			  int dswap = dptr[jj];
			  swap4bytes_(&dswap, &one);
			  fprintf(stderr," [jj=%d:%d/%d]",jj,dptr[jj],dswap);
			}
			fprintf(stderr,"\n");
		      }
		    } /* if (DCA_DEBUG(0x32)) */
		    if (cma != vptr) FREE(cma);
		  }
		  ptr = vptr;
		}
		else {
		  ptr = chptr;
		}
	      }
	      else {
		ptr = chptr;
	      } /* if (chptr && Nrows > 0) ... else ... */
	    }
	    break; /* for (j=0; j<pchain->dcalen; j++) */
	  } /* if (pdca->poolno == poolno && ... */
	  pdca++;
	} /* for (j=0; j<pchain->dcalen; j++) */
      error_processing:
	if (errflg) {
	  fprintf(stderr,"***Error in DCA_fetch: errflg = %d\n",errflg);
	  RAISE(SIGABRT);
	}
      } /* if (pchain->dca && pchain->dcalen > 0) */
    } /* if (pchain) */
    FREE(p_colname);
  } /* if (handle > 0 && dbname && tblname && colname) */
  if (nrows)  *nrows  = Nrows;
  if (nbytes) *nbytes = Nbytes;
  if (datatype) *datatype = Datatype;
  if (offset) *offset = ioffset;
  DRHOOK_END(Nbytes);
  return ptr;
}


PUBLIC double *
DCA_fetch_double(int handle,
		 const char *dbname,
		 const char *tblname,
		 const char *colname,
		 int poolno,
		 const int idx[],
		 int idxlen,
		 int *nrows)
{
  double *d = NULL;
  int Nrows = 0;
  DRHOOK_START(DCA_fetch_double);
  {
    const int unpack = 1;
    uint datatype = 0;
    int ioffset = 0;
    void *ptr = DCA_fetch(handle,
			  dbname,
			  tblname,
			  colname,
			  poolno,
			  unpack,
			  idx,
			  idxlen,
			  &Nrows,
			  NULL,
			  &datatype,
			  &ioffset);
    if (ptr && Nrows > 0) {
      const int flag = 1; /* i.e. FLAG_FETCH(flag) will be true */
      const int row_offset = 0; /* Not parameterized -- not at least for now */
      ALLOC(d, Nrows);
      /* Convert "any" datatype to real*8 i.e. double */
      Nrows = ODBCopyGetTable(flag, Nrows, d, ptr, datatype, row_offset);
      FREE(ptr);
      if (ioffset != 0) {
	/* Need to "shift" data to left (ioffset < 0) or right (ioffset > 0)
	   by ioffset positions. Underflow/overflow areas omitted
	   and areas not filled, are set to "mdi" */
	const double mdi = ABS(RMDI);
	int j;
	double *dcopy = NULL;
	ALLOC(dcopy, Nrows);
	if (ABS(ioffset) < Nrows) {
	  /* Underflow/overflow less than Nrows */
	  int len = Nrows - ABS(ioffset);
	  if (ioffset > 0) {
	    for (j=0; j<len; j++) dcopy[j] = d[j+ioffset];
	    for (j=len; j<Nrows; j++) dcopy[j] = mdi;
	  }
	  else { /* ioffset < 0 */
	    ioffset = -ioffset;
	    for (j=0; j<len; j++) dcopy[j+ioffset] = d[j];
	    for (j=0; j<ioffset; j++) dcopy[j] = mdi;
	  }
	}
	else {
	  for (j=0; j<Nrows; j++) dcopy[j] = mdi;
	}
	FREE(d);
	d = dcopy;
      } /* if (ioffset != 0) */
    }
    else {
      Nrows = 0;
    }
  }
  if (nrows) *nrows = Nrows;
  DRHOOK_END(Nrows);
  return d;
}


PUBLIC int *
DCA_fetch_int(int handle,
	      const char *dbname,
	      const char *tblname,
	      const char *colname,
	      int poolno,
	      const int idx[],
	      int idxlen,
	      int *nrows)
{
  int *i = NULL;
  int Nrows = 0;
  DRHOOK_START(DCA_fetch_int);
  {
    double *d = DCA_fetch_double(handle,
				 dbname,
				 tblname,
				 colname,
				 poolno,
				 idx,
				 idxlen,
				 &Nrows);
    if (Nrows > 0) {
      int j;
      ALLOC(i, Nrows);
      /* No flp. or integer overflow checking below ;-( */
      for (j=0; j<Nrows; j++) i[j] = (int)d[j];
    }
    if (nrows) *nrows = Nrows;
    FREE(d);
  }
  DRHOOK_END(Nrows);
  return i;
}


PUBLIC void
DCA_debugprt(FILE *fp,
	     int handle,
	     const char *dbname,
	     const char *tblname,
	     const char *colname,
	     int poolno)
{
  if (fp && handle > 0 && dbname && tblname) {
    int it = get_thread_id_();
    dca_chain_t *pchain = NULL;
    if (*tblname == '@') tblname++;
    coml_set_lockid_(&DCA_mylock);
    pchain = dca_chain;
    while (pchain) {
      if (pchain->handle == handle &&
	  pchain->it == it &&
	  strequ(pchain->dbname, dbname) &&
	  strequ(pchain->tblname, tblname)) {
	break;
      }
      pchain = pchain->next;
    }
    coml_unset_lockid_(&DCA_mylock);
    if (pchain) { /* found it !! */
      int j;
      dca_t *pdca = pchain->dca;
      fprintf(fp,
	      "DCA_debugprt(handle=%d, dbname=%s, tblname=%s, poolno=%d, colname=%s)\n",
	      handle, dbname, tblname, poolno, colname ? colname : "<any>");
      for (j=0; j<pchain->dcalen; j++) {
	if ((poolno == -1 || pdca->poolno == poolno) &&
	    (!colname || strequ(pdca->colname,colname))) {
	  fprintf(fp,
		  "col#%d, hash=%u, name='%s', type='%s', typenum=(0x%x;%u), file='%s', pool#%d\n",
		  pdca->colnum, pdca->hash, pdca->colname, pdca->dtname, pdca->dtnum, pdca->dtnum,
		  pdca->filename, pdca->poolno);
	  fprintf(fp,
		  "\t(offset,length)=(%llu,%d), pmethods=(%d,%d), nrows=%d, nMDIs=%d\n",
		  pdca->offset, pdca->length,
		  pdca->pmethod, pdca->pmethod_actual,
		  pdca->nrows, pdca->nmdis);
	  fprintf(fp,
		  "\t(avg,min,max,card)=(%.14g,%.14g,%.14g,%d), is little-endian ? %s\n",
		  pdca->avg, pdca->min, pdca->max, pdca->ncard, 
		  pdca->is_little ? "yes" : "no");
	}
	pdca++;
      } /* for (j=0; j<pchain->dcalen; j++) */
    } /* if (pchain) */
  }
}

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

PUBLIC void
codb_detect_dcadir_(const char *dbname, 
		    int *io_method_env
		    /* Hidden arguments */
		    , int dbname_len)
{
  int iom = *io_method_env;
  if (iom > 0) {
    /* by setting ODB_IO_METHOD (or ODB_IO_METHOD_<dbname>) <= 0
       you avoid using dcadir-check altogether */
    const char base[] = "ODB_SRCPATH";
    int i, baselen = strlen(base);
    int found = 0;
    DECL_FTN_CHAR(dbname);
    ALLOC_FTN_CHAR(dbname);
    
    for (i=1; i<=3 && !found; i++) {
      int len;
      char *env, *path;

      if (i == 1) {
	/* (1) Try with "$ODB_SRCPATH_<dbname>/dca" */
	len = baselen + 1 + strlen(p_dbname) + 1;
	ALLOC(env,len);
	snprintf(env,len,"%s_%s",base,p_dbname);
	path = getenv(env);
	FREE(env); 
      }
      else if (i == 2) {
	/* (2) Try with "$ODB_SRCPATH/dca" */
	path = getenv(base);
      }
      else if (i == 3) {
	/* (3) Try with "./dca" */
	path = ".";
      }

      if (path) {
	struct stat buf;
	char *dcadir;
	len = strlen(path) + 4 + 1;
	ALLOC(dcadir, len);
	snprintf(dcadir,len,"%s/dca",path);
	/*
	fprintf(stderr,
		"codb_detect_dcadir_(%s,%d): Checking dcadir='%s'\n",
		p_dbname, *io_method_env, dcadir);
	*/
	if (stat(dcadir,&buf) == 0 && S_ISDIR(buf.st_mode)) {
	  /* Matched !!
	     We have a reason to believe that the
	     I/O-method = 5 can be used ;
	     In doubt --> set I/O-method for this database to 0 !! */
	  iom = 5;
	  found = 1;
	}
	FREE(dcadir);
      }

    } /* for (i=1; i<=3 && !found; i++) */

    if (iom == 5 && !found) iom = -5; /* Using I/O-method#5 in this case would be wrong */
    *io_method_env = iom;

    /*
    fprintf(stderr,
	    "codb_detect_dcadir_(%s,%d) finished.\n",
	    p_dbname, *io_method_env);
    */

    FREE_FTN_CHAR(dbname);
  }
}
