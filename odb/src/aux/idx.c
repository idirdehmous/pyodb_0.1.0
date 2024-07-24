
/* idx.c */

/* Under development as of 1-Jan-2006 */
/* Lots of new meat under the skin as of 8-Jun-2007 ;-) */

#include "odb.h"
#include "odbmd5.h"
#include "idx.h"
#include "info.h"
#include "cmaio.h"
#include "result.h"
#include "magicwords.h"
#include "swapbytes.h"
#include "pcma_extern.h"
#include "bits.h"
#include "evaluate.h"
#include "cdrhook.h"

/* 
   CREATE [UNIQUE|DISTINCT|BITMAP] INDEX idxname 
   ON tblname 
   [( colname(s) )] 
   [WHERE condition] 
   ; 
*/


PRIVATE char *MergeStr(const char *a, const char *delim, const char *b)
{
  char *s = NULL;
  DRHOOK_START(MergeStr);
  {
    int len = STRLEN(a) + STRLEN(delim) + STRLEN(b) + 1;
    ALLOC(s, len);
    snprintf(s, len, "%s%s%s",
	     a ? a : "",
	     delim ? delim : "",
	     b ? b : "");
  }
  DRHOOK_END(0);
  return s;
}


PUBLIC int codb_IDXF_drop(void *Info)
{
  int rc = 0;
  info_t *info = Info;
  DRHOOK_START(codb_IDXF_drop);
  if (info && info->ph) {
    const char *idxpath = info->ph->idxpath ? info->ph->idxpath : "idx";
    const char *idxname = info->use_index_name ? info->use_index_name : "*";
    int nfrom = info->nfrom;
    table_t *t = info->t;
    do {
      int iret = 0;
      const char *tblname = t ? t->name : "*";
      char *cmd = NULL;
      int len = STRLEN(idxpath) + STRLEN(tblname) + STRLEN(idxname) + 30;
      ALLOC(cmd, len);
      snprintf(cmd, len, "rm -f %s/%s/%s.* < /dev/null", idxpath, tblname, idxname);
      iret = system(cmd);
      FREE(cmd);
      if (t) t = t->next;
      if (iret == 0) ++rc;
    } while (t);
  }
  DRHOOK_END(0);
  return rc;
}


PRIVATE char *
RemoveWhiteSpace(const char *in)
{
  char *s = NULL;
  int slen = STRLEN(in);
  if (slen > 0) {
    const char *p = in;
    char *ps;
    int quote_char = 0;
    Bool in_quote = false;
    ALLOC(s, slen+1);
    ps = s;
    while (*p) {
      if (in_quote || !isspace(*p)) {
	*ps++ = *p;
      }
      if (!in_quote && (*p == '"' || *p == '\'')) {
	in_quote = true;
	quote_char = *p;
      }
      else if (in_quote && *p == quote_char) {
	in_quote = false;
	quote_char = 0;
      }
      ++p;
    }
    *ps = '\0';
  }
  else
    s = STRDUP(in);
  return s;
}


PUBLIC char *
codb_IDXF_filename(Bool create_name,
		   const char *idxpath,
		   const char *tblname, 
		   const char *idxname,
		   int ncols,
		   const char *colnames,
		   const char *wherecond,
		   const char *filesuffix)
{
  char *name = NULL;
  DRHOOK_START(codb_IDXF_filename);
  {
    const char *any = create_name ? "ANY" : "*";

    const char *m1 = colnames;
    char *md5sum_m1 = NULL;

    const char *m2 = wherecond;
    char *md5sum_m2 = NULL;

    char *merge = NULL;

    unsigned char sign[16];

    /* Handle column names */

    if (ncols <= 0 || strequ(colnames,"*")) m1 = NULL;
    if (m1) {
      char *mpp = RemoveWhiteSpace(m1);
      if (MD5_str2sign(mpp, sign) == 0) {
	md5sum_m1 = MD5_sign2hex(sign, 0); /* STRDUP'ped, no lowercase hex */
      }
      else {
	fprintf(stderr,
		"***Error in codb_IDXF_filename(): Unable to obtain md5sum on colnames '%s'\n",m1);
	RAISE(SIGABRT);
      }
      FREE(mpp);
    }
    else {
      md5sum_m1 = STRDUP(any); /* Any set of columns */
    }

    /* Handle WHERE-condition */

    if (!wherecond && create_name) m2 = "1";
    if (m2) {
      char *mpp = RemoveWhiteSpace(m2);
      if (MD5_str2sign(mpp, sign) == 0) {
	md5sum_m2 = MD5_sign2hex(sign, 0); /* STRDUP'ped, no lowercase hex */
      }
      else {
	fprintf(stderr,
		"***Error in codb_IDXF_filename(): Unable to obtain md5sum on wherecond '%s'\n",m2);
	RAISE(SIGABRT);
      }
      FREE(mpp);
    }
    else {
      md5sum_m2 = STRDUP(any); /* Any WHERE-statement */
    }

    merge = MergeStr(md5sum_m1, ".", md5sum_m2);

    FREE(md5sum_m1);
    FREE(md5sum_m2);

    if (tblname && *tblname == '@') ++tblname;

    if (!idxpath && !tblname && !idxname && merge) {
      name = STRDUP(merge);
    }
    else {
      char numstr[40];
      int len = 1;
      len += STRLEN(idxpath) + 1;
      len += STRLEN(tblname) + 1;
      len += STRLEN(idxname ? idxname : any) + 1;
      if (ncols >= 0) {
	snprintf(numstr,sizeof(numstr),"%d",ncols);
      }
      else {
	strcpy(numstr,any);
      }
      len += STRLEN(numstr) + 1;
      len += STRLEN(merge) + 1;
      len += STRLEN(filesuffix);
      ALLOC(name, len);
      /* 
	 Create index-file name ==> It becomes usually :
	 $ODB_IDXPATH_<dbname>/<tablename>/<indexname>.<ncols>.<md5sum_of_colnames>.<md5sum_of_wherecond>.gz
	 The $ODB_IDXPATH_<dbname> is by default $ODB_SRCPATH_<dbname>/idx 

	 The "merge"-string below is <md5sum_of_colnames>.<md5sum_of_wherecond>

	 If colnames is empty, then <md5sum_of_colnames> is set to variable "any"
	 If wherecond is empty or 1, then <md5sum_of_wherecond> is set to variable "any"
      */
      printf( "%s", merge ) ;
      snprintf(name, len, "%s%s%s%s%s.%s.%s%s",
	       idxpath ? idxpath : "", idxpath ? "/" : "",
	       tblname ? tblname : "", tblname ? "/" : "",
	       idxname ? idxname : any,
	       numstr,
	       merge,
	       filesuffix ? filesuffix : "");
    }

    FREE(merge);
  }
  DRHOOK_END(0);
  return name;
}


PUBLIC char *
codb_IDXF_open(int *Io_idx, const char *filename, const char *mode)
{
  int rc = 0;
  int io_idx = -1;
  char *filename_out = NULL;
  DRHOOK_START(codb_IDXF_open);
  if (Io_idx) {
    if (mode && *mode == 'r' && strchr(filename,'*')) {
      /* Wildcard found --> search for the most up to date filename */
      FILE *fp;
      char *cmd;
      int len = STRLEN(filename) + 80;
      ALLOC(cmd, len);
      snprintf(cmd, len, "ls -C1t %s 2>/dev/null | head -1", filename);
      fp = popen(cmd, "r");
      if (fp) {
	char *fullname;
	len = STRLEN(filename) + 4096;
	ALLOC(fullname, len);
	if (fscanf(fp,"%s",fullname) == 1) {
	  cma_open_(&io_idx, fullname, mode, &rc, STRLEN(fullname), STRLEN(mode));
	  if (io_idx >= 0) filename_out = STRDUP(fullname);
	}
	FREE(fullname);
	pclose(fp);
      }
      FREE(cmd);
    }
    else {
      cma_open_(&io_idx, filename, mode, &rc, STRLEN(filename), STRLEN(mode));
      if (io_idx >= 0) filename_out = STRDUP(filename);
    }
    *Io_idx = io_idx;
  }
  DRHOOK_END(0);
  return filename_out; /* Watch for memory leakage */
}


PRIVATE int
FreeIdxSets(odbidx_set_t *idxset, int nsets, int ncols)
{
  int rc = 0;
  DRHOOK_START(FreeIdxSets);
  if (idxset && nsets > 0) {
    int j;
    for (j=0; j<nsets; j++) {
      FREE(idxset->value);
      FREE(idxset->idxdata);
      ++idxset;
      ++rc;
    }
  }
  DRHOOK_END(0);
  return rc;
}


PUBLIC void *
codb_IDXF_freeidx(odbidx_t *idx, int recur)
{
  DRHOOK_START_RECUR(codb_IDXF_freeidx, recur);
  if (idx) {
    int ncols = idx->ncols;
    int j, npools = idx->npools;
    odbidx_pp_t *pp = idx->pp;
    if (pp) {
      for (j=0; j<npools; j++) {
	(void) FreeIdxSets(pp->idxset, pp->nsets, ncols);
	FREE(pp->idxset);
	++pp;
      }
      FREE(idx->pp);
    }
    FREE(idx->filename);
    FREE(idx->tblname);
    FREE(idx->idxname);
    FREE(idx->colnames);
    FREE(idx->wherecond);
    FREE(idx->dcard);
    if (idx->next) idx->next = codb_IDXF_freeidx(idx->next, recur + 1);
    FREE(idx);
  }
  DRHOOK_END_RECUR(0, recur);
  return idx;
}


PUBLIC int
codb_IDXF_close(int io_idx, odbidx_t *idx)
{
  int rc = 0;
  DRHOOK_START(codb_IDXF_close);
  cma_close_(&io_idx, &rc);
  if (rc == 0 && idx) idx = codb_IDXF_freeidx(idx, 0);
  DRHOOK_END(0);
  return rc;
}


PRIVATE odbidx_t *
IdxAllocAndFetch(odbidx_t *Idx,
		 const char *filename,
		 int handle,
		 int *poolno_offset,
		 int *retcode,
		 int recur,
		 int oper,
		 const double *rhs)
{
  int rc = 0;
  odbidx_t *idx = NULL;
  DRHOOK_START_RECUR(IdxAllocAndFetch, recur);
  if (filename && handle >= 0) {
    int io_idx = -1;
    char *true_filename = codb_IDXF_open(&io_idx, filename, "r");
    if (true_filename && io_idx >= 0) {
      if (Idx) idx = Idx; else ALLOC(idx, 1);
      memset(idx, 0, sizeof(*idx));
      idx->filename = STRDUP(true_filename);
      rc = codb_IDXF_read(io_idx, idx, handle, poolno_offset, recur, oper, rhs);
      (void) codb_IDXF_close(io_idx, NULL);
    }
    FREE(true_filename);
  }
  if (retcode) *retcode = rc;
  DRHOOK_END_RECUR(0, recur);
  return idx;
}


#define CMARD(x, n) \
  opaque = x; nbytes = sizeof(*(x)) * (n); \
  cma_readb_(&io_idx, opaque, &nbytes, &iret); \
  if (byteswap) { \
    int sz = sizeof(*(x)), nelem = n; \
    void (*byteswap_func)(void *v, const int *vlen) = \
      (sz == 4) ? swap4bytes_ : swap8bytes_; \
    byteswap_func(opaque, &nelem); \
  }

#define CMARD_TFLAQUE(x, n) \
  opaque = x; nbytes = sizeof(*(x)) * (n); \
  cma_readb_(&io_idx, opaque, &nbytes, &iret); \
  if (byteswap) { \
    const int one_elem = 1; \
    int je, sz = sizeof(*(x)), nelem = n; \
    void (*byteswap_func)(void *v, const int *vlen) = \
      (sz == 4) ? swap4bytes_ : swap8bytes_; \
    for (je=0; je<nelem; je++) { \
      if (tflaque[je] != 1) byteswap_func((x)+je, &one_elem); \
    } \
  }

#define EAT_FF() \
{ \
  int c; \
  while ((c = fgetc(fp)) != EOF) { \
   if (c != '\f') { ungetc(c, fp); break; } \
  } \
}

#define EAT_NL() \
{ \
  int c; \
  while ((c = fgetc(fp)) != EOF) { \
   if (c != '\n') { ungetc(c, fp); break; } \
  } \
}

#define EAT_MONKEY(array, str, skip_1st) \
 FREE(array); EAT_NL(); \
 if (fgets(line, sizeof(line), fp)) { \
   char *sl = line; \
   while (isspace(*sl)) ++sl; \
   { \
     char *s = strchr(sl,'\n'); \
     if (s) *s = '\0'; \
     if (str) { s = strstr(sl, str); if (s) *s = '\0'; } \
     if (skip_1st) { \
       s = strchr(sl,' '); \
      (array) = s ? STRDUP(s+1) : NULL; \
     } else { (array) = STRDUP(sl); } \
   } \
 }

#define EAT_GORILLA(array) EAT_MONKEY(array, NULL, 0)

#define TAKESET(OP) takeset = (Lhs OP Rhs) ? true : false

#define TAKESET_EVAL() { \
  double Lhs = *pvalue; \
  double Rhs = *rhs; \
  switch (oper) { \
  case EQ: TAKESET(==); break; \
  case NE: TAKESET(!=); break; \
  case GT: TAKESET(> ); break; \
  case GE: TAKESET(>=); break; \
  case LT: TAKESET(< ); break; \
  case LE: TAKESET(<=); break; \
  default: takeset = false; break; \
 } /* switch (oper) */ \
}

#define MAKE_TFLAQUE() \
if (ncols > 0 && ncols <= MAXBITS) { \
  if (typeflag == 0) { \
    memset(tflaque, 0, ncols * sizeof(*tflaque)); \
  } \
  else { \
    int ii; \
    for (ii=0; ii<ncols; ii++) { \
      int is_string = (typeflag == 0) ? 0 : \
	ODBIT_test(&typeflag, MAXBITS, MAXBITS, ii, ii); \
      tflaque[ii] = is_string; \
    } /* for (ii=0; ii<ncols; ii++) */ \
  } \
}


PUBLIC int
codb_IDXF_read(int io_idx, odbidx_t *idx,
	       int handle, int *poolno_offset, int recur,
	       int oper,
	       const double *rhs)
{
  int rc = 0;
  DRHOOK_START_RECUR(codb_IDXF_read, recur);
  if (idx && io_idx >= 0) {
    FILE *fp = CMA_get_fp(&io_idx);
    if (fp) {
      Bool last_pool_hit = false;
      unsigned int idxf;
      int binary;
      Bool byteswap = false;
      int info[8];
      void *opaque;
      int nbytes, iret;
      int this_version;
      int filekind;
      char line[65536];
      char *tmpline = NULL;
      int j, poolno, npools;
      unsigned int typeflag;
      int ncols, npp, ncard;
      int tflaque[MAXBITS];
      
      CMARD(&idxf, 1);

      if (idxf == IDXB || idxf == BXDI) {
	binary = 1;
	byteswap = (idxf == BXDI) ? true : false;
      }
      else if (idxf == IDXT || idxf == TXDI) {
	binary = 0;
	byteswap = (idxf == TXDI) ? true : false;
      }
      else {
	binary = -1;
	fprintf(stderr,
		"***Error: Invalid index file (%s) magic number %u\n",
		idx->filename, idxf);
	RAISE(SIGABRT);
      }

      EAT_GORILLA(tmpline);
      sscanf(tmpline, "%d", &this_version);
      EAT_GORILLA(tmpline);
      sscanf(tmpline, "%d", &filekind);
      FREE(tmpline);

      if (filekind == 2) {
	/* Create a chain odbidx_t's */
	odbidx_t *start_idx = idx;
	odbidx_t *cur_idx = start_idx;

	while (!feof(fp)) {
	  int iret = 0;
	  odbidx_t *new_idx = NULL;
	  char *filename = NULL;
	  
	  EAT_MONKEY(filename, NULL, 1);
	  if (!filename) break; /* No more input --> most likely an EOF reached */

	  new_idx = IdxAllocAndFetch(cur_idx, filename, 
				     handle, poolno_offset, &iret, recur + 1, 
				     oper, rhs);

	  if (new_idx && new_idx->npools > 0 && iret > 0) {
	    cur_idx->next = new_idx;
	    cur_idx = new_idx;
	    cur_idx->next = NULL;
	    rc += iret;
	  }

	  FREE(filename);
	} /* while (!feof(fp)) */
	goto finish;
      }

      EAT_MONKEY(tmpline, NULL, 1); /* Original index-filename as it was written */
      FREE(tmpline);

      EAT_MONKEY(idx->tblname, NULL, 1);
      EAT_MONKEY(idx->idxname, NULL, 1);
      EAT_MONKEY(idx->colnames, NULL, 1);
      EAT_MONKEY(idx->wherecond, NULL, 1);

      EAT_NL();
      EAT_FF();

      if (binary) {
	CMARD(info, 8);
	idx->creation_date     = (yyyymmdd)info[0];
	idx->creation_time     = (hhmmss)info[1];
	idx->modification_date = (yyyymmdd)info[2];
	idx->modification_time = (hhmmss)info[3];
	ncols                  = info[4];
	npp                    = info[5];
	typeflag               = (unsigned int)info[6];
	ncard                  = info[7];
	MAKE_TFLAQUE();
	if (ncols > 0) {
	  double *dcard;
	  int ntot = ncols * ncard;
	  ALLOC(dcard, ntot);
	  if (typeflag == 0 || !byteswap) {
	    /* No strings or no byteswap'ping needed */
	    CMARD(dcard, ntot);
	  }
	  else {
	    /* At least one column is a string */
	    double *pcard = dcard;
	    for (j=0; j<ncard; j++) {
	      CMARD_TFLAQUE(pcard, ncols);
	      pcard += ncols;
	    }
	  }
	  idx->ncard = ncard;
	  idx->dcard = dcard;
	}
	else {
	  idx->ncard = 0;
	  idx->dcard = NULL;
	}
      }
      else {
	EAT_GORILLA(tmpline);
	sscanf(tmpline, "%d %d", &idx->creation_date, &idx->creation_time);
	EAT_GORILLA(tmpline);
	sscanf(tmpline, "%d %d", &idx->modification_date, &idx->modification_time);
	EAT_GORILLA(tmpline);
	sscanf(tmpline, "%d %d", &ncols, &npp);
	EAT_GORILLA(tmpline);
	sscanf(tmpline, "%u", &typeflag);
	MAKE_TFLAQUE();
	if (ncols > 0) {
	  int k = 0;
	  int ntot;
	  double *dcard;
	  EAT_GORILLA(tmpline);
	  sscanf(tmpline, "%d", &ncard);
	  ntot = ncols * ncard;
	  ALLOC(dcard, ntot);
	  for (j=0; j<ncard; j++) {
	    int ii;
	    char *ptr = NULL;
	    EAT_GORILLA(tmpline); 
	    ptr = tmpline;
	    while (isspace(*ptr)) ++ptr;
	    for (ii=0; ii<ncols; ii++) {
	      int is_string = tflaque[ii];
	      if (is_string == 1) {
		S2D_Union u;
		ptr = strstr(ptr, "'"); /* Opening quote */
		strncpy(u.str,ptr+1,sizeof(double));
		dcard[k] = u.dval;
		ptr += sizeof(double) + 1; /* Now at the closing quote */
	      }
	      else {
		sscanf(ptr, "%lf", &dcard[k]);
		ptr = strchr(ptr,' ');
	      }
	      ++k;
	      ++ptr;
	    } /* for (ii=0; ii<ncols; ii++) */
	    FREE(tmpline);
	  } /* for (j=0; j<ncard; j++) */
	  idx->ncard = ncard;
	  idx->dcard = dcard;
	}
	else {
	  idx->ncard = 0;
	  idx->dcard = NULL;
	}
	FREE(tmpline);
      }

      idx->ncols = ncols;
      idx->npools = npools = npp;
      idx->typeflag = typeflag;
      CALLOC(idx->pp, npools);

      if (npp > 0) {
	npp = 0;
	for (j=0; j<npools; j++) {
	  odbidx_pp_t *pp = &idx->pp[npp];
	  int nsets, idxtype, nrows;
	  Bool poolno_okay = false;

	  pp->nsets = 0;
	  
	  if (binary) {
	    CMARD(info, 1);
	    poolno  = info[0];

	    if (poolno <= 0) {
	      last_pool_hit = true;
	      break; /* for (j=0; j<npools; j++) */
	    }

	    {
	      ll_t ward[2];
	      CMARD(ward, 2);
	      pp->backwd = ward[0];
	      pp->fwd = ward[1];
	    }

	    /* The following is essential for timeseries 
	       (chained/odbmerge/odbdup'ped) databases */
	    if (poolno_offset && *poolno_offset > 0) poolno += *poolno_offset;
	    poolno_okay = ODB_in_permanent_poolmask(handle, poolno) ? true : false;

	    if (!poolno_okay) {
	      int iret;
	      const int binwhence = 1; /* SEEK_CUR (= seek from the current pos) */
	      int binoff = pp->fwd;
	      cma_seekb_(&io_idx, &binoff, &binwhence, &iret);
	      goto end_pool_loop;
	    }

	    CMARD(info, 3);
	    idxtype = info[0];
	    nrows   = info[1];
	    nsets   = info[2];

	  }
	  else {
	    EAT_GORILLA(tmpline);
	    sscanf(tmpline, "%d", &poolno);
	    FREE(tmpline);

	    if (poolno <= 0) {
	      last_pool_hit = true;
	      break; /* for (j=0; j<npools; j++) */
	    }

	    /* The following is essential for timeseries 
	       (chained/odbmerge/odbdup'ped) databases */
	    if (poolno_offset && *poolno_offset > 0) poolno += *poolno_offset;
	    poolno_okay = ODB_in_permanent_poolmask(handle, poolno) ? true : false;
	    
	    EAT_GORILLA(tmpline);
	    sscanf(tmpline, "%d %d %d", &idxtype, &nrows, &nsets);

	    FREE(tmpline);
	  }

	  { /* Start examine and store contents of this index */
	    int i;
	    int ksets = 0;

	    /* Note: 
	       When "poolno_okay" is false, all data is still read in
	       but not processed, nor stored anywhere 
	    */

	    pp->poolno = poolno;
	    pp->idxtype = idxtype;
	    pp->nrows = nrows;
	    pp->nsets = 0;

	    CALLOC(pp->idxset, nsets);
	    
	    for (i=0; i<nsets; i++) {
	      double *pvalue = NULL;
	      Bool takeset = poolno_okay;
	      odbidx_set_t *idxset = &pp->idxset[ksets];
	      int ndata, idxlen, nclusters;
	      unsigned int *idxdata = NULL;
	      
	      if (ncols > 0) {
		ALLOC(idxset->value, ncols);
		pvalue = idxset->value;
	      }
	      else {
		idxset->value = NULL;
	      }

	      if (binary) {
		if (ncols > 0) {
		  if (typeflag == 0 || !byteswap) {
		    /* No strings or no byteswap'ping needed */
		    CMARD(pvalue, ncols);
		  }
		  else {
		    /* At least one column is a string */
		    CMARD_TFLAQUE(pvalue, ncols);
		  }
		  if (poolno_okay && rhs && ncols == 1) TAKESET_EVAL();
		}
		CMARD(info, 2);
		ndata = info[0];
		idxlen = info[1];
		idxset->ndata = ndata;
		idxset->idxlen = idxlen;
		idxset->nclusters = 0;
		ndata = ABS(ndata);
		ALLOC(idxdata, ndata);
		CMARD(idxdata, ndata);
		if (!takeset) FREE(idxdata);
	      }
	      else /* text */ {
		int ii;
		int set_number;

		EAT_GORILLA(tmpline);
		sscanf(tmpline,"%d",&set_number);
		FREE(tmpline);

		if (ncols > 0) {
		  char *ptr = NULL;
		  EAT_GORILLA(tmpline); 
		  ptr = tmpline;
		  while (isspace(*ptr)) ++ptr;
		  for (ii=0; ii<ncols; ii++) {
		    int is_string = tflaque[ii];
		    if (is_string == 1) {
		      S2D_Union u;
		      ptr = strstr(ptr, "'"); /* Opening quote */
		      strncpy(u.str,ptr+1,sizeof(double));
		      if (poolno_okay) pvalue[ii] = u.dval;
		      ptr += sizeof(double) + 1; /* Now at the closing quote */
		    }
		    else {
		      if (poolno_okay) sscanf(ptr, "%lf", &pvalue[ii]);
		      ptr = strchr(ptr,' ');
		    }
		    ++ptr;
		  } /* for (ii=0; ii<ncols; ii++) */
		  FREE(tmpline);
		  if (poolno_okay && rhs && ncols == 1) TAKESET_EVAL();
		}

		EAT_GORILLA(tmpline);
		sscanf(tmpline, "%d %d %d", &ndata, &idxlen, &nclusters);
		idxset->ndata = ndata;
		idxset->idxlen = idxlen;
		idxset->nclusters = nclusters;
		FREE(tmpline);

		if (ndata < 0 || idxtype == 2) {
		  if (ndata < 0) {
		    /* PCMA-packed */
		    ndata = ABS(ndata);
		    if (takeset) {
		      ALLOC(idxdata, ndata);
		      for (ii=0; ii<ndata; ii++) fscanf(fp, "%u", &idxdata[ii]);
		    }
		    else {
		      unsigned int idxdummy;
		      for (ii=0; ii<ndata; ii++) fscanf(fp, "%u", &idxdummy);
		    }
		  }
		  else {
		    int k;
		    char pcl[80];
		    if (takeset) ALLOC(idxdata, ndata);
		    ii = 0;
		    for (k=0; k<nclusters; k++) {
		      fscanf(fp,"%s",pcl);
		      if (takeset) {
			int cnt;
			unsigned int v;
			char *star = strchr(pcl,'*');
			if (star) {
			  int z;
			  *star = ' ';
			  sscanf(pcl,"%d %u",&cnt,&v);
			  for (z=0; z<cnt; z++) {
			    idxdata[ii++] = v;
			  }
			}
			else {
			  sscanf(pcl,"%u",&v);
			  idxdata[ii++] = v;
			}
		      } /* if (takeset) */
		    } /* for (k=0; k<nclusters; k++) */
		  }
		}
		else if (ndata == 0) {
		  if (takeset) ALLOC(idxdata, 0);
		}
		else { /* ndata > 0 */
		  int k;
		  char pcl[80];
		  if (takeset) ALLOC(idxdata, ndata);
		  ii = 0;
		  for (k=0; k<nclusters; k++) {
		    fscanf(fp,"%s",pcl);
		    if (takeset) {
		      unsigned int z,s,e,inc;
		      char *col1;
		      col1 = strchr(pcl,':');
		      if (col1) {
			char *col2 = strchr(col1+1,':');
			*col1 = ' ';
			if (col2) {
			  *col2 = ' ';
			  sscanf(pcl,"%u %u %u",&s,&e,&inc);
			}
			else {
			  sscanf(pcl,"%u %u",&s,&e);
			  inc = 1;
			}
			for (z=s; z<=e; z+=inc) {
			  idxdata[ii++] = z;
			}
		      }
		      else {
			sscanf(pcl,"%u",&s);
			idxdata[ii++] = s;
		      }
		    } /* if (takeset) */
		  } /* for (k=0; k<nclusters; k++) */
		} /* if (ndata < 0) ... else if (ndata == 0) ... else */
		
	      } /* if (binary) ... else ... */
	      
	      idxset->idxdata = idxdata;

	      if (takeset) 
		ksets++; 
	      else {
		FREE(idxset->value);
		FREE(idxset->idxdata);
	      }
	    } /* for (i=0; i<nsets; i++) */
	    
	    pp->nsets = ksets;

	  end_pool_loop:
	    if (poolno_okay) {
	      if (pp->nsets > 0) npp++;
	    }
	    else { /* This pool wasn't in the poolmask */
	      FREE(pp->idxset);
	    }

	  } /* End examine and store contents of this index */
	} /* for (j=0; j<npools; j++) */

	idx->npools = npp; /* The true number of pools, taking poolmask into account */

	if (poolno_offset) *poolno_offset += npp;
      } /* if (npp > 0) */

      if (!last_pool_hit) {
	if (binary) {
	  CMARD(&poolno, 1);
	}
	else {
	  EAT_GORILLA(tmpline);
	  sscanf(tmpline, "%d", &poolno);
	  FREE(tmpline);
	}
      }

      ++rc;
    }
  }

 finish:
  DRHOOK_END_RECUR(0, recur);
  return rc;
}


PUBLIC odbidx_t *
codb_IDXF_fetch(int handle,
		const char *idxpath,
		const char *tblname,
		const char *idxname,
		int ncols,
		const char *colnames,
		const char *wherecond,
		int oper,
		const double *rhs)
{
  odbidx_t *idx = NULL;
  DRHOOK_START(codb_IDXF_fetch);
  {
    const char *filesuffix = "*";
    char *filename = codb_IDXF_filename(false, 
					idxpath, 
					tblname, 
					idxname, 
					ncols,
					colnames, 
					wherecond, 
					filesuffix);
    int poolno_offset = 0;
    int rc = 0;
    idx = IdxAllocAndFetch(NULL, filename, 
			   handle, &poolno_offset, &rc, 0, 
			   oper, rhs);
    if (idx && idx->npools == 0 && rc == 0) idx = codb_IDXF_freeidx(idx, 0);
    FREE(filename);
  }
  DRHOOK_END(0);
  return idx;
}


PUBLIC odbidx_pp_t *
codb_IDXF_pack(odbidx_t *idx, int poolno, int pmethod, int idxtype, int recur)
{
  /* Not implemented yet */
  return NULL;
}


PUBLIC odbidx_pp_t *
codb_IDXF_locate_pp(odbidx_t *idx, int poolno, int recur)
{
  odbidx_pp_t *PP = NULL;
  DRHOOK_START_RECUR(codb_IDXF_locate_pp, recur);
  if (idx && poolno > 0) {
    int j, npp = idx->npools;
    for (j=0; j<npp; j++) {
      odbidx_pp_t *pp = &idx->pp[j];
      if (pp->poolno == poolno) {
	PP = pp;
	break;
      }
    } /* for (j=0; j<npp; j++) */
    /* Not found ? Check the chain */
    if (!PP && idx->next) PP = codb_IDXF_locate_pp(idx->next, poolno, recur + 1);
  }
  DRHOOK_END_RECUR(0, recur);
  return PP;
}

PUBLIC odbidx_pp_t *
codb_IDXF_unpack(odbidx_t *idx, int poolno, int recur)
{
  /* 
     Make sure that the idxdata[] in corresponding idxsets is 
     (a) unpacked in upcma() sense, where applicable
     (b) idxtype is set 1 i.e. idxdata[] is direct rowid's ready for use
  */

  odbidx_pp_t *PP = NULL;
  DRHOOK_START_RECUR(codb_IDXF_unpack, recur);
  if (idx) {
    int j;
    if (poolno == -1) {
      /* All available pools */
      for (j=0; j<idx->npools; j++) {
	odbidx_pp_t *pp = &idx->pp[j];
	poolno = pp->poolno;
	if (poolno > 0) (void) codb_IDXF_unpack(idx, poolno, recur + 1);
      }
    }
    else if (poolno > 0) {
      /* A particular pool only */
      odbidx_pp_t *pp = codb_IDXF_locate_pp(idx, poolno, 0);
      if (pp) {
	int idxtype = pp->idxtype;
	int nrows = pp->nrows; /* Gives the valid idx-range [0..nrows-1], C-indexing */
	int js, nsets = pp->nsets;
	odbidx_set_t *pidxset = pp->idxset;
	
	for (js=0; js<nsets; js++) {
	  int ndata = pidxset->ndata;
	  
	  if (ndata < 0) {
	    /* Firstly, dismantle possible pcma()-packing */
	    /* Not implemented yet */
	    ndata = ABS(ndata); /* for now */
	  }
	  
	  if (idxtype == 2) {
	    /* Secondly, go from idxtype = 2 to idxtype = 1 */
	    unsigned int *bitmap = pidxset->idxdata;
	    pidxset->idxdata = ODBIT_getidx(bitmap, nrows, MAXBITS, &ndata);
	    FREE(bitmap); /* The same as ODBIT_free(bitmap); */
	  }
	  
	  pidxset->ndata = ndata;
	  ++pidxset;
	} /* for (js=0; js<nsets; js++) */

	pp->idxtype = 1;
	PP = pp;
      } /* if (pp) */
    }
  }
  DRHOOK_END_RECUR(0, recur);
  return PP;
}


PRIVATE int
Sequence(const unsigned int idxdata[], int istart, int iend, int *tryInc)
{
  int j, last = istart;
  int inc = (last < iend) ? idxdata[istart+1] - idxdata[last] : 1;
  int cnt = 0;
  DRHOOK_START(Sequence);
  for (j=istart+1; j<iend; j++) {
    if (idxdata[j] == idxdata[j-1] + inc) {
      last = j;
      ++cnt;
    }
    else
      break;
  }
  if (tryInc) *tryInc = (cnt > 0) ? inc : 0;
  DRHOOK_END(0);
  return last;
}

#define CMAWR(x, n) \
  opaque = x; nbytes = sizeof(*(x)) * (n); \
  cma_writeb_(&io_idx, opaque, &nbytes, &iret)

PUBLIC int
codb_IDXF_write(int io_idx, odbidx_t *idx, int binary, int pmethod, int use_idxtype)
{
  int rc = 0;
  DRHOOK_START(codb_IDXF_write);
  if (idx && io_idx >= 0) {
    FILE *fp = CMA_get_fp(&io_idx);
    if (fp) {
      char bitstream[MAXBITS+1];
      const int this_version = 1;
      const int filekind = 1;
      int j, poolno, npools = idx->npools;
      unsigned int typeflag = idx->typeflag;
      int ncols = idx->ncols;
      unsigned int idxf = binary ? IDXB : IDXT;
      int info[8];
      const void *opaque;
      int nbytes, iret, npp;
      int nsets_tot, ncard = idx->ncard;
      double *dcard = NULL;
      
      codb_datetime_(&idx->modification_date, &idx->modification_time);

      CMAWR(&idxf, 1);

      fprintf(fp,"\n%d ! Index-file format version", this_version);
      fprintf(fp,"\n%d ! Index-file kind", filekind);

      fprintf(fp, "\n%d %s", STRLEN(idx->filename), idx->filename);
      fprintf(fp, "\n%d %s", STRLEN(idx->tblname), idx->tblname);
      fprintf(fp, "\n%d %s", STRLEN(idx->idxname), idx->idxname);
      {
	char *s = RemoveWhiteSpace(idx->colnames);
	fprintf(fp, "\n%d %s", STRLEN(s), s);
      }
      {
	char *s = RemoveWhiteSpace(idx->wherecond);
	fprintf(fp, "\n%d %s", STRLEN(s), s);
	FREE(s);
      }
      fprintf(fp, "\n\f");

      {
	ll_t backwd = 0;
	nsets_tot = 0;
	npp = 0;
	for (j=0; j<npools; j++) {
	  odbidx_pp_t *pp = &idx->pp[j];
	  poolno = pp->poolno;
	  if (poolno > 0) {
	    ll_t sz = 0; /* size of the current pool when in binary index-file */
	    int i, nsets = pp->nsets;
	    int idxtype = pp->idxtype;
	    int poolno_sz = 1 * sizeof(int); /* poolno */
	    int ward_sz = 2 * sizeof(ll_t); /* backwd, fwd */
	    int hdr_sz = 3 * sizeof(int);  /* idxtype, nrows, nsets */
	    int delta = poolno_sz + ward_sz;
	    nsets_tot += nsets;
	    sz += poolno_sz;
	    sz += ward_sz;
	    sz += hdr_sz;
	    for (i=0; i<nsets; i++) {
	      const odbidx_set_t *idxset = &pp->idxset[i];
	      const double *value = idxset->value;
	      int ndata = ABS(idxset->ndata);
	      int idxlen = idxset->idxlen;
	      const unsigned int *idxdata = idxset->idxdata;
	      if (ncols > 0) sz += ncols * sizeof(*value); /* value[0..ncols-1] */
	      sz += sizeof(ndata); /* ndata */
	      sz += sizeof(idxlen); /* idxlen */
	      /* "nclusters" is not written for binary file */
	      sz += ndata * sizeof(*idxdata); /* idxdata[] */
	    } /* for (i=0; i<nsets; i++) */
	    /* How many bytes to jump backward to reach the beginning of the
	       previous poolno-word ? */
	    pp->backwd = -backwd;
	    /* How many bytes to jump forward to reach the beginning of the
	       next poolno-word ? */
	    pp->fwd = sz - delta;
	    backwd = sz + delta; /* Remember for the next pool */
	    npp++;
	  } /* if (poolno > 0) */
	} /* for (j=0; j<npools; j++) */
      }

      if (ncols > 0) {
	/* 
	   Create global SELECT DISTINCT -set (over all available [npp] pools).

	   Basically we would like to execute over all pools the following query:

	     SELECT DISTINCT cols FROM table WHERE cond

	   But since all cols-information has already been collected, we just need
	   to pick up the distinct set (total ncard-rows) and fill dcard[].
	*/

	int jc, jr, nr = nsets_tot;
	Bool row_wise = false; /* We need Fortran column-major for codb_cardinality_() */
	result_t *res = ODBc_new_res("codb_IDXF_write() for dcard[]",
				     __FILE__, __LINE__,
				     nr, ncols, NULL, row_wise, -1);
	result_t *sortres = NULL;
	int ntot;
	int *unique_idx = NULL;
	int *keys = NULL;

	res->typeflag = typeflag;

	jr = 0;
	for (j=0; j<npools; j++) {
	  const odbidx_pp_t *pp = &idx->pp[j];
	  poolno = pp->poolno;
	  if (poolno > 0) {
	    int i, nsets = pp->nsets;
	    for (i=0; i<nsets; i++) {
	      const odbidx_set_t *idxset = &pp->idxset[i];
	      const double *value = idxset->value;
	      for (jc=0; jc<ncols; jc++) {
		/* Note below : Using d[jc][jr], not d[jr][jc], since row_wise = false !! */
		res->d[jc][jr] = value[jc];
	      } /* for (jc=0; jc<ncols; jc++) */
	      ++jr;
	    } /* for (i=0; i<nsets; i++) */
	  } /* if (poolno > 0) */
	} /* for (j=0; j<npools; j++) */

	/* Note : Returned unique_idx[] is using Fortran-indexing 1..N, not 0..N-1 !! */
	CALLOC(unique_idx, nr);
	codb_cardinality_(&res->ncols_in, &res->nrows_in, &res->nra,
			  res->mem,
			  &ncard,
			  unique_idx,
			  &nr,
			  NULL);

	/* Values picked for uniqueness may be unordered --> sort them */
	sortres = ODBc_new_res("codb_IDXF_write() for sorted dcard[]",
			       __FILE__, __LINE__,
			       ncard, ncols, NULL, row_wise, -1);

	for (jr=0; jr<ncard; jr++) {
	  int ir = unique_idx[jr] - 1;
	  for (jc=0; jc<ncols; jc++) {
	    sortres->d[jc][jr] = res->d[jc][ir];
	  } /* for (jc=0; jc<ncols; jc++) */
	} /* for (jr=0; jr<ncard; jr++) */
	
	FREE(unique_idx);
	(void) ODBc_unget_data(res);

	ALLOC(keys, ncols);
	for (jc=0; jc<ncols; jc++) {
	  /* Note : Fortran indexing for keys */
	  keys[jc] = jc+1;
	}

	sortres = ODBc_sort(sortres, keys, ncols);

	FREE(keys);

	ntot = ncols * ncard;
	CALLOC(dcard, ntot);

	row_wise = sortres->row_wise;
	j = 0;
	for (jr=0; jr<ncard; jr++) {
	  for (jc=0; jc<ncols; jc++) {
	    /* Note again : after ODBc_sort() row_wise may have changed */
	    dcard[j++] = row_wise ? sortres->d[jr][jc] : sortres->d[jc][jr];
	  } /* for (jc=0; jc<ncols; jc++) */
	} /* for (jr=0; jr<ncard; jr++) */

	(void) ODBc_unget_data(sortres);
      }
      else {
	ncard = 0;
	dcard = NULL;
      }

      idx->ncard = ncard;
      idx->dcard = dcard;

      if (binary) {
	info[0] = (int)idx->creation_date;
	info[1] = (int)idx->creation_time;
	info[2] = (int)idx->modification_date;
	info[3] = (int)idx->modification_time;
	info[4] = ncols;
	info[5] = npp;
	info[6] = (int)typeflag;
	info[7] = ncard;
	CMAWR(info, 8);
	if (ncols > 0) {
	  int ntot = ncols * ncard;
	  CMAWR(dcard, ntot);
	}
      }
      else {
	int nbits = ncols;
	(void) ODBIT_getmap(&typeflag, nbits, MAXBITS, bitstream);
	if (nbits < MAXBITS) {
	  memset(bitstream + nbits, '-', (MAXBITS - nbits) * sizeof(*bitstream));
	  bitstream[MAXBITS] = '\0';
	}

	fprintf(fp, "\n%8.8d %6.6d ! Index creation date & time",
		(int)idx->creation_date, (int)idx->creation_time);
	fprintf(fp, "\n%8.8d %6.6d ! Last modification date & time",
		(int)idx->modification_date, (int)idx->modification_time);
	fprintf(fp, "\n");

	fprintf(fp, "\n%d %d ! ncols npools", ncols, npp);
	fprintf(fp, "\n%u ! typeflag = %s", typeflag, bitstream);
	if (ncols > 0) {
	  int k=0;
	  fprintf(fp, "\n\n%d ! ncard", ncard);
	  for (j=0; j<ncard; j++) {
	    int ii;
	    fprintf(fp, "\n");
	    for (ii=0; ii<ncols; ii++) {
	      int is_string = (typeflag == 0) ? 0 : 
		ODBIT_test(&typeflag, MAXBITS, MAXBITS, ii, ii);
	      if (is_string == 1) {
		register char *c;
		S2D_Union u;
		u.dval = dcard[k];
		if (u.llu == S2D_all_blanks) {
		  c = &u.str[sizeof(double)];
		}
		else {
		  int k;
		  c = u.str;
		  for (k=0; k<sizeof(double); k++) {
		    if (!isprint(*c)) *c = '?';
		    c++;
		  }
		}
		*c = 0;
		fprintf(fp, " '%s'", u.str);
	      }
	      else {
		fprintf(fp, " %.14g", dcard[k]);
	      }
	      ++k;
	    } /* for (ii=0; ii<ncols; ii++) */
	  } /* for (j=0; j<ncard; j++) */
	}
      }

      if (npp > 0) {
	for (j=0; j<npools; j++) {
	  odbidx_pp_t *pp = &idx->pp[j];
	  poolno = pp->poolno;
	  if (poolno > 0) {
	    int i, nsets = pp->nsets;
	    int idxtype = pp->idxtype;
	    int nrows = pp->nrows;
	    
	    if (binary) {
	      info[0] = poolno;
	      CMAWR(info, 1);

	      {
		ll_t ward[2];
		ward[0] = pp->backwd;
		ward[1] = pp->fwd;
		CMAWR(ward, 2);
	      }

	      info[0] = idxtype;
	      info[1] = nrows;
	      info[2] = nsets;
	      CMAWR(info, 3);
	    }
	    else {
	      fprintf(fp, "\n\n%d ! poolno", poolno);
	      fprintf(fp, "\n%d %d %d ! idxtype, nrows, nsets", idxtype, nrows, nsets);
	    }
	    
	    for (i=0; i<nsets; i++) {
	      odbidx_set_t *idxset = &pp->idxset[i];
	      const double *value = idxset->value;
	      int ndata = idxset->ndata;
	      int idxlen = ABS(ndata); /* for now ... */
	      const unsigned int *idxdata = idxset->idxdata;
	      
	      if (binary) {
		if (ncols > 0) {
		  CMAWR(value, ncols);
		}
		info[0] = ndata;
		info[1] = idxlen;
		CMAWR(info, 2);
		ndata = ABS(ndata);
		CMAWR(idxdata, ndata);
	      }
	      else /* text */ {
		int ii;
		int set_number = i + 1;
		fprintf(fp,"\n%d ! set#",set_number);
		if (ncols > 0) {
		  fprintf(fp, "\n");
		  for (ii=0; ii<ncols; ii++) {
		    int is_string = (typeflag == 0) ? 0 : 
		      ODBIT_test(&typeflag, MAXBITS, MAXBITS, ii, ii);
		    if (is_string == 1) {
		      register char *c;
		      S2D_Union u;
		      u.dval = value[ii];
		      if (u.llu == S2D_all_blanks) {
			c = &u.str[sizeof(double)];
		      }
		      else {
			int k;
			c = u.str;
			for (k=0; k<sizeof(double); k++) {
			  if (!isprint(*c)) *c = '?';
			  c++;
			}
		      }
		      *c = 0;
		      fprintf(fp, " '%s'", u.str);
		    }
		    else {
		      fprintf(fp, " %.14g", value[ii]);
		    }
		  } /* for (ii=0; ii<ncols; ii++) */
		}

		idxset->nclusters = 0;

		if (ndata < 0 || idxtype == 2) {
		  if (ndata < 0) {
		    /* PCMA-packed */
		    fprintf(fp, "\n%d %d %d ! ndata, idxlen, nclusters",ndata,idxlen,-ndata);
		    ndata = ABS(ndata);
		    for (ii=0; ii<ndata; ii++) {
		      if (ii%5 == 0) fprintf(fp, "\n");
		      fprintf(fp, " %u", idxdata[ii]);
		    }
		  }
		  else {
		    /* Not PCMA-packed */
#if 0
		    int bit_start = 0, bit_end;
		    fprintf(fp, "\n%d %d %d ! ndata, idxlen, nclusters",ndata,idxlen,ndata);
		    for (ii=0; ii<ndata; ii++) {
		      int nbits;
		      bit_end = bit_start + MAXBITS - 1;
		      bit_end = MIN(bit_end, nrows-1);
		      nbits = bit_end - bit_start + 1;
		      (void) ODBIT_getmap(&idxdata[ii], nbits, MAXBITS, bitstream);
		      if (nbits < MAXBITS) {
			memset(bitstream + nbits, '-', (MAXBITS - nbits) * sizeof(*bitstream));
			bitstream[MAXBITS] = '\0';
		      }
		      fprintf(fp, "\n %10u ! %s : %d - %d", 
			      idxdata[ii], bitstream,
			      bit_start, bit_end);
		      bit_start = bit_end + 1;
		    }
#else
		    int k = 0;
		    int iprev = 0;
		    for (ii=0; ii<ndata; ii++) {
		      if (idxdata[ii] != idxdata[iprev]) {
			iprev = ii;
			k++;
		      }
		    } /* for (ii=1; ii<ndata; ii++) */
		    k++;
		    fprintf(fp, "\n%d %d %d ! ndata, idxlen, nclusters",ndata,idxlen,k);
		    idxset->nclusters = k;
		    k = 0;
		    iprev = 0;
		    for (ii=0; ii<ndata; ii++) {
		      if (idxdata[ii] != idxdata[iprev]) {
			int cnt = ii - iprev;
			if (k++%5 == 0) fprintf(fp, "\n");
			if (cnt > 1) {
			  fprintf(fp, " %d*%u", cnt, idxdata[iprev]);
			}
			else {
			  fprintf(fp, " %u", idxdata[iprev]);
			}
			iprev = ii;
			k++;
		      }
		    } /* for (ii=1; ii<ndata; ii++) */
		    { /* Trailing part */
		      int cnt = ndata - iprev;
		      if (k++%5 == 0) fprintf(fp, "\n");
		      if (cnt > 1) {
			fprintf(fp, " %d*%u", cnt, idxdata[iprev]);
		      }
		      else {
			fprintf(fp, " %u", idxdata[iprev]);
		      }
		    }
#endif
		  }
		}
		else if (ndata == 0) {
		  fprintf(fp, "\n0 0 0 ! ndata, idxlen, nclusters");
		}
		else { /* ndata > 0 */
		  int k = 0;

		  for (ii=0; ii<ndata; ) {
		    int tryinc = 1;
		    int last = Sequence(idxdata, ii, ndata, &tryinc);
		    ii = last + 1;
		    k++;
		  } /* for (ii=0; ii<ndata; ) */

		  idxset->nclusters = k;
		  fprintf(fp, "\n%d %d %d ! ndata, idxlen, nclusters",ndata,idxlen,k);

		  k = 0;
		  for (ii=0; ii<ndata; ) {
		    int tryinc = 1;
		    int last = Sequence(idxdata, ii, ndata, &tryinc);
		    if (k%5 == 0) fprintf(fp, "\n");
		    if (tryinc == 0) {
		      fprintf(fp, " %u", idxdata[ii]);
		    }
		    else if (tryinc == 1) {
		      fprintf(fp, " %u:%u", idxdata[ii], idxdata[last]);
		    }
		    else { /* tryinc > 1 */
		      fprintf(fp, " %u:%u:%d", idxdata[ii], idxdata[last], tryinc);
		    }
		    ii = last + 1;
		    k++;
		  } /* for (ii=0; ii<ndata; ) */

		} /* if (ndata < 0) ... else if (ndata == 0) ... else */

	      } /* if (binary) ... else *text* ... */
	    } /* for (i=0; i<nsets; i++) */
	  }
	} /* for (j=0; j<npools; j++) */
      } /* if (npp > 0) */

      poolno = -1;
      if (binary) {
	CMAWR(&poolno, 1);
      }
      else {
	fprintf(fp, "\n\n%d ! poolno (end-of-pools)\n",poolno);
      }
    }
  }
  DRHOOK_END(0);
  return rc;
}


PUBLIC void *
ODBc_create_index_prepare(int handle, const void *Info, const void *Infoaux)
{
  void *retval = NULL;
  const info_t *info = Info;
  const info_t *infoaux = Infoaux;
  DRHOOK_START(ODBc_create_index_prepare);
  if (info && !infoaux) {
    info_t *retinfo = NULL;
    if (info && info->create_index > 0 && info->nfrom == 1 && info->t) {
      char *sql_query = NULL;
      int j, ncols = info->ncols_true;
      const char *tblname = info->t->name;
      int lentbl = STRLEN(tblname);
      char *setvars = NULL;
      int nsetvars = info->nset;
      int lensetvars = 1;
      char *cols = NULL;
      int lencols = 1;
      const char *wherecond = info->wherecond ? info->wherecond : "1";
      char *pwherecond = S2D_fix(info, wherecond);
      int lenwhere = STRLEN(pwherecond);

      for (j=0; j<nsetvars; j++) {
	const set_t *sv = &info->s[j];
	const char *name = sv->name;
	if (!strequ(name,"$__maxcount__")) {
	  lensetvars += 4 + STRLEN(name) + 3 + 50;
	}
      }

      ALLOC(setvars, lensetvars);
      *setvars = 0;
      for (j=0; j<nsetvars; j++) {
	const set_t *sv = &info->s[j];
	const char *name = sv->name;
	if (!strequ(name,"$__maxcount__")) {
	  double value = sv->value;
	  int len = STRLEN(setvars);
	  char *p = setvars + len;
	  snprintf(p, lensetvars-len, "SET %s = %.14g;",
		   name, value);
	}
      }
      lensetvars = STRLEN(setvars);

      for (j=0; j<ncols; j++) {
	const col_t *colthis = &info->c[j];
	lencols += STRLEN(colthis->name) + 1;
      }

      ALLOC(cols, lencols);
      *cols = 0;
      for (j=0; j<ncols; j++) {
	const col_t *colthis = &info->c[j];
	if (j>0) strcat(cols, ",");
	strcat(cols, colthis->name);
      }
      lencols = STRLEN(cols);

      /* Create an info-chain of 3 auxiliary queries */

      if (ncols > 1) {
	const char fmt[] = 
	  "CREATE VIEW _1_%s AS SELECT count(*) FROM %s;" /* nrows */
	  "%s"
	  "CREATE VIEW _2_%s AS SELECT DISTINCT %s,cksum(%s) FROM %s WHERE %s ORDERBY %s;" /* nsets */
	  "CREATE VIEW _3_%s AS SELECT %s,cksum(%s),#%s FROM %s WHERE %s;";  /* nsets * (ncols - 2) */
      
	int lenq = STRLEN(fmt) + 3 * STRLEN(info->view) +
	  lentbl +
	  lensetvars +
	  3 * lencols + lentbl + lenwhere +
	  2 * lentbl + 2 * lencols + lenwhere;
      
	ALLOC(sql_query, lenq);

	snprintf(sql_query, lenq, fmt,
		 info->view, tblname,
		 setvars,
		 info->view, cols, cols, tblname, pwherecond, cols,
		 info->view, cols, cols, tblname, tblname, pwherecond);
      }
      else if (ncols == 1) { /* we do not need to call cksum() since just one column */
	const char fmt[] = 
	  "CREATE VIEW _1_%s AS SELECT count(*) FROM %s;" /* nrows */
	  "%s"
	  "CREATE VIEW _2_%s AS SELECT DISTINCT %s FROM %s WHERE %s ORDERBY %s;" /* nsets */
	  "CREATE VIEW _3_%s AS SELECT %s,#%s FROM %s WHERE %s;";  /* nsets * (ncols - 1) */
      
	int lenq = STRLEN(fmt) + 3 * STRLEN(info->view) +
	  lentbl +
	  lensetvars +
	  2 * lencols + lentbl + lenwhere +
	  2 * lentbl + lencols + lenwhere;
      
	ALLOC(sql_query, lenq);

	snprintf(sql_query, lenq, fmt,
		 info->view, tblname,
		 setvars,
		 info->view, cols, tblname, pwherecond, cols,
		 info->view, cols, tblname, tblname, pwherecond);
      }
      else { /* ncols == 0 */
	const char fmt[] = 
	  "CREATE VIEW _1_%s AS SELECT count(*) FROM %s;" /* nrows */
	  "CREATE VIEW _2_%s AS SELECT #%s FROM %s WHERE #%s = 1 AND maxcount(1);" /* nsets == 1 */
	  "%s"
	  "CREATE VIEW _3_%s AS SELECT #%s FROM %s WHERE %s;"; /* ndata for the only set */
      
	int lenq = STRLEN(fmt) + 3 * STRLEN(info->view) +
	  lentbl +
	  lentbl + 3 * lentbl +
	  lensetvars +
	  2 * lentbl + lenwhere;
	
	ALLOC(sql_query, lenq);

	snprintf(sql_query, lenq, fmt,
		 info->view, tblname,
		 info->view, tblname, tblname, tblname,
		 setvars,
		 info->view, tblname, tblname, pwherecond);
      }

      FREE(pwherecond);
      FREE(cols);
      FREE(setvars);

      if (sql_query) {
	retinfo = ODBc_sql_prepare(handle, sql_query, NULL, 0);
	FREE(sql_query);
      }
    }
    retval = retinfo;
  }
  else if (info && infoaux) {
    int j, ncols = info->ncols_true;
    char *cols = NULL;
    int lencols = 1;
    odbidx_t *stored_idx;
    CALLOC(stored_idx, 1);
    stored_idx->tblname = STRDUP(info->t->name);
    stored_idx->idxname = STRDUP(info->view);
    stored_idx->wherecond = info->wherecond ? STRDUP(info->wherecond) : STRDUP("1");

    codb_datetime_(&stored_idx->creation_date, &stored_idx->creation_time);
    stored_idx->modification_date = stored_idx->creation_date;
    stored_idx->modification_time = stored_idx->creation_time;

    stored_idx->typeflag = 0;

    if (ncols > 0) {
      int string_count = 0;
      unsigned int stridx[MAXBITS]; /* Note : ncols >= 0 && <= MAXBITS enforced by odb98.x compiler */
      memset(stridx, 0, sizeof(stridx));
      string_count = 0;
      for (j=0; j<ncols; j++) {
	const col_t *colthis = &info->c[j];
	lencols += STRLEN(colthis->name) + 1;
	if (colthis->dtnum == DATATYPE_STRING) {
	  stridx[string_count++] = j;
	}
      }
      if (string_count > 0) {
	unsigned int typeflag = 0;
	const int nbits = MAXBITS;
	const int nbw = MAXBITS;
	const int nstridx = MAXBITS;
	codbit_setidx_(&typeflag, &nbits, &nbw, stridx, &string_count);
	stored_idx->typeflag = typeflag;
      }
      ALLOC(cols, lencols);
      *cols = 0;
      for (j=0; j<ncols; j++) {
	const col_t *colthis = &info->c[j];
	if (j>0) strcat(cols, ",");
	strcat(cols, colthis->name);
      }
      stored_idx->colnames = cols;
    }
    else { /* ncols == 0 */
      stored_idx->colnames = STRDUP("*");
    }

    stored_idx->npools = 0;
    stored_idx->pp = NULL;
    stored_idx->ncols = ncols;

    retval = stored_idx;
  }
  DRHOOK_END(0);
  return retval;
}


typedef struct _btree_t {
  double cksum;
  int keylen; /* in bytes */
  void *keys;
  unsigned int *idxdata;
  int ndata;
  int nalloc;
} btree_t;


PRIVATE int
cmp_bt(const void *A, const void *B)
{
  const btree_t *a = A;
  const btree_t *b = B;
  if (a->cksum == b->cksum) {
    return memcmp(a->keys, b->keys, a->keylen);
  }
  else return (a->cksum > b->cksum) ? 1 : -1;
}


PUBLIC odbidx_t *
codb_IDXF_create_index(int handle,
		       void *Idx, int poolno, 
		       int idxtype, 
		       void *Info, int *Nrows)
{
  int nrows_out = 0;
  odbidx_t *idx = Idx;
  info_t *info = Info;
  DRHOOK_START(codb_IDXF_create_index);
  if (idx && info && poolno > 0 && idxtype > 0) {
    info_t *info1 = info; /* SELECT count(*) FROM table */
    info_t *info2 = info1->next; /* Usually SELECT DISTINCT cols,cksum(cols) FROM table WHERE ... ORDERBY cols */
    info_t *info3 = info2 ? info2->next : NULL; /* Usually SELECT cols,cksum(cols),#table FROM table WHERE ... */
    if (info1 && info2 && info3) {
      FILE *fp_echo = ODBc_get_debug_fp();
      int nrows, ncols = idx->ncols;
      int jloc = -1, npp = idx->npools;
      int nsets;
      odbidx_pp_t *pp = NULL;
      Bool do_alloc = false;
      Bool poolno_okay = ODB_in_permanent_poolmask(handle, poolno) ? true : false;

      if (!poolno_okay) {
	ODB_fprintf(fp_echo,
		    "***Warning in codb_IDXF_create_index() of %s : Pool number %d not in poolmask\n",
		    info->view, poolno);
	nrows = 0; /* Not an error */
	goto finish;
      }

      /* Running#1 : SELECT count(*) FROM table */
      nrows = ODBc_sql_exec(handle, info1, poolno, NULL, NULL);
      if (nrows <= 0) goto finish;

      if (npp <= 0 || !idx->pp) {
	FREE(idx->pp);
	npp = 1;
	do_alloc = true;
      }
      else {
	int j;
	Bool found = false;
	pp = idx->pp;
	for (j=0; j<npp; j++) {
	  if (pp->poolno == poolno) {
	    jloc = j;
	    found = true;
	    FreeIdxSets(pp->idxset, pp->nsets, ncols);
	    FREE(pp->idxset);
	    pp->nsets = 0;
	    break;
	  }
	  ++pp;
	}
	if (!found) {
	  ++npp;
	  do_alloc = true;
	}
      }
      if (do_alloc) {
	REALLOC(idx->pp, npp);
	jloc = npp - 1;
      }

      idx->npools = npp;

      pp = &idx->pp[jloc];

      pp->backwd = pp->fwd = 0;

      pp->poolno = poolno;
      pp->idxtype = idxtype;

      if (nrows == 1) {
	result_t *res = ODBc_get_data(handle, NULL, info1, poolno, 1, nrows, false, NULL);
	nrows = res->d[0][0];
	res = ODBc_unget_data(res);
      }
      pp->nrows = nrows;
	    
      /* Running#2 : 
	 if (ncols > 1)
	   SELECT DISTINCT cols,cksum(cols) FROM table WHERE ... ORDERBY cols 
	 else if (ncols == 1)
	   SELECT DISTINCT cols FROM table WHERE ... ORDERBY cols
	 else
	   SELECT #table FROM table WHERE #table = 1 AND maxcount(1);
      */

      if (info2->has_select_distinct) {
	codb_hash_set_lock_();
	codb_hash_init_();
      }

      nsets = pp->nsets = ODBc_sql_exec(handle, info2, poolno, NULL, NULL);

      if (info2->has_select_distinct) {
	codb_hash_init_();
	codb_hash_unset_lock_();
      }

      if (nsets > 0) {
	odbidx_set_t *idxset;
	CALLOC(idxset, nsets);

	if (ncols > 0) {
	  result_t *uniq = ODBc_get_data(handle, NULL, info2, poolno, 1, nsets, true, NULL);
	  if (uniq) {
	    /* Running#3 : 
	       if (ncols > 1)
  	         SELECT cols,cksum(cols),#table FROM table WHERE cond
	       else if (ncols == 1)
  	         SELECT cols,#table FROM table WHERE cond
	    */
	    int nres = ODBc_sql_exec(handle, info3, poolno, NULL, NULL);
	    result_t *res = ODBc_get_data(handle, NULL, info3, poolno, 1, nres, false, NULL);
	    if (res) {
	      int jcksum = (ncols == 1) ? 0 : ncols;
	      int jrowid = jcksum + 1;
	      int js, ntot = ncols * nsets;
	      double *value = NULL;
	      int keylen = ncols * sizeof(*value);
	      btree_t *bt = NULL;
	      
	      ALLOC(value, ntot);
	      
	      { /* Create binary tree, where each branch (set) has 
		   cksum(cols) and values (keys) */
		double *pvalue = value;
		btree_t *p;
		DRHOOK_START(codb_IDXF_create_index@qsort);

		CALLOC(bt, nsets); p = bt;

		for (js=0; js<nsets; js++) {
		  int jcol;
		  double cksum;
		  for (jcol=0; jcol<ncols; jcol++) {
		    pvalue[jcol] = uniq->row_wise ? uniq->d[js][jcol] : uniq->d[jcol][js];
		  }
		  cksum = pvalue[jcksum];
		  
		  p->cksum = cksum;
		  p->keylen = keylen;
		  p->keys = pvalue;
		  ++p;

		  pvalue += ncols;
		} /* for (js=0; js<nsets; js++) */

		qsort(bt, nsets, sizeof(btree_t), cmp_bt);
		DRHOOK_END(nsets);
	      }

	      { /* Perform binary searches for each row in the res-set */
		int j;
		int ichunk = nres/nsets + 1; /* alloc increment ; can't be too large */
		double *tmp = NULL;
		btree_t *find = NULL;
		DRHOOK_START(codb_IDXF_create_index@bsearch);
		
		CALLOC(find, 1); find->keylen = keylen;

		if (ncols > 1) ALLOC(tmp, ncols);
		
		for (j=0; j<nres; j++) {
		  int jcol;
		  btree_t *match = NULL;
		  
		  find->cksum = res->row_wise ? res->d[j][jcksum] : res->d[jcksum][j];
		  if (ncols > 1) {
		    for (jcol=0; jcol<ncols; jcol++) {
		      tmp[jcol] = res->row_wise ? res->d[j][jcol] : res->d[jcol][j];
		    }
		  }
		  else
		    tmp = res->row_wise ? &res->d[j][0] : &res->d[0][j];

		  find->keys = tmp;

		  match = bsearch(find, bt, nsets, sizeof(btree_t), cmp_bt);
		  if (match) {
		    int rowid = res->row_wise ? res->d[j][jrowid] : res->d[jrowid][j];
		    int nalloc = match->nalloc;
		    int ndata = match->ndata;
		    if (nalloc <= ndata) {
		      nalloc += MIN(ichunk,nres-j);
		      REALLOC(match->idxdata, nalloc);
		      match->nalloc = nalloc;
		    }
		    match->idxdata[ndata++] = rowid - 1;
		    match->ndata = ndata;
		  }
		  else { /* Should not happen */
		    fprintf(stderr,
			    "***Error in codb_IDXF_create_index() (ncols=%d): Unable to match record#%d, poolno=%d\n",
			    ncols, j, poolno);
		    RAISE(SIGABRT);
		  }
		} /* for (j=0; j<nres; j++) */

		if (ncols > 1) FREE(tmp);
		FREE(find);
		
		nrows_out = res->nrows_out;
		res = ODBc_unget_data(res);

		DRHOOK_END(nres);
	      }

	      { /* The final fiddle : fill in odbidx_set_t's */
		odbidx_set_t *pidxset = idxset;
		btree_t *p = bt;
		for (js=0; js<nsets; js++) {
		  int ndata = p->ndata;
		  unsigned int *idxdata = p->idxdata;

		  ALLOC(pidxset->value, ncols);
		  memcpy(pidxset->value, p->keys, keylen);

		  if (idxtype == 2) {
		    int nr = ndata;
		    unsigned int *bitmap;
		    const int maxbits = MAXBITS;
		    ndata = RNDUP_DIV(nrows, maxbits);
		    CALLOC(bitmap, ndata);
		    codbit_setidx_(bitmap,&nrows,&maxbits,idxdata,&nr);
		    FREE(idxdata);
		    idxdata = bitmap;
		  }
		  pidxset->nclusters = 0;
		  pidxset->ndata = ndata;
		  pidxset->idxlen = ndata;
		  pidxset->idxdata = idxdata;
		  
		  ++pidxset;
		  ++p;
		} /* for (js=0; js<nsets; js++) */
	      }

	      FREE(bt);
	      FREE(value);
	    } /* if (res) */
	    uniq = ODBc_unget_data(uniq);
	  } /* if (uniq) */
	} /* if (ncols > 0) */
	else { /* ncols == 0 --> just one idxset */
	  /* Running#3 : SELECT #table FROM table WHERE cond */
	  int ndata = ODBc_sql_exec(handle, info3, poolno, NULL, NULL);
	  result_t *res = ODBc_get_data(handle, NULL, info3, poolno, 1, ndata, false, NULL);
	  if (res) {
	    int j, nr = res->nrows_out;
	    unsigned int *idxdata;
	    ALLOC(idxdata, nr);
	    for (j=0; j<nr; j++) {
	      int rowid = res->row_wise ? res->d[j][0] : res->d[0][j];
	      idxdata[j] = rowid - 1;
	    }
	    nrows_out = nr;
	    res = ODBc_unget_data(res);
	    if (idxtype == 1) {
	      ndata = nr;
	    }
	    else if (idxtype == 2) {
	      unsigned int *bitmap;
	      const int maxbits = MAXBITS;
	      ndata = RNDUP_DIV(nrows, maxbits);
	      CALLOC(bitmap, ndata);
	      codbit_setidx_(bitmap,&nrows,&maxbits,idxdata,&nr);
	      FREE(idxdata);
	      idxdata = bitmap;
	    }
	    idxset->value = NULL;
	    idxset->ndata = ndata;
	    idxset->idxlen = ndata;
	    idxset->idxdata = idxdata;
	  } /* if (res) */
	} /* if (ncols > 0) ... else ... */
	pp->idxset = idxset;
      }
      else {
	pp->idxset = NULL;
	pp->nsets = 0;
      }
    }
  }
 finish:
  if (Nrows) *Nrows = nrows_out;
  DRHOOK_END(0);
  return idx;
}
