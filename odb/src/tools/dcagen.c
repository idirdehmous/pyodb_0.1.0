#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "alloc.h"
#include "privpub.h"
#include "swapbytes.h"
#include "magicwords.h"
#include "pcma_extern.h"
#include "idx.h"
#include "cmaio.h"

#define ODB_CACHE_LIMIT 128 /* In MegaBytes i.e. 1024 * 1024 -chunks */
static int odb_cache_limit = ODB_CACHE_LIMIT;

#define ODB_CACHE_MAX 1500 /* No more than this many MegaBytes (1024 * 1024 bytes) per cachefile */

#define ODB_CACHE_DISK "cache" /* Override via f.ex. ODB_CACHE_DISK="/fast/disk" */

#define BAILOUT 100 /* Bailout and give up cardinality study after this/too many distinct entries found */
static int bailout = BAILOUT;

#define NIBUF 14     /* Do not change this ! Never-ever !!  */
#define NDBUF  4     /* This can however go up to NDBUF_MAX */
#define NDBUF_MAX 50 /* Keep in sync with aux/dca.c         */

static int  extract = 0;
static int    debug = 0;
static char **colname = NULL;
static char **datatype = NULL;
static char *oprefix = NULL;
int oprefix_percent_s = 0;
static char *cache_disk = NULL;

static int
read_ddfile(const char *ddfile, const char *tablename, int *npools)
{
  int ncols = 0;
  int unit, iret;
  /* FILE *fp = fopen(ddfile,"r"); */
  FILE *fp = NULL;
  cma_open_(&unit, ddfile, "r", &iret, strlen(ddfile), strlen("r"));
  fp = CMA_get_fp(&unit);
  if (fp) {
    char line[256];
    char tbl[4096];
    int n, jcol;
    int found = 0;
    int lineno = 0;
    while (!feof(fp)) {
      int nel = 0;
      lineno++;
      if (!fgets(line, sizeof(line), fp)) break;
      if (npools && lineno == 5) { /* get npools */
        nel = sscanf(line,"%d",npools);
        continue;
      }
      nel = sscanf(line,"%s %d",tbl,&n);
      if (nel != 2) continue;
      /* fprintf(stderr,"%d: %s %d\n",lineno,tbl,n); */
      if (found && *tbl == '@') break;
      if (*tbl == '@' && strequ(tbl+1,tablename)) {
        ncols = n;
        CALLOC(colname, ncols);
        CALLOC(datatype, ncols);
        found = 1;
        jcol = 0;
      }
      else if (found) {
        char *p = strchr(tbl,':');
        if (p) {
          /*
	    char *at = strchr(tbl,'@');
	    if (at) *at = '\0';
          */
          colname[jcol] = STRDUP(p+1);
          *p = '\0';
          datatype[jcol] = STRDUP(tbl);
          jcol++;
          if (jcol >= ncols) break;
        }
      }
    } /* while (!feof(fp)) */
    /* fclose(fp); */
    cma_close_(&unit, &iret);
  }
  return ncols;
}

#define EVAL(allok, calc_loop, type, swapfunc) \
{ \
  int j; \
  type Mdi = mdi; \
  type *d = data; \
  double *dtmp = allok ? NULL : data; \
  if (allok) ALLOC(dtmp, nrows); \
  if (swap_bytes) swapfunc(d, &nrows); \
  if (allok) for (j=0; j<nrows; j++) dtmp[j] = d[j]; \
  codb_cardinality_(NULL, &nrows, NULL, dtmp, &Ncard, NULL, NULL, &bailout); \
  if (allok) FREE(dtmp); \
  if (calc_loop) { \
    if (Ncard == 1) { /* a common short-cut */ \
      type tmp = d[0]; \
      if (tmp == Mdi || tmp == -Mdi) Nmdis = nrows; \
      else Minvalue = Maxvalue = Avg = tmp; \
    } \
    else { \
      for (j=0; j<nrows; j++) { \
        type tmp = d[j]; \
        if (tmp == Mdi || tmp == -Mdi) Nmdis++; \
        else { \
          if (tmp < Minvalue) Minvalue = tmp; \
          if (tmp > Maxvalue) Maxvalue = tmp; \
          Avg += tmp; N++; \
        } \
      } /* for (j=0; j<nrows; j++) */ \
      if (N > 0) { Avg /= N; } \
    } \
  } \
}


static
void swapdummy(void *x, int *n) { }


static void
update_items(void *data, int nrows, unsigned int dtnum,
             int swap_bytes, int mdi,
             int *nmdis, 
             double *avg, double *minvalue, double *maxvalue, int *ncard)
{
  int Nmdis = 0; 
  double Avg = 0;
  double Minvalue = mdi; 
  double Maxvalue = -mdi;
  int Ncard = 0;
  int N = 0;
  if (nrows > 0) {
    if (dtnum == DATATYPE_REAL8) {
      EVAL(0, 1, double, swap8bytes_);
    }
    else if (dtnum == DATATYPE_INT4) {
      EVAL(1, 1, int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_LINKOFFSET) {
      EVAL(1, 1, unsigned int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_LINKLEN) {
      EVAL(1, 1, unsigned int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_YYYYMMDD) {
      EVAL(1, 1, int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_HHMMSS) {
      EVAL(1, 1, int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_BITFIELD) {
      EVAL(1, 1, int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_STRING) {
      EVAL(0, 0, string, swap8bytes_);
    }
    else if (dtnum == DATATYPE_UINT4) {
      EVAL(1, 1, unsigned int, swap4bytes_);
    }
    else if (dtnum == DATATYPE_REAL4) {
      EVAL(1, 1, float, swap4bytes_);
    }
    else if (dtnum == DATATYPE_INT2) {
      EVAL(1, 1, short int, swap2bytes_);
    }
    else if (dtnum == DATATYPE_UINT2) {
      EVAL(1, 1, unsigned short int, swap2bytes_);
    }
    else if (dtnum == DATATYPE_INT1) {
      EVAL(1, 1, signed char, swapdummy);
    }
    else if (dtnum == DATATYPE_UINT1) {
      EVAL(1, 1, unsigned char, swapdummy);
    }
  }
  *nmdis = Nmdis; 
  *avg = Avg;
  *minvalue = Minvalue; 
  *maxvalue = Maxvalue;
  *ncard = Ncard;
}


typedef struct filecache_t {
  unsigned long long int cache_seekpos;
  char *name;
  char *cache_file;
  int cache_unit;
  int cache_limit; 
  int cache_cnt;
  struct filecache_t *nextfc;
} filecache_t;

static filecache_t *filecache = NULL;
static filecache_t *pfilecache = NULL;

static filecache_t *alloc_filecache(const char *name)
{
  if (pfilecache) {
    CALLOC(pfilecache->nextfc,1);
    pfilecache = pfilecache->nextfc;
  }
  else {
    CALLOC(filecache,1);
    pfilecache = filecache;
  }
  pfilecache->cache_seekpos = 0;
  pfilecache->name = STRDUP(name);
  pfilecache->cache_file = NULL;
  pfilecache->cache_unit = -1;
  pfilecache->cache_limit = 0; 
  pfilecache->cache_cnt = 0;
  pfilecache->nextfc = NULL;
  return pfilecache;
}

static filecache_t *get_filecache(const char *name, int *alloc)
{
  filecache_t *pthis = filecache;
  while (pthis) {
    if (strequ(pthis->name, name)) break;
    pthis = pthis->nextfc;
  }
  if (pthis) {
    if (alloc) *alloc = 0;
  }
  else {
    if (alloc) {
      pthis = alloc_filecache(name);
      *alloc = 1;
    }
  }
  if (debug) fprintf(stderr,"get_filecache(name='%s', *alloc=%s) --> %p\n",
		     name, alloc ? (*alloc ? "1" : "0") : NIL,
		     pthis);
  return pthis;
}



static int free_filecache(filecache_t *pfc)
{
  int rc = 0;
  if (pfc) {
    if (pfc->name) FREE(pfc->name);
    if (pfc->cache_file) FREE(pfc->cache_file);
    FREE(pfc);
  }
  return rc;
}


typedef struct colcache_t {
  char *colname;
  int unpack;
  filecache_t *fc;
  int fc_alloc_here;
  struct colcache_t *next;
} colcache_t;

static colcache_t *cache = NULL;
static colcache_t *pcache = NULL;

static colcache_t *
add_cache(const char *col, const char *tablename, int unpack, int *errflg);

static colcache_t *
in_cache(const char *col, const char *tablename, int *unpack)
{
  colcache_t *pthis = NULL;
  colcache_t *pc = cache;
  char *colname = (char *)col;
  int allok = 0;
  int found_star = 0;
  static int recur = 0;
  if (unpack) *unpack = 0;
  if (pc && tablename && *col != '*') {
    int len = strlen(col) + 1 + strlen(tablename) + 1;
    ALLOC(colname,len);
    snprintf(colname,len,"%s@%s",col,tablename);
    allok = 1;
  }
  while (pc) {
    if (strequ(colname,pc->colname)) {
      if (unpack) *unpack = pc->unpack;
      pthis = pc;
      break;
    }
    else if (*pc->colname == '*') {
      found_star = 1;
    }
    pc = pc->next;
  }
  if (!pthis && found_star && !recur) {
    pc = cache;
    while (pc) {
      if (*pc->colname == '*') {
	recur++;
	pthis = add_cache(colname, NULL, pc->unpack, NULL);
	recur--;
	if (unpack) *unpack = pc->unpack;
	break;
      }
      pc = pc->next;
    }
  }
  if (allok) FREE(colname);
  return pthis;
}


static colcache_t *
add_cache(const char *col, const char *tablename, int unpack, int *errflg)
{
  colcache_t *pthis = NULL;
  if (!tablename && col) {
    const char *at = strchr(col,'@');
    if (at) tablename = at + 1;
  }
  if (!tablename) {
    fprintf(stderr,"***Error: Always supply the -c or -C option AFTER the -t option\n");
    if (errflg) (*errflg)++;
  }
  else if (col) {
    char *colname = STRDUP(col);
    char *at = strchr(colname,'@');
    if (at) *at = '\0'; /* No more "colname@table"; just "colname" */
    if (!at || (at && strequ(at+1,tablename))) {

      /* check for links with nickname i.e.
	 {child_table.len|child_table.length|child_table.off|child_table.offset}[@table] */

      {
	char *pdot = strchr(colname,'.');
	int is_len = pdot && (strequ(pdot+1,"length") || strequ(pdot+1,"len"));
	int is_off = pdot && !is_len && (strequ(pdot+1,"offset") || strequ(pdot+1,"off"));
	if (pdot && !is_len && !is_off) {
	  fprintf(stderr,"***Error: Unrecognized column name syntax for '%s'\n",col);
	  if (errflg) (*errflg)++;
	  goto finish; /* skip this entry and flag error */
	}
	if (pdot) *pdot = '\0'; /* colname.{len,offset} now just a colname */
	/* make new colname, if applicable */
	if (is_len) {
	  char *name = NULL;
	  int len = strlen("LINKLEN(") + strlen(colname) + strlen(")") + 1;
	  ALLOC(name,len);
	  snprintf(name,len,"LINKLEN(%s)",colname);
	  FREE(colname);
	  colname = name;
	}
	else if (is_off) {
	  char *name = NULL;
	  int len = strlen("LINKOFFSET(") + strlen(colname) + strlen(")") + 1;
	  ALLOC(name,len);
	  snprintf(name,len,"LINKOFFSET(%s)",colname);
	  FREE(colname);
	  colname = name;
	}
      }

      /* check if already in list; if true, then update the unpack-flag only */

      if ((pthis = in_cache(colname,tablename,NULL)) == NULL) {
	/* not in list ==> create new entry */
	int len = strlen(colname) + 1 + strlen(tablename) + 1;
	if (pcache) {
	  CALLOC(pcache->next,1);
	  pcache = pcache->next;
	}
	else {
	  CALLOC(cache,1);
	  pcache = cache;
	}
	ALLOC(pcache->colname, len);
	snprintf(pcache->colname, len, "%s@%s", colname, tablename);

	if (*colname != '*') {
	  pcache->fc = get_filecache(oprefix ? 
				     (oprefix_percent_s ? tablename : oprefix) : 
				     pcache->colname, 
				     &pcache->fc_alloc_here);
	}
	else {
	  pcache->fc = NULL;
	  pcache->fc_alloc_here = 0;
	}

	pthis = pcache;
      }

      pthis->unpack = unpack;
    }
  finish:
    FREE(colname);
  }
  return pthis;
}


static int
close_cache(colcache_t *pc)
{
  int rc = 0;
  if (pc && pc->fc && pc->fc->cache_unit != -1) {
    int iret;
    if (!extract) {
      if (debug) fprintf(stderr,
			 "close_cache(%p): Closing file '%s', CMA I/O-unit = %d\n",
			 pc, pc->fc->cache_file, pc->fc->cache_unit);
      cma_close_(&pc->fc->cache_unit, &iret);
    }
    pc->fc->cache_unit = -1;
    pc->fc->cache_seekpos = 0;
  }
  return rc;
}


static int
open_cache(colcache_t *pc)
{
  int rc = 0;
  if (pc && pc->fc && pc->fc->cache_unit != -1 && pc->fc->cache_seekpos > pc->fc->cache_limit) {
    (void) close_cache(pc);
  }
  if (extract && pc && pc->fc && pc->fc->cache_unit == -1) {
    pc->fc->cache_unit = -2; /* Pretend its open */
    /* do nothing else */
  }
  else if (pc  && pc->fc && pc->fc->cache_unit == -1) {
    int iret;
    FILE *fp = NULL;
    char *mode = "w";
    const int modelen = 1;
    int len = strlen(cache_disk) + 1 + strlen(pc->fc->name) + 6 + 20 + 1;

    if (pc->fc->cache_limit == 0) {
      char *env = getenv("ODB_CACHE_LIMIT"); /* In MegaBytes */
      if (env) pc->fc->cache_limit = atoi(env);
      else     pc->fc->cache_limit = odb_cache_limit;
      pc->fc->cache_limit = MAX(pc->fc->cache_limit, 1);
      pc->fc->cache_limit = MIN(pc->fc->cache_limit, ODB_CACHE_MAX);
      pc->fc->cache_limit *= 1024 * 1024; /* Convert to bytes */
    }

    do {
      FREE(pc->fc->cache_file);
      ALLOC(pc->fc->cache_file,len);
      snprintf(pc->fc->cache_file,len,
	       "%s/%s.cache.%d",cache_disk,pc->fc->name,++(pc->fc->cache_cnt));
      cma_filesize_(pc->fc->cache_file, &iret, len);
    } while (iret > pc->fc->cache_limit);

    if (iret > 0) { /* File already exists; using append mode */
      mode = "a";
      pc->fc->cache_seekpos = iret;
    }
    else {
      pc->fc->cache_seekpos = 0;
    }

    if (debug) fprintf(stderr,
		       "open_cache(%p): Opening file '%s', mode='%s', seekpos = %llu\n",
		       pc, pc->fc->cache_file, mode, pc->fc->cache_seekpos);

    cma_open_(&pc->fc->cache_unit, pc->fc->cache_file, mode, &iret, len, modelen);
    fp = CMA_get_fp(&pc->fc->cache_unit);
    if (iret != 1 || !fp) {
      fprintf(stderr,
	      "Error: Unable to open ODB cache data file '%s' for writing (mode=%s)\n",
	      pc->fc->cache_file, mode);
      rc++;
    }

    if (pc->fc->cache_seekpos == 0) { /* Write magic word "OCAC" at the beginning */
      const int rev = 0;
      unsigned int ocac = 0;
      const int len_bytes = sizeof(ocac);
      int nwrite = 0;
      get_magic_ocac_(&rev, &ocac);
      cma_writeb_(&pc->fc->cache_unit, (const byte1 *)&ocac, &len_bytes, &nwrite);
      if (nwrite != len_bytes) {
	fprintf(stderr,
		"Error: Unable to write initial %d bytes to cache-file '%s' : retcode=%d\n",
		len_bytes, pc->fc->cache_file, nwrite);
	rc++;
      }
      else {
	pc->fc->cache_seekpos += len_bytes;
      }
    }
  }
  return rc;
}


static int
write_cache(colcache_t *pc, int j, int unit, void *pdata, 
	    int nbytes, int *Nread, int jp, int line)
{
  int rc = 0;
  rc = open_cache(pc);
  if (rc == 0 && pc && pc->fc->cache_unit != -1) {
    int nread = 0;
    byte1 *data = pdata;
    if (!pdata) ALLOC(data, nbytes);
    if (unit >= 0) {
      cma_readb_(&unit, data, &nbytes, &nread);
      nread = (nread == nbytes) ? 0 : -1;
    }
    if (!extract && (nread == 0 && nbytes > 0)) {
      int nwrite;
      cma_writeb_(&pc->fc->cache_unit, data, &nbytes, &nwrite);
      if (nwrite != nbytes) {
	fprintf(stderr,
		"Error: Unable to write %d bytes to cache-file '%s'"
		" column#%d '%s', datatype '%s', pool-chunk#%d : retcode=%d (called from %s:%d)\n",
		nbytes, pc->fc->cache_file, j, pc->colname, datatype[j], jp, nwrite,
		__FILE__, line);
	rc++;
      }
    }
    if (Nread) *Nread = nread;
    if (!pdata) FREE(data);
  }
  return rc;
}


#define COPY(allok, type) \
{ \
  int j; \
  double *d = data; \
  type *dtmp = allok ? NULL : data; \
  if (allok) ALLOC(dtmp, nrows); \
  if (allok) for (j=0; j<nrows; j++) dtmp[j] = d[j]; \
  nbytes = sizeof(*dtmp) * nrows; \
  cma_writeb_(&pc->fc->cache_unit, (const byte1 *)dtmp, &nbytes, &nwrite); \
  if (allok) FREE(dtmp); \
}


static int
write_cache2(colcache_t *pc, int j, void *data, int nrows, 
	     uint dtnum, int *Nbytes, int jp, int line)
{
  int rc = 0;
  int nbytes = 0;
  if (extract) return rc;
  if (nrows > 0) rc = open_cache(pc);
  if (nrows > 0 && rc == 0 && pc && pc->fc->cache_unit != -1) {
    int nwrite = -1;

    if (dtnum == DATATYPE_REAL8) {
      COPY(0, double);
    }
    else if (dtnum == DATATYPE_INT4) {
      COPY(1, int);
    }
    else if (dtnum == DATATYPE_LINKOFFSET) {
      COPY(1, unsigned int);
    }
    else if (dtnum == DATATYPE_LINKLEN) {
      COPY(1, unsigned int);
    }
    else if (dtnum == DATATYPE_YYYYMMDD) {
      COPY(1, int);
    }
    else if (dtnum == DATATYPE_HHMMSS) {
      COPY(1, int);
    }
    else if (dtnum == DATATYPE_BITFIELD) {
      COPY(1, int);
    }
    else if (dtnum == DATATYPE_STRING) {
      COPY(0, string);
    }
    else if (dtnum == DATATYPE_UINT4) {
      COPY(1, unsigned int);
    }
    else if (dtnum == DATATYPE_REAL4) {
      COPY(1, float);
    }
    else if (dtnum == DATATYPE_INT2) {
      COPY(1, short int);
    }
    else if (dtnum == DATATYPE_UINT2) {
      COPY(1, unsigned short int);
    }
    else if (dtnum == DATATYPE_INT1) {
      COPY(1, signed char);
    }
    else if (dtnum == DATATYPE_UINT1) {
      COPY(1, unsigned char);
    }
    else if (dtnum == DATATYPE_BUFR) {
      COPY(1, bufr);
    }
    else if (dtnum == DATATYPE_GRIB) {
      COPY(1, grib);
    }
    else if (dtnum == DATATYPE_INT8) {
      COPY(1, longlong);
    }
    else if (dtnum == DATATYPE_UINT8) {
      COPY(1, ulonglong);
    }

    if (nwrite != nbytes) {
      fprintf(stderr,
              "Error: Unable to write %d bytes (unpacked) to cache-file '%s'"
              " column#%d '%s', datatype '%s', pool-chunk#%d : retcode=%d (called from %s:%d)\n",
              nbytes, pc->fc->cache_file, j, pc->colname, datatype[j], jp, nwrite,
              __FILE__, line);
      rc++;
      nbytes = 0;
    }
  }

  if (Nbytes) *Nbytes = nbytes;
  return rc;
}



#define FLAGS "aAbB:c:C:dhf:i:l:no:p:t:ux"

#define USAGE \
"Usage: %s \n" \
"       [-b]      (create binary DCA-file)\n" \
"       [-c colname@table]  (cache column(s) 'colname@table' to file cache/colname@table.cache.<#>; after -t option)\n" \
"       [-C colname@table]  (same as -c, but column will be unpacked & written to cache-file)\n" \
"       [-a]      (cache all encountered columns *without* unpacking)\n" \
"       [-A]      (cache all encountered columns WITH unpacking them first)\n" \
"       [-x] (extract only 'colname poolno nrows nmdis cardinality min max ' of the column(s) given via -C/-c)\n" \
"       [-d]      (to activate debug output)\n" \
"       [-h]      (suppress printing the header -- the first line)\n" \
"       [-u]      (update avg/min/max, nmdis, cardinality)\n" \
"       [-n]      (do NOT update i.e. calculate avg/min/max, nmdis, cardinality)\n" \
"       [-i ODB_CACHE_LIMIT] (if -c/-C used specify approx. maxsize of files in MBytes; default=%d)\n" \
"       [-B bailout] (max. no of distinct items allowed before giving up cardinality study; default=%d)\n" \
"       -t table_name\n" \
"       -f dd_input_file\n" \
"       -l dbname\n" \
"       [-p starting_pool_number]\n" \
"       [-o output_file_prefix] (a special case: if set to %%s, then use the prevailing tablename)\n" \
"       ODB_table_data_file(s) > Direct_Column_Access_file\n"

static void set_cache_disk()
{
  char *env = getenv("ODB_CACHE_DISK");
  if (!env) env = ODB_CACHE_DISK;
  cache_disk = STRDUP(env);
}

int main(int argc, char *argv[]) {
  int jarg;
  int poolno = 1;
  int rc = 0;
  const double mdi = 2147483647;
  char *tablename = NULL;
  char *ddfile = NULL;
  char *dbname = NULL;
  int c;
  int errflg = 0;
  int ncols_ref = 0;
  int update = 0;
  int npools = 1;
  int jptot = 1;
  int binary = 0;
  int suppress_header = 0;
  int i_am_little;
  extern int ec_is_little_endian();
  extern void ec_set_umask_(); /* ifsaux/support/endian.c */
  
  putenv("DR_HOOK=0"); /* We do not want Dr.Hook to interfere */

  ec_set_umask_(); /* Is umask via export EC_SET_UMASK=<octal_umask> given ? */

  i_am_little = ec_is_little_endian();

  set_cache_disk();

  while ((c = getopt(argc, argv, FLAGS)) != -1) {
    switch (c) {
    case 'a':
      (void) add_cache("*", tablename, 0, &errflg);
      break;
    case 'A':
      (void) add_cache("*", tablename, 1, &errflg);
      break;
    case 'b':
      binary = 1;
      break;
    case 'B':
      bailout = atoi(optarg);
      break;
    case 'c':
      (void) add_cache(optarg, tablename, 0, &errflg);
      break;
    case 'C':
      (void) add_cache(optarg, tablename, 1, &errflg);
      break;
    case 'd':
      debug = 1;
      break;
    case 'f':
      FREE(ddfile);
      ddfile = STRDUP(optarg);
      break;
    case 'h':
      suppress_header = 1;
      break;
    case 'i': /* changed meaning : change default ODB_CACHE_LIMIT */
      odb_cache_limit = atoi(optarg);
      if (odb_cache_limit <= 0)            odb_cache_limit = ODB_CACHE_LIMIT;
      if (odb_cache_limit > ODB_CACHE_MAX) odb_cache_limit = ODB_CACHE_MAX;
      break;
    case 'l':
      FREE(dbname);
      dbname = STRDUP(optarg);
      break;
    case 'n':
      update = 0;
      break;
    case 'o':
      FREE(oprefix);
      oprefix = STRDUP(optarg);
      oprefix_percent_s = strequ(oprefix,"%s") ? 1 : 0;
      if (debug) fprintf(stderr,"--> oprefix = '%s'\n",oprefix ? oprefix : NIL);
      break;
    case 'p':
      poolno = atoi(optarg);
      jptot = poolno;
      break;
    case 't':
      FREE(tablename);
      tablename = STRDUP(optarg);
      break;
    case 'u':
      update = 1;
      break;
    case 'x':
      extract = 1;
      break;
    default:
      errflg++;
      break;
    }
  }

  if (!ddfile && dbname) {
    int len = strlen(dbname) + 4;
    ALLOC(ddfile,len);
    snprintf(ddfile,len,"%s.dd", dbname);
  }

  if (ddfile && !dbname) { /* ddfile = "/dir/DBNAME.dd" */
    char *p = strrchr(ddfile, '/');
    dbname = p ? STRDUP(p+1) : STRDUP(ddfile); /* dbname now is "DBNAME.dd" */
    p = strchr(dbname,'.');
    if (p) *p = '\0';
  }

  if (!tablename || !ddfile || !dbname) errflg++;

  if (errflg) {
    fprintf(stderr,USAGE,argv[0],ODB_CACHE_LIMIT,BAILOUT);
    return errflg;
  }

  ncols_ref = read_ddfile(ddfile, tablename, &npools);

  /* fprintf(stderr,"optind=%d, argc=%d\n",optind,argc); */

  if (extract) {
    colcache_t *pc = cache;
    while (pc) {
      pc->unpack = 1;
      pc = pc->next;
    }
    suppress_header = 1;
    update = 0;
    binary = 0;
  }

  if (!suppress_header) {
    if (binary) {
      unsigned int magic_word = DCA2;
      short int nbuf[2] = { NIBUF, NDBUF };
      fwrite(&magic_word, sizeof(magic_word), 1, stdout);
      fwrite(nbuf, sizeof(*nbuf), 2, stdout);
    }
    else {
      printf("#DCA2: col# colname dtname dtnum file pool#"
             " offset length pmethod pmethod_actual"
             " nrows nmdis avg min max compression_ratio cardinality is_little\n");
    }
  }

  for (jarg=optind; jarg<argc; jarg++) {
    unsigned long long int seekpos = 0;
    int nread;
    unsigned int first_word, word;
    unsigned int hc32_ctrlw[6];
    unsigned int odb1_ctrlw[6];
    unsigned int odb1_colhdr[2];
    int grpsize = 0;
    int jp;
    int nrows = 0;
    int ncols = 0;
    int nbytes = 0;
    int pmethod = 0;
    int pmethod_actual = 0;
    int big_endian = 1;
    int little_endian = 1;
    int is_hcat = 0;
    int is_odbfile = 0;
    int byteswap_needed = 0;
    const char *mode = "r";
    FILE *fp = NULL;
    const char *file = argv[jarg];
    int unit, iret, rdlen;

    if (debug) fprintf(stderr,"Opening ODB data file '%s'\n",argv[jarg]);
    /* fp = fopen(argv[jarg],"r"); */
    cma_open_(&unit, file, mode, &iret, strlen(file), strlen(mode));
    fp = CMA_get_fp(&unit);
    if (iret != 1 || !fp) {
      fprintf(stderr,"Error: Cannot open ODB data file '%s'\n",file);
      rc++;
      continue;
    } 

    /* First word */
    word = 0;
    /* nread = fread(&word, sizeof(word), 1, fp); */
    rdlen = sizeof(word);
    cma_readb_(&unit, (byte1 *)&word, &rdlen, &iret);
    nread = iret/sizeof(word);

    if (debug) fprintf(stderr,"... [%llu:%d] 1st word = %u\n", seekpos, nread, word);
    if (nread != 1) {
      fprintf(stderr,"Error: Unable to read the 1st word\n");
      rc++;
      goto finish;
    }

    first_word = word;
    seekpos += nread * sizeof(word);

    /* Detect big/little endianity and 
       whether file is a normal ODB-file (I/O-method=1) or 
       concatenated ODB-file (I/O-method=4) */

    is_hcat         = (word ==  HC32 || word == _23CH);

    if (i_am_little) {
      big_endian    = (word == _23CH || word == _BDO);
      little_endian = (word ==  HC32 || word == ODB_);
    }
    else {
      big_endian    = (word ==  HC32 || word == ODB_);
      little_endian = (word == _23CH || word == _BDO);
    }

    is_odbfile = (big_endian || little_endian);

    if (!is_odbfile) {
      fprintf(stderr,"Error: File is not a recognized ODB-file\n");
      rc++;
      goto finish;
    }

    if (debug) fprintf(stderr,"... File is %s-endian, %s ODB-file\n",
                       big_endian ? "big" : "little",
                       is_hcat ? "concatenated (hcat)" : "normal");

    /* The initial "guess" for the need of byte swapping */
    byteswap_needed = ((i_am_little && big_endian) || (!i_am_little && little_endian));

    for (jp=1; jp<=npools; jp++) {
      int byteswap_needed_local = byteswap_needed;
      int j, nelem;
      if (debug && (!is_hcat || jp > 1)) 
        fprintf(stderr,"... Processing pool-chunk#%d of %d\n",jp,npools);
      if (is_hcat) {
        /* Read control word information before ODB1-word */
        nelem = sizeof(hc32_ctrlw)/sizeof(*hc32_ctrlw);
        /* nread = fread(hc32_ctrlw, sizeof(*hc32_ctrlw), nelem, fp); */
        rdlen = sizeof(hc32_ctrlw);
        cma_readb_(&unit, (byte1 *)hc32_ctrlw, &rdlen, &iret);
        nread = (iret >= 0) ? iret/sizeof(*hc32_ctrlw) : 0;
        if (nread == 0 && (iret == -1 || feof(fp))) goto finish;
        if (nread != nelem) {
          fprintf(stderr,
		  "Error: Unexpected problems to read HC32 control-words at pool-chunk#%d: nread=%d, expected=%d\n",
                  jp,nread,nelem);
          rc++;
          goto finish;
        }

        if (byteswap_needed_local) swap4bytes_(hc32_ctrlw, &nelem);
        /* poolno = hc32_ctrlw[0]; */
        grpsize = hc32_ctrlw[1];
        poolno = (grpsize < npools) ? (jptot-1)%npools + 1 : hc32_ctrlw[0]; /* ??? */
        nbytes = hc32_ctrlw[2];
        nrows = hc32_ctrlw[3];
        ncols = hc32_ctrlw[4];
        if (debug && jp == 1) fprintf(stderr,"... Processing pool-chunk#%d of %d/%d\n",jp,npools,grpsize);
        if (debug) fprintf(stderr,
                           "... [%llu:%d] poolno=%d, jptot=%d, grpsize=%d, npools=%d,"
                           " nbytes=%d, nrows=%d, ncols=%d\n",
                           seekpos,nread,poolno,jptot,grpsize,npools,nbytes,nrows,ncols);
        if (nbytes == 0) {
           goto finish;
        } 
        seekpos += sizeof(hc32_ctrlw);
        jptot++;

        /* Read ODB1-word */
        word = 0;
        /* nread = fread(&word, sizeof(word), 1, fp); */
        rdlen = sizeof(word);
        cma_readb_(&unit, (byte *)&word, &rdlen, &iret);
        nread = iret/sizeof(word);
        if (nread != 1) {
          fprintf(stderr,"Error: Unable to read the ODB1-word\n");
          rc++;
          goto finish;
        }
        seekpos += sizeof(word);
      }
      else
        word = first_word;

      if (word == _BDO) word = ODB_; /* bytes swapped inline */

      if (word != ODB_) {
        fprintf(stderr,
                "Error: Corrupted ODB1-word: was=%u, expected=%u (i.e. %u)\n",
                word,first_word,ODB_);
        rc++;
        goto finish;
      }

      /* Read control information just after ODB1-word */
      nelem = sizeof(odb1_ctrlw)/sizeof(*odb1_ctrlw);
      /* nread = fread(odb1_ctrlw, sizeof(*odb1_ctrlw), nelem, fp); */
      rdlen = sizeof(odb1_ctrlw);
      cma_readb_(&unit, (byte1 *)odb1_ctrlw, &rdlen, &iret);
      nread = iret/sizeof(*odb1_ctrlw);
      if (nread != nelem) {
        fprintf(stderr,
                "Error: Unexpected problems to read ODB1 control-words"
                " at pool-chunk#%d: nread=%d, expected=%d\n",
                jp,nread,nelem);
        rc++;
        goto finish;
      }

      /* 
         Maybe byteswap is not actually applicable !!
         Scenario: the first pool has been updated last time on the little-endian machine,
	 but the remaining pools are still from big-endian, but treated as little-endian
	 ==> a big problem
         We need to fix the obvious problem associated with this scenario !!
      */
        
      ncols = odb1_ctrlw[1];
      byteswap_needed_local = (ncols != ncols_ref) ? 1 : 0;

      if (byteswap_needed_local) swap4bytes_(odb1_ctrlw, &nelem);
      nrows = odb1_ctrlw[0];
      ncols = odb1_ctrlw[1];
      if (debug) fprintf(stderr,"..... [%llu:%d] poolno=%d, npools=%d, nrows=%d, ncols=%d\n",
                         seekpos,nread,poolno,npools,nrows,ncols);
      seekpos += sizeof(odb1_ctrlw);

      if (ncols != ncols_ref) {
        fprintf(stderr,
                "Error: Invalid number of columns: ncols=%d, expected=%d\n",ncols,ncols_ref);
        rc++;
        goto finish;
      }

      if (nrows > 0) {
      /* Loop over column header immediately before data */
      for (j=0; j<ncols; j++) {
        void *data = NULL;
        void *data_alloc = NULL;
        int swp = byteswap_needed_local;
        uint dtnum = DATATYPE_UNDEF;
        uint dtnum_saved = dtnum;
        int unpack = 0;
	colcache_t *pc = in_cache(colname[j], NULL, &unpack);
        int update_local = (update || unpack) ? 1 : 0;
        int seekinc = 0;
        int can_swp_data = 0;
        int nmdis = 0;
        double avg = 0;
        double minvalue = 1;
        double maxvalue = -1;
        int ncard = 0;
        int jump_over;
        int search_dtnum = 0;
        int is_little = 0;
        double upksize = ((double) nrows) * get_dtsize(datatype[j]); /* Unpacked size */
        double pksize = upksize; /* The default packed size == unpacked size */
        double cr = 1; /* The default compression ratio = upksize:pksize */
        nelem = sizeof(odb1_colhdr)/sizeof(*odb1_colhdr);
        /* nread = fread(odb1_colhdr, sizeof(*odb1_colhdr), nelem, fp); */
        rdlen = sizeof(odb1_colhdr);
        cma_readb_(&unit, (byte1 *)odb1_colhdr, &rdlen, &iret);
        nread = iret/sizeof(*odb1_colhdr);
        if (nread != nelem) {
          fprintf(stderr,
                  "Error: Unexpected problems to read ODB1 column header-words"
                  " at pool-chunk#%d, col#%d: nread=%d, expected=%d\n",
                  jp,j,nread,nelem);
          rc++;
          goto finish;
        }
        if (byteswap_needed_local) swap4bytes_(odb1_colhdr, &nelem);
        if (i_am_little) { /* the running machine is little endian */
          is_little = byteswap_needed_local ? 0 : 1;
        }
        else { /* the running machine is big endian */
          is_little = byteswap_needed_local ? 1 : 0;
        }
        pksize = nbytes = odb1_colhdr[0];
        cr = (upksize <= 0 || pksize <= 0) ? 0 : (upksize/pksize);
        pmethod = pmethod_actual = EXTRACT_PMETHOD(odb1_colhdr[1]);
        dtnum = EXTRACT_DATATYPE(odb1_colhdr[1]);
        if (dtnum == DATATYPE_UNDEF) {
          dtnum = get_dtnum(datatype[j]);
          search_dtnum = 1;
        }
        dtnum_saved = dtnum;
        odb1_colhdr[1] = pmethod + 256U * dtnum;
        can_swp_data = EXTRACT_SWAPPABLE(odb1_colhdr[1]);
        seekinc = nbytes;
        if (nbytes == 0) pc = NULL;
        if (debug) {
          fprintf(stderr,
                  "....... [%llu:%d] col#%d <%s> nbytes=%d, pmethod=%d, dtnum=%u (0x%x)%s,"
                  " can_swp_data=%d, cacheable-pc=%d\n",
                  seekpos,nread,j,colname[j],nbytes,pmethod,dtnum,dtnum,
                  search_dtnum ? " searched" : "",
                  can_swp_data, pc ? 1 : 0);
        }
        seekpos += sizeof(odb1_colhdr);

        jump_over = 
          (nbytes == 0) ||
          (!update_local) || 
          (dtnum == DATATYPE_UNDEF);

        if (jump_over) {
          /* Jump over the column data section */
          if (pc) {
            rc += write_cache(pc, j, unit, NULL, nbytes, &nread, jp, __LINE__);
          }
          else {
            /* nread = fseek(fp, nbytes, SEEK_CUR); */
            const int whence = 1; /* SEEK_CUR */
            cma_seekb_(&unit, &nbytes, &whence, &nread);
          }
        }
        else {
          if (pmethod == 0) { /* data just aligned into the 'double'-boundary;
                                 data may still be other than a 'double'-type */
            double *dp;
            int ndpw = RNDUP_DIV(nbytes,sizeof(*dp)); /* Round-up & truncate */
            ALLOC(dp, ndpw);
            data = data_alloc = dp;
            if (pc) {
              rc += write_cache(pc, j, unit, data, nbytes, &nread, jp, __LINE__);
            }
            else {
              /* nread = fread(data, 1, nbytes, fp); */
              cma_readb_(&unit, (byte1 *)data, &nbytes, &nread);
              nread = (nread == nbytes) ? 0 : -1;
            }
            if (swp) swp = can_swp_data;
          }
          else {
            const int fill_zeroth_cma = 0; /* set this to 1 if in doubt */
            unsigned int hdr[PCMA_HDRLEN];
            int bytes_in, bytes_out;
            int lencma, msgbytes, new_version;
            double nmdi, rmdi;
            int swpaux;
            int rc_hdr = upcma_hdr(fp, &swpaux, hdr, 1,
                                   &pmethod_actual, &lencma, &msgbytes,
                                   &nmdi, &rmdi, &new_version);
            int numval;
            double *cma = NULL;
            Packbuf pbuf;

            if (rc_hdr >= 0) ALLOC(cma, lencma);

	    if (pc && unpack && pmethod_actual == 9) {
	      /* Allow a special case for pmethod actually being 9 
		 i.e. all values the same ==>
		 retain packing, since unpacking on-the-fly (from cache-file)
		 is very cheap */
	      unpack = 0;
	    }

            if (pc && !unpack) {
              pbuf.counter = 0;
              pbuf.maxalloc = 0;
              pbuf.len = 0;
              pbuf.p = NULL;
              pbuf.allocatable = 0;
            }

            numval = upcma(can_swp_data, fp, NULL, 
                           NULL, 0, fill_zeroth_cma,
                           cma, lencma, hdr, 
                           (pc && !unpack) ? &pbuf : NULL,
                           &bytes_in, &bytes_out);

            if (pc && !unpack && pbuf.p) {
              rc += write_cache(pc, j, -1, pbuf.p, nbytes, NULL, jp, __LINE__);
              FREE(pbuf.p);
            }

            if (pc && unpack) {
              is_little = i_am_little;
              pmethod_actual = 0;
            }
            else {
              /* This individual column can still be little endian, despite file being big endian */
              /* Luckily swpaux gives us this status information */
              if (i_am_little) { /* the running machine is little endian */
                is_little = swpaux ? 0 : 1;
              }
              else { /* the running machine is big endian */
                is_little = swpaux ? 1 : 0;
              }
            }
            if (debug) {
              fprintf(stderr,
                      "\tnumval=%d,lencma=%d,pmethod/act=%d/%d,msgbytes=%d,"
                      "mdis=(%.20g,%.20g),vers=%d,in/out=%d/%d,swpaux=%d,is_little=%d\n",
                      numval,lencma,pmethod,pmethod_actual,msgbytes,
                      nmdi,rmdi,new_version,bytes_in,bytes_out,
                      swpaux,is_little);
              if (nrows >= 2) {
                fprintf(stderr,"\t(1)=%.20g\t(nrows=%d)=%.20g\n",
                        cma[1],nrows,cma[nrows]);
              }
            }
            if (numval != lencma) {
              fprintf(stderr,
                      "Error: upcma()-error; numval=%d, lencma=%d, nrows=%d\n",
                      numval,lencma,nrows);
              nread = -1;
            }
            else {
              data_alloc = cma;
              data = &cma[fill_zeroth_cma];
              nread = 0;
              if (pc && unpack) {
                rc += write_cache2(pc, j, data, nrows, dtnum_saved, &seekinc, jp, __LINE__);
              }
            }
            swp = 0; /* byte swap already performed in upcma => switch off */
            /* force datatype to be REAL8, unless already a string */
            if (dtnum != DATATYPE_STRING) dtnum = DATATYPE_REAL8;
          }
        }

        if (nread != 0) {
          fprintf(stderr,
                  "Error: Unexpected problems to get over column data"
                  " at pool-chunk#%d, col#%d: retcode=%d\n",
                  jp,j,nread);
          rc++;
          goto finish;
        }

        nmdis = 0;
        avg = 0;
        minvalue = 1;
        maxvalue = -1;
        ncard = 0;

        if (data) {
          update_items(data, nrows, dtnum,
                       swp, mdi,
                       &nmdis, 
                       &avg, &minvalue, &maxvalue, &ncard);
          if (debug) {
            fprintf(stderr,
                    "\tnrows=%d, no. of MDIs=%d, avg=%.20g, min=%.20g, max=%.20g, cardinality=%d\n",
                    nrows, nmdis, avg, minvalue, maxvalue, ncard);
          }
          FREE(data_alloc);
        }

        if (binary) {
          int ibuf[NIBUF];
          double dbuf[NDBUF];

          ibuf[0] = poolno; /* Poolno first and upon reading so we can quickly skip this record */

          ibuf[1] = strlen(colname[j]) /* + 1 + strlen(tablename) */;
          ibuf[2] = strlen(datatype[j]);
          ibuf[3] = pc ? strlen(pc->fc->cache_file) : strlen(argv[jarg]);

          ibuf[4] = dtnum_saved;
          ibuf[5] = j;
          ibuf[6] = pc ? seekinc : nbytes;
          ibuf[7] = pmethod;
          ibuf[8] = pmethod_actual;
          ibuf[9] = nrows;
          ibuf[10] = nmdis;
          ibuf[11] = cr;
          ibuf[12] = ncard;
          ibuf[13] = is_little;

          dbuf[0] = pc ? pc->fc->cache_seekpos : seekpos;
          dbuf[1] = avg;
          dbuf[2] = minvalue;
          dbuf[3] = maxvalue;

          fwrite(ibuf, sizeof(*ibuf), NIBUF, stdout);
          fwrite(dbuf, sizeof(*dbuf), NDBUF, stdout);
          fwrite(colname[j], 1, strlen(colname[j]), stdout);
          /*
	    fwrite("@", 1, 1, stdout);
	    fwrite(tablename, 1, strlen(tablename), stdout);
          */
          fwrite(datatype[j], 1, strlen(datatype[j]), stdout);
          if (pc) {
            fwrite(pc->fc->cache_file, 1, strlen(pc->fc->cache_file), stdout);
          }
          else {
            fwrite(argv[jarg], 1, strlen(argv[jarg]), stdout);
          }
        }
        else if (!extract) {
	  /* printf("%d %s@%s %s %u %s %d %llu %d %d %d %d %d %.20g %.20g %.20g %.4f %d %d\n", */
          printf("%d %s %s %u %s %d %llu %d %d %d %d %d %.20g %.20g %.20g %.4f %d %d\n",
		  j, colname[j],
		        datatype[j], dtnum_saved, 
		              pc ? pc->fc->cache_file : argv[jarg],
		                 poolno,
		                    pc ? pc->fc->cache_seekpos : seekpos, 
		                         pc ? seekinc : nbytes,
		                            pmethod, pmethod_actual,
		                                  nrows, nmdis,
		                                        avg,minvalue,maxvalue,
		                                                          cr,  ncard,
		                                                                  is_little);
        }
	else if (extract && pc) {
	  printf("%s %d %d %d %d %.20g %.20g\n",
		 colname[j], poolno, nrows, nmdis, ncard, minvalue, maxvalue);
	}

        seekpos += nbytes;
        if (pc) pc->fc->cache_seekpos += seekinc;
      } /* for (j=1; j<=ncols; j++) */
     }
      if (!is_hcat) break;
    } /* for (jp=1; jp<=npools; jp++) */
  finish:
    /* fclose(fp); */
    cma_close_(&unit, &iret);
    if (!is_hcat) poolno++;
  } /* for (jarg=optind; jarg<=argc; jarg++) */

  if (rc == 0 && cache) { /* close still opened cache-files */
    colcache_t *pc = cache;
    while (pc) {
      (void) close_cache(pc);
      if (pc->fc && pc->fc_alloc_here) {
	(void) free_filecache(pc->fc);
	pc->fc_alloc_here = 0;
      }
      pc->fc = NULL;
      pc = pc->next;
    }
  }

  if (debug || rc != 0) fprintf(stderr,"Exiting with return code = %d\n",rc);
  return rc;
}

