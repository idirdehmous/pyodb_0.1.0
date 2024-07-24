
/* New I/O library for ODB (coding started : 15-Oct-2002) */

#include "newio.h"
#include "swapbytes.h"
#include "dca.h"
#include "cdrhook.h"

extern char *IOtruename(const char *name, const int *len_str);
#ifdef RS6K
extern void xl__trbk_();
#else
#define xl__trbk_() 
#endif

#define PERMS 0644 /* R/W for owner, R for group & others */

#define WORDLEN sizeof(ll_t)
#define DIMLEN  (3*WORDLEN)

PRIVATE int first_time = 1;
PRIVATE int io_write_empty_files = 0;
PRIVATE int io_profile = 0;
PRIVATE int io_lock = 0;

/* 
   io_method:
   1 = use cma_readb/cma_writeb as usual (the default)
   2 = use read/write directly  -- 1) lacks I/O-buffering 2) not tested really well
   3 = use QTAR-facility (via pipe i.e. popen()/pclose()) -- 1) slow 2) not tested well
   4 = assume data has been written into memory (horizontal concatenation) -- is working
   5 = Access to individual columns via DCA-files

   For method#4, you may need to understand the coding in ../lib/msgpass_loaddata.F90 and ../lib/msgpass_storedata.F90, too.
 */

PRIVATE int io_method = 1;

PRIVATE int io_bufsize = IO_BUFSIZE_DEFAULT;
PRIVATE int io_verbose = 0;
PRIVATE int io_keep_incore = 1;
#if !defined(IO_FILESIZE_DEFAULT)
#define IO_FILESIZE_DEFAULT 32
#endif
PRIVATE int io_filesize = IO_FILESIZE_DEFAULT; /* in megabytes */
PRIVATE int io_grpsize = -1;
PRIVATE char *consider_tables = NULL;
PRIVATE int newio_myproc = 0;
PRIVATE int newio_nproc = 1;

PRIVATE int db_io_size = 0; /* How many handles allowed */
PRIVATE IO_db_t *db_io = NULL;

PRIVATE const int badnum = 2147483647;

#define DB_IO_DEF(handle, poolno) \
  IO_db_t *pdb = ((handle) >= 1) ? &db_io[(handle)-1] : NULL; \
  int jpool = (pdb && (poolno) >= 1) ? pdb->poolaccess[(poolno)] : badnum; \
  IO_pool_t *ppool = (pdb && jpool != badnum) ? &pdb->pool[jpool] : NULL

#define Free_INCORE(ptbl, keep_incore) \
{ if (ptbl) { \
    if (!keep_incore) { \
      FREE(ptbl->incore); \
      ptbl->n_incore = -1; ptbl->n_alloc = 0; \
      ptbl->incore_ptr = 0; ptbl->in_use = 0; \
    } \
    ptbl->updated = 0; \
  } \
}

#define Alloc_INCORE(ptbl, nbytes, roundup) \
{ if (ptbl) { \
    Free_INCORE(ptbl, 0); \
    ptbl->n_incore = nbytes; \
    ptbl->n_alloc = RNDUP(DIMLEN + ptbl->n_incore, roundup); \
    ALLOC(ptbl->incore, ptbl->n_alloc); \
    ptbl->incore_ptr = DIMLEN; \
    ptbl->in_use = 1; \
  } \
}

#define	Take_INCORE(ptbl) \
  (ptbl && (ptbl->tblname && ptbl->in_use && ptbl->updated &&  \
 ((ptbl->tblname && ptbl->incore && ptbl->n_incore > 0) || \
  (ptbl->tblname && ptbl->n_incore == 0 && io_write_empty_files))))

#define NEWIO_OPEN_UNDEF  0
#define NEWIO_OPEN_READ   1
#define NEWIO_OPEN_WRITE  2
#define NEWIO_OPEN_DELETE 4

static o_lock_t NEWIO_mylock = 0; /* A specific OMP-lock; initialized only once in
				     odb/lib/codb.c, routine codb_init_omp_locks_() */

PRIVATE char *version_env = NULL; 
PRIVATE char *version_sw = NULL; 

PRIVATE void
init_IOs()
{
  DRHOOK_START(init_IOs);
  if (first_time) { /* 1st */
    coml_set_lockid_(&NEWIO_mylock);
    if (first_time) { /* 2nd */
      char *env = NULL;
      /* Get ODB_VERSION is effect */
      env = getenv("ODB_VERSION");
      if (env) version_env = STRDUP(env);
      {
	/* Get ODB_VERSION from function call codb_versions_() */
	const char *vers_ptr = codb_versions_(NULL,NULL,NULL,NULL);
	version_sw = STRDUP(vers_ptr);
      }
      /* Get myproc */
      codb_procdata_(&newio_myproc, &newio_nproc, NULL, NULL, NULL);
      /* I/O locking to be used ? */
      env = getenv("ODB_IO_LOCK");
      if (env) io_lock = atoi(env);
      /* Perform I/O profiling of newio's */
      env = getenv("ODB_IO_PROFILE");
      if (env) io_profile = atoi(env);
      /* Change I/O method */
      env = getenv("ODB_IO_METHOD");
      if (env) io_method = atoi(env);
      if (io_method < 1 || io_method > 5) io_method = 1;
      /* Write "empty" file i.e. table files w/o any rows */
      env = getenv("ODB_WRITE_EMPTY_FILES");
      if (env) io_write_empty_files = atoi(env);
      /* I/O buffer size */
      env = getenv("ODB_IO_BUFSIZE");
      if (env) io_bufsize = atoi(env);
      if (io_bufsize <= 0) io_bufsize = IO_BUFSIZE_DEFAULT;
      /* Little more verbose on ODB_IO_VERBOSE processor ? */
      env = getenv("ODB_IO_VERBOSE");
      if (env) io_verbose = atoi(env);
      if (io_verbose < 0) io_verbose = -1;
      if (io_verbose == -1) io_verbose = newio_myproc;
      if (io_verbose == newio_myproc) {
	/* Turn on CMA I/O dbg, too */
	const int toggle = 1;
	int old_value;
	cma_debug_(&toggle, &old_value);
      }
      /* Keep I/O data incore as long as possible (up to end_IO_struct) */
      env = getenv("ODB_IO_KEEP_INCORE");
      if (env) io_keep_incore = atoi(env);
      /*== NOTE == Make sure ODB_IO_FILESIZE & ODB_IO_GRPSIZE consistent with
	../lib/msgpass_loaddata.F90 and ../lib/msgpass_storedata.F90*/
      /* Display preferred filesize (used in horizontal concatenation scheme) */
      env = getenv("ODB_IO_FILESIZE");
      if (env) io_filesize = atoi(env);
      if (io_filesize <= 0) io_filesize = IO_FILESIZE_DEFAULT; /* in megabytes */
      /* Display maximum I/O group size i.e. how many pools' data will be concatenated into one file */
      env = getenv("ODB_IO_GRPSIZE");
      if (!env) env = getenv("NPES_AN");
      if (env) io_grpsize = atoi(env);
      if (io_grpsize <= 0) io_grpsize = newio_nproc;
      /* Consider loading of these tables only, by default ; (used in horizontal concat) */
      env = getenv("ODB_CONSIDER_TABLES");
      if (env) consider_tables = STRDUP(env);
      else     consider_tables = STRDUP("*"); /* i.e. all */
      
      if (io_profile) Profile_newio32_init(newio_myproc); 

      if (newio_myproc == 1) {
	FILE *fp  ; 
	fp = fopen("pyodb.stdout"  ,  "w");
	fprintf(fp,"*** %s:init_IOs() ***\n",__FILE__);
	fprintf(fp,"\tODB_WRITE_EMPTY_FILES=%d\n",io_write_empty_files);
	fprintf(fp,"\t  ODB_CONSIDER_TABLES=%s\n",consider_tables);
	fprintf(fp,"\t   ODB_IO_KEEP_INCORE=%d\n",io_keep_incore);
	fprintf(fp,"\t      ODB_IO_FILESIZE=%d MB\n",io_filesize);
	fprintf(fp,"\t       ODB_IO_BUFSIZE=%d bytes\n",io_bufsize);
	fprintf(fp,"\t       ODB_IO_GRPSIZE=%d (or max no. of pools)\n",io_grpsize);
	fprintf(fp,"\t       ODB_IO_PROFILE=%d\n",io_profile);
	fprintf(fp,"\t       ODB_IO_VERBOSE=%d\n",io_verbose);
	fprintf(fp,"\t        ODB_IO_METHOD=%d\n",io_method);
	if (version_env) {
	  fprintf(fp,"\t          ODB_VERSION=%s (environment variable)\n",version_env);
	}
	if (version_sw) {
	  fprintf(fp,"\t          ODB_VERSION=%s (software release)\n",version_sw);
	}
	fprintf(fp,"\t          ODB_IO_LOCK=%d\n",io_lock);
	fprintf(fp,"*********************\n");
      }
      first_time = 0;
    } /* if (first_time) 2nd */
    coml_unset_lockid_(&NEWIO_mylock);
  } /* if (first_time) 1st */
  DRHOOK_END(0);
}

PUBLIC void init_NEWIO_lock()
{
  INIT_LOCKID_WITH_NAME(&NEWIO_mylock,"newio.c:NEWIO_mylock");
  init_IOs();
}

PRIVATE void
lock_IO(int onoff)
{
  if (io_lock) {
    if (io_profile) {
      if (onoff) Profile_newio32_start(ioprof_iolock, 0);
    }
    ODB_iolock(onoff);
    if (io_profile) {
      if (!onoff) Profile_newio32_end(ioprof_iolock, 0);
    }
  }
}

PRIVATE void
TableDump(FILE *fp, const IO_pool_t *ppool, const IO_tbl_t *ptbl_specific)
{
  if (fp) {
    int poolno = ppool->poolno;
    int ntables = ppool->ntables;
    int maxtables = ppool->maxtables;
    int jtbl;
    fprintf(fp,
	    "***Pool#%d, active table count %d of %d; file=%s, exist=%d,\n\tlast_write_cmd=%s\n",
	    poolno, ntables, maxtables, 
	    ppool->file ? ppool->file : NIL,
	    ppool->file_exist,
	    ppool->last_write_cmd ? ppool->last_write_cmd : NIL);
    for (jtbl=0; jtbl<ntables; jtbl++) {
      IO_tbl_t *ptbl = &ppool->tbl[jtbl];
      if (ptbl_specific && ptbl != ptbl_specific) continue;
      if (ptbl->tblname) {
	fprintf(fp,
"[%d]: '%s', ksiz=%d, upd=%d, mode=%d, f=%s, incore=%p, n=%d, alloc=%d, ptr=%d, used=%d,\n\tread=%s,\n\tdel=%s\n",
		jtbl, ptbl->tblname, ptbl->known_size, ptbl->updated, ptbl->open_mode, 
		ptbl->file ? ptbl->file : NIL, 
		ptbl->incore, ptbl->n_incore, ptbl->n_alloc, ptbl->incore_ptr, ptbl->in_use,
		ptbl->read_cmd ? ptbl->read_cmd : NIL,
		ptbl->delete_cmd ? ptbl->delete_cmd : NIL
		);
      }
    } /* for (jtbl=0; jtbl<ntables; jtbl++) */
  } /* if (fp) */
}

PRIVATE int
LocateTable(const char *table, IO_pool_t *ppool, int create_new_entry)
{
  int found = 0;
  int rc = -1;
  int maxtables = ppool->maxtables;
  int ntables = ppool->ntables;
  int jtbl;
  DRHOOK_START(LocateTable);

/*printf("LocateTable \n");*/
  coml_set_lockid_(&NEWIO_mylock);
  for (jtbl=0; jtbl<ntables; jtbl++) {
    IO_tbl_t *ptbl = &ppool->tbl[jtbl];
/*    printf("LocateTable %s \n" , table); */
    if (ptbl->tblname && strequ(ptbl->tblname,table)) {
      rc = jtbl;
      found = 1;
      break;
    }
  } /* for (jtbl=0; jtbl<ntables; jtbl++) */

  if (!found && create_new_entry) {
    for (jtbl=0; jtbl<maxtables; jtbl++) {
      IO_tbl_t *ptbl = &ppool->tbl[jtbl];
      if (!ptbl->tblname) {
	IO_db_t *pdb = ppool->mydb;
	ptbl->tblname = STRDUP(table);
	if (pdb->is_new) ptbl->known_size = 0;
	ppool->ntables++;
	rc = jtbl;
	found = 1;
	break;
      }
    } /* for (jtbl=0; jtbl<maxtables; jtbl++) */
  } /* if (!found) */
  coml_unset_lockid_(&NEWIO_mylock);

  DRHOOK_END(0);
  /* if (!found) --> error [when create_new_entry]; should be impossible, though */
  return rc;
}

PRIVATE int
FileExist(const char *filename)
{
  int exist = 0;
  struct stat buf;
  if (stat(filename,&buf) == 0) {
    /* Caveat: We do not check if this file is a directory or some
       other special file. Perhaps we should */
    exist = 1;
  }
  return exist;
}

PRIVATE char *
GetQTARFILE(int handle, int poolno, IO_pool_t *ppool, int *exist)
{
  char *qtarfile = ppool->file;
  if (!qtarfile) {
    IO_db_t *pdb = ppool->mydb;
    char *file = NULL;
    int len;
    MakeFileName(file, pdb->dbname, "QTARFILE", poolno);
    len = strlen(file);
    ppool->file = qtarfile = IOtruename(file, &len);
    FREEX(file);
    ppool->file_exist = pdb->is_new ? 0 : FileExist(qtarfile);
  }
  if (exist) *exist = ppool->file_exist;
  /* if (exist) *exist = FileExist(qtarfile); */
  return qtarfile;
}


/* Version for 32-bit (4 byte) integer lengths i.e. one slurp "limited" to 2GB */


PRIVATE void 
newio_Error32(const char *what, 
	      const char *filename,
	      const char *desc,
	      const char *entry,
	      int handle,
	      int poolno,
	      int bytes, 
	      int rc,
	      int io_method_used,
	      const char *format,
	      ...) 
{
  DRHOOK_START(newio_Error32);
  if (io_profile) Profile_newio32_flush();
  if (io_method_used == 1 || 
      io_method_used == 2) perror(filename);
  fprintf(stderr,
  "***Error: Unable to %s file=%s, desc=%s, entry=%s, handle=%d, poolno=%d: %d bytes, I/O-method=%d : rc=%d\n",
  what, filename, desc, entry, handle, poolno, bytes, io_method_used, rc); 
  if (format) {
    int len = strlen(format);
    va_list args;
    va_start(args, format);
    vfprintf(stderr, format, args);
    if (format[len-1] != '\n') fprintf(stderr,"\n");
    va_end(args);
  }
  if (handle > 0 && poolno > 0) {
    DB_IO_DEF(handle, poolno);
    TableDump(stderr, ppool, NULL);
  }
  RAISE(SIGABRT);
  DRHOOK_END(0);
}


PUBLIC int
newio_Size32(const char *filename,
	     const char *dbname,
	     const char *table,
	     int handle,
	     int poolno)
{
  DB_IO_DEF(handle, poolno);
  int size = 0;
  int this_io_method = pdb ? pdb->io_method : 0;
  DRHOOK_START(newio_Size32);

  if (this_io_method == 0) size = -2;

  if (size == 0) {
    if (this_io_method == 3 ||
	this_io_method == 4) { /* QTAR or horizontal concat */
      int jtbl = LocateTable(table, ppool, 0);
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	size = ptbl->known_size;
      } /* if (jtbl >= 0) */
    }
    else {
      codb_filesize_(filename, &size, strlen(filename));
    }
  }

  DRHOOK_END(0);
  return size;
}


PRIVATE int
ReadData32(int fd, void *data, int bytes)
{
  int len, n;
  char *c = data;
  int rem = bytes;
  int rc = 0;
  DRHOOK_START(ReadData32);
  for (;;) {
    len = MIN(io_bufsize, rem);
    if (len <= 0) break;
    n = read(fd, c, len);
    if (n > 0) {
      c += n;
      rem -= n;
      rc += n;
    }
    else /* Error (the stmt "rc != bytes" below will catch it) */
      break;
  }
  DRHOOK_END(rc);
  return rc;
} 


PUBLIC int
newio_GetByteswap(int *fp_idx, int handle, int poolno)
{
  DB_IO_DEF(handle, poolno);
  int swp = pdb ? pdb->req_byteswap : 0;
  return swp;
}

PUBLIC int
newio_SetByteswap(int *fp_idx, int toggle, int handle, int poolno)
{
  DB_IO_DEF(handle, poolno);
  int oldswp = pdb ? pdb->req_byteswap : 0;
  if (pdb) {
    int this_io_method = pdb->io_method;
    if (this_io_method == 1) {
      int oldvalue;
      cma_set_byteswap_(fp_idx, &toggle, &oldvalue);
    }
    pdb->req_byteswap = toggle;
  }
  return oldswp;
}

PUBLIC int
newio_Read32(int *fp_idx, 
	     const char *filename, 
	     const char *desc, 
	     const char *entry,
	     int handle,
	     int poolno,
	     void *data, 
	     int sizeof_data, 
	     int n)
{
  DB_IO_DEF(handle, poolno);
  int rc = 0;
  int bytes = sizeof_data * n;
  int this_io_method = pdb ? pdb->io_method : 0;
  int req_byteswap = pdb ? pdb->req_byteswap : -1;
  DRHOOK_START(newio_Read32);

  if (this_io_method == 0) goto finish;

  if ( bytes > 0 ) {
    if (io_profile) Profile_newio32_start(ioprof_read, bytes);
    if (this_io_method == 3 ||
	this_io_method == 4) { /* QTAR or horizontal concat */
      int jtbl = *fp_idx;
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	if (ptbl->open_mode == NEWIO_OPEN_READ) {
	  /* "read" data from incore-array */
	  rc = bytes;
	  if (!ptbl->incore) {
	    rc = 0;
	  }
	  else if (ptbl->n_alloc - ptbl->incore_ptr < bytes) {
	    rc = -ABS(ptbl->n_alloc - ptbl->incore_ptr);
	  }
	  else { /* All okay */
	    memcpy(data, &ptbl->incore[ptbl->incore_ptr], bytes);
	    ptbl->incore_ptr += bytes;
	    /* Note: Do NOT update n_incore nor known_size, since they've been handled okay elsewhere */
	  }
	}
      } /* if (jtbl >= 0) */
    }      
    else if (this_io_method == 2) {
      rc = ReadData32(*fp_idx, data, bytes);
    }
    else {
      cma_readb_(fp_idx, data, &bytes, &rc);
      if (rc == -2) { /* Error : Invalid internal file unit : indication of too small MAXCMAIO-value ? */
	int maxcmaio = CMA_get_MAXCMAIO();
	newio_Error32("newio_Read32: cma_readb_() failed possibly due to too small MAXCMAIO-value",
		      filename, desc, entry, handle, poolno, bytes, rc, this_io_method,
		      "newio_Read32: sizeof_data=%d, n=%d, *fp_idx=%d, data=%p, req_byteswap=%d.\n"
		      "newio_Read32: Suggesting to increase MAXCMAIO via 'export MAXCMAIO=%d' to %d before re-running",
		      sizeof_data, n, *fp_idx, data, req_byteswap, maxcmaio, 10*maxcmaio);
      }
    }
    if (io_profile) Profile_newio32_end(ioprof_read, rc);
  }

 finish:
  if (rc != bytes) { 
    newio_Error32("newio_Read32", 
		  filename, desc, entry, handle, poolno, bytes, rc, this_io_method, 
		  "newio_Read32: sizeof_data=%d, n=%d, *fp_idx=%d, data=%p, req_byteswap=%d",
		  sizeof_data, n, *fp_idx, data, req_byteswap); 
  }

  DRHOOK_END(bytes);
  return bytes;
}


PRIVATE int
WriteData32(int fd, const void *data, int bytes)
{
  int len, n;
  const char *c = data;
  int rem = bytes;
  int rc = 0;
  DRHOOK_START(WriteData32);
  for (;;) {
    len = MIN(io_bufsize, rem);
    if (len <= 0) break;
    n = write(fd, c, len);
    if (n > 0) {
      c += n;
      rem -= n;
      rc += n;
    }
    else /* Error (the stmt "rc != bytes" below will catch it) */
      break;
  }
  DRHOOK_END(rc);
  return rc;
} 


PUBLIC int
newio_Write32(int *fp_idx, 
	      const char *filename, 
	      const char *desc, 
	      const char *entry,
	      int handle,
	      int poolno,
	      const void *data, 
	      int sizeof_data, 
	      int n)
{
  DB_IO_DEF(handle, poolno);
  int rc = 0;
  int bytes = sizeof_data * n;
  int this_io_method = pdb ? pdb->io_method : 0;
  int req_byteswap = pdb ? pdb->req_byteswap : -1;
  DRHOOK_START(newio_Write32);

  if (this_io_method == 0) goto finish;

  if (bytes > 0 && !pdb->is_readonly) {
    if (io_profile) Profile_newio32_start(ioprof_write, bytes);
    if (this_io_method == 3 ||
	this_io_method == 4) { /* QTAR or horizontal concat */
      int jtbl = *fp_idx;
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	if (ptbl->open_mode == NEWIO_OPEN_WRITE) {
	  /* cache data to incore-array */
	  rc = bytes;
	  if (!ptbl->incore) {
	    Alloc_INCORE(ptbl, bytes, io_bufsize);
	  }
	  else if (ptbl->n_alloc - ptbl->incore_ptr < bytes) { /* allocate more */
	    if (io_verbose == newio_myproc) {
	      fprintf(stderr,
		      "\tReallocation of %d bytes (n_alloc=%d, incore_ptr=%d, n_incore=%d, incore=%p)\n", 
		      bytes, ptbl->n_alloc, ptbl->incore_ptr, ptbl->n_incore, ptbl->incore);
	      TableDump(stderr, ptbl->mypool, ptbl);
	    }
            ptbl->n_alloc = RNDUP(ptbl->n_alloc + bytes, io_bufsize);
	    REALLOC(ptbl->incore, ptbl->n_alloc);
	  }
	  memcpy(&ptbl->incore[ptbl->incore_ptr], data, bytes);
	  ptbl->incore_ptr += bytes;
	  ptbl->known_size = ptbl->n_incore = ptbl->incore_ptr - DIMLEN;
	  ptbl->updated = 1;
	}
      } /* if (jtbl >= 0) */
    }      
    else if (this_io_method == 2) {
      rc = WriteData32(*fp_idx, data, bytes);
    }
    else {
      cma_writeb_(fp_idx, data, &bytes, &rc);
    }
    if (io_profile) Profile_newio32_end(ioprof_write, rc);
  }

 finish:
  if (rc != bytes) { 
    newio_Error32("newio_Write32", 
		  filename, desc, entry, handle, poolno, bytes, rc, this_io_method,
		  "newio_Write32: sizeof_data=%d, n=%d, *fp_idx=%d, data=%p, req_byteswap=%d",
		  sizeof_data, n, *fp_idx, data, req_byteswap); 
  }

  DRHOOK_END(bytes);
  return bytes;
}


PUBLIC int
newio_Close32(int *fp_idx, 
	      const char *filename, 
	      const char *dbname,
	      const char *table,
	      const char *desc,
	      const char *entry,
	      int handle,
	      int poolno,
	      unsigned int info[],
	      int infolen
	      )
{
  DB_IO_DEF(handle, poolno);
  int rc = 0;
  int nrows = (infolen >= 2) ? info[1] : -1;
  int ncols = (infolen >= 3) ? info[2] : -1;
  int this_io_method = pdb ? pdb->io_method : 0;
  DRHOOK_START(newio_Close32);

  if (this_io_method == 0) goto finish;

  if (io_profile) Profile_newio32_start(ioprof_close, 0);

  if (this_io_method == 3 ||
      this_io_method == 4) { /* QTAR or horizontal concat */
    int jtbl = *fp_idx;
    if (jtbl >= 0) {
      IO_tbl_t *ptbl = &ppool->tbl[jtbl];
      if (io_verbose == newio_myproc) {
	fprintf(stderr,
		"newio_Close32(%s=%d,%s='%s',%s='%s',%s='%s',\n\t%s='%s',%s='%s',%s=%d,%s=%d)\n",
		"*fp_idx", *fp_idx,
		"filename", filename,
		"dbname", dbname,
		"table", table,
		"desc", desc,
		"entry", entry,
		"handle", handle,
		"poolno", poolno);
      }
      if (ptbl->incore) ptbl->incore_ptr = DIMLEN;
      ptbl->open_mode = NEWIO_OPEN_UNDEF;
      if (nrows != -1) ptbl->nrows = nrows;
      if (ncols != -1) ptbl->ncols = ncols;
    }
  }
  else if (this_io_method == 2) {
    int fd = *fp_idx;
    close(fd);
  }
  else {
    cma_close_(fp_idx, &rc);
  }
  if (io_profile) Profile_newio32_end(ioprof_close, rc);
  if (io_profile) Profile_newio32_flush();
  if (io_lock) lock_IO(0);  /* Release I/O-lock */
  if (rc != 0) { 
    newio_Error32("newio_Close32", 
		  filename, desc, entry, handle, poolno, 0, rc, this_io_method, NULL); 
  }

 finish:
  *fp_idx = -1;
  DRHOOK_END(0);
  return rc;
}


PUBLIC int
newio_Open32(int *fp_idx, 
	     const char *filename, 
	     const char *dbname,
	     const char *table,
	     const char *desc,
	     const char *entry,
	     int handle,
	     int poolno,
	     const char *mode,
	     int close_too,
	     int lookup_only,
	     unsigned int info[],
	     int infolen)
{
  DB_IO_DEF(handle, poolno);
  int rc = 0;
  int Nbytes = 0;
  int nrows = 0;
  int ncols = 0;
  int reading = (*mode == 'r') ? 1 : 0;
  int writing = (reading == 0);
  int this_io_method = pdb ? pdb->io_method : 0;
  DRHOOK_START(newio_Open32);

  *fp_idx = -1;
  if (this_io_method == 0) goto finish;

  if (!pdb->in_use && !pdb->is_new && this_io_method == 3) {
    /* Get information of member files in QTAR via table of contents */
    int exist = 0;
    char *qtarfile = GetQTARFILE(handle, poolno, ppool, &exist);

    if (exist) { /* Retrieve table of contents from QTAR */
      FILE *fp;
      char *toc_cmd = NULL;
      int len = strlen(qtarfile) + 20;
      ALLOC(toc_cmd, len);
      sprintf(toc_cmd, "qtar -s -t -M -f %s", qtarfile);
      
      if (io_verbose == newio_myproc) {
	fprintf(stderr,"Executing: popen(%s, r)\n",toc_cmd);
	xl__trbk_();
      }

      fp = popen(toc_cmd, "r");
      if (fp) {
	char memfile[8192];
	int j, nmem = 0;
	int nitems = 0;
	nitems = fscanf(fp, "%d\n", &nmem);
	if (nitems == 1) {
	  for (j=0; j<nmem; j++) {
	    ll_t nbytes, offset;
	    int nrows, ncols;
	    nitems = fscanf(fp,"%s %lld %lld %d %d\n",memfile,&nbytes,&offset,&nrows,&ncols);
	    if (nitems == 5) {
	      int jtbl = LocateTable(memfile, ppool, 1);
	      IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	      ptbl->known_size = (int)nbytes;
	      ptbl->offset = offset;
	      ptbl->nrows = nrows;
	      ptbl->ncols = ncols;
	      ptbl->updated = 0; /* for security */
	      ptbl->in_use = 0; /* for security */
	    }
	    else {
	      newio_Error32("newio_Open32: Cannot retrieve member file information from QTAR's TOC-output",
			    filename, desc, entry, handle, poolno, j+1, nmem,
			    this_io_method,
			    "newio_Open32: QTAR-cmd='%s'",toc_cmd);
	    }
	  } /* for (j=0; j<nmem; j++) */
	}
	else {
	  newio_Error32("newio_Open32: Cannot retrieve no. of members from QTAR's TOC-output",
			filename, desc, entry, handle, poolno, 1, nitems,
			this_io_method,
			"newio_Open32: QTAR-cmd='%s'",toc_cmd);
	}
	pclose(fp);
      }
      else {
	newio_Error32("newio_Open32: Cannot retrieve TOC-information. QTAR corrupted ?",
		      filename, desc, entry, handle, poolno, 0, 0,
		      this_io_method,
		      "newio_Open32: QTAR-cmd='%s'",toc_cmd);
      }
      FREE(toc_cmd);
    }
    else {
      fprintf(stderr,
	      "***Warning: Could not locate QTAR='%s' for dbname='%s', handle=%d, poolno=%d\n",
	      qtarfile, dbname, handle, poolno);
    }
  }
  pdb->in_use = 1;

  if (writing) {
    /* Get data amount indicator ; triggers whether to open/write file at all */
    if (infolen >= 2) nrows = info[1];
/*    printf("newioc.c nrows = %d\n", nrows); */
  }

  if (reading || (writing && (nrows > 0 || io_write_empty_files))) {
    int perror_onoff = -1;
    const int on = 1;
    if (reading) { /* temporarely disable perror() messages */
      const int off = 0;
      cma_set_perror_(&off, &perror_onoff); 
    }
    if (io_lock) lock_IO(1); /* Acquire I/O-lock */
    if (io_profile) Profile_newio32_start(ioprof_open, 0);
    if (this_io_method == 4) { /* horizontal concat */
      int open_new_table = (writing && (nrows > 0 || io_write_empty_files)) ? 1 : 0;
      int jtbl = LocateTable(table, ppool, open_new_table);
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	*fp_idx = jtbl;
	if (reading) {
	  ptbl->open_mode = NEWIO_OPEN_READ;
	}
	else if (writing) {
	  /* We do almost nothing ; just record the *fp_idx and fake rc to 1 */
	  if (!pdb->is_readonly) {
	    ptbl->open_mode = NEWIO_OPEN_WRITE;
	  }
	  else {
	    ptbl->open_mode = NEWIO_OPEN_UNDEF;
	  }
	}
	if (!ptbl->incore) Alloc_INCORE(ptbl, 0, WORDLEN);
	ptbl->incore_ptr = DIMLEN;
	if (reading && ptbl->n_incore == 0) {
	  /* No data loaded/available ==> corresponds to empty file situation */
	  rc = -1;
	}
	else {
	  rc = 1;
	}
      }
      else if (reading) {
	rc = -1; /* Means: data not supplied from I/O-level i.e. 
		    must be an "empty file" or not considered to load (<-- too bad) */
      } /* if (jtbl >= 0) ... else if (reading) ... */
    }
    else if (this_io_method == 3) { /* QTAR */
      int exist = 0;
      int is_incore_already = 0;
      char *qtarfile = GetQTARFILE(handle, poolno, ppool, &exist);
      int jtbl = LocateTable(table, ppool, 1);
      IO_tbl_t *ptbl = &ppool->tbl[jtbl];
      *fp_idx = jtbl;

      if (io_verbose == newio_myproc) {
	fprintf(stderr,
"newio_Open32(%s=%d,%s='%s',%s='%s',%s='%s',\n\t%s='%s',%s='%s',\n\t%s=%d,%s=%d,%s='%s',%s=%d,%s=%p,%s=%d)\n",
		"*fp_idx", *fp_idx,
		"filename", filename,
		"dbname", dbname,
		"table", table,
		"desc", desc,
		"entry", entry,
		"handle", handle,
		"poolno", poolno,
		"mode", mode,
		"close_too", close_too,
		"info[]-addr", info,
		"infolen", infolen);
      }

      if (reading && !ptbl->read_cmd) {
	int len = strlen(table) + strlen(qtarfile) + 50;
	ALLOC(ptbl->read_cmd, len);
	sprintf(ptbl->read_cmd,"qtar -s -x -M -b %d -f %s -m %s", io_bufsize, qtarfile, table);
      }

      /* check whether incore already and potentially save from re-reading */
      if (reading) {
	if (ptbl->n_incore >= 0 || ptbl->known_size == 0) is_incore_already = 1;
	if (is_incore_already && !ptbl->incore) Alloc_INCORE(ptbl, 0, WORDLEN);
      }

      if (reading) {
	FILE *fp = NULL;
	if (!is_incore_already) Free_INCORE(ptbl, 0);
	if (exist && !is_incore_already) {
	  if (io_verbose == newio_myproc) {
	    fprintf(stderr,"Executing: popen(%s, r)\n",ptbl->read_cmd);
	    xl__trbk_();
	  }
	  fp = popen(ptbl->read_cmd, "r");
	}
	if (fp) {
	  ll_t dim[DIMLEN/WORDLEN];
	  int fd = fileno(fp);
	  int nread = ReadData32(fd, dim, DIMLEN);
	  if (nread == DIMLEN) {
	    int bytes;
	    ll_t nbytes;
	    int nrows, ncols;
	    nbytes = dim[0];
	    nrows = dim[1];
	    ncols = dim[2];
	    bytes = (int)nbytes;
	    if (bytes > 0) {
	      Alloc_INCORE(ptbl, bytes, WORDLEN);
	      nread = ReadData32(fd, ptbl->incore + DIMLEN, ptbl->n_incore);
	      if (nread != ptbl->n_incore) {
		newio_Error32("newio_Open32: Unable to ReadData32()", 
			      filename, desc, entry, handle, poolno, ptbl->n_incore, nread, 
			      this_io_method,
			      "newio_Open32: QTAR-cmd='%s'",ptbl->read_cmd);
	      }
	      ptbl->incore_ptr = DIMLEN;
	      ptbl->known_size = ptbl->n_incore;
	      ptbl->nrows = nrows;
	      ptbl->ncols = ncols;
	      rc = 1;
	    }
	    else {
	      rc = -1; /* File-member not found or length info indicates that its size is zero */
	      Alloc_INCORE(ptbl, 0, WORDLEN);
	    } /* if (bytes > 0) */
	    ptbl->open_mode = NEWIO_OPEN_READ;
	    pclose(fp);
	  }
	  else {
	    newio_Error32("newio_Open32: Invalid length info from ReadData32()",
			  filename, desc, entry, handle, poolno, DIMLEN, nread,
			  this_io_method,
			  "newio_Open32: QTAR-cmd='%s'",ptbl->read_cmd);
	  } /* if (nread == DIMLEN) */
	}
	else if (is_incore_already) {
	  ptbl->open_mode = NEWIO_OPEN_READ;
	  if (ptbl->n_incore > 0) {
	    rc = 1; /* Data is already in ptbl->incore[] */
	  }
	  else { /* i.e. ptbl->n_incore == 0 */
	    rc = -1; /* Data file wasn't there when first time retrieved */
	  }
	}
	else { /* i.e. NOT exist */
	  Alloc_INCORE(ptbl, 0, WORDLEN);
	  ptbl->open_mode = NEWIO_OPEN_UNDEF;
	  rc = -1; /* QTARFILE not present ==> File-member not found either */
	} /* if (fp) */
      }
      else if (writing) {
	/* We do almost nothing ; just record the *fp_idx; 
	   the newio_flush32_() will do the I/O, when called */
	if (!pdb->is_readonly) {
	  ptbl->open_mode = NEWIO_OPEN_WRITE;
	}
	else {
	  ptbl->open_mode = NEWIO_OPEN_UNDEF;
	}
      }
      if (ptbl->incore) ptbl->incore_ptr = DIMLEN;
    }
    else if (this_io_method == 2) {
      int len = strlen(filename);
      char *truename = IOtruename(filename, &len);
      int fd = -1;
      if (reading) {
	fd = open(truename, O_RDONLY, 0); 
	/* We don't care about open()-errors in reading ==> we assume empty file situation */
	if (fd < 0) rc = -1;
      }
      else if (writing) {
	(void) IOmkdir(truename); /* Makes sure the underlaying directory really exists */
	fd = open(truename, O_WRONLY | O_CREAT | O_TRUNC, PERMS);
	/* Abort immediately */
	if (fd < 0) {
	  newio_Error32("newio_Open32: open() for writing failed", 
			filename, desc, entry, handle, poolno, 0, rc, 
			this_io_method,
			NULL);
	}
      }
      *fp_idx = fd;
      FREE(truename);
    }
    else {
      cma_open_(fp_idx, filename, mode, &rc, strlen(filename), strlen(mode));
      cma_get_byteswap_(fp_idx, &pdb->req_byteswap);
    }

    if (io_profile) Profile_newio32_end(ioprof_open, rc);
    if (perror_onoff == on) { /* switch perror() output back on */
      cma_set_perror_(&on, &perror_onoff); 
    }
  }
  else if (writing && nrows == 0) {
    if (io_lock) lock_IO(1); /* Acquire I/O-lock */
    if (io_profile) Profile_newio32_start(ioprof_rmfile, 0);
    if (this_io_method == 4) { /* horizontal concat */
/* AF begin */
      int open_new_table = (writing && (nrows >= 0 || io_write_empty_files)) ? 1 : 0;
      int jtbl = LocateTable(table, ppool, open_new_table);
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	*fp_idx = jtbl;
      }
      jtbl = *fp_idx;
/* END AF */
 /*     int jtbl = *fp_idx;
*/
/*printf("AF write & nrows = 0 %d\n", jtbl);*/
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	Alloc_INCORE(ptbl, 0, WORDLEN);
	if (!pdb->is_readonly) ptbl->updated = 1;
	ptbl->open_mode = NEWIO_OPEN_DELETE;
      } /* if (jtbl >= 0) */
    }
    else if (this_io_method == 3) { /* QTAR */
      int exist = 0;
      char *qtarfile = GetQTARFILE(handle, poolno, ppool, &exist);
      int jtbl = *fp_idx;
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	if (!pdb->is_readonly && ptbl->known_size > 0) {
	  if (exist) {
	    int status;
	    if (!ptbl->delete_cmd) {
	      int len = strlen(table) + strlen(qtarfile) + 50;
	      ALLOC(ptbl->delete_cmd, len);
	      sprintf(ptbl->delete_cmd,"qtar -s -d -M -b %d -f %s -m %s", 
		      io_bufsize, qtarfile, table);
	    }
	    if (io_verbose == newio_myproc) {
	      fprintf(stderr,"Executing: system(%s)\n",ptbl->delete_cmd);
	      xl__trbk_();
	    }
	    status = system(ptbl->delete_cmd);
	    if (status != 0) {
	      newio_Error32("newio_Open32: Error in deleting member-file via system()",
			    filename, desc, entry, handle, poolno, 0, 0,
			    this_io_method,
			    "newio_Open32: system() QTAR-cmd='%s'",ptbl->delete_cmd);
	    }
	    ppool->file_exist = 1;
	    ptbl->known_size = -1;
	  } /* if (exist) */
	} /* if (!pdb->is_readonly && ptbl->known_size > 0) */
	ptbl->open_mode = NEWIO_OPEN_DELETE;
      } /* if (jtbl >= 0) */
    }
    else {
      remove_file_(filename); /* Make sure the existing file is removed */
    }
    if (io_profile) Profile_newio32_end(ioprof_rmfile, 0);
    if (io_lock) lock_IO(0);  /* Release I/O-lock */
  }

  if (reading && rc == -1) { /* File not found for reading */
    int date_now = 0, time_now = 0;
    /* #1: Magic number */                    if (infolen >= 1) info[0] = ODB_;
    nrows = 0;
    /* #2: Number of rows */                  if (infolen >= 2) info[1] = nrows;
    ncols = 0; /* undefined ==> fill it later */
    /* #3: Number of cols */                  if (infolen >= 3) info[2] = ncols; 
    if (infolen >= 5) codb_datetime_(&date_now, &time_now);
    /* #4: Creation date (YYYYMMDD) */        if (infolen >= 4) info[3] = date_now;
    /* #5: Creation time (HHMMSS) */          if (infolen >= 5) info[4] = time_now;
    /* #6: Modification date (YYYYMMDD) */    if (infolen >= 6) info[5] = date_now;
    /* #7: Modification time (HHMMSS) */      if (infolen >= 7) info[6] = time_now;
    close_too = 0;
  }
  else if (reading) {
    int swp = 0;
    Nbytes += newio_Read32(fp_idx, filename, desc, entry, handle, poolno, 
			   info, sizeof(*info), infolen);
    if (infolen >= 1) {
      if (info[0] == _BDO) {
	int oldswp;
	swp = 1;
	oldswp = newio_SetByteswap(fp_idx, swp, handle, poolno);
	swap4bytes_(info, &infolen);
      }
    }
    if (infolen >= 2) nrows = info[1];
    if (infolen >= 3) ncols = info[2];
    if (swp) swap4bytes_(info, &infolen); /* need to swap back, otherwise logic fails */
  }
  else if (writing && (nrows > 0 || io_write_empty_files)) {
    /* write to a non-empty file */
/*printf("AF write to a non-empty file \n"); */
    Nbytes += newio_Write32(fp_idx, filename, desc, entry, handle, poolno, 
			    info, sizeof(*info), infolen);
    if (infolen >= 2) nrows = info[1];
    if (infolen >= 3) ncols = info[2];
  }
  else {
    close_too = 0;
  }

  if (this_io_method == 4) { /* horizontal concat */
    int jtbl = *fp_idx;
    if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	ptbl->nrows = nrows;
	ptbl->ncols = ncols;
    }
  }

  if (close_too) { newio_Close32(fp_idx, filename, dbname, table, desc, 
				 entry, handle, poolno, info, infolen); }

 finish:
  DRHOOK_END(0);
  return Nbytes;
}

/* --- New internal I/O structures --- */

PRIVATE int
init_IO_struct(int maxhandle)
{
  DRHOOK_START(init_IO_struct);
  if (!db_io) {
    coml_set_lockid_(&NEWIO_mylock);
    if (!db_io) {
      IO_db_t *tmp_db_io = NULL;
      int jdb;
      db_io_size = MAX(1,maxhandle);
      ALLOC(tmp_db_io, db_io_size);
      for (jdb=0; jdb<db_io_size; jdb++) {
	IO_db_t *pdb = &tmp_db_io[jdb];
	pdb->dbname = NULL;
	pdb->in_use =  0;
	pdb->handle = -1;
	pdb->is_new = -1;
	pdb->is_readonly = -1;
	pdb->req_byteswap = 0;
	pdb->io_method = io_method; /* i.e. via ODB_IO_METHOD */
	pdb->npools = 0;
	pdb->pool = NULL;
	pdb->maxpools = 0;
	pdb->poolaccess = NULL;
      }
      db_io = tmp_db_io;
    } /* if (!db_io) */
    coml_unset_lockid_(&NEWIO_mylock);
  }
  DRHOOK_END(0);
  return db_io_size;
}


PRIVATE int
end_IO_struct(int handle)
{
  int jdb, jpool; 
  IO_db_t *pdb = NULL;
  int maxsize = 0;
  int npools;
  DRHOOK_START(end_IO_struct);
  
  maxsize = init_IO_struct(handle);

  if (handle < 1 || handle > maxsize) { /* Out of range */
    newio_Error32("end_IO_struct: Handle out of range",
		  NULL, NULL, NULL,
		  handle, -1, 0, 0,
		  -1, NULL);
  }

  jdb = handle-1;
  pdb = &db_io[jdb];
  FREE(pdb->dbname);
  pdb->in_use =  0;
  pdb->handle = -1;
  pdb->is_new = -1;
  pdb->is_readonly = -1;
  pdb->req_byteswap = 0;
  pdb->io_method = io_method;
  npools = pdb->npools;

  for (jpool=0; jpool<npools; jpool++) {
    int rc;
    IO_pool_t *ppool = &pdb->pool[jpool];
    int poolno = ppool->poolno;
    newio_release_pool32_(&handle, &poolno, &rc);
  } /* for (jpool=0; jpool<npools; jpool++) */

  pdb->npools = 0;
  FREE(pdb->pool);

  pdb->maxpools = 0;
  FREE(pdb->poolaccess);

  DRHOOK_END(0);
  return handle;
}


PRIVATE void
copy_IO_dbstr(IO_db_t *to, IO_db_t *from)
{
  DRHOOK_START(copy_IO_dbstr);
  if (to && from && to != from) {
    int npools = to->npools;
    int old_npools = from->npools;
    int jpool, np = MIN(old_npools, npools);

    FREE(to->dbname);
    to->dbname = STRDUP(from->dbname);
    FREE(from->dbname);

    to->in_use = from->in_use;
    to->handle = from->handle;
    to->is_readonly = from->is_readonly;
    to->req_byteswap = from->req_byteswap;
    to->is_new = from->is_new;
    to->io_method = from->io_method;
  
    for (jpool=0; jpool<np; jpool++) {
      IO_pool_t *ppool = &to->pool[jpool];
      IO_pool_t *old_ppool = &from->pool[jpool];
      int poolno = ppool->poolno;
      if (old_ppool) {
	FREE(ppool->tbl);
	memcpy(ppool, old_ppool, sizeof(*old_ppool)); /* Alright ? Copies ptrs etc. */
      }
      ppool->mydb = to;
      ppool->poolno = poolno;
      { /* Re-assign ptbl->mypool */
	int jtbl, maxtables = ppool->maxtables;
	for (jtbl=0; jtbl<maxtables; jtbl++) {
	  IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	  ptbl->mypool = ppool; /* Used to point to old_ppool */
	}
      }
    } /* for (jpool=0; jpool<np; jpool++) */
  } /* if (to && from && to != from) */
  DRHOOK_END(0);
}


PRIVATE void
init_IO_dbstr(IO_db_t *pdb,
	      const char *dbname, 
	      int handle, int maxhandle, int io_method_input,
	      int maxtables, int is_new, int is_readonly,
	      int maxpools, int npools, const int poolnos[])
{
  int jpool;
  DRHOOK_START(init_IO_dbstr);

  pdb->dbname = STRDUP(dbname);
  pdb->in_use = 0;
  pdb->handle = handle;
  pdb->is_readonly = is_readonly;
  pdb->req_byteswap = 0;
  pdb->is_new = is_new;
  /* pdb->io_method = (is_new) ? io_method : io_method_input; : this was a bug !! */
  pdb->io_method = io_method_input; /* This is how it should always be */
  pdb->npools = npools;
  ALLOC(pdb->pool, npools);
  pdb->maxpools = maxpools;
  CALLOC(pdb->poolaccess, maxpools+1);
  
  for (jpool=0; jpool<npools; jpool++) {
    int jtbl, poolno;
    IO_pool_t *ppool = &pdb->pool[jpool];
    ppool->mydb = pdb;
    poolno = ppool->poolno = poolnos[jpool];
    if (poolno >= 1 && poolno <= maxpools) {
      pdb->poolaccess[poolno] = jpool;
    }
    else {
      /* basically will trigger an error */
      pdb->poolaccess[poolno] = badnum;
    }
    ppool->ntables = 0;
    ppool->maxtables = maxtables;
    ALLOC(ppool->tbl, maxtables);
    for (jtbl=0; jtbl<maxtables; jtbl++) {
      IO_tbl_t *ptbl = &ppool->tbl[jtbl];
      ptbl->mypool = ppool;
      ptbl->tblname = NULL; /* initially */
      ptbl->known_size = -1;
      ptbl->updated = 0;
      ptbl->nrows = 0;
      ptbl->ncols = 0;
      /* incore items */
      ptbl->open_mode = NEWIO_OPEN_UNDEF; /* initially */
      ptbl->file = NULL; /* initially */
      ptbl->offset = 0; /* initially */
      ptbl->incore = NULL; /* initially */
      ptbl->n_incore = -1; /* initially */
      ptbl->n_alloc = 0; /* initially */
      ptbl->incore_ptr = 0; /* initially */
      ptbl->in_use = 0; /* initially */
      ptbl->read_cmd = NULL; /* initially */
      ptbl->delete_cmd = NULL; /* initially */
    } /* for (jtbl=0; jtbl<maxtables; jtbl++) */
    ppool->file = NULL; /* initially */
    ppool->file_exist = 0; /* initially */
    ppool->last_write_cmd = NULL; /* initially */
  } /* for (jpool=0; jpool<npools; jpool++) */
  DRHOOK_END(0);
}


PRIVATE IO_db_t *
copy_IO_struct(IO_db_t *curdb,
	       const char *dbname, 
	       int handle, int maxhandle, int io_method_input,
	       int maxtables, int is_new, int is_readonly,
	       int maxpools, int npools, const int poolnos[])
{
  IO_db_t *pdb = NULL;
  DRHOOK_START(copy_IO_struct);

  if (curdb) {
    int jdb; 
    int maxsize = 0;
    
    maxsize = init_IO_struct(maxhandle);
    
    if (handle < 1 || handle > maxsize) { /* Out of range */
      newio_Error32("copy_IO_struct: Handle out of range",
		    NULL, NULL, NULL,
		    handle, -1, 0, 0,
		    io_method_input,
		    "copy_IO_struct(dbname='%s')", dbname);
    }
    
    jdb = handle-1;
    pdb = &db_io[jdb];
    
    if (curdb == pdb && (curdb->npools < npools || 
			 curdb->maxpools < maxpools)) {

      if (newio_myproc == 1) {
	fprintf(stderr,
	"copy_IO_struct(handle=%d, dbname='%s'; npools=(%d => %d); maxpools=(%d => %d)\n", 
	handle, dbname, curdb->npools, npools, curdb->maxpools, maxpools);
      }

      /* Allocate new database structure */
      ALLOC(pdb, 1);
      pdb->dbname = NULL;
      pdb->in_use =  0;
      pdb->handle = -1;
      pdb->is_new = -1;
      pdb->is_readonly = -1;
      pdb->req_byteswap = 0;
      pdb->io_method = io_method; /* i.e. via ODB_IO_METHOD */
      pdb->npools = 0;
      pdb->pool = NULL;
      pdb->maxpools = 0;
      pdb->poolaccess = NULL;

      /* Firstly, fully initialize newly allocated structure */

      init_IO_dbstr(pdb,
		    dbname, 
		    handle, maxhandle, io_method_input,
		    maxtables, is_new, is_readonly,
		    maxpools, npools, poolnos);

      /* Now do copying */
      copy_IO_dbstr(pdb, curdb);

      /* Finally, copy data structure "back" 
	 (i.e. effectively replaces contents pointed by "curdb") */
      memcpy(&db_io[jdb], pdb, sizeof(*pdb));
      pdb = &db_io[jdb];
    }
    else { /* The best solution we can offer */
      pdb = (IO_db_t *)curdb;
    }
  }

  DRHOOK_END(0);
  return pdb;
}

			 
PRIVATE IO_db_t *
start_IO_struct(const char *dbname, 
		int handle, int maxhandle, int io_method_input,
		int maxtables, int is_new, int is_readonly,
		int maxpools, int npools, const int poolnos[])
{
  int jdb, jpool; 
  int maxsize = 0;
  IO_db_t *pdb = NULL;
  DRHOOK_START(start_IO_struct);

  maxsize = init_IO_struct(maxhandle);

  if (handle < 1 || handle > maxsize) { /* Out of range */
    newio_Error32("start_IO_struct: Handle out of range",
		  NULL, NULL, NULL,
		  handle, -1, 0, 0,
		  io_method_input,
		  "start_IO_struct(dbname='%s')", dbname);
  }

  jdb = handle-1;
  pdb = &db_io[jdb];

  if (!pdb->in_use) { 
    /* I/O-requests not started yet ==> re-initialization *is* allowed */
    (void) end_IO_struct(handle);

    init_IO_dbstr(pdb,
		  dbname, 
		  handle, maxhandle, io_method_input,
		  maxtables, is_new, is_readonly,
		  maxpools, npools, poolnos);
  }
  else { 
    /* I/O-requests have started => copy & extend data structures 
       This situation may occur due to ODB_addpools() on-the-fly */

    pdb = copy_IO_struct(pdb,
			 dbname, 
			 handle, maxhandle, io_method_input,
			 maxtables, is_new, is_readonly,
			 maxpools, npools, poolnos);
			 
  }

  DRHOOK_END(0);
  return pdb;
}

/*=== Fortran callables ===*/

PUBLIC void
newio_start32_(const char *dbname,
	       const int *handle, const int *maxhandle, const int *io_method_input,
               const int *maxtables, const int *is_new, const int *is_readonly,
               const int *glbNpools, const int *locNpools, const int poolidx[],
               int *rc
	       /* Hidden argument */
	       , int dbname_len)
{
  DECL_FTN_CHAR(dbname);
  DRHOOK_START(newio_start32_);

  ALLOC_FTN_CHAR(dbname);

  init_IOs();

  (void) start_IO_struct(p_dbname, 
			 *handle, *maxhandle, *io_method_input,
			 *maxtables, *is_new, *is_readonly,
			 *glbNpools, *locNpools, poolidx);

  FREE_FTN_CHAR(dbname);

  /* for now */
  DRHOOK_END(0);
  *rc = 0;
}

#define SKIP_RUBBISH(p) \
  { while (*p    && (*p == '/' || *p == '@' || *p == ' ')) p++; }
#define NEXT_RUBBISH(p) \
  { while (*p) { if (*p == '/' || *p == '@' || *p == ' ') { *p = '\0'; break; } else p++; } }

PRIVATE char **
TableList(const char table[], int Ntables_in, int *Ntables)
{
  char **t = NULL;
  int m = Ntables_in;
  int n = 0;
  DRHOOK_START(TableList);

  if (table && m > 0) {
    char *p = STRDUP(table);
    CALLOC(t, m);
    while (*p && n < m) {
      char *s, *saved;
      SKIP_RUBBISH(p);
      saved = s = STRDUP(p);
      NEXT_RUBBISH(s);
      SKIP_RUBBISH(s);
      t[n++] = STRDUP(saved);
      p = STRDUP(s+1);
      FREE(saved);
    }
    FREE(p);
  }

  if (Ntables) *Ntables = n;
  DRHOOK_END(0);
  return t;
}


PUBLIC void
newio_flush32_(const int *handle,
	       const int *poolno,
	       const int *enforce_to_disk,
	       const int *ntbl,
	       /* Tables in format "/@table1/@tables2/.../"
		  For all tables (currently in memory), use "*" or put *ntables to -1 */
	       const char table[],
	       int *rc
	       /* Hidden argument */
	       , int table_len)
{
  int retcode = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  DB_IO_DEF(Handle, Poolno);
  int this_io_method = pdb ? pdb->io_method : 0;
  int ef2d = *enforce_to_disk;

  if (this_io_method == 0) goto finish;

  if (this_io_method != 3) goto finish; /* Applies currently for QTAR-only */

  /* Use of this "enforcement" not implemented yet */
  if (*enforce_to_disk) {
    ef2d = 1;
  }
  else {
    if (io_keep_incore > 1) ef2d = 0;
  }

  if (pdb && pdb->in_use && this_io_method == 3) { /* QTAR-only */
    if (Poolno > 0) { 
      /* A specific pool (macro DB_IO_DEF sorted out some pointers) */
      int Ntables = *ntbl;
      int all_tables = (Ntables < 0 || (table_len > 0 && table && *table == '*'));

      if (all_tables) {
	int len = 0;
	int jtbl, ntables = ppool->ntables;
	int nt = 0;

	for (jtbl=0; jtbl<ntables; jtbl++) {
	  IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	  if (!pdb->is_readonly && Take_INCORE(ptbl)) {
	    len += strlen(ptbl->tblname) + 1;
	    nt++;
	  }
	  else {
	    Free_INCORE(ptbl, io_keep_incore);
	  }
	}

	if (len > 0) { /* There is indeed something to write */
	  char *pt = NULL;
	  len += 2;
	  ALLOC(pt, len);
	  strcpy(pt,"/");
	  for (jtbl=0; jtbl<ntables; jtbl++) {
	    IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	    if (Take_INCORE(ptbl)) {
	      strcat(pt,ptbl->tblname);
	      strcat(pt,"/");
	    }
	  }
	  /* Call recursively */
	  newio_flush32_(handle, &Poolno, enforce_to_disk, &nt, pt, &retcode, len);
	  FREE(pt);
	}
      }
      else { /* List of tables only */
	DECL_FTN_CHAR(table);
	int len = 0;
	int n, Ntables = *ntbl;
	int j, m = Ntables;
	char **t = NULL;
	int *jlist = NULL;

	ALLOC_FTN_CHAR(table);
	t = TableList(p_table, m, &Ntables);

	if (io_verbose == newio_myproc) {
	  fprintf(stderr,
"newio_flush32_(*handle=%d, *poolno=%d, *ef2d=%d, *ntbl=%d, table[]='%s', *rc=N/A, table_len=%d)\n",
		*handle, *poolno, ef2d, *ntbl, p_table, table_len);
	  xl__trbk_();
	  fprintf(stderr,"newio_flush32_(): p_table='%s', m=%d, Ntables=%d\n",p_table,m,Ntables);
	  for (j=0; j<Ntables; j++) {
	    fprintf(stderr,"<t[%d]='%s'\n", j, t[j] ? t[j] : NIL);
	  }
	}

	ALLOC(jlist, Ntables);
	n = Ntables;
	for (j=0; j<Ntables; j++) {
	  int jtbl = LocateTable(t[j], ppool, 0);
	  if (jtbl >= 0) jlist[j] = jtbl;
	  else n--;
	} /* for (j=0; j<Ntables; j++) */
	Ntables = n;

	if (io_verbose == newio_myproc) {
	  fprintf(stderr,"newio_flush32_(): p_table='%s', m=%d, Ntables=%d\n",p_table,m,Ntables);
	  for (j=0; j<Ntables; j++) {
	    int jtbl = jlist[j];
	    IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	    fprintf(stderr,">t[%d]='%s', data bytes=%d\n", j, ptbl->tblname, ptbl->n_incore);
	  }
	  TableDump(stderr, ppool, NULL);
	}

	for (j=0; j<Ntables; j++) {
	  int jtbl = jlist[j];
	  IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	  if (!pdb->is_readonly && Take_INCORE(ptbl)) {
	    len += strlen(ptbl->tblname) + 5;
	  }
	  else {
	    Free_INCORE(ptbl, io_keep_incore);
	  }
	} /* for (j=0; j<Ntables; j++) */

	if (len > 0) { /* There is indeed something to write */
	  char *qtarfile = GetQTARFILE(Handle, Poolno, ppool, NULL);
	  FILE *fp = NULL;
	  char *cmd = NULL;
	  len += strlen(qtarfile) + 50;
	  /* Make command */
	  ALLOC(cmd, len);
	  sprintf(cmd, "qtar -s -u -M -b %d -f %s", io_bufsize, qtarfile); 
	  for (j=0; j<Ntables; j++) {
	    int jtbl = jlist[j];
	    IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	    if (Take_INCORE(ptbl)) {
	      strcat(cmd, " -m ");
	      strcat(cmd, ptbl->tblname);
	    }
	  } /* for (j=0; j<Ntables; j++) */

	  /* Do write */

	  FREE(ppool->last_write_cmd);
	  ppool->last_write_cmd = STRDUP(cmd);

	  if (io_verbose == newio_myproc) {
	    fprintf(stderr,"Executing: popen(%s, w)\n",cmd);
	    xl__trbk_();
	  }

	  fp = popen(cmd, "w");
	  if (fp) {
	    int fd = fileno(fp);
	    for (j=0; j<Ntables; j++) {
	      int jtbl = jlist[j];
	      IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	      if (Take_INCORE(ptbl)) {
		ll_t dim[DIMLEN/WORDLEN];
		int nwrite;
		if (!ptbl->incore && ptbl->n_incore == 0) { /* write_empty_files -situation */
		  Alloc_INCORE(ptbl, 0, WORDLEN); /* Guarantees one WORDLEN-bytes allocation for "nbytes" */
		}
		dim[0] = ptbl->n_incore; /* no. of DATA bytes */
		dim[1] = ptbl->nrows; /* not up to date yet; to be done */
		dim[2] = ptbl->ncols; /* not up to date yet; to be done */
		memcpy(ptbl->incore, dim, DIMLEN);
		nwrite = WriteData32(fd, ptbl->incore, DIMLEN + ptbl->n_incore); /* All in one go */
		if (nwrite != DIMLEN + ptbl->n_incore) {
		  newio_Error32("newio_flush32_: Unable to write data",
				ptbl->tblname, "(length+data)", "(all)", 
				Handle, Poolno, DIMLEN + ptbl->n_incore, nwrite,
				this_io_method,
				"newio_flush32_: QTAR-cmd='%s'", cmd);
		}
		else {
		  ptbl->known_size = ptbl->n_incore;
		  Free_INCORE(ptbl, io_keep_incore);
		} /* if (nwrite != DIMLEN + ptbl->n_incore) */
	      } /* if (Take_INCORE(ptbl)) */
	    } /* for (j=0; j<Ntables; j++) */
	    pclose(fp);
	    ppool->file_exist = 1;
	  }
	  else {
	    newio_Error32("newio_flush32_: Unable open write-pipe",
			  qtarfile, "(QTAR)", "(write-pipe)", Handle, Poolno, 0, errno,
			  this_io_method,
			  "newio_flush32_: QTAR-cmd='%s'", cmd);
	  }
	  FREE(cmd);
	} /* if (len > 0) */

	FREE(jlist);
	for (j=0; j<m; j++) FREE(t[j]);
	FREE(t);
	FREE_FTN_CHAR(table);
      }
    }
    else {
      /* All pools (that contain something to flush) */
      int jpool, npools = pdb->npools;
      for (jpool=0; jpool<npools; jpool++) {
	ppool = &pdb->pool[jpool];
	Poolno = ppool->poolno;
	/* Call recursively (one pool at a time) to simplify coding */
	newio_flush32_(handle, &Poolno, enforce_to_disk, ntbl, table, &retcode, table_len);
      } /* for (jpool=0; jpool<npools; jpool++) */
    }
  }

 finish:
  *rc = retcode;
}


PUBLIC void
newio_end32_(const int *handle, int *rc)
{
  const char *all = "*";
  const int all_len = 1;
  const int Poolno = -1;
  const int Ntables = -1;
  const int Enforce_to_disk = 1; /* A must */
  int Retcode = 0;
  DRHOOK_START(newio_end32_);
  /* Finish all outstanding I/O (except for io_method==4) */
  newio_flush32_(handle, &Poolno, &Enforce_to_disk, &Ntables, all, &Retcode, all_len);
  (void) end_IO_struct(*handle);
  /* for now */
  DRHOOK_END(0);
  *rc = 0;
}


PUBLIC void
newio_get_incore32_(const int *handle,
		    const int *poolno, /* must be a specific pool ; -1 doesn't do */
		    char data[],       /* data area where incore data will be memcpy'ed to */
		    const int *nbytes, /* size of supplied data bytes array */
		    const int *reset_updated_flag, /* if == 1 --> reset updated-flag to 0 */
		    const char *table, /* particular table name (both "@table" or "table" do) */
		    int *rc
		    /* Hidden argument */
		    , int table_len)
{
  int retcode = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  DB_IO_DEF(Handle, Poolno);
  int this_io_method = pdb ? pdb->io_method : 0;
  DRHOOK_START(newio_get_incore32_);

  if (this_io_method == 0) goto finish;

  if (pdb->in_use && this_io_method == 4) { /* horizontal concat */
    if (Poolno > 0) { 
      /* A specific pool */
      int Nbytes = *nbytes;
      int jtbl;
      char *p;
      DECL_FTN_CHAR(table);

      ALLOC_FTN_CHAR(table);
      p = p_table;
      if (*p == '@') p++;
      
      jtbl = LocateTable(p, ppool, 0);
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	int size = ptbl->n_incore;
	if (ptbl->incore && Nbytes >= size) { /* ok : copy data to user-area */
	  memcpy(data, ptbl->incore + DIMLEN, size);
	  /* Data fed back to application or I/O-level ==> updated-flag reset */
	  if (*reset_updated_flag) ptbl->updated = 0;
	  retcode = size;
	}
	else { /* Data not copied ; could be zero-length, too */
	  retcode = -size;
	}
      } /* if (jtbl >= 0) */  

      FREE_FTN_CHAR(table);
    } /* if (Poolno > 0) */
  }

 finish:
  DRHOOK_END(retcode >= 0 ? retcode : 0);
  *rc = retcode;
}


PUBLIC void
newio_put_incore32_(const int *handle,
		    const int *poolno, /* must be a specific pool ; -1 doesn't do */
		    const char data[], /* data area from which data is copied into the incore */
		    const int *nbytes, /* amount of data */
		    const int *nrows,  /* no. of rows in data-matrix */
		    const int *ncols,  /* no. of cols in data-matrix */
		    const char *table, /* particular table name (both "@table" or "table" do) */
		    int *rc
		    /* Hidden argument */
		    , int table_len)
{
  int retcode = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  DB_IO_DEF(Handle, Poolno);
  int this_io_method = pdb ? pdb->io_method : 0;
  DRHOOK_START(newio_put_incore32_);

  if (io_verbose == newio_myproc) {
    fprintf(stderr,
	    "newio_put_incore32_(h=%d, p=%d, d[]=%p, nb=%d, nr=%d, nc=%d, tbl_len=%d)\n",
	    *handle, *poolno, data, *nbytes, *nrows, *ncols, table_len);
    fprintf(stderr,"Handle=%d, Poolno=%d, this_io_method=%d, pdb=%p\n",
	    Handle, Poolno, this_io_method, pdb);
  }

  if (this_io_method == 0) goto finish;

  if ((pdb->in_use || (!pdb->in_use && !pdb->is_new)) && 
      this_io_method == 4) { /* horizontal concat */
    if (Poolno > 0) { 
      /* A specific pool */
      int Nbytes = *nbytes;
      int jtbl;
      char *p;
      DECL_FTN_CHAR(table);

      ALLOC_FTN_CHAR(table);
      p = p_table;
      if (*p == '@') p++;
      
      jtbl = LocateTable(p, ppool, 1);
      if (io_verbose == newio_myproc) {
	fprintf(stderr, "--> For table='%s', jtbl=%d, Nbytes=%d\n", p, jtbl, Nbytes);
      }
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	if (Nbytes >= 0) { /* ok : copy data from user-area */
	  ll_t dim[DIMLEN/WORDLEN];
	  Alloc_INCORE(ptbl, Nbytes, WORDLEN);
	  dim[0] = ptbl->n_incore; /* no. of DATA bytes */
	  dim[1] = ptbl->nrows = *nrows;
	  dim[2] = ptbl->ncols = *ncols;
	  memcpy(ptbl->incore, dim, DIMLEN);
	  if (Nbytes > 0) memcpy(ptbl->incore + DIMLEN, data, Nbytes);
	  ptbl->known_size = Nbytes;
	  ptbl->updated = 0;
	  retcode = Nbytes;
	}
	else { /* Soft error */
	  Alloc_INCORE(ptbl, 0, WORDLEN);
	  retcode = -Nbytes;
	}
      } /* if (jtbl >= 0) */  
      if (io_verbose == newio_myproc) {
	fprintf(stderr, "<-- Out for table='%s', jtbl=%d, retcode=%d\n", p, jtbl, retcode);
      }

      FREE_FTN_CHAR(table);
      pdb->in_use = 1;
    } /* if (Poolno > 0) */
  }

 finish:
  DRHOOK_END(retcode >= 0 ? retcode : 0);
  *rc = retcode;
}


PUBLIC void /* Just in case we need to know this from Fortran */
newio_dimlen32_(int *rc)
{
  *rc = DIMLEN; /* no. of bytes in the header of the incore-array before data actually starts */
}


PUBLIC void
newio_status_incore32_(const int *handle,
		       const int *poolno,  /* must be a specific pool ; -1 doesn't do */
		             int status[], /* status[]-array */
		       const int *nstatus, /* no. of words in status[]-array */
		       const char *table, /* particular table name (both "@table" or "table" do) */
		             int *rc       /* no. of words filled/returned */
		       /* Hidden argument */
		       , int table_len)
{
  /* status[0] (fortran index=1) : no. of data bytes */
  /* status[1] (fortran index=2) : no. of rows */
  /* status[2] (fortran index=3) : no. of cols */
  /* status[3] (fortran index=4) : has been updated */

  int retcode = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  DB_IO_DEF(Handle, Poolno);
  int this_io_method = pdb ? pdb->io_method : 0;
  DRHOOK_START(newio_status_incore32_);

  if (this_io_method == 0) goto finish;

  if (pdb->in_use && this_io_method == 4) { /* horizontal concat */
    if (Poolno > 0) { 
      /* A specific pool */
      int Nstatus = *nstatus;
      int jtbl;
      char *p;
      DECL_FTN_CHAR(table);

      ALLOC_FTN_CHAR(table);
      p = p_table;
      if (*p == '@') p++;

      jtbl = LocateTable(p, ppool, 0);
      if (jtbl >= 0) {
	IO_tbl_t *ptbl = &ppool->tbl[jtbl];
	if (Nstatus >= 1) { status[0] = ptbl->n_incore; retcode++; }
	if (Nstatus >= 2) { status[1] = ptbl->nrows; retcode++; }
	if (Nstatus >= 3) { status[2] = ptbl->ncols; retcode++; }
	if (Nstatus >= 4) { status[3] = ptbl->updated; retcode++; }
      } /* if (jtbl >= 0) */  

      FREE_FTN_CHAR(table);
    } /* if (Poolno > 0) */
  }
  
 finish:
  DRHOOK_END(0);
  *rc = retcode;
}


PUBLIC void
newio_release_pool32_(const int *handle,
		      const int *poolno,  /* must be a specific pool ; -1 doesn't do */
		      int *rc)
{
  int retcode = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  DB_IO_DEF(Handle, Poolno);
  int this_io_method = pdb ? pdb->io_method : 0;
  DRHOOK_START(newio_release_pool32_);

  if (this_io_method == 0) goto finish;

  if (ppool) {
    int jtbl;
    int maxtables = ppool->maxtables;
    for (jtbl=0; jtbl<maxtables; jtbl++) {
      IO_tbl_t *ptbl = &ppool->tbl[jtbl];
      FREE(ptbl->tblname);
      ptbl->known_size = -1;
      /* incore items */
      ptbl->open_mode = NEWIO_OPEN_UNDEF;
      FREE(ptbl->file);
      Free_INCORE(ptbl, 0);
      FREE(ptbl->read_cmd);
      FREE(ptbl->delete_cmd);
      ptbl->mypool = NULL;
    } /* for (jtbl=0; jtbl<maxtables; jtbl++) */
    FREE(ppool->tbl);
    ppool->maxtables = 0;
    ppool->ntables = 0;
    ppool->poolno = 0;
    FREE(ppool->file);
    ppool->file_exist = 0;
    FREE(ppool->last_write_cmd);
    ppool->mydb = NULL;
  }
    
 finish:
  DRHOOK_END(0);
  *rc = retcode;
}
