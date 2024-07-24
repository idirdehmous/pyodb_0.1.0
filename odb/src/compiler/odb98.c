#include "defs.h"
#include "magicwords.h"
#include "cdrhook.h"

PRIVATE const char vers[] = "v2.12-Feb-2012";
PRIVATE const char date_str[] = __DATE__;
PRIVATE const char time_str[] = __TIME__;

PRIVATE char *main_prog = NULL;

#ifdef SGI
#define SYSTEM_NAME "(Silicon Graphics)"
#endif

#ifdef VCCPX
#if defined(VPP5000)
#define SYSTEM_NAME "(Fujitsu VPP5000 [cross compiler])"
#else
#define SYSTEM_NAME "(Fujitsu VPP [cross compiler])"
#endif
#elif VPP5000
#define SYSTEM_NAME "(Fujitsu VPP5000) [native]"
#elif VPP
#define SYSTEM_NAME "(Fujitsu VPP) [native]"
#endif

#if defined(NECSX)
#define SYSTEM_NAME "(NEC SX series [cross compiler])"
#endif

#ifdef SUN4
#define SYSTEM_NAME "(Sun/Solaris)"
#endif

#ifdef HPPA
#define SYSTEM_NAME "(HP PA-Risc)"
#endif

#ifdef LINUX
#if defined(NECTX)
#define SYSTEM_NAME "(LINUX/NEC FRONT-END)"
#elif defined(CYGWIN)
#define SYSTEM_NAME "(WINDOWS/CYGWIN)"
#elif defined(SUN4)
#define SYSTEM_NAME "(Sun/Linux)"
#elif defined(CRAYXT)
#define SYSTEM_NAME "(Cray XT)"
#else
#define SYSTEM_NAME "(LINUX)"
#endif
#endif

#ifdef ALPHA
#define SYSTEM_NAME "(ALPHA)"
#endif

#ifdef RS6K
#define SYSTEM_NAME "(IBM SP RS/6000 AIX)"
#endif

#ifndef SYSTEM_NAME
#define SYSTEM_NAME "(Probably Intel or AMD)"
#endif

PRIVATE char system_name[] = SYSTEM_NAME; 

int pcma_blocksize = 256;
int pcma_restore_packed = 0;

#define DEVNULL "/dev/null"
FILE *fpdevnull = NULL;

PRIVATE Boolean traceback_done = 0;

int filtered_info = 0;
int ddl_piped = 0;
PRIVATE char *tmp_file = NULL;

PRIVATE char *original_name = NULL;
PRIVATE Boolean print_help = 0;
PRIVATE Boolean views_only = 0;
PRIVATE Boolean create_so = 1;
PRIVATE Boolean check_syntax_only = 0;
Boolean debug = 0;
Boolean verbose = 0;
PRIVATE Boolean print_version = 0;
int optlevel = 3; /* this used to be 2 until 2-Mar-2006/SS; now we don't need to specify -O3 anymore */
int genstatic = 0;
int incremental = 0;
int merge_table_indices = 0;
int use_indices = 1; /* the default can be overridden via 'export ODB_USE_INDICES=0' in ksh */
int reorder_tables = 1; /* the default can be overridden via 'export ODB_REORDER_TABLES=0' in ksh */
int insert_tables = 1;  /* the default can be overridden via 'export ODB_INSERT_TABLES=0' in ksh */
int Ccompile_Sstatic_stubb = 1;
char *one_tables = NULL;
int safeGuard = 0; /* the default can be overridden via 'export ODB_SAFEGUARD=1' in ksh */
Boolean reset_align_issued = 0;
Boolean reset_onelooper_issued = 0;
Boolean readonly_mode = 0;
Boolean insert_drhook = 1;
int latlon_rad = -1; /* 
			Assumptions about (lat,lon) [or $ODB_LAT / $ODB_LON]
			Check value of environment $ODB_LATLON_RAD
			-1 : Not initialized
			 1 : (lat,lon) in radians
			 0 : (lat,lon) in degrees
			 This has impact on new functions 
			 lat_degrees & lon_degrees  and
			 lat_radians & lon_radians
			 Instead of _radians/_degrees you can use _deg2rad/_rad2deg, too
		     */

char *bailout_string = NULL;
int bailout_level = 0; /*
			 -b               (bailout_level == -1)
			 -B bailout_level

			 To bail out from incorrect column or table names in SQL
			 When set to -1 (via use of -B flag or export ODB_BAILOUT=-1, 
			 bailout ([no]bailout) keyword in SQL), it means that :
			 (1) Spelling mistakes in SELECT columns result 
			     in substitution of a column into a formula zero ("0")
			 (2) Spelling mistakes in formulas in SELECT columns
			     lead to substitution of (formula) 0 (zero) inplace of a column
			 (3) Incorrect table name in FROM statement lead to ignore 
			     that table
			 (4) Columns in UNIQUEBY are simply ignored
			 (5) Columns in ORDERBY are ignored as in UNIQUEBY
			 (6) Column variables in WHERE condition are substituted with zero.

			 If you want other than "0", f.ex. NULL, set also the environment variable
			 export ODB_BAILOUT_STRING='$mdi' for instance

		       */

PRIVATE Boolean empty_tables = 0;
PRIVATE char *table_list = NULL;
PRIVATE int n_table_list = 0;
PRIVATE char delim_table_list = 0;
PRIVATE char *sql_query = NULL;

char *ARGH = NULL;

FILE *fpsrc = NULL;
int include_nesting = 0;
char *odb_source = NULL;
char *filter_cmd = NULL;
char *odb_syspath = NULL;
char *odb_outpath = NULL;
char *odb_cc = NULL;
char *odb_ld = NULL;
char *odb_libs = NULL;
char *odb_libs_keep = NULL;
char *odb_label = NULL;

#ifndef ODB_SYSPATH
#define ODB_SYSPATH "."
#endif

PRIVATE void rm()
{
  if (tmp_file && is_regular_file_(tmp_file)) remove_file_(tmp_file);
}

PRIVATE void
set_signals()
{
  static Boolean done = 0;
  if (!done) {
    signal(SIGBUS, ODB_sigexit);
    signal(SIGSEGV, ODB_sigexit);
    signal(SIGILL, ODB_sigexit);
#if defined(SIGEMT)
    signal(SIGEMT, ODB_sigexit);
#endif
#if defined(SIGSTKFLT)
    signal(SIGSTKFLT,ODB_sigexit); /* Stack fault */
#endif
    /* signal(SIGFPE, ODB_sigexit); */
    signal(SIGTRAP, ODB_sigexit);
    signal(SIGINT, ODB_sigexit);
    signal(SIGQUIT, ODB_sigexit);
    signal(SIGTERM, ODB_sigexit);
    signal(SIGIO, ODB_sigexit);
    signal(SIGABRT, ODB_sigexit);
    signal(SIGXCPU, ODB_sigexit);
    /* signal(SIGCHLD, SIG_IGN); */
    atexit(rm);
    done = 1;
  }
}

PRIVATE void
fixenv(const char *name, const char *value)
{
  char *env;
  int len = STRLEN(name) + 1 + STRLEN(value) + 1;
  ALLOC(env, len);
  sprintf(env,"%s=%s",name,value);
  /* fprintf(stderr,"fixenv: %s\n",env); */
  putenv(env);
  /* env is purposely NOT freed; space internally allocated & used by char **environ */
}


PRIVATE int   *unit4fp     = NULL;
PRIVATE char **unit4fp_str = NULL;
PRIVATE int  unit4fp_len   = 0;

PRIVATE void
mapfp2unit(FILE *fp, int unit, const char *file)
{
  if (fp) {
    int fno = fileno(fp);
    if (!unit4fp) {
      int j;
#ifdef CRAYXT
      unit4fp_len = sysconf(_SC_OPEN_MAX);
#else
      unit4fp_len = getdtablesize();
#endif
      ALLOC(unit4fp, unit4fp_len);
      ALLOC(unit4fp_str, unit4fp_len);
      for (j=0; j<unit4fp_len; j++) {
	unit4fp[j] = -1; /* Means: Not in use */
	unit4fp_str[j] = NULL;
      }
    }
    if (fno >= 0 && fno < unit4fp_len) {
      unit4fp[fno] = unit;
      unit4fp_str[fno] = STRDUP(file);
    }
  }
}


PUBLIC void
cma_prt_stat_(const int *ftn_unit, const int *cma_unit, 
	      const int *binno, const int *numbins, 
	      const int *fileno, 
	      const char *str_open_time,  const char *str_close_time, 
	      const char *logical_name,   const char *true_name, 
	      const char *pipecmd,        const char *cmd, 
	      const int *read_only, const int *packmethod, const int *blocksize, 
	      const int *numddrs,   const int *lenddrs, 
	      const int *numobs,    const int *maxreplen,  const int *cmalen, 
	      const int *filesize,  const int *filepos, const int *blksize,  
	      const int *bytes,     const int *num_trans, 
	      const int *readbuf_len, const int *readbuf_is_alloc, 
	      const int *writebuf_len, const int *writebuf_is_alloc, 
	      const int *prealloc, const int *extent,   
	      const int *mrfs_flag, 
	      const double *walltime,  const double *xfer_speed, 
	      const double *usercpu,   const double *syscpu,
	      /* Hidden arguments */
	      int str_open_time_len,  int str_close_time_len, 
	      int logical_name_len,   int true_name_len, 
	      int pipecmd_len,        int cmd_len
	      )
{
  /* A dummy replacement routine to avoid the need to
     have Fortran I/O library linked into the C-code.
     This routine might be called from cma_close_(),
     called by ODB_fclose() */
  return;
}

int drhook_lhook = 0; /* dummy; to satisfy externals */

void
Dr_Hook(const char *name, int option, double *handle, 
	const char *filename, int sizeinfo,
	int name_len, int filename_len)
{ 
  /* A dummy Dr.Hook -call to satisfy externals */ 
}

#ifdef RS6K
PUBLIC void /* Just a dummy ; due to the new RAISE(sig)-macro in privpub.h */
xl__trbk_() { }
#endif

#if defined(VPP5000) || defined(VPP)

/* Fujitsu VPP doesn't have snprintf, so we created one */
/* The same code sits in ifsaux/support/endian.c */

int snprintf(char *str, size_t size, const char *format, ...)
{
  int rc;
  va_list ap;
  va_start(ap, format);
  rc = vsprintf(str, format, ap);
  va_end(ap);
  return rc;
}

#endif

/* A dummy in case macro ALLOCA gets used */
ll_t getstk_() { return 0; }

/* A copy of odb/tools/abor1.c */

void abor1_(const char *s, int slen)
{
  if (s && slen > 0 && slen < 1024) {
    fprintf(stderr,"***Error in abor1_: %*s\n",slen,s);
  }
  else {
    fprintf(stderr,"***Error in abor1_: Aborting ...\n");
  }
  raise(SIGABRT);
  _exit(1); /* Should never end up here */
}

void abor1fl_(const char *filename, const int *linenum, 
	      const char *s, 
	      int filenamelen, int slen)
{
  if (filename && filenamelen > 0 && linenum && *linenum > 0) {
    fprintf(stderr,
	    "***Error: abor1fl_ has been called at %*s:%d\n",
	    filenamelen, filename, *linenum);
  }
  abor1_(s,slen);
  _exit(1); /* Should never end up here */
}

PUBLIC FILE *
ODB_trace_fp(void)
{
  /* A dummy routine to return NULL fp of non-existent tracing file */
  return NULL;
}

/* More dummies to patch gaps due to thread safe implementation of CMA I/O */
PUBLIC void coml_in_parallel_(int *kispar_region) { if (kispar_region) *kispar_region = 0; }
PUBLIC void coml_init_lockid_with_name_(o_lock_t *mylock, const char *name, int name_len) { }
PUBLIC void coml_init_lockid_(o_lock_t *mylock) { }
PUBLIC void coml_set_lockid_(o_lock_t *mylock)  { }
PUBLIC void coml_unset_lockid_(o_lock_t *mylock) { }
PUBLIC void coml_test_lockid_(int *is_set, o_lock_t *mylock) { if (is_set) *is_set = 1; }
PUBLIC int get_thread_id_() { return 1; }
PUBLIC int get_max_threads_() { return 1; }

PUBLIC int
ODB_fclose(FILE *fp, const char *srcfile, int srcline)
{
  int rc = 0;
  if (fp && fp == stdin) {
    /* do nothing */
  }
  else if (fp && fp == stdout) {
    fflush(fp);
  }
  else if (fp && fp == stderr) {
    fflush(fp);
  }
  else if (fp) {
    extern void cma_close_(const int *unit, 
			   int *retcode);
    char *str = NULL;
    int unit = -1;
    int fno = fileno(fp);

    if (fno >= 0 && fno < unit4fp_len) {
      unit = unit4fp[fno];
      if (unit == -1) {
	/* Presumably not opened via ODB_fopen */
	rc = fclose(fp);
	goto finish;
      }
      str = STRDUP(unit4fp_str[fno]);
    }

    if (verbose && str) {
      fprintf(stderr,
	      "***Closing file '%s' at (fp=%p, fno=%d, unit=%d) [%s:%d]\n", 
	      str, fp, fno, unit,
	      srcfile, srcline);
    }

    cma_close_(&unit, &rc);

    if (rc != 0) {
      fprintf(stderr,
	      "***Error in ODB_fclose(%s:%d): Unable to close file '%s' (fp=%p, fno=%d, unit=%d) : rc=%d\n",
	      srcfile, srcline,
	      str?str:"<UNDEFINED>", fp, fno, unit, rc);
      exit(1);
    }

    FREE(str);
    if (fno >= 0 && fno < unit4fp_len) {
      unit4fp[fno] = -1;
      FREE(unit4fp_str[fno]);
    }
  }

 finish:
  return rc;
}

PUBLIC FILE *
ODB_fopen(const char *file, const char *mode, const char *srcfile, int srcline)
{
  FILE *fp = NULL;

  if (strequ(file, "stdin")) {
    fp = stdin;
  }
  else if (strequ(file, "stdout")) {
    fp = stdout;
  }
  else if (strequ(file, "stderr")) {
    fp = stderr;
  }
  else {

    /* Benefit implicitly from ioassign */

    extern void cma_open_(int *unit,
			  const char *filename,
			  const char *mode,
			  int *retcode,
			  /* Hidden arguments to Fortran-program */
			  int len_filename,
			  int len_mode);
    extern FILE *CMA_get_fp(const int *unit);
    
    int unit;
    int rc;
    
    cma_open_(&unit, file, mode, &rc, STRLEN(file), STRLEN(mode));
    if (rc == 1) fp = CMA_get_fp(&unit);
    
    if (!fp) {
      fprintf(stderr,
	      "***Error in ODB_fopen(%s:%d): Unable to open '%s', mode='%s' : rc=%d\n",
	      srcfile, srcline,
	      file, mode, rc);
      exit(1);
    }
    
    mapfp2unit(fp, unit, file);
    
    if (verbose) {
      char *str = NULL;
      int fno = fileno(fp);
      if (fno >= 0 && fno < unit4fp_len) {
	unit = unit4fp[fno];
	str = STRDUP(unit4fp_str[fno]);
      }
      if (str) fprintf(stderr,
		       "***Opened file '%s' at (fp=%p, fno=%d, unit=%d, mode='%s') [%s:%d]\n", 
		       str, fp, fno, unit, mode,
		       srcfile, srcline);
      FREE(str);
    }
    
  }
  return fp;
}

PRIVATE void
write_symbol_map()
{
  if (!ddl_piped) {
    FILE *fpmap;
    char *map;
    ALLOC(map, STRLEN(odb_outpath) + STRLEN(original_name) + 20);
    sprintf(map,"%s/%s.lst",odb_outpath,original_name);
    if (verbose) fprintf(stderr,"Symbol output goes to file '%s'\n",map);
    fpmap = FOPEN(map, "w");
    ODB_print_symbols(fpmap);
    FCLOSE(fpmap);
    FREE(map);
  }
}

PRIVATE void
Print_ODB_env()
{
  if (verbose) {
    fprintf(stderr,"ODB_SYSPATH ==> '%s'\n",odb_syspath);
    fprintf(stderr,"ODB_OUTPATH ==> '%s'\n",odb_outpath ? odb_outpath : "<undefined>");
    fprintf(stderr,"ODB_CC ==> '%s'\n",odb_cc);
    fprintf(stderr,"ODB_LD ==> '%s'\n",odb_ld);
    fprintf(stderr,"ODB_LIBS ==> '%s'\n",odb_libs);
    fprintf(stderr,"ODB_LIBS_KEEP ==> '%s'\n",odb_libs_keep);
  }
}

PRIVATE char *
transform_label(const char *s)
{
  char *new_s = s ? STRDUP(s) : NULL;
  char *sp = new_s;
  if (sp) {
    char *p = STRDUP(sp);
    char *new_p = p;
    /* Must start with [a-zA-Z] */
    if (!((*sp >= 'a' && *sp <= 'z')||
	  (*sp >= 'A' && *sp <= 'Z'))) 
      sp++;
    while (*sp) {
      /* accepted chars [a-zA-Z0-9] ==> uppercase where applicable */
      if ((*sp >= 'a' && *sp <= 'z') ||
	  (*sp >= 'A' && *sp <= 'Z') ||
	  (*sp >= '0' && *sp <= '9')) {
	if (islower(*sp)) {
	  *p++ = toupper(*sp);
	}
	else {
	  *p++ = *sp;
	}
      }
      sp++;
    }
    *p = '\0';
    FREE(new_s);
    new_s = new_p;
  }
  return new_s;
}

char *
ODB_uppercase(const char *s)
{
  char *new_s = s ? STRDUP(s) : NULL;
  char *sp = new_s;
  if (sp) {
    do {
      if (islower(*sp)) *sp = toupper(*sp);
    } while(*++sp);
  }
  return new_s;
}

char *
ODB_lowercase(const char *s)
{
  char *new_s = s ? STRDUP(s) : NULL;
  char *sp = new_s;
  if (sp) {
    do {
      if (isupper(*sp)) *sp = tolower(*sp);
    } while(*++sp);
  }
  return new_s;
}

char *
ODB_capitalize(const char *s)
{
  char *new_s = s ? ODB_lowercase(s) : NULL;

  if (new_s) {
    if (islower(new_s[0])) new_s[0] = toupper(new_s[0]);
  }

  /* fprintf(stderr,"ODB_capitalize(%s) ==> %s\n",s,new_s); */

  return new_s;
}


PRIVATE void
ODB_getenv()
{
  char *p;

  p = getenv("ODB98_DEBUG");
  if (p && strequ(p,"1")) debug = 1;

  p = getenv("ODB_SYSPATH");
  if (p) {
    odb_syspath = STRDUP(p);
  }
  else {
    odb_syspath = STRDUP(ODB_SYSPATH);
    fixenv("ODB_SYSPATH", odb_syspath);
  }

  p = getenv("ODB_OUTPATH");
  odb_outpath = p ? STRDUP(p) : STRDUP(".");
  fixenv("ODB_OUTPATH", odb_outpath);

  p = getenv("ODB_CC");
#ifdef VPP
  odb_cc = p ? STRDUP(p) : STRDUP("vcc -g -DVPP");
#else
  odb_cc = p ? STRDUP(p) : STRDUP("cc -g");
#endif

  p = getenv("ODB_LD");
#ifdef VPP
  /* odb_ld = p ? STRDUP(p) : STRDUP("frt -X9 -Wl,-dy,-G,-Bdynamic"); */
  odb_ld = p ? STRDUP(p) : STRDUP("ld -dy -G -Bdynamic");
#else
  /* odb_ld = p ? STRDUP(p) : STRDUP("f90 -shared"); */
  odb_ld = p ? STRDUP(p) : STRDUP("ld -shared");
#endif

  p = getenv("ODB_LIBS");
  odb_libs = p ? STRDUP(p) : STRDUP("");

  p = getenv("ODB_LIBS_KEEP");
  odb_libs_keep = p ? STRDUP(p) : STRDUP("0");

  p = getenv("ODB_LABEL");
  odb_label = p ? transform_label(p) : NULL;

  p = getenv("ODB_READONLY");
  if (p) readonly_mode = atoi(p);

  p = getenv("ODB_LATLON_RAD");
  if (p) {
    latlon_rad = atoi(p);
    if (latlon_rad != -1 &&
	latlon_rad !=  0 && 
	latlon_rad !=  1) latlon_rad = -1; /* i.e. as if undefined */
  }

  p = getenv("ODB_USE_INDICES");
  if (p) use_indices = atoi(p);

  p = getenv("ODB_REORDER_TABLES");
  if (p) reorder_tables = atoi(p);

  p = getenv("ODB_INSERT_TABLES");
  if (p) insert_tables = atoi(p);

  p = getenv("ODB_SAFEGUARD");
  if (p) safeGuard = atoi(p);

  p = getenv("ODB_BAILOUT");
  if (p) bailout_level = atoi(p);

  p = getenv("ODB_BAILOUT_STRING");
  if (p) bailout_string = STRDUP(p);
  else bailout_string = STRDUP("0");
}


void 
odb_warn(char *errormsg, int rc) 
{ 
  extern int ODB_ncmds;
  extern char yytext[];

  if (ODB_ncmds > 0 || STRLEN(yytext) > 0) {
    extern int ODB_lineno;
    fprintf(stderr,"*** %s in \"%s\":%d near or before token '%s'\n",
	    (rc == 0) ? "Warning" : "Error", 
	    odb_source,ODB_lineno,yytext);
    if (errormsg) {
      fprintf(stderr,"%s\n",errormsg);
    }
    if (odb_source) {
      char cmd[256];
      sprintf(cmd,"head -%d %s | tail -1",ODB_lineno,odb_source);
      (void)system(cmd);
      fflush(stdout);
    }

    /*
    LEX_print_state(stderr);
    
    {
      extern int include_nesting;
      fprintf(stderr,"\tinclude_nesting = %d\n",include_nesting);
    }
    */
    
    write_symbol_map();
  }
  else
    fprintf(stderr,"Input file '%s' contains no statements\n",odb_source);

  /* sleep(5); */
}

void 
yyerror(char *errormsg) 
{
  odb_warn(errormsg, 1);
  exit(1);
}


PUBLIC int
ODB_fprintf(FILE *fp, const char *format, ...)
{
  /* The same as fprintf(), but does nothing when fp points to NULL or fpdevnull */
  if (!fp || fp == fpdevnull) return 0;
  else {
    int n;
    va_list args;
    va_start(args, format);
    n = vfprintf(fp, format, args);
    va_end(args);
    return n;
  }
}

PUBLIC uint
ODB_hash(int kind, const char *s) /* taken from ioassign_hash.c */
{
  static const uint hashsize = 123457U; /* hardcoded for now */
  uint hashval = kind;
  for (; *s ;) {
    hashval = (*s++) + 31U * hashval;
  }
  return hashval%hashsize;
}

PRIVATE char *
Check_Env(int fixtoo, const char *prefix, const char *dbname, const char *idx)
{
  char *env = NULL;
  if (prefix && dbname) {
    char *long_env, *short_env;
    char *long_name = NULL;
    const char *short_name = prefix;
    int len;

    len = STRLEN(prefix) + 1 + STRLEN(dbname) + 1;
    ALLOC(long_name, len);
    sprintf(long_name,"%s_%s", prefix, dbname);
    long_env = getenv(long_name);

    if (fixtoo || !long_env) {
      short_env = getenv(short_name);
      
      if (long_env) {
	if (!short_env) fixenv(short_name, long_env);
      }
      else if (short_env) {
	fixenv(long_name, short_env);
      }
      
      short_env = getenv(short_name);
      if (!short_env) fixenv(short_name, idx ? idx : ".");

      long_env = getenv(long_name);
      if (!long_env) fixenv(long_name, idx ? idx : ".");

      env = getenv(long_name);
      
      FREE(long_name);
    }
    else {
      env = long_env;
    }
  }
  return env;
}

PRIVATE void
Check_Empty_Tables(const char *dbname)
{
  FREE(table_list);
  n_table_list = 0;
  delim_table_list = 0;
  if (dbname && empty_tables) {
    char *ddfile = NULL;
    int len = STRLEN(dbname) + STRLEN(".dd") + 1;
    ALLOC(ddfile,len);
    snprintf(ddfile,len,"%s.dd",dbname);
    if (!is_regular_file_(ddfile)) {
      /* This was completely fresh database w/o any data ==> compile all tables */
      empty_tables = 0;
      goto finish;
    }
    /* Read from Unix-pipe those files i.e. table names, which have got some data
       and save the list to the 'table_list' */
    {
      const char cmd[] = "find [0-9]* -type f -follow -print | perl -pe 's|^.*/||' | sort -u";
      FILE *fp = popen(cmd,"r");
      /* fprintf(stderr,"||| cmd='%s' : fp=%p\n",cmd,fp); */
      if (fp) { /* Success */
	char line[MAXLINE];
	char *p;
	init_list(NULL);
	while ( (p = fgets(line, MAXLINE, fp)) != NULL ) {
	  while (isspace(*p)) p++;
	  if (*p) {
	    char *nl = strchr(p,'\n');
	    if (nl) *nl = '\0';
	    (void) add_list(p);
	  }
	  if (feof(fp)) break;
	}
	n_table_list = get_list_elemcount();
	p = get_list();
	/* fprintf(stderr,"||| n_table_list=%d\n",n_table_list); */
	table_list = STRDUP(p);
	/* fprintf(stderr,"||| table_list='%s'\n",table_list); */
	delim_table_list = get_list_delim();
	/* fprintf(stderr,"||| delim_table_list=%c\n",delim_table_list); */
	destroy_list();
	pclose(fp);
      }
      else { /* And error, but ignored ==> compile all tables */
	empty_tables = 0;
	goto finish;
      }
    }
  finish:
    FREE(ddfile);
    return;
  }
  if (!table_list || n_table_list == 0) {
    empty_tables = 0; /* Did not satisfy boundary conditions ==> compile all tables */
  }
  /* fprintf(stderr,"||| empty_tables=%d\n",(int)empty_tables); */
}


PUBLIC Boolean
is_dummy_table(const char *name)
{
  Boolean dummy_table = empty_tables; /* By default no table is considered dummy i.e. as if -z was NOT specified */
  if (empty_tables) { /* -z was supplied */
    if (in_mylist(name, table_list, delim_table_list)) dummy_table = 0;
  }
  if (dummy_table && verbose) 
    fprintf(stderr,"**Warning: table '%s' is considered empty --> minimal code generated\n",name);
  return dummy_table;
}


#define FLAGS "1:A:bB:cCdD:f:F:GhHiI:l:mO:o:q:Q:rRsSU:uvVwz"
#define USAGE \
  "Usage: %s\n" \
  "       [-c (compile_only)]\n" \
  "       [-C (check syntax of ddl/sql only)]\n" \
  "       [-d(ebug)] [-h(elp)]\n" \
  "       [-w (vieWs_only)] [-l odb_label]\n" \
  "       [-o output_source_path]\n" \
  "       [-I system_data_path]\n" \
  "       [-v(erbose)]\n" \
  "       [-V(ersion)]\n" \
  "       [-O{0|1|2|3}]\n" \
  "       [-s] [-i]\n" \
  "       [-R] [-S]\n" \
  "       [-D define_string]\n" \
  "       [-1 master_table=slave_table]\n" \
  "       [-A master_table=slave_table]\n" \
  "       [-m(erge_table_indices)]\n" \
  "       [-G (turn safeGuard-tests in views' C-code back on)]\n" \
  "       [-f filter_command]\n" \
  "       [-r (generate read/only-code)]\n" \
  "       [-H (do *NOT* insert Dr.Hook-calls)] \n" \
  "       [-z (dummy code for empty tables)]\n" \
  "       [-F[-]{0|1|2|3}]\n" \
  "       [-q 'sql_query']\n" \
  "       [-Q sql_query_file]\n" \
  "       [-b (to bailout from column/table name spelling mistakes in SQLs)]\n" \
  "       [-B bailout_level_bitmap]\n" \
  "       [-u (do not use CREATE INDEX-indices)]\n" \
  "       {DDL-file | SQL-file(s)}\n"

/* Supported flags:

   -c               ! Compile only i.e. do not create shared objects (.so)
   -C               ! Check syntax of ddl/sql only, but don't create .o nor .so
   -d               ! Debug mode  (also activated via export ODB98_DEBUG=1)
   -h               ! Help me! Prints usage and exits
   -H               ! Insert Dr.Hook -calls
   -l odb_label     ! Change the default label otherwise determined from 
                    !   the DDL-file name or ODB_LABEL. Must contain letters [A-Z]
   -o source_path   ! Where to place the output files (.h, .c, .o, .so, .ddl_)
   -w               ! Handle vieWs only
   -I system_path   ! Location of the ODB system files, like "odb.h" etc.
   -v               ! More verbose output
   -V               ! Print version number of the compiler
   -O [0|1|2|3]     ! Generate optimized C-code (-O3 is the default)
   -s               ! Generate static stubb (xxx_Sstatic.c)
   -i               ! Enable incremental update of static stubb (if applicable via -s)
   -D def_string    ! Define preprocessing key, say -DMETEO_FRANCE in layout/sql
   -U def_string    ! Undefine preprocessing key, say -UECMWF in layout/sql
   -1 master=slave  ! The master table (say 'index') that refers to the given slave table
                    ! (say 'hdr') with constant length of one (in level -O3 only)
                    ! See also ONELOOPER-stmt in ODB/SQL-language
   -A master=slave  ! Define align tables, that go in sync-step when selecting data
                    ! See also ALIGN-stmt in ODB/SQL-language
   -m               ! To merge table indices in views, if ALIGN or -A was specified
   -R               ! Do *NOT* perform reordering of TABLEs in FROM-statement
                    ! Can also be specified/overridden by REORDER/NOREORDER ODB/SQL-stmts
   -S               ! Suppress C-compilation of static stubb (xxx_Sstatic.c)
   -G               ! Turn safeGuard-tests in views' C-code back on (default is off)
   -r               ! Generate read/only-code by default (as if READONLY; was given in SQL)
   -z               ! Provides dummy routines for non-existent (empty) tables to both
                    ! reduce compilation time and and size of object library.
                    ! "Emptyness" is checked by the odb98.x compile by help of command
                    ! 'find [0-9]* -type f -follow -print | xargs -n1 basename | sort -u'
                    ! (However we are using substantially faster perl-version instead of xargs)
   -F[-][0|1|2|3]   ! Provides filtered compilation information for use by generic select 
                    ! (goes to file <dbname>_<viewname>.info)
                    ! Level 0 : No info created at all (the default)
                    ! Level 1 : Standard info created (implemented)
                    ! Level 2 : As Level 1, but no C-code is created (<dbname>.ddl_ will be created)
                    ! Level 3 : As Level 2, but more extensive information about layout is also written
                    ! Level >= 2 implies also -O3 -C i.e. optlevel=3 and no compilation of non-existent C-code
                    ! If you DO NOT employ the negative (-) sign, then the I/O is ...
                    !    ... to/from stdout/stdin i.e. unix pipes are ok
   -q 'sql_query'   ! Supply SQL-query via command line string; only one query possible
                    ! View name by default "myview", but an attempt to search 'create view' is also made
                    ! Implies -w. Useful when "piping" the .ddl_ i.e. with -F2 option
   -Q sql_query_file! The same as -q 'sql_query', but the "sql_query" is read from the file "sql_query_file"
                    ! This option was made available for long queries (Unix shell cmd line limit problems)
   -b               ! See discussion above ; search backwards for "bailout_level"
   -B bailout_level ! To supply specific OR'red bitmap as bailout_level
   -u               ! Opt out from using CREATE INDEX-indices (same as export ODB_USE_INDICES=0)

  Typically for tables :  odb98.x dbase.ddl
            for view(s):  odb98.x -w -l dbase myview.sql
            for view(s):  odb98.x -w -l dbase myview1.sql myview2.sql
	    no .so-file:  odb98.x -c -w -l dbase myview.sql

  In addition defaults may be read from $ODB_COMPILER_FLAGS (if present)
  If not present, then set to "/dev/null", otherwise odb98.x may hang forever (29-Jun-2006/SS)

 */

PRIVATE char **
get_default_args(int *numargs, int *errflg)
{
  /* Use purposely fopen & fclose (rather than FOPEN & FCLOSE)
     to prevent from using CMAIO & IOASSIGN routines,
     since environment variables they may access are not
     defined at this early stage */

  char **args = NULL;
  int nargs = 0;
  char *env = getenv("ODB_COMPILER_FLAGS");

  if (!env) {
    if (debug) fprintf(stderr,"get_default_args: ODB_COMPILER_FLAGS not set; assuming '/dev/null'\n");
    /* To prevent further hanging */
    fixenv("ODB_COMPILER_FLAGS", "/dev/null");
    env = getenv("ODB_COMPILER_FLAGS");
  }

  if (debug) fprintf(stderr,"get_default_args: ODB_COMPILER_FLAGS=%s\n",env?env:NIL);

  if (env) {
    FILE *fp = fopen(env,"r");
    if (fp) {
      char *p;
      char line[MAXLINE];

      while ( (p = fgets(line, MAXLINE, fp)) != NULL ) {
	while (isspace(*p)) p++;
	if (*p == '-') nargs++;
      }

      if (debug) fprintf(stderr,"get_default_args: no. of args = %d\n",nargs);

      if (nargs > 0) {
	int j = 0;
	CALLOC(args, nargs);
	rewind(fp);
	while ( (p = fgets(line, MAXLINE, fp)) != NULL ) {
	  while (isspace(*p)) p++;
	  if (*p == '-') {
	    char *nl = strchr(p,'\n');
	    if (nl) *nl = '\0';
	    args[j++] = STRDUP(p);
	  }
	}
      } /* if (nargs > 0) */

      fclose(fp);

      if (debug) {
	int j;
	for (j=0; j<nargs; j++) {
	  fprintf(stderr,"get_default_args: args[%d] = '%s'\n",j,args[j]);
	}
      }
    }
    else {
      fprintf(stderr,"***Error: Unable to open file behind the ODB_COMPILER_FLAGS=%s\n",env);
      (*errflg)++;
    } /* if (fp) ... else ... */
  }

  if (numargs) *numargs = nargs;
  return args;
}


PRIVATE Boolean
scan_include(FILE *fp, const char *target)
{
  Boolean has_include = 0;
  char *p;
  char line[MAXLINE];
  while ( (p = fgets(line, MAXLINE, fp)) != NULL ) {
    while ( *p == ' ' || *p == '\t' ) p++;
    if (strnequ(p,"INCLUDE ",8) || 
	strnequ(p,"INCLUDE\t",8) ||
	strnequ(p,"#include ",9) ||
	strnequ(p,"#include\t",9)) {
      char *s;
      int slen = STRLEN(target) + 5;
      ALLOC(s,slen);
      sprintf(s,"%s.ddl",target);
      has_include = (strstr(s,p+8) != NULL);
      FREE(s);
      if (has_include) break;
    }
    else if (strnequ(p,"CREATE",6) || strnequ(p,"VIEW",4)) {
      has_include = 0;
      break;
    }
  }
  FREWIND(fp);
  return has_include;
}


PUBLIC int
ODB_addrel(const char *p, char separ)
{
  int rc = 0;
  char *start = STRDUP(p);
  char *s_in = start;
  char *buf = STRDUP(s_in);
  char *s_out = buf;
  char *master = NULL;
  char **slaves = NULL;
  int j, nslaves = 0;
  char *s;

  while (*s_in) {
    if (*s_in != ' ' && *s_in != '\t' &&
	*s_in != '(' && *s_in != ')') {
      *s_out++ = *s_in++;
    }
    else
      s_in++;
  }
  *s_out = '\0';

  s = strchr(buf,separ);
  if (s) {
    char *token;
    char *saved = NULL;
    master = STRDUP(buf);
    s = strchr(master,separ);
    *s = '\0';
    saved = STRDUP(s+1);
    token = strtok(s+1,",");
    while (token) { /* Count no. slaves */
      char *t = STRDUP(token);
      token = strtok(NULL,",");
      nslaves++;
      FREE(t);
    }
    if (nslaves > 0) {
      CALLOC(slaves, nslaves);
      token = strtok(saved,",");
      j = 0;
      while (token) { /* Count no. slaves */
	slaves[j++] = STRDUP(token);
	token = strtok(NULL,",");
      }
      rc = 0;
      /* for (j=-1; j<nslaves-1; j++) */ {
	int k;
	j = -1;
	/* if (j >= 0) master = STRDUP(slaves[j]); */
	for (k=j+1; k<nslaves; k++) {
	  char *x;
	  int xlen = STRLEN(master) + 1 + STRLEN(slaves[k]) + 1;
	  ALLOC(x,xlen);
	  sprintf(x,"%s%c%s",master,separ,slaves[k]);
	  add_list(x);
	  FREE(x);
	  rc++;
	} /* for (k=j+1; k<nslaves; k++) */
	FREE(master);
      } /* for (j=-1; j<nslaves; j++) */
    } /* if (nslaves > 0) */
    FREE(saved);
  }
    
  FREE(start);
  FREE(buf);
  FREE(master);
  for (j=0; j<nslaves; j++) {
    FREE(slaves[j]);
  }
  FREE(slaves);
  
  /* rc = ((nslaves + 1) * nslaves)/2; */ /* This many pairs formed */
  return rc;
}


PRIVATE void Compile(const char *cmd, int retry)
{
  if (retry > 0) {
    fprintf(stderr,"## Compilation [retry%d]: %s\n",retry,cmd);
  }
  else if (verbose) {
    fprintf(stderr,"## Compilation: %s\n",cmd);
  }
  else {
    fprintf(stderr,"%s\n",cmd);
  }
  (void)system(cmd);
}


PRIVATE void WriteQuery(FILE *fp, const char *s)
{
  if (fp && s) {
    char quote = 0;
    Boolean comment = 0;
    while (*s) {
      char ch = *s++;
      if (isspace(ch) || isprint(ch)) {
	ODB_fprintf(fp, "%c", ch);
        if (!comment && (ch == '/' || ch == '-') && ch == *s) comment = 1;
        else if (comment && ch == '\n') comment = 0;
	if (quote && quote == ch) quote = 0;
	else if (!quote) {
	  if (!comment && ch == ';') ODB_fprintf(fp,"\n");
	  else if (ch == '\'' || ch == '"') quote = ch;
	}
      }
    }
    ODB_fprintf(fp, " ;\n");
  }
}

PRIVATE char *Slurp(const char *path)
{
  extern int IOgetsize(const char *path, int *filesize, int *blksize);
  int filesize = 0;
  int rc = IOgetsize(path, &filesize, NULL);
  char *s = NULL;
  if (rc == 0 && filesize > 0) {
    FILE *fp = fopen(path,"r");
    if (fp) {
      ALLOC(s, filesize+20);
      rc = fread(s, sizeof(*s), filesize, fp); /* slurp!! */
      if (rc == filesize) { /* Success */
	s[filesize] = '\0'; 
	strcat(s," ;\n");
      }
      else FREE(s); /* Failure */
      fclose(fp);
    }
  }
  if (!s) {
    fprintf(stderr,"***Error: Unable to inline non-existent (or empty) ODB/SQL-file '%s'\n",
	    path ? path : NIL);
    RAISE(SIGABRT);
    _exit(1);
  }
  return s;
}

int
main(int cmdline_argc, char *cmdline_argv[])
{
  char *p;
  extern FILE *yyin;
  int c;
  int errflg = 0;
  int one_count = 0;
  extern char *optarg;
  extern int optind;
  char *Pwd = "";
  char *fpsrc_file = NULL;
  int argc = 0;
  char **argv = NULL;
  int def_argc = 0;
  char **def_argv = NULL;
  int k,j;

  main_prog = cmdline_argv[0];

  (void) ODB_std_mem_alloc(1); /* Always enforce standard memory alloc
				  since some yacc/lex malloc()'s are out
				  of our control */

  fixenv("IOASSIGN_INCORE","0"); /* Make sure no incore IOASSIGN gets used */

  {
    int argh_len = 1 + cmdline_argc;
    for (j=0; j<cmdline_argc; j++) argh_len += STRLEN(cmdline_argv[j]) + 10;
    argh_len *= 2;
    ALLOC(ARGH, argh_len);
  }
  ARGH[0] = '\0';

  set_signals();

  ODB_getenv();

  def_argv = get_default_args(&def_argc, &errflg);
  argc = def_argc + cmdline_argc;

/*
  fprintf(stderr,"main: def_argc=%d, cmdline_argc=%d, argc=%d, errflg=%d\n",
	  def_argc, cmdline_argc, argc, errflg);
*/

  CALLOC(argv,argc+1);

  k = 0;
  if (cmdline_argc > 0) argv[k++] = cmdline_argv[0];

  if (def_argc > 0) {
    for (j=0; j<def_argc; j++) {
      argv[k++] = def_argv[j];
    }
  }

  if (cmdline_argc > 1) {
    for (j=1; j<cmdline_argc; j++) {
      argv[k++] = cmdline_argv[j];
    }
  }

  argv[k] = NULL;

/*
  {
    int j;
    fprintf(stderr,"main: argc = %d\n",argc);
    for (j=0; j<argc; j++) {
      fprintf(stderr,"main: argv[%d] = '%s'\n",j,argv[j] ? argv[j] : NIL);
    }
  }
*/

  init_list(NULL);

  while ((c = getopt(argc, argv, FLAGS)) != -1) {
    switch (c) {
    case '1': /* ONE-looper */
      p = optarg;
      if (!strchr(p,'=')) {
	fprintf(stderr,
		"*** Error: Invalid master-to-slave table relation in flag '-1%s'\n",
		p);
	errflg++;
      }
      else {
	one_count += ODB_addrel(p,'=');
      }
      /* -- delayed until process_one_tables() finished --
      strcat(ARGH,"\n\t -1");
      strcat(ARGH,p);
      */
      break;
    case 'A': /* ALIGNed table(s) */
      p = optarg;
      if (!strchr(p,'=')) {
	fprintf(stderr,
		"*** Error: Invalid master-to-slave table relation in flag '-A%s'\n",
		p);
	errflg++;
      }
      else {
	char *s  = STRDUP(p);
	char *ps = strchr(s,'=');
	if (ps) *ps = '@';
	one_count += ODB_addrel(s,'@');
	FREE(s);
      }
      /* -- delayed until process_one_tables() finished --
      strcat(ARGH,"\n\t -A");
      strcat(ARGH,p);
      */
      break;
    case 'b':
      bailout_level = -1;
      strcat(ARGH,"\n\t -b");
      break;
    case 'B':
      bailout_level = atoi(optarg);
      strcat(ARGH,"\n\t -B");
      strcat(ARGH,optarg);
      break;
    case 'c':
      create_so = 0;
      strcat(ARGH,"\n\t -c");
      break;
    case 'C':
      create_so = 0;
      check_syntax_only = 1;
      strcat(ARGH,"\n\t -C");
      break;
    case 'd':
      debug = 1;
      strcat(ARGH,"\n\t -d");
      break;
    case 'f':
      filter_cmd = STRDUP(optarg);
      strcat(ARGH,"\n\t -f");
      strcat(ARGH,filter_cmd);
      break;
    case 'F':
      filtered_info = atoi(optarg);
      {
	const char *p = strchr(optarg,'.');
	if (ABS(filtered_info) <= 3 && !p) {
	  strcat(ARGH,"\n\t -F");
	  if (ABS(filtered_info) >= 2) {
	    strcat(ARGH,optarg);
	    optlevel = 3;
	    strcat(ARGH,"\n\t -O3");
	    genstatic = -1; /* prevents "-s" having any effect */
	    create_so = 0;
	    check_syntax_only = 1;
	    strcat(ARGH,"\n\t -C");
	  }
	  ddl_piped = (filtered_info > 0) ? 1 : 0;
	}
	else { /* Not a valid level */
	  filtered_info = 0;
	}
      }
      break;
    case 'G':
      safeGuard = 1;
      strcat(ARGH,"\n\t -G");
      break;
    case '?':
    case 'h':
      print_help = 1;
      break;
    case 'H':
      insert_drhook = 0; /* Note the reversed meaning (04-Feb-2006) */
      strcat(ARGH,"\n\t -H");
      break;
    case 'i':
      incremental = 1;
      strcat(ARGH,"\n\t -i");
      break;
    case 'I':
      FREE(odb_syspath);
      odb_syspath = STRDUP(optarg);
      fixenv("ODB_SYSPATH", odb_syspath);
      strcat(ARGH,"\n\t -I");
      strcat(ARGH,odb_syspath);
      break;
    case 'l':
      FREE(odb_label);
      odb_label = transform_label(optarg);
      strcat(ARGH,"\n\t -l");
      strcat(ARGH,odb_label);
      /* break; */
      /* Let purposely "fall through" to also enable -Ddatabase_name !! */
      optarg = odb_label;
    case 'D':
      ODB_put_define(optarg,1);
      strcat(ARGH,"\n\t -D");
      strcat(ARGH,optarg);
      break;
    case 'U':
      ODB_put_define(optarg,0);
      strcat(ARGH,"\n\t -U");
      strcat(ARGH,optarg);
      break;
    case 'm':
      merge_table_indices = 1;
      break;
    case 'O':
      optlevel = atoi(optarg);
      if (optlevel >= 0) {
	strcat(ARGH,"\n\t -O");
	strcat(ARGH,optarg);
      }
      optlevel = MAX(0,optlevel);
      break;
    case 'o':
      FREE(odb_outpath);
      odb_outpath = STRDUP(optarg);
      fixenv("ODB_OUTPATH", odb_outpath);
      strcat(ARGH,"\n\t -o");
      strcat(ARGH,odb_outpath);
      break;
    case 'q':
      views_only = 1;
      strcat(ARGH,"\n\t -w");
      FREE(sql_query);
      sql_query = STRDUP(optarg);
      strcat(ARGH,"\n\t -q'");
      strcat(ARGH,sql_query);
      strcat(ARGH,"'");
      break;
    case 'Q':
      views_only = 1;
      strcat(ARGH,"\n\t -w");
      FREE(sql_query);
      sql_query = Slurp(optarg);
      strcat(ARGH,"\n\t -Q");
      strcat(ARGH,optarg);
      break;
    case 'r':
      readonly_mode = 1;
      strcat(ARGH,"\n\t -r");
      break;
    case 'R':
      reorder_tables = 0;
      strcat(ARGH,"\n\t -R");
      break;
    case 's':
      if (genstatic == 0) {
	genstatic = 1;
	strcat(ARGH,"\n\t -s");
      }
      break;
    case 'S':
      Ccompile_Sstatic_stubb = 0;
      strcat(ARGH,"\n\t -S");
      break;
    case 'u':
      use_indices = 0;
      strcat(ARGH,"\n\t -u");
      break;
    case 'v':
      verbose = 1;
      strcat(ARGH,"\n\t -v");
      break;
    case 'V':
      print_version = 1;
      strcat(ARGH,"\n\t -V");
      break;
    case 'w':
      views_only = 1;
      strcat(ARGH,"\n\t -w");
      break;
    case 'z':
      empty_tables = 1;
      strcat(ARGH,"\n\t -z");
      break;
    default:
      errflg++;
      break;
    }
  }

  if (one_count > 0) {
    char *plist = add_list(NULL);
    if (plist && optlevel >= 3) one_tables = STRDUP(plist);
  }

  destroy_list();

  /* if (one_count == 0 || optlevel < 3) merge_table_indices = 0; */
  if (optlevel < 3) merge_table_indices = 0;
  if (merge_table_indices) strcat(ARGH,"\n\t -m");

  if (verbose) {
    Pwd = "\npwd;";
  }

  if (print_version) {
    fprintf(stderr,"ECMWF ODB/SQL Compiler, Revision %s [%s], %s, %s   %s\n",
	    vers, codb_versions_(NULL, NULL, NULL, NULL),
	    date_str, time_str,
	    system_name);
    fprintf(stderr,"Copyright (c) 1998-2008 ECMWF. All Rights Reserved. ");
    fprintf(stderr,"Contact your ODB representative\n");
  }

  if (print_help) {
    fprintf(stderr,USAGE,argv[0]);
    exit(errflg);
  }

  if (argc - 1 == optind) {
    char *arg = STRDUP(argv[argc-1]);
    char *pslash = strrchr(arg, '/');
    char *dir = NULL;
    if (pslash) {
      *pslash = '\0';
      dir = arg;
      if (verbose) fprintf(stderr,"*** Changing dir to '%s'\n", dir);
      if (chdir(dir) != 0) { /* Error */
	PERROR(dir);
	errflg++;
      }
      odb_source = STRDUP(pslash+1);
    }
    else {
      odb_source = STRDUP(arg);
    }
    if (!errflg && sql_query) {
      /* Ignore IOASSIGN, even if available */
      fixenv("IOASSIGN","/dev/null");
    }
    if (!errflg && sql_query) {
      FILE *fp_in = NULL;
      FILE *fp_out = NULL;
      char *env = getenv("TMPDIR");
      char *viewname = getenv("ODB98_VIEWNAME");
      if (viewname) {
	char *ODBSQL_tmp_file = 
	  env ? "$TMPDIR/%s.sql" : "./%s.sql";
	int len = STRLEN(ODBSQL_tmp_file) + STRLEN(viewname) + 1;
	ALLOC(tmp_file,len);
	snprintf(tmp_file,len,ODBSQL_tmp_file,viewname);
      }
      else {
	char *ODBSQL_tmp_file = 
	  env ? "$TMPDIR/myview_%d.sql" : "./myview_%d.sql";
	int len = STRLEN(ODBSQL_tmp_file) + 20;
	int pid = (int)getpid();
	ALLOC(tmp_file,len);
	snprintf(tmp_file,len,ODBSQL_tmp_file,pid);
      }
      fp_out = FOPEN(tmp_file,"w");
      WriteQuery(fp_out, sql_query);
      FCLOSE(fp_out);
      if (verbose) fprintf(stderr,"*** tmp_file='%s' created\n", tmp_file);
      if (verbose) fprintf(stderr,"*** Treating file '%s' as stdin\n", odb_source);
      /* if (stdin) fclose(stdin); */
      /* stdin = FOPEN(odb_source, "r"); */
      fp_in = freopen(odb_source, "r", stdin);
      if (!fp_in || fp_in != stdin) {
	PERROR(odb_source);
	errflg++;
      }
      else {
	ddl_piped++;
	if (!odb_label) {
	  char *pdot = strchr(odb_source,'.');
	  if (pdot) *pdot = '\0';
	  odb_label = transform_label(odb_source);
	}
	FREE(odb_source);
	odb_source = STRDUP(tmp_file);
      }
    }
    FREE(arg);
  }
  else if (argc > optind || sql_query) {
    /* More than one file supplied or -q -option (or -Q) to supply the query was given */
    FILE *fp_out = NULL;
    char *env = getenv("TMPDIR");
    char *viewname = getenv("ODB98_VIEWNAME");
    if (viewname) {
      char *ODBSQL_tmp_file = 
	env ? "$TMPDIR/%s.sql" : "./%s.sql";
      int len = STRLEN(ODBSQL_tmp_file) + STRLEN(viewname) + 1;
      ALLOC(tmp_file,len);
      snprintf(tmp_file,len,ODBSQL_tmp_file,viewname);
    }
    else {
      char *ODBSQL_tmp_file = 
	env ? "$TMPDIR/myview_%d.sql" : "./myview_%d.sql";
      int len = STRLEN(ODBSQL_tmp_file) + 20;
      int pid = (int)getpid();
      ALLOC(tmp_file,len);
      snprintf(tmp_file,len,ODBSQL_tmp_file,pid);
    }
    fp_out = FOPEN(tmp_file,"w");
    if (sql_query) {
      WriteQuery(fp_out, sql_query);
    }
    else {
      for ( ; optind < argc; optind++) {
	char *sqlfile = argv[optind];
	/* fprintf(fp_out,"#include \"%s\"\n",sqlfile); */
	if (ODB_copyfile(fp_out, sqlfile, NULL, NULL, 0, 1) <= 0) {
	  fprintf(stderr,
		  "***Error: Problems including SQL-file='%s' into tmp_file='%s'\n", 
		  sqlfile, tmp_file);
	  exit(1);
	}
      } /* for ( ; optind < argc; optind++) */
    }
    FCLOSE(fp_out);
    if (verbose) fprintf(stderr,"*** tmp_file='%s' created\n", tmp_file);
    odb_source = STRDUP(tmp_file);
  }

  if (filter_cmd) { /* Not tested */
    /* Apply CPP + PERL filtering (or some other filterings)
       Reason: Enable more relaxed input of KEYWORDs (say, in lowercase) */
    char *cmd;
    char *newfile;
    ALLOC(newfile,STRLEN(odb_source) + 100);
    sprintf(newfile,"%s__%d",odb_source,getpid());
    ALLOC(cmd,STRLEN(filter_cmd) + STRLEN(odb_source) + STRLEN(newfile) + 10);
    sprintf(cmd,"%s < %s > %s", filter_cmd, odb_source, newfile);
    (void)system(cmd);
    if (tmp_file) remove_file_(tmp_file);
    tmp_file = STRDUP(newfile);
    FREE(newfile);
    FREE(odb_source);
    odb_source = STRDUP(tmp_file);
  }

  if (odb_source) {
    int len;
    if (verbose) fprintf(stderr,"*** Input file = '%s'\n",odb_source);
    p = strrchr(odb_source,'/');
    if (!odb_label) {
      char *xp = p ? ODB_uppercase(p+1) : ODB_uppercase(odb_source);
      char *c  = strchr(xp,'.');
      if (c) *c = '\0';
      odb_label = transform_label(xp);
      FREE(xp);
    }
    if (!odb_outpath) {
      char *a = p ? STRDUP(odb_source) : NULL;
      char *b = a ? strrchr(a,'/') : NULL;
      if (b) *b = '\0'; 
      odb_outpath = p ? STRDUP(a) : STRDUP(".");
      FREE(a);
    }
    original_name = p ? STRDUP(p+1) : STRDUP(odb_source);
    {
      char *c = strchr(original_name,'.');
      if (c) *c = '\0';
    }

    /* check for "ddl_" suffix ; do not allow such file for input */
    len = STRLEN(odb_source);
    if (len >= 5) {
      if (strequ(&odb_source[len-5], ".ddl_")) {
	fprintf(stderr, 
		"***Error: Direct compilation of 'ddl_' file '%s' is forbidden\n",
		odb_source);
	errflg++;
      }
    }
  }
  else {
    errflg++;
  }

  if (errflg) {
    fprintf(stderr,USAGE,argv[0]);
    exit(errflg);
  }

  Print_ODB_env();

  p = strchr(odb_label,'.');
  if (p) *p = '\0';
  ALLOC(p, STRLEN(odb_label) + 3);
  sprintf(p, "%s.c", odb_label);

  /* Make sure env-variable ODB_SRCPATH_<dbname>/ODB_SRCPATH is available */
  Check_Env(1, "ODB_SRCPATH", odb_label, NULL);

  /* Make sure env-variable ODB_DATAPATH_<dbname>/ODB_DATAPATH is available */
  Check_Env(1, "ODB_DATAPATH", odb_label, NULL);

  /* Make sure env-variable ODB_IDXPATH_<dbname>/ODB_IDXPATH is available */
  Check_Env(1, "ODB_IDXPATH", odb_label, "./idx");

  Check_Empty_Tables(odb_label); /* Only applicable, if "-z" option was supplied */

  /* fpdevnull = FOPEN(DEVNULL, "w"); */
  fpdevnull = NULL; /* NULL is accepted, since we use our own ODB_fprintf(), not fprinf() */

  if (verbose) fprintf(stderr,"*** Output source path = '%s'\n",odb_outpath);

  if (ddl_piped) {
    /* .ddl_ -file read/written from/to stdin/stdout */
    fpsrc_file = views_only ? STRDUP("stdin") : STRDUP("stdout");
  }
  else {
    int len = STRLEN(odb_outpath) + STRLEN(odb_label) + 7 + 1;
    ALLOC(fpsrc_file, len);
    snprintf(fpsrc_file, len, "%s/%s.ddl_", odb_outpath, odb_label);
  }

  yyin = FOPEN(odb_source,"r");

  if (views_only) {
    fpsrc = NULL; /* Since we are not writing anything out */
    if (!scan_include(yyin, odb_label)) {
      extern FILE *LEX_open_include(const char *filename);

      if (!ddl_piped) {
	if (!is_regular_file_(fpsrc_file)) {
	  /* Trying "odb_syspath" instead of "odb_outpath" */
	  int len = STRLEN(odb_syspath) + STRLEN(odb_label) + 7 + 1;
	  FREE(fpsrc_file);
	  ALLOC(fpsrc_file, len);
	  snprintf(fpsrc_file, len, "%s/%s.ddl_", odb_syspath, odb_label);
	}
      }

      (void) LEX_open_include(fpsrc_file);
    }
  }
  else {
    fpsrc = FOPEN(fpsrc_file, "w");
  }

  { /* Set defaults */
    /* from yacc.y */
    extern char *YACC_current_dbname;
    extern char *YACC_current_srcpath;
    extern char *YACC_current_datapath;
    extern char *YACC_current_idxpath;
    extern char *YACC_current_poolmask;
    char *env;

    YACC_current_dbname = STRDUP(odb_label);

    env = Check_Env(0, "ODB_SRCPATH", odb_label, NULL);
    YACC_current_srcpath = STRDUP(env);

    env = Check_Env(0, "ODB_DATAPATH", odb_label, NULL);
    YACC_current_datapath = STRDUP(env);

    env = Check_Env(0, "ODB_IDXPATH", odb_label, "./idx");
    YACC_current_idxpath = STRDUP(env);

    env = getenv("ODB_PERMANENT_POOLMASK");
    if (!env) env = "-1";
    YACC_current_poolmask = STRDUP(env);
  }

  if (yyin) { 
    while(!feof(yyin)) {
      int success = yyparse();
      Boolean done = (success != 0);
      if (done) break;
    } /* while(!feof(yyin)) */

    FCLOSE(yyin);
  }
  else {
    PERROR(odb_source);
    errflg++;
  }

  FREE(fpsrc_file);

  if (!errflg) {
    if (fpsrc) {
      fprintf(fpsrc,";\n");
      FCLOSE(fpsrc);
      fpsrc = NULL;
    }
    ODB_link_massage();
  }

  /* Change directory to ODB_OUTPATH for output files */

  if (!errflg) {
    char pold[4096]; 
    getcwd(pold, sizeof(pold));
    if (chdir(odb_outpath) == -1) {
      PERROR(odb_outpath);
      errflg++;
    }
    else {
      if (verbose) {
	char pnew[4096];
	getcwd(pnew, sizeof(pnew));
	if (verbose) fprintf(stderr,"==> Working directory: '%s' --> '%s'\n",pold,pnew);
      }
    }
  }

  if (debug) {
    write_symbol_map();
  }

  /* Generate C-file(s) and compile */

  if (!errflg) {
    ODB_Filelist *flist = genc(views_only);
    ODB_Filelist *x = flist;
    int count = 0;
    char *dbase_object = NULL;
    char *table_obj_stack = NULL;

    if (check_syntax_only) goto out_of_compile;

    if (!x) {
      fprintf(stderr,"*** No files to compile ***\n");
      errflg++;
    }
    else {
      while (x) {
	count++;
	/*
	   fprintf(stderr,"Count#%d: filename=%s, create_so=%d\n",
	   count, x->filename, (int)x->create_so);
	   */
	x = x->next;
      }
    }

    if (views_only) {
      char *dso_file = NULL;
      Boolean found = 0;
      ALLOC(dbase_object, STRLEN(odb_label) + 4);
      sprintf(dbase_object,"%s.so",odb_label);
      found = is_regular_file_(dbase_object);
      if (!found) {
	char *rldpath = getenv("ODB_RLDPATH");
	if (verbose && create_so) {
	  fprintf(stderr,
		  "*** Warning: Not located '%s'\n",
		  dbase_object);
	}
	if (rldpath) {
	  char *token = strtok(rldpath,":");
	  while (!found && token) {
	    char *t = STRDUP(token);
	    char *tcolon = strchr(t,':');
	    if (tcolon) *tcolon = '\0';
	    ALLOC(dso_file, STRLEN(t) + STRLEN(odb_label) + 5);
	    sprintf(dso_file,"%s/%s.so",t,odb_label);
	    FREE(t);
	    found = is_regular_file_(dso_file);
	    if (!found) {
	      if (verbose && create_so) {
		fprintf(stderr,
			"*** Warning: Not located '%s'\n",
			dso_file);
	      }
	    } /* if (!found) */
	    if (!found) FREE(dso_file);
	    token = strtok(NULL,":");
	  }
	} /* if (rldpath) */
	if (found) {
	  FREE(dbase_object);
	  dbase_object = STRDUP(dso_file);
	}
      } /* if (!found) */
      if (create_so) {
	if (!found) {
	  fprintf(stderr,
		  "***Error: Database shareable object '%s' not located\n",
		  dbase_object);
	  errflg++;
	}
	else {
	  fprintf(stderr,
		  "Using database shareable object from '%s'\n",
		  dbase_object);
	}
      } /* if (create_so) */
    }
    else {
      dbase_object = STRDUP("");
    }

    while (flist) {
      char *cmd;
      char *file = flist->filename;
      char *c_file, *o_file, *so_file;

      ALLOC(c_file, STRLEN(file) + 3);
      sprintf(c_file, "%s.c", file);

      ALLOC(o_file, STRLEN(file) + 3);
      sprintf(o_file, "%s.o", file);

      ALLOC(so_file, STRLEN(file) + 4);
      sprintf(so_file, "%s.so", file);

      /* if (verbose) */ fprintf(stderr,"Compiling '%s' ...\n",c_file);

      ALLOC(cmd, 
	    STRLEN(Pwd) + STRLEN(o_file) + 
	    STRLEN(odb_cc) + STRLEN(odb_syspath) + STRLEN(c_file) + 
	    500);

      sprintf(cmd,
	      "%s/bin/rm -f %s;\n%s -c -I %s %s",
	      Pwd, o_file, 
	      odb_cc, odb_syspath, c_file);

      Compile(cmd,0);

      if (!is_regular_file_(o_file)) {
	Boolean problem = 1;
	/* Try to recover from unknown problems (especially with VPP700 cross compilation) */
	/* Re-try #1 */
	sprintf(cmd,
		"%s/bin/rm -f %s;export TMPDIR=.;\n%s -c -I %s -DK0_lo_const %s",
		Pwd, o_file, 
		odb_cc, odb_syspath, c_file);
	Compile(cmd,1);

	if (is_regular_file_(o_file)) 
	  problem = 0; /* Problem cleared */
	else {
	  /* Re-try #2 */
	  sprintf(cmd,
		  "%s/bin/rm -f %s;export TMPDIR=.;\n%s -c -I %s -DK0_lo_var %s",
		  Pwd, o_file, 
		  odb_cc, odb_syspath, c_file);
	  Compile(cmd,2);
	  if (is_regular_file_(o_file)) 
	    problem = 0; /* Problem cleared */
	}

	if (problem) { /* A genuine problem ... sigh !! */
	  fprintf(stderr,"***Error: C-compilation failed for '%s'\n",c_file);
	  PERROR(o_file);
	  errflg++;
	}
      }

      FREE(cmd);

      if (!errflg && create_so && flist->create_so) {
	char *dobj = NULL;

	if (STRLEN(dbase_object) == 0) {
	  FREE(dbase_object);
	  ALLOC(dbase_object, STRLEN(odb_label) + 4);
	  sprintf(dbase_object,"%s.so",odb_label);
	}

	if (strequ(so_file,dbase_object)) {
	  dobj = table_obj_stack ? STRDUP(table_obj_stack) : STRDUP("");
	}
	else {
	  dobj = STRDUP(dbase_object);
	}

	ALLOC(cmd, 
	      STRLEN(so_file) + 
	      STRLEN(odb_ld) + STRLEN(o_file) + 
	      STRLEN(so_file) + STRLEN(odb_libs) +
	      STRLEN(dobj) +
	      50);

	sprintf(cmd,"\n\t%s\n\t/bin/rm -f %s;\n\t%s %s %s -o %s %s",
		Pwd, so_file, 
		odb_ld, o_file, dobj, so_file, odb_libs);

	FREE(dobj);

	if (verbose) fprintf(stderr,"## DSO creation: %s\n",cmd);
	(void)system(cmd);
	FREE(cmd);
	
	if (!is_regular_file_(so_file)) {
	  fprintf(stderr,
		  "***Error: Unable to create shareable object from '%s'\n",
		  o_file);
	  PERROR(so_file);
	  errflg++;
	}
      } /* if (create_so && flist->create_so) */

      if (!errflg && create_so && !flist->create_so && o_file) {
	char *s = NULL;
	int slen = STRLEN(o_file) + 1;
	if (table_obj_stack) slen += STRLEN(table_obj_stack) + 1;
	ALLOC(s,slen);
	if (table_obj_stack) {
	  sprintf(s,"%s %s",table_obj_stack,o_file);
	  FREE(table_obj_stack);
	} else {
	  strcpy(s,o_file);
	}
	table_obj_stack = s;
	if (verbose) fprintf(stderr,"Table object stack : '%s'\n",table_obj_stack);
      }

      FREE(c_file);
      FREE(o_file);
      FREE(so_file);

      flist = flist->next;
    } /* while (flist) */

    FREE(dbase_object);
    FREE(table_obj_stack);
  } /* if (!errflg) */

 out_of_compile: ;

  FCLOSE(fpdevnull);

  if (!debug && errflg) {
    write_symbol_map();
  }

  FREE(original_name);

  /* if (errflg == 0) remove_file_(tmp_file); */
  exit(errflg);

  return errflg;
}

/* Error trapping */

/* Note: there was a #define exit ODB_exit in defs.h ; so all exit()'s call this */
#undef exit
/* Now reverted back to normal exit() */

#if defined(SGI) || defined(VCCPX)
PUBLIC void 
errtra_()
{
  char cmd[1024];
  static char def[] = "/usr/bin/echo where | /usr/bin/dbx -c /dev/null -p";
  char *x = getenv("ODB_ERRTRA_CMD");
  Boolean x_given = 0;
  if (!x) x = def;
  else x_given = 1;
  sprintf(cmd,"%s %d",x,getpid());
  if (x_given) fprintf(stderr,"\nerrtra_(): Executing '%s' ...\n",cmd);
  fflush(stderr);
  fflush(stdout);
  (void)system(cmd);
  fflush(stderr);
  fflush(stdout);
}
#endif

PUBLIC void
ODB_exit(int status)
{ 
#if defined(SGI) || defined(VPPCX)
  if (status != 0) errtra_();
#endif
  if (status != 0) fprintf(stderr,"***Error: Exiting with status = %d\n",status);
  /* else fprintf(stderr,"\nExiting with status = %d\n",status); */
  if (status != 0) {
    extern int gdb_trbk();
    extern FILE *fpinf;
    FILE *fp = NULL;
    if (fpinf) fp = fpinf;
    else if (ddl_piped || ABS(filtered_info) != 0) fp = stdout;
    ODB_fprintf(fp, "\n/error_code=%d\n", status);
    if (!traceback_done) {
      if (!gdb_trbk()) { }
      /* else if (!dbx_tbrk();) { } ... to-be-done */
      traceback_done = 1;
    }
  }
  exit(status);
}

/* GNU-debugger traceback */

#if !defined(GNUDEBUGGER)
#define GNUDEBUGGER /usr/bin/gdb
#endif

#define PRETOSTR(x) #x
#define TOSTR(x) PRETOSTR(x)

PUBLIC
int gdb_trbk()
{
  int invoked = 0;
  char *gdb = getenv("GNUDEBUGGER");
  /* fprintf(stderr,"[1] gdb_trbk(): gdb = '%s'\n",gdb ? gdb : NIL); */
  if (!gdb) gdb = "1";
  /* fprintf(stderr,"[2] gdb_trbk(): gdb = '%s' (exe=%s)\n",gdb ? gdb : NIL, TOSTR(GNUDEBUGGER)); */
  if (gdb && (access(TOSTR(GNUDEBUGGER),X_OK) != 0)) gdb = NULL;
  /* fprintf(stderr,"[3] gdb_trbk(): gdb = '%s'\n",gdb ? gdb : NIL); */
  if (gdb && 
      (strequ(gdb,"1")    || 
       strequ(gdb,"true") || 
       strequ(gdb,"TRUE"))) {
    char gdbcmd[65536];
    pid_t pid = getpid();
    const char *a_out = main_prog ? main_prog : "./odb98.x";
    fprintf(stderr,
	    "[gdb_trbk] : Invoking %s ...\n",
	    TOSTR(GNUDEBUGGER));
    snprintf(gdbcmd,sizeof(gdbcmd),
	     "set +e; /bin/echo '"
	     "set watchdog 1\n"
	     "set confirm off\n"
	     "set pagination off\n"
	     "set print elements 16\n"
	     "set print repeats 3\n"
	     "set print sevenbit-strings on\n"
	     "where\n"
	     "quit\n' > ./gdb_trbk.%d ; "
	     "%s -x ./gdb_trbk.%d -q -n -f -batch %s %d < /dev/null ; "
	     "/bin/rm -f ./gdb_trbk.%d"
	     , pid
	     , TOSTR(GNUDEBUGGER), pid, a_out, pid
	     , pid);
    
    /* fprintf(stderr,"%s\n",gdbcmd); */
    fflush(NULL);
    (void)system(gdbcmd);
    fflush(NULL);
    invoked = 1;
  }
  return invoked;
}

PUBLIC void
ODB_sigexit(int signum)
{
  fprintf(stderr,"***Error: Received signal#%d\n",signum);
  if (!gdb_trbk()) { }
  /* else if (!dbx_tbrk();) { } ... to-be-done */
  traceback_done = 1;
  ODB_exit(signum);
}

