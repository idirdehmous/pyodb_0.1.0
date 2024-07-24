/* 
   ODB to NETCDF -interface (SS/7-Jul-2003)

   Only applicable if you compile with -DHAS_NETCDF=1 or -DHAS_NETCDF
   To disable use of NetCDF, compile with -DHAS_NETCDF=0

*/

#include "odb.h"
#include "cmaio.h"
#include "codb_netcdf.h"

#ifdef HAS_NETCDF /* NETCDF is available */

#include "netcdf.h"

/* Define some macros */

#define CHKERR(do_abort) check_err(do_abort, stat, __LINE__, __FILE__)

#define ASSIGN_FV_MDI(type, functype, nct, value) { \
  type fillvalue = ((*colpack) == 1) ? (*mdi) : value; \
  stat = nc_put_att_##functype(*ncid, colid, _FillValue, nct, 1, &fillvalue); \
  CHKERR(1); \
  stat = nc_put_att_double(*ncid, colid, "missing_value", NC_DOUBLE, 1, mdi); \
  CHKERR(1); \
}

#define VIACOPY_TO_NETCDF(type,functype,x) { \
  int j; \
  type fillvalue; \
  type *array = NULL; \
  stat = nc_get_att_##functype(*ncid, *colid, _FillValue, &fillvalue); \
  CHKERR(1); \
  if (debug_mode) fprintf(stderr,"\t%s = %.13g\n", _FillValue, (double)fillvalue); \
  ALLOC(array, count); \
  for (j=0; j<count; j++) { \
    array[j] = (ABS(x[j]) == ABS(mdi)) ? fillvalue : x[j]; \
  } \
  stat = nc_put_vara_##functype(*ncid, *colid, &start, &count, array); \
  CHKERR(1); \
  FREE(array); \
}  

#define RAW_TO_NETCDF(type,functype,x) { \
  stat = nc_put_vara_##functype(*ncid, *colid, &start, &count, (const type *)x); \
  CHKERR(1); \
}

#define RAW_TO_NETCDF_VAR(type,functype,x) { \
  stat = nc_put_var_##functype(*ncid, *colid, (const type *)x); \
  CHKERR(1); \
}

static int debug_mode = 0;

static int check_debug_mode()
{
  static int first_time = 1;
  if (first_time) {
    char *env = getenv("ODB_NETCDF_DEBUG");
    if (env) debug_mode = atoi(env);
    debug_mode = (debug_mode > 0) ? 1 : 0;
    first_time = 0;
  }
  return debug_mode;
}

static void
check_err(const int   do_abort,
	  const int   stat, 
	  const int   line, 
	  const char *file)
{
  if (stat != NC_NOERR) {
    fprintf(stderr,
	    "***NetCDF-%s detected in line=%d, file=%s: %s\n",
	    do_abort ? "error" : "warning",
	    line, file, nc_strerror(stat));
    if (do_abort) RAISE(SIGABRT);
  }
}

static char *make_printable(const char s[], int slen)
{
  int j;
  char *p = NULL;
  ALLOC(p, slen + 1);
  for (j=0; j<slen; j++) {
    int c = s[j];
    if (!isprint(c)) c = ' ';
    p[j] = c;
  }
  p[slen] = '\0';
  return p;
}

void
codb_has_netcdf_(int *retcode) 
{ 
  *retcode = 1; 
}

void codb_gettype_netcdf_(const char *typename,
			  int *retcode
			  /* Hidden arguments */
			  ,int typename_len)
{
  int rc;
  char *p;
  DECL_FTN_CHAR(typename);

  ALLOC_FTN_CHAR(typename);
  p = p_typename;
  while (*p) {
    if (isupper(*p)) *p = tolower(*p);
    p++;
  }

  if      (strequ(p_typename,"char"))   rc = NC_CHAR;
  else if (strequ(p_typename,"byte"))   rc = NC_BYTE;
  else if (strequ(p_typename,"short"))  rc = NC_SHORT;
  else if (strequ(p_typename,"int"))    rc = NC_INT;
  else if (strequ(p_typename,"float"))  rc = NC_FLOAT;
  else if (strequ(p_typename,"double")) rc = NC_DOUBLE;
  else rc = NC_EBADTYPE;

  FREE_FTN_CHAR(typename);
  *retcode = rc;
}

void codb_open_netcdf_(int *ncid,
		       const char *filename,
		       const char *mode,
		       int *retcode
		       /* Hidden arguments */
		       ,int filename_len
		       ,int mode_len)
{
  int stat = NC_NOERR;
  int write_mode;
  char *truename = NULL;
  DECL_FTN_CHAR(mode);

  (void) check_debug_mode();

  ALLOC_FTN_CHAR(mode);

  truename = IOtruename(filename,&filename_len);
  write_mode = (*p_mode == 'w' || *p_mode == 'W');

  if (write_mode) {
    if (debug_mode) fprintf(stderr,"Creating NetCDF-file '%s' ...\n",truename);
    stat = nc_create(truename, NC_CLOBBER, ncid);
  }
  else {
    if (debug_mode) fprintf(stderr,"Opening NetCDF-file '%s' ...\n",truename);
    stat = nc_open(truename, NC_NOWRITE, ncid); /* read/only */
  }
  CHKERR(1);

  *retcode = stat;

  FREE(truename);

  FREE_FTN_CHAR(mode);
}

void codb_begindef_netcdf_(const int *ncid,
			   const char *title,
			   const char *sql_query,
			   const int *nrows, 
			   const int *ncols,
			   const double *mdi,
			   const int *poolno,
			   int *retcode
			   /* Hidden arguments */
			   , int title_len
			   , int sql_query_len)
{
  int rc = 0;
  int stat, dimid_rows, dimid_cols, dimid_str;

  /* nrows = number of data rows (elements in every column) */
  /* stat = nc_def_dim(*ncid, "nrows", *nrows, &dimid); */
  stat = nc_def_dim(*ncid, "nrows", NC_UNLIMITED, &dimid_rows);
  CHKERR(1);
  rc = dimid_rows;

  /* ncols = number of variables/columns */
  stat = nc_def_dim(*ncid, "ncols", *ncols, &dimid_cols);
  CHKERR(1);

  /* nstr = number of bytes per in character strings (fixed to 8 bytes) */
  stat = nc_def_dim(*ncid, "nstr", sizeof(string), &dimid_str);
  CHKERR(1);

  /* global metadata */

  {
    const char Conventions[] = "ODB";
    stat = nc_put_att_text(*ncid, NC_GLOBAL, "Conventions", STRLEN(Conventions), Conventions);
    CHKERR(1);
  }

  {
    DECL_FTN_CHAR(title);
    ALLOC_FTN_CHAR(title);
    stat = nc_put_att_text(*ncid, NC_GLOBAL, "title", STRLEN(p_title), p_title);
    CHKERR(1);
    FREE_FTN_CHAR(title);
  }

  {
    DECL_FTN_CHAR(sql_query);
    ALLOC_FTN_CHAR(sql_query);
    if (STRLEN(p_sql_query) > 0) {
      /* Change all unprintable as well as \t's & \n's into blanks ' ' */
      char *p = p_sql_query;
      while (*p) {
	int c = *p;
	if (!isprint(c) || c == '\n' || c == '\n') c = ' ';
	*p++ = c;
      }
      stat = nc_put_att_text(*ncid, NC_GLOBAL, "sql_query", STRLEN(p_sql_query), p_sql_query);
      CHKERR(1);
    }
    FREE_FTN_CHAR(sql_query);
  }

  {
    const char institution[] = "ECMWF, Reading, U.K.";
    stat = nc_put_att_text(*ncid, NC_GLOBAL, "institution", STRLEN(institution), institution);
    CHKERR(1);
  }

  {
    char *version = STRDUP(nc_inq_libvers());
    const char *odbvers = codb_versions_(NULL, NULL, NULL, NULL);
    const char *dtbuf = odb_datetime_(NULL, NULL);
    char *source;
    int len;
    len = STRLEN(version) + STRLEN(odbvers) + STRLEN(dtbuf) + 50;
    ALLOC(source,len);
    snprintf(source, len,
	    "ODB2NETCDF (%s) on %s ; NetCDF %s",
	    odbvers, dtbuf, version);
    stat = nc_put_att_text(*ncid, NC_GLOBAL, "source", STRLEN(source), source);
    CHKERR(1);
    FREE(version);
    FREE(source);
  }

  { /* Missing data indicator ; 
       don't use "missing_value", since ncview emits this as the 
       only true missing value, which is wrong for packed data */
    stat = nc_put_att_double(*ncid, NC_GLOBAL, "mdi", NC_DOUBLE, 1, mdi);
    CHKERR(1);
  }

  { /* Pool number */
    stat = nc_put_att_int(*ncid, NC_GLOBAL, "poolno", NC_INT, 1, poolno);
    CHKERR(1);
  }

  { /* Analysis date & time, if available ;
       If not present, sets to 19700101 at 000000 */
    int yyyymmdd_loc, hhmmss_loc;
    codb_analysis_datetime_(&yyyymmdd_loc, &hhmmss_loc);
    stat = nc_put_att_int(*ncid, NC_GLOBAL, "analysis_date", NC_INT, 1, &yyyymmdd_loc);
    CHKERR(1);
    stat = nc_put_att_int(*ncid, NC_GLOBAL, "analysis_time", NC_INT, 1, &hhmmss_loc);
    CHKERR(1);
  }

  { /* Creation date & time */
    /* NEC_LG: with PGI 10.9, yyyymmdd and hhmmss conflict with
     * yyyymmdd/hhmmss defined types in privpub.h
     */
    int yyyymmdd_loc, hhmmss_loc;
    codb_datetime_(&yyyymmdd_loc, &hhmmss_loc);
    stat = nc_put_att_int(*ncid, NC_GLOBAL, "creation_date", NC_INT, 1, &yyyymmdd_loc);
    CHKERR(1);
    stat = nc_put_att_int(*ncid, NC_GLOBAL, "creation_time", NC_INT, 1, &hhmmss_loc);
    CHKERR(1);
  }

  { /* Jobid, experiment version & owner & user, if available */
    char chpid[100];
    char *env;
    env = getenv("ODB_NETCDF_JOBID");
    if (!env) {
      snprintf(chpid,sizeof(chpid),"%d",(int)getpid());
      env = chpid;
    }
    if (env) {
      char *jobid = STRDUP(env);
      stat = nc_put_att_text(*ncid, NC_GLOBAL, "jobid", STRLEN(jobid), jobid);
      CHKERR(1);
      FREE(jobid);
    }
    env = getenv("EXPVER");
    if (env) {
      char *expver = STRDUP(env);
      stat = nc_put_att_text(*ncid, NC_GLOBAL, "expver", STRLEN(expver), expver);
      CHKERR(1);
      FREE(expver);
    }
    env = getenv("OWNER");
    if (env) {
      char *owner = STRDUP(env);
      stat = nc_put_att_text(*ncid, NC_GLOBAL, "owner", STRLEN(owner), owner);
      CHKERR(1);
      FREE(owner);
    }
    env = getenv("USER");
    if (env) {
      char *user = STRDUP(env);
      stat = nc_put_att_text(*ncid, NC_GLOBAL, "user", STRLEN(user), user);
      CHKERR(1);
      FREE(user);
    }
    else {
      env = getenv("LOGNAME");
      if (env) {
	char *logname = STRDUP(env);
	stat = nc_put_att_text(*ncid, NC_GLOBAL, "user", STRLEN(logname), logname);
	CHKERR(1);
	FREE(logname);
      }
    }
  }

  *retcode = rc; /* Dimension id of "nrows" */
}

void codb_enddef_netcdf_(const int *ncid, 
			 int *retcode)
{
  int stat = nc_enddef(*ncid);
  CHKERR(1);
  *retcode = stat;
}

void codb_putheader_netcdf_(const int *ncid,
			    const int *colnum,

			    const char *odb_type,
			    const char *odb_name,
			    const char *odb_nickname,
			    const char *mapname,
			    const char *long_name,
			    const char *units,
			    
			    const double *mdi,
			    const int *coltype,
			    const int *colpack,
			    const double *scale_factor,
			    const double *add_offset,
			    
			    int *retcode
			    /* Hidden arguments */
			    ,int odb_type_len
			    ,int odb_name_len
			    ,int odb_nickname_len
			    ,int mapname_len
			    ,int units_len
			    ,int long_name_len)
{
  DECL_FTN_CHAR(odb_type);
  DECL_FTN_CHAR(odb_name);
  DECL_FTN_CHAR(odb_nickname);
  DECL_FTN_CHAR(mapname);
  DECL_FTN_CHAR(long_name);
  DECL_FTN_CHAR(units);
  int ndims = (*coltype == NC_CHAR) ? 2 : 1;
  int dimid[2];
  int colid;
  int stat;
  int has_ll = 0;
  static char *odb_lat = NULL;
  static char *odb_lon = NULL;

  if (!odb_lat) {
    char *env = getenv("ODB_LAT");
    odb_lat = env ? STRDUP(env) : STRDUP("lat@hdr");
  }
  if (!odb_lon) {
    char *env = getenv("ODB_LON");
    odb_lon = env ? STRDUP(env) : STRDUP("lon@hdr");
  }

  stat = nc_inq_dimid(*ncid, "nrows", &dimid[0]);
  CHKERR(1);

  if (ndims == 2) {
    stat = nc_inq_dimid(*ncid, "nstr", &dimid[1]);
    CHKERR(1);
  }

  ALLOC_FTN_CHAR(odb_type);
  ALLOC_FTN_CHAR(odb_name);
  ALLOC_FTN_CHAR(odb_nickname);
  ALLOC_FTN_CHAR(mapname);
  ALLOC_FTN_CHAR(long_name);
  ALLOC_FTN_CHAR(units);

  {
    {
      char p[30];
      snprintf(p, sizeof(p), "col_%d", *colnum);
      stat = nc_def_var(*ncid, p, *coltype, ndims, dimid, &colid);
      CHKERR(1);
    }

    if (STRLEN(p_long_name) > 0 && !strequ(p_long_name,"-")) {
      stat = nc_put_att_text(*ncid, colid, "long_name", STRLEN(p_long_name), p_long_name);
      CHKERR(1);
    }

    stat = nc_put_att_text(*ncid, colid, "odb_name", STRLEN(p_odb_name), p_odb_name);
    CHKERR(1);

    if (STRLEN(p_odb_nickname) > 0 && !strequ(p_odb_name,p_odb_nickname)) {
      has_ll = (strequ(p_odb_nickname, odb_lat) || strequ(p_odb_nickname, odb_lon)) ? 1 : 0;
      stat = nc_put_att_text(*ncid, colid, "odb_nickname", STRLEN(p_odb_nickname), p_odb_nickname);
    }
    else {
     has_ll = (strequ(p_odb_name, odb_lat) || strequ(p_odb_name, odb_lon)) ? 1 : 0;
     stat = nc_put_att_text(*ncid, colid, "odb_nickname", STRLEN(p_odb_name), p_odb_name);
    }
    CHKERR(0); /* bail out on error */

    stat = nc_put_att_text(*ncid, colid, "odb_type", STRLEN(p_odb_type), p_odb_type);
    CHKERR(1);

    if (*colpack == 1 || *coltype != NC_CHAR) {
      switch (*coltype) {
      case NC_CHAR: /* Comes here only with *colpack == 1 */
	{
	  char *s = NULL;
	  const char fillvalue[] = " ";
	  stat = nc_put_att_text(*ncid, colid, _FillValue, STRLEN(fillvalue), fillvalue);
	  CHKERR(1);
	  s = make_printable((const char *)mdi, sizeof(*mdi));
	  stat = nc_put_att_text(*ncid, colid, "missing_value", STRLEN(s), s);
	  CHKERR(1);
	  FREE(s);
	}
	break;
      case NC_BYTE:
	ASSIGN_FV_MDI(signed char, schar, NC_BYTE, -127-1);
	break;
      case NC_SHORT:
	ASSIGN_FV_MDI(short, short, NC_SHORT, -32767-1);
	break;
      case NC_INT:
	ASSIGN_FV_MDI(int, int, NC_INT, -2147483647-1);
	break;
      case NC_FLOAT:
	ASSIGN_FV_MDI(float, float, NC_FLOAT, 1.7e+38);
	break;
      case NC_DOUBLE:
	ASSIGN_FV_MDI(double, double, NC_DOUBLE, *mdi);
	break;
      }
    }

    if (*colpack == 2 || *colpack == 3) {
      stat = nc_put_att_double(*ncid, colid, "scale_factor", NC_DOUBLE, 1, scale_factor);
      CHKERR(1);
      stat = nc_put_att_double(*ncid, colid, "add_offset", NC_DOUBLE, 1, add_offset);
      CHKERR(1);
      stat = nc_put_att_int(*ncid, colid, "colpack", NC_INT, 1, colpack);
      CHKERR(1);
    }

    if (STRLEN(p_units) > 0 && !strequ(p_units,"-")) {
      stat = nc_put_att_text(*ncid, colid, "units", STRLEN(p_units), p_units);
      CHKERR(1);
    }
    else if (has_ll) {
      const char Units[] = "degrees";
      stat = nc_put_att_text(*ncid, colid, "units", STRLEN(Units), Units);
      CHKERR(1);
    }
  }

  /* finish: */
  FREE_FTN_CHAR(odb_type);
  FREE_FTN_CHAR(odb_name);
  FREE_FTN_CHAR(odb_nickname);
  FREE_FTN_CHAR(mapname);
  FREE_FTN_CHAR(long_name);
  FREE_FTN_CHAR(units);

  *retcode = colid;
}

/* put to NetCDF */ 

static void pack4cdf_short(const int *ncid,
			   const int *colid,
			   size_t start,
			   const double d[],
			   size_t count,
			   double mdi)
{
  int j;
  short *pk = NULL;
  double scale_factor, add_offset;
  short fillvalue;
  int stat;

  stat = nc_get_att_short(*ncid, *colid, _FillValue, &fillvalue);
  CHKERR(1);
  if (debug_mode) fprintf(stderr,"\t%s = %.13g\n", _FillValue, (double)fillvalue);

  stat = nc_get_att_double(*ncid, *colid, "scale_factor", &scale_factor);
  CHKERR(1);
  if (debug_mode) fprintf(stderr,"\tscale_factor = %.14g\n", scale_factor);

  stat = nc_get_att_double(*ncid, *colid, "add_offset", &add_offset);
  CHKERR(1);
  if (debug_mode) fprintf(stderr,"\tadd_offset = %.14g\n", add_offset);

  ALLOC(pk, count);
  for (j=0; j<count; j++) {
    double tmp = d[j];
    if (ABS(tmp) == ABS(mdi)) {
      tmp = fillvalue;
    }
    else {
      tmp = tmp - add_offset;
      tmp /= scale_factor;
    }
    pk[j] = tmp;
  }

  RAW_TO_NETCDF(short,short,pk);

  FREE(pk);
}

static void pack4cdf_byte(const int *ncid,
			  const int *colid,
			  size_t start,
			  const double d[],
			  size_t count,
			  double mdi)
{
  int j;
  signed char *pk = NULL;
  double scale_factor, add_offset;
  signed char fillvalue;
  int stat;

  stat = nc_get_att_schar(*ncid, *colid, _FillValue, &fillvalue);
  CHKERR(1);
  if (debug_mode) fprintf(stderr,"\t%s = %.13g\n", _FillValue, (double)fillvalue);

  stat = nc_get_att_double(*ncid, *colid, "scale_factor", &scale_factor);
  CHKERR(1);
  if (debug_mode) fprintf(stderr,"\tscale_factor = %.14g\n", scale_factor);

  stat = nc_get_att_double(*ncid, *colid, "add_offset", &add_offset);
  CHKERR(1);
  if (debug_mode) fprintf(stderr,"\tadd_offset = %.14g\n", add_offset);

  ALLOC(pk, count);
  for (j=0; j<count; j++) {
    double tmp = d[j];
    if (ABS(tmp) == ABS(mdi)) {
      tmp = fillvalue;
    }
    else {
      tmp = tmp - add_offset;
      tmp /= scale_factor;
    }
    pk[j] = tmp;
  }

  RAW_TO_NETCDF(signed char,schar,pk);

  FREE(pk);
}

void codb_putdata_netcdf_(const int *ncid,
			  const int *colid,
			  const int *colpack,
			  const double d[],
			  const int *f90_addr,
			  const int *nd,
			  int *retcode)
{
  size_t start = (*f90_addr) - 1;
  size_t count = *nd;
  int id;
  double mdi;
  nc_type coltype;
  int stat;

  if (debug_mode) fprintf(stderr,"Processing colid %d ...\n", *colid);

  stat = nc_inq_vartype(*ncid, *colid, &coltype);
  CHKERR(1);

  if (debug_mode) fprintf(stderr,"\tcolpack = %d\n", *colpack);

  if (coltype != NC_CHAR) {
    stat = nc_get_att_double(*ncid, *colid, "missing_value", &mdi);
    CHKERR(1);
    if (debug_mode) fprintf(stderr,"\tmdi = %.14g\n", mdi);
  }

  if (*colpack == 2) {
    pack4cdf_short(ncid,colid,start,d,count,mdi);
  }
  else if (*colpack == 3) {
    pack4cdf_byte(ncid,colid,start,d,count,mdi);
  }
  else if (*colpack == 0) {
    switch (coltype) {
    case NC_CHAR:
      {
	char *s = NULL;
	count *= sizeof(string);
	s = make_printable((const char *)d,count);
	RAW_TO_NETCDF_VAR(char,text,s);
	FREE(s);
	count /= sizeof(string);
      }
      break;
    case NC_BYTE:
      VIACOPY_TO_NETCDF(signed char,schar,d);
      break;
    case NC_SHORT:
      VIACOPY_TO_NETCDF(short,short,d);
      break;
    case NC_INT:
      VIACOPY_TO_NETCDF(int,int,d);
      break;
    case NC_FLOAT:
      VIACOPY_TO_NETCDF(float,float,d);
      break;
    case NC_DOUBLE:
      RAW_TO_NETCDF(double,double,d);
      break;
    default:
      fprintf(stderr,
	      "***Warning: Unknown type = %d for NetCDF variable id = %d\n",
	      coltype, *colid);
      stat = NC_EBADTYPE;
      CHKERR(0); /* bail out on error */
      count = 0;
      break;
    }
  }
  else { /* *colpack == 1 --> do nothing */
    fprintf(stderr,
	    "***Warning: Unsupported packing = %d for NetCDF variable id = %d, type = %d\n",
	    *colpack, *colid, coltype);
    count = 0;
  }

 finish:
  *retcode = count;
}

void codb_close_netcdf_(int *ncid, 
			int *retcode)
{
  int stat = nc_close(*ncid);
  CHKERR(1);
  *ncid = -1;
  *retcode = stat;
}

#else /* NETCDF is NOT available */

void
codb_has_netcdf_(int *retcode) 
{ 
  *retcode = 0; 
}

void codb_gettype_netcdf_(const char *typename,
			  int *retcode
			  /* Hidden arguments */
			  ,int typename_len)
{
  *retcode = -1;
}

void codb_open_netcdf_(int *ncid,
		       const char *filename,
		       const char *mode,
		       int *retcode
		       /* Hidden arguments */
		       ,int filename_len
		       ,int mode_len)
{
  *ncid = -1;
  *retcode = -1;
}

void codb_begindef_netcdf_(const int *ncid,
			   const char *title,
			   const char *sql_query,
			   const int *nrows, 
			   const int *ncols,
			   const double *mdi,
			   const int *poolno,
			   int *retcode
			   /* Hidden arguments */
			   , int title_len
			   , int sql_query_len)
{
  *retcode = -1;
}

void codb_enddef_netcdf_(const int *ncid, 
			 int *retcode)
{
  *retcode = -1;
}

void codb_putheader_netcdf_(const int *ncid,
			    const int *colnum,

			    const char *odb_type,
			    const char *odb_name,
			    const char *odb_nickname,
			    const char *mapname,
			    const char *long_name,
			    const char *units,
			    
			    const double *mdi,
			    const int *coltype,
			    const int *colpack,
			    const double *scale_factor,
			    const double *add_offset,
			    
			    int *retcode
			    /* Hidden arguments */
			    ,int odb_type_len
			    ,int odb_name_len
			    ,int odb_nickname_len
			    ,int mapname_len
			    ,int units_len
			    ,int long_name_len)
{
  *retcode = -1;
}

void codb_putdata_netcdf_(const int *ncid,
			  const int *colid,
			  const int *colpack,
			  const double d[],
			  const int *f90_addr,
			  const int *nd,
			  int *retcode)
{
  *retcode = -1;
}

void codb_close_netcdf_(int *ncid, 
			int *retcode)
{
  *ncid = -1;
  *retcode = -1;
}

#endif
