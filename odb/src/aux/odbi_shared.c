/*!
     \file odbi_shared.c
     \brief ODBI API with functions common to Client/server and Direct access 
     Intended to be include from odbi_direct.c or odbi_client.c or odbi_common.c
 */

#if defined(ODBI_DIRECT) || defined(ODBI_CLIENT)

/*!
  Given an SQL request, it returns the jcol column name with its associated table name (col_name\@table_name)

   @param vq   ODBI query handle i.e. contains information about the current SQL query (read only).
   @param jcol index of the column name. Note: the range is [1..ncols]
   @return The jcol column name for the current SQL query. Please note that it also contains the table name i.e. lat\@hdr and not lat only.
 */
const char *ODBI_colname(const void *vq, 
			 int jcol)
{
  const ODBI_query_t *q = vq;
  const char *colname = NULL;
  DRHOOK_START(ODBI_colname);
  if (q && q->cols && jcol >= 1 && jcol <= q->ncols_all) {
    colname = q->cols[--jcol].name;
  }
  DRHOOK_END(0);
  return colname;
}

/*!
  Given an SQL request, it returns the jcol column nickname. The column nickname is the nickname given in the SQL query (select lat\@hdr as lat) and if not given it returns the column name.

   @param vq   ODBI query handle i.e. contains information about the current SQL query (read only).
   @param jcol index of the column nickname. Note: the range is [1..ncols]
   @return The jcol column nickname for the current SQL query. If no nickname is defined for this column name then it returns the full column name (col_name\@table_name).
 */
const char *ODBI_colnickname(const void *vq, 
			 int jcol)
{
  const ODBI_query_t *q = vq;
  const char *colnickname = NULL;
  DRHOOK_START(ODBI_colnickname);
  if (q && q->cols && jcol >= 1 && jcol <= q->ncols_all) {
    colnickname = q->cols[--jcol].nickname;
  }
  DRHOOK_END(0);
  return colnickname;
}

/*!
  Given an SQL request, it returns the "ascii" type of the jcol column (pk1int, pk9real, string, etc.).
   @param vq   ODBI query handle i.e. contains information about the current SQL query (read only).
   @param jcol index of a column. Note: the range is [1..ncols]
   @return The ODB type of the jcol column (pk1int, pk9real, string, etc.)
 */
const char *ODBI_coltype(const void *vq, 
			 int jcol)
{
  const ODBI_query_t *q = vq;
  const char *coltype = NULL;
  DRHOOK_START(ODBI_coltype);
  if (q && q->cols && jcol >= 1 && jcol <= q->ncols_all) {
    coltype = q->cols[--jcol].type;
  }
  DRHOOK_END(0);
  return coltype;
}

/*!
  Given an SQL request, it returns the type of the jcol column as a numerical value. See include/privpub.h for more details.
   @param vq   ODBI query handle i.e. contains information about the current SQL query (read only).
   @param jcol index of a column. Note: the range is [1..ncols]
   @return The ODB type of the jcol column as a numerical value
 */
unsigned int ODBI_coltypenum(const void *vq, 
			     int jcol)
{
  const ODBI_query_t *q = vq;
  unsigned int coltypenum = 0;
  DRHOOK_START(ODBI_coltypenum);
  if (q && q->cols && jcol >= 1 && jcol <= q->ncols_all) {
    coltypenum = q->cols[--jcol].dtnum;
  }
  DRHOOK_END(0);
  return coltypenum;
}

/*!
  Return the number of columns for a given SQL query
   @param  vq  ODBI query handle i.e. contains information about the current SQL query (read only).
   @return The number of columns for the given SQL query
 */
int ODBI_ncols(const void *vq)
{
  const ODBI_query_t *q = vq;
  int ncols = 0; /* ODB_maxcols() ?? */
  DRHOOK_START(ODBI_ncols);
  if (q) {
    ncols = q->ncols_fixed;
  }
  DRHOOK_END(0);
  return ncols;
}

/*!
  Check whether a value corresponds to NULL or not (compared to $mdi). Please note that this function is not thread safe.
  @param d column value
  @return 1 if d is NULL else 0
 */
int ODBI_is_null(double d)
{
  int is_null;
  static int first_time = 1; /* not thread safe */
  static double nmdi = 0;
  static double rmdi = 0;
  DRHOOK_START(ODBI_is_null);
  if (first_time) {
    pcma_get_mdis_(&nmdi, &rmdi);
    first_time = 0;
  }
  is_null = ((d == rmdi) || (d == nmdi)) ? 1 : 0;
  DRHOOK_END(0);
  return is_null;
}

/*!
  Get the IO method for a given database handle
   @param vdb ODB database handle (ODBI_db_t)
   @return the IO method
 */
int ODBI_io_method(void *vdb)
{
  ODBI_db_t *db = vdb;
  int io_method = 0;
  DRHOOK_START(ODBI_io_method);
  if (db) {
    io_method = db->io_method;
  }
  DRHOOK_END(0);
  return io_method;
}

/*!
  Return the number of pools of a given ODB database
  @param vdb ODB database handle (ODBI_db_t)
  @return The total number of pools
 */
int ODBI_npools(void *vdb)
{
  ODBI_db_t *db = vdb;
  int npools = 0;
  DRHOOK_START(ODBI_npools);
  if (db) {
    npools = db->npools;
  }
  DRHOOK_END(0);
  return npools;
}

/*!
  Swap bytes (conversion little/big endian). 
  @param v a vector of consecutive 2,4 or 8-bytes to be swapped. vlen elements of v will be swapped.
  @param vlen length of v
  @param elemsize set to 2,4 or 8 ==> v[] is treated as 2,4 or 8-byte vector
 */
void ODBI_swapbytes(void *v,
		    int vlen,
		    int elemsize)
{
  DRHOOK_START(ODBI_swapbytes);
  if (v) {
    switch (elemsize) {
    case 2:
      swap2bytes_(v, &vlen);
      break;
    case 4:
      swap4bytes_(v, &vlen);
      break;
    case 8:
      swap8bytes_(v, &vlen);
      break;
    default: /* do nothing */
      break;
    } /* switch (elemsize) */
  }
  DRHOOK_END(0);
}

/*!
  Prints a column value according to its ODB type (make conversion when necessary)
  @param fp File pointer (where to output data)
  @param vq SQL query handle (ODBI_query_t)
  @param jcol Column index to be printed
  @param d value to be printed
  @param delim Delimiter character to use
  @param print_nulls if >0 then prints NULL if d is null (use ODBI_is_null) else prints the integer value $mdi
 */
void ODBI_printcol(FILE *fp,
		   void *vq,
		   int jcol,
		   const double d,
		   const char *delim,
		   int print_nulls)
{
  ODBI_query_t *q = vq;
  DRHOOK_START(ODBI_printcol);
  if (q && q->cols && jcol >= 1 && jcol <= q->ncols_fixed) {
    /* More later */
    unsigned int dtnum = ODBI_coltypenum(q,jcol);
    if (!delim) delim = ",";
    fprintf(fp,"%s",delim);
    if (dtnum == DATATYPE_STRING) {
      int i;
      union {
	double d;
	char s[sizeof(double)+1];
      } u;
      u.d = d;
      for (i=0; i<sizeof(double); i++) {
	int c = u.s[i];
	if (!isprint(c)) u.s[i] = ' '; /* replace non-printables with spaces */
      }
      u.s[sizeof(double)] = '\0';
      fprintf(fp,"\"%s\"",u.s);
    }
    else {
      if (print_nulls && ODBI_is_null(d)) {
	fprintf(fp,"NULL");
      }
      else {
	if (dtnum == DATATYPE_YYYYMMDD) {
	  yyyymmdd x = d;
	  fprintf(fp,"%8.8d",x);
	}
	else if (dtnum == DATATYPE_HHMMSS) {
	  hhmmss x = d;
	  fprintf(fp,"%6.6d",x);
	}
	else {
	  fprintf(fp,"%.14g",d);
	}
      } /* if (print_nulls && ODBI_is_null(d)) */
    }
  }
  DRHOOK_END(0);
}
			  
#include <sys/time.h>

static 
double cs_walltime()
{
  static double time_init = 0;
  double time_in_secs;
  struct timeval tbuf;
  if (gettimeofday(&tbuf,NULL) == -1) perror("CS_WALLTIME");

  if (time_init == 0) time_init =
    (double) tbuf.tv_sec + (tbuf.tv_usec / 1000000.0);

  time_in_secs =
  (double) tbuf.tv_sec + (tbuf.tv_usec / 1000000.0) - time_init;

  return time_in_secs;
}

/*!
  ODB timer (in seconds)
  @param reftime reference time to use to compute the wall clock time
  @return The time difference (in seconds) between the current time and the reference time.
 */
double ODBI_timer(const double *reftime)
{
  double delta = 0;
  DRHOOK_START(ODBI_timer);
  delta = cs_walltime();
  if (reftime) delta -= *reftime;
  DRHOOK_END(0);
  return delta;
}

/* The following will check [when implemented] the value of ODBI_errno */
/*!
  It will print [when implemented] and error message to stderr
  @param s error message to be printed
 */
void ODBI_perror(const char *s)
{
  /* To be implemented */
}

/*!
  It will check [when implemented] the error code (ODBI_error) and return an error message
  @param errnum error code
  @return The error message corresponding to the given error code.
 */
const char *ODBI_strerror(int errnum)
{
  return (const char *)NULL; /* To be implemented */
}


#else

/*!
 A dummy, when compiled alone 
*/

void dummy_ODBI_shared() { }

#endif
