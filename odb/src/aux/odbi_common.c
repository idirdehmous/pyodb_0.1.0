/*! 
    \file odbi_common.c
    \brief ODB API both for client-server and direct usage

     Contains a set of high-level subroutines to be used to interrogate ODB database (read-only).
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include "privpub.h"
#include "cdrhook.h"

#include "odbi.h"
#include "odbi_direct.h"
#include "odbi_cs.h"
#include "odb.h"
#include "odbi_struct.h"

#define ODBI_DIRECT 1
#define ODBI_CLIENT 1

int ODBI_errno;      /*!< Variable to store ODBI error code. 0 if OK */   


int ODBI_direct = 0; /*!< 1 if local database and 0 otherwise. Default 0 */


/*!
  SET $var = value. Overrides $-variable defaults found in the given SQL request; $ can be missing
  @param q ODBI query i.e. contains all information concerning the current SQL query. 
         q->info may be updated to contain the parameter name (var) and its value
  @param param_name The variable name ($var or var)
  @param new_value The new value for this parameter.
  @return The old value for this parameter or 0 if the variable was never set before.
 */
double ODBI_bind_param
(void *q, 
 const char *param_name, 
 double new_value) {

  if (ODBI_direct)
    return D_ODBI_bind_param(q, param_name, new_value);
  else
    return CS_ODBI_bind_param(q, param_name, new_value);
}


/*!
  Connect to a given a database name and initialise the database structure ODBI_db_t. If the database name contains ":", the client/server mode is disabled and the database will be directly accessed from disk.
  @param dbname The ODB database name. Its form will depend on the access mode:
                - direct access: the database name contains the path to the database. The database name (associated with *.sch file) will be guessed from the path (ex:/hugetmp/data/CCMA --> CCMA database)
                - remote access (client/server mode): the database name can be given in various way:
		    -# odb://host/datapath/DBNAME         
                    -# odb://host:port/datapath/DBNAME    
                    -# host:/datapath in which case the DBNAME is guessed from datapath     
                    -# /datapath in which case the DBNAME is guessed from datapath                       
                    -# DBNAME  in which case the list of parameters "params" is mandatory
   The default dbname is ECMA.
  @param params The list of parameter types to be used to connect to a remote database. It is usually set to NULL in direct mode (no client/server) or if the database name was given as a), b) or c) i.e. containing all information for remote connection. The following can be specified:
     - fp=\%F               a FILE pointer to return output results (associated to /dev/null by default)
     - host=\%s             the remote hostname or its IP address. Default: 127.0.0.1
     - port=\%d             the server port number. Default: mod(uid,10000)+10000)
     - timeout=\%d          timeout in seconds. Default: 3600 
     - datapath=\%s         path to the database. No default.
     - poolmask=\%s         the list of pools to be used (use "," as a separator i.e. "1,2,3") Can be used for both direct and client/server database access.
     Use ";" as a separator i.e. "fp=%F;host=%s;port=%d;timeout=%d;datapath=%s;poolmask=%s"
     Defaults are given in odb/include/odbcsdefs.h
  @param ... Meant to be used for client/server mode and is associated to params (which give the type of the variable argument).
  @return The ODB handle (ODBI_db_t) associated to the connected database
*/
void *ODBI_connect
(const char *dbname,
 const char *params, 
 ...)  {
  FILE *fp = NULL;
  char *server_host = NULL;
  int server_port = 0;
  int server_timeout = 3600;
  char *server_datapath = NULL;
  char *poolmask = strdup(""); 
  char *options = /* compulsory parameters for C/S */
      strdup("fp=%F;host=%s;port=%d;timeout=%d;datapath=%s;poolmask=%s"); 

  ODBI_direct = 1; 

  if (params) {

    va_list ap;

    char *s, *fmt;
    int fmtlen = STRLEN(params) + 1;
  
    if (strstr(params,"host") || strstr(params,"port"))
      ODBI_direct=0;

    va_start(ap, params);
    ALLOC(fmt,fmtlen);
    s = fmt; 
    while (*params) {
      if (!isspace(*params)) *s++ = *params;
      params++;
    }
    *s = '\0';

    {
      const char *delim = ";";
      char *token = strtok(fmt,delim);
    
    /* Read list of arguments */
      while (token) {
	char *ss = STRDUP(token);
	char *t = strchr(ss,'=');
	char *lhs = ss;
	union {
	  char *s;
	  int ii;
	  double dd;
	  FILE *fp;
	} rhs;
	int free_rhss = 0;
	int do_process = 0;
	
	fmt = token;
	if (t) {
	  rhs.s = STRDUP(t+1);
	  do_process = 1;
	  *t = '\0'; /* affects "lhs" */
	}
	if (do_process) {
	  do_process = 0;
	  if (*rhs.s == '%') {
	    char *s = strchr(fmt,'%');
	    if (s) {
	      fmt = s;
	      fmt++;
	      switch (*fmt++) {
	      case 's':
	      FREE(rhs.s);
	      s = va_arg(ap, char *);
	      rhs.s = STRDUP(s);
	       fprintf(stderr,"Vparam(at %%s): '%s'\n",rhs.s); 
	      free_rhss = 1;
	      do_process = 1;
	      break;
	      case 'd':
		FREE(rhs.s);
		rhs.ii = va_arg(ap, int);
		 fprintf(stderr,"Vparam(at %%f): %d\n",rhs.ii); 
		free_rhss = 0;
		do_process = 1;
		break;
	      case 'f':
		FREE(rhs.s);
		rhs.dd = va_arg(ap, double);
		 fprintf(stderr,"Vparam(at %%f): %g\n",rhs.dd); 
		free_rhss = 0;
		do_process = 1;
		break;
	      case 'F':
		FREE(rhs.s);
		rhs.fp = va_arg(ap, FILE *);
		 fprintf(stderr,"Vparam(at %%F): %p, fileno=%d\n",rhs.fp,fileno(rhs.fp)); 
		free_rhss = 0;
		do_process = 1;
		break;
	      } /* end switch *fmt++ */
	    } /* end if s */
	  } /* end if *rhs.s == '%' */
	  if (strcaseequ(lhs,"fp"))              fp                = rhs.fp;
	  else if (strcaseequ(lhs,"host"))       server_host       = STRDUP(rhs.s);
	  else if (strcaseequ(lhs,"port"))       server_port       = rhs.ii;
	  else if (strcaseequ(lhs,"timeout"))    server_timeout    = rhs.ii;
	  else if (strcaseequ(lhs,"datapath"))   server_datapath   = STRDUP(rhs.s);
	  else if (strcaseequ(lhs,"poolmask"))   poolmask          = STRDUP(rhs.s);
	  
	} /* end if do_process */
	if (free_rhss) FREE(rhs.s);
	FREE(ss);
	
	token = strtok(NULL,delim);
	
      } /* while (token) */
    } 
    va_end(ap);
  }  /* if params NOT NULL */
  
  if (strchr(dbname, ':') || ODBI_direct == 0) {
    ODBI_direct = 0; 
    printf("Client/Server \n");
    return (void *) CS_ODBI_connect(dbname,options
		    , fp
		    , server_host
		    , server_port
		    , server_timeout
		    , server_datapath
		    , poolmask );
  } else {  
    ODBI_direct = 1;   
    printf("Direct access \n");
    if (poolmask) 
      setenv("ODB_PERMANENT_POOLMASK",poolmask, 1);
    return (void *) D_ODBI_connect(dbname, NULL );
  }
}


/*!
   Disconnect from a local or remote database and free the ODB handle (ODBI_db_t). 
   @param db Database handle (ODBI_db_t) 
   @return an error code. 0 if OK.
 */
int ODBI_disconnect
(void *db)  {
  if (ODBI_direct)
    return D_ODBI_disconnect(db);
  else
    return CS_ODBI_disconnect(db);
}

/*!
   - In direct mode (fast_odbsql = 1):
     -# When called first time (usually outside ODBI_fetchrow_array), it only initialises q->info (see odb/include/info.h).
     -# When called later (usually in ODBI_fetchrow_array), it executes the SQL request associated to the query handle. 
   - In client/server mode:
     -# Execute the SQL request
  @param q  Query handle (ODBI_query_t) to be executed
  @return an error code. 0 if OK.
 */
int ODBI_execute
(void *q)  {
 if (ODBI_direct)
    return D_ODBI_execute(q);
  else
    return CS_ODBI_execute(q);
}
/*!
   Fetch a given number of rows associated to an existing query handle in an output array.
   @param q      Query handle (ODBI_query_t)
   @param nrows  [in] the number of rows to fetch and [out] the actual number of rows fetched 
   @param ncols  Number of columns for this Query handle
   @param d      Array where the result of the SQL query will be fetched. It has to be allocated and has a minimum size of nrows*ncols (nd) 
   @param nd     Size of d (nrows*ncols)
   @return An error code. 0 if OK.
 */
int ODBI_fetchrow_array
(void *q,
 int *nrows, 
 int *ncols,
 double d[],
 int nd) {
  if (ODBI_direct)
    return D_ODBI_fetchrow_array(q,nrows,ncols,d,nd);
  else
    return CS_ODBI_fetchrow_array(q,nrows,ncols,d,nd);
}

/*!
  - Meaningful for direct access only and when data is fetched pool by pool. It release the current query handle for the current pool and increase the pool number    
  - In Client/server mode, it is not implemented yet.
  @param q Query handle (ODBI_query_t)
  @ return An error code. 0 if OK.
 */
int ODBI_finish 
(void *q) {
 if (ODBI_direct)
    return D_ODBI_finish(q);
  else
    return CS_ODBI_finish(q);
}

/*!
  Set limits for a given query handle
  @param q          Query handle (ODBI_query_t)
  @param start_row  if <=0 then sets the start row of the query handle to 1 else sets to its value
  @param maxrows    if <=0 then sets to ODBI_MAX_INT (2147483647) else sets ot its value
  @param bufsize    Sets the buffer size to be used for SQL retrievals. The minimum size is the number of columns in the SQL query
 */
void ODBI_limits
(void *q,
 int *start_row,
 int *maxrows,
 int *bufsize) {
  if (ODBI_direct)
    D_ODBI_limits(q,start_row,maxrows,bufsize);
  else
    CS_ODBI_limits(q,start_row,maxrows,bufsize);
}


/*!
   Set the maximum name length. Default: 64
   @param new_len Set the maximum name length to new_len
   @return the old value for the maximum length
 */
int ODBI_maxnamelen
(int new_len) {
 if (ODBI_direct)
    return D_ODBI_maxnamelen(new_len);
  else
    return CS_ODBI_maxnamelen(new_len);
}

/*!
  Associate an SQL query to a given ODB database handle and return the associated query handle
  @param db             ODB database handle (ODBI_db_t)
  @param viewname       View name associated to the given SQL query (query_string)
  @param query_string   Query
  @return Query handle
 */
void *ODBI_prepare
(void *db,
 const char *viewname,
 const char *query_string /* For now: applicable only for client */
 ) {
  if (ODBI_direct)
    return (void *) D_ODBI_prepare(db, viewname, query_string);
  else
    return (void *) CS_ODBI_prepare(db, viewname, query_string);
}

/*!
  Write the result of the SQL query associated to the given query handle in an output file. The output format is given as an argument of this function. Available output format:
      - default           Ascii where field delimiter is space (" "), and output is rather compressed
      - binary            binary format will be used for output
      - plotobs           $ODB_PLOTTER compatible binary file for Magics will be created
      - wplotobs          $ODB_PLOTTER compatible binary file for Magics will be created (wind-arrows)
      - odbtool           input(s) for IDL/odbtool (by Phil Watts while at ECMWF, now in Eumetsat) will be created
      - odbtk             field delimiters become "!,!" and unique column ids ($uniq#) will be printed as "zero-th" column
      - odb               "odbviewer"/"odbless" .rpt-file format is emulated 
      - netcdf            create NetCDF-file with ODB Conventions (lat/lon are in degrees)
      - unetcdf           as "netcdf" above, but create without packing (lat/lon are in degrees)
      - dump              similar to "default", but delimiter is comma "," (lat/lon are in degrees)
      - bindump           similar to "binary", but all data will be printed row_wise + metadata & title-structure always embedded (lat/lon are in degrees)
      - geo               (standard) Metview GEO-points file with "lat lon level date time value"
      - geo:xyv           GEO-points file with "x/long y/lat value"
      - geo:xy_vector     GEO-points file with "lat lon height date time u v"
      - geo:polar_vector  GEO-points file with "lat lon height date time speed direction"

  @param q           Query handle (ODBI_query_t)
  @param filename    output filename
  @param fileformat  output format. The default output format is "default"
  @return Error code. 0 if OK.
 */
int ODBI_fetchfile
(void *q,
 const char *filename,
 const char *fileformat) {
  if (ODBI_direct)
    return D_ODBI_fetchfile(q, filename, fileformat);
  else
    return CS_ODBI_fetchfile(q, filename, fileformat);
}

/*!
   Write metadata of a given ODB database handle
   @param fp             File pointer (where to print outputs)
   @param db             ODB database handle (ODBI_db_t)
   @param complete_info  write metadata only if complete_info is not null
 */
void ODBI_print_db_metadata
(FILE *fp,
 void *db,
 int complete_info) {
  if (ODBI_direct)
    D_ODBI_print_db_metadata(fp, db, complete_info);
  else
    CS_ODBI_print_db_metadata(fp, db, complete_info);
}

/*!
  Write metadata of a given query handle
  @param fp File pointer  (where to print outputs)
  @param q  Query handle
 */
void ODBI_print_query_metadata
(FILE *fp,
 void *q) {
  if (ODBI_direct)
    D_ODBI_print_query_metadata(fp, q);
  else
    CS_ODBI_print_query_metadata(fp, q);
}

#include "odbi_shared.c"
