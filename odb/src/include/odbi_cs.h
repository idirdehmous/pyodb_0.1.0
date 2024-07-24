#ifndef _ODBI_CS_H_
#define _ODBI_CS_H_


/*!
    \file odbi_cs.h 
    \brief ODB API include file

     Define functions and subroutines for client/server mode.
*/

/*! Client/Server */

#if defined(__cplusplus)
extern "C" {
#endif

  extern int CS_ODBI_errno;
  extern void CS_ODBI_perror(const char *s);
  extern const char *CS_ODBI_strerror(int errnum);

  extern double CS_ODBI_bind_param
  (void *q, 
   const char *param_name, 
   double new_value);

  extern const char *CS_ODBI_colname
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern const char *CS_ODBI_colnickname
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern const char *CS_ODBI_coltype
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern unsigned int CS_ODBI_coltypenum
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern void *CS_ODBI_connect
  (const char *dbname,
   const char *params, /* More like for use by client */
   ...);

  extern int CS_ODBI_disconnect
  (void *db);

  extern int CS_ODBI_execute
  (void *q);

  extern int CS_ODBI_fetchrow_array
  (void *q,
   int *nrows, 
   int *ncols,
   double d[],
   int nd);

  extern int CS_ODBI_fetchonerow_array
  (void *q,
   int *nrows, 
   int *ncols,
   double d[],
   int nd);

  extern int CS_ODBI_finish
  (void *q);

  extern int CS_ODBI_is_null
  (double d);

  extern int CS_ODBI_io_method
  (void *db);

  extern void CS_ODBI_limits
  (void *q,
   int *start_row,
   int *maxrows,
   int *bufsize);

  extern int CS_ODBI_maxnamelen
  (int new_len);

  extern int CS_ODBI_ncols
  (const void *q);

  extern int CS_ODBI_npools
  (void *db);

  extern void *CS_ODBI_prepare
  (void *db,
   const char *viewname,
   const char *query_string /* For now: applicable only for client */
   );

  extern int CS_ODBI_fetchfile
  (void *q,
   const char *filename,
   const char *fileformat);

  extern void CS_ODBI_printcol
  (FILE *fp,
   void *q,
   int jcol,
   double d,
   const char *delim,
   int print_nulls);

  extern void CS_ODBI_print_db_metadata
  (FILE *fp,
   void *db,
   int complete_info);

  extern void CS_ODBI_print_query_metadata
  (FILE *fp,
   void *q);
			   
  extern void CS_ODBI_swapbytes
  (void *v,      /* a vector of consecutive 2, 4 or 8-byte to be swapped */
   int vlen,
   int elemsize  /* set to 2,4 or 8 => v[] is treated as 2, 4 or 8-byte vector */
   );
  
  extern double CS_ODBI_timer
  (const double *reftime);

#if defined(__cplusplus)
}
#endif
#endif
