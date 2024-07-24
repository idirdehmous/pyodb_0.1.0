#ifndef _ODBI_DIRECT_H_
#define _ODBI_DIRECT_H_


/*!
    \file odbi_direct.h 
    \brief ODB API include file

     Define functions and subroutines for direct access modes.
*/

/*! Direct access */

#if defined(__cplusplus)
extern "C" {
#endif

  extern int D_ODBI_errno;
  extern void D_ODBI_perror(const char *s);
  extern const char *D_ODBI_strerror(int errnum);

  extern double D_ODBI_bind_param
  (void *q, 
   const char *param_name, 
   double new_value);

  extern const char *D_ODBI_colname
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern const char *D_ODBI_colnickname
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern const char *D_ODBI_coltype
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern unsigned int D_ODBI_coltypenum
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern void *D_ODBI_connect
  (const char *dbname,
   const char *params, /* More like for use by client */
   ...);

  extern int D_ODBI_disconnect
  (void *db);

  extern int D_ODBI_execute
  (void *q);

  extern int D_ODBI_fetchrow_array
  (void *q,
   int *nrows, 
   int *ncols,
   double d[],
   int nd);

  extern int D_ODBI_fetchonerow_array
  (void *q,
   int *nrows, 
   int *ncols,
   double d[],
   int nd);

  extern int D_ODBI_finish
  (void *q);

  extern int D_ODBI_is_null
  (double d);

  extern int D_ODBI_io_method
  (void *db);

  extern void D_ODBI_limits
  (void *q,
   int *start_row,
   int *maxrows,
   int *bufsize);

  extern int D_ODBI_maxnamelen
  (int new_len);

  extern int D_ODBI_ncols
  (const void *q);

  extern int D_ODBI_npools
  (void *db);

  extern void *D_ODBI_prepare
  (void *db,
   const char *viewname,
   const char *query_string /* For now: applicable only for client */
   );

  extern int D_ODBI_fetchfile
  (void *q,
   const char *filename,
   const char *fileformat);

  extern void D_ODBI_printcol
  (FILE *fp,
   void *q,
   int jcol,
   double d,
   const char *delim,
   int print_nulls);

  extern void D_ODBI_print_db_metadata
  (FILE *fp,
   void *db,
   int complete_info);

  extern void D_ODBI_print_query_metadata
  (FILE *fp,
   void *q);
			   
  extern void D_ODBI_swapbytes
  (void *v,      /* a vector of consecutive 2, 4 or 8-byte to be swapped */
   int vlen,
   int elemsize  /* set to 2,4 or 8 => v[] is treated as 2, 4 or 8-byte vector */
   );
  
  extern double D_ODBI_timer
  (const double *reftime);

#if defined(__cplusplus)
}
#endif
#endif
