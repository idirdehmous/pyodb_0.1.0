#ifndef _ODBI_H_
#define _ODBI_H_


/*!
    \file odbi.h 
    \brief ODB API include file

     Define functions and subroutines common to client/server and direct access modes.
*/

/*! Both for client/server and direct access */

#if ODBI_CLIENT == 1
/* Client/Server mode (the default, unless ODBI_DIRECT is set to non-zero value) */

#define ODBI_bind_param            CS_ODBI_bind_param
#define ODBI_connect               CS_ODBI_connect
#define ODBI_disconnect            CS_ODBI_disconnect
#define ODBI_execute               CS_ODBI_execute
#define ODBI_fetchrow_array        CS_ODBI_fetchrow_array
#define ODBI_fetchonerow_array     CS_ODBI_fetchonerow_array
#define ODBI_fetchfile             CS_ODBI_fetchfile
#define ODBI_finish                CS_ODBI_finish
#define ODBI_limits                CS_ODBI_limits
#define ODBI_maxnamelen            CS_ODBI_maxnamelen
#define ODBI_prepare               CS_ODBI_prepare
#define ODBI_print_db_metadata     CS_ODBI_print_db_metadata
#define ODBI_print_query_metadata  CS_ODBI_print_query_metadata

  /* Shared routines i.e. source code is identical in direct *and* client/server -mode */

#define ODBI_colname               CS_ODBI_colname
#define ODBI_colnickname           CS_ODBI_colnickname
#define ODBI_coltype               CS_ODBI_coltype
#define ODBI_coltypenum            CS_ODBI_coltypenum
#define ODBI_io_method             CS_ODBI_io_method
#define ODBI_is_null               CS_ODBI_is_null
#define ODBI_ncols                 CS_ODBI_ncols
#define ODBI_npools                CS_ODBI_npools
#define ODBI_printcol              CS_ODBI_printcol
#define ODBI_swapbytes             CS_ODBI_swapbytes
#define ODBI_timer                 CS_ODBI_timer

#define ODBI_errno                 CS_ODBI_errno
#define ODBI_perror                CS_ODBI_perror
#define ODBI_strerror              CS_ODBI_strerror

#endif

#if ODBI_DIRECT == 1
/* Direct mode */

#define ODBI_bind_param            D_ODBI_bind_param
#define ODBI_connect               D_ODBI_connect
#define ODBI_disconnect            D_ODBI_disconnect
#define ODBI_execute               D_ODBI_execute
#define ODBI_fetchrow_array        D_ODBI_fetchrow_array
#define ODBI_fetchonerow_array     D_ODBI_fetchonerow_array
#define ODBI_fetchfile             D_ODBI_fetchfile
#define ODBI_finish                D_ODBI_finish
#define ODBI_limits                D_ODBI_limits
#define ODBI_maxnamelen            D_ODBI_maxnamelen
#define ODBI_prepare               D_ODBI_prepare
#define ODBI_print_db_metadata     D_ODBI_print_db_metadata
#define ODBI_print_query_metadata  D_ODBI_print_query_metadata

/* Shared routines i.e. source code is identical in direct *and* client/server -mode */

#define ODBI_colname               D_ODBI_colname
#define ODBI_colnickname           D_ODBI_colnickname
#define ODBI_coltype               D_ODBI_coltype
#define ODBI_coltypenum            D_ODBI_coltypenum
#define ODBI_io_method             D_ODBI_io_method
#define ODBI_is_null               D_ODBI_is_null
#define ODBI_ncols                 D_ODBI_ncols
#define ODBI_npools                D_ODBI_npools
#define ODBI_printcol              D_ODBI_printcol
#define ODBI_swapbytes             D_ODBI_swapbytes
#define ODBI_timer                 D_ODBI_timer

#define ODBI_errno                 D_ODBI_errno
#define ODBI_perror                D_ODBI_perror
#define ODBI_strerror              D_ODBI_strerror

#endif


#if defined(__cplusplus)
extern "C" {
#endif

  extern int ODBI_errno;
  extern void ODBI_perror(const char *s);
  extern const char *ODBI_strerror(int errnum);

  extern double ODBI_bind_param
  (void *q, 
   const char *param_name, 
   double new_value);

  extern const char *ODBI_colname
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern const char *ODBI_colnickname
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern const char *ODBI_coltype
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern unsigned int ODBI_coltypenum
  (const void *q, 
   int jcol); /* Note: The range is [1..ncols] */

  extern void *ODBI_connect
  (const char *dbname,
   const char *params, /* More like for use by client */
   ...);

  extern int ODBI_disconnect
  (void *db);

  extern int ODBI_execute
  (void *q);

  extern int ODBI_fetchrow_array
  (void *q,
   int *nrows, 
   int *ncols,
   double d[],
   int nd);

  extern int ODBI_fetchonerow_array
  (void *q,
   int *nrows, 
   int *ncols,
   double d[],
   int nd);

  extern int ODBI_finish
  (void *q);

  extern int ODBI_is_null
  (double d);

  extern int ODBI_io_method
  (void *db);

  extern void ODBI_limits
  (void *q,
   int *start_row,
   int *maxrows,
   int *bufsize);

  extern int ODBI_maxnamelen
  (int new_len);

  extern int ODBI_ncols
  (const void *q);

  extern int ODBI_npools
  (void *db);

  extern void *ODBI_prepare
  (void *db,
   const char *viewname,
   const char *query_string /* For now: applicable only for client */
   );

  extern int ODBI_fetchfile
  (void *q,
   const char *filename,
   const char *fileformat);

  extern void ODBI_printcol
  (FILE *fp,
   void *q,
   int jcol,
   double d,
   const char *delim,
   int print_nulls);

  extern void ODBI_print_db_metadata
  (FILE *fp,
   void *db,
   int complete_info);

  extern void ODBI_print_query_metadata
  (FILE *fp,
   void *q);
			   
  extern void ODBI_swapbytes
  (void *v,      /* a vector of consecutive 2, 4 or 8-byte to be swapped */
   int vlen,
   int elemsize  /* set to 2,4 or 8 => v[] is treated as 2, 4 or 8-byte vector */
   );
  
  extern double ODBI_timer
  (const double *reftime);

#if defined(__cplusplus)
}
#endif
#endif
