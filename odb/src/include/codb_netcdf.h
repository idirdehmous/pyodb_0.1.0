#ifndef _CODB_NETCDF_H_
#define _CODB_NETCDF_H_

/* codb_netcdf.h */

#include "privpub.h"

PUBLIC void codb_has_netcdf_(int *retcode);

PUBLIC void codb_gettype_netcdf_(const char *typename,
				 int *retcode
				 /* Hidden arguments */
				 ,int typename_len);

PUBLIC void dtnum_to_netcdf_type_(const uint *Dtnum, int *retcode);

PUBLIC void codb2netcdf_(const char title[], const char ncfile[], const char namecfg[], const char sql_query[],
			 const double *d, const int *nra, const int *nrows, const int *ncols,
			 const uint idtnum[], const int *ipoolno,
			 const char *odb_type_tag, const char *odb_name_tag, const char *odb_nickname_tag
			 /* Hidden arguments */
			 , int title_len
			 , int ncfile_len
			 , int namecfg_len
			 , int sql_query_len
			 , int odb_type_tag_len
			 , int odb_name_tag_len
			 , int odb_nickname_tag_len
			 );

PUBLIC void codb_open_netcdf_(int *ncid,
			      const char *filename,
			      const char *mode,
			      int *retcode
			      /* Hidden arguments */
			      ,int filename_len
			      ,int mode_len);

PUBLIC void codb_begindef_netcdf_(const int *ncid,
				  const char *title,
				  const char *sql_query,
				  const int *nrows, 
				  const int *ncols,
				  const double *mdi,
				  const int *poolno,
				  int *retcode
				  /* Hidden arguments */
				  , int title_len
				  , int sql_query_len);

PUBLIC void codb_enddef_netcdf_(const int *ncid, 
				int *retcode);

PUBLIC void codb_putheader_netcdf_(const int *ncid,
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
				   ,int long_name_len);

PUBLIC void codb_putdata_netcdf_(const int *ncid,
				 const int *colid,
				 const int *colpack,
				 const double d[],
				 const int *f90_addr,
				 const int *nd,
				 int *retcode);

PUBLIC void codb_close_netcdf_(int *ncid, 
			       int *retcode);

/* To set up the HAS_NETCDF-option properly */

/* If you definitely don't want/have NetCDF then specify

      -DHAS_NETCDF=0

   while you compile, since -UHAS_NETCDF may still pick NetCDF depending on the ARCH (see the test-logic below) */

#undef TEST_HAS_NETCDF

#ifdef HAS_NETCDF

#define TEST_HAS_NETCDF HAS_NETCDF

#else 

/* Add new ARCH here (defined(ARCH)) after checking their implementation on NetCDF exists.
   The new ARCH will then be automatically supported */

#if (defined(LINUX) && !defined(CRAYXT)) || defined(RS6K) || defined(SUN4) || defined(HPPA) || defined(SGI)
#define TEST_HAS_NETCDF 1
#else
#define TEST_HAS_NETCDF 0
#endif

#endif /* HAS_NETCDF */

#if TEST_HAS_NETCDF == 0
#undef HAS_NETCDF
#else
#undef HAS_NETCDF
#define HAS_NETCDF 1
#endif

#undef TEST_HAS_NETCDF

#endif /* _CODB_NETCDF_H_ */
