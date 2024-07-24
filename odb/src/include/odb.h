#ifndef _ODB_H_
#define _ODB_H_

#include <stdio.h>
#include <ctype.h>
#include <string.h>
#ifdef USE_MALLOC_H
#include <malloc.h>
#endif
#include "ecstdlib.h"
#include <unistd.h>
#include <math.h>
#include <limits.h>
#include <errno.h>
#include <time.h>
#include <signal.h>
#include <stdarg.h>

#include "alloc.h"
#include "swapbytes.h"
#include "magicwords.h"
#include "dca.h"
#include "bits.h"
#include "pcma_extern.h"
#include "privpub.h"
#ifdef VPP
#pragma global noalias
#pragma global novrec
#elif defined(NECSX)
#pragma cdir options -pvctl,nodep
#endif

#define ODB_COMPILER_DEFAULT "'ODB_COMPILER undefined : '"

#define MAXVARLEN 128

#define MAXTRACEARGS 16

#define PKRATIO(in,out) (((in) != 0) ? ((1 - ((double)(out)/(double)(in))) * 100.0) : 0)

#define UNUSED "Unused"

#define INFOLEN 7

#define SPACE(fp,n) { int l; for (l=0; l<(n); l++) fprintf(fp," "); }
#define TAB(fp) fprintf(fp,"  ")
#define NL(fp)  fprintf(fp,"\n")

#define FORPOOL for (p = first_pool; p; p = p->next)

#define POOLREG_DEF  ODB_Pool *p, *sp, *ep
#define POOLREG_FOR \
  sp = AnyPool ? NULL : get_poolreg(Handle,Poolno); \
  ep = sp ? sp : NULL; \
  sp = sp ? sp : first_pool; \
  for (p = sp ; p ; p = p->next)
#define POOLREG_BREAK if (ep) break

#define FORFUNC for (pf = p->funcs; pf != NULL; pf = pf->next)

#define PFCOM pf->common

#define HASHFUNC(h,u,hashsize) ((u)%(hashsize) + 31U * (h))

#define Func_uintequal(x,y) \
  (ODB_uint_equal((const uint *)&(x),(const uint *)&(y),sizeof(x)/sizeof(uint)))

#define Func_twindow(tgdate, tgtime, andate, antime, lmargin, rmargin) \
  ODB_twindow(tgdate, tgtime, andate, antime, lmargin, rmargin)
#define Func_tdiff(tgdate, tgtime, andate, antime) \
  ODB_tdiff(tgdate, tgtime, andate, antime)

extern double ODB_datenum(double Date);
extern void  codb_datenum_(const double *Date, double *Result);
#define Func_datenum(tgdate) ODB_datenum(tgdate)

extern double ODB_hours_utc(double Time);
extern void  codb_hours_utc_(const double *Time, double *Result);
#define Func_hours_utc(tgtime) ODB_hours_utc(tgtime)

extern double ODB_hour_angle(double Time);
extern void codb_hour_angle_(double *Time, double *Result);
#define Func_ha(tgtime) ODB_time_angle(tgtime)

extern double ODB_time_angle(double Time, double lon);
extern void codb_time_angle_(double *Time, double *lon, double *Result);
#define Func_ta(tgtime, lon) ODB_time_angle(tgtime, lon)

extern double ODB_solar_declination(double Date, double Time);
extern void codb_solar_declination_(double *Date, double *Time, double *Result);
#define Func_sda(tgdate, tgtime) ODB_solar_declination(tgdate, tgtime)

extern double ODB_solar_elevation(double Date, double Time, double lat, double lon);
extern void codb_solar_elevation_(double *Date, double *Time, double *lat, double *lon, double *Result);
#define Func_sela(tgdate, tgtime, lat, lon) ODB_solar_elevation(tgdate, tgtime, lat, lon)

extern double ODB_solar_zenith(double Date, double Time, double lat, double lon);
extern void codb_solar_zenith_(double *Date, double *Time, double *lat, double *lon, double *Result);
#define Func_sza(tgdate, tgtime, lat, lon) ODB_solar_zenith(tgdate, tgtime, lat, lon)

extern double ODB_daynight(double Date, double Time, double lat, double lon);
extern void codb_daynight_(double *Date, double *Time, double *lat, double *lon, double *Result);
#define Func_daynight(tgdate, tgtime, lat, lon) ODB_daynight(tgdate, tgtime, lat, lon)

extern double ODB_solar_azimuth(double Date, double Time, double lat, double lon);
extern void codb_solar_azimuth_(double *Date, double *Time, double *lat, double *lon, double *Result);
#define Func_saza(tgdate, tgtime, lat, lon) ODB_solar_azimuth(tgdate, tgtime, lat, lon)


extern double ODB_duint(double d);
extern double ODB_dfloat(double d);

#define Func_int(x) trunc(x)
#define Func_nint(x) F90nint(x)
#define Func_uint(x) ODB_duint(x)
#define Func_float(x) ODB_dfloat(x)
#define Func_dble(x) ((double)(x))
#define Func_floor(x) floor(x)
#define Func_trunc(x) trunc(x)
#define Func_ceil(x) ceil(x)
#define Func_ldexp(x,n) ldexp(x,n)
#define Func_touch(x) (1)
#define Func_maxcount(x) ((Count + tmpcount)<(x))
#define Func_maxrows() (Count + tmpcount)
#define Func_abs(x)   ABS(x)

/* Used in generic.c */
extern double ODB_maxcount(double x);
extern double ODB_maxrows();

/*
  These two (min/max) are obsolete as of 27-Mar-2006 by SS.
  Now available are variable length min/max or minval/maxval
#define Func_max(x,y) MAX(x,y)
#define Func_min(x,y) MIN(x,y)
*/
#define Func_mod(x,y) fmod(x,y)
#define Func_pow(x,y) ((y) == 2 ? (x)*(x) : pow(x,y))

/* Note: ODB's trigonometric funcs require args in degrees 
   and return degrees (where applicable) */

#define Func_cos(x) cos(D2R(x))
#define Func_sin(x) sin(D2R(x))
#define Func_tan(x) tan(D2R(x))
#define Func_acos(x) R2D(acos(x))
#define Func_asin(x) R2D(asin(x))
#define Func_atan(x) R2D(atan(x))
#define Func_atan2(x,y) R2D(atan2(x,y))

#define Func_exp(x) exp(x)
#define Func_cosh(x) cosh(x)
#define Func_sinh(x) sinh(x)
#define Func_tanh(x) tanh(x)
#define Func_sqrt(x) sqrt(x)
#define Func_log(x) log(x)
#define Func_ln(x)  log(x)
#define Func_log10(x) log10(x)
#define Func_lg(x)    log10(x)
#define Func_ibits(x,pos,len) GET_BITS(x, pos, len) 

#define Func_year(x)   ((ABS(x) != ABS(RMDI)) ? (double)((int)((x)/10000)) : (double)NMDI)
#define Func_month(x)  ((ABS(x) != ABS(RMDI)) ? (double)(((int)((x)/100))%100) : (double)NMDI)
#define Func_day(x)    ((ABS(x) != ABS(RMDI)) ? (double)(((int)(x))%100) : (double)NMDI)
#define Func_hour(x)   ((ABS(x) != ABS(RMDI)) ? (double)((int)((x)/10000)) : (double)NMDI)
#define Func_minute(x) ((ABS(x) != ABS(RMDI)) ? (double)(((int)((x)/100))%100) : (double)NMDI)
#define Func_minutes(x) Func_minute(x)
#define Func_second(x) ((ABS(x) != ABS(RMDI)) ? (double)(((int)(x))%100) : (double)NMDI)
#define Func_seconds(x) Func_second(x)

extern double ODB_jd(double Date, double Time);
extern void  codb_jd_(const double *Date, const double *Time, double *Result);
#define Func_julian_date ODB_jd
#define Func_jd          ODB_jd


#define Func_circle(x,x0,y,y0,r) (Func_pow(x-x0,2) + Func_pow(y-y0,2) <= Func_pow(r,2))
/* (((x)-(x0))*((x)-(x0)) + ((y)-(y0))*((y)-(y0)) <= (r)*(r)) */
/* NPEs-variable below defined inside the generated C-code and denotes no. of pools */
/*AF 07/05/09 remove test <= NPEs
#define Func_paral(PEvar, targetPE) \
( ( (PEvar) <= 0 ) || \
  ( (targetPE) >= 1 && (targetPE) <= NPEs && (((int)(targetPE)-1)%NPEs + 1) == (PEvar) ) \
)
*/
#define Func_paral(PEvar, targetPE) \
( ( (PEvar) <= 0 ) || \
  ( (targetPE) >= 1 && (((int)(targetPE)-1)%NPEs + 1) == (PEvar) ) \
)


/* Special for the observation handling */

#define Func_rad(reflat, reflon, refdeg, obslat, obslon) \
(acos( \
      Func_cos(reflat) * Func_cos(obslat) * Func_cos(obslon-reflon) + \
      Func_sin(reflat) * Func_sin(obslat) ) <= D2R(refdeg))

#define Func_distance(reflat, reflon, obslat, obslon) \
(R_Earth * acos( \
      Func_cos(reflat) * Func_cos(obslat) * Func_cos(obslon-reflon) + \
      Func_sin(reflat) * Func_sin(obslat) ))

#define Func_dist(reflat, reflon, refdist_km, obslat, obslon) \
(R_Earth_km * acos( \
      Func_cos(reflat) * Func_cos(obslat) * Func_cos(obslon-reflon) + \
      Func_sin(reflat) * Func_sin(obslat) ) <= (refdist_km))

#define Func_km(reflat, reflon, obslat, obslon) \
(R_Earth_km * acos( \
      Func_cos(reflat) * Func_cos(obslat) * Func_cos(obslon-reflon) + \
      Func_sin(reflat) * Func_sin(obslat) ))

extern double ODB_rotlat(double lat, double lon, double polelat, double polelon);
extern double ODB_rotlon(double lat, double lon, double polelat, double polelon);

#define Func_rotlat(lat,lon,polelat,polelon) ODB_rotlat(lat,lon,polelat,polelon)
#define Func_rotlon(lat,lon,polelat,polelon) ODB_rotlon(lat,lon,polelat,polelon)

extern double ODB_lldegrees(double d);
extern double ODB_llradians(double d);
extern double ODB_degrees(double d);
extern double ODB_radians(double d);
extern void codb_init_latlon_rad_();
extern int codb_change_latlon_rad_(const int *newvalue);

/* The following two rely on env-variable $ODB_LATLON_RAD */
#define Func_lldegrees(x)    ODB_lldegrees(x)
#define Func_llradians(x)    ODB_llradians(x)

/* The following two rely convert unconditionally [use inline macros here for performance gain] */
#define Func_degrees(x) R2D(x) /* ODB_degrees(x) */
#define Func_radians(x) D2R(x) /* ODB_radians(x) */

#define Func_lon0to360(x) Func_mod((x)+360e0,360e0)

/* Aggregate functions */

#define Func__min(x)   (x)
#define Func__max(x)   (x)

#define Func__density(resol) (resol)
#define Func__count(x)     (1)
#define Func__bcount(x)   ((x)?1:0)
#define Func__sum(x)       (x)
#define Func__avg(x)       (x)
#define Func__median(x)    (x)
#define Func__stdev(x)     (x)
#define Func__var(x)       (x)
#define Func__rms(x)       (x)
#define Func__dotp(x,y)   ((x) * (y))
#define Func__norm(x,y)   Func__dotp(x,y)

#define Func__count_distinct(x)   (x)
#define Func__bcount_distinct(x)  (x)
#define Func__sum_distinct(x)     Func__sum(x)
#define Func__avg_distinct(x)     Func__avg(x)
#define Func__median_distinct(x)  Func__median(x)
#define Func__stdev_distinct(x)   Func__stdev(x)
#define Func__var_distinct(x)     Func__var(x)
#define Func__rms_distinct(x)     Func__rms(x)
#define Func__dotp_distinct(x,y)  Func__dotp(x,y)
#define Func__norm_distinct(x,y)  Func__dotp(x,y)

#define Take2Arg(x,y) ((ArgNo == 1) ? (x) : (y))

#define Func__covar(x,y)           Take2Arg(x,y)
#define Func__corr(x,y)            Take2Arg(x,y)
#define Func__linregr_a(x,y)       Take2Arg(x,y)
#define Func__linregr_b(x,y)       Take2Arg(x,y)

#define Func__maxloc(x,y)          Take2Arg(x,y)
#define Func__minloc(x,y)          Take2Arg(x,y)

/* End of aggregate functions */

extern double ODB_boxlat(double lat, double deltalat);
extern double ODB_boxlon(double lon, double deltalon);
extern double ODB_boxid_lat(double lat, double deltalat);
extern double ODB_boxid_lon(double lon, double deltalon);
extern double ODB_boxid(double lat, double lon, double deltalat, double deltalon);

#define Func_boxlat(lat,deltalat) ODB_boxlat(lat, deltalat)
#define Func_boxlon(lon,deltalon) ODB_boxlon(lon, deltalon)
#define Func_boxid_lat(lat,deltalat) ODB_boxid_lat(lat, deltalat)
#define Func_boxid_lon(lon,deltalon) ODB_boxid_lon(lon, deltalon)
#define Func_boxid(lat,lon,deltalat,deltalon) ODB_boxid(lat,lon,deltalat,deltalon)

extern double ODB_eq_resol(double rn);
extern double ODB_eq_n(double resol);
extern double ODB_eq_min_resol();
extern double ODB_eq_max_n();

#define Func_eq_resol(n) ODB_eq_resol(n)
#define Func_eq_n(resol) ODB_eq_n(resol)
#define Func_eq_min_resol ODB_eq_min_resol
#define Func_eq_max_n ODB_eq_max_n

extern double ODB_eq_boxid(double lat, double lon, double rn);
extern double ODB_eq_boxlat(double lat, double lon, double rn);
extern double ODB_eq_boxlon(double lat, double lon, double rn);
extern double ODB_eq_boxid_with_resol(double lat, double lon, double resol);
extern double ODB_eq_boxlat_with_resol(double lat, double lon, double resol);
extern double ODB_eq_boxlon_with_resol(double lat, double lon, double resol);

#define Func_eq_boxid(lat,lon,resol) ODB_eq_boxid(lat,lon,ODB_eq_n(resol))
#define Func_eq_boxlat(lat,lon,resol) ODB_eq_boxlat(lat,lon,ODB_eq_n(resol))
#define Func_eq_boxlon(lat,lon,resol) ODB_eq_boxlon(lat,lon,ODB_eq_n(resol))

extern double ODB_eq_area(double rn);
extern double ODB_eq_area_with_resol(double resol);
#define Func_eq_area(resol) ODB_eq_area(ODB_eq_n(resol))

extern double ODB_eq_truearea(double lat, double lon, double rn);
extern double ODB_eq_truearea_with_resol(double lat, double lon, double resol);
#define Func_eq_truearea(lat, lon, resol) ODB_eq_truearea(lat, lon, ODB_eq_n(resol))

extern double ODB_eq_latband(double bandnum, double rn);
extern double ODB_eq_latband_with_resol(double bandnum, double resol);
#define Func_eq_latband(bandnum, resol) ODB_eq_latband(bandnum, ODB_eq_n(resol))

extern double ODB_rgg_boxid(double lat, double lon, double Txxxx);
extern double ODB_rgg_boxlat(double lat, double lon, double Txxxx);
extern double ODB_rgg_boxlon(double lat, double lon, double Txxxx);
extern double ODB_rgg_resol(double Txxxx);

#define Func_rgg_boxid  ODB_rgg_boxid
#define Func_rgg_boxlat ODB_rgg_boxlat
#define Func_rgg_boxlon ODB_rgg_boxlon
#define Func_rgg_resol ODB_rgg_resol

#define Func_sign(x) ((x) < 0 ? -1 : 1)

extern double ODB_binlo(double x, double deltax);
extern double ODB_binhi(double x, double deltax);

#define Func_binlo(x,dx) ODB_binlo(x,dx)
#define Func_binhi(x,dx) ODB_binhi(x,dx)

#define Func_cmp(x,y) ((double)((x < y) ? -1 : ((x > y) ? +1 : 0)))

#define Func_k2c(kelvin)      ((kelvin) - ZERO_POINT)
#define Func_c2k(celsius)     ((celsius) + ZERO_POINT)
#define Func_c2f(celsius)     ((((double)9)*(celsius))/((double)5) + ((double)32))
#define Func_f2c(fahrenheit)  (((fahrenheit) - (double)32)*((double)5))/((double)9)
#define Func_k2f(kelvin)      (Func_c2f(Func_k2c(kelvin)))
#define Func_f2k(fahrenheit)  (Func_c2k(Func_f2c(fahrenheit)))

extern double ODB_timestamp(double indate, double intime);
#define Func_tstamp(d,t) ODB_timestamp(d,t)

extern double ODB_basetime(double indate, double intime);
#define Func_basetime(d,t) ODB_basetime(d,t)

extern double ODB_now();
extern double ODB_date_now();
extern double ODB_time_now();

#define Func_now ODB_now
#define Func_date_now ODB_date_now
#define Func_time_now ODB_time_now

extern void fodb_register_in_vector_(void *d, const int *nd, const int *is_int, double *addr);
extern void fodb_unregister_in_vector_(double *addr);
extern double ODB_in_vector(double var, double addr);
#define Func_in_vector(var,addr) ODB_in_vector(var,addr)

#define Func_debug_print odb_debug_print_

extern double ODBoffset(double colvar, double offset);
#define Func_offset ODBoffset

extern double ODB_pi();
extern double ODB_speed(double u, double v);
extern double ODB_dir(double u, double v);
extern double ODB_ucom(double dd, double ff);
extern double ODB_vcom(double dd, double ff);

#define Func_pi    ODB_pi
#define Func_speed ODB_speed
#define Func_ff    ODB_speed
#define Func_uv2ff ODB_speed
#define Func_dir   ODB_dir
#define Func_dd    ODB_dir
#define Func_uv2dd ODB_dir
#define Func_ucom  ODB_ucom
#define Func_ucomp ODB_ucom
#define Func_u     ODB_ucom
#define Func_vcom  ODB_vcom
#define Func_vcomp ODB_vcom
#define Func_v     ODB_vcom

extern double ODB_myproc();
extern double ODB_nproc();
extern double ODB_pid();
extern double ODB_tid();
extern double ODB_nthreads();
extern void init_proc_funcs();

extern void init_RANDOM();
extern double ODB_seed(double myseed);
extern double ODB_random();
#define Func_random ODB_random
#define Func_seed(myseed) ODB_seed(myseed)

extern double ODB_InGenList(double target, double begin, double end, double step);
#define Func_InGenList    ODB_InGenList

extern double ODB_Infile(const char *filename, double arg);
extern double ODBinfile(double filename, double arg);
#define Func_InFile    ODB_InFile

extern double ODB_NotInFile(const char *filename, double arg);
extern double ODBnotinfile(double filename, double arg);
#define Func_NotInFile ODB_NotInFile

extern boolean odb_debug_print_(const double arg);

#define Func_thin      ODB_thin
extern double ODB_thin(const int n, /* double key, double Every_nth, */ ...);
extern double ODB_vthin(const int n, const double d[]);
extern void codb_thin_init_();
extern void codb_thin_reset_(const int *it);

#define Func_max    ODB_maxval
extern double ODB_maxval(const int n, ...);

#define Func_min    ODB_minval
extern double ODB_minval(const int n, ...);

#define Func_cksum  ODB_Cksum32
extern double ODB_Cksum32(const int n, ...);

/* End */

extern void
exec_dummy(int dummy, ...);

extern void 
cdummy_load_();

extern FILE *
CMA_get_fp(const int *unit);

extern void 
cma_open_(int            *unit, 
	  const char     *filename,
	  const char     *mode,
	  int            *retcode,	 
	  /* Hidden arguments to Fortran-program */
	  int             len_filename,
	  int             len_mode);

extern void 
cma_close_(const int *unit, int *retcode);

extern int
ODB_error(const char *where,
	  int how_much,
	  const char *what,
	  const char *srcfile,
	  int srcline,
	  int perform_abort);

/* The next 3 are meant to be Fortran-callable */

extern void
newio_start32_(const char *dbname,
	       const int *handle, const int *maxhandle, const int *io_method_input,
               const int *ntables, const int *is_new, const int *is_readonly,
	       const int *glbNpools, const int *locNpools, const int poolidx[],
               int *rc
	       /* Hidden argument */
	       , int dbname_len);

extern void
newio_flush32_(const int *handle,
	       const int *poolno,
	       const int *enforce_to_disk,
	       const int *ntables,
	       /* Tables in format "/@table1/@tables2/.../"
		  For all tables (currently in memory), use "*" or put *ntables to -1 */
	       const char table[],
	       int *rc
	       /* Hidden argument */
	       , int table_len);

extern void
newio_end32_(const int *handle, int *rc);

extern void
newio_get_incore32_(const int *handle,
		    const int *poolno, /* must be a specific pool ; -1 doesn't do */
		    char data[],       /* data area where incore data will be memcpy'ed to */
		    const int *nbytes, /* size of supplied data bytes array */
		    const int *reset_updated_flag, /* if == 1 --> reset updated-flag to 0 */
		    const char *table, /* particular table name (both "@table" or "table" do) */
		    int *rc
		    /* Hidden argument */
		    , int table_len);

extern void
newio_put_incore32_(const int *handle,
		    const int *poolno, /* must be a specific pool ; -1 doesn't do */
		    const char data[], /* data area from which data is copied into the incore */
		    const int *nbytes, /* amount of data */
		    const int *nrows,  /* no. of rows in data-matrix */
		    const int *ncols,  /* no. of cols in data-matrix */
		    const char *table, /* particular table name (both "@table" or "table" do) */
		    int *rc
		    /* Hidden argument */
		    , int table_len);

extern void /* Just in case we need to know this from Fortran */
newio_dimlen32_(int *rc);

extern void
newio_status_incore32_(const int *handle,
		       const int *poolno,  /* must be a specific pool ; -1 doesn't do */
		             int status[], /* status[]-array */
		       const int *nstatus, /* no. of words in status[]-array */
		       const char *table, /* particular table name (both "@table" or "table" do) */
		             int *rc       /* no. of words filled/returned */
		       /* Hidden argument */
		       , int table_len);


extern void
newio_release_pool32_(const int *handle,
		      const int *poolno,  /* must be a specific pool ; -1 doesn't do */
		      int *rc);

extern int
newio_Read32(int *fp_idx, 
	     const char *filename, 
	     const char *desc, 
	     const char *entry,
	     int handle,
	     int poolno,
	     void *data, 
	     int sizeof_data, 
	     int nelem);

extern int
newio_Write32(int *fp_idx, 
	      const char *filename, 
	      const char *desc, 
	      const char *entry,
	      int handle,
	      int poolno,
	      const void *data, 
	      int sizeof_data, 
	      int nelem);

extern int
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
	     int infolen);

extern int
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
	      );

extern int
newio_Size32(const char *filename,
	     const char *dbname,
	     const char *table,
	     int handle,
	     int poolno);

extern int
newio_GetByteswap(int *fp_idx, int handle, int poolno);

extern int
newio_SetByteswap(int *fp_idx, int toggle, int handle, int poolno);


extern void 
dd2ddl_(const char *dd_in,
	const char *ddl_out
	/* Hidden arguments */
	,int dd_in_len
	,int ddl_out_len
	);

#define ALLOCMORE(type,name,table,x,xlen,size) REALLOC(x,size)

#ifndef MIN_ALLOC
#define MIN_ALLOC 1
#endif

#ifndef INC_ALLOC
#define INC_ALLOC 1
#endif

#define FREE_data(Var)     { FREE((Var).d); (Var).dlen = 0; (Var).nalloc = 0; }
#define FREE_pkdata(Var)   { FREE((Var).pd); (Var).pdlen = 0; (Var).is_packed = 0; }
#define FREE_savelist(Var) { FREE((Var).saved_data); (Var).saved_data_nbytes = 0; (Var).savelist = -1; }
#define FREE_alldata(Var)  { FREE_data(Var); FREE_pkdata(Var); FREE_savelist(Var); }

#define LINKOFFSET(s) Link2##s##_offset
#define LINKLEN(s)    Link2##s##_length

#define CHECK_PDS_ERROR(x) \
  if (!PDS->on_error) Nbytes += BYTESIZE(PDS->pd); else { RAISE(SIGABRT); return -(x); }

#define BW(bs, x) ((bs && Is_bitmap) ? MAXBITS : (x))

#define FLAG_UPDATE(x) ((x) & 1U)
#define FLAG_FETCH(x)  ((x) & 1U)
#define FLAG_PACK(x)   ((x) & 2U)
#define FLAG_FREE(x)   ((x) & 4U)

#define PRINT_BITMAP(x)   ((x) & 1U)
#define PRINT_BITFIELD(x) ((x) & 2U)
#define PRINT_COLWISE(x)  ((x) & 4U)

typedef struct _ODB_Setvar {
  char *symbol;
  double value;
} ODB_Setvar;

typedef struct _ODB_PE_Info {
  double *addr;
  char   *varname;
  int     varname_len;
  int     npes;
  int    *nrowvec;
  int     replicate_PE;
} ODB_PE_Info;

typedef struct _ODB_Tags {
  char  *name;
  int    is_usddothash;
  int    nmem;
  char **memb;
} ODB_Tags;

typedef enum { 
  preptag_name    = 0x1,
  preptag_type    = 0x2,
  preptag_extname = 0x4,
  preptag_exttype = 0x8,
  preptag_tblname = 0x16
} _ODB_PrepTag_Types;

typedef struct _ODB_PrepTags { 
  /* Prepared tags; for use by codb_getnames_() */
  int    tagtype;      /* OR'red _ODB_PrepTag_Types */
  int    longname_len; /* strlen() of longname */
  char  *longname;     /* the "/xxx/yyy/zzz/" */
} ODB_PrepTags;

typedef struct _ODB_Trace {
  int handle;
  int mode;
  char *msg;
  int msglen;
  int numargs;
  int args[MAXTRACEARGS];
} ODB_Trace;

typedef struct _ODB_InterMed {
  double **d;
  int nr;
  int nc;
} ODB_InterMed;

typedef struct _ODB_Pool {
  int               handle;
  int               inuse;
  char             *dbname;
  char             *srcpath;
  int               poolno;
  struct _ODB_PoolMask *pm;
  void             (*add_var)(const char *dbname, 
			      const char *symbol, 
			      const char *viewname,
			      int it,
			      double addr);
  int              (*load)(struct _ODB_Pool *pool, int io_method);
  int              (*store)(const struct _ODB_Pool *pool, int io_method);
  int               nfuncs;
  struct _ODB_Funcs *funcs;
  struct _ODB_Pool *next;
} ODB_Pool;

typedef struct _ODB_CommonFuncs {
  char *name;
  int is_table;
  int is_considered; /* From ODB_CONSIDER_TABLES; by default considered i.e. == 1 */
  int is_writable; /* From ODB_WRITE_TABLES; by default writable i.e. == 1 */
  int ntables; /* Number of tables in view's ODB_CONSIDER_TABLES or 0 for tables */
  int ncols;
  int ncols_aux; /* Number of auxiliary columns not in SELECT; tot. columns = ncols */
  int tableno;  /* Table's order number as it appears in the data layout file ("how many'th table") */
  int rank;  /* Rank in TABLE hierarchy */
  double wt; /* Hierarchy weight */
  const ODB_Tags *tags;
  const ODB_PrepTags *preptags;
  int ntag;
  int npreptag;
  int nmem;
  int has_select_distinct; /* > 0 for UNIQUEBY, < 0 for SELECT DISTINCT, 0 otherwise */
  int has_usddothash; /* Has "$<parent_table>.<child_table>" -variables in SELECT */
  int create_index; /* 0=Normal SELECT stmt, 1=CREATE UNIQUE INDEX, 2=CREATE BITMAP INDEX */
  void *Info; /* Alias info_t: A future extension to allow "dynamic" queries via 'odbsql' */
  void *(*init)(void *p, struct _ODB_Pool *pool, int is_new, int io_method, int it, int add_vars);
  void  (*swapout)(void *p);
  void  (*dim)(void *p, int *nrows, int *ncols, int *nrowoffset, int procid);
  int  *(*sortkeys)(void *p, int *nsortkeys);
  int   (*update_info)(void *p, const int ncols, int can_UPDATE[]);
  int   (*aggr_info)(void *p, const int ncols, int aggr_func_flag[]);
  int   (*select)(void *p, ODB_PE_Info *PEinfo, int phase, void *feedback);
  void  (*peinfo)(void *p, ODB_PE_Info *PEinfo);
  void  (*cancel)(void *p);
  /* 64-bit floating point values */
  int   (*dget)(void *p,       double d[], 
	        int ldimd, int nrows, int ncols, int procid, const int flag[], int row_offset);
  int   (*dput)(void *p, const double d[], 
	        int ldimd, int nrows, int ncols, int procid, const int flag[]);
  int   (*remove)(void *p);
  int   (*load)(void *p);
  int   (*store)(void *p);
  int   (*pack)(void *p);
  int   (*unpack)(void *p);
  int   *(*getindex)(void *p, const char *table, int *lenidx);
  int   (*putindex)(void *p, const char *table, int lenidx, int idx[], int by_address);
  int   (*sql)(FILE *fp, int mode, const char *prefix, const char *postfix, char **sqlout);
  int   (*colaux)(void *p, int colaux[], int colaux_len);
} ODB_CommonFuncs;

typedef struct _ODB_Funcs {
  int   it; /* virtual thread id: 0 for TABLEs, 1..N for VIEWs */
  void *data;
  void *Res;  /* Alias result_t : A future extension to allow "dynamic" queries via 'odbsql' */
  struct _ODB_InterMed *tmp; /* Intermediate data used by aggregate functions f.ex. */
  struct _ODB_Pool *pool;
  struct _ODB_CommonFuncs *common;
  struct _ODB_Funcs *next;
} ODB_Funcs;

typedef struct _ODB_PoolMask {
  int handle;
  int npools;     /* No. of pools (fixed per handle) */
  int inumt;      /* No. of threads (the N below) */
  int **poolmask; /* For each thread it (1..N):
                     Entry [it-1][0] ==> poolmask set (=1), not set (=0; the default)
	 	     Entries [it-1][1..npools] : =1 if particular pool is to be INCLUDED
		                                 =0 if particular pool is to be EXCLUDED
		  */
  char *dbname;
} ODB_PoolMask;

#define MAX_ODB_GLOBAL_LOCKS_OPENMP 4

#if defined(ODB_POOLS)
ODB_Pool *first_pool = NULL;
ODB_Pool *last_pool = NULL;
o_lock_t ODB_global_mylock[MAX_ODB_GLOBAL_LOCKS_OPENMP] = { 0 }; /* Specific OMP-locks; GLOBAL-variable; initialized only once in
							     odb/lib/codb.c, routine codb_init_omp_locks_() */
#else
extern ODB_Pool *first_pool;
extern ODB_Pool *last_pool;
extern o_lock_t ODB_global_mylock[MAX_ODB_GLOBAL_LOCKS_OPENMP];  /* Specific OMP-locks; GLOBAL-variable; initialized only once in
							     odb/lib/codb.c, routine codb_init_omp_locks_() */
#endif

extern int
ODB_savelist(const char *dbname, /* NULL or f.ex. "ECMA" */
	     const char *name);  /* Like "colname@tablename" */

typedef struct _ODB_Anchor_Funcs {
  int  (*create_funcs)(ODB_Pool *pool, int is_new, int io_method, int it);
  int  (*load)(ODB_Pool *pool, int io_method);
  int  (*store)(const ODB_Pool *pool, int io_method);
} ODB_Anchor_Funcs;

extern void
codb_set_entrypoint_(const char *dbname,
		     /* Hidden arguments */
		     int dbname_len);

extern void
ODB_add2funclist(const char *dbname, 
		 void (*func)(),
		 int funcno);

#define ODB_ANCHOR(db) \
  { \
    extern void db##_print_flags_file(); \
    extern ODB_Funcs *Anchor2##db(void *, ODB_Pool *, int *, int it, int add_vars); \
    ODB_addstatfunc(#db, NULL, Anchor2##db); \
    ODB_add2funclist(#db, db##_print_flags_file, 1); \
  }

#define ODB_ANCHOR_VIEW(db, view) \
  { \
    extern ODB_Funcs *Anchor2##db##_##view(void *, ODB_Pool *, int *, int it, int add_vars); \
    ODB_addstatfunc(#db, #view, Anchor2##db##_##view); \
  }

extern void 
codb_save_peinfo_(const int *vhandle,
		  const int *replicate_PE, 
		  const int *npes);

extern void 
codb_restore_peinfo_(const int *vhandle,
		     int *replicate_PE, 
		     int *npes);

extern ODB_Pool *
get_poolreg(int handle, int poolno);

extern void
put_poolreg(int handle, int poolno, ODB_Pool *pool);

extern ODB_Funcs *
get_forfunc(int handle, 
	    const char *dbname,
	    int poolno, 
	    int it, 
	    const char *dataname,
	    int do_abort);

extern int 
put_forfunc(ODB_Funcs *thisfunc, 
	    int handle, 
	    const char *dbname,
	    int poolno, 
	    int it, 
	    const char *dataname);

extern void
nullify_forfunc(int handle, 
		const char *dbname,
		int poolno, 
		int it, 
		const char *dataname);

extern void
print_forfunc_(const int *enforce,
	       const char *msg,
	       /* Hidden arguments */
	       int msg_len);

extern void
ctxreg_(const char *dbname,
	const char *retr,
	const int  *version,
	const int  *ctxid,
	int *retcode,
	/* Hidden arguments */
	int dbname_len,
	int retr_len);

extern void
ctxid_(const char *dbname,
       const char *retr,
       const int  *version,
       int *ctxid,
       const int *ifailmsg,
       int *retcode,
       /* Hidden arguments */
       int dbname_len,
       int retr_len);

extern void
ctxdebug_(const int *enforce,
	  const char *msg,
	  /* Hidden arguments */
	  int msg_len);

extern void 
cmdb_name_(const char *colname,
	   char mdbname[],
	   int *retcode,
	   /* Hidden arguments */
	         int colname_len,
	   const int mdbname_len);

extern void
cmdb_reg_(const char *colname,
	  const char *mdbname,
	  int *mdbkey,
	  const int *it,
	  int *retcode,
	  /* Hidden arguments */
	  int colname_len,
	  int mdbname_len);

extern void
cmdb_vecreg_(const char *colname,
	     const char *mdbname,
	     int vecmdbkey[],
	     const int *low,
	     const int *high,
	     const int *it,
	     int *retcode,
	     /* Hidden arguments */
	     int colname_len,
	     int mdbname_len);

extern void
cmdb_vecset_(const double zaddr[],
	     const int *n_zaddr,
	     int *retcode);

extern void
cmdb_set_(const char *colname,
	  const int *actual_column_index,
	  const int *it,
	  int *retcode,
	  /* Hidden arguments */
	  int colname_len);

extern void
cmdb_reset_(const char *colname,
	    int *reset_value,
	    const int *it,
	    int *retcode,
	    /* Hidden arguments */
	    int colname_len);

extern void
cmdb_get_(const char *colname,
	  int *get_value,
	  const int *it,
	  int *retcode,
	  /* Hidden arguments */
	  int colname_len);

extern void
cmdb_addr_(const char *colname,
	   double *zaddr,
	   const int *it,
	   int *retcode,
	   /* Hidden arguments */
	   int colname_len);

extern void
cmdb_debug_(const int *enforce,
	    const char *msg,
	    /* Hidden arguments */
	    int msg_len);

extern void
cmdb_print_(const int *unit,
	    const char *msg,
	    const int *only_active,
	    const int *it,
	    int *retcode
	    /* Hidden arguments */
	    ,int msg_len);

extern ODB_Pool *
ODB_create_pool(int handle,
		const char *dbname,
		int poolno,
		int is_new,
		int io_method,
		int add_vars);

extern void
ODB_del_vars(const char *dbname, 
	     const char *viewname, 
	     int it);

extern int
ODB_get_vars(const char *dbname,
	     const char *viewname,
	     int it,
	     int nsetvar,
	     ODB_Setvar setvar[]);

extern void
ODB_add_var(const char *dbname, 
	    const char *symbol, 
	    const char *viewname,
	    int it,
	    double addr);

extern double *
ODB_alter_var(const char   *dbname, 
	      const char   *symbol,
	      const char   *viewname,
	      int it,
	      const double *newvalue,
	      double       *oldvalue,
	      int           recur);

extern double *
ODB_getaddr_var(const char   *dbname, 
		const char   *symbol,
		const char   *viewname,
		int it);

extern double
ODB_getval_var(const char   *dbname, 
	       const char   *symbol,
	       const char   *viewname,
	       int it);

extern int
ODB_add_funcs(ODB_Pool *pool, ODB_Funcs *funcs, int it);

extern int
ODB_dynfill(const char *usddothash,
	    void *V,
	    int *(*getindex)(void *V, const char *table, int *lenidx),
	    double data[], int ndata,
	    int istart, int iend, int ioffset);

extern ODB_PoolMask *
ODB_get_poolmask_by_handle(int handle);

extern void
codb_reset_poolmask_(const int *handle,
		     const int *ithread);

extern void
codb_print_poolmask_(const int *handle,
		     const int *ithread);

extern void 
apply_poolmasking_(const int *khandle, const int *kversion, 
		   const char *cdlabel, const int *kvlabel,
		   const int *ktslot, const int *kobstype, const int *kcodetype, const int *ksensor,
		   const int *ksubtype, const int *kbufrtype, const int *ksatinst
		   /* Hidden arguments */
		   , int cdlabel_len);

extern void
codb_poolmasking_status_(const int *handle,
			 int *retcode);

extern void
codb_toggle_poolmask_(const int *handle, 
		      const int *onoff, 
		      int *oldvalue);

extern void
codb_alloc_poolmask_(const int *maxhandle);

extern void
codb_end_poolmask_(const int *handle);

extern void
codb_init_poolmask_(const int *handle,
		    const char *dbname,
		    const int *poolnos_in
		    /* Hidden arguments */
		    ,int dbname_len);

extern void
codb_get_permanent_poolmask_(const int *handle,
			     const int *num_poolnos_in,
			     int  poolnos[],
			     int *num_poolnos_out);

extern void
codb_in_permanent_poolmask_(const int *handle,
			    const int *poolno,
			    int *retcode);

extern int
ODB_in_permanent_poolmask(int handle, int poolno);

extern void
codb_get_poolmask_(const int *handle,
		   const int *poolnos_in,
		         int  poolnos[],
		         int *poolmask_set,
		         int *poolnos_out);

extern int *
ODB_get_permanent_poolmask(int handle, int *npools);

extern void
codb_set_poolmask_(const int *handle,
		   const int *poolnos_in,
		   const int  poolnos[],
		   const int *onoff);

extern void
odb_flpcheck_(const double d[],
	      const   int *nd,
	              int  flag[],
	      const   int *nf,
	              int *rinf,
	              int *rtiny,
	              int *rnan);

extern void
odb_intcheck_(const double d[],
	      const   int *nd,
	      const   int *test_unsigned_only,
	              int  flag[],
	      const   int *nf,
	              int *nlo,
	              int *nhi);

extern FILE *
ODB_getprt_FP();

extern void
ODB_check_considereness(int is_considered,
			const char *tblname);
extern void
codb_consider_table_(const int *handle,
		     const char *tblname, /* Must begin with '@' in front of */
		     int *option_code, /* -1 = get info; 1/0 = set consider/not_consider'ness */
		     int *retcode,     /* previous value of consider'ness */
		     /* Hidden arguments */
		     int tblname_len);

extern int
TableIsConsidered(const char *tblname);

extern void
codb_table_is_considered_(const char *tblname,
			  int *retcode,
			  /* Hidden arguments */
			  int tblname_len);

/* Anne Fouilloux - 23/02/10 - add ODB_WRITE_TABLES to give a list of table to write back on disk (if modified) */
extern void
codb_write_table_(const int *handle,
             const char *tblname, /* Must begin with '@' in front of */
             int *option_code, /* -1 = get info; 1/0 = set writable/not_writable'ness */
             int *retcode,     /* previous value of consider'ness */
             /* Hidden arguments */
             int tblname_len);

extern int
TableIsWritable(const char *tblname);

extern void
codb_table_is_writable_(const char *tblname,
              int *retcode,
              /* Hidden arguments */
              int tblname_len);

/* End Anne Fouilloux - 23/02/10 */

extern void
codb_print_flags_file_(const char *dbname,
		       const int *myproc,
		       int *retcode,
		       /* Hidden arguments */
		       int dbname_len);

extern void
codb_static_init_(const char *a_out,
		  const char *dbname,
		  const int *myproc,
		  int *retcode,
		  /* Hidden arguments */
		  int a_out_len,
		  int dbname_len);

extern void
codb_sqlprint_(const int *handle,
	       const char *dataname, 
	       int *retcode,
	       /* Hidden arguments */
	       int dataname_len);

extern void
codb_getindex_(const int *handle,
	       const int  *poolno,
	       const char *viewname,
	       const char *tablename,
	       const int *idxlen,
	       int idx[],
	       int *retcode,
	       const int *using_it,
	       /* Hidden arguments */
	       int viewname_len,
	       int tablename_len);

extern void
codb_putindex_(const int *handle,
	       const int  *poolno,
	       const char *viewname,
	       const char *tablename,
	       const int *idxlen,
	       const int idx[],
	       int *retcode,
	       const int *using_it,
	       /* Hidden arguments */
	       int viewname_len,
	       int tablename_len);

extern void 
codb_gethandle_(const int *handle,
		const char *dataname,
		int *retcode,
		const int *using_it,
		/* Hidden arguments */
		int dataname_len);

extern void 
codb_get_view_info_(const int *handle,
		    const char *dataname,
		    int  view_info[],
		    const int *nview_info,
		    int *retcode,
		    /* Hidden arguments */
		    int dataname_len);

extern void 
codb_sortkeys_(const int *handle,
	       const char *dataname,
	       int *nkeys,
	       int  mkeys[],
	       int *retcode,
	       /* Hidden arguments */
	       int dataname_len);

extern void 
codb_update_info_(const int *handle,
		  const char *dataname,
		  const int *ncols,
		  int  can_UPDATE[],
		  int *retcode,
		  /* Hidden arguments */
		  int dataname_len);

extern void 
codb_aggr_info_(const int *handle,
		const int  *poolno,
		const char *dataname,
		const int *ncols,
		int  aggr_func_flag[],
		int *phase_id,
		const int *using_it,
		int *retcode,
		/* Hidden arguments */
		int dataname_len);

extern void
codb_calc_aggr_(const int    *phase_id,
		const int    *aggr_func_flag,
		int          *nrc,
		double        rc[],
		const int    *nd, 
		const double  d[],
		const double  daux[]);

extern void
codb_init_twindow_();

extern void
codb_twindow_(const int *target_date,
	      const int *target_time,
	      const int *anal_date,
	      const int *anal_time,
	      const int *left_margin,
	      const int *right_margin,
	      int *retcode);

extern void
codb_tdiff_(const int *target_date,
	    const int *target_time,
	    const int *anal_date,
	    const int *anal_time,
	    int *diff);

extern void
codb_envtransf_(const char *in,
		      char *out,
		       int *retcode,
		/* Hidden arguments */
		      int  in_len,
		const int  out_len);

extern void 
codb_truename_(const char *in,
	             char *out,
	              int *retcode,
	       /* Hidden arguments */
	             int  in_len,
	       const int  out_len);

extern void
codb_pc_filter_(const char *in,
		char *out,
		const int *procid,
		int *retcode,
		/* Hidden arguments */
		int in_len,
		const int out_len);

extern void
codb_init_omp_locks_();

extern void
codb_trace_init_(int *trace_on);

extern void
codb_trace_(const int  *handle,
	    const int  *mode,
	    const char *msg,
	    const int   args[],
	    const int  *numargs,
	    /* Hidden arguments */
	    int msg_len);

extern FILE *
ODB_trace_fp();

extern void 
codb_trace_begin_();

extern void 
codb_trace_end_();

extern void
ODB_debug_print_index(FILE *fp, const char *View, int poolno, int n, int ntbl, ...);

extern void
codb_set_progress_bar_(const char *param,
		       const int *value
		       /* Hidden arguments */
		       , int param_len);

extern void
codb_get_progress_bar_(const char *param,
		       int *value
		       /* Hidden arguments */
		       , int param_len);

extern void
codb_progress_nl_(const int *iounit);

extern void
codb_progress_bar_(const int *iounit,
		   const char *dtname,
		   const int *curpool,
		   const int *maxpool,
		   const int *currows,
		   const int *totalrows,
		   const double *wtime,
		   const int *newline
		   /* Hidden arguments */
		   , int dtname_len);

extern void
ODB_packing_setup(int *prtmsg, int *threshold, double *factor);

extern int
ODB_packing(const int *pmethod);

extern void
ODB_packing_trace(int mode,
		  const char *type, const char *name, const char *table,
		  int pmethod, int pmethod_from_datatype,
		  int dlen, int sizeof_dlen, int pdlen, uint datatype);

extern double *
ODB_unpack_DBL(const uint pd[],
               const int  pdlen,
                     int *dlen,
                     int *method_used,
	            uint  datatype);

extern uint *
ODB_pack_DBL(const double d[],
               const int  dlen,
                     int *method,
                     int *pdlen,
	             Bool avoid_copy);

extern int *
ODB_unpack_INT(const uint pd[],
               const int  pdlen,
                     int *dlen,
                     int *method_used,
	            uint  datatype);

extern uint *
ODB_pack_INT(const int  d[],
             const int  dlen,
                   int *method,
                   int *pdlen,
	           Bool avoid_copy);

extern uint *
ODB_unpack_UINT(const uint pd[],
                const int  pdlen,
                      int *dlen,
                      int *method_used,
	             uint  datatype);

extern uint *
ODB_pack_UINT(const uint d[],
              const int  dlen,
                    int *method,
                    int *pdlen,
	            Bool avoid_copy);

extern void
codb_print_vars_(const int *enforce,
		 const char *msg,
		 /* Hidden arguments */
		 int msg_len);

extern void
codb_procdata_(int *myproc,
	       int *nproc,
	       int *pid,
	       int *it,
	       int *inumt);

extern void
codb_init_(const int *myproc, 
	   const int *nproc);

extern void
setup_sort_(const int *func_no,
	    int *prev_func_no);

extern void /* a Fortran-routine */
ckeysort_(double *a, 
	  const int *nra, 
	  const int *nrows, 
	  const int *ncols, 
	  const int keys[], 
	  const int *nkeys,
	  int idx[],
	  const int *nidx,
	  const int *init_idx,
	  int *iret);

extern void 
init_region_(int array[], 
	     const int *nbytes, 
	     const int *the_magic_4bytes);

extern void
codb_abort_func_(const char *msg,
		 /* Hidden arguments */
		 int msg_len);

extern void 
codb_register_abort_func_(const char *name,
                          /* Hidden arguments */
                          int name_len);

extern const char *
odb_datetime_(int *date_out, int *time_out);

extern const char *
odb_resource_stamp_(const char *label);

extern void
codb_datetime_(int *date_out, int *time_out);

extern void
codb_analysis_datetime_(int *date_out, int *time_out);

extern void 
codb_create_pool_(const int  *handle,
		  const char *dbname,
		  const int  *poolno,
		  const int  *is_new,
		  const int  *io_method,
		  const int  *add_vars,
		  int *retcode,
		  /* Hidden arguments */
		  int dbname_len);

extern void
codb_close_(const int *handle,
	    const int *save,
	          int *retcode);

extern void
codb_load_(const int *handle,
	   const int *poolno,
	   const int *io_method,
	   int *retcode);

extern void
codb_store_(const int *handle,
	    const int *poolno,
	    const int *io_method,
	    int *retcode);

extern void
codb_select_(const int *handle,
	     const int  *poolno,
	     const char *dataname,
	     int *nrows,
	     int *ncols,
	     int *retcode,
	     const int *inform_progress,
	     const int  *using_it,
	     /* Hidden arguments */
	     int dataname_len);

extern void
codb_mp_select_(const int *handle,
		const int *poolno,
		const char *dataname,
		int nrows[],
		int *ncols,
		int *retcode,
		const char *pevar,
		const int *npes,
		const int *replicate_PE,
		const int  *using_it,
		/* Hidden arguments */
		int dataname_len,
		int pevar_len);

extern void
codb_get_npes_(const int *handle,
	       const int *poolno,
	       const char *dataname,
	       int *replicate_PE,
	       int *retcode,
	       const int  *using_it,
	       /* Hidden arguments */
	       int dataname_len);

extern void
codb_get_rowvec_(const int *handle,
		 const int *poolno,
		 const char *dataname,
		 int  nrows[],
		 const int *npes,
		 int *retcode,
		 const int  *using_it,
		 /* Hidden arguments */
		 int dataname_len);

extern void
codb_cancel_(const int  *handle,
	     const int  *poolno,
	     const char *dataname,
	     int *retcode,
	     const int  *delete_intermed,
	     const int  *using_it,
	     /* Hidden arguments */
	     int dataname_len);

extern void
codb_swapout_(const int  *handle,
	      const int  *poolno,
	      const char *dataname,
	      const int  *save,
	      int  *retcode,
	      const int  *delete_intermed,
	      const int  *using_it,
	      /* Hidden arguments */
	      int dataname_len);

extern void
codb_packer_(const int  *handle,
	     const int  *poolno,
	     const char *dataname,
	     const int  *pack_it,
	     int *retcode,
	     /* Hidden arguments */
	     int dataname_len);

extern void
codb_getval_(const char   *dbname,
	     const char   *varname,
	     const char   *viewname,
	     double *value,
	     const int  *using_it,
	     /* Hidden arguments */
	     int dbname_len,
	     int varname_len,
	     int viewname_len);

extern void
codb_setval_(const char   *dbname,
	     const char   *varname,
	     const char   *viewname,
	     const double *newvalue,
	           double *oldvalue,
	     const int  *using_it,
	     /* Hidden arguments */
	     int dbname_len,
	     int varname_len,
	     int viewname_len);


extern void
codb_remove_(const int *handle,
	     const int  *poolno,
	     const char *dataname,
	     int *retcode,
	     /* Hidden arguments */
	     int dataname_len);


extern void
codb_dget_(const int *handle,
	   const int  *poolno,
	   const char *dataname,
	   double d[],
	   const int *doffset,
	   const int *ldimd,
	   const int *nrows,
	   const int *ncols,
	   const int  flag[],
	   const int *procid,
	   const int *istart,
	   const int *ilimit,
	   const int *inform_progress,
	   const int  *using_it,
	   int *retcode,
	   /* Hidden arguments */
	   int dataname_len);

extern void
codb_dput_(const int *handle,
	   const int  *poolno,
	   const char *dataname,
	   const double d[],
	   const int *doffset,
	   const int *ldimd,
	   const int *nrows,
	   const int *ncols,
	   const int  flag[],
	   const int *procid,
	   const int *fill_intermed,
	   const int  *using_it,
	   int *retcode,
	   /* Hidden arguments */
	   int dataname_len);

extern void
codb_getsize_(const int *handle,
	      const int  *poolno,
	      const char *dataname,
	      int *nrows,
	      int *ncols,
	      const int *recur,
	      int *retcode,
	      const int  *using_it,
	      /* Hidden arguments */
	      int dataname_len);

extern void
codb_getsize_aux_(const int *handle,
		  const int  *poolno,
		  const char *dataname,
		  const int *ncols,
		  int *ncols_aux,
		  int  colaux[],
		  const int *colaux_len,
		  int *retcode,
		  const int  *using_it,
		  /* Hidden arguments */
		  int dataname_len);

extern void
write_ddl_(const int  *handle,
	   const int  *iounit,
	   int *retcode);

extern void
codb_write_metadata_(const int  *handle,
		     const int  *iounit,
		     const int  *npools,
		     const int   CreationDT[],
		     const int   AnalysisDT[],
		     const double  *major,
		     const double  *minor,
		     const int  *io_method,
		     int *retcode);

extern void
codb_write_metadata2_(const int  *handle,
		      const int  *iounit,
		      const int  *npools,
		      const int  *ntables,
		      /* const int   fsize[], *//* Npools x Ntables */
		      int *retcode);

extern void
codb_write_metadata3_(const int  *handle,
		      const int  *iounit,
		      int *retcode);

extern void
codb_read_metadata_(const int  *handle,
		    const int  *iounit,
		    int  *npools,
		    int   CreationDT[],
		    int   AnalysisDT[],
		    double  *major,
		    double  *minor,
		    int  *io_method,
		    int *retcode);

extern void
codb_read_metadata2_(const int  *handle,
		     const int  *iounit,
		     const int  *npools,
		     const int  *ntables,
		     /*      int   fsize[], */ /* Npools x Ntables */
		     int *retcode);

extern void
codb_tag_delim_(char *output
		/* Hidden arguments */
		, const int output_len);

extern void 
codb_getnames_(const int *handle,
	       const char *dataname,
	       const int *what, /* 1  = datatype, 
				   2  = varname, 
				   3  = tablename, 
				   4  = viewname,
				   11 = ftntype,
				  101 = exttype,    (same as datatype, but adds bit<#> for bitfield members)
				  102 = extname,    (same as varname, but gets bitfield members)
				  111 = extftntype, (same as ftntype, but handles bitfield members)
				*/
	       char *output,    /* Output string: "/xxx/yyy/zzz/" */
	       int *actual_output_len, /* # of bytes written into *output, excluding '\0' */
	       int *retcode,    /* # of cols resolved */
	       /* Hidden arguments */
	       int dataname_len,
	       const int output_len);

extern void 
codb_getprecision_(const int *handle,
		   const char *dataname,
		   int *maxbits,
		   int *anyflp,
		   int *retcode,
		   /* Hidden arguments */
		   int dataname_len);

extern void
codb_makeview_(const int  *handle,
               const char *dataname,
               const char *viewfile,
               const char *select,
               const char *uniqueby,
               const char *from,
               const char *where,
               const char *orderby,
	       const char *set,
	       const char *query,
               int *retcode,
	       /* Hidden arguments */
               int dataname_len,
               int viewfile_len,
	       int select_len,
	       int uniqueby_len,
	       int from_len,
	       int where_len,
	       int orderby_len,
	       int set_len,
	       int query_len);

extern void
codb_linkdb_(const char *dbname,
	     int *retcode,
	     /* Hidden arguments */
	     int dbname_len);

extern void
codb_linkview_(const int *handle, 
	       const char *dataname,
	       int *retcode,
	       /* Hidden arguments */
	       int dataname_len);

extern void
codb_filesize_(const char *filename,
	       int *retcode,
	       /* Hidden arguments */
	       int filename_len);

extern void
codb_remove_file_(const char *filename,
		  int *retcode,
                  /* Hidden arguments */
                  int filename_len);

extern void
codb_rename_file_(const char *oldfile,
		  const char *newfile,
		  int *retcode,
                  /* Hidden arguments */
		  int newfile_len,
                  int oldfile_len);

extern void
codb_tablesize_(const char *dbname,
		const char *table,
		const int  *poolno,
		int *retcode,
		/* Hidden arguments */
		int dbname_len,
		int table_len);

extern void
ODB_addstatfunc(const char *dbname,
		const char *viewname,
		ODB_Funcs *(*anchor)(void *V, ODB_Pool *pool, int *ntables, int it, int add_vars));

extern int
ODB_undo_dynlink(const char *dbname, 
		 boolean views_only);

extern void
codb_test_index_range_(const int vec[],
		       const int *veclen,
		       const int *low,
		       const int *high,
		       int *retcode);

extern void 
codb_mask_control_word_(double vec[],
			const int *k1, /* Note : C-indexing */
			const int *k2, /* Note : C-indexing */
			const int *doffset,
			const int poolno[]);

extern void 
codb_put_control_word_(double vec[],
		       const int *k1,
		       const int *k2,
		       const int *doffset,
		       const int *poolno,
		       const int *noffset);

extern double
ODB_put_one_control_word(int k, int poolno);

extern void 
codb_get_control_word_(const double vec[],
		       const int *k1,
		       const int *k2,
		       const int *doffset,
		       ll_t ctrlw[]);

extern void
codb_get_pool_count_(const double vec[],
		     const int *k1,
		     const int *k2,
		     const int *doffset,
		     const int *npools,
		     const int poolno[],
		     int *retcode);

extern void
codb_get_poolnos_(const double vec[],
		  const int *k1,
		  const int *k2,
		  const int *doffset,
		  int poolno_out[]);

extern void
codb_get_rownum_(const double vec[],
		 const int *k1,
		 const int *k2,
		 const int *doffset,
		 const int *noffset,
		 int rownum[]);

extern void 
cmask32bits_(uint u[], const int *n, const int *nbits);

extern void 
cmask64bits_(u_ll_t u[], const int *n, const int *nbits);

extern void
codb_init_ae_dump_(int *retcode);

void
codb_ae_dump_(const int *handle,
	      const int *is_get,
	      const int *poolno,
	      const char *dtname,
	      const int *datatype,
	      const int *nra,
	      const int *nrows,
	      const int *ncols,
	      const void *data
	      /* Hidden arguments */
	      , int dtname_len);

extern void
codb_vechash_(const int *nval,
	      const int *nlda,      /* the leading dimension of u ; >= Nval */
	      const int *nelem, 
	      const uint u[], /* actual size : Nlda x  Nelem */
	            uint h[]);

extern uint 
ODB_Hashsize();

extern void 
ODB_Hash_print(FILE *fp);

#define Func_Unique    ODB_Unique
extern int ODB_Unique(const int n, ...);
extern double ODBunique(const int n, const double args[]);

extern void
ODB_Update_Hashmaxmin(int n, const uint h[]);

extern void
codb_hash_set_lock_();

extern void
codb_hash_unset_lock_();

extern void
codb_hash_init_();

extern void
codb_hash_reset_();

extern void
codb_d_unique_(const int *n, 
	       const double d[],
	       const uint *hash,
	       int *is_unique,
	       int *tag,
	       uint *hash_out);

extern void
codb_r_unique_(const int *n,
	       const float r[],
	       const uint *hash,
	       int *is_unique,
	       int *tag,
	       uint *hash_out);

extern void
codb_ui_unique_(const int *n,
		const uint ui[],
		const uint *hash,
		int *is_unique,
		int *tag,
		uint *hash_out);

extern void
ODB_dbl2dbl(double to[], 
	    const double from[],
	    int n);

extern void
ODB_float2dbl(double to[], 
	      const float from[],
	      int n);

extern void
ODB_uint2dbl(double to[],
	     const uint from[],
	     int n);

extern double
ODB_twindow(double dtarget_date,  /* Date to be checked */   
	    double dtarget_time,  /* Time to be checked */
	    double danal_date,    /* Analysis date in format YYYYMMDD */
	    double danal_time,    /* Analysis time in format   HHMMSS */
	    double dleft_margin,  /* Left bdry; offset from analysis tstamp in +/-HHMMSS, HH can be > 23 */
	    double dright_margin  /* Right bdry; offset from analysis tstamp in +/-HHMMSS, HH can be > 23 */
	    );

extern double
ODB_tdiff(double dtarget_date,  /* Date to be checked */   
	  double dtarget_time,  /* Time to be checked */
	  double danal_date,    /* Analysis date in format YYYYMMDD */
	  double danal_time     /* Analysis time in format   HHMMSS */
	  );

extern boolean
ODB_uint_equal(const uint u1[], const uint u2[], int n);

extern boolean
ODB_uchar_equal(const unsigned char u1[], const unsigned char u2[], int n);

extern void
ODB_get_packing_consts(uint *magic, uint *hdrlen, uint *maxshift,
		       double *nmdi, double *rmdi, int *new_version);

extern void
codb_d2u_(const double *d, uint *u);

extern int
ODB_min_alloc();

extern int
ODB_inc_alloc(int n, int min_alloc);

extern void
codb_set_signals_();

extern void 
codb_ignore_alarm_();

extern void 
codb_catch_alarm_(int *sig);

extern void 
codb_set_alarm_();

extern void 
codb_send_alarm_(int *pid);

extern void
codb_closeprt_(int *retcode);

extern void
codb_openprt_(const char *filename,
	      const int *append_mode,
	      int *retcode,
	      /* Hidden arguments */
	      int filename_len);

extern void
codb_lineprt_(const unsigned char *line,
	      int *retcode,
	      /* Hidden arguments */
	      int line_len);

extern void
codb_flushprt_(int *retcode);

extern void
ODB_iolock(int onoff);

#define Func_WildCard  ODB_WildCard
extern int
ODB_WildCard(int n, const char *str, ...);
extern double
ODBwildcard(const int Nd, const double d[]);

#define Func_StrEqual  ODB_StrEqual
extern int
ODB_StrEqual(int n, const char *str, ...);
extern double
ODBstrequal(const int Nd, const double d[]);

#define Func_Inside ODB_Inside
extern int
ODB_Inside(const char *region_name, double lat, double lon);
extern double
ODBinside(double region_name, double lat, double lon);

#define Func_InPolygon ODB_InPolygon
extern int
ODB_InPolygon(const char *polygon_filename, double lat, double lon);
extern double
ODBinpolygon(double polygon_filename, double lat, double lon);

#define Func_Near ODB_Near
extern double
ODB_Near(const char *place_name, double lat, double lon);
extern double
ODBnear(double place_name, double lat, double lon);

#define Func_lat ODB_Lat
extern double ODB_Lat(const char *place_name);
extern double ODBlat(double place_name);

#define Func_lon ODB_Lon
extern double ODB_Lon(const char *place_name);
extern double ODBlon(double place_name);

#define Func_alt ODB_Alt
extern double ODB_Alt(const char *place_name);
extern double ODBalt(double place_name);

#define Func_pop ODB_Pop
extern double ODB_Pop(const char *place_name);
extern double ODBpop(double place_name);

#define Func_RunOnceQuery  ODB_SubQuery
#define Func_SubQuery  ODB_SubQuery
extern double ODB_SubQuery(const int n, 
			   /* const char *poolmask, const char *db, const char *query, double make_sort_unique */ ...);
extern double ODBsubquery(const int nargs, const double args[]);

#define Func_InQuery  ODB_InQuery
extern double ODB_InQuery(const int num_expr, const int nummatch, double subquery, double runonce 
			  , /* double expr1, [double expr2, ... ] */ ...);
extern double ODBinquery(const int nargs, const double args[]);

extern int
ODB_Common_StrEqual(const char *str,
		    const char *cmpstr,
		    int n, const double d[], 
		    Boolean is_wildcard);

extern void    destroy_alist();
extern char   *init_alist(const char *p);
extern boolean in_alist(const char *p);
extern char   *add_alist(const char *p);

/* Consistent logical filenames for TABLEs */

/* Since ALLOCX may use alloca(), don't encapsulate the MakeFileName around {}'s 
   Please note that you have to use FREEX, not FREE-macro to deallocate "filename" */
#define MakeFileName(filename, dbname, table, poolno) \
 ALLOCX(filename, strlen(dbname) + strlen(table) + 20); \
 sprintf(filename, "%s.%s.%d", dbname, table, poolno); \

/* Generic data structure (DS) manipulation macros */

typedef struct {
  uint *pd;
  int   pdlen;
  void *opaque;
  int   nbytes;
  int   on_error;
} Packed_DS;

#define DefineDS(Type) \
  typedef struct { \
    char *type; \
    char *name; \
    char *xname; \
    char *table; \
    Type *d; \
    uint *pd; \
    uint  datatype; \
    int   elemsize; \
    int   dlen; \
    int   nalloc; \
    int   pdlen; \
    int   pmethod; \
    int   pmethod_from_datatype; \
    int   is_packed; \
    int   savelist; \
    void *saved_data; \
    int   saved_data_nbytes; \
    int   saved_data_N[2]; \
  } DS_##Type

#define DeclareDS(Type, Var) DS_##Type Var

#endif /* _ODB_H_ */
