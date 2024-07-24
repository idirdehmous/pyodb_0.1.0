#ifndef _FUNCS_H_
#define _FUNCS_H_

/* funcs.h */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <math.h>
#include "privpub.h"
#include "odbcrc.h"

/* Natural logarithm */
#define Ln log

/* Base-10 logarithm */
#define Lg log10

static double ftrunc(double x) { return trunc(x); }
static double Cot(double x) { return 1/tan(x); }
static double ACot(double x) { return atan(1/x); }
static double ACot2(double y, double x) { return atan2(x,y); }
static double Coth(double x) { return 1/tanh(x); }
static double Asinh(double x) { return Ln(x+sqrt(x*x+1)); }
static double Acosh(double x) { return Ln(x+sqrt(x*x-1)); }
/* static double Acosh(double x) { return Ln(x-sqrt(x*x-1)); } */
static double Atanh(double x) { return Ln((1+x)/(1-x))/2; }
static double ACoth(double x) { return Ln((x+1)/(x-1))/2; }
static double sign(double x) { return x < 0 ? 1 : -1; }
static double touch(double x) { return 1; }
static double binlo(double x, double deltax)
{
  double rc = RMDI;
  if (deltax != 0) {
    if (deltax < 0) deltax = -deltax;
    if (x >= 0) {
      rc = ((int)(x/deltax)) * deltax;
    }
    else {
      rc = ((int)(x/deltax)) * deltax - deltax;
    }
  }
  return rc;
}
static double binhi(double x, double deltax)
{
  double rc = RMDI;
  if (deltax != 0) {
    if (deltax < 0) deltax = -deltax;
    if (x >= 0) {
      rc = ((int)(x/deltax)) * deltax + deltax;
    }
    else {
      rc = ((int)(x/deltax)) * deltax;
    }
  }
  return rc;
}
static double dnint(double d) { return F90nint(d); }
static double dint(double d) { return ftrunc(d); }
static double cmp(double x, double y) { return ((x < y) ? -1 : ((x > y) ? +1 : 0)); }

static double fmaxval(const int n, const double d[])
{
  const double mdi = ABS(RMDI);
  double dmax = mdi;
  int jj=0;
  while (jj<n && ABS(d[jj]) == mdi) jj++;
  if (jj<n) {
    int j;
    dmax = d[jj];
    for (j=jj+1; j<n; j++) {
      if (ABS(d[j]) != mdi && dmax < d[j]) dmax = d[j];
    }
  }
  return dmax;
}

static double fminval(const int n, const double d[])
{
  const double mdi = ABS(RMDI);
  double dmin = mdi;
  int jj=0;
  while (jj<n && ABS(d[jj]) == mdi) jj++;
  if (jj<n) {
    int j;
    dmin = d[jj];
    for (j=jj+1; j<n; j++) {
      if (ABS(d[j]) != mdi && dmin > d[j]) dmin = d[j];
    }
  }
  return dmin;
}

static double Cksum32(const int n, const double d[])
{
  unsigned int crc32 = 0;
  int keylen = n * sizeof(double);
  fodb_crc32_(d, &keylen, &crc32);
  return (double) crc32;
}

static double duint(double d)
{
  d = ftrunc(d);
  if (d >= -INT_MAX && d <= INT_MAX) {
    /* see also : ../lib/codb.c, function codb_d2u_(d,u) */
    int tmp = d;
    unsigned int u = tmp;
    d = u;
  }
  else if (d < 0) {
    /* Not correct ? */
    d = -d;
  }
  return d;
}

static double dfloat(double d) { float f = d; d = f; return d; }

static double dble(double d) { return d; }

static double k2c(double kelvin) {
  double celsius = kelvin - ZERO_POINT;
  return celsius;
}

static double c2k(double celsius)
{
  double kelvin = celsius + ZERO_POINT;
  return kelvin;
}

static double c2f(double celsius)
{
  double fahrenheit = ((9*celsius)/5) + 32;
  return fahrenheit;
}

static double f2c(double fahrenheit)
{
  double celsius = ((fahrenheit - 32)*5)/9;
  return celsius;
}

static double k2f(double kelvin)
{
  double fahrenheit = c2f(k2c(kelvin));
  return fahrenheit;
}

static double f2k(double fahrenheit)
{
  double kelvin = c2k(f2c(fahrenheit));
  return kelvin;
}

static double
rad(double reflat, double reflon, double refdeg, double obslat, double obslon) 
{
  return (double)(acos(cos(reflat) * cos(obslat) * cos(obslon-reflon) +
		       sin(reflat) * sin(obslat) ) <= refdeg);
}

static double
distance(double obslat, double obslon, double reflat, double reflon)
{
  return (double)( R_Earth * 
		   acos(cos(reflat) * cos(obslat) * cos(obslon-reflon) +
			sin(reflat) * sin(obslat)) );
}

static double
dist(double reflat, double reflon, double refdist_km, double obslat, double obslon)
{
  return (double)( R_Earth_km * 
		   acos(cos(reflat) * cos(obslat) * cos(obslon-reflon) +
			sin(reflat) * sin(obslat)) <= (refdist_km) );
}

static double
km(double reflat, double reflon, double obslat, double obslon)
{
  return (double)( R_Earth_km * 
		   acos(cos(reflat) * cos(obslat) * cos(obslon-reflon) +
			sin(reflat) * sin(obslat)) );
}

#define POW(x,y) ((y) == 2 ? (x)*(x) : pow(x,y))

static double circle(double x, double x0, double y, double y0, double r)
{
  return (double)( POW(x-x0,2) + POW(y-y0,2) <= POW(r,2) );
}

static double lon0to360(double lon_minus180_to_plus180)
{
  const double three_sixty = 360;
  return fmod(lon_minus180_to_plus180 + three_sixty, three_sixty);
}

#define CASE_GET_BITS(x, pos, len) \
  case len: rc = GET_BITS(x, pos, len); break

static double ibits(double X, double Pos, double Len)
{
  int rc = 0; /* the default */
  X = ftrunc(X);
  Pos = ftrunc(Pos);
  Len = ftrunc(Len);
  if (X   >= INT_MIN && X   <= INT_MAX &&
      Pos >= 0       && Pos <  MAXBITS &&
      Len >= 1       && Len <= MAXBITS) {
    int x = X;
    int pos = Pos;
    int len = Len;
    switch (len) {
      CASE_GET_BITS(x, pos, 1);
      CASE_GET_BITS(x, pos, 2);
      CASE_GET_BITS(x, pos, 3);
      CASE_GET_BITS(x, pos, 4);
      CASE_GET_BITS(x, pos, 5);
      CASE_GET_BITS(x, pos, 6);
      CASE_GET_BITS(x, pos, 7);
      CASE_GET_BITS(x, pos, 8);
      CASE_GET_BITS(x, pos, 9);
      CASE_GET_BITS(x, pos,10);
      CASE_GET_BITS(x, pos,11);
      CASE_GET_BITS(x, pos,12);
      CASE_GET_BITS(x, pos,13);
      CASE_GET_BITS(x, pos,14);
      CASE_GET_BITS(x, pos,15);
      CASE_GET_BITS(x, pos,16);
      CASE_GET_BITS(x, pos,17);
      CASE_GET_BITS(x, pos,18);
      CASE_GET_BITS(x, pos,19);
      CASE_GET_BITS(x, pos,20);
      CASE_GET_BITS(x, pos,21);
      CASE_GET_BITS(x, pos,22);
      CASE_GET_BITS(x, pos,23);
      CASE_GET_BITS(x, pos,24);
      CASE_GET_BITS(x, pos,25);
      CASE_GET_BITS(x, pos,26);
      CASE_GET_BITS(x, pos,27);
      CASE_GET_BITS(x, pos,28);
      CASE_GET_BITS(x, pos,29);
      CASE_GET_BITS(x, pos,30);
      CASE_GET_BITS(x, pos,31);
      CASE_GET_BITS(x, pos,32);
    }
  }
  return (double) rc;
}

static double aYear(double date)
{
  const double mdi = ABS(RMDI);
  int year = NMDI;
  if (ABS(date) != mdi) {
    year = (int)(date/10000);
  }
  return (double)year;
}

static double aMonth(double date)
{
  const double mdi = ABS(RMDI);
  int month = NMDI;
  if (ABS(date) != mdi) {
    month = (int)(date/100);
    month %= 100;
  }
  return (double)month;
}
 
static double aDay(double date)
{
  const double mdi = ABS(RMDI);
  int day = NMDI;
  if (ABS(date) != mdi) {
    day = (int)date;
    day %= 100;
  }
  return (double)day;
}

static double anHour(double time)
{
  const double mdi = ABS(RMDI);
  int hour = NMDI;
  if (ABS(time) != mdi) {
    hour = (int)(time/10000);
  }
  return (double)hour;
}

static double aMinute(double time)
{
  const double mdi = ABS(RMDI);
  int minute = NMDI;
  if (ABS(time) != mdi) {
    minute = (int)(time/100);
    minute %= 100;
  }
  return (double)minute;
}

static double aSecond(double time)
{
  const double mdi = ABS(RMDI);
  int second = NMDI;
  if (ABS(time) != mdi) {
    second = (int)time;
    second %= 100;
  }
  return (double)second;
}

/* Wind speed (ff), direction (dd), u- and v-components */

#define parc  ((double)360)
#define degcon  pi_over_180

static double Pi()
{
  return pi; /* A macro #define from privpub.h */
}

static double Speed(double u, double v)
{ /* Aliases : uv2ff, ff */
  const double mdi = ABS(RMDI);
  double ff = (ABS(u) == mdi || ABS(v) == mdi) ? mdi : sqrt(u*u + v*v);
  return ff;
}

static double Dir(double u, double v)
{ /* Aliases : ff2uv, dd */
  const double mdi = ABS(RMDI);
  double dd;
  if (ABS(u) == mdi || ABS(v) == mdi) {
    dd = mdi;
  }
  else {
    if (u == 0 && v == 0) {
      dd = 0.5e0 * parc;
    }
    else {
      dd = fmod(parc+atan2(-u,-v)/degcon,parc);
    }
  }
  return dd;
}

static double Ucom(double dd, double ff)
{ /* Aliases : ucomp, u */
  const double mdi = ABS(RMDI);
  double u = (ABS(dd) == mdi || ABS(ff) == mdi) ? mdi : -ff * sin(dd*degcon);
  return u;
}

static double Vcom(double dd, double ff)
{ /* Aliases : vcomp, v */
  const double mdi = ABS(RMDI);
  double v = (ABS(dd) == mdi || ABS(ff) == mdi) ? mdi : -ff * cos(dd*degcon);
  return v;
}

static double Within(double target, double lo, double hi)
{
  return (target >= lo && target <= hi) ? 1 : 0;
}

static double Within360(double target, double lo, double hi)
{
  target = lon0to360(target);
  lo = lon0to360(lo);
  hi = lon0to360(hi);
  return Within(target, lo, hi);
}

static double llu2double(double i0, double i1)
{
  union {
    double d;
    int i[2];
  } u;
  u.i[0] = i0;
  u.i[1] = i1;
  return u.d;
}

#if defined(FUNCS_C)

static double Min(double x) { return x; }
static double Max(double x) { return x; }

static double Density(double resol) { return resol; }
static double Count(double x)       { return (double)1; }
static double Bcount(double x)      { return (double) (((x) != 0) ? 1 : 0); }
static double Sum(double x)         { return x; }
static double Avg(double x)         { return x; }
static double Median(double x)      { return x; }
static double Stdev(double x)       { return x; }
static double Var(double x)         { return x; }
static double Rms(double x)         { return x; }
static double Dotp(double x, double y) { return (x * y); }
static double Norm(double x, double y) { return Dotp(x,y); }

static double Count_distinct(double x)   { return x; }
static double Bcount_distinct(double x)  { return x; }
static double Sum_distinct(double x)     { return Sum(x); }
static double Avg_distinct(double x)     { return Avg(x); }
static double Median_distinct(double x)  { return Median(x); }
static double Stdev_distinct(double x)   { return Stdev(x); }
static double Var_distinct(double x)     { return Var(x); }
static double Rms_distinct(double x)     { return Rms(x); }
static double Dotp_distinct(double x, double y) { return Dotp(x,y); }
static double Norm_distinct(double x, double y) { return Dotp(x,y); }

/* from evaluate.h */

#define Aggr_argnO thsp[IT].aggr_argno

static double Shared2ArgFunc(double x, double y)
{
  DEF_IT;
  const thsafe_parse_t *thsp = GetTHSP();
  double value = 0;
  if (thsp) {
    if (Aggr_argnO == 1) value = x;
    else if (Aggr_argnO == 2) value = y;
  }
  return value;
}

static double Covar(double x, double y)      { return Shared2ArgFunc(x,y); }
static double Corr(double x, double y)       { return Shared2ArgFunc(x,y); }
static double Linregr_a(double x, double y)  { return Shared2ArgFunc(x,y); }
static double Linregr_b(double x, double y)  { return Shared2ArgFunc(x,y); }
static double Maxloc(double x, double y)     { return Shared2ArgFunc(x,y); }
static double Minloc(double x, double y)     { return Shared2ArgFunc(x,y); }

#endif

/* 
   Included from two places : 
   lib/funcs.c --> #define FUNCS_C set to 1
   compiler/tree.c --> FUNCS_C not set at all
*/

typedef struct _funcs_t {
  char *name;
  int numargs;
  union {
    void *compile_time;
    double dbladdr;
    double (*zeroarg) (void);
    double (*onearg)  (double arg1);
    double (*twoarg)  (double arg1, double arg2);
    double (*threearg)(double arg1, double arg2, double arg3);
    double (*fourarg) (double arg1, double arg2, double arg3, double arg4);
    double (*fivearg) (double arg1, double arg2, double arg3, double arg4, double arg5);
    double (*sixarg)  (double arg1, double arg2, double arg3, double arg4, double arg5, double arg6);
    double (*vararg)  (const int n, const double args[]);
  } u;
  union {
    void *run_time;
    double dbladdr;
    double (*zeroarg) (void);
    double (*onearg)  (double arg1);
    double (*twoarg)  (double arg1, double arg2);
    double (*threearg)(double arg1, double arg2, double arg3);
    double (*fourarg) (double arg1, double arg2, double arg3, double arg4);
    double (*fivearg) (double arg1, double arg2, double arg3, double arg4, double arg5);
    double (*sixarg)  (double arg1, double arg2, double arg3, double arg4, double arg5, double arg6);
    double (*vararg)  (const int n, const double args[]);
  } f;
  int deg2rad; /*
		 0=No conversions
		 1=degrees argument(s) need to be converted into radians
		 2=the result needs to be converted back to degrees
		 < 0 : Convert all arguments to radians except the ABS(deg2rad)'th [hack]
	       */
  int joffset; /* The count of extra args added in front of the user supplied args */
} funcs_t;

#if defined(FUNCS_C)
#define AGGRO(x) "_" #x
#define RUNTIME(x) (void *)x
#define timestamp ODB_timestamp
#define basetime ODB_basetime
#else
#define AGGRO(x) #x
#define RUNTIME(x) NULL
#endif

static int NfuncS = 0;

static funcs_t Func[] = {
  /* Compile-time evaluable functions */
  "sin", 1, (void *)sin, NULL, 1, 0,
  "cos", 1, (void *)cos, NULL, 1, 0,
  "tan", 1, (void *)tan, NULL, 1, 0,
  "cot", 1, (void *)Cot, NULL, 1, 0,
  "asin", 1, (void *)asin, NULL, 2, 0,
  "acos", 1, (void *)acos, NULL, 2, 0,
  "atan", 1, (void *)atan, NULL, 2, 0,
  "atan2", 2, (void *)atan2, NULL, 2, 0,
  "acot", 1, (void *)ACot, NULL, 2, 0,
  "acot2", 2, (void *)ACot2, NULL, 2, 0,
  "sinh", 1, (void *)sinh, NULL, 0, 0,
  "cosh", 1, (void *)cosh, NULL, 0, 0,
  "tanh", 1, (void *)tanh, NULL, 0, 0,
  "coth", 1, (void *)Coth, NULL, 0, 0,
  "asinh", 1, (void *)Asinh, NULL, 0, 0,
  "acosh", 1, (void *)Acosh, NULL, 0, 0,
  "atanh", 1, (void *)Atanh, NULL, 0, 0,
  "acoth", 1, (void *)ACoth, NULL, 0, 0,
  "sqrt", 1, (void *)sqrt, NULL, 0, 0,
  "mod", 2, (void *)fmod, NULL, 0, 0,
  "pow", 2, (void *)pow, NULL, 0, 0,
  "exp", 1, (void *)exp, NULL, 0, 0,
  "log", 1, (void *)log, NULL, 0, 0,
  "ln", 1, (void *)Ln, NULL, 0, 0,
  "lg", 1, (void *)Lg, NULL, 0, 0,
  "log10", 1, (void *)log10, NULL, 0, 0,
  "floor", 1, (void *)floor, NULL, 0, 0,
  "ceil", 1, (void *)ceil, NULL, 0, 0,
  "ldexp", 2, (void *)ldexp, NULL, 0, 0,
  "abs", 1, (void *)fabs, NULL, 0, 0,
  "trunc", 1, (void *)ftrunc, NULL, 0, 0,
  "touch", 1, (void *)touch, NULL, 0, 0,
  "sign", 1, (void *)sign, NULL, 0, 0,
  "binlo", 2, (void *)binlo, NULL, 0, 0,
  "binhi", 2, (void *)binhi, NULL, 0, 0,
  "int", 1, (void *)dint, NULL, 0, 0,
  "nint", 1, (void *)dnint, NULL, 0, 0,
  "cmp", 2, (void *)cmp, NULL, 0, 0,
  "max", -1, (void *)fmaxval, NULL, 0, 1,
  "maxval", -1, (void *)fmaxval, NULL, 0, 1,
  "min", -1, (void *)fminval, NULL, 0, 1,
  "minval", -1, (void *)fminval, NULL, 0, 1,
  "cksum", -1, (void *)Cksum32, NULL, 0, 1,
  "uint", 1, (void *)duint, NULL, 0, 0,
  "float", 1, (void *)dfloat, NULL, 0, 0,
  "dble", 1, (void *)dble, NULL, 0, 0,
  "celsius", 1, (void *)k2c, NULL, 0, 0, /* Kelvin to Celsius */
  "k2c", 1, (void *)k2c, NULL, 0, 0, /* Kelvin to Celsius */
  "c2k", 1, (void *)c2k, NULL, 0, 0, /* and Celsius to Kelvin */
  "fahrenheit", 1, (void *)k2f, NULL, 0, 0, /*  Kelvin to Fahrenheit */
  "k2f", 1, (void *)k2f, NULL, 0, 0, /* Kelvin to Fahrenheit */
  "f2k", 1, (void *)f2k, NULL, 0, 0, /* and vice versa */
  "c2f", 1, (void *)c2f, NULL, 0, 0, /* Celsius to Fahrenheit */
  "f2c", 1, (void *)f2c, NULL, 0, 0, /* Fahrenheit to Celsius */
  "rad", 5, (void *)rad, NULL, 1, 0,
  "distance", 4, (void *)distance, NULL, 1, 0,
  "dist", 5, (void *)dist, NULL, -3, 0, /* Uses [hack] for deg2rad : No deg2rad for 3rd arg */
  "km", 4, (void *)km, NULL, 1, 0,
  "lon0to360", 1, (void *)lon0to360, NULL, 1, 0,
  "ibits", 3, (void *)ibits, NULL, 0, 0,
  "circle", 5, (void *)circle, NULL, 0, 0,
  "year", 1, (void *)aYear, NULL, 0, 0,
  "month", 1, (void *)aMonth, NULL, 0, 0,
  "day", 1, (void *)aDay, NULL, 0, 0,
  "hour", 1, (void *)anHour, NULL, 0, 0,
  "hours", 1, (void *)anHour, NULL, 0, 0,
  "minutes", 1, (void *)aMinute, NULL, 0, 0,
  "minute", 1, (void *)aMinute, NULL, 0, 0,
  "seconds", 1, (void *)aSecond, NULL, 0, 0,
  "second", 1, (void *)aSecond, NULL, 0, 0,
  "pi", 0, (void *)Pi, NULL, 0, 0,
  "speed", 2, (void *)Speed, NULL, 0, 0,
  "uv2ff", 2, (void *)Speed, NULL, 0, 0,
  "ff", 2, (void *)Speed, NULL, 0, 0,
  "dir", 2, (void *)Dir, NULL, 0, 0,
  "uv2dd", 2, (void *)Dir, NULL, 0, 0,
  "dd", 2, (void *)Dir, NULL, 0, 0,
  "u", 2, (void *)Ucom, NULL, 0, 0,
  "ucomp", 2, (void *)Ucom, NULL, 0, 0,
  "ucom", 2, (void *)Ucom, NULL, 0, 0,
  "v", 2, (void *)Vcom, NULL, 0, 0,
  "vcomp", 2, (void *)Vcom, NULL, 0, 0,
  "vcom", 2, (void *)Vcom, NULL, 0, 0,
  "within", 3, (void *)Within, NULL, 0, 0,
  "within", 3, (void *)Within, NULL, 0, 0,

  "tstamp", 2, (void *)timestamp, RUNTIME(ODB_timestamp), 0, 0, /* alias to timestamp */
  "basetime", 2, (void *)basetime, RUNTIME(ODB_basetime), 0, 0,

  /* Run-time only -evaluable functions */
  "eq_min_resol", 0, NULL, RUNTIME(ODB_eq_min_resol), 0, 0,
  "eq_max_n", 0, NULL, RUNTIME(ODB_eq_max_n), 0, 0,
  "eq_resol", 1, NULL, RUNTIME(ODB_eq_resol), 0, 0,
  "eq_area", 1, NULL, RUNTIME(ODB_eq_area_with_resol), 0, 0,
  "eq_truearea", 3, NULL, RUNTIME(ODB_eq_truearea_with_resol), 0, 0,
  "eq_latband", 2, NULL, RUNTIME(ODB_eq_latband_with_resol), 0, 0,
  "eq_n", 1, NULL, RUNTIME(ODB_eq_n), 0, 0,
  "eq_boxid", 3, NULL, RUNTIME(ODB_eq_boxid_with_resol), 0, 0,
  "eq_boxlat", 3, NULL, RUNTIME(ODB_eq_boxlat_with_resol), 0, 0,
  "eq_boxlon", 3, NULL, RUNTIME(ODB_eq_boxlon_with_resol), 0, 0,
  "rgg_boxid", 3, NULL, RUNTIME(ODB_rgg_boxid), 0, 0,
  "rgg_boxlat", 3, NULL, RUNTIME(ODB_rgg_boxlat), 0, 0,
  "rgg_boxlon", 3, NULL, RUNTIME(ODB_rgg_boxlon), 0, 0,
  "rgg_resol", 1, NULL, RUNTIME(ODB_rgg_resol), 0, 0,
  "rotlat", 4, NULL, RUNTIME(ODB_rotlat), 0, 0,
  "rotlon", 4, NULL, RUNTIME(ODB_rotlon), 0, 0,
  "boxlat", 2, NULL, RUNTIME(ODB_boxlat), 0, 0,
  "boxlon", 2, NULL, RUNTIME(ODB_boxlon), 0, 0,
  "boxid_lat", 2, NULL, RUNTIME(ODB_boxid_lat), 0, 0,
  "boxid_lon", 2, NULL, RUNTIME(ODB_boxid_lon), 0, 0,
  "boxid", 4, NULL, RUNTIME(ODB_boxid), 0, 0,
  "random", 0, NULL, RUNTIME(ODB_random), 0, 0,
  "seed", 1, NULL, RUNTIME(ODB_seed), 0, 0,
  "now", 0, NULL, RUNTIME(ODB_now), 0, 0,
  "date_now", 0, NULL, RUNTIME(ODB_date_now), 0, 0,
  "time_now", 0, NULL, RUNTIME(ODB_time_now), 0, 0,
  "InFile", 2, NULL, RUNTIME(ODBinfile), 0, 0,
  "NotInFile", 2, NULL, RUNTIME(ODBnotinfile), 0, 0,
  "InGenList", 4, NULL, RUNTIME(ODB_InGenList), 0, 0,
  "lldegrees", 1, NULL, RUNTIME(ODB_lldegrees), 0, 0,
  "llradians", 1, NULL, RUNTIME(ODB_llradians), 0, 0,
  "degrees", 1, NULL, RUNTIME(ODB_degrees), 0, 0,
  "radians", 1, NULL, RUNTIME(ODB_radians), 0, 0,
  "thin", -1, NULL, RUNTIME(ODB_vthin), 0, 2,
  "offset", 2, NULL, RUNTIME(ODBoffset), 0, 0,
  "twindow", 6, NULL, RUNTIME(ODB_twindow), 0, 0,
  "tdiff", 4, NULL, RUNTIME(ODB_tdiff), 0, 0,
  "maxcount", 1, NULL, RUNTIME(ODB_maxcount), 0, 0,
  "maxrows", 0, NULL, RUNTIME(ODB_maxrows), 0, 0,
  "Unique", -1, NULL, RUNTIME(ODBunique), 0, 0, /* Now in sync with ODB_Unique(const int n, ...) */
  "SubQuery", -1, NULL, RUNTIME(ODBsubquery), 0, 1,
  "RunOnceQuery", -1, NULL, RUNTIME(ODBsubquery), 0, 1,
  "InQuery", -1, NULL, RUNTIME(ODBinquery), 0, 0,
  "WildCard", -1, NULL, RUNTIME(ODBwildcard), 0, 1,
  "StrEqual", -1, NULL, RUNTIME(ODBstrequal), 0, 1,
  "Inside", 3, NULL, RUNTIME(ODBinside), 0, 0,
  "InPolygon", 3, NULL, RUNTIME(ODBinpolygon), 0, 0,
  "Near", 3, NULL, RUNTIME(ODBnear), 0, 0,
  "lat", 1, NULL, RUNTIME(ODBlat), 0, 0,
  "latitude", 1, NULL, RUNTIME(ODBlat), 0, 0,
  "lon", 1, NULL, RUNTIME(ODBlon), 0, 0,
  "longitude", 1, NULL, RUNTIME(ODBlon), 0, 0,
  "alt", 1, NULL, RUNTIME(ODBalt), 0, 0,
  "altitude", 1, NULL, RUNTIME(ODBalt), 0, 0,
  "elev", 1, NULL, RUNTIME(ODBalt), 0, 0,
  "elevation", 1, NULL, RUNTIME(ODBalt), 0, 0,
  "pop", 1, NULL, RUNTIME(ODBpop), 0, 0,
  "popul", 1, NULL, RUNTIME(ODBpop), 0, 0,
  "population", 1, NULL, RUNTIME(ODBpop), 0, 0,
  "walltime", 0, NULL, RUNTIME(util_walltime_), 0, 0,
  "cputime", 0, NULL, RUNTIME(util_cputime_), 0, 0,
  "datenum", 1, NULL, RUNTIME(ODB_datenum), 0, 0,
  "hours_utc", 1, NULL, RUNTIME(ODB_hours_utc), 0, 0,
  "julian_date", 2, RUNTIME(ODB_jd), NULL, 0, 0,
  "jd", 2, RUNTIME(ODB_jd), NULL, 0, 0,
  "ta", 2, NULL, RUNTIME(ODB_time_angle), 0, 0,
  "ha", 1, NULL, RUNTIME(ODB_hour_angle), 0, 0,
  "sda", 2, NULL, RUNTIME(ODB_solar_declination), 0, 0,
  "sela", 4, NULL, RUNTIME(ODB_solar_elevation), 0, 0,
  "sza", 4, NULL, RUNTIME(ODB_solar_zenith), 0, 0,
  "saza", 4, NULL, RUNTIME(ODB_solar_azimuth), 0, 0,
  "daynight", 4, NULL, RUNTIME(ODB_daynight), 0, 0,

  "myproc", 0, NULL, RUNTIME(ODB_myproc), 0, 0,
  "nproc", 0, NULL, RUNTIME(ODB_nproc), 0, 0,
  "pid", 0, NULL, RUNTIME(ODB_pid), 0, 0,
  "tid", 0, NULL, RUNTIME(ODB_tid), 0, 0,
  "nthreads", 0, NULL, RUNTIME(ODB_nthreads), 0, 0,

  /* Aggregate functions (prepended with underscore when with FUNCS_C) */
  AGGRO(density), 1, NULL, RUNTIME(Density), 0, 0,
  AGGRO(count), 1, NULL, RUNTIME(Count), 0, 0,
  AGGRO(bcount), 1, NULL, RUNTIME(Bcount), 0, 0,
  AGGRO(sum), 1, NULL, RUNTIME(Sum), 0, 0,
  AGGRO(avg), 1, NULL, RUNTIME(Avg), 0, 0,
  AGGRO(median), 1, NULL, RUNTIME(Median), 0, 0,
  AGGRO(stdev), 1, NULL, RUNTIME(Stdev), 0, 0,
  AGGRO(var), 1, NULL, RUNTIME(Var), 0, 0,
  AGGRO(rms), 1, NULL, RUNTIME(Rms), 0, 0,
  AGGRO(dotp), 2, NULL, RUNTIME(Dotp), 0, 0,
  AGGRO(norm), 2, NULL, RUNTIME(Norm), 0, 0,
  AGGRO(count_distinct), 1, NULL, RUNTIME(Count_distinct), 0, 0,
  AGGRO(bcount_distinct), 1, NULL, RUNTIME(Bcount_distinct), 0, 0,
  AGGRO(sum_distinct), 1, NULL, RUNTIME(Sum_distinct), 0, 0,
  AGGRO(avg_distinct), 1, NULL, RUNTIME(Avg_distinct), 0, 0,
  AGGRO(median_distinct), 1, NULL, RUNTIME(Median_distinct), 0, 0,
  AGGRO(stdev_distinct), 1, NULL, RUNTIME(Stdev_distinct), 0, 0,
  AGGRO(var_distinct), 1, NULL, RUNTIME(Var_distinct), 0, 0,
  AGGRO(rms_distinct), 1, NULL, RUNTIME(Rms_distinct), 0, 0,
  AGGRO(dotp_distinct), 2, NULL, RUNTIME(Dotp_distinct), 0, 0,
  AGGRO(norm_distinct), 2, NULL, RUNTIME(Norm_distinct), 0, 0,
  AGGRO(covar), 2, NULL, RUNTIME(Covar), 0, 0,
  AGGRO(corr), 2, NULL, RUNTIME(Corr), 0, 0,
  AGGRO(linregr_a), 2, NULL, RUNTIME(Linregr_a), 0, 0,
  AGGRO(linregr_b), 2, NULL, RUNTIME(Linregr_b), 0, 0,
  AGGRO(minloc), 2, NULL, RUNTIME(Minloc), 0, 0,
  AGGRO(maxloc), 2, NULL, RUNTIME(Maxloc), 0, 0,

  /* Special cases */

  "Conv_llu2double", 2, (void *)llu2double, NULL, 0, 0,
#if defined(FUNCS_C)
  "_min", 1, NULL, RUNTIME(Min), 0, 0, /* alias _minval */
  "_max", 1, NULL, RUNTIME(Max), 0, 0, /* alias _maxval */
#else
  "in_vector", 2, NULL, RUNTIME(ODB_in_vector), 0, 0, /* ctxgetdb.F90 only */
  "paral", 2, NULL, NULL, 0, 0, /* only relevant in generated C-code */
  "debug_print", 1, NULL, NULL, 0, 0, /* only relevant in generated C-code, if at all */
#endif

  NULL,
};

#endif /* _FUNCS_H_ */
