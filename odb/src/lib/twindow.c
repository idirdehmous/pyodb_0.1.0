
/* twindow.c */


#ifdef STANDALONE_TEST
#include <stdio.h>
#include <stdlib.h>
#define boolean int
#define PUBLIC
#define PRIVATE static
#define ABS(x) ( (x) >= 0 ? (x) : -(x) )
extern int get_thread_id_();
extern int get_max_threads_();
#else
#include "odb.h"
#endif

PRIVATE int allow_sec_error = 0; /* When non-zero, allows seconds to be > 59 ==> fixed to 59 */
PRIVATE int allow_min_error = 0; /* When non-zero, allows minutes to be > 59 ==> fixed to 59 */
PRIVATE int allow_hour_error= 0; /* When non-zero, allows hours to be > 23 ==> fixed to 23 */

/* secdiff() from Y2K-compliant ECLIB */

#if defined(_ABI64) || defined(__uxpch__) || defined(VPP5000) || defined(__64BIT__) || defined(NECSX)
        typedef int _int32_t;
#else
        typedef long int _int32_t;
#endif


extern void
secdiff(const _int32_t *const year1,
	const _int32_t *const month1,
	const _int32_t *const day1,
	const _int32_t *const hour1,
	const _int32_t *const min1,
	const _int32_t *const sec1,
	const _int32_t *const year2,
	const _int32_t *const month2,
	const _int32_t *const day2,
	const _int32_t *const hour2,
	const _int32_t *const min2,
	const _int32_t *const sec2,
	_int32_t *const seconds,
	_int32_t *const ret);


PRIVATE _int32_t *RC_tdiff = NULL; /* Used to be thread unsafe */

PUBLIC void
codb_init_twindow_()
{
  if (!RC_tdiff) {
    int max_omp_threads = get_max_threads_();
    RC_tdiff = (_int32_t *)calloc(max_omp_threads , sizeof(*RC_tdiff));
  }
}


PUBLIC void
codb_allow_time_error_(int *p_allow_errors) /* when set to HHMMSS , then allow errors
					       in seconds, if SS > 0
					       in minutes, if MM > 0
					       in hours, if HH > 0
					       So :
					       if set to 1 --> tolerate errors only in seconds
					       if set to 100 --> tolerate errors only in minutes
					       if set to 10000 --> tolerate errors only in hours
					       or if set to 10101 --> tolerate errors in HH,MM & SS
					     */
{
  if (p_allow_errors) {
    int x = ABS(*p_allow_errors);
    int hh = x/10000;
    int mm = (x%10000)/100;
    int ss = x%100;
    allow_sec_error = (ss > 0);
    allow_min_error = (mm > 0);
    allow_hour_error = (hh > 0);
  }
}

PUBLIC double
ODB_tdiff(double dtarget_date,  /* Date to be checked */   
	  double dtarget_time,  /* Time to be checked */
	  double danal_date,    /* Analysis date in format YYYYMMDD */
	  double danal_time     /* Analysis time in format   HHMMSS */
	  )
     /* Calculates difference in seconds */
{
  _int32_t diff = 0;

  int target_date = dtarget_date;
  int target_time = dtarget_time;
  int anal_date   = danal_date;
  int anal_time   = danal_time;

  _int32_t yyyy_target = target_date/10000;
  _int32_t month_target = (target_date%10000)/100;
  _int32_t day_target = target_date%100;
  _int32_t hour_target = target_time/10000;
  _int32_t min_target = (target_time%10000)/100;
  _int32_t sec_target = target_time%100;
  
  _int32_t yyyy_anal = anal_date/10000;
  _int32_t month_anal = (anal_date%10000)/100;
  _int32_t day_anal = anal_date%100;
  _int32_t hour_anal = anal_time/10000;
  _int32_t min_anal = (anal_time%10000)/100;
  _int32_t sec_anal = anal_time%100;

  if (allow_hour_error) {
    if (hour_target > 23) hour_target = 23; 
    if (hour_anal > 23) hour_anal = 23;
  }

  if (allow_min_error) {
    if (min_target > 59) min_target = 59; 
    if (min_anal > 59) min_anal = 59;
  }

  if (allow_sec_error) {
    if (sec_target > 59) sec_target = 59;
    if (sec_anal > 59) sec_anal = 59;
  }

  {
    int it = get_thread_id_();
    if (!RC_tdiff) codb_init_twindow_();
    secdiff(&yyyy_target, &month_target, &day_target, &hour_target, &min_target, &sec_target,
	    &yyyy_anal, &month_anal, &day_anal, &hour_anal, &min_anal, &sec_anal,
	    &diff,
	    &RC_tdiff[--it]);
  }

  return (double)diff;
}


PUBLIC double
ODB_twindow(double dtarget_date,  /* Date to be checked */   
	    double dtarget_time,  /* Time to be checked */
	    double danal_date,    /* Analysis date in format YYYYMMDD */
	    double danal_time,    /* Analysis time in format   HHMMSS */
	    double dleft_margin,  /* Left bdry; offset from analysis tstamp in +/-HHMMSS, HH can be > 23 */
	    double dright_margin  /* Right bdry;  offset from analysis tstamp in +/-HHMMSS, HH can be > 23 */
	    )
{
  /* twindow: Resolution in seconds */

  int target_date = dtarget_date;
  int target_time = dtarget_time;
  int anal_date   = danal_date;
  int anal_time   = danal_time;
  int left_margin = dleft_margin;
  int right_margin = dright_margin;

  boolean inside = 0;
  int rc = 0;
  int leftsec = 0, rightsec = 0;
  int diff = 0;
  int sign, hour, min, sec;
  
  sign = (left_margin < 0) ? -1 : 1;
  left_margin = ABS(left_margin);
  hour = left_margin/10000;
  min = (left_margin%10000)/100;
  sec = left_margin%100;
  left_margin *= sign;

#if 0
  if (hour > 23) {
    if (allow_hour_error) hour = 23;
    else rc++;
  }
#endif

  if (min > 59) {
    if (allow_min_error) min = 59;
    else rc++;
  }

  if (sec > 59) {
    if (allow_sec_error) sec = 59;
    else rc++;
  }

  if (rc == 0) {
    leftsec = hour * 3600 + min * 60 + sec;
    leftsec *= sign;
  }

  sign = (right_margin < 0) ? -1 : 1;
  right_margin = ABS(right_margin);
  hour = right_margin/10000;
  min = (right_margin%10000)/100;
  sec = right_margin%100;
  right_margin *= sign;

#if 0
  if (hour > 23) {
    if (allow_hour_error) hour = 23;
    else rc++;
  }
#endif

  if (min > 59) {
    if (allow_min_error) min = 59;
    else rc++;
  }

  if (sec > 59) {
    if (allow_sec_error) sec = 59;
    else rc++;
  }

  if (rc == 0) {
    rightsec = hour * 3600 + min * 60 + sec;
    rightsec *= sign;
  }

  if (rc == 0) {
    int it = get_thread_id_();
    diff = ODB_tdiff(target_date, target_time, anal_date, anal_time);
    rc = RC_tdiff[--it];
  }
  else {
    fprintf(stderr,"ODB_twindow: Invalid left and/or right margins\n");
    fprintf(stderr,"             Use +/-HHMMSS format (HH >= 0, MM <= 59, SS <= 59)\n");
    fprintf(stderr,"              Left margin = %d\n", left_margin);
    fprintf(stderr,"             Right margin = %d\n", right_margin);
  }

  if (rc != 0) {
#ifdef STANDALONE_TEST
    fprintf(stderr,"*** Error ***\n");
    exit(1);
#else
    /* char p[] = "ODB_twindow"; */
    fprintf(stderr,"ODB_twindow: Invalid date, time and/or margin(s)\n");
    fprintf(stderr,"               Target (date,time) = (%8.8d, %6.6d)\n",
	    target_date, target_time);
    fprintf(stderr,"             Analysis (date,time) = (%8.8d, %6.6d)\n",
	    anal_date, anal_time);
    fprintf(stderr,"             Margins (left,right) = (%d, %d)\n",
	    left_margin, right_margin);
    /* codb_abort_func_(p, strlen(p)); */
    inside = 0;
#endif
  }
  else {
    inside = (( rc == 0 ) && ( diff >= leftsec && diff <= rightsec ));
  }

#ifdef STANDALONE_TEST
  printf("target: %8.8d %6.6d\n", target_date, target_time);
  printf("  anal: %8.8d %6.6d\n",   anal_date,   anal_time);
  printf("  diff: %d seconds with rc = %d\n", diff, rc);
  printf("  left: %6d (%d sec)\n",  left_margin, leftsec);
  printf(" right: %6d (%d sec)\n", right_margin, rightsec);
  printf("inside: %d\n",inside);
#endif

  return (double)inside;
}

/* Fortran callable */

PUBLIC void
codb_twindow_(const int *target_date,
	      const int *target_time,
	      const int *anal_date,
	      const int *anal_time,
	      const int *left_margin,
	      const int *right_margin,
	      int *retcode)
{
  *retcode = ODB_twindow(*target_date,
			 *target_time,
			 *anal_date,
			 *anal_time,
			 *left_margin,
			 *right_margin);
}


PUBLIC void
codb_tdiff_(const int *target_date,
	    const int *target_time,
	    const int *anal_date,
	    const int *anal_time,
	    int *diff)
{
  *diff = ODB_tdiff(*target_date,
		    *target_time,
		    *anal_date,
		    *anal_time);
}


PUBLIC double
ODB_jd(double Date, double Time)
{
  /* Borrowed from http://www.macho.mcmaster.ca/JAVA/JD.html ; view source */
  int Iyyyymmdd = Date;
  int Ihhmmss = Time;
  double y = Iyyyymmdd/10000;
  double m = (Iyyyymmdd % 10000)/100;
  double d = Iyyyymmdd % 100;
  double uh = Ihhmmss/10000;
  double um = (Ihhmmss % 10000)/100;
  double us = Ihhmmss % 100;
  double extra = 100.0*y + m - 190002.5;
  double rjd = 367.0*y;
  rjd -= floor(7.0*(y+floor((m+9.0)/12.0))/4.0);
  rjd += floor(275.0*m/9.0);
  rjd += d;
  rjd += (uh + (um + us/60.0)/60.)/24.0;
  rjd += 1721013.5;
  rjd -= 0.5*extra/abs(extra);
  rjd += 0.5;
  return rjd;
}

PUBLIC void
codb_jd_(const double *Date, const double *Time, double *Result)
{
  if (Date && Time && Result) *Result = ODB_jd(*Date, *Time);
}

PUBLIC double
ODB_datenum(double Date) /* Assumed format YYYYMMDD */
{ /* This is NOT EXACTLY correct interpreration of MATLAB datenum() function */
  int dtnum = 0;
  if (Date >= 0 && Date <= INT_MAX) {
    int yyyymmdd = (int)Date;
    int yyyy = yyyymmdd/10000;
    int mm = (yyyymmdd/100)%100;
    int dd = yyyymmdd%100;
    int is_leap_year = ((yyyy%400 == 0) || ((yyyy%4 == 0) && (yyyy%100 != 0)));
    static const int days_in_month[12] = {
      31, 28, 31, 
      30, 31, 30,
      31, 31, 30, 
      31, 30, 31 
    };
    static const int days_since_beginning_of_the_year[13] = {
      0, 
      31, 59, 90, 120, 151, 181,
      212, 243, 273, 304, 334, 365
    };
    if (mm >= 1 && mm <= 12 && dd >= 1 && 
	((mm != 2 && dd <= days_in_month[mm-1]) ||
	 (mm == 2 && dd <= days_in_month[mm-1] + is_leap_year))) {
      int j;
      dtnum = dd;
      if (mm > 2 && is_leap_year) dtnum++;
      dtnum += days_since_beginning_of_the_year[--mm];
    }
  }
  return (double)dtnum;
}


PUBLIC void /* Fortran callable */
codb_datenum_(const double *Date, double *Result)
{
  if (Date && Result) *Result = ODB_datenum(*Date);
}


PUBLIC double
ODB_hours_utc(double Time) /* Assumed format HHMMSS */
{
  double utc = 0;
  if (Time >= 0 && Time <= INT_MAX) {
    int rc = 0;
    int hhmmss = (int)Time;
    int hh = hhmmss/10000;
    int mm = (hhmmss/100)%100;
    int ss = hhmmss%100;
#if 0
    if (hh > 23) {
      if (allow_hour_error) hh = 23;
      else rc++;
    }
#endif
    if (ss > 59) {
      if (allow_min_error) ss = 59;
      else rc++;
    }
    if (mm > 59) {
      if (allow_min_error) mm = 59;
      else rc++;
    }
    if (rc == 0) {
      double totsec = mm * 60 + ss;
      double frac = totsec / 3600;
      utc = hh + frac;
    }
  }
  return utc;
}

PUBLIC void /* Fortran callable */
codb_hours_utc_(const double *Time, double *Result)
{
  if (Time && Result) *Result = ODB_hours_utc(*Time);
}
#ifdef STANDALONE_TEST
main(int argc, char *argv[])
{
  if (--argc != 6) {
    fprintf(stderr,"Usage: %s tgdate tgtime andate antime leftmargin rightmargin\n",argv[0]);
    exit(1);
  }
  else {
    int target_date  = atoi(argv[1]);  /* Date to be checked */   
    int target_time  = atoi(argv[2]);  /* Time to be checked */
    int anal_date    = atoi(argv[3]);  /* Analysis date in format YYYYMMDD */
    int anal_time    = atoi(argv[4]);  /* Analysis time in format   HHMMSS */
    int left_margin  = atoi(argv[5]);  /* Left bdry in format HHMMSS*/
    int right_margin = atoi(argv[6]);  /* Right bdry in format HHMMSS*/

    ODB_twindow(target_date,
		target_time,
		anal_date,
		anal_time,
		left_margin,
		right_margin);
  }
}
#endif
