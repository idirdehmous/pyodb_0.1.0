/*

$Header: /home/us/usu/dates/my/newdate/RCS/julian.h,v 1.1 1998/03/26 17:49:42 usu Exp usu $


$Revision: 1.1 $
$Date: 1998/03/26 17:49:42 $
$Source: /home/us/usu/dates/my/newdate/RCS/julian.h,v $
$Author: usu $
$Locker: usu $


$Log: julian.h,v $
 * Revision 1.1  1998/03/26  17:49:42  usu
 * Initial revision
 *


Author: Dr. Umberto Modigliani, User Support.

*/

#ifndef __julian_H__
#define __julian_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <unistd.h>
#if !defined(__alpha) && !defined(linux) && !defined(_AIX43) && !defined(CYGWIN)
#include <pfmt.h>
#endif
#include <limits.h>

#include "myhdr.h"

#if defined(linux)
#include <stdint.h>
#endif



/*******************************
 * Macros
 *******************************/


#define leap(y) (((y) % 4 == 0 && (y) % 100 != 0) || (y) % 400 == 0)


#ifdef _LANGUAGE_C_PLUS_PLUS
#define INLINE inline
#else
#define INLINE 
#endif



/*******************************
 * Types
 *******************************/


#if defined(POINTER_64) || defined(_ABI64) || defined(__uxpch__) || defined(__alpha)
	typedef long int _int64_t;
#else
	typedef long long int _int64_t;
#endif


#if defined(POINTER_64) || defined(_ABI64) || defined(__uxpch__) || defined(__alpha)
	typedef int _int32_t;
#else
	typedef long int _int32_t;
#endif

typedef short int _int16_t;

typedef _int32_t exit_t;

/*
typedef unsigned long int _u_int32_t;
typedef unsigned short int _uint16_t;
*/



typedef struct iso_date_str {
	_int32_t julian;
	_int32_t seconds;
} mydate_t;



typedef struct  date_str {  
        _int32_t     day;
        _int32_t     month;
        _int32_t     year;
} yyyymmdd_t;


typedef struct  hms_str {  
        _int32_t     hour;
        _int32_t     min;
        _int32_t     sec;
} hhmmss_t;




/*******************************
 * Externals
 *******************************/

/* None */



/*******************************
 *  Prototypes
 *******************************/ 

/* 
 *
 * Internal routines: set static
 *
 */


static exit_t   addDays(const yyyymmdd_t *const date_ptr, const _int32_t days, yyyymmdd_t *const new_date_ptr);
static exit_t   addHours(const yyyymmdd_t *const date_ptr, const hhmmss_t *const hms_ptr, const _int32_t hours,  yyyymmdd_t *const new_date_ptr, hhmmss_t *const new_hms_ptr);
static exit_t   addMinutes(const yyyymmdd_t *const date_ptr, const hhmmss_t *const hms_ptr, const _int32_t minutes, yyyymmdd_t *const new_date_ptr, hhmmss_t *const new_hms_ptr);
static exit_t   addSeconds(const yyyymmdd_t *const date_ptr, const hhmmss_t *const hms_ptr, const _int32_t seconds, yyyymmdd_t *const new_date_ptr, hhmmss_t *const new_hms_ptr);

static exit_t   dateMinusDate(const yyyymmdd_t *const date1_ptr, const yyyymmdd_t *const date2_ptr, _int32_t *const days_ptr); 
static exit_t   hour_dateMinusDate(const yyyymmdd_t *const date1_ptr, const hhmmss_t *const hms1_ptr, const yyyymmdd_t *const date2_ptr, const hhmmss_t *const hms2_ptr, _int32_t *const hours_ptr);
static exit_t   min_dateMinusDate(const yyyymmdd_t *const date1_ptr, const hhmmss_t *const hms1_ptr, const yyyymmdd_t *const date2_ptr, const hhmmss_t *const hms2_ptr, _int32_t *const minutes_ptr);
static exit_t   sec_dateMinusDate(const yyyymmdd_t *const date1_ptr, const hhmmss_t *const hms1_ptr, const yyyymmdd_t *const date2_ptr, const hhmmss_t *const hms2_ptr, _int32_t *const seconds_ptr);



static exit_t   is_hms(const hhmmss_t *const hms_ptr);
static exit_t   is_date(const yyyymmdd_t *const date_ptr);



static exit_t _addHours(const mydate_t *const fulldate_ptr, const _int32_t hours,  mydate_t *const new_fulldate_ptr);
static exit_t _addMinutes(const mydate_t *const fulldate_ptr, const _int32_t minutes, mydate_t *const new_fulldate_ptr);
static exit_t _addSeconds(const mydate_t *const fulldate_ptr, const _int32_t seconds, mydate_t *const new_fulldate_ptr);



static exit_t   julianToDate(const _int32_t julian, yyyymmdd_t *const date_ptr);
static exit_t   dateToJulian(const yyyymmdd_t *const date_ptr, _int32_t *const julian);
static exit_t   secondsToHms(const _int32_t seconds, hhmmss_t *const hms_ptr);
static exit_t   hmsToSeconds(const hhmmss_t *const hms_ptr, _int32_t *const seconds);
static exit_t   centuryToDate(const _int32_t century, yyyymmdd_t *date_ptr);
static exit_t   dateToCentury(const yyyymmdd_t *const date_ptr, _int32_t *const century_ptr);
static exit_t   dateToYearday(const yyyymmdd_t *const date_ptr, _int32_t *const yearday_ptr);
static exit_t   yeardayToDate(const _int32_t yearday, const _int32_t year, yyyymmdd_t *date_ptr);





/*******************************
 *   Constants
 *******************************/
 

static const _int16_t month_len[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};




static const _int32_t MJDSHIFT = 0;
static const _int32_t CENTURYSHIFT = 2415021;

/*
static const _int32_t MJDSHIFT = 2400000;
static const _int32_t CENTURYSHIFT = 15021;
*/



static const _int64_t   JULIAN_MIN = 0LL;


static const _int32_t   EC_OK       = 0;
static const _int32_t   EC_FALSE    = -1;

static const _int32_t   EC_WRGOPT   = -2;
static const _int32_t   EC_WRGPAR   = -3;
static const _int32_t   EC_WRGLEN   = -4;

static const _int32_t   EC_DATELEN  = -5;
static const _int32_t   EC_DATEFMT  = -6;
static const _int32_t   EC_DATEINV  = -7;
static const _int32_t   EC_TIMEINV  = -8;
static const _int32_t   EC_NAN      = -9;
static const _int32_t   EC_RANGE    = -10;


static const _int32_t   YEAR_MIN    = 0;
static const _int32_t   YEAR_MAX    = 9999;

static const _int32_t   SEC_MIN     = 60;
static const _int32_t   SEC_HOUR    = 3600;
static const _int32_t   SEC_DAY     = 86400;
static const _int32_t   MIN_HOUR    = 60;
static const _int32_t   MIN_DAY     = 1440;
static const _int32_t   HOUR_DAY    = 24;




#endif /* __julian_H__ */
