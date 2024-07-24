
/* solar.c */

/* Various solar angle/time related functions */

#include "odb.h"


PUBLIC double
ODB_time_angle(double Time, double lon)
{ /* Assume time in format HHMMSS */
  /* Assume lon(gitude) in degrees [-180 .. +180] */
  double ta = 15 * (ODB_hours_utc(Time) - 12) + lon;
  if (ta <= -180) {
    ta = ta + 360;
  }else if (ta > 180){
    ta = ta - 360;
  }
  return ta;
}
PUBLIC void /* Fortran callable */
codb_time_angle_(double *Time, double *lon, double *Result)
{
  if (lon && Time && Result) *Result = ODB_time_angle(*Time, *lon);
}


PUBLIC double
ODB_hour_angle(double Time)
{ /* Assume time in format HHMMSS */
  /* Assume lon(gitude) in degrees [-180 .. +180] */
  double ha = ODB_time_angle(Time, 0);
  return ha;
}
PUBLIC void /* Fortran callable */
codb_hour_angle_(double *Time, double *Result)
{
  if (Time && Result) *Result = ODB_hour_angle(*Time);
}


PRIVATE double
fraction_through_year(double Date, double Time)
{
  double Year0101 = 10000*((int)Date/10000) +  101;
  double Year1231 = 10000*((int)Date/10000) + 1231;
  double days_per_year = ODB_datenum(Year1231) - ODB_datenum(Year0101) + 1;
  double result = (ODB_datenum(Date) - ODB_datenum(Year0101) + 10 + ODB_hours_utc(Time)/24);
  result /= days_per_year;
  return result;
}


PUBLIC double
ODB_solar_declination(double Date, double Time)
{
  double sda;
  sda = -23.45 * cos(fraction_through_year(Date, Time) * two_pi);
  return sda;
}
PUBLIC void /* Fortran callable */
codb_solar_declination_(double *Date, double *Time, double *Result)
{
  if (Date && Time && Result) *Result = ODB_solar_declination(*Date, *Time);
}


PUBLIC double
ODB_solar_zenith(double Date, double Time, double lat, double lon)
{ /* Assume date in format YYYYMMDD */
  /* Assume time in format HHMMSS */
  /* Assume lat(itude) in degrees [-90 .. +90] */
  /* Assume lon(gitude) in degrees [-180 .. +180] */
  double sza;
  double ta = ODB_time_angle(Time, lon) * pi_over_180;
  double sda = ODB_solar_declination(Date, Time) * pi_over_180;
  lat *= pi_over_180;
  sza = acos(sin(lat) * sin(sda) + cos(lat) * cos(sda) * cos(ta)) * recip_pi_over_180;
  return sza;
}
PUBLIC void /* Fortran callable */
codb_solar_zenith_(double *Date, double *Time, double *lat, double *lon, double *Result)
{
  if (lat && lon && Date && Time && Result) *Result = ODB_solar_zenith(*Date, *Time, *lat, *lon);
}


PUBLIC double
ODB_daynight(double Date, double Time, double lat, double lon)
{
  /* Input arguments : same as in ODB_solar_zenith()-function above

     Calculates day/night fraction [-1..++1] using solar zenith angle (sza)
     When sza > 90 (degrees), then its considered ~ night time

     The result is scaled so that values >= 0 == day, < 0 night.

     Note: sza's range is [0..180] degrees.
  */
  double sza = ODB_solar_zenith(Date, Time, lat, lon);
  return (double)(1 - sza/90);
}
PUBLIC void /* Fortran callable */
codb_daynight_(double *Date, double *Time, double *lat, double *lon, double *Result)
{
  if (lat && lon && Date && Time && Result) *Result = ODB_daynight(*Date, *Time, *lat, *lon);
}


PUBLIC double
ODB_solar_elevation(double Date, double Time, double lat, double lon)
{ /* Also known as "solar altitude angle" */
  /* Assume date in format YYYYMMDD */
  /* Assume time in format HHMMSS */ 
  /* Assume lat(itude) in degrees [-90 .. +90] */
  /* Assume lon(gitude) in degrees [-180 .. +180] */
  double sela = 90 - ODB_solar_zenith(Date, Time, lat, lon);
  return sela;
}
PUBLIC void /* Fortran callable */
codb_solar_elevation_(double *Date, double *Time, double *lat, double *lon, double *Result)
{
  if (lat && lon && Date && Time && Result) *Result = ODB_solar_elevation(*Date, *Time, *lat, *lon);
}


PUBLIC double
ODB_solar_azimuth(double Date, double Time, double lat, double lon)
{ /* Assume date in format YYYYMMDD */
  /* Assume time in format HHMMSS */
  /* Assume lat(itude) in degrees [-90 .. +90] */
  /* Assume lon(gitude) in degrees [-180 .. +180] */
  /* Routine gives azimuth clockwise from north.  */

  /* The working version of this routine was kindly provided 
     by Niels Bormann, ECMWF, 24-Jul-2007 ; Cheers Niels !! */ 

  double saza = RMDI;                                                    /* Az */

  double sza = ODB_solar_zenith(Date, Time, lat, lon) * pi_over_180;    /* solar zenith angle */
  double denom = sin(sza);
  double ta = ODB_time_angle(Time, lon);                                /* time angle */
  double lat_R = lat * pi_over_180;                                     /* latitude */
  double sda = ODB_solar_declination(Date, Time) * pi_over_180;         /* solar declination angle */
  double nom = sin(sda) * cos(lat_R) - cos(sda) * sin(lat_R) * cos(ta * pi_over_180);
  if (ABS(denom) < 0.001 || ABS(nom) > ABS(denom)) {
    saza = RMDI;
  }
  else {
    double frac = nom / denom;
    saza = acos(frac) * recip_pi_over_180;
    if (ta > 0) { 
      saza = -saza;
    }
  }

  return saza;
}
PUBLIC void /* Fortran callable */
codb_solar_azimuth_(double *Date, double *Time, double *lat, double *lon, double *Result)
{
  if (lat && lon && Date && Time && Result) *Result = ODB_solar_azimuth(*Date, *Time, *lat, *lon);
}
