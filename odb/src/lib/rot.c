/* rot.c */

#include "odb.h"

static const double small = 1e-6;

/*
  The sign transfer function SIGN(X,Y)  takes the sign of the second argument and
  puts it on the first argument, ABS(X)  if Y >= 0  and -ABS(X)  if Y < 0.
*/

#define SIGN(X,Y) (((Y) >= 0) ? ABS(X) : -ABS(X))

PRIVATE void
RotArea(double lat, double lon,
	double rpole1, double rpole2,
	double *Rlat, double *Rlon)
{
  /* Originates from UK MetOffice Unified Model's Fortran-routine "rotarea" */

  /*
    !-----------------------------------------------------------------------
    !
    ! subroutine    : rotarea
    !
    ! purpose       : to calculate latitude and longitude of an observation
    !               : if placed on a grid where the pole is has been moved
    !
    ! description   : the transformation formula are described in the
    !               : unified model on-line documentation paper s1
    !
    ! arguments     : lat    - i/p, real value of latitude to be rotated
    !               : lon    - i/p, real value of longitude to be rotated
    !               : rpole1 - i/p, real value of latitude pole coordinate
    !               : rpole2 - i/p, real value of longitude pole coordinate
    !               : rlat   - o/p, real value of rotated latitude
    !               : rlon   - o/p, real value of rotated longitude
    !-----------------------------------------------------------------------
  */

  /*
    !-----------------------------------------------------------------------
    ! declare real variables (in alphabetical order)
    !-----------------------------------------------------------------------
  */

  double a_lambda;            /* !- following variables declared and      */
  double a_phi;               /* !- initialized are expressions or values */
  double arg;                 /* !- of the rotated coords.                */
  double cos_phi_pole;
  double e_lambda;            /* !- used in the calculation               */
  double e_phi;
  double lambda_zero;
  double rlat;                /* !- rotated latitude                      */
  double rpole_lat;           /* !- latitude of rotated pole from rpole   */
  double rlon;                /* !- rotated longitude                     */
  double rpole_lon;           /* !- longitude of rotated pole from rpole  */
  double sin_phi_pole;
  double term1;
  double term2;

  /*
    !-----------------------------------------------------------------------
    ! initialize variables
    !-----------------------------------------------------------------------
  */

  rpole_lat=rpole1;
  rpole_lon=rpole2;
  lambda_zero=rpole_lon+180;
  sin_phi_pole=sin(pi_over_180*rpole_lat);
  cos_phi_pole=cos(pi_over_180*rpole_lat);

  /*
    !----------------------------------------------------------------------
    ! check latitude and longitude values are sensible.
    !----------------------------------------------------------------------
  */

  if (lat <= 90 && lat >= -90 && lon <= 180 && lon >= -180) {

    /*
      !----------------------------------------------------------------------
      ! scale longitude to range -180 to +180 degrees
      !----------------------------------------------------------------------
    */
    
    a_lambda=lon-lambda_zero;
    if (a_lambda >  180) a_lambda=a_lambda-360;
    if (a_lambda < -180) a_lambda=a_lambda+360;


    /*
      !----------------------------------------------------------------------
      ! convert latitude and longitude to radians
      !----------------------------------------------------------------------
    */

    a_lambda=pi_over_180*a_lambda;
    a_phi=pi_over_180*lat;
    
    /*
      !----------------------------------------------------------------------
      ! calculate rotated latitude using equation 4.6 in UM documentation
      !----------------------------------------------------------------------
    */
    
    arg=-cos_phi_pole*cos(a_lambda)*cos(a_phi)+sin(a_phi)*sin_phi_pole;
    arg=MIN(arg,1);
    arg=MAX(arg,-1);
    e_phi=asin(arg);
    rlat=recip_pi_over_180*e_phi;

    /*
      !----------------------------------------------------------------------
      ! compute rotated longitude using equation 4.6 in um documentation
      !----------------------------------------------------------------------
    */

    term1=(cos(a_phi)*cos(a_lambda)*sin_phi_pole+sin(a_phi)*cos_phi_pole);
    term2=cos(e_phi);
    
    if (term2 < small) {
      e_lambda=0;
    }
    else {
      arg=term1/term2;
      arg=MIN(arg,1);
      arg=MAX(arg,-1);
      e_lambda=recip_pi_over_180*acos(arg);
      e_lambda=SIGN(e_lambda,a_lambda);
    }

    /*
      !----------------------------------------------------------------------
      ! scale longitude to range 0 to 360 degs
      !----------------------------------------------------------------------
    */

    if (e_lambda >= 360) e_lambda=e_lambda-360;
    if (e_lambda <    0) e_lambda=e_lambda+360;

    /*
      !----------------------------------------------------------------------
      ! we want the longitude in the range -180 to 180 degs so..
      !----------------------------------------------------------------------
    */

    if (e_lambda > 180) e_lambda=e_lambda-360;
    rlon=e_lambda;
  }
  else {

    /*
      !----------------------------------------------------------------------
      ! bad lat/lon, rotated lat/lon = input lat/lon
      !----------------------------------------------------------------------
    */
    
    rlat=lat;
    rlon=lon;
  }

  if (Rlat) *Rlat = rlat;
  if (Rlon) *Rlon = rlon;
}


PUBLIC double
ODB_rotlat(double lat, double lon, double polelat, double polelon)
{
  double rlat;
  RotArea(lat,lon,polelat,polelon,&rlat,NULL);
  return rlat;
}


PUBLIC double
ODB_rotlon(double lat, double lon, double polelat, double polelon)
{
  double rlon;
  RotArea(lat,lon,polelat,polelon,NULL,&rlon);
  return rlon;
}
