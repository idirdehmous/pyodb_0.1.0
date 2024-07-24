/* eq_regions.c */

/*
  A PARTITION OF THE UNIT SPHERE INTO REGIONS OF EQUAL AREA AND SMALL DIAMETER
  Algorithm by Paul Leopardi, School of Mathematics, University of South Wales
*/

#include "odb.h"
#include "regcache.h"
#include "result.h"
#include "cdrhook.h"

#define swap(T, vi, vj) { register T temp = vi; vi = vj; vj = temp; }

#define NINT(x) F90nint(x)

#define ERROR(msg) { if (msg) fprintf(stderr,"%s\n",msg); RAISE(SIGABRT); }

PRIVATE int
gcd(int a, int b)
{
  int m = ABS(a);
  int n = ABS(b);
  if ( m > n ) swap(int, m, n);
  return (m == 0) ? n : gcd(n%m, m);
}


PRIVATE double
my_gamma(double x)
{
  const double p0  =  0.999999999999999990e0;
  const double p1  = -0.422784335098466784e0;
  const double p2  = -0.233093736421782878e0;
  const double p3  =  0.191091101387638410e0;
  const double p4  = -0.024552490005641278e0;
  const double p5  = -0.017645244547851414e0;
  const double p6  =  0.008023273027855346e0;
  const double p7  = -0.000804329819255744e0;
  const double p8  = -0.000360837876648255e0;
  const double p9  =  0.000145596568617526e0;
  const double p10 = -0.000017545539395205e0;
  const double p11 = -0.000002591225267689e0;
  const double p12 =  0.000001337767384067e0;
  const double p13 = -0.000000199542863674e0;
  int n = NINT(x - 2);
  double w = x - (n + 2);
  double y = ((((((((((((p13 * w + p12) * w + p11) * w + p10) *
		      w + p9) * w + p8) * w + p7) * w + p6) * w + p5) *
		 w + p4) * w + p3) * w + p2) * w + p1) * w + p0;
  int k;
  if (n > 0) {
    w = x - 1;
    for (k = 2; k <= n; k++) w *= (x - k);
  }
  else {
    int nn = -n - 1;
    w = 1;  
    for (k=0; k <= nn; k++) y *= (x + k);
  }
  return w / y;
}


PRIVATE double
circle_offset(int n_top, int n_bot)
{
  /*
    !
    !CIRCLE_OFFSET Try to maximize minimum distance of center points for S^2 collars
    !
    ! Given n_top and n_bot, calculate an offset.
    !
    ! The values n_top and n_bot represent the numbers of
    ! equally spaced points on two overlapping circles.
    ! The offset is given in multiples of whole rotations, and
    ! consists of three parts;
    ! 1) Half the difference between a twist of one sector on each of bottom and top.
    ! This brings the centre points into alignment.
    ! 2) A rotation which will maximize the minimum angle between
    ! points on the two circles.
    !
  */
  double co = 
    (double)(1/(double)(n_bot) - 1/(double)(n_top))/2 + 
    (double)(gcd(n_top,n_bot))/(2*(double)(n_top)*(double)(n_bot));
  return co;
}

/* Please note that in Fortran: real*8 region(dim,2) */

#define region(i,j) reg[((j)-1)*dim + (i) - 1]

PRIVATE void
sphere_region(int dim, double reg[])
{
  /* 
     !
     ! An array of two points representing S^dim as a region.
     !
  */
  if ( dim == 1 ) {
    region(1,1)=0;
    region(1,2)=two_pi;
  }
  else if ( dim == 2 ) {
    /* 
       ! sphere_region_1 = sphere_region(dim-1);
       !  region = [[sphere_region_1(:,1); 0],[sphere_region_1(:,2); pi]] ;
    */
    region(1,1)=0;
    region(1,2)=two_pi;
    region(2,1)=0;
    region(2,2)=pi;
  }
  else {
    ERROR("sphere_region: dim > 2 not supported");
  }
}


PRIVATE void
top_cap_region(int dim, double a_cap, double reg[])
{
  /*
    !
    ! An array of two points representing the top cap of radius a_cap as a region.
    !
  */
  if ( dim == 1 ) {
    region(1,1)=0;
    region(1,2)=a_cap;
  }
  else if ( dim == 2 ) {
    /*
      ! sphere_region_1 = sphere_region(dim-1);
      ! region = [[sphere_region_1(:,1); 0], [sphere_region_1(:,2); a_cap]];
    */
    region(1,1)=0;
    region(1,2)=two_pi;
    region(2,1)=0;
    region(2,2)=a_cap;
  }
  else {
    ERROR("top_cap_region: dim > 2 not supported");
  }
}


PRIVATE void
bot_cap_region(int dim, double a_cap, double reg[])
{
  /*
    !
    ! An array of two points representing the bottom cap of radius a_cap as a region.
    !
  */
  if ( dim == 1 ) {
    region(1,1)=two_pi-a_cap;
    region(1,2)=two_pi;
  }
  else if ( dim == 2 ) {
    /*
      ! sphere_region_1 = sphere_region(dim-1);
      ! region = [[sphere_region_1(:,1); pi-a_cap],[sphere_region_1(:,2); pi]];
    */
    region(1,1)=0;
    region(1,2)=two_pi;
    region(2,1)=pi-a_cap;
    region(2,2)=pi;
  }
  else {
    ERROR("bot_cap_region: dim > 2 not supported");
  }
}


PRIVATE double
area_of_cap(int dim, double s_cap)
{
  /*
    !AREA_OF_CAP Area of spherical cap
    !
    !Syntax
    ! area = area_of_cap(dim, s_cap);
    !
    !Description
    ! AREA = AREA_OF_CAP(dim, S_CAP) sets AREA to be the area of an S^dim spherical
    ! cap of spherical radius S_CAP.
    !
    ! The argument dim must be a positive integer.
    ! The argument S_CAP must be a real number or an array of real numbers.
    ! The result AREA will be an array of the same size as S_CAP.
    !
  */
  double area;
  if ( dim == 1 ) {
    area = 2 * s_cap;
  }
  else if ( dim == 2 ) {
    area = four_pi * pow(sin(s_cap/2),2);
  }
  else {
    ERROR("area_of_cap: dim > 2 not supported");
  }
  return area;
}


PRIVATE double
area_of_collar(int dim, double a_top, double a_bot)
{
  /* 
     !
     !AREA_OF_COLLAR Area of spherical collar
     !
     !Syntax
     ! area = area_of_collar(dim, a_top, a_bot);
     !
     !Description
     ! AREA = AREA_OF_COLLAR(dim, A_TOP, A_BOT) sets AREA to be the area of
     ! an S^dim spherical collar specified by A_TOP, A_BOT, where
     ! A_TOP is top (smaller) spherical radius,
     ! A_BOT is bottom (larger) spherical radius.
     !
     ! The argument dim must be a positive integer.
     ! The arguments A_TOP and A_BOT must be real numbers or arrays of real numbers,
     ! with the same array size.
     ! The result AREA will be an array of the same size as A_TOP.
     !
  */
  return area_of_cap(dim, a_bot) - area_of_cap(dim, a_top);
}


PRIVATE double
sradius_of_cap(int dim, double area)
{
  /*
    !
    ! S_CAP = SRADIUS_OF_CAP(dim, AREA) returns the spherical radius of
    ! an S^dim spherical cap of area AREA.
    !
  */
  double radius = 0;
  if ( dim == 1 ) {
    radius = area/2;
  }
  else if ( dim == 2 ) {
    radius = 2*asin(sqrt(area/pi)/2);
  }
  else {
    ERROR("sradius_of_cap: dim > 2 not supported");
  }
  return radius;
}


PRIVATE double
area_of_sphere(int dim)
{
  /*
    ! 
    ! AREA = AREA_OF_SPHERE(dim) sets AREA to be the area of the sphere S^dim
    !
  */
  double power = (double)(dim+1)/(double)2;
  return 2*pow(pi,power)/my_gamma(power);
}


PRIVATE double
area_of_ideal_region(int dim, int n)
{
  /*
    !
    ! AREA = AREA_OF_IDEAL_REGION(dim,N) sets AREA to be the area of one of N equal
    ! area regions on S^dim, that is 1/N times AREA_OF_SPHERE(dim).
    !
    ! The argument dim must be a positive integer.
    ! The argument N must be a positive integer or an array of positive integers.
    ! The result AREA will be an array of the same size as N.
    !
  */
  double area = area_of_sphere(dim)/n;
  return area;
}


PRIVATE double
polar_colat(int dim, int n)
{
  /*
    !
    ! Given dim and N, determine the colatitude of the North polar spherical cap.
    !
  */
  double colat;
  if ( n == 1 ) colat=pi;
  else if ( n == 2 ) colat=half_pi;
  else if ( n > 2 ) {
    double area = area_of_ideal_region(dim,n);
    colat=sradius_of_cap(dim,area);
  }
  return colat;
}


PRIVATE void
cap_colats(int dim, int n, int n_collars, double c_polar,
	   const int n_regions[/* n_collars+2 */], 
	   double c_caps[/* n_collars+2 */])
{
  /*
    !
    !CAP_COLATS Colatitudes of spherical caps enclosing cumulative sum of regions
    !
    ! Given dim, N, c_polar and n_regions, determine c_caps,
    ! an increasing list of colatitudes of spherical caps which enclose the same area
    ! as that given by the cumulative sum of regions.
    ! The number of elements is n_collars+2.
    ! c_caps[1] is c_polar.
    ! c_caps[n_collars+1] is Pi-c_polar.
    ! c_caps[n_collars+2] is Pi.
    !
  */

  int subtotal_n_regions, collar_n;
  double ideal_region_area = area_of_ideal_region(dim,n);
  c_caps[0] = c_polar;
  subtotal_n_regions = 1;
  for (collar_n = 1; collar_n <= n_collars; collar_n++) {
    subtotal_n_regions += n_regions[collar_n];
    c_caps[collar_n] = sradius_of_cap(dim,subtotal_n_regions*ideal_region_area);
  }
  c_caps[n_collars+1] = pi;
}


PRIVATE void
round_to_naturals(int n, int n_collars,
		  const double r_regions[/* n_collars+2 */],
		  int n_regions[/* n_collars+2 */])
{
  /*
    !
    !ROUND_TO_NATURALS Round off a given list of numbers of regions
    !
    ! Given N and r_regions, determine n_regions,
    ! a list of the natural number of regions in each collar and the polar caps.
    ! This list is as close as possible to r_regions, using rounding.
    ! The number of elements is n_collars+2.
    ! n_regions[1] is 1.
    ! n_regions[n_collars+2] is 1.
    ! The sum of n_regions is N.
    !
  */
  int j;
  double discrepancy = 0;
  for (j=0; j<n_collars+2; j++) {
    n_regions[j] = NINT(r_regions[j]+discrepancy);
    discrepancy += r_regions[j]-n_regions[j];
  }
}


PRIVATE double
ideal_collar_angle(int dim, int n)
{
  /*
    !
    !IDEAL_COLLAR_ANGLE The ideal angle for spherical collars of an EQ partition
    !
    !Syntax
    ! angle = ideal_collar_angle(dim,N);
    !
    !Description
    ! ANGLE = IDEAL_COLLAR_ANGLE(dim,N) sets ANGLE to the ideal angle for the
    ! spherical collars of an EQ partition of the unit sphere S^dim into N regions.
    !
    ! The argument dim must be a positive integer.
    ! The argument N must be a positive integer or an array of positive integers.
    ! The result ANGLE will be an array of the same size as N.
    !
  */
  return pow(area_of_ideal_region(dim,n),(1/(double)(dim)));
}


PRIVATE void
ideal_region_list(int dim, int n, double c_polar,
		  int n_collars, double r_regions[/* n_collars+2 */])
{
  /*
    !
    !IDEAL_REGION_LIST The ideal real number of regions in each zone
    !
    ! List the ideal real number of regions in each collar, plus the polar caps.
    !
    ! Given dim, N, c_polar and n_collars, determine r_regions,
    ! a list of the ideal real number of regions in each collar,
    ! plus the polar caps.
    ! The number of elements is n_collars+2.
    ! r_regions[1] is 1.
    ! r_regions[n_collars+2] is 1.
    ! The sum of r_regions is N.
    !
  */
  r_regions[0] = 1;
  if ( n_collars > 0 ) {
    /*
      !
      ! Based on n_collars and c_polar, determine a_fitting,
      ! the collar angle such that n_collars collars fit between the polar caps.
      !
    */
    int collar_n;
    double a_fitting = (pi-2*c_polar)/n_collars;
    double ideal_region_area = area_of_ideal_region(dim,n);
    for (collar_n=1; collar_n<=n_collars; collar_n++) {
      double ideal_collar_area = area_of_collar(dim, c_polar+(collar_n-1)*a_fitting, 
						c_polar+collar_n*a_fitting);
      r_regions[collar_n] = ideal_collar_area / ideal_region_area;
    }
  } /* if ( n_collars > 0 ) */
  r_regions[n_collars+1] = 1;
}


PRIVATE int
num_collars(int n, double c_polar, double a_ideal)
{
  /*
    !
    !NUM_COLLARS The number of collars between the polar caps
    !
    ! Given N, an ideal angle, and c_polar,
    ! determine n_collars, the number of collars between the polar caps.
    !
  */
  int num_c;
  /*
    ! n_collars = zeros(size(N));
    ! enough = (N > 2) & (a_ideal > 0);
    ! n_collars(enough) = max(1,round((pi-2*c_polar(enough))./a_ideal(enough)));
  */
  Bool enough = ((n > 2) && (a_ideal > 0)) ? true : false;
  if ( enough ) {
    num_c = NINT((pi-2*c_polar)/a_ideal);
    if (num_c < 1) num_c = 1;
  }
  else {
    num_c = 0;
  }
  return num_c;
}


PRIVATE void
eq_caps(int dim, int n, 
	double s_cap[/* n */],
	int n_regions[/* n */],
	int *N_collars)
{
  int j;
  double c_polar;
  int n_collars = *N_collars;

  if ( n == 1 ) {
    /*
      !
      ! We have only one region, which must be the whole sphere.
      !
    */
    s_cap[0]=pi;
    n_regions[0]=1;
    *N_collars=0;
    return;
  }

  if ( dim == 1 ) {
    /*
      !
      ! We have a circle. Return the angles of N equal sectors.
      !
      !sector = 1:N;
      !
      ! Make dim==1 consistent with dim>1 by
      ! returning the longitude of a sector enclosing the
      ! cumulative sum of arc lengths given by summing n_regions.
      !
      !s_cap = sector*two_pi/N;
      !n_regions = ones(size(sector));
    */
    for (j=0; j<n; j++) {
      s_cap[j]=(j+1)*two_pi/(double)n;
      n_regions[j]=1;
    }
    *N_collars=0;
    return;
  }

  if ( dim == 2 ) {
    /*
      !
      ! Given dim and N, determine c_polar
      ! the colatitude of the North polar spherical cap.
      !
    */

    c_polar = polar_colat(dim,n);

    /*
      !
      ! Given dim and N, determine the ideal angle for spherical collars.
      ! Based on N, this ideal angle, and c_polar,
      ! determine n_collars, the number of collars between the polar caps.
      !
    */

    n_collars = *N_collars = num_collars(n,c_polar,ideal_collar_angle(dim,n));

    /*
      !
      ! Given dim, N, c_polar and n_collars, determine r_regions,
      ! a list of the ideal real number of regions in each collar,
      ! plus the polar caps.
      ! The number of elements is n_collars+2.
      ! r_regions[1] is 1.
      ! r_regions[n_collars+2] is 1.
      ! The sum of r_regions is N.
      !
      ! r_regions = ideal_region_list(dim,N,c_polar,n_collars)
    */

    {
      double *r_regions = NULL;
      CALLOC(r_regions,n_collars+2);
      ideal_region_list(dim,n,c_polar,n_collars,r_regions);
      
      /*
	!
	! Given N and r_regions, determine n_regions,
	! a list of the natural number of regions in each collar and
	! the polar caps.
	! This list is as close as possible to r_regions.
	! The number of elements is n_collars+2.
	! n_regions[1] is 1.
	! n_regions[n_collars+2] is 1.
	! The sum of n_regions is N.
	!
	! n_regions = round_to_naturals(N,r_regions)
      */
      
      round_to_naturals(n,n_collars,r_regions,n_regions);
      FREE(r_regions);
    }
    
    /*
      !
      ! Given dim, N, c_polar and n_regions, determine s_cap,
      ! an increasing list of colatitudes of spherical caps which enclose the same area
      ! as that given by the cumulative sum of regions.
      ! The number of elements is n_collars+2.
      ! s_cap[1] is c_polar.
      ! s_cap[n_collars+1] is Pi-c_polar.
      ! s_cap[n_collars+2] is Pi
      ! 
      ! s_cap = cap_colats(dim,N,c_polar,n_regions)
    */

    cap_colats(dim,n,n_collars,c_polar,n_regions,s_cap);
  }
}


#define regions(i,j,k)   regs  [ ((k)-1)*dim*2     + ((j)-1)*dim     + (i) - 1 ]
#define regions_1(i,j,k) regs_1[ ((k)-1)*(dim-1)*2 + ((j)-1)*(dim-1) + (i) - 1 ]

PRIVATE void
eq_regions(int recur, int dim, int n, double regs[])
{
  /*
    !
    ! REGIONS = EQ_REGIONS(dim,N) uses the recursive zonal equal area sphere
    ! partitioning algorithm to partition S^dim (the unit sphere in dim+1
    ! dimensional space) into N regions of equal area and small diameter.
    !
    ! The arguments dim and N must be positive integers.
    !
    ! The result REGIONS is a (dim by 2 by N) array, representing the regions
    ! of S^dim. Each element represents a pair of vertex points in spherical polar
    ! coordinates.
    ! 
    ! Each region is defined as a product of intervals in spherical polar
    ! coordinates. The pair of vertex points regions(:,1,n) and regions(:,2,n) give
    ! the lower and upper limits of each interval.
    !
  */

  double *regs_1 = NULL;
  double *s_cap = NULL, r_top[2], r_bot[2], c_top, c_bot, offset;
  int *n_regions = NULL, collar_n, n_collars, n_in_collar, region_1_n, region_n;
  int i, j, k;
  DRHOOK_START_RECUR(eq_regions,recur);

  CALLOC(s_cap, n+2);
  CALLOC(n_regions, n+2);

  if ( n == 1 ) {
    /*
      !
      ! We have only one region, which must be the whole sphere.
      !
      ! regions(:,:,1)=sphere_region(dim)
    */
    sphere_region(dim,&regions(1,1,1));
    goto finish;
  }

  /*
    !
    ! Start the partition of the sphere into N regions by partitioning
    ! to caps defined in the current dimension.
    !
    ! [s_cap, n_regions] = eq_caps(dim,N)
  */

  eq_caps(dim,n,s_cap,n_regions,&n_collars);

  /*
    !
    ! s_cap is an increasing list of colatitudes of the caps.
    !
  */

  if ( dim == 1 ) {
    /*
      !
      ! We have a circle and s_cap is an increasing list of angles of sectors.
      !
      !
      ! Return a list of pairs of sector angles.
      !
    */

    for (k=1; k<=n; k++)
      for (j=1; j<=2; j++)
	for (i=1; i<=dim; i++)
	  regions(i,j,k) = 0;

    for (k=2; k<=n; k++) {
      regions(1,1,k) = s_cap[k-2];
    }

    for (k=1; k<=n; k++) {
      regions(1,2,k) = s_cap[k-1];
    }
  }
  else {
    /*
      !
      ! We have a number of zones: two polar caps and a number of collars.
      ! n_regions is the list of the number of regions in each zone.
      !
      ! n_collars = size(n_regions,2)-2
      !
      ! Start with the top cap
      !
      ! regions(:,:,1) = top_cap_region(dim,s_cap(1))
    */
    
    top_cap_region(dim,s_cap[0],&regions(1,1,1));
    region_n = 1;

    /*
      !
      ! Determine the dim-regions for each collar
      !
    */

    if ( dim == 2 ) offset=0;

    for (collar_n=1; collar_n<=n_collars; collar_n++) {
      int size_regions_1_3;
      /*
	!
	! c_top is the colatitude of the top of the current collar.
	!
      */

      c_top = s_cap[collar_n-1];

      /*
	!
	! c_bot is the colatitude of the bottom of the current collar.
	!
      */

      c_bot = s_cap[collar_n];

      /*
	!
	! n_in_collar is the number of regions in the current collar.
	!
      */

      n_in_collar = n_regions[collar_n];

      /*
	!
	! The top and bottom of the collar are small (dim-1)-spheres,
	! which must be partitioned into n_in_collar regions.
	! Use eq_regions recursively to partition the unit (dim-1)-sphere.
	! regions_1 is the resulting list of (dim-1)-region pairs.
	!
	! regions_1 = eq_regions(dim-1,n_in_collar)
      */

      size_regions_1_3 = n_in_collar;
      ALLOC(regs_1,(dim-1)*2*n_in_collar);
      eq_regions(1, dim-1,n_in_collar,regs_1);

      /*
	!
	! Given regions_1, determine the dim-regions for the collar.
	! Each element of regions_1 is a (dim-1)-region pair for the (dim-1)-sphere.
      */

      if ( dim == 2 ) {
	/*
	  !
	  !  The (dim-1)-sphere is a circle
	  !  Offset each sector angle by an amount which accumulates over
	  !  each collar.
	*/

	for (region_1_n=1; region_1_n<=n_in_collar; region_1_n++) {
	  /*
	    !
	    ! Top of 2-region
	    ! The first angle is the longitude of the top of
	    ! the current sector of regions_1, and
	    ! the second angle is the top colatitude of the collar
	    !
	    ! r_top = [mod(regions_1(1,1,region_1_n)+two_pi*offset,two_pi); c_top]
	  */

	  r_top[0]=fmod(regions_1(1,1,region_1_n)+two_pi*offset,two_pi);
	  r_top[1]=c_top;

	  /*
	    !
	    ! Bottom of 2-region
	    ! The first angle is the longitude of the bottom of
	    ! the current sector of regions_1, and
	    ! the second angle is the bottom colatitude of the collar
	    !
	    ! r_bot = [mod(regions_1(1,2,region_1_n)+two_pi*offset,two_pi); c_bot]
	  */

	  r_bot[0]=fmod(regions_1(1,2,region_1_n)+two_pi*offset,two_pi);
	  r_bot[1]=c_bot;
	  if ( r_bot[0] <= r_top[0] ) {
	    r_bot[0] += two_pi;
	  }
	  region_n++;
	  /* regions(:,:,region_n) = [r_top,r_bot] */
	  regions(1,1,region_n) = r_top[0];
	  regions(1,2,region_n) = r_bot[0];
	  regions(2,1,region_n) = r_top[1];
	  regions(2,2,region_n) = r_bot[1];
	}

	/*
	  !
	  ! Given the number of sectors in the current collar and
	  ! in the next collar, calculate the next offset.
	  ! Accumulate the offset, and force it to be a number between 0 and 1.
	  ! 
	*/

	offset += circle_offset(n_in_collar,n_regions[1+collar_n]);
	offset -= floor(offset);
      }
      else {
	for (region_1_n=1; region_1_n <= size_regions_1_3; region_1_n++) {
	  region_n++;

	  /*
	    !
	    ! Dim-region;
	    ! The first angles are those of the current (dim-1) region of regions_1.
	    !
	  */

	    for (j=1; j<=2; j++)
	      for (i=1; i<=dim-1; i++)
		regions(i,j,region_n) = regions_1(i,j,region_1_n);

	  /*
	    !
	    ! The last angles are the top and bottom colatitudes of the collar.
	    !
	    ! regions(dim,:,region_n) = [c_top,c_bot]
	  */

	  regions(dim,1,region_n) = c_top;
	  regions(dim,2,region_n) = c_bot;
	}
      }
      FREE(regs_1);
    } /* for (collar_n=1; collar_n<=n_collars; collar_n++) */

    /*
      !
      ! End with the bottom cap.
      !
      ! regions(:,:,N) = bot_cap_region(dim,s_cap(1))
    */

    bot_cap_region(dim,s_cap[0],&regions(1,1,n));
  }

  FREE(s_cap);
  FREE(n_regions);
 finish:
  DRHOOK_END_RECUR(n,recur);
}


static const int dim = 2;

/* Cached regs, per each resolution encountered, per thread-id */

#define eq_cache_t regcache_t

static eq_cache_t **eq_cache = NULL;

PUBLIC eq_cache_t **
ODBc_init_eq_cache()
{
  if (!eq_cache) {
    DEF_INUMT;
    CALLOC(eq_cache, inumt);
  }
  return eq_cache;
}


PUBLIC regcache_t *
ODBc_put_regcache(regcache_kind kind,
		  int n, int nb,
		  double *latband, double *midlat,
		  double *stlon, double *deltalon, 
		  int *loncnt,
		  regcache_t **(*init_code)(void), regcache_t **regcache)
{
  DEF_IT;
  regcache_t *p;
  DRHOOK_START(ODBc_put_regcache);
  {
    FILE *fp = ODBc_get_debug_fp();
    if (fp) {
      int jb;
      for (jb=0; jb<nb; jb++) {
	fprintf(fp, "[%d] latband=[%.14g .. %.14g%s, midlat=%.14g, "
		"stlon=%.14g, deltalon=%.14g, loncnt=%d\n",
		jb, latband[jb], latband[jb+1],
		(jb < nb-1) ? ")" : "]", midlat[jb],
		stlon[jb], deltalon[jb], loncnt[jb]);
      }
    }
  }
  CALLOC(p,1);
  p->kind = kind;
  p->n = n;
  p->nb = nb;
  p->latband = latband;
  p->midlat = midlat;
  p->stlon = stlon;
  p->deltalon = deltalon;
  p->loncnt = loncnt;
  {
    int jb, sum = 0;
    CALLOC(p->sum_loncnt, nb);
    for (jb=0; jb<nb; jb++) {
      p->sum_loncnt[jb] = sum;
      sum += loncnt[jb];
    }
    p->nboxes = sum;
  }
  p->last.jb = -1;
  p->last.lonbox = -1;
  p->last.boxid = -1;
  {
    FILE *fp = ODBc_get_debug_fp();
    if (fp) {
      int jb;
      fprintf(fp,
	      "ODBc_put_regcache(kind=%d ('%s'), n=%d, nb=%d, p->nboxes=%d, latband=%p, "
	      "midlat=%p, stlon=%p, deltalon=%p, loncnt=%p)\n",
	      (int)kind, regcache_names[(int)kind], n, nb, p->nboxes, latband, 
	      midlat, stlon, deltalon, loncnt);

      for (jb=0; jb<nb; jb++) {
	fprintf(fp,"Band#%d = [ %.4f .. %.4f ] with midlat=%.4f, loncnt = %d, sum_loncnt = %d, "
		"lon-starts at %.4f with delta = %.4f\n",
		jb+1,latband[jb],latband[jb+1],midlat[jb],
		loncnt[jb],p->sum_loncnt[jb],
		stlon[jb],deltalon[jb]);
      }
    }
  }
  if (!regcache) regcache = init_code(); /* Normally not called */
  if (regcache[IT]) {
    regcache_t *top = regcache[IT];
    while (top) {
      regcache_t *next = NULL;
      if (n < top->n) {
	if (!top->left) { top->left = p; } else { next = top->left; }
      }
      else {
	if (!top->right) { top->right = p; } else { next = top->right; }
      }
      top = next; /* If next == NULL => new position in tree found => get out */
    } /* while (top) */
  }
  else {
    regcache[IT] = p;
  }
  DRHOOK_END(n);
  return p;
}


PUBLIC eq_cache_t *
ODBc_get_eq_cache(int n)
{
  DEF_IT;
  eq_cache_t *p = NULL;
  DRHOOK_START(ODBc_get_eq_cache);
  {
    if (!eq_cache) eq_cache = ODBc_init_eq_cache(); /* Normally not called */
    p = eq_cache[IT];

    while (p) {
      if (n == p->n || p->kind == eq_cache_kind) {
	/* Matched */
	break;
      }
      p = (n < p->n) ? p->left : p->right;
    } /* while (p) */

    if (!p) {

      /* This is done only once per "n", per "it" */

      double *regs = NULL;
      int jb, nb = 0;
      double *latband = NULL;
      double *midlat = NULL;
      int *loncnt = NULL;
      double *stlon = NULL;
      double *deltalon = NULL;

      CALLOC(regs,dim*2*n);
      eq_regions(0, dim,n,&regions(1,1,1));

      {
	/* 
	   After eq_regions() function call, latitudes & longitudes are in radians
	   and between [ 0 .. pi ] and [ 0 .. two_pi ], respectively.

	   Pay special attention to cap regions (j=1 & j=n), where starting and
	   ending longitude has the same constant value; make them -180 and +180, respectively .
	*/

	int j;
	for (j=1; j<=n; j++) {
	  /* In radians */
	  double startlon = regions(1,1,j);
	  double startlat = regions(2,1,j);
	  double endlon   = regions(1,2,j);
	  double endlat   = regions(2,2,j);

	  /* shift latitudes to start from -half_pi, not 0 */
	  startlat -= half_pi;
	  endlat -= half_pi;

	  if (j == 1 || j == n) { /* cap regions */
	    endlon = startlon;
	  }

	  /* Replace values in regions, still in radians */
	  regions(1,1,j) = startlon;
	  regions(2,1,j) = startlat;
	  regions(1,2,j) = endlon;
	  regions(2,2,j) = endlat;
	}
      }

      {
	/*
	  Derive how many (nb) latitude bands we have i.e. keep track how often 
	  the consecutive (startlat,endlat)-pairs change their value.
	*/
	
	int j = 1;
	int ncnt;
	double three_sixty = 360;
	double last_startlat = regions(2,1,j);
	double last_endlat   = regions(2,2,j);

	nb = 1; /* Southern polar cap */
	for (j=2; j<n; j++) { /* All but caps */
	  double startlat = regions(2,1,j);
	  double endlat   = regions(2,2,j);
	  if (last_startlat == startlat && last_endlat == endlat) {
	    /* No change i.e. we are still in the same latitude band */
	    continue;
	  }
	  else {
	    /* New latitude band */
	    ++nb;
	    last_startlat = startlat;
	    last_endlat   = endlat;
	  }
	} /* for (j=2; j<n; j++) */
	if (n > 1) { /* Northern polar cap */
	  ++nb;
	}

	/* 
	   We have now in total "nb" latitude bands.
	   Lets store their starting and ending latitudes for much faster lookup.
	*/

	ALLOC(latband, nb + 1);
	CALLOC(loncnt, nb);
	CALLOC(stlon, nb);
	CALLOC(deltalon, nb);

	jb = -1;

	j = 1;
	last_startlat = regions(2,1,j);
	last_endlat   = regions(2,2,j);
	loncnt[++jb] = 1;
	latband[jb] = -R2D(last_startlat); /* Reverse latitudes from S->N to N->S pole */

	for (j=2; j<n; j++) { /* All but caps */
	  double startlat = regions(2,1,j);
	  double endlat   = regions(2,2,j);
	  if (last_startlat == startlat && last_endlat == endlat) {
	    /* Increase longitude count by one for this latitude band */
	    loncnt[jb]++;
	  }
	  else {
	    /* For previous latitude band ... */
	    ncnt = loncnt[jb];
	    deltalon[jb] = three_sixty/ncnt;
	    stlon[jb] = -deltalon[jb]/2; /* Starting longitude at 0 GMT minus half longitudinal delta */
	    /* New latitude band */
	    last_startlat = startlat;
	    last_endlat   = endlat;
	    loncnt[++jb] = 1;
	    latband[jb] = -R2D(last_startlat); /* Reverse latitudes from S->N to N->S pole */
	  }
	} /* for (j=2; j<n; j++) */

	if (n > 1) { /* Northern polar cap (mapped to be the South pole, in fact) */
	  ncnt = loncnt[jb];
	  deltalon[jb] = three_sixty/ncnt;
	  stlon[jb] = -deltalon[jb]/2; /* Starting longitude at 0 GMT minus half longitudinal delta */
	  j = n;
	  last_startlat = regions(2,1,j);
	  last_endlat   = regions(2,2,j);
	  loncnt[++jb] = 1;
	  latband[jb] = -R2D(last_startlat); /* Reverse latitudes from S->N to N->S pole */
	}

	{
	  ncnt = loncnt[jb];
	  deltalon[jb] = three_sixty/ncnt;
	  stlon[jb] = -deltalon[jb]/2; /* Starting longitude at 0 GMT minus half longitudinal delta */
	  j = n+1;
	  latband[++jb] = -R2D(last_endlat); /* Reverse latitudes from S->N to N->S pole */
	}
      }

      /* Release space allocated by regs */

      FREE(regs);

      /* Calculate mid latitudes for each band by a simple averaging process */

      ALLOC(midlat, nb);
      for (jb=0; jb<nb; jb++) {
	midlat[jb] = (latband[jb] + latband[jb+1])/2;
      }

      /* Store in cache */
      p = ODBc_put_regcache(eq_cache_kind,
			    n, nb, 
			    latband, midlat,
			    stlon, deltalon, 
			    loncnt,
			    ODBc_init_eq_cache, eq_cache);

      p->resol = ODB_eq_resol(n);
    }
  }
  DRHOOK_END(n);
  return p;
}


PUBLIC int
ODBc_bsearch(const double key,
	     const int n, 
	     const double x[ /* with n elements */ ],
	     const double sign /* +1 forward and -1 for reverse search */)
{
  int lo = 0;
  int hi = n-1;
  while (lo <= hi) {
    int k = (lo+hi)/2;
    double xk = sign*x[k];
    if (key == xk) {
      /* A match has been found */
      return k;
    }
    else if (key < xk) {
      /* Search the lower section */
      hi = k-1;
    }
    else {
      /* Search the upper section */
      lo = k+1;
    }
  } /* while (lo <= hi) */
  /* No EXACT match */
  return -1;
}


PUBLIC int
ODBc_interval_bsearch(const double key, 
		      const int n, 
		      const double x[ /* with n+1 elements */ ],
		      const double *delta, /* if present, only x[0] will be used */
		      const double add,
		      const double sign /* +1 forward and -1 for reverse search */)
{
  const double eps = 1.0e-7;
  double Delta = delta ? *delta : 0;
  Bool wrap_around = (delta && add != 0) ? true : false;  
  double halfDelta = Delta/2;
  double x0 = x[0] + halfDelta;
  double Key = wrap_around ? sign*fmod(key + halfDelta + add, add) : sign*(key + halfDelta);
  int lo = 0, hi = n-1;
  while (lo <= hi) {
    int k = (lo+hi)/2;
    double xk = wrap_around ? sign*fmod(x0 + k*Delta + add, add) : sign*(x[k] + halfDelta);
    if (wrap_around && k > 0 && ABS(xk) < eps) xk = add;
    if (Key < xk) {
      /* Search the lower section */
      hi = k-1;
    }
    else {
      int kp1 = k+1;
      double xkp1 = wrap_around ? sign*fmod(x0 + kp1*Delta + add, add) : sign*(x[kp1] + halfDelta);
      if (wrap_around && ABS(xkp1) < eps) xkp1 = add;
      if (Key > xkp1) {
	/* Search the upper section */
	lo = kp1;
      }
      else {
	/* The interval has been found */
	return k;
      }
    }
  } /* while (lo <= hi) */
#if 0
  if (wrap_around) {
    /* Check if between [-halfDelta,0] */
    FILE *fp = ODBc_get_debug_fp();
    Key = fmod(key + add, add);
    ODB_fprintf(fp, "key = %.14g, add = %.14g --> Key = %.14g\n",key,add,Key);
    halfDelta = -Delta/2;
    ODB_fprintf(fp, "halfDelta = %.14g --> ",halfDelta);
    halfDelta = fmod(halfDelta + add, add);
    ODB_fprintf(fp, "halfDelta = %.14g\n",halfDelta);
    if (Key >= halfDelta && Key <= add) return 0; /* First box */
    ODB_fprintf(fp, "\t%.14g not between %.14g and %.14g\n",Key,halfDelta,add);
    halfDelta = Delta/2;
    if (Key >= 0 && Key <= halfDelta) return 0; /* First box */
    ODB_fprintf(fp, "\t%.14g not between %.14g and %.14g\n",Key,(double)0,halfDelta);
  }
#endif
  /* No INTERVAL match */
  return -1;
}


PRIVATE int
find_latband(regcache_t *p, const double lat)
{
  int jb, lastjb = p->last.jb;
  if (lastjb >= 0 &&
      /* Note: In the following we go from N->S pole, not S->N ==> thus the minus sign!! */
      -lat >= -p->latband[lastjb] && -lat < -p->latband[lastjb+1]) {
    jb = lastjb;
  }
  else if (lastjb == p->nb - 1 && lat == p->latband[lastjb+1]) {
    /* Exactly at the South pole */
    jb = lastjb;
  }
  else {
    jb = ODBc_interval_bsearch(lat, p->nb, p->latband, NULL, 0, -1);
    if (jb != -1) {
      /* New latitude band found */
      p->last.jb = jb;
      /* Don't know anything about the final box numbers yet */
      p->last.lonbox = -1;
      p->last.boxid = -1;
    }
    {
      FILE *fp = ODBc_get_debug_fp();
      if (fp) {
	fprintf(fp,"find_latband: jb = %d",jb);
	fprintf(fp,", p->nb = %d, lat = %.4f", p->nb, lat);
	if (jb != -1) {
	  fprintf(fp,", p->midlat[jb] = %.4f", p->midlat[jb]);
	  if (jb > 0) fprintf(fp,", p->latband[jb-1] = %.4f",p->latband[jb-1]);
	  fprintf(fp,", p->latband[jb] = %.4f",p->latband[jb]);
	  if (jb < p->nb-1) fprintf(fp,", p->latband[jb+1] = %.4f",p->latband[jb+1]);
	}
	fprintf(fp,"\n");
      }
    }
  }
  return jb;
}


PRIVATE int
find_lonbox(regcache_t *p, const int jb, const double lon, 
	    double *midlon,
	    double *leftlon, double *rightlon)
{
  const double three_sixty = 360;
  double mid   = RMDI;
  double left  = RMDI;
  double right = RMDI;
  int boxid = -1;
  if (jb >= 0 && jb < p->nb) {
    double Lon = fmod(lon + three_sixty, three_sixty);
    double deltalon = p->deltalon[jb];

    if (p->last.boxid >= 0) {
      int k = p->last.lonbox;
      double startlon = p->stlon[jb] + k * deltalon;
      double endlon = p->stlon[jb] + (k+1) * deltalon;
      startlon = fmod(startlon + three_sixty, three_sixty);
      endlon = fmod(endlon + three_sixty, three_sixty);
      if (endlon >= startlon) {
	if (Lon >= startlon && Lon < endlon) boxid = p->last.boxid;
      }
      else {
	if ((Lon >= 0 && Lon < endlon) || (Lon >= startlon && Lon < three_sixty)) boxid = p->last.boxid;
      }
    }

    if (boxid == -1) {
      const double *stlon = &p->stlon[jb];
      int loncnt = p->loncnt[jb];
      int lonbox = ODBc_interval_bsearch(lon, loncnt, stlon, &deltalon, three_sixty, +1);
      if (lonbox != -1) {
	int k = lonbox;
	left = (*stlon) + k * deltalon;
	mid = (*stlon) + (k+0.5) * deltalon;
	right = (*stlon) + (k+1) * deltalon;

	p->last.left  = left  = fmod(left + three_sixty, three_sixty);
	p->last.mid   = mid   = fmod(mid + three_sixty, three_sixty);
	p->last.right = right = fmod(right + three_sixty, three_sixty);
	
	p->last.jb = jb;
	p->last.lonbox = k;
	p->last.boxid = boxid = p->sum_loncnt[jb] + k;
      }
      {
	FILE *fp = ODBc_get_debug_fp();
	if (fp) {
	  fprintf(fp,"find_lonbox: boxid = %d, jb = %d",boxid, jb);
	  fprintf(fp,", loncnt = %d, lon/Lon = %.4f/%.4f", loncnt, lon, Lon);
	  fprintf(fp,", *stlon = %.4f, deltalon = %.4f, lonbox = %d, left/mid/right = %.14g/%.14g/%.14g\n", 
		  *stlon, deltalon, lonbox, 
		  left, mid, right); 
	}
      }
    }
    else {
      left  = p->last.left;
      mid   = p->last.mid;
      right = p->last.right;
    }
  }
  if (midlon) {
    const double one_eighty = 180;
    if (mid > one_eighty) mid -= three_sixty;
    *midlon = mid;
  }
  if (leftlon) *leftlon = left;
  if (rightlon) *rightlon = right;
  return boxid;
}


/* Please note that in the subsequent routines (lat,lon,resol)
   are all assumed to be in degrees, not radians */

const double sphere_area = four_pi; /* Actually: 4 * pi * R^2 */
const double min_resol = 0.1e0; /* => Smallest patch ~ 0.1-by-0.1 degree^2 */

PUBLIC double ODB_eq_min_resol() { return min_resol; }

PUBLIC double
ODB_eq_area(double rn)
{
  int n = rn;
  double eq_area = (sphere_area/n);
  return eq_area;
}


PUBLIC double
ODB_eq_area_with_resol(double resol)
{
  return Func_eq_area(resol); /* from odb.h */
}


PUBLIC double
ODB_eq_resol(double rn)
{
  double eq_area = ODB_eq_area(rn);
  double resol = sqrt(eq_area) * recip_pi_over_180; /* In degrees ~ at Equator for small resol's */
  if (resol < min_resol) resol = min_resol;
  return resol;
}


PUBLIC double
ODB_eq_n(double resol)
{
  double dres = ( resol <  min_resol ? min_resol : resol ) * pi_over_180;
  double eq_area = dres * dres ; /* Actually: (resol*pi/180)^2 * R^2 (approx.) */
  int n = NINT(sphere_area/eq_area); /* Note: R^2's would have been divided away */
  return n;
}


PUBLIC double ODB_eq_max_n() { return ODB_eq_n(min_resol); }


PUBLIC int
ODBc_get_boxid(double lat, double lon, int n, 
	      regcache_t *ptr, regcache_t *(*get_cache)(int))
{
  int boxid = -1; 
  DRHOOK_START(ODBc_get_boxid);
  {
    regcache_t *p = ptr ? ptr : get_cache(n);
    int jb = find_latband(p, lat);
    boxid = find_lonbox(p, jb, lon, NULL, NULL, NULL);
  }
  DRHOOK_END(0);
  return boxid;
}


PUBLIC double
ODB_eq_boxid(double lat, double lon, double rn)
{
  return ODBc_get_boxid(lat, lon, rn, NULL, ODBc_get_eq_cache);
}


PUBLIC double
ODB_eq_boxid_with_resol(double lat, double lon, double resol)
{
  return Func_eq_boxid(lat, lon, resol); /* from odb.h */
}


PUBLIC double
ODBc_get_boxlat(double lat, double lon, double rn,
		regcache_t *(*get_cache)(int))
{
  double res = RMDI;
  DRHOOK_START(ODBc_get_boxlat);
  {
    int n = rn;
    regcache_t *p = get_cache(n);
    int jb = find_latband(p, lat);
    if (jb >= 0 && jb < p->nb) {
      res = p->midlat[jb];
    }
  }
  DRHOOK_END(0);
  return res;
}


PUBLIC double
ODB_eq_boxlat(double lat, double lon, double rn)
{
  return ODBc_get_boxlat(lat, lon, rn, ODBc_get_eq_cache);
}


PUBLIC double
ODB_eq_boxlat_with_resol(double lat, double lon, double resol)
{
  return Func_eq_boxlat(lat, lon, resol); /* from odb.h */
}


PUBLIC double
ODBc_get_boxlon(double lat, double lon, double rn,
		regcache_t *(*get_cache)(int))
{
  double res = RMDI;
  DRHOOK_START(ODBc_get_boxlon);
  {
    int n = rn;
    regcache_t *p = get_cache(n);
    int jb = find_latband(p, lat);
    int boxid = find_lonbox(p, jb, lon, &res, NULL, NULL);
  }
  DRHOOK_END(0);
  return res;
}


PUBLIC double
ODB_eq_boxlon(double lat, double lon, double rn)
{
  return ODBc_get_boxlon(lat, lon, rn, ODBc_get_eq_cache);
}


PUBLIC double
ODB_eq_boxlon_with_resol(double lat, double lon, double resol)
{
  return Func_eq_boxlon(lat, lon, resol); /* from odb.h */
}


PUBLIC double
ODB_eq_truearea(double lat, double lon, double rn)
{
  double truearea = RMDI;
  int n = rn;
  double startlon;
  double   endlon;
  eq_cache_t *p = ODBc_get_eq_cache(n);
  int jb = find_latband(p, lat);
  int boxid = find_lonbox(p, jb, lon, NULL, &startlon, &endlon);
  if (jb >= 0 && jb < p->nb &&
      boxid >= 0 && boxid < n) {
    double startlat = p->latband[jb];    /* Starting latitude, range = [ -90 .. 90 ] */
    double   endlat = p->latband[jb+1];  /* Ending latitude  , range = [ -90 .. 90 ] */
    const double three_sixty = 360;
    if (endlon < startlon) endlon += three_sixty;
    truearea = 
      (sin(pi_over_180 * endlat) - sin(pi_over_180 * startlat)) *
      (endlon - startlon) * pi_over_180; /* Note: R^2 left away */
  }
  return truearea;
}


PUBLIC double
ODB_eq_truearea_with_resol(double lat, double lon, double resol)
{
  return Func_eq_truearea(lat, lon, resol); /* from odb.h */
}


PUBLIC double
ODB_eq_latband(double bandnum, double rn)
{
  double res = RMDI;
  int band_no = bandnum;
  int n = rn;
  eq_cache_t *p = ODBc_get_eq_cache(n);
  if (band_no >= 1 && band_no <= p->nb + 1) {
    res = p->latband[--band_no];
  } 
  return res;
}


PUBLIC double
ODB_eq_latband_with_resol(double bandnum, double resol)
{
  return Func_eq_latband(bandnum, resol);  /* from odb.h */
}


/* The following may become obsolete (moved from orlist.c) */


PRIVATE
int boxid(double x, double deltax, double xmin, double xmax,
	  Bool wrap_around)
{
  int id = NMDI;
  if (deltax < 0) deltax = -deltax;
  if (deltax != 0 && xmin < xmax) {
    double xspan = xmax - xmin;
    int n = (int)(xspan/deltax);
    id = (int)((x - xmin)/deltax);
    if (id < 0 || id >= n) {
      if (wrap_around) {
	if (id < 0) id = n-1;
	else if (id >= n) id = 0;
      }
      else {
	if (id < 0) id = 0;
	else if (id >= n) id = n-1;
      }
    }
  }
  return id;
}

PRIVATE
double box(double x, double deltax, double xmin, double xmax,
	   Bool wrap_around)
{
  double value = RMDI;
  if (deltax < 0) deltax = -deltax;
  if (deltax != 0 && xmin < xmax) {
    int id = boxid(x,deltax,xmin,xmax,wrap_around);
    value = xmin + id * deltax + 0.5 * deltax;
  }
  return value;
}

PUBLIC
double ODB_boxid_lat(double lat, double deltalat)
{
  const double latmax = 90;
  return boxid(lat,deltalat,-latmax,+latmax,false);
}

PUBLIC
double ODB_boxlat(double lat, double deltalat)
{
  const double latmax = 90;
  return box(lat,deltalat,-latmax,+latmax,false);
}

PUBLIC
double ODB_boxid_lon(double lon, double deltalon)
{
  const double lonmax = 180;
  return boxid(lon,deltalon,-lonmax,+lonmax,true);
}

PUBLIC
double ODB_boxlon(double lon, double deltalon)
{
  const double lonmax = 180;
  return box(lon,deltalon,-lonmax,+lonmax,true);
}

PUBLIC 
double ODB_boxid(double lat, double lon, double deltalat, double deltalon)
{
  const double latmax = 90;
  int nlat = ODB_boxid_lat(latmax,deltalat);
  int idlat = ODB_boxid_lat(lat,deltalat);
  int idlon = ODB_boxid_lon(lon,deltalon);
  int id = idlon * nlat + idlat;
  return id;
}


#if 0
#ifdef TESTING

/* Testing, testing, ... */

static void 
prt(FILE *fp, double resol, eq_cache_t *p)
{
  if (fp && p) {
    fprintf(fp,"++++++++++++++++++++ n=%d, resol=%.20g\n",p->n,resol);
    {
#if 1
      int jb, nb = p->nb;
      for (jb=0; jb<nb; jb++) {
	double avglat = (p->latband[jb] + p->latband[jb+1])/2;
	int j;
	int j1 = p->jband[jb];
	int j2 = p->jband[jb+1]-1;
	fprintf(fp,
		"---> Band#%d/%d: j1=%d, j2=%d, latband=[%10.4f .. %10.4f]\n",
		jb+1,nb,j1,j2,p->latband[jb],p->latband[jb+1]);
	for (j=j1; j<=j2; j++) {
	  double startlon = p->stlon[j];
	  double endlon   = (p->stlon[j+1] < startlon) ? p->stlon[j+1] + 360 : p->stlon[j+1];
	  double avglon = (startlon + endlon)/2;
	  int boxid = ODB_eq_boxid(avglat, avglon, resol);
	  if (avglon > 180) avglon -= 360;
	  fprintf(fp,"regions(:,:,%d) = [boxid=%d, avg (lat,lon)=(%10.4f,%10.4f)]\n",
		  j+1,boxid,avglat,avglon);
	  fprintf(fp,"\tlat = [%10.4f .. %10.4f]\tlon = [%10.4f .. %10.4f]\n",
		  p->latband[jb], p->latband[jb+1],
		  startlon, endlon);
	}
      }
#else
      int j;
      for (j=1; j<=n; j++) {
	double avglat = (regions(2,1,j) + regions(2,2,j))/2;
	double avglon = (regions(1,1,j) + regions(1,2,j))/2;
	int boxid = ODB_eq_boxid(avglat, avglon, resol);
	fprintf(fp,"regions(:,:,%d) = [boxid=%d, avg (lat,lon)=(%10.4f,%10.4f)]\n",
		j,boxid,avglat,avglon);
	fprintf(fp,"\tlat = [%10.4f .. %10.4f]\tlon = [%10.4f .. %10.4f]\n",
		regions(2,1,j),regions(2,2,j),
		regions(1,1,j),regions(1,2,j));
      }
#endif
    }
  }
}

int main(int argc, char *argv[])
{
#define size_ilist 12
  const int ilist[size_ilist]= { 1,2,3,4,5,31,128,256,512,1024,2048,4096 };
  int jj;
  int nlist;

  if (argc > 1) {
#ifdef INPUT_FILE
    int jp, np = 0;
    double *lat, *lon;
    scanf("%d\n",&np);
    ALLOC(lat,np);
    ALLOC(lon,np);
    for (jp=0; jp<np; jp++) scanf("%lf %lf\n",&lat[jp],&lon[jp]);
#endif
    nlist = argc;
    for (jj=1; jj<nlist; jj++) {
      double resol = atof(argv[jj]);
      int n = ODB_eq_n(resol);
      double eq_area = ODB_eq_area(n);
      eq_cache_t *p = ODBc_get_eq_cache(n);
#if 0
      prt(stderr, resol, p);
#endif
#ifdef INPUT_FILE
      printf(">>> np = %d, resol = %.14g, n = %d <<<\n",np,resol,n);
      for (jp=0; jp<np; jp++) {
	printf("(lat,lon) = (%10.4f,%10.4f)\n",lat[jp],lon[jp]);
	{
	  int boxid = ODB_eq_boxid_with_resol(lat[jp], lon[jp], resol);
	  double boxlat = ODB_eq_boxlat_with_resol(lat[jp], lon[jp], resol);
	  double boxlon = ODB_eq_boxlon_with_resol(lat[jp], lon[jp], resol);
	  printf("(lat,lon) = (%10.4f,%10.4f) --> boxid=%d, box (lat,lon) = (%10.4f,%10.4f)\n",
		 lat[jp],lon[jp],boxid,boxlat,boxlon);
	}
      }
#endif
    }
  }
  else {
    nlist = size_ilist;
    for (jj=0; jj<nlist; jj++) {
      int n = ilist[jj];
      double resol = ODB_eq_resol(n);
      double eq_area = ODB_eq_area(n);
      eq_cache_t *p = ODBc_get_eq_cache(n);
      prt(stderr, resol, p);
    } /* for (jj=0; jj<nlist; jj++) */
  }
  return 0;
}

#endif

#endif
