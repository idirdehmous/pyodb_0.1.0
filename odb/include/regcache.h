#ifndef _REGCACHE_H_
#define _REGCACHE_H_

/* regcache.h */

/* 
   All common data structures for region searches
   for use by the gridding of obs in source files:

   1) lib/eq_regions.c
   2) lib/rgg_regions.c
*/

typedef enum { eq_cache_kind = 1, rgg_cache_kind = 2 } regcache_kind;

static const char *regcache_names[] = { 
  "<undef>",
  "EQ-region", 
  "Reduced Gaussian Grid",
  NULL
};

typedef struct _regcache_t {
  regcache_kind kind;
  int n; /* # of eq_regions patches/boxes or rgg_regions Txxxx resolution */
  int nboxes; /* Actual number of boxes (=n for eq_regions), but calculated for rgg_regions */
  double resol; /* Approximate resolution in degrees at Equator */
  int nb; /* number of latitude bands (also known as "ndgl" in rgg_regions.c) */
  double *latband; /* starting latitudes for each latitude band : size nb+1 */
  double *midlat; /* mid latitudes for each latitude band : size nb */
  int *loncnt; /* # of longitude boxes for each latitude band : size nb */
  int *sum_loncnt; /* Sum of (longitude) boxes BEFORE this latitude band : size nb */
  double *stlon; /* starting longitudes for each latitude band : size nb */
  double *deltalon; /* longitudinal delta for each latitude band : size nb */
  struct {
    int jb;
    int lonbox;
    int boxid;
    double left, mid, right;
  } last; /* Results from the last find_latband, find_lonbox */
  struct _regcache_t *left;
  struct _regcache_t *right;
} regcache_t;

extern regcache_t *ODBc_put_regcache(regcache_kind kind,
				     int n, int nb,
				     double *latband, double *midlat,
				     double *stlon, double *deltalon, 
				     int *loncnt,
				     regcache_t **(*init_code)(void), regcache_t **regcache);

extern regcache_t **ODBc_init_eq_cache();
extern regcache_t **ODBc_init_rgg_cache();

extern regcache_t *ODBc_get_rgg_cache(int Txxxx);
extern regcache_t *ODBc_get_eq_cache(int n);

extern int ODBc_get_boxid(double lat, double lon, int n,
			  regcache_t *ptr, regcache_t *(*get_cache)(int));

extern double ODBc_get_boxlon(double lat, double lon, double rn,
			      regcache_t *(*get_cache)(int));

extern double ODBc_get_boxlat(double lat, double lon, double rn,
			      regcache_t *(*get_cache)(int));

#endif

