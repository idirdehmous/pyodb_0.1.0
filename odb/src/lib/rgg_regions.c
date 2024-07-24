
/* rgg_regions.c */

/* Maps (lat,lon) into its Reduced Gaussian Grid box at resolution Txxxx */

/* A big thanks to Lars Isaksen, ECMWF, 14/3/2008 !!! */

#include "odb.h"
#include "regcache.h"
#include "cmaio.h"
#include "cdrhook.h"

extern void gauaw_odb_(double PA[],
		       double PW[],
		       const int *K,
		       int *KRET);         /* See lib/gauaw_odb.F90 */

#define rgg_cache_t regcache_t

#define ERROR(msg) { if (msg) fprintf(stderr,"%s\n",msg); RAISE(SIGABRT); }

static rgg_cache_t **rgg_cache = NULL;

PRIVATE int *
read_rtablel_2_file(int Txxxx, int *NRGRI_len, int *Nlons)
{
  /* Reads F90 namelist file $ODB_RTABLE_PATH/rtablel_2<xxxx>
     to find out how many latitude bands there are (must be xxxx+1)
     and how many longitudes boxes per latband there are */

  int *NRGRI = NULL;
  int nb = 0;
  int nlons = 0;
  int nexp = Txxxx + 1; /* Expect this many latitude bands */

  char *rtable_file = NULL;
  const char fmt[] = "$ODB_RTABLE_PATH/rtablel_2%3.3d";
  int len = STRLEN(fmt) + 10;
  FILE *fp = NULL;
  int iounit = -1;
  int iret = 0;

  ALLOCX(rtable_file, len);
  snprintf(rtable_file, len, fmt, Txxxx);

  cma_open_(&iounit, rtable_file, "r", &iret, STRLEN(rtable_file), 1);
  fp = (iounit >= 0 && iret == 1) ? CMA_get_fp(&iounit) : NULL;

  if (fp) {
    char line[80];
    CALLOC(NRGRI, nexp);

    while (!feof(fp) && fgets(line, sizeof(line), fp)) {
      char *lb = strchr(line,'(');
      char *rb = lb ? strchr(line,')') : NULL;
      char *eq = rb ? strchr(line,'=') : NULL;
      if (lb && rb && rb) {
	if (lb < rb && rb < eq) {
	  char *comma = strchr(line,',');
	  int id = 0;
	  int npts = 0;
	  if (comma && eq < comma) *comma = '\0';
	  ++lb;
	  *rb = '\0';
	  ++eq;
	  id = atoi(lb) - 1;
	  npts = atoi(eq);
	  if (id >= 0 && id < nexp && npts > 0) {
	    NRGRI[id] = npts;
	    nlons += npts;
	  }
	} /* if (lb < rb && rb < eq) */
      } /* if (lb && rb && rb) */
    } /* while (!feof(fp) ... ) */

    cma_close_(&iounit, &iret);
    nb = nexp;
  }
  else {
    char *env = getenv("ODB_RTABLE_PATH");
    fprintf(stderr,
	    "read_rtablel_2_file(): Unsupported resolution Txxxx = %d or $ODB_RTABLE_PATH not defined\n", Txxxx);
    if (env) {
      const char cmd[] = 
	"/bin/ls -C1 $ODB_RTABLE_PATH/rtablel_2* | /bin/sed 's|^.*/rtablel_2||' | /bin/sort -n >&2";
      fprintf(stderr,
	      "                      Currently supported resolutions are:\n");
      fflush(stderr);
      system(cmd);
    }
    ERROR(NULL);
  }

  if (NRGRI_len) *NRGRI_len = nb;
  if (Nlons) *Nlons = nlons;

  FREEX(rtable_file);

  return NRGRI;
}


PUBLIC rgg_cache_t **
ODBc_init_rgg_cache()
{
  if (!rgg_cache) {
    DEF_INUMT;
    CALLOC(rgg_cache, inumt);
  }
  return rgg_cache;
}


PUBLIC rgg_cache_t *
ODBc_get_rgg_cache(int n)
{
  /* Here "n" is the "xxxx" of resolution "Txxxx" */
  DEF_IT;
  rgg_cache_t *p = NULL;
  DRHOOK_START(ODBc_get_rgg_cache);
  {
    if (!rgg_cache) rgg_cache = ODBc_init_rgg_cache(); /* Normally not called */
    p = rgg_cache[IT];

    while (p) {
      if (n == p->n || p->kind == rgg_cache_kind) {
	/* Matched */
	break;
      }
      p = (n < p->n) ? p->left : p->right;
    } /* while (p) */

    if (!p) {

      /* This is done only once per "n", per "it" */

      int iret = 0;
      int ndgl = n + 1; /* Number of latitude bands i.e. "nb" */
      int nlons = 0;
      int *loncnt = read_rtablel_2_file(n, &ndgl, &nlons);
      double *latband = NULL;
      double *midlat = NULL;
      double *stlon = NULL;
      double *deltalon = NULL;
      double *zlmu = NULL;
      double *zw = NULL;
      double *zlatedge = NULL;
      const double zfact = recip_pi_over_180;
      double zfact2 = ((double)45)/atan(1);
      int ii, iequator = ndgl/2;

#if 0
      fprintf(stderr,"ndgl = %d = nb, iequator = %d\n", ndgl, iequator);
#endif

      ALLOC(zlmu, ndgl);
      ALLOC(zw, ndgl);
      gauaw_odb_(zlmu,zw,&ndgl,&iret);

      midlat = zlmu;
      for (ii=0; ii<ndgl; ii++) {
	midlat[ii] = zfact2*asin(zlmu[ii]);
      }

      /* FREE(zlmu); */

      ALLOC(zlatedge, ndgl + 1);
      zlatedge[0] = half_pi;
      for (ii=0; ii<ndgl/2; ii++) {
	zlatedge[ii+1] = asin( MAX(-1, MIN(1,sin(zlatedge[ii])-zw[ii]) ) );
      }

      FREE(zw);
      
      zlatedge[iequator] = 0;

      for (ii=iequator+1; ii<ndgl+1; ii++) {
	/* fprintf(stderr, "\t[%d] = -[%d]\n", ii, ndgl-ii); */
	zlatedge[ii] = -zlatedge[ndgl-ii];
      }

      /* Latitude bands : from North (+90) to South (-90) */

      ALLOC(latband, ndgl + 1);
      for (ii=0; ii<ndgl+1; ii++) {
	latband[ii] = zfact*zlatedge[ii];
      }

      FREE(zlatedge);

      /* Starting longitudes, deltalon's */
      
      ALLOC(stlon, ndgl);
      ALLOC(deltalon, ndgl);

      for (ii=0; ii<ndgl; ii++) {
	int cnt = loncnt[ii];
	const double three_sixty = 360;
	double delta = three_sixty/cnt;
	deltalon[ii] = delta;
	stlon[ii] = -delta/2;
      }

#if 0
      {
	for (ii=0; ii<ndgl; ii++) {
	  fprintf(stderr,"%d: (%d) latband[] = %.4f, loncnt[] = %d\n",
		  ii, ii+1, latband[ii], loncnt[ii]);
	}
	fprintf(stderr,"and : latband[%d] = %.4f\n",
		ii, latband[ii]);
      }
#endif
	
      /* Store in cache */
      p = ODBc_put_regcache(rgg_cache_kind,
			    n, ndgl, 
			    latband, midlat,
			    stlon, deltalon, 
			    loncnt,
			    ODBc_init_rgg_cache, rgg_cache);

      p->resol = (deltalon[iequator] + deltalon[iequator+1])/2;
#if 0
      fprintf(stderr,"Equator resolution is %.2f degrees\n", p->resol);
#endif

    }
  }
  DRHOOK_END(n);
  return p;
}

PUBLIC double 
ODB_rgg_boxid(double lat, double lon, double Txxxx)
{
  return ODBc_get_boxid(lat, lon, Txxxx, NULL, ODBc_get_rgg_cache);
}

PUBLIC double 
ODB_rgg_boxlat(double lat, double lon, double Txxxx)
{
  return ODBc_get_boxlat(lat, lon, Txxxx, ODBc_get_rgg_cache);
}

PUBLIC double 
ODB_rgg_boxlon(double lat, double lon, double Txxxx)
{
  return ODBc_get_boxlon(lat, lon, Txxxx, ODBc_get_rgg_cache);
}

PUBLIC double
ODB_rgg_resol(double Txxxx)
{
  rgg_cache_t *p = ODBc_get_rgg_cache(Txxxx);
  double resol = p ? p->resol : 0;
  return resol;
}
