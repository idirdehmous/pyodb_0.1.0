#include "odb.h"
#include "info.h"
#include "result.h"
#include "evaluate.h"
#include "symtab.h"
#include "cdrhook.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

/* GIS = Geographic Information System */

PRIVATE Bool InSidePolygon(float lat, float lon, int n,
			   const float edgelat[], const float edgelon[])
{
  Bool found = false;
  DRHOOK_START(InSidePolygon);
  {
    int cnt = 0;
    int j, ne = n-1;
    for (j=0; j<ne; j++) {
      const float *p1lat = &edgelat[j];
      const float *p2lat = p1lat + 1; /* &edgelat[j+1]; */
      /* Trivially outside the lat-boundaries ? */
      if (*p1lat <  lat && *p2lat <  lat) continue;
      if (*p1lat >= lat && *p2lat >= lat) continue;
      { 
	/* Find a possible intersection point S = (sx,sy) */
	/*
	  sy - p1lat = [(p2lat-p1lat)/(p2lon-p1lon)] * (sx-p1lon)
	  sy = lat
	  sx = p1lon + [(lat-p1lat) * (p2lon-p1lon)]/(p2lat-p1lat)
	*/
	const float eps = 1e-6;
	const float *p1lon = &edgelon[j];
	const float *p2lon = p1lon + 1; /* &edgelon[j+1]; */
	float latdiff = (*p2lat - *p1lat);
	int sign = (latdiff >= 0) ? 1 : -1;
	float div = (ABS(latdiff) < eps) ? sign*eps : latdiff;
	float sx = *p1lon + ((lat - *p1lat) * (*p2lon - *p1lon))/div;
	if (sx >= lon) ++cnt;
      }
    } /* for (j=0; j<ne; j++) */
    found = (cnt%2 == 1) ? true : false;
  }
  DRHOOK_END(n);
  return found;
}

typedef struct {
  float minlat;
  float maxlat;
  float minlon;
  float maxlon;
} Box_t;

typedef struct _Edge_Cache_t {
  int poolno; /* Pool number the region belongs to */
  int id; /* Region id */
  int partno; /* Part number */
  Box_t box; /* Bounding box */
  int n; /* Number of vertices (closed region => the last & the first (lat,lon) are the same) */
  float *lat;
  float *lon;
} Edge_Cache_t;

typedef struct _Region_Cache_t {
  char *name; /* Country or region name or polygon filename */
  int flag; /* 1=country/region, 2=polygon */
  Box_t box; /* Bounding box */
  int nparts; /* Consists of this many subregions */
  Edge_Cache_t *part;  /* Part/edge information */
  struct _Region_Cache_t *next; /* Next (main) region in chain */
} Region_Cache_t;

static Region_Cache_t *region_start = NULL;
static Region_Cache_t *region_last = NULL;

static char *odb_gisroot = NULL;
static char *odb_gisworld = NULL;
static char *odb_gisplace = NULL;

/* Reference location (by default ECMWF, Reading, UK ~ 51.4N, 1W) */
static double odb_gislat = 51.4;
static double odb_gislon = -1;

PRIVATE o_lock_t INSIDE_mylock = 0; /* A specific OMP-lock; initialized only once in
				       odb/lib/codb.c, routine codb_init_omp_locks_() */

PUBLIC void init_inside()
{
  char *env;
  INIT_LOCKID_WITH_NAME(&INSIDE_mylock,"inside.c:INSIDE_mylock");
  coml_set_lockid_(&INSIDE_mylock);
  if (!odb_gisroot) {
    env = getenv("ODB_GISROOT");
    if (env) odb_gisroot = STRDUP(env);
  }
  if (!odb_gisworld) {
    env = getenv("ODB_GISWORLD");
    if (env) odb_gisworld = STRDUP(env);
  }
  if (!odb_gisplace) {
    env = getenv("ODB_GISPLACE");
    if (env) odb_gisplace = STRDUP(env);
  }
  {
    env = getenv("ODB_GISLAT");
    if (env) odb_gislat = atof(env);
  }
  {
    env = getenv("ODB_GISLON");
    if (env) odb_gislon = atof(env);
  }
  coml_unset_lockid_(&INSIDE_mylock);
}


PRIVATE int
cmpNvertices(const void *A, const void *B)
{
  const Edge_Cache_t *a = A;
  const Edge_Cache_t *b = B;
  return (a->n < b->n) ? -1 : ((a->n > b->n) ? 1 : 0);
}


PRIVATE char *
RcpFile(const char *file, Bool *rm)
{
  char *s = NULL;
  if (rm) *rm = false;
  if (file) {
    struct stat st;
    if (stat(file, &st) == -1) {
      /* Not found ? Perhaps on another machine ? Use rcp */
      const char *colon = strchr(file,':');
      if (colon) {
	const char *basename = strrchr(file,'/');
	char *cmd = NULL;
	int cmdlen;
	char *tmpdir = getenv("TMPDIR");
	if (!tmpdir) tmpdir = ".";
	if (!basename) basename = colon + 1;
	cmdlen = STRLEN(file) + STRLEN(tmpdir) + STRLEN(basename) + 30;
	ALLOC(cmd, cmdlen);
	snprintf(cmd, cmdlen, "/usr/bin/rcp %s\t%s/%s", file, tmpdir, basename);
	if (system(cmd) == 0) { /* All ok */
	  const char *tab = strchr(cmd, '\t');
	  if (tab) { ++tab; s = STRDUP(tab); if (rm) *rm = true; }
	}
      } /* if (colon) */
    }
  }
  if (!s) s = STRDUP(file);
  return s;
}

PRIVATE Region_Cache_t *
LoadPolygonFile(const char *polygon_filename)
{
  Region_Cache_t *reg = NULL;
  DRHOOK_START(LoadPolygonFile);
  if (polygon_filename) {
    /* Polygon file format, version#1 : text format */
    /* 
       rec#1 : POLT 1
       rec#2 : Number of polygons (nparts)
       For each polygon:
         rec#1 : Number of vertices (n)
         rec#2..n+1 : (lat,lon) pair
       End-of-file marker: -1 (optional)

       Note: if given polygon is not a closed polygon,
       it will be closed by adding one extra (lat,lon) vertex,
       which is the very first vertex for this polygon
    */

    /* A quick hack */
    int rc = 0;
    Bool rm = false;
    char *pf = RcpFile(polygon_filename, &rm);
    FILE *fp = fopen(pf,"r");
    if (fp) {
      int nel, nparts, n;
      char magicword[80];
      int version;
      nel = fscanf(fp,"%s %d\n",magicword,&version);
      if (nel == 2 && strequ(magicword,"POLT") && version == 1) {
	CALLOC(reg, 1);
	reg->name = STRDUP(polygon_filename);
	reg->flag = 2;
	nel = fscanf(fp, "%d\n",&nparts);
	if (nel == 1 && nparts > 0) {
	  int jp;
	  reg->nparts = nparts;
	  CALLOC(reg->part, nparts);
	  for (jp=0; jp<nparts; jp++) {
	    nel = fscanf(fp, "%d\n", &n);
	    if (nel == 1 && n > 0) {
	      int j;
	      Edge_Cache_t *e = &reg->part[jp];
	      e->poolno = 0;
	      e->id     = 0;
	      e->partno = jp+1;
	      e->n      = n;
	      ALLOC(e->lat, n+1); /* One extra in case polygon wasn't closed */
	      ALLOC(e->lon, n+1); /* One extra in case polygon wasn't closed */
	      for (j=0; j<n; j++) {
		double xlat, xlon;
		nel = fscanf(fp, "%lf %lf\n",&xlat,&xlon);
		if (nel == 2) {
		  e->lat[j] = xlat;
		  e->lon[j] = xlon;
		}
		else {
		  FREE(e->lat);
		  FREE(e->lon);
		  fprintf(stderr,
			  "LoadPolygonFile(%s): Couldn't read (lat,lon) for vertex#%d of polygon#%d [rc=-5]\n",
			  polygon_filename,j+1,jp+1);
		  rc = -5;
		  goto bailout;
		}
	      } /* for (j=0; j<n; j++) */

	      if (e->lat[0] != e->lat[n-1] || e->lon[0] != e->lon[n-1]) {
		/* Polygon wasn't closed --> close it now */
		e->lat[n] = e->lat[0];
		e->lon[n] = e->lon[0];
		e->n = ++n;
	      }
	    }
	    else {
	      fprintf(stderr,
		      "LoadPolygonFile(%s): Couldn't read the number of vertices for polygon#%d [rc=-4]\n",
		      polygon_filename,jp+1);
	      rc = -4;
	      goto bailout;
	    }
	  } /* for (jp=0; jp<nparts; jp++) */
	}
	else {
	  fprintf(stderr,
		  "LoadPolygonFile(%s): Couldn't read the number of polygons [rc=-3]\n",polygon_filename);
	  rc = -3;
	  goto bailout;
	}
      }
      else {
	fprintf(stderr,
		"LoadPolygonFile(%s): Couldn't read the first line [rc=-2]\n",polygon_filename);
	rc = -2;
	goto bailout;
      }
    bailout:
      fclose(fp);
    }
    else {
      fprintf(stderr,
	      "LoadPolygonFile(%s): Couldn't open the file [rc=-1]\n",polygon_filename);
      rc = -1;
    }
    if (rc < 0) {
      if (reg) {
	FREE(reg->name);
	FREE(reg->part);
	FREE(reg);
      }
    }
    if (rm) remove_file_(pf);
    FREE(pf);
  }
  DRHOOK_END(0);
  return reg;
}


PRIVATE Region_Cache_t *
LoadPolygons(const char *region_name)
{
  Region_Cache_t *reg = NULL;
  DRHOOK_START(LoadPolygons);
  if (odb_gisworld && region_name) {
    /* At present we cannot just call ODBc_open() etc. since IOASSIGN
       is static and refers to already opened database;
       But in the future this indeed is a prospect!
       For now, we 'ave use the "boring" odbsql-approach & unix-pipes */

    int nparts = 0;
    int ntotal = 0;

    typedef struct _texas_chainsaw_t {
      int poolno, id, partno, n;
      Box_t box;
      struct _texas_chainsaw_t *next;
    } texas_chainsaw_t;
    texas_chainsaw_t *cs = NULL;
    texas_chainsaw_t *pcs;

    {
      uresult_t u;

      static const char fmt[] = /* A short query */
	"SELECT $#,id,partno,hdr.len "
	"FROM region,part "
	"WHERE name[1:$namelen] LIKE \"%s\" "
	"ORDERBY 1,2,3";

      char *query = NULL;
      int len = STRLEN(fmt) + STRLEN(region_name) + 1;
      ALLOCX(query, len);
      snprintf(query, len, fmt, region_name);

      u.alias = ODB_SubQuery(4, "-1", odb_gisworld, query, (double)0);
      FREEX(query);
      if (u.res) {
	result_t *r = u.res;
	if (r->ncols_out == 4 && r->nrows_out > 0) {
	  Bool row_wise = r->row_wise;
	  int j;
	  nparts = r->nrows_out; /* Just one result-set, since ORDERBY was used */
	  for (j=0; j<nparts; j++) {
	    texas_chainsaw_t *pcsnew;
	    CALLOC(pcsnew, 1);
	    pcsnew->poolno = row_wise ? r->d[j][0] : r->d[0][j];
	    pcsnew->id     = row_wise ? r->d[j][1] : r->d[1][j];
	    pcsnew->partno = row_wise ? r->d[j][2] : r->d[2][j];
	    pcsnew->n      = row_wise ? r->d[j][3] : r->d[3][j];
	    pcsnew->next = NULL;
	    ntotal += pcsnew->n;
	    if (!cs) {
	      pcs = cs = pcsnew;
	    }
	    else {
	      pcs->next = pcsnew;
	      pcs = pcsnew;
	    }
	  } /* for (j=0; j<nparts; j++) */
	} /* if (r->ncols_out == 4 && r->nrows_out > 0) */
	u.res = ODBc_unget_data(u.res);
      } /* if (u.res) */
    }

    if (nparts > 0) {
      uresult_t u;

      static const char fmt[] = /* Can produce thousands of lines of output */
	"SELECT lat,lon "
	"FROM region,part,hdr "
	"WHERE name[1:$namelen] LIKE \"%s\"";

      char *query = NULL;
      int len = STRLEN(fmt) + STRLEN(region_name) + 1;
      ALLOCX(query, len);
      snprintf(query, len, fmt, region_name);

      u.alias = ODB_SubQuery(4, "-1", odb_gisworld, query, (double)0);
      FREEX(query);
      if (u.res) {
	result_t *r = u.res;
	if (r->ncols_out == 2) {
	  int nrows = 0;
	  while (r) {
	    nrows += r->nrows_out;
	    r = r->next;
	  } /* while (r) */
	  if (nrows == ntotal) {
	    int jj, jp;
	    CALLOC(reg, 1);
	    reg->name = STRDUP(region_name);
	    reg->flag = 1;
	    reg->nparts = nparts;
	    CALLOC(reg->part, nparts);
	    pcs = cs;
	    r = u.res; jj = 0;
	    for (jp=0; jp<nparts; jp++) {
	      Bool row_wise = r->row_wise;
	      int j, n = pcs->n;
	      Edge_Cache_t *e = &reg->part[jp];
	      e->poolno = pcs->poolno;
	      e->id     = pcs->id;
	      e->partno = pcs->partno;
	      e->n      = n;
	      ALLOC(e->lat, n);
	      ALLOC(e->lon, n);
	      for (j=0; j<n; j++) {
		float xlat = row_wise ? r->d[jj][0] : r->d[0][jj];
		float xlon = row_wise ? r->d[jj][1] : r->d[1][jj];
		e->lat[j] = xlat;
		e->lon[j] = xlon;
		++jj;
	      } /* for (j=0; j<n; j++) */
	      pcs = pcs->next;
	      if (jj >= r->nrows_out) { r = r->next; jj = 0; } /* Go for the next chunk */
	    } /* for (jp=0; jp<nparts; jp++) */
	  }
	} /* if (r->ncols_out == 2) */
	u.res = ODBc_unget_data(u.res);
      } /* if (u.res) */
    }

    if (cs) {
      /* Release tmp space reserved by Texas chain-saw ... */
      pcs = cs;
      while (pcs) {
	texas_chainsaw_t *pcs_next = pcs->next;
	FREE(pcs);
	pcs = pcs_next;
      }
    } /* if (cs) */

  } /* if (odb_gisworld && region_name) */
  DRHOOK_END(0);
  return reg;
}


PRIVATE int Common4Insiders(int flag,
			    const char *region_name, double lat, double lon)
{
  int rc = 0;
  DRHOOK_START(Common4Insiders);
  if (region_name && ( (flag == 1 && odb_gisworld) || flag == 2 ) ) {
    unsigned int code = *region_name++;
    Region_Cache_t *reg = NULL;
    Bool add2chain = false;
    
    coml_set_lockid_(&INSIDE_mylock);
    {
      reg = region_start;
      while (reg) {
	if (reg->flag == flag && 
	    ODB_Common_StrEqual(reg->name,
				region_name,
				0, NULL,
				(flag == 1) ? true : false)) {
	  break; /* matched !! */
	}
	reg = reg->next;
      }
    }
    coml_unset_lockid_(&INSIDE_mylock);
    
    if (!reg) {
      reg = (flag == 1) ? LoadPolygons(region_name) : LoadPolygonFile(region_name);
      if (reg) add2chain = true;
    }
    
    if (!reg) {
      /* Still not found --> create an empty region to disable further invocations of odbsql */
      CALLOC(reg, 1);
      reg->name = STRDUP(region_name);
      reg->flag = flag;
      add2chain = true;
    }

    if (add2chain && reg) {
      /* Calculate bounding boxes, sort parts w.r.t. no. of vertices etc. */
      int nparts = reg->nparts;
      if (nparts > 0) {
	int jp;
	Edge_Cache_t *e = reg->part;
	reg->box.minlat =  9999;
	reg->box.maxlat = -9999;
	reg->box.minlon =  9999;
	reg->box.maxlon = -9999;
	for (jp=0; jp<nparts; jp++) {
	  int j, n = e->n;
	  e->partno = jp+1; /* renumbered */
	  e->box.minlat =  9999;
	  e->box.maxlat = -9999;
	  e->box.minlon =  9999;
	  e->box.maxlon = -9999;
	  for (j=0; j<n; j++) {
	    float xlat = e->lat[j];
	    float xlon = e->lon[j];
	    e->box.minlat = MIN(e->box.minlat, xlat);
	    e->box.maxlat = MAX(e->box.maxlat, xlat);
	    e->box.minlon = MIN(e->box.minlon, xlon);
	    e->box.maxlon = MAX(e->box.maxlon, xlon);
	  }
	  reg->box.minlat = MIN(reg->box.minlat, e->box.minlat);
	  reg->box.maxlat = MAX(reg->box.maxlat, e->box.maxlat);
	  reg->box.minlon = MIN(reg->box.minlon, e->box.minlon);
	  reg->box.maxlon = MAX(reg->box.maxlon, e->box.maxlon);
	  ++e;
	}
	if (nparts > 1) qsort(reg->part, nparts, sizeof(Edge_Cache_t), cmpNvertices);
      } /* if (nparts > 0) */
      reg->next = NULL;

      coml_set_lockid_(&INSIDE_mylock);
      if (!region_start) {
	region_last = region_start = reg;
      }
      else {
	region_last->next = reg;
	region_last = reg;
      }
      coml_unset_lockid_(&INSIDE_mylock);
    } /* if (add2chain && reg) */

    if (reg && reg->nparts > 0) {
      /* Find if (lat,lon) is inside the region */
      int nparts = reg->nparts;
      if ((code & 0x1) == 0x1) lat = ODB_lldegrees(lat);
      if (lat >= reg->box.minlat && lat <= reg->box.maxlat) {
	if ((code & 0x2) == 0x2) lon = ODB_lldegrees(lon);
	if (lon >= reg->box.minlon && lon <= reg->box.maxlon) {
	  int jp;
	  Edge_Cache_t *e = reg->part;
	  /* Note: Parts have been sorted with smallest number of polylines first */
	  for (jp=0; jp<nparts; jp++) {
	    int n = e->n;
	    if (n > 0 &&
		lat >= e->box.minlat && lat <= e->box.maxlat &&
		lon >= e->box.minlon && lon <= e->box.maxlon) {
	      Bool inside = InSidePolygon(lat, lon, n, e->lat, e->lon);
	      if (inside) {
		rc = 1;
		break; /* for (jp=0; jp<nparts; jp++) */
	      }
	    }
	    ++e;
	  } /* for (jp=0; jp<nparts; jp++) */
	} /* if (lon >= reg->box.minlon && lon <= reg->box.maxlon) */
      } /* if (lat >= reg->box.minlat && lat <= reg->box.maxlat) */
    } /* if (reg && reg->nparts > 0) */
  }
  DRHOOK_END(0);
  return rc;
}

PUBLIC int
ODB_Inside(const char *region_name, double lat, double lon)
{
  int rc = 0;
  DRHOOK_START(ODB_Inside);
  rc = Common4Insiders(1, region_name, lat, lon);
  DRHOOK_END(0);
  return rc;
}


PUBLIC double
ODBinside(double region_name, double lat, double lon)
{
  double rc = 0;
  if (odb_gisworld) {
    /* Note: region_name is interpreted as an address to a character string (saddr) */
    char *str = NULL;
    S2D_Union u;
    u.dval = region_name;
    str = u.saddr;
    rc = (double)ODB_Inside(str, lat, lon);
  }
  return rc;
}


PUBLIC int
ODB_InPolygon(const char *polygon_filename, double lat, double lon)
{
  int rc = 0;
  DRHOOK_START(ODB_InPolygon);
  rc = Common4Insiders(2, polygon_filename, lat, lon);
  DRHOOK_END(0);
  return rc;
}


PUBLIC double
ODBinpolygon(double polygon_filename, double lat, double lon)
{
  double rc = 0;
  /* Note: polygon_filename is interpreted as an address to a character string (saddr) */
  char *str = NULL;
  S2D_Union u;
  u.dval = polygon_filename;
  str = u.saddr;
  rc = (double)ODB_InPolygon(str, lat, lon);
  return rc;
}


typedef struct _Place_t {
  char *name;
  double dis; /* distance to a reference (lat,lon)-location */
  double lat;
  double lon;
  double alt;
  double pop;
} Place_t;

static int nplaces = 0;
static Place_t *places = NULL;

PRIVATE Place_t *
FindPlace(const char *place_name, int *n)
{
  int nfound = 0;
  Place_t *pl = NULL;
  if (odb_gisplace && place_name) {
    int j;
    for (j=0; j<nplaces; j++) {
      Place_t *p = &places[j];
      if (ODB_Common_StrEqual(p->name,
			      place_name,
			      0, NULL,
			      true)) {
	pl = p; /* found */
	nfound = 1;
	break;
      }
    } /* for (j=0; j<nplaces; j++) */

    if (!pl) {
      /* Not found --> run a query */
      uresult_t u;

      static const char fmt[] =
	"SELECT distance(lat,lon,%.3f,%.3f) as dis,lat,lon,alt,pop "
	"FROM hdr "
	"WHERE name[1:$namelen] LIKE \"%s\" "
	"ORDERBY 1";

      char *query = NULL;
      int len = STRLEN(fmt) + STRLEN(place_name) + 100;
      ALLOCX(query, len);
      snprintf(query, len, fmt, odb_gislat, odb_gislon, place_name);

      u.alias = ODB_SubQuery(4, "-1", odb_gisplace, query, (double)0);
      FREEX(query);
      if (u.res) {
	result_t *r = u.res;
	if (r->ncols_out == 5) {
	  int nrows = r->nrows_out; /* Just one result-set, since ORDERBY was used */
	  if (nrows > 0) {
	    Bool row_wise = r->row_wise;
	    int last = nplaces;
	    int i, offset = last;
	    nplaces += nrows;
	    REALLOC(places, nplaces);
	    for (i=0; i<nrows; i++) {
	      double dis = row_wise ? r->d[i][0] : r->d[0][i];
	      double lat = row_wise ? r->d[i][1] : r->d[1][i];
	      double lon = row_wise ? r->d[i][2] : r->d[2][i];
	      double alt = row_wise ? r->d[i][3] : r->d[3][i];
	      double pop = row_wise ? r->d[i][4] : r->d[4][i];
	      places[offset+i].name = STRDUP(place_name);
	      places[offset+i].dis = dis;
	      places[offset+i].lat = lat;
	      places[offset+i].lon = lon;
	      places[offset+i].alt = alt;
	      places[offset+i].pop = pop;
	      if (++nfound == 1) pl = &places[last];
	    }
	  } /* if (nrows > 0) */
	}
	u.res = ODBc_unget_data(u.res);
      }
      if (!pl) {
	/* SQL did not find this place => do not search again */
	int last = nplaces++;
	REALLOC(places, nplaces);
	places[last].name = STRDUP(place_name);
	places[last].dis = mdi;
	places[last].lat = mdi;
	places[last].lon = mdi;
	places[last].alt = mdi;
	places[last].pop = mdi;
	pl = &places[last];
	nfound = 1;
      }
    }
  } /* if (odb_gisplace && place_name) */
  if (n) *n = nfound;
  return pl;
}


PUBLIC double
ODB_Near(const char *place_name, double lat, double lon)
{
  double distance = mdi;
  coml_set_lockid_(&INSIDE_mylock);
  if (place_name) {
    /* Must be locked at this level due to potentially changing "pl" of FindPlace() */
    int nfound = 0;
    unsigned int code = *place_name++;
    Place_t *pl = FindPlace(place_name, &nfound);
    if (nfound > 0 && pl && pl->lat != mdi && pl->lon != mdi) {
      if ((code & 0x1) == 0x1) lat = ODB_lldegrees(lat);
      if ((code & 0x2) == 0x2) lon = ODB_lldegrees(lon);
      distance = Func_distance(lat, lon, pl->lat, pl->lon); /* A macro from odb.h */
    }
  }
  coml_unset_lockid_(&INSIDE_mylock);
  return distance;
}


PUBLIC double
ODBnear(double place_name, double lat, double lon)
{
  double rc = mdi;
  if (odb_gisplace) { /* $ODB_GISROOT/PLACE -database */
    /* Note: place_name is interpreted as an address to a character string (saddr) */
    char *str = NULL;
    S2D_Union u;
    u.dval = place_name;
    str = u.saddr;
    rc = ODB_Near(str, lat, lon);
  }
  return rc;
}

#define ODB_LATITUDE   1
#define ODB_LONGITUDE  2
#define ODB_ALTITUDE   3
#define ODB_POPULATION 4

PRIVATE double CommonStrFunc1(int key,
			      const char *place_name,
			      double dplace_name)
{
  double value = mdi;
  if (odb_gisplace && key > 0) { /* F.ex. $ODB_GISROOT/PLACE -database */
    coml_set_lockid_(&INSIDE_mylock);
    { /* Must be locked at this level due to potentially changing "pl" of FindPlace() */
      int nfound = 0;
      Place_t *pl = FindPlace(place_name, &nfound);
      if (nfound > 0 && pl) {
	char *str = place_name ? (char *)place_name : NULL;
	if (!str) {
	  /* Note: dplace_name is interpreted as an address to a character string (saddr) */
	  S2D_Union u;
	  u.dval = dplace_name;
	  str = u.saddr;
	}
	switch (key) {
	case ODB_LATITUDE   : value = pl->lat; break;
	case ODB_LONGITUDE  : value = pl->lon; break;
	case ODB_ALTITUDE   : value = pl->alt; break;
	case ODB_POPULATION : value = pl->pop; break;
	}
      } /* if (nfound > 0 && pl) */
    }
    coml_unset_lockid_(&INSIDE_mylock);
  }
  return value;
}

PUBLIC double ODB_Lat(const char *place_name)
{ return CommonStrFunc1(ODB_LATITUDE, place_name, 0); }

PUBLIC double ODBlat(double place_name)
{ return CommonStrFunc1(ODB_LATITUDE, NULL, place_name); }

PUBLIC double ODB_Lon(const char *place_name)
{ return CommonStrFunc1(ODB_LONGITUDE, place_name, 0); }

PUBLIC double ODBlon(double place_name)
{ return CommonStrFunc1(ODB_LONGITUDE, NULL, place_name); }

PUBLIC double ODB_Alt(const char *place_name)
{ return CommonStrFunc1(ODB_ALTITUDE, place_name, 0); }

PUBLIC double ODBalt(double place_name)
{ return CommonStrFunc1(ODB_ALTITUDE, NULL, place_name); }

PUBLIC double ODB_Pop(const char *place_name)
{ return CommonStrFunc1(ODB_POPULATION, place_name, 0); }

PUBLIC double ODBpop(double place_name)
{ return CommonStrFunc1(ODB_POPULATION, NULL, place_name); }

/* SUB-query processing */

PUBLIC double
ODBsubquery(const int nargs, const double args[])
{
  FILE *fp_echo = ODBc_get_debug_fp();
  double rc = 0; /* uresult_t's alias */
  if (fp_echo) ODB_fprintf(fp_echo, "ODBsubquery: nargs=%d\n", nargs);
  if (nargs >= 4 && args) {
    uresult_t u;
    S2D_Union u1, u2, u3;
    char *poolmask = NULL;
    char *db = NULL;
    char *query = NULL;
    double make_sort_unique;
    int n = nargs - 4; /* No. of additional (double) SET-parameters */
    int dblen = 0;
    u.res = NULL;
    u1.dval = args[0]; poolmask = u1.saddr;
    u2.dval = args[1]; db = u2.saddr;
    u3.dval = args[2]; query = u3.saddr;
    make_sort_unique = args[3];
    dblen = STRLEN(db);
    if (dblen >= 0) {
      /* For the moment using *ONLY* odbsql ... */
      static const char fmt[] = 
	"|$ODB_BINPATH/odbsql -q '%s%s'%s%s -NBT -e /dev/null -p'%s' -f binary";
      char *cmd = NULL;
      int len = STRLEN(fmt) + STRLEN(query) + dblen + 50 + STRLEN(poolmask);
      char *set = NULL;
      if (n > 0) { /* SET-params */
	const char setfmt[] = "SET $%d = %.14g;";
	char *aset = NULL;
	int asetlen = STRLEN(setfmt) + 100;
	int j, lenset = n * asetlen;
	ALLOC(set, lenset);
	*set = '\0';
	ALLOCX(aset, asetlen);
	for (j=0; j<n; j++) {
	  snprintf(aset, asetlen, setfmt, j+1, args[3+j]);
	  strcat(set, aset);
	}
	FREEX(aset);
      }
      ALLOCX(cmd, len);
      snprintf(cmd, len, fmt, 
	       set ? set : "",
	       query,
	       (dblen > 0) ? " -i" : "",
	       (dblen > 0) ? db : "",
	       poolmask);
      if (fp_echo) ODB_fprintf(fp_echo, "ODBsubquery: cmd='%s', make_sort_unique = %d\n", 
			       cmd,(int)make_sort_unique);
      /* true below means: make column oriented, with sorted unique values across rows */
      u.res = ODBc_get_data_from_binary_file(cmd, 
					     (int)make_sort_unique ? true : false); 
      FREEX(cmd);
      FREE(set);
    }
#if 0
    else {
      /* Use faster method, since the current database (BUT IS BROKEN ...) */
      /* Need to make IOASSIGN more dynamic first */
      int handle = ODBc_get_handle(NULL);
      info_t *info = ODBc_sql_prepare(handle, query, NULL, 0);
      if (info) {
	result_t *res = NULL;
	if (poolno > 0) {
	  int nrows = ODBc_sql_exec(handle, info, poolno, NULL, NULL);
	  res = (nrows > 0) ? ODBc_get_data(handle, NULL, info, poolno, 1, nrows, false, NULL) : NULL;
	}
	else {
	  /* Also this is not quite right !! What if global_oper ? 
	     See odbsql.c for correct way in dealing with this */
	  int jp, npools = info->npools;
	  for (jp=1; jp<=npools; jp++) {
	    int iret = ODBc_sql_exec(handle, info, jp, NULL, NULL);
	    if (iret > 0) {
	      res = ODBc_get_data(handle, res, info, jp, 1, iret, false, NULL);
	    }
	  } /* for (jp=1; jp<=npools; jp++) */
	}
	u.res = res;
	(void) ODBc_sql_cancel(info);
      }
    }
#endif
    rc = u.res ? u.alias : 0;
    if (fp_echo) ODB_fprintf(fp_echo, 
			     "ODBsubquery: u.res = %p, u.alias = %.14g, u.i[0:1] = (%d, %d)\n",
			     u.res, u.alias, u.i[0], u.i[1]);
  }
  return rc;
}


PUBLIC double
ODB_SubQuery(const int n, 
	     /* const char *poolmask, const char *db, const char *query, double make_sort_unique */ ...)
{
  double rc = 0;
  if (n >= 4) {
    int j;
    double *args = NULL;
    char *poolmask, *db, *query;
    S2D_Union u1, u2, u3;
    double make_sort_unique;
    va_list ap;
    va_start(ap, n);

    ALLOCX(args, n);

    poolmask = va_arg(ap, char *);
    u1.saddr = poolmask;
    args[0] = u1.dval;

    db = va_arg(ap, char *);
    u2.saddr = db;
    args[1] = u2.dval;

    query = va_arg(ap, char *);
    u3.saddr = query;
    args[2] = u3.dval;

    make_sort_unique = va_arg(ap, double);

    for (j=4; j<n; j++) {
      args[j] = va_arg(ap, double);
    }
    rc = ODBsubquery(n, args);
    FREEX(args);
    va_end(ap);
  }
  return rc; /* uresult_t's alias */
}


PUBLIC double
ODB_InQuery(const int num_expr, const int nummatch, 
	    double subquery, double runonce, /* double expr1, [double expr2, ... ] */ ...)
{
  double rc = 0;
  if (num_expr >= 5) {
    int j, n = num_expr;
    double *args = NULL;
    va_list ap;
    va_start(ap, runonce);
    ALLOCX(args, n);
    args[0] = num_expr;
    args[1] = nummatch;
    args[2] = subquery;
    args[3] = runonce;
    for (j=4; j<n; j++) {
      args[j] = va_arg(ap, double);
    }
    rc = ODBinquery(n, args);
    FREEX(args);
    va_end(ap);
  }
  return rc; /* uresult_t's alias */
}


PUBLIC double
ODBinquery(const int nargs, const double args[])
{
  double rc = 0; /* Not found in query */

  if (nargs >= 5) {
    FILE *fp_echo = ODBc_get_debug_fp();
    int j, num_expr = (int)args[0] - 4;
    int nummatch = (int)args[1];
    double subquery = args[2];
    double runonce = args[3];
    const double *expr = &args[4];
    uresult_t u;

    u.alias = subquery;

    if (fp_echo) {
      ODB_fprintf(fp_echo, 
		  "ODBinquery([nargs=%d] ; num_expr = %d, nummatch = %d,"
		  " subquery = %.14g, runonce = %.14g",
		  nargs, num_expr, nummatch, subquery, runonce);
      for (j=0; j<num_expr; j++) ODB_fprintf(fp_echo,", %.14g",expr[j]);
      ODB_fprintf(fp_echo,")\n");
      ODB_fprintf(fp_echo, 
		  "ODBinquery: u.res = %p, u.alias = %.14g, u.i[0:1] = (%d, %d)\n",
		  u.res, u.alias, u.i[0], u.i[1]);
    }

    if (u.res) {
      result_t *r = u.res;
      int ncols = r->ncols_out;
      int nrows = r->nrows_out;

      if (fp_echo) {
	static int cnt = 0; /* not thread safe */
	ODB_fprintf(fp_echo,
		    "ODBinquery: num_expr = %d, ncols = %d, nrows = %d, poolno = %d\n",
		    num_expr, ncols, nrows, r->poolno);
	if (cnt++ < 5) ODBc_DebugPrintRes("ODBinquery", r);
      }

      if (nummatch == 0) {
	num_expr = MIN(num_expr, ncols);
	if (nrows > 0 && num_expr >= 1) {
	  /* Subquery is in a "stream-lined" (nrows x ncols), with 1st column sorted */
	  const double *col_1 = &r->d[0][0];
	  int k = ODBc_bsearch(expr[0], nrows, col_1, +1);
	  if (k >= 0 && num_expr == 1) {
	    rc = 1; /* Found */
	  }
	  else if (k >= 0 && num_expr > 1) {
	    Bool found = false;
	    int kpivot = k; /* pivot point */
	    
	    ODB_fprintf(fp_echo,"\tkpivot = %d, *col_1 = %.14g\n", kpivot, *col_1);
	    
	    while (!found && k < nrows) { /* Search forward */
	      Bool match = (expr[0] == col_1[k]) ? true : false;
	      if (!match) break; /* Not even the first column the same anymore */
	      for (j=1; match && j<num_expr; j++) {
		double value = r->d[j][k];
		if (expr[j] != value) match = false;
	      }
	      if (match) found = true;
	      ++k;
	    } /* while (!found && k < nrows) */
	    
	    k = kpivot;
	    while (!found && k > 0) { /* Search backward */
	      Bool match = (expr[0] == col_1[--k]) ? true : false;
	      if (!match) break; /* Not even the first column the same anymore */
	      for (j=1; match && j<num_expr; j++) {
		double value = r->d[j][k];
		if (expr[j] != value) match = false;
	      }
	      if (match) found = true;
	    } /* while (!found && k > 0) */
	    
	    if (found) rc = 1;
	  } /* if (k >= 0 && num_expr > 1) */
	} /* if (nrows > 0 && num_expr >= 1) */
      }
      else if (nummatch > 0) { /* Ought to be 1 */
	/* loop over nrows */
	/* for each row assign values to \1, \2, ..., \ncols -variables in succession */
	/* stop when first match */
	uresult_t e;
	e.alias = expr[0];
	ODB_fprintf(fp_echo, 
		    "ODBinquery: e.parse_tree = %p, e.alias = %.14g, e.i[0:1] = (%d, %d)\n",
		    e.parse_tree, e.alias, e.i[0], e.i[1]);
	if (e.parse_tree && nrows > 0) {
	  DEF_IT;
	  thsafe_parse_t *thsp = fp_echo ? (thsafe_parse_t *)GetTHSP() : NULL;
	  void *save_chan_err = thsp ? thsp[IT].chan_err : NULL;
	  Bool found = false;
	  double value;
	  int iret, ir, jc;
	  typedef struct {
	    char tmp[16];
	    symtab_t *stab;
	    double *value;
	    double *vec;
	  } tmp_stab_t;
	  tmp_stab_t *st;
	  static int cnt = 0; /* not thread safe */

	  if (thsp) thsp[IT].chan_err = fp_echo;

	  if (fp_echo && cnt < 5) {
	    PrintNode(fp_echo,e.parse_tree);
	    ODB_fprintf(fp_echo,"\tnrows = %d, ncols = %d\n", nrows, ncols);
	  }

	  /* Allocate \<number>-variables to provide fastest possible access & update */
	  CALLOC(st, ncols);
	  for (jc=0; jc<ncols; jc++) {
	    tmp_stab_t *pst = &st[jc];
	    snprintf(pst->tmp, sizeof(pst->tmp), "\\%d", jc+1);
	    pst->value = putsymvec(pst->tmp, mdi, r->d[jc], nrows);
	    pst->stab = getsymtab(pst->tmp);
	    pst->vec = pst->stab->vec; /* alias r->d[jc] */
	    if (fp_echo && cnt < 5) {
	      ODB_fprintf(fp_echo,
			  "[init: jc=%d] \t%s = %.14g (r->d[jc] = 0x%x, pst->vec = %p, pst->value = 0x%x)\n", 
			  jc, pst->tmp, *pst->value, r->d[jc], pst->vec, pst->value);
	    }
	  } /* for (jc=0; jc<ncols; jc++) */

	  for (ir=0; !found && ir<nrows; ir++) {
	    if (fp_echo && cnt < 5 && ir < 100) ODB_fprintf(fp_echo,"[ir = %d] : ", ir);
	    for (jc=0; jc<ncols; jc++) {
	      /* Quick & direct update \<number>-variables */
	      tmp_stab_t *pst = &st[jc];
	      *pst->value =  *pst->vec++;
	      if (fp_echo && cnt < 5 && ir < 100) {
		ODB_fprintf(fp_echo,"[%d:%s] = %.14g ", jc, pst->tmp, *pst->value);
	      }
	    } /* for (jc=0; jc<ncols; jc++) */
	    if (fp_echo && cnt < 5 && ir < 100) ODB_fprintf(fp_echo, "\n");
	    /* value = RunTree(e.parse_tree, NULL, &iret); */
	    /* The following is better than RunTree(), since only one expr/condition, no chain */
	    value = RunNode(e.parse_tree, &iret); 
	    if (iret != 0) {
	      /* Should not happen */
	      fprintf(stderr, 
		      "***Error in ODBinquery() [iret = %d] : "
		      "Could not evaluate (nummatch = %d) ; ir=%d, nrows=%d, ncols=%d\n",
		      iret, nummatch, ir, nrows, ncols);
	      PrintNode(stderr,e.parse_tree);
	      RAISE(SIGABRT);
	    }
	    if (value != 0) found = true;
	  } /* for (ir=0; !found && ir<nrows; ir++) */
	  ++cnt;

	  if (found) rc = 1;

	  /* De-activate \<number>-variables quickly */
	  for (jc=0; jc<ncols; jc++) {
	    tmp_stab_t *pst = &st[jc];
	    pst->stab->active = false;
	  } /* for (jc=0; jc<ncols; jc++) */
	  FREE(st);

	  if (thsp) thsp[IT].chan_err = save_chan_err;
	}
      }

      if (fp_echo) {
	ODB_fprintf(fp_echo, " --> rc = %d [nummatch was %d]\n", (int)rc, nummatch);
      }
    } /* if (u.res) */

    /* When the subquery-parameter wasn't a RunOnceQuery(), 
       then perform cleanup every time (==> very, very slow search) */
    if (u.res && runonce != 1) {
      if (fp_echo) ODB_fprintf(fp_echo, "ODBinquery: u.res && runonce = %g != 1\n", runonce);
      u.res = ODBc_unget_data(u.res);
    }
  }

  return rc;
}
