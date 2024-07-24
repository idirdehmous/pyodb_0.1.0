/* wolrd2odb.c */

/* Translates ASCII/Shape format regional polygons into 
   a series of ASCII input files to be fed into ODB
   via simulobs2odb -utility */

#include "alloc.h"
#include <time.h>

typedef struct {
  int offset;
  int len;
  double minlat, maxlat;
  double minlon, maxlon;
} Part_t;

typedef struct {
  char *name;
  double minlat, maxlat;
  double minlon, maxlon;
  int nparts;
  Part_t *part;
  int n;
  double *lat;
  double *lon;
} Country_t;


PRIVATE int
descending(const void *A, const void *B)
{
  const Country_t *a = A;
  const Country_t *b = B;
  if      ( a->n < b->n ) return +1;
  else if ( a->n > b->n ) return -1;
  else                    return  0;
}


#define KEY_SHP     "# Shp "
#define KEY_NRECS   "# number of recs "
#define KEY_NAME    "# Name: "
#define KEY_BOX     "# Box: "
#define KEY_NPARTS  "# Num of parts: "
#define KEY_NPOINTS "# Num of points: "
#define KEY_PARTNO  "# Part "

int 
main(int argc, char *argv[]) 
{
  int errcnt = 0;
  int poolno = 0;
  static char *ddl = NULL;
  int jj;
  putenv("DR_HOOK=0"); /* We do not want Dr.Hook to interfere */
  for (jj=1; jj<argc; jj++) {
    char *filename = STRDUP(argv[jj]);
    char *cpools = strchr(filename,':');
    int npp = 0; /* not in use yet */
    FILE *fp;
    if (cpools) {
      *cpools++ = '\0';
      npp = atoi(cpools);
    }
    fp = fopen(filename,"r");
    if (fp) {
      int npools = 0;
      char txt[80];
      int offset = 0;
      Part_t *pp = NULL;
      int shpinfo_read = 0;
      int nc = 0;
      Country_t *country = NULL;
      Country_t *pc = NULL;
      int n_countries = 0;
      char line[4096];
      while (!feof(fp) && fgets(line,sizeof(line),fp)) {
	char *p = line;
	char *nl = strchr(line,'\n');
	while (isspace(*p)) p++;
	if (nl) {
	  *nl = '\0';
	  if (nl != p) {
	    for (;;) {
	      --nl;
	      if (nl == p || !isspace(*nl)) break;
	    }
	  }
	  *++nl = '\0';
	}
#if 0
	fprintf(stderr,"%s\n",p);
#endif
	if (!country) {
	  const char s[] = KEY_NRECS;
	  const int slen = sizeof(KEY_NRECS)-1;
	  if (strncaseequ(p,s,slen)) {
	    int n = 0;
	    if (sscanf(p+slen,"%d",&n) == 1 && n > 0) {
	      nc = n;
	      CALLOC(country, nc+1);
	      pc = country;
#ifdef DEBUG
	      fprintf(stderr,"---> country allocated : n = %d\n",n);
#endif
	    }
	  }
	}
	else if (!shpinfo_read) {
	  const char s[] = KEY_SHP;
	  const int slen = sizeof(KEY_SHP)-1;
	  if (strncaseequ(p,s,slen)) {
	    shpinfo_read = 1;
#ifdef DEBUG
	    fprintf(stderr,"---> shp info read\n");
#endif
	  }
	}
	else if (n_countries == 0) {
	  const char s[] = KEY_NAME;
	  const int slen = sizeof(KEY_NAME) - 1;
	  if (strncaseequ(p,s,slen)) {
	    pc->name = STRDUP(p+slen);
	    pp = NULL;
	    offset = 0;
#ifdef DEBUG
	    fprintf(stderr,"---> in country#%d (%s)\n",
		    n_countries,pc->name);
#endif
	    ++n_countries;
	  }
	}
	else if (n_countries > 0) {
	  const char s[] = KEY_NAME;
	  const int slen = sizeof(KEY_NAME) - 1;

	  const char sbox[] = KEY_BOX;
	  const int sboxlen = sizeof(KEY_BOX) - 1;

	  const char snparts[] = KEY_NPARTS;
	  const int snpartslen = sizeof(KEY_NPARTS) - 1;

	  const char snpoints[] = KEY_NPOINTS;
	  const int snpointslen = sizeof(KEY_NPOINTS) - 1;

	  const char spartno[] = KEY_PARTNO;
	  const int spartnolen = sizeof(KEY_PARTNO) - 1;

	  if (strncaseequ(p,s,slen)) {
	    ++pc;
	    pc->name = STRDUP(p+slen);
	    pp = NULL;
	    offset = 0;
#ifdef DEBUG
	    fprintf(stderr,"---> in country#%d (%s)\n",
		    n_countries,pc->name);
#endif
	    ++n_countries;
	  }
	  else if (strncaseequ(p,sbox,sboxlen)) {
	    double x[4];
	    if (sscanf(p+sboxlen,"%lf %lf %lf %lf",x,x+1,x+2,x+3) == 4) {
	      pc->minlon = x[0];
	      pc->minlat = x[1];
	      pc->maxlon = x[2];
	      pc->maxlat = x[3];
#ifdef DEBUG
	      fprintf(stderr,
		      "---> box = %.4f %.4f %.4f %.4f\n",
		      x[0],x[1],x[2],x[3]);
#endif
	    }
	  }
	  else if (strncaseequ(p,snparts,snpartslen)) {
	    int n = 0;
	    if (sscanf(p+snpartslen, "%d", &n) == 1 && n > 0) {
	      pc->nparts = n;
	      CALLOC(pc->part, n);
	      pp = pc->part - 1;
#ifdef DEBUG
	      fprintf(stderr,"---> number of parts = %d\n",pc->nparts);
#endif
	    }
	  }
	  else if (strncaseequ(p,snpoints,snpointslen)) {
	    int n = 0;
	    if (sscanf(p+snpointslen, "%d", &n) == 1 && n > 0) {
	      pc->n = n;
	      ALLOC(pc->lat, n);
	      ALLOC(pc->lon, n);
	      offset = 0;
#ifdef DEBUG
	      fprintf(stderr,"---> number of points = %d\n",pc->n);
#endif
	    }
	  }
	  else if (strncaseequ(p,spartno,spartnolen)) {
	    int n = -1;
	    if (sscanf(p+spartnolen, "%d", &n) == 1 && n >= 0) {
	      ++pp;
	      pp->offset = offset;
	      pp->len = 0;
#ifdef DEBUG
	      fprintf(stderr,"---> part number = %d\n",n);
#endif
	    }
	  }
	  else if (*p != '#') {
	    double lon, lat;
	    if (sscanf(p,"%lf %lf", &lon, &lat) == 2) {
	      pc->lat[offset] = lat;
	      pc->lon[offset] = lon;
	      pp->len++;
	      offset++;
	    }
	  }
	}
      }
      fclose(fp);

#ifdef DEBUG
      fprintf(stderr,"*** No ODBs were generated, since this was a DEBUG-run only\n");
      goto release_space;
#endif

      /* Write input files for simulobs2odb */

      /* Sort region with the biggest (most polygons) first */
      qsort(country, n_countries, sizeof(*country), descending);

      for (pc=country; pc && pc->name; ++pc) {
	++poolno;
	{ /* Desc.txt */
	  int j;
	  int creadate, creatime;
	  char *creaby = getenv("USER");
	  const int sourcelen = 10;
	  time_t tp;
	  char buf[80];
	  time(&tp);
	  strftime(buf, sizeof(buf), "%Y%m%d %H%M%S", localtime(&tp));
	  sscanf(buf,"%d %d", &creadate, &creatime);
	  snprintf(txt,sizeof(txt),"%dDesc.txt.%d",jj,poolno);
	  fprintf(stderr, "Creating %s\n",txt);
	  fp = fopen(txt,"w");
	  fprintf(fp,"#desc\n");
	  fprintf(fp,"#/poolno=%d\n",poolno);
	  fprintf(fp,"#/nrows=1\n");
	  fprintf(fp,"#/end\n");
	  fprintf(fp,"\ncreadate creatime creaby region.len latlon_rad");
	  for (j=1; j<=sourcelen; j++) fprintf(fp," source_%d",j);
	  fprintf(fp,"\n");
	  fprintf(fp,"%8.8d,%6.6d,'%8.8s',%d,0",
		  creadate,creatime,creaby?creaby:"unknown",1);
	  {
	    char *s = filename;
	    j = 0;
	    while (j<sourcelen) {
	      char ss[9];
	      int slen = STRLEN(s);
	      if (slen >= 8) {
		slen = 8;
		strncpy(ss,s,slen);
		ss[8] = '\0';
	      }
	      else if (slen > 0 && slen < 8) {
		const char blanks[] = "        ";
		strcpy(ss,s);
		strncat(ss+slen,blanks,slen-8);
		ss[8] = '\0';
	      }
	      else {
		slen = 0;
		strcpy(ss," ");
	      }
	      fprintf(fp,",\"%s\"",ss);
	      s += slen;
	      j++;
	    }
	    fprintf(fp,"\n");
	  }
	  fclose(fp);
	}

	{ /* Region.txt */
	  int j;
	  const int namelen = 8;
	  snprintf(txt,sizeof(txt),"%dRegion.txt.%d",jj,poolno);
	  fprintf(stderr, "Creating %s : %d regions\n",txt,1);
	  fp = fopen(txt,"w");
	  fprintf(fp,"#region\n");
	  fprintf(fp,"#/poolno=%d\n",poolno);
	  fprintf(fp,"#/nrows=%d\n",1);
	  fprintf(fp,"#/end\n");
	  fprintf(fp,"\nid part.len minlat minlon maxlat maxlon");
	  for (j=1; j<=namelen; j++) fprintf(fp," name[%d]",j);
	  fprintf(fp,"\n");
	  if (pc) {
	    int id = 0;
	    char *s = pc->name;
	    char *comma = strchr(s,',');
	    while (comma) {
	      *comma = ' ';
	      comma = strchr(comma+1,',');
	    }
	    fprintf(fp,"%d,%d",++id,pc->nparts);
	    fprintf(fp,",%.6g,%.6g,%.6g,%.6g",
		    pc->minlat, pc->minlon, 
		    pc->maxlat, pc->maxlon);
	    j = 0;
	    while (j<namelen) {
	      char ss[9];
	      int slen = STRLEN(s);
	      if (slen >= 8) {
		slen = 8;
		strncpy(ss,s,slen);
		ss[8] = '\0';
	      }
	      else if (slen > 0 && slen < 8) {
		const char blanks[] = "        ";
		strcpy(ss,s);
		strncat(ss+slen,blanks,slen-8);
		ss[8] = '\0';
	      }
	      else {
		slen = 0;
		strcpy(ss," ");
	      }
	      fprintf(fp,",\"%s\"",ss);
	      s += slen;
	      j++;
	    }
	    fprintf(fp,"\n");
	  }
	  fclose(fp);
	}

	{ /* Part.txt */
	  snprintf(txt,sizeof(txt),"%dPart.txt.%d",jj,poolno);
	  fprintf(stderr, "Creating %s : %d parts\n",txt,pc->nparts);
	  fp = fopen(txt, "w");
	  fprintf(fp,"#part\n");
	  fprintf(fp,"#/poolno=%d\n",poolno);
	  fprintf(fp,"#/nrows=%d\n",pc->nparts); /* optional, but since we know already this ... */
	  fprintf(fp,"#/end\n");
	  fprintf(fp,"\npartno hdr.len\n");
	  if (pc) {
	    int jp;
	    for (jp=0; jp<pc->nparts; jp++) {
	      pp = &pc->part[jp];
	      fprintf(fp,"%d,%d\n",jp+1,pp->len);
	    }
	  }
	  fclose(fp);
	}

	{ /* Hdr.txt */
	  int nvertex = 0;
	  if (pc) {
	    int jp;
	    for (jp=0; jp<pc->nparts; jp++) {
	      pp = &pc->part[jp];
	      nvertex += pp->len;
	    }
	  }
	  snprintf(txt,sizeof(txt),"%dHdr.txt.%d",jj,poolno);
	  fprintf(stderr, "Creating %s : %d vertices\n",txt,nvertex);
	  fp = fopen(txt, "w");
	  fprintf(fp,"#hdr\n");
	  fprintf(fp,"#/poolno=%d\n",poolno);
	  fprintf(fp,"#/nrows=%d\n",nvertex); /* optional, but since we know already this ... */
	  fprintf(fp,"#/end\n");
	  fprintf(fp,"\nlat lon\n");
	  if (pc) {
	    int jp;
	    for (jp=0; jp<pc->nparts; jp++) {
	      int j, jstart, jend;
	      pp = &pc->part[jp];
	      jstart = pp->offset;
	      jend = jstart + pp->len;
	      for (j=jstart; j<jend; j++) {
		fprintf(fp,"%.6g,%.6g\n", pc->lat[j], pc->lon[j]);
#ifdef DEBUG
		if (j == jstart) {
		  fprintf(stderr, " /* part#%d starts here */",jp);
		}
#endif
	      }
	    }
	  }
	  fclose(fp);
	}

	if (!ddl) {
	  /* Create data layout from the embedded input below */
	  static char *layout[] = {
	    "SET $sourcelen = 10;",
	    "",
	    "CREATE TABLE desc AS (",
	    "  source[1:$sourcelen] string, // 80-bytes for source description / source file name",
	    "  latlon_rad pk1int, // ==1 if (lat,lon) is in radians, ==0 if in degrees",
	    "  // creation date, time and created by whom (username)",
	    "  creadate YYYYMMDD,",
	    "  creatime HHMMSS,",
	    "  creaby   string,",
	    "  region @LINK,",
	    ");",
	    "",
	    "SET $namelen = 8;",
	    "",
	    "CREATE TABLE region AS (",
	    "  id pk1int,",
	    "  name[1:$namelen] string,",
	    "  part @LINK,",
	    "  minlat float,",
	    "  minlon float,",
	    "  maxlat float,",
	    "  maxlon float,",
	    ");",
	    "",
	    "CREATE TABLE part AS (",
	    "  partno pk1int,",
	    "  hdr @LINK,",
	    ");",
	    "",
	    "CREATE TABLE hdr AS (",
	    "  lat pk35real,",
	    "  lon pk35real,",
	    ");",
	  };
	  int j, n_layout = sizeof(layout)/sizeof(*layout);
	  char *p, *pout;
	  char *schema = filename;
	  int len;
	  char *ddl_out = NULL;
	  int first = 1;
	  p = strrchr(schema,'/');
	  if (p) { *p++ = '\0'; ddl = STRDUP(p); }
	  else ddl = STRDUP(schema);
	  p = strchr(ddl,'.');
	  if (p) *p = '\0';
	  p = ddl;
	  len = STRLEN(ddl) + 5;
	  ALLOC(ddl_out, len);
	  pout = ddl_out;
	  while (*p) {
	    int c = *p;
	    if (islower(c)) *p = toupper(c);
	    if ((*p >= 'A' && *p <= 'Z') ||
		(!first && *p >= '0' && *p <= '9')) *pout++ = *p;
	    ++p;
	    if (first) first = 0;
	  }
	  *pout = '\0';
	  FREE(ddl);
	  ddl = STRDUP(ddl_out);
	  FREE(ddl_out);
	  len = STRLEN(ddl) + 5;
	  ALLOC(schema, len);
	  snprintf(schema, len, "%s.ddl", ddl);

	  fprintf(stderr,"Creating data definition layout '%s'\n",schema);
	  fp = fopen(schema,"w");
	  for (j=0; j<n_layout; j++) {
	    fprintf(fp,"%s\n",layout[j]);
	  }
	  FREE(schema);
	  fclose(fp);
	}
	++npools;
      } /* for (;;) */

      {
	char *cmd = NULL;
	int len = STRLEN(ddl) + 2000;
	ALLOCX(cmd, len);
	snprintf(cmd,len,
		 "$ODB_BINPATH/simulobs2odb %s-l%s -n%d -1 "
		 "-i%dDesc.txt.%%d "
		 "-i%dRegion.txt.%%d "
		 "-i%dPart.txt.%%d "
		 "-i%dHdr.txt.%%d", 
		 (jj == 1) ? "-c " : "",
		 ddl, poolno,
		 jj,
		 jj,
		 jj,
		 jj);
	fprintf(stderr,"%s region database '%s', poolno(s) from %d to %d\n",
		(jj == 1) ? "Creating" : "Updating", ddl, poolno-npools+1,poolno);
	fprintf(stderr,"Running : %s\n",cmd);
	if (system(cmd) != 0) {
	  fprintf(stderr,"***Error(s) found in simulobs2odb\n");
	  exit(++errcnt);
	}
	snprintf(cmd,len,
		 "rm -f %dDesc.txt.* %dRegion.txt.* %dPart.txt.* %dHdr.txt.*",
		 jj,jj,jj,jj);
	fprintf(stderr,"Running : %s\n",cmd);
	system(cmd);
	FREEX(cmd);
      }

    release_space:
      {
	int j;
	pc = country;
	for (j=0; j<n_countries; j++) {
	  int jp;
	  FREE(pc->name);
	  FREE(pc->part);
	  FREE(pc->lat);
	  FREE(pc->lon);
	  ++pc;
	}
	FREE(country);
      }
    }
    else {
      perror(filename);
      fprintf(stderr,"***Error: Unable to open input file '%s'\n",filename);
      errcnt++;
    }
    FREE(filename);
  } /* for (jj=1; jj<argc; jj++) */

  if (ddl && errcnt == 0) {
    fprintf(stderr,"Changing I/O-method to 4 in order to reduce no. of files\n");
    {
      const char fmt[] = "$ODB_BINPATH/odb1to4 %s";
      char *cmd = NULL;
      int len = STRLEN(fmt) + STRLEN(ddl) + 1;
      ALLOCX(cmd, len);
      snprintf(cmd, len, fmt, ddl);
      fprintf(stderr,"Running : %s\n",cmd);
      if (system(cmd) != 0) {
	fprintf(stderr,"***Error(s) found in %s\n",cmd);
	exit(++errcnt);
      }
      FREEX(cmd);
    }
  }

  if (ddl && errcnt == 0) {
    fprintf(stderr,"Creating tarball out of database '%s'\n",ddl);
    {
      const char fmt[] = "$ODB_BINPATH/odbtar c9v %s.tgz %s";
      char *cmd = NULL;
      int len = STRLEN(fmt) + 2*STRLEN(ddl) + 1;
      ALLOCX(cmd, len);
      snprintf(cmd,len,fmt,ddl,ddl);
      fprintf(stderr,"Running : %s\n",cmd);
      if (system(cmd) != 0) {
	fprintf(stderr,"***Error(s) found in %s\n",cmd);
	exit(++errcnt);
      }
      FREEX(cmd);
    }
    
    fprintf(stderr,
	    "Now you can now untar the tarball '%s.tgz' "
	    "into your $ODB_GISROOT as follows:\n", ddl);
    fprintf(stderr,"  gunzip -c < %s.tgz | (cd $ODB_GISROOT; tar xvf -)\n",ddl);
    fprintf(stderr,"  (cd $ODB_GISROOT; find %s -type d -print | xargs chmod a+rx)\n",ddl);
    fprintf(stderr,"  (cd $ODB_GISROOT; find %s -type f -print | xargs chmod a+r)\n",ddl);
    
    FREE(ddl);
  }

  return errcnt;
}
