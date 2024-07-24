
/* odb2rgg.c */

#include "odbdump.h"
#include "regcache.h"

/* 
   Creates a Reduced Gaussian Grid from observations 
   at a given T-resolution (rtablel_2-type) from observations 
   Output should be suitable for GRIB-generation programs
*/

#define USAGE \
"Usage: odb2rgg [-T<resol>|-t<resol>]\n" \
"               [-c ODB_column_name]\n" \
"               [-f 'FROM-table(s)']\n" \
"               [-w 'where statement']\n" \
"               [-i database_path]\n" \
"               [-p poolmask]\n" \
"               [-b]   # binary output mode\n" \
"               [-g]   # debug output to odb2rgg.stderr\n" \
"               [-N]   # print ALSO missing data boxes\n" \
"               [-h]   # print this help/usage\n" \
"\n" \
"\tIf resolution <resol> is missing, then <resol>=31 is assumed.\n" \
"\tIf no ODB-column is given, then 'obsvalue@body' is assumed.\n" \
"\tIf no FROM-table(s) are given, then 'hdr,body,errstat' is assumed.\n" \
"\tIf no WHERE-statement given, then 'obstype@hdr = $synop AND varno@body = $t2m' is assumed. \n" \
"\t  Note: You should omit the word WHERE !\n" \
"\tIf no database_path given, then 'ECMA.conv' is assumed.\n" \
"\tIf no poolmask given, then all pools will be scanned.\n"

#define FLAGS "bc:f:ghi:Np:t:T:w:"

int main(int argc, char *argv[])
{
  char *database = STRDUP("ECMA.conv");
  char *from = STRDUP("hdr,body,errstat");
  int Txxxx = 31;
  char *colname = STRDUP("obsvalue@body");
  char *where = STRDUP("obstype@hdr = $synop AND varno@body = $t2m");
  char *poolmask = NULL;
  Bool binary = false;
  Bool debug_on = false;
  Bool print_mdi = false;
  int errflg = 0;
  int c;
  extern int optind;

  while ((c = getopt(argc, argv, FLAGS)) != -1) {
    switch (c) {
    case 'b':
      binary = true;
      break;

    case 'c':
      if (colname) FREE(colname);
      colname = STRDUP(optarg);
      break;

    case 'f':
      if (from) FREE(from);
      from = STRDUP(optarg);
      break;

    case 'g':
      debug_on = true;
      break;

    case 'i':
      if (database) FREE(database);
      database = STRDUP(optarg);
      break;

    case 'N':
      print_mdi = true;
      break;

    case 'p':
      if (poolmask) { /* Append more ; remember to add the comma (',') between !! */
	int len = STRLEN(poolmask) + 1 + STRLEN(optarg) + 1;
	char *p;
	ALLOC(p, len);
	snprintf(p, len, "%s,%s", poolmask, optarg);
	FREE(poolmask);
	poolmask = p;
      }
      else {
	poolmask = STRDUP(optarg);
      }
      break;

    case 't':
    case 'T':
      Txxxx = atoi(optarg);
      break;

    case 'w':
      if (where) FREE(where);
      where = STRDUP(optarg);
      break;

    case 'h': /* help !! */
    default:
      ++errflg;
      break;
    }
  } /* while ((c = getopt(argc, argv, FLAGS)) != -1) */

  if (errflg > 0) {
    fprintf(stderr,USAGE);
    goto finish;
  }


  {
    void *h = NULL;
    const double Rmdi = -ABS(mdi);
    int maxcols = 0;
    double *d = NULL;
    const char sqlfmt_txt[] = 
      "SET $Txxxx = %d;"
      "SELECT count(*), avg(%s), stdev(%s), min(%s), max(%s),"
      "       rgg_boxlat(lldegrees(lat@hdr), lldegrees(lon@hdr), $Txxxx),"
      "       rgg_boxlon(lldegrees(lat@hdr), lldegrees(lon@hdr), $Txxxx),"
      "       rgg_boxid(lldegrees(lat@hdr), lldegrees(lon@hdr), $Txxxx),"
      "FROM %s "
      "WHERE %s "
      "ORDERBY %d";
    const char sqlfmt_bin[] = 
      "SET $Txxxx = %d;"
      "SELECT count(*), avg(%s), stdev(%s), min(%s), max(%s),"
      "       rgg_boxid(lldegrees(lat@hdr), lldegrees(lon@hdr), $Txxxx),"
      "FROM %s "
      "WHERE %s "
      "ORDERBY %d";
    int nsort = binary ? 6 : 8;
    char *sql_query = NULL;
    int len = 
      STRLEN(binary ? sqlfmt_bin : sqlfmt_txt) + 
      4 * STRLEN(colname) + STRLEN(from) + 
      STRLEN(where) + 100;

    if (debug_on) (void) ODBc_debug_fp(stderr);
    odbdump_reset_stderr(NULL, "odb2rgg.stderr", NULL);

    ALLOC(sql_query, len);
    snprintf(sql_query, len,
	     binary ? sqlfmt_bin : sqlfmt_txt,
	     Txxxx, 
	     colname, colname, colname, colname,
	     from, where, nsort);

    h = odbdump_open(database, sql_query, NULL, poolmask, NULL, &maxcols);

    if (debug_on) fprintf(stderr, "h = %p, maxcols = %d, nsort = %d\n", h, maxcols, nsort);
    
    if (h && maxcols >= nsort) {
      FILE *fp = stdout;
      regcache_t *regp = ODBc_get_rgg_cache(Txxxx); /* Blows up if unsupported resolution */
      int nboxes = regp->nboxes;
      int last_boxid = 0;
      int dlen = maxcols;
      int nd, new_dataset = 0;
      colinfo_t *ci = NULL;
      int nci = 0;
      static const char fmt1[] = "#%11s %12s %20s %20s %20s %20s %20s %20s\n";
      static const char fmt2[] = "%12d %12d %20.12g %20.12g %20.12g %20.12g %20.12g %20.12g\n";


      if (binary) {
	int reclen = sizeof(Txxxx) + sizeof(nboxes) + sizeof(Rmdi);
	fwrite(&reclen, sizeof(reclen), 1, fp); /* Fortran start record delimeter */
	fwrite(&Txxxx, sizeof(Txxxx), 1, fp);   /* integer*4 */
	fwrite(&nboxes, sizeof(nboxes), 1, fp); /* integer*4 */
	fwrite(&Rmdi, sizeof(Rmdi), 1, fp);     /* real*8 */
	fwrite(&reclen, sizeof(reclen), 1, fp); /* Fortran end record delimeter */
      }
      else {
	fprintf(fp,"%d    # Resolution w.r.t. rtablel_2<resol>\n", Txxxx);
	fprintf(fp,"%d    # of points or boxes\n", nboxes);
	fprintf(fp,"%.12g # missing data indicator\n", Rmdi);
	/* The following are present only for the non-binary output */
	fprintf(fp,"%s    # database\n", database);
	fprintf(fp,"%s    # poolmask\n", poolmask ? poolmask : "-1");
	fprintf(fp,"%s    # ODB-column in concern\n", colname);
	fprintf(fp,"%s    # FROM-tables\n", from);
	fprintf(fp,"%s    # WHERE-condition\n", where);
	fprintf(fp,fmt1, "boxid", "count", "average", "stdev", "min", "max", "lat", "lon");
      }

      ALLOCX(d, maxcols);
      
      while ( (nd = odbdump_nextrow(h, d, dlen, &new_dataset)) > 0) {
	if (new_dataset) {
	  /* New query ? */
	  ci = odbdump_destroy_colinfo(ci, nci);
	  ci = odbdump_create_colinfo(h, &nci);
	}

	nd = MIN(nd, nci);

	{
	  int i, boxid = (int)d[nd-1];
	  int cnt = 0;
	  double avg = Rmdi, stdev = Rmdi;
	  double lat = Rmdi, lon = Rmdi;
	  double min = Rmdi, max = Rmdi;

	  if (print_mdi && boxid > last_boxid + 1) {
	    /* Generate missing data info */
	    int j;
	    for (j=last_boxid + 1; j<boxid; j++) {
	      if (binary) {
		/* Binary output ignores lat,lon & boxid */
		int reclen = sizeof(j) + sizeof(cnt) + 
		  sizeof(avg) + sizeof(stdev) + sizeof(min) + sizeof(max);
		fwrite(&reclen, sizeof(reclen), 1, fp); /* Fortran start record delimeter */
		fwrite(&j, sizeof(j), 1, fp);           /* integer*4 */
		fwrite(&cnt, sizeof(cnt), 1, fp);       /* integer*4 */
		fwrite(&avg, sizeof(avg), 1, fp);       /* real*8 */
		fwrite(&stdev, sizeof(stdev), 1, fp);   /* real*8 */
		fwrite(&min, sizeof(min), 1, fp);       /* real*8 */
		fwrite(&max, sizeof(max), 1, fp);       /* real*8 */
		fwrite(&reclen, sizeof(reclen), 1, fp); /* Fortran end record delimeter */
	      }
	      else {
		fprintf(fp, fmt2, j, cnt, avg, stdev, min, max, lat, lon);
	      }
	    } /* for (j=last_boxid + 1; j<boxid; j++) */
	  }

	  cnt = (int)d[0];
	  avg = d[1];
	  stdev = d[2];
	  min = d[3];
	  max = d[4];

	  if (print_mdi || ABS(avg) != mdi) {
	    if (binary) {
	      /* Binary output ignores lat,lon & boxid */
	      int reclen = sizeof(boxid) + sizeof(cnt) + 
		sizeof(avg) + sizeof(stdev) + sizeof(min) + sizeof(max);
	      fwrite(&reclen, sizeof(reclen), 1, fp); /* Fortran start record delimeter */
	      fwrite(&boxid, sizeof(boxid), 1, fp);   /* integer*4 */
	      fwrite(&cnt, sizeof(cnt), 1, fp);       /* integer*4 */
	      fwrite(&avg, sizeof(avg), 1, fp);       /* real*8 */
	      fwrite(&stdev, sizeof(stdev), 1, fp);   /* real*8 */
	      fwrite(&min, sizeof(min), 1, fp);       /* real*8 */
	      fwrite(&max, sizeof(max), 1, fp);       /* real*8 */
	      fwrite(&reclen, sizeof(reclen), 1, fp); /* Fortran end record delimeter */
	    }
	    else {
	      lat = d[5];
	      lon = d[6];
	      fprintf(fp, fmt2, boxid, cnt, avg, stdev, min, max, lat, lon);
	    }
	  }

	  last_boxid = boxid;
	}
      } /* while (...) */

      if (print_mdi && last_boxid < nboxes) {
	/* Generate remainder of the missing data info */
	int i, j;
	const int cnt = 0;
	const double avg = Rmdi, stdev = Rmdi;
	const double lat = Rmdi, lon = Rmdi;
	const double min = Rmdi, max = Rmdi;
	for (j=last_boxid + 1; j<=nboxes; j++) {
	  if (binary) {
	    /* Binary output ignores lat,lon & boxid */
	    int reclen = sizeof(j) + sizeof(cnt) + 
	      sizeof(avg) + sizeof(stdev) + sizeof(min) + sizeof(max);
	    fwrite(&reclen, sizeof(reclen), 1, fp); /* Fortran start record delimeter */
	    fwrite(&j, sizeof(j), 1, fp);           /* integer*4 */
	    fwrite(&cnt, sizeof(cnt), 1, fp);       /* integer*4 */
	    fwrite(&avg, sizeof(avg), 1, fp);       /* real*8 */
	    fwrite(&stdev, sizeof(stdev), 1, fp);   /* real*8 */
	    fwrite(&min, sizeof(min), 1, fp);       /* real*8 */
	    fwrite(&max, sizeof(max), 1, fp);       /* real*8 */
	    fwrite(&reclen, sizeof(reclen), 1, fp); /* Fortran end record delimeter */
	  }
	  else {
	    fprintf(fp, fmt2, j, cnt, avg, stdev, min, max, lat, lon);
	  }
	} /* for (j=last_boxid + 1; j<=nboxes; j++) */
      }

      ci = odbdump_destroy_colinfo(ci, nci);
      errflg = odbdump_close(h);

      FREEX(d);
    
    }  /* if (h && maxcols >= nsort) ... */
    else {
      errflg = -1;
    }

    FREE(sql_query);
  }

 finish:
  return errflg;
}
