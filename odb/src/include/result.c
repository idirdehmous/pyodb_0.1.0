
/*!
 \file result.c
 \brief write the result of an SQL query in an output file. 
 The output format can be:
      - default           Ascii where field delimiter is space (" "), and output is rather compressed
      - binary            binary format will be used for output
      - plotobs           $ODB_PLOTTER compatible binary file for Magics will be created
      - wplotobs          $ODB_PLOTTER compatible binary file for Magics will be created (wind-arrows)
      - odbtool           input(s) for IDL/odbtool (by Phil Watts while at ECMWF, now in Eumetsat) will be created
      - odbtk             field delimiters become "!,!" and unique column ids ($uniq#) will be printed as "zero-th" column
      - odb               "odbviewer"/"odbless" .rpt-file format is emulated
      - netcdf            create NetCDF-file with ODB Conventions (lat/lon are in degrees)
      - unetcdf           as "netcdf" above, but create without packing (lat/lon are in degrees)
      - dump              similar to "default", but delimiter is comma "," (lat/lon are in degrees)
      - bindump           similar to "binary", but all data will be printed row_wise + metadata & title-structure always embedded (lat/lon are in degrees)
      - geo               (standard) Metview GEO-points file with "lat lon level date time value"
      - geo:xyv           GEO-points file with "x/long y/lat value"
      - geo:xy_vector     GEO-points file with "lat lon height date time u v"
      - geo:polar_vector  GEO-points file with "lat lon height date time speed direction"


*/

#include "odb.h"
#include "odb_macros.h"
#include "dca.h"
#include "evaluate.h"
#include "info.h"
#include "result.h"
#include "iostuff.h"
#include "cdrhook.h"
#include "magicwords.h"
#include "swapbytes.h"
#include "idx.h"
#include "bits.h"
#include "cmaio.h"
#include "codb_netcdf.h"

#define COLUMN_FETCH_ERROR() \
fprintf(stderr, \
	"***Error: Unable to fetch column '%s' for pool#%d. " \
	"Expecting %d (= # of rows in table '%s'), but got %d rows.\n", \
	s, poolno, tc->nrows, tc->name, Nrows)

/*!
 Modified from the version in odb/compiler/odb98.c. 
 The same as fprintf(), but does nothing when fp points to NULL
  @param fp File pointer
  @param format Format of the format string (see fprintf manual for more information)
  @return Upon successful return, it returns the number of characters
       printed  (not  including  the  trailing  '\0'  used  to  end  output to
       strings).
 */
PUBLIC int
ODB_fprintf(FILE *fp, const char *format, ...)
{ 
  int n = 0;
  if (fp) {
    va_list args;
    va_start(args, format);
    n = vfprintf(fp, format, args);
    va_end(args);
  }
  return n;
}


typedef struct _cmap_t {
  char *fname;
  char text[80+1];
  char var[80+1];
  float value_min;
  float value_max;
  float scale[6];
} cmap_t;

PRIVATE cmap_t *cmap = NULL; /* Not thread safe */

PUBLIC Bool
ODBc_set_kolor_map(const char *cmapfile)
{
  cmap_t *p = NULL;
  DRHOOK_START(ODBc_set_kolor_map);
  if (cmap) {
    FREE(cmap->fname);
    FREE(cmap);
  }
  if (cmapfile && !strequ(cmapfile,"/dev/null")) {
    FILE *fp = NULL;
    int iounit = -1;
    int iret = 0;
    cma_open_(&iounit, cmapfile, "r", &iret, STRLEN(cmapfile), 1);
    fp = (iounit >= 0 && iret == 1) ? CMA_get_fp(&iounit) : NULL;
    if (fp) {
      char line[80+1];
      char *s;
      CALLOC(p, 1);
      p->fname = STRDUP(cmapfile);
      memset(p->text, ' ', 80);
      memset(p->var, ' ', 80);
      p->value_min = 0;
      p->value_max = -1;
      memset(p->scale, 0, sizeof(float) * 6);
      /* Read 1st line */
      if ((s = fgets(line, sizeof(line), fp)) != NULL) {
	int len;
	char *nl = strchr(s, '\n'); if (nl) *nl = '\0';
	strcpy(p->text, s);
	len = strlen(p->text);
	if (len < 80) memset(p->text+len,' ',80-len);
	/* Read 2nd line */
	s = fgets(line, sizeof(line), fp);
	if (s) {
	  nl = strchr(s, '\n'); if (nl) *nl = '\0'; 
	  strcpy(p->var, s);
	  len = strlen(p->var);
	  if (len < 80) memset(p->var+len,' ',80-len);
	  /* Read 3rd line */
	  s = fgets(line, sizeof(line), fp);
	  if (s) {
	    nl = strchr(s, '\n'); if (nl) *nl = '\0';
	    while (*s) {
	      if (*s == '!') {
		*s = '\0';
		break;
	      }
	      else if (*s == ',') {
		*s = ' ';
	      }
	      s++;
	    }
	    {
	      float x,y;
	      int nelem = sscanf(line, "%f %f", &x, &y);
	      if (nelem == 2) {
		p->value_min = x;
		p->value_max = y;
		/* Read 4th and last line */
		s = fgets(line, sizeof(line), fp);
		if (s) {
		  int iscale = 0;
		  nl = strchr(s, '\n'); if (nl) *nl = '\0';
		  while (*s) {
		    if (*s == '!') {
		      *s = '\0';
		      break;
		    }
		    else if (*s == ',') {
		      *s = ' ';
		    }
		    s++;
		  }
		  nelem = sscanf(line, "%d", &iscale);
		  if (nelem == 1) {
		    if (iscale >= 1 && iscale <= 4) {
		      nelem = sscanf(line, "%d %f %f", &iscale, &x, &y);
		      if (nelem == 3) {
			p->scale[0] = iscale;
			p->scale[1] = x;
			p->scale[2] = y;
		      } /* if (nelem == 3) */
		    } /* if (iscale >= 1 && iscale <= 4) */
		  } /* if (nelem == 1) */
		} /* if (s) */
	      } /* if (nelem == 2) */
	    }
	  } /* if (s) */
	} /* if (s) */
      } /* if ((s = fgets(line, sizeof(line), fp)) != NULL) */
      p->text[80] = '\0';
      p->var[80] = '\0';
      cma_close_(&iounit, &iret);
      cmap = p;
    } /* if (fp) */
  }
  DRHOOK_END(0);
  return p ? true : false;
}

PUBLIC Bool 
ODBc_get_kolor_map(float *value_min, float *value_max, float scale[], 
		   char *text, int textlen)
{ 
  Bool ok = false;
  DRHOOK_START(ODBc_get_kolor_map);
  if (cmap) {
    cmap_t *p = cmap;
    int len = MIN(textlen,80);
    if (text) strncpy(text, p->text, textlen);
    if (scale) memcpy(scale, p->scale, sizeof(float) * 6);
    if (value_min) *value_min = p->value_min;
    if (value_max) *value_max = p->value_max;
    ok = true;
  }
  DRHOOK_END(0);
  return ok;
}


PRIVATE char *output_format = NULL; /* Not thread safe */

PUBLIC void
ODBc_set_format(const char *format)
{
  FREE(output_format);
  if (format) output_format = STRDUP(format);
}

PUBLIC Bool
ODBc_test_format_1(const char *test_format)
{
  return strequ(output_format, test_format) ? true : false;
}

PUBLIC Bool
ODBc_test_format_2(const char *format, const char *test_format)
{
  return format ? strequ(format, test_format) : ODBc_test_format_1(test_format);
}

PUBLIC Bool
ODBc_test_format_3(const char *format, const char *test_format, int len)
{
  return format ? strnequ(format, test_format, len) : strnequ(output_format, test_format, len);
}


PRIVATE int
GeoFormat()
{
  int fmt = 0;
  Bool is_geo = ODBc_test_format_3(output_format, "geo", 3);
  if (is_geo) {
    if (ODBc_test_format_2(output_format, "geo")) fmt = 1;
    else if (ODBc_test_format_2(output_format, "geo:xyv")) fmt = 2;
    else if (ODBc_test_format_2(output_format, "geo:xy_vector")) fmt = 3;
    else if (ODBc_test_format_2(output_format, "geo:polar_vector")) fmt = 4;
    else fmt = 0;
  }
  return fmt;
}


PUBLIC
Bool ODBc_nothing_to_plot(void *Info)
{
  Bool nothing = true;
  FILE *fp_echo = ODBc_get_debug_fp();

  DRHOOK_START(ODBc_nothing_to_plot);
  if (Info) {
    info_t *info = Info;
    int cnt = 0;

    if (info->create_index == 0 && info->odb_lat && info->odb_lon) {
      Bool is_plotobs = ODBc_test_format_1("plotobs");
      Bool warrow = ODBc_test_format_1("wplotobs");
      Bool is_odbtool = ODBc_test_format_1("odbtool");
      int geofmt = GeoFormat();
      const char *lat = info->odb_lat->name;
      const char *lon = info->odb_lon->name;
      int j, ncols = info->ncols_true;

      ODB_fprintf(fp_echo,"ODBc_nothing_to_plot(): ncols = %d\n",ncols);
      ODB_fprintf(fp_echo,"ODBc_nothing_to_plot(): lat = '%s'\n",lat);
      ODB_fprintf(fp_echo,"ODBc_nothing_to_plot(): lon = '%s'\n",lon);

      if (is_plotobs || warrow) {
	/* Any two $ODB_LAT & $ODB_LON entries would do */
	ODB_fprintf(fp_echo,"ODBc_nothing_to_plot(): For %splotobs\n",
		    warrow ? "w" : "");

	for (j=0; j<ncols && cnt < 2; j++) {
	  col_t *colthis = &info->c[j];
	  const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	  ODB_fprintf(fp_echo,
		      "ODBc_nothing_to_plot(): info->c[%d].%s = '%s'\n",
		      j, colthis->nickname ? "nickname" : "name", s ? s : NIL);
	  if (strequ(s, lat)) {
	    cnt++;
	    lat = NULL; /* don't bovver to test me anymore */
	  }
	  else if (strequ(s, lon)) {
	    cnt++;
	    lon = NULL; /* don't bovver to test me anymore */
	  }
	  ODB_fprintf(fp_echo,"ODBc_nothing_to_plot(): cnt now = %d\n",cnt);
	}

	nothing = (cnt == 2) ? false : true;
      }
      else if (is_odbtool) {
	/* Following restrictions apply:
	   1) lat and lon exactly in the first & second columns, in this order
	   2) body.len in the third (but here we accept "$hdr.body#", too
	   3) At least 4 columns must be present
	*/

	if (ncols >= 4) {
	  col_t *colthis;
	  const char *s;

	  /* First column */
	  j=0;
	  colthis = &info->c[j];
	  s = colthis->nickname ? colthis->nickname : colthis->name;
	  ODB_fprintf(fp_echo,
		      "ODBc_nothing_to_plot(): info->c[%d].%s = '%s'\n",
		      j, colthis->nickname ? "nickname" : "name", s ? s : NIL);
	  if (strequ(s, lat)) cnt++;

	  /* Second column */
	  j=1;
	  colthis = &info->c[j];
	  s = colthis->nickname ? colthis->nickname : colthis->name;
	  ODB_fprintf(fp_echo,
		      "ODBc_nothing_to_plot(): info->c[%d].%s = '%s'\n",
		      j, colthis->nickname ? "nickname" : "name", s ? s : NIL);
	  if (strequ(s, lon)) cnt++;

	  /* Third column */
	  j=2;
	  colthis = &info->c[j];
	  s = colthis->nickname ? colthis->nickname : colthis->name;
	  ODB_fprintf(fp_echo,
		      "ODBc_nothing_to_plot(): info->c[%d].%s = '%s'\n",
		      j, colthis->nickname ? "nickname" : "name", s ? s : NIL);
	  {
	    const char bodylen1[] = "$hdr.body#";
	    const char bodylen2[] = "body.len@hdr";
	    const char bodylen3[] = "body.len";
	    const char bodylen4[] = "LINKLEN(body)@hdr";
	    if (strequ(s, bodylen1) || strequ(s, bodylen2) ||
		strequ(s, bodylen3) || strequ(s, bodylen4)) cnt++;
	  }

	  nothing = (cnt == 3) ? false : true;
	}
      }
      else if (geofmt) {
	/* 
	   Recognized MetView GEO-points formats :
	   --------------------------------------
	   1 = lat      long       level          date      time        value
	   2 = #FORMAT XYV = x/long       y/lat       value
	   3 = #FORMAT XY_VECTOR = lat        lon height      date        time        u       v
	   4 = #FORMAT POLAR_VECTOR = lat        lon     height      date        time        speed   direction

	   !!! Please note, that for some reason the 2nd format (XYV) requires longitude first !!!
	*/

	/* 
	   First check the number of columns :
	   1 == 6 columns
	   2 == 3 columns
	   3 == 7 columns
	   4 == 7 columns
	*/
	
	Bool is_okay = false;
	const int req_ncols[1+4] = { -1, 6, 3, 7, 7 };

	if      (geofmt == 1 && ncols == req_ncols[geofmt]) is_okay = true;
	else if (geofmt == 2 && ncols == req_ncols[geofmt]) is_okay = true;
	else if (geofmt == 3 && ncols == req_ncols[geofmt]) is_okay = true;
	else if (geofmt == 4 && ncols == req_ncols[geofmt]) is_okay = true;

	if (is_okay) {
	  /* So far ok; now check that (lat,lon) exist, and (date,time) [except for fmt=2] */
	  col_t *colthis;
	  const char *s;

	  /* First column */
	  j=0;
	  colthis = &info->c[j];
	  s = colthis->nickname ? colthis->nickname : colthis->name;
	  ODB_fprintf(fp_echo,
		      "ODBc_nothing_to_plot(): info->c[%d].%s = '%s'\n",
		      j, colthis->nickname ? "nickname" : "name", s ? s : NIL);
	  if (strequ(s, (geofmt == 2) ? lon : lat)) cnt++;

	  /* Second column */
	  j=1;
	  colthis = &info->c[j];
	  s = colthis->nickname ? colthis->nickname : colthis->name;
	  ODB_fprintf(fp_echo,
		      "ODBc_nothing_to_plot(): info->c[%d].%s = '%s'\n",
		      j, colthis->nickname ? "nickname" : "name", s ? s : NIL);
	  if (strequ(s, (geofmt == 2) ? lat : lon)) cnt++;

	  if (geofmt != 2) {
	    const char *date = info->odb_date->name;
	    const char *time = info->odb_time->name;

	    /* Fourth column */
	    j=3;
	    colthis = &info->c[j];
	    s = colthis->nickname ? colthis->nickname : colthis->name;
	    ODB_fprintf(fp_echo,
			"ODBc_nothing_to_plot(): info->c[%d].%s = '%s'\n",
			j, colthis->nickname ? "nickname" : "name", s ? s : NIL);
	    if (strequ(s, date)) cnt++;

	    /* Fifth column */
	    j=4;
	    colthis = &info->c[j];
	    s = colthis->nickname ? colthis->nickname : colthis->name;
	    ODB_fprintf(fp_echo,
			"ODBc_nothing_to_plot(): info->c[%d].%s = '%s'\n",
			j, colthis->nickname ? "nickname" : "name", s ? s : NIL);
	    if (strequ(s, time)) cnt++;

	    is_okay = (cnt == 4) ? true : false;
	  }
	  else {
	    is_okay = (cnt == 2) ? true : false;
	  }
	} /* if (is_okay) */

	if (!is_okay) {
	  ODB_fprintf(stderr,
		      "***Warning: Nothing to plot for GEO-points format#%d. Check your SQL.\n",geofmt);
	  nothing = true;
	}
	else {
	  nothing = false;
	}

      } /* if (is_plotobs || warrow) ... else if (is_odbtool) ... else if (geofmt) ... */
    } /* if (info->create_index == 0 && info->odb_lat && info->odb_lon) */

    ODB_fprintf(fp_echo,"ODBc_nothing_to_plot(): cnt = %d\n",cnt);
  }

  ODB_fprintf(fp_echo,"ODBc_nothing_to_plot() = %s\n",nothing ? "true" : "false");
  DRHOOK_END(0);
  return nothing;
}

PRIVATE table_t *
GetTable(info_t *info, const char *colname)
{
  table_t *tc = NULL;
  DRHOOK_START(GetTable);
  if (info && colname) { 
    const char *tblname = IS_HASH(colname) ? colname : strchr(colname, '@');
    if (tblname++) {
      table_t *t = info->t;
      const char *poffset = GET_OFFSET(tblname);
      int len = poffset ? (int)(poffset - tblname) : STRLEN(tblname);
      while (t) {
	if (strnequ(t->name, tblname, len)) {
	  tc = t;
	  break;
	}
	t = t->next;
      } /* while (t) */
    } /* if (tblname) */
  } /* if (info && colname) */
  DRHOOK_END(0);
  return tc;
}


PRIVATE Bool
isPowerOfTwo(int iarg)
{
  return (IAND(iarg,-iarg) == iarg) ? true : false;
}


PUBLIC int
ODBc_lda(int m, int method)
{ /* As in odb/module/odbshared.F90 */
  int lda;
  DRHOOK_START(ODBc_lda);
  m = MAX(1,m);
  switch (method) {
  case 0: lda = m + (m+1)%2; break;
  case 1: lda = m; break;
  case 2: if (isPowerOfTwo(m)) m++; lda = m; break;
  default: lda = m; break;
  }
  DRHOOK_END(0);
  return lda;
}


PUBLIC void *
ODBc_new_res(const char *label, const char *file, int lineno,
	     int nrows, int ncols, void *Info, Bool row_wise, int poolno)
{
  FILE *fp_echo = ODBc_get_debug_fp();
  const int method = 1; /* Changed from 2 to 1 by SS/25-Feb-2014 */
  int nra = row_wise ? ODBc_lda(ncols, method) : ODBc_lda(nrows, method);
  int nmem = row_wise ? nra * nrows : nra * ncols;
  int i, k;
  info_t *info = Info;
  result_t *newres = NULL;
  DRHOOK_START(ODBc_new_res);

  /* Allocate and initialize */

  CALLOC(newres, 1);

  if (fp_echo && label) {
    ODB_fprintf(fp_echo,"ODBc_new_res => %s (%s:%d) : addr=%p "
		"nrows=%d, ncols=%d, nra=%d, row_wise=%s, poolno=%d\n",
		label, file, lineno, newres,
		nrows, ncols, nra, row_wise ? "true" : "false", poolno);
  }

  CALLOC(newres->mem, nmem);
  newres->nmem = nmem;
  newres->ncols_in = newres->ncols_out = ncols;
  newres->nrows_in = newres->nrows_out = nrows;
  newres->nra = nra;
  newres->row_wise = row_wise;
  newres->poolno = poolno;
  newres->info = info;
  newres->next = NULL;
  newres->backup = false;
  newres->backup_file = NULL;
  
  if (row_wise) {
    ALLOC(newres->d, nrows);
    k = 0;
    for (i=0; i<nrows; i++) {
      newres->d[i] = &newres->mem[k];
      k += nra;
    }
  }
  else {
    ALLOC(newres->d, ncols);
    k = 0;
    for (i=0; i<ncols; i++) {
      newres->d[i] = &newres->mem[k];
      k += nra;
    }
  }

  DRHOOK_END(0);
  return newres;
}


PUBLIC void *
ODBc_merge_res(const char *label, const char *file, int lineno,
	       void *Res, int ncols, void *Info, Bool row_wise)
{
  FILE *fp_echo = ODBc_get_debug_fp();
  info_t *info = Info;
  result_t *res = Res;
  result_t *newres = NULL;
  result_t *r = res;
  int chain_len = 0;
  int count_row_wises_are_the_same = 0;
  int nrows = 0;
  DRHOOK_START(ODBc_merge_res);

#if 0
  fprintf(stderr,"ODBc_merge_res() < res follows\n");
  ODBc_print_data(NULL, stderr, res, 
		  "odb", "-",
		  0, 1, 0,
		  NULL);
#endif

  while (r) {
    nrows += r->nrows_out;
    if (r->row_wise == row_wise) count_row_wises_are_the_same++;
    r = r->next;
    chain_len++;
  } /* while (r) */

  if (fp_echo && label) {
    ODB_fprintf(fp_echo,"ODBc_merge_res => %s (%s:%d) : addr=%p, info=%p "
		"nrows=%d, ncols=%d, chain_len=%d, row_wise=%s\n",
		label, file, lineno, res, info,
		nrows, ncols, chain_len, row_wise ? "true" : "false");
  }

  if (res && nrows > 0 && (chain_len > 1 || count_row_wises_are_the_same < chain_len)) {
    int i, j, k;

    newres = ODBc_new_res("ODBc_merge_res => newres", 
			  __FILE__, __LINE__,
			  nrows, ncols, info, row_wise, 
			  (chain_len > 1) ? -1 : res->poolno);

    /* Copy from the existing res-chain */
    k = 0;
    r = res;
    while (r) {
      int nr = r->nrows_out;
      for (i=0; i<ncols; i++) {
	for (j=0; j<nr; j++) {
	  double value = r->row_wise ? r->d[j][i] : r->d[i][j];
	  if (row_wise) {
	    newres->d[k+j][i] = value;
	  }
	  else {
	    newres->d[i][k+j] = value;
	  }
	}
      }
      k += nr;
      r = r->next;
    } /* while (r) */

    /* Get rid off the old res, which potentially releases a lot of memory */
    (void) ODBc_unget_data(res);
  }
  else {
    newres = res;
  }

#if 0
  fprintf(stderr,"ODBc_merge_res() > newres follows\n");
  ODBc_print_data(NULL, stderr, newres, 
		  "odb", "-",
		  0, 1, 0,
		  NULL);
#endif
  DRHOOK_END(0);
  return newres;
}


PRIVATE int
DynFill(double data[], int ndata,
	const int idx[], int idxlen,
	int istart, int iend, int ioffset)
{
  int rc = 0;
  int inc = iend - istart;
  DRHOOK_START(DynFill);
  if (idx && idxlen > 0 && idxlen >= inc &&
      data && ndata >= inc && inc >= 1) {
    int j, js, prev, len;
    js = istart;
    prev = idx[js];
    len = 1;
    for (j=istart+1; j<iend; j++) {
      if (prev != idx[j]) {
	int i, je = j;
	for (i=js; i<je; i++) data[i-ioffset] = len;
	js = j;
	prev = idx[js];
	len = 1;
      }
      else
	len++;
    } /* for (j=istart+1; j<iend; j++) */
    if (len > 0) {
      int i, je = iend;
      for (i=js; i<je; i++) data[i-istart] = len;
    }
    rc = inc;
  } /* if (idx && idxlen > 0 && ... */
  DRHOOK_END(0);
  return rc;
}


PUBLIC int
ODB_dynfill(const char *s,
	    void *V,
	    int *(*getindex)(void *V, const char *table, int *lenidx),
	    double data[], int ndata,
	    int istart, int iend, int ioffset)
{
  int rc = 0;
  DRHOOK_START(ODB_dynfill);
  if (getindex && IS_USDDOTHASH(s) && 
      data && ndata > 0) {
    char *p = STRDUP(s);
    char *dot = strchr(p, '.');
    if (dot) {
      int *idx = NULL;
      int lenidx = 0;
      char *parent_table = p+1;
      *dot = '\0';
      idx = getindex(V, parent_table, &lenidx);
      rc = DynFill(data, ndata,
		   idx, lenidx,
		   istart, iend, ioffset);
    }
    FREE(p);
  }
  DRHOOK_END(0);
  return rc;
}


PUBLIC void
ODBc_DebugPrintRes(const char *when, const result_t *r)
{
  FILE *fp_echo = ODBc_get_debug_fp();
  if (fp_echo && when && r) {
    int i, j, maxrows;
    uresult_t u;
    fprintf(fp_echo,"\n");
    fprintf(fp_echo,
	    "ODBc_DebugPrintRes for '%s' : nrows = %d, ncols = %d, nra=%d, row_wise = %s\n",
	    when, r->nrows_out, r->ncols_out, r->nra, r->row_wise ? "true" : "false");
    u.res = (result_t *)r; /* not changing the const value, though */
    fprintf(fp_echo,
	    "ODBc_DebugPrintRes: u.res = %p, u.alias = %.14g, u.i[0:1] = (%d, %d)\n",
	    u.res, u.alias, u.i[0], u.i[1]);
    maxrows = MIN(100,r->nrows_out);
    for (j=0; j<maxrows; j++) {
      fprintf(fp_echo,"row#%d: ",j+1);
      for (i=0; i<r->ncols_out; i++) {
	fprintf(fp_echo," %.14g",r->row_wise ? r->d[j][i] : r->d[i][j]);
      }
      fprintf(fp_echo,"\n");
    }
    if (maxrows < r->nrows_out) fprintf(fp_echo, "... etc.\n");
    fprintf(fp_echo,"\n");
  }
}

PUBLIC void *
ODBc_get_data_from_binary_file(const char *filename, Boolean sort_unique)
{
  result_t *res = NULL;
  DRHOOK_START(ODBc_get_data_from_binary_file);
  {
    Boolean is_pipe = (filename && *filename == '|');
    FILE *fp = is_pipe ? popen(++filename,"r") : fopen(filename,"r");
    if (fp) {
      Boolean on_error = false;
      int nrows_tot = 0;
      int ncols_here = 0;
      result_t *last = NULL;
      int word[2];
      /* setvbuf here */
      while (!on_error && fread(word, sizeof(*word), 2, fp) == 2) {
	if (word[0] == ODBX) { /* initially no byteswapping; assuming the same machine */
	  int *hdr = NULL;
	  int nhdr = word[1];
	  ALLOCX(hdr, nhdr);
	  hdr[0] = word[0];
	  hdr[1] = word[1];
	  if (fread(hdr+2, sizeof(*hdr), nhdr-2, fp) == nhdr - 2) {
	    int poolno = hdr[5];
	    int nrows = hdr[6];
	    int ncols = hdr[7];
	    Boolean row_wise = hdr[8] ? true : false;
	    result_t *r = ODBc_new_res("ODBc_get_data_from_binary_file => r",
				       __FILE__, __LINE__,
				       nrows, ncols, NULL, row_wise, poolno);
	    int i;
	    if (ncols_here == 0) ncols_here = ncols;
	    nrows_tot += nrows;
	    if (row_wise) { /* Row-wise (C) */
	      for (i=0; i<nrows; i++) {
		double *value = &r->d[i][0];
		if (fread(value, sizeof(*value), ncols, fp) != ncols) {
		  on_error = true;
		  goto bailout;
		}
	      } /* for (i=0; i<nrows; i++) */
	    }
	    else { /* Column-wise (Fortran) */
	      for (i=0; i<ncols; i++) {
		double *value = &r->d[i][0];
		if (fread(value, sizeof(*value), nrows, fp) != nrows) {
		  on_error = true;
		  goto bailout;
		}
	      } /* for (i=0; i<ncols; i++) */
	    }
	  bailout:
	    if (!res) {
	      res = last = r;
	    }
	    else {
	      last->next = r;
	      last = r;
	    }
	  } /* if (fread(hdr+2, sizeof(*hdr), nhdr-2, fp) == nhdr - 2) */
	  else {
	    on_error = true;
	  }
	  FREEX(hdr);
	} /* if (word[0] == ODBX) */
	else {
	  on_error = true;
	}
      } /* while (!on_error && fread(word, sizeof(*word), 2, fp) == 2) */ 

      is_pipe ? pclose(fp) : fclose(fp);

      if (on_error) {
	res = ODBc_unget_data(res); /* Some error(s) were encountered */
      }
      else if (res && sort_unique) {
	/* Create a new result set with just 
	   unique numbers across row sorted in an ascending order (w.r.t column#1 only) */
	result_t *r, *pr;
	int *idx = NULL;
	Boolean row_wise = false;
	int nrows = nrows_tot;
	int ncols = ncols_here;
	int jj = 0;
	int lda;
	r = ODBc_new_res("ODBc_get_data_from_binary_file => sort_unique",
			 __FILE__, __LINE__,
			 nrows, ncols, NULL, row_wise, -1);
	lda = r->nra;
	pr = res;
	while (pr) {
	  /* Note : the new result set (r) is column-wise (Fortran) i.e. its r->row_wise == false */
	  int nr = pr->nrows_out;
	  int nc = MIN(pr->ncols_out, ncols);
	  int i, j;
	  if (pr->row_wise) { /* Row-wise (C) */
	    for (i=0; i<nc; i++) {
	      for (j=0; j<nr; j++) {
		r->d[i][jj+j] = pr->d[j][i];
	      } /* for (j=0; j<nr; j++) */
	    } /* for (i=0; i<nc; i++) */
	  }
	  else { /* Column-wise (Fortran) */
	    for (i=0; i<nc; i++) {
	      const double *value = &pr->d[i][0];
	      memcpy(&r->d[i][jj], value, nr * sizeof(*value));
	    } /* for (i=0; i<nc; i++) */
	  }
	  jj += nr;
	  pr = pr->next;
	} /* while (pr) */
	res = ODBc_unget_data(res);

	/* Keep distinct values (across all cols in rows) only */

	{
	  const int method = 1; /* Changed from 2 to 1 by SS/25-Feb-2014 */
	  int *idx = NULL;
	  double *d = NULL;
	  int i, j, k, ncard = 0;
	  int lda_ncard;
	  int nmem;

	  ALLOC(idx, nrows);
	  codb_cardinality_(&ncols, &nrows, &lda,
			    r->mem, &ncard, idx, &nrows, NULL);

	  lda_ncard = ODBc_lda(ncard, method);
	  nmem = lda_ncard * ncols;
	  ALLOC(d, nmem);

	  for (i=0; i<ncols; i++) {
	    for (j=0; j<ncard; j++) {
	      k = idx[j] - 1; /* "-1" due to C -> Fortran indexing conversion in codb_cardinality_() */
	      d[i*lda_ncard + j] = r->d[i][k];
	    }
	  } /* for (i=0; i<ncols; i++) */

	  FREE(idx);
	  FREE(r->mem);

	  r->mem = d;
	  r->nmem = nmem;
	  lda = r->nra = lda_ncard;
	  r->nrows_in = r->nrows_out = ncard;

	  k = 0;
	  for (i=0; i<ncols; i++) {
	    r->d[i] = &r->mem[k];
	    k += lda;
	  }

	  ODBc_DebugPrintRes("ODBc_get_data_from_binary_file @ before sort", r);
	}

	/* Then sort */

	{
	  const int key = 1;
	  r = ODBc_sort(r, &key, 1);
	  ODBc_DebugPrintRes("ODBc_get_data_from_binary_file @ after sort", r);
	}

	res = r;
      }
    } /* if (fp) */
  }
  DRHOOK_END(0);
  return res;
}


#define SKIPCOL(j) if (skipcol && skipcol[j]) continue

PRIVATE result_t *
GetData(const DB_t *ph, info_t *info, int poolno, 
	int begin_row, int end_row, const Bool *row_wise_preference)
{
  result_t *res = NULL;
  DRHOOK_START(GetData);
  if (ph && info) {
    int nrows = info->idxlen;
    int ncols = info->ncols;

    if ((info->optflags & 0x1) == 0x1) {
      /* A special case : SELECT count(*) FROM table */
      table_t *t = info->t;
      double value = t->nrows;
      Bool row_wise = true; /* doesn't matter ; just one value involved */
      nrows = 1;
      ncols = 1;
      res = ODBc_new_res("GetData => res of count(*)", 
			 __FILE__, __LINE__,
			 nrows, ncols, info, row_wise, poolno);
      res->d[0][0] = value;
      res->nrows_out = nrows;
      res->ncols_out = ncols;
    }
    else if (nrows > 0 && ncols > 0) {
      DEF_IT;
      thsafe_parse_t *thsp = (thsafe_parse_t *)GetTHSP(); /* Wanna change aggr_argno directly !! */
      int i, j, k;
      int errflg = 0;
      int ncols_true = info->ncols_true;
      Bool *skipcol = NULL;
      Bool is_plotobs = ODBc_test_format_1("plotobs");
      Bool warrow = ODBc_test_format_1("wplotobs");
      Bool is_netcdf = ODBc_test_format_1("netcdf");
      Bool has_orderby = (info->o && info->norderby > 0) ? true : false;
      Bool row_wise = (has_orderby || info->has_aggrfuncs || is_netcdf) ? false : 
	(row_wise_preference ? *row_wise_preference : true);
      double *_poolno = getsymaddr(_POOLNO);
      double *_uniqnum = getsymaddr(_UNIQNUM);
      double *_rownum = getsymaddr(_ROWNUM);
      double *_colnum = getsymaddr(_COLNUM);
      double *_nrows = getsymaddr(_NROWS);
      double *_ncols = getsymaddr(_NCOLS);
      int ndyn = info->ndyn;
      int istart, iend;
      int *colmask = NULL;

      if (is_plotobs || warrow) {
	/* When plotting and all columns are non-formulas, then
	   we can load only those columns, which are
	   indicated by $ODB_LAT, $ODB_LON and $ODB_COLOR 
	   
	   But, but ... we MUST also not forget to load the nonassociated columns (kind=0)
	   i.e. those which are possibly used in function call of the primary columns !!
	*/
	int maxcol = 0;
	int cnt = 0;
	ALLOC(skipcol, ncols);
	for (j=0; j<ncols; j++) {
	  col_t *colthis = &info->c[j];
	  skipcol[j] = true;
	  if (colthis->kind == 1 || colthis->kind == 2 || colthis->kind == 4) {
	    /* Note: We accept the same columns as in ODBc_get_info() just before MakePlotColumns() */
	    const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	    if (strequ(s, info->odb_lat->name) ||
		strequ(s, info->odb_lon->name)) {
	      skipcol[j] = false;
	      maxcol = j+1;
	      cnt++;
	    }
	    else if (strequ(s, info->odb_color->name)) {
	      skipcol[j] = false;
	      maxcol = j+1;
	    }
	    else if (warrow && (strequ(s, info->odb_u->name) ||
				strequ(s, info->odb_v->name))) {
	      skipcol[j] = false;
	      maxcol = j+1;
	    }
	  }
	  else if (colthis->kind == 0) {
	    /* Now not forgetting the nonassociated columns */
	    skipcol[j] = false;
	    maxcol = j+1;
	  }
	} /* for (j=0; j<ncols; j++) */

	if (cnt >= 2 && maxcol > 0) {
	  info->ncols_true = ncols_true = maxcol;
	  info->ncols = ncols = maxcol;
	  if (_ncols) *_ncols = maxcol;
	}
	else {
	  FREE(skipcol);
	}
      }

      res = ODBc_new_res("GetData => res", 
			 __FILE__, __LINE__,
			 nrows, ncols, info, row_wise, poolno);

      if (ndyn > 0 && skipcol) {
	/* Recalculate "ndyn", since "ncols" may have changed */
	ndyn = 0;
	for (j=0; j<info->ndyn; j++) {
	  dyn_t *dyn = &info->dyn[j];
	  int colid = dyn->colid;
	  SKIPCOL(colid);
	  ndyn++;
	}
      }

      /* Determine istart & iend */
      istart = -1;
      k = 0;
      for (i=0; i<nrows; i++) {
	int ii = i+1;
	if (end_row >= 1 && ii > end_row) break;
	if (begin_row >= 1 && ii < begin_row) continue;
	if (istart == -1) istart = i;
	k++;
      }
      if (istart == -1) istart = 0;
      iend = istart + k;

      /* Update also _nrows */
      if (_nrows) *_nrows = k;

      if (ndyn > 0) {
	/* Initialize "$<parent_tblname>.<child_tblname>#" -variables */
	int nalloc = iend - istart;
	for (j=0; j<info->ndyn; j++) {
	  dyn_t *dyn = &info->dyn[j];
	  int colid = dyn->colid;
	  col_t *colthis = &info->c[colid];
	  const char *s = colthis->name;
	  SKIPCOL(colid);
	  FREE(dyn->data);
	  CALLOC(dyn->data, nalloc);
	  dyn->ndata = nalloc;
	  colthis->dinp = dyn->data;
	  colthis->dinp_alloc = false;
	  colthis->dsym = putsymvec(s, 0, colthis->dinp, nalloc);
	  if (dyn && dyn->data) {
	    table_t *tp = dyn->parent;
	    table_t *tc = dyn->child;
	    if (tp && tp->idx && tc && tc->idx &&
		tp->info && tp->info == tc->info) {
	      info_t *info = tp->info;
	      (void) DynFill(dyn->data, dyn->ndata,
			     tp->idx, info->idxlen,
			     istart, iend, istart);
	    }
	  } /* if (dyn && dyn->data) */
	} /* for (j=0; j<info->ndyn; j++) */
      }

      CALLOC(colmask, ncols);

      for (j=0; j<ncols; j++) {
	/* Initial sweep over columns to make sure "dsym"'s are defined */
	col_t *colthis = &info->c[j];
	const char *s = colthis->name;
	SKIPCOL(j);
	if (colthis->kind == 0 && IS_POOLNO(s)) {
	  /* This is the current pool number */
	  double *testvalue = getsymaddr(s);
	  /* Initialize, if not already available */
	  if (!testvalue) testvalue = putsym(s, 0);
	  if (testvalue) *testvalue = poolno;
	  colthis->dsym = testvalue;
	  colthis->dinp = NULL;
	  colthis->dinp_alloc = false;
	}
	else if (colthis->kind == 0 && IS_USDDOTHASH(s)) {
	  /* One of the special "$<parent_tblname>.<child_tblname>#" -variables */
	  /* "dsym" should already be initialized ; if not then do it ! */
	  double *testvalue = getsymaddr(s);
	  /* Initialize, if not already available */
	  if (!testvalue) {
	    colthis->dinp = NULL;
	    colthis->dinp_alloc = false;
	    testvalue = putsym(s, 0);
	    if (testvalue) *testvalue = mdi;
	  }
	  colthis->dsym = testvalue;
	  colmask[j] = 1;
	  continue; 
	}
	else if (colthis->kind == 0 && IS_USDHASH(s)) {
	  /* One of the special '$...#' variables (excluding _POOLNO, covered already above) */
	  double *testvalue = getsymaddr(s);
	  /* Initialize, if not already available */
	  if (!testvalue) testvalue = putsym(s, 0);
	  if      (IS_(UNIQNUM,s)) _uniqnum = testvalue;
	  else if (IS_(ROWNUM,s))  _rownum = testvalue;
	  else if (IS_(COLNUM,s))  _colnum = testvalue;
	  else if (IS_(NROWS,s))   _nrows = testvalue;
	  else if (IS_(NCOLS,s))   _ncols = testvalue;
	  colthis->dsym = testvalue;
	  colthis->dinp = NULL;
	  colthis->dinp_alloc = false;
	}
	else if (colthis->kind == 0 && IS_DOLLAR(s)) {
	  /* This is a $-variable */
	  double *testvalue = getsymaddr(s);
	  /* Initialize, if not already available (should be available) */
	  if (!testvalue) testvalue = putsym(s, mdi);
	  colthis->dsym = testvalue;
	  colthis->dinp = NULL;
	  colthis->dinp_alloc = false;
	}
	else if (colthis->kind == 0 && IS_HASH(s)) {
	  /* This is '#table_name' -variable */
	  double *testvalue = getsymaddr(s);
	  table_t *tc = colthis->t ? colthis->t : GetTable(info, s);
	  if (!tc) {
	    fprintf(stderr,"***Error: Unable to locate table for #-variable '%s'\n",s);
	    errflg++;
	  }
	  else {
	    colthis->t = tc;
	  }
	  /* Initialize, if not already available */
	  if (!testvalue) testvalue = putsym(s, 0);
	  colthis->dsym = testvalue;
	  colthis->dinp = NULL;
	  colthis->dinp_alloc = false;
	  colmask[j] = 2;
	}
	else if (colthis->kind == 0 || colthis->kind == 1) {
	  double *testvalue = getsymaddr(s);
	  table_t *tc = colthis->t ? colthis->t : GetTable(info, s);

	  if (!tc) {
	    fprintf(stderr,"***Error: Unable to locate table for column '%s'\n",s);
	    errflg++;
	  }
	  else {
	    colthis->t = tc;
	  }

	  if (testvalue) {
	    colthis->dinp = getsymvec(s, &colthis->dinp_len);
	    colthis->dinp_alloc = false;
	    colthis->dsym = testvalue;
	  }
	  else {
	    int Nrows;
	    colthis->dinp = DCA_fetch_double(ph->h, ph->dbname, tc->name, colthis->fetch_name,
					     poolno, NULL, 0, &Nrows);
	    if (Nrows != tc->nrows) {
	      COLUMN_FETCH_ERROR();
	      errflg++;
	    }
	    else {
	      Bool thesame = strequ(s,colthis->fetch_name) ? true : false;
	      ODB_fprintf(ODBc_get_debug_fp(),
			  "  %s:%d: Successfully fetched (double) %s%s%s (Nrows=%d/%d) : poolno#%d\n",
			  __FILE__, __LINE__,
			  thesame ? s : colthis->fetch_name,
			  thesame ? "" : " alias ",
			  thesame ? (const char *)"" : s,
			  Nrows, tc->nrows, poolno);
	      colthis->dinp_len = Nrows;
	      colthis->dinp_alloc = true;
	      if (colthis->dtnum == DATATYPE_INT4 && 
		  colthis->bitpos >= 0 && colthis->bitpos <  MAXBITS &&
		  colthis->bitlen >= 1 && colthis->bitlen <= MAXBITS) {
		ODBc_vget_bits(colthis->dinp, colthis->dinp_len, colthis->bitpos, colthis->bitlen);
	      }
	      colthis->dsym = putsymvec(s, mdi, colthis->dinp, Nrows);
	    }
	  }
	  colmask[j] = 3;
	}
      } /* for (j=0; j<ncols; j++) */

      if (errflg) RAISE(SIGABRT);

      k = 0;
      for (i=istart; i<iend; i++) {
	double *dk = row_wise ? &res->d[k][0] : NULL;

	for (j=0; j<ncols; j++) { 
	  if (colmask[j] > 0) {
	    /* 1st sweep : assign values to symbol table entries */
	    col_t *colthis = &info->c[j];
	    const char *s = colthis->name;
	    if (colmask[j] == 1) {
	      /* One of the special "$<parent_tblname>.<child_tblname>#" -variables */
	      *colthis->dsym = colthis->dinp ? colthis->dinp[i-istart] : mdi;
	    }
	    else if (colmask[j] == 2) {
	      /* This is a '#table_name' -variable */
	      table_t *tc = colthis->t;
	      int jr = tc->idx[i];
	      *colthis->dsym = jr + 1;
	    }
	    else if (colmask[j] == 3) {
	      /* As if (colthis->kind == 0 || colthis->kind == 1) */
	      table_t *tc = colthis->t;
	      int jr = tc->idx[i];
	      if (colthis->dinp && jr >= 0 && jr < tc->nrows) {
		/* Extra safeguarding */
		*colthis->dsym = colthis->dinp[jr];
	      }
	      else {
		*colthis->dsym = mdi;
	      }
	    }
	  } /* if (colmask[j] > 0) */
	} /* for (j=0; j<ncols; j++) */

	if (_rownum) *_rownum = k+1;

	if (_uniqnum) *_uniqnum = ODB_put_one_control_word(k, poolno);

	if (info->has_aggrfuncs) {
	  /* Note : Aggr_argnO is a macro #define'd in include/evaluate.h !! */
	  Aggr_argnO = 1; /* Uses *thsp ;
			     The value "1" means: we return the 1st arg from a multi-argument
			     aggregate function call (like corr(x,y)), and set this to "2", when 
			     j >= ncols_true, after which we return the 2nd arg ;
			     See include/funcs.h and also include/evaluate.h
			  */
	}

	for (j=0; j<ncols; j++) {
	  /* 2nd sweep : evaluate & store data */
	  col_t *colthis = &info->c[j];
	  const char *s = colthis->name;
	  double value = mdi;
	  SKIPCOL(j);	  
	  if (_colnum) *_colnum = j+1; /* Live update !! */

	  if (colthis->kind == 1) {
	    value = *colthis->dsym;
	  }
	  else if (colthis->kind == 2) { /* Standard formulas */
	    int iret = 0;
	    value = RunTree(colthis->formula_ptree, NULL, &iret);
	    if (iret != 0) {
	      fprintf(stderr,
		      "***Error: Unable to evaluate the column#%d expression '%s'\n",
		      j+1,s);
	      RAISE(SIGABRT);
	    }
	  }
	  else if (colthis->kind == 4 || colthis->kind == 8) { /* Aggregate functions */
	    int iret = 0;
	    Aggr_argnO = (colthis->kind == 4) ? 1 : 2;
	    value = RunTree(colthis->formula_ptree, NULL, &iret);
	    if (iret != 0) {
	      fprintf(stderr,
		      "***Error: Unable to evaluate the aggregate column#%d expression '%s'\n",
		      j+1,s);
	      RAISE(SIGABRT);
	    }
	  }
	  if (row_wise) dk[j] = value; else res->d[j][k] = value;
	} /* for (j=0; j<ncols; j++) */

	if (info->has_aggrfuncs) Aggr_argnO = 1;

	++k;
      } /* for (i=istart; i<iend; i++) */

      res->nrows_out = k;
      res->ncols_out = ncols_true;

      /* Clean-up */

      FREE(skipcol);
      FREE(colmask);

      for (j=0; j<info->ndyn; j++) {
	dyn_t *dyn = &info->dyn[j];
	FREE(dyn->data);
	dyn->ndata = 0;
      } /* for (j=0; j<info->ndyn; j++) */

    } /* if (nrows > 0 && ncols > 0) */
  } /* if (ph && info) */
#if 0
  fprintf(stderr,"res follows ... (at end of GetData)\n");
  ODBc_print_data(NULL, stderr, res, 
		  "odb", "-",
		  0, 1, 0,
		  NULL);
#endif
  DRHOOK_END(0);
  return res;
}


PUBLIC void *
ODBc_get_data(int handle, void *Result, void *Info, int poolno, 
	      int begin_row, int end_row, 
	      Bool do_sort, const Bool *row_wise_preference)
{
  result_t *res = Result;
  info_t *info = Info;
  DRHOOK_START(ODBc_get_data);
  if (info) {
    int maxhandle = 0;
    DB_t *free_handles = ODBc_get_free_handles(&maxhandle);
    if (free_handles && handle >= 1 && handle <= maxhandle) {
      DB_t *ph = &free_handles[handle-1];
      if (ph->h == handle) {
	int npools = ph->npools;
	if (poolno >= 1 && poolno <= npools) {
	  Bool poolno_okay = ODB_in_permanent_poolmask(handle, poolno) ? true : false;
	  if (poolno_okay) { /* Finally !!! */
	    result_t *p_res = res;
	    result_t *thisres = GetData(ph, info, poolno, begin_row, end_row, row_wise_preference);
	    if (thisres) {
	      if (info->has_aggrfuncs) {
		/* Perform phase#0-processing for aggregate functions */
		thisres = ODBc_aggr(thisres, 0);
	      }
	      else if (do_sort && info->norderby > 0) {
		thisres = ODBc_sort(thisres, NULL, 0);
	      }
	      if (p_res) {
		/* Add to the chain */
		while (p_res) {
		  if (p_res->next) {
		    p_res = p_res->next;
		  }
		  else {
		    p_res->next = thisres;
		    break;
		  }
		} /* while (p_res) */
	      }
	      else {
		res = thisres;
	      }
	    } /* if (thisres) */
	  } /* if (poolno_okay) */
	} /* if (poolno >= 1 && poolno <= npools) */
      } /* if (ph->h == handle) */
    } /* if (free_handles && handle >= 1 && handle <= maxhandle) */
  } /* if (info) */
  DRHOOK_END(0);
  return res;
}


PUBLIC void *
ODBc_unget_data(void *Result)
{
  DRHOOK_START(ODBc_unget_data);
  if (Result) {
    FILE *fp_echo = ODBc_get_debug_fp();
    int cnt = 0;
    result_t *res = Result;
    while (res) {
      result_t *nextres = res->next;
      if (res->mem) FREE(res->mem);
      if (res->d) FREE(res->d);
      if (res->backup_file) FREE(res->backup_file);
      ODB_fprintf(fp_echo,"ODBc_unget_data: releasing res @ %p [#%d] : %d %d-byte words\n", 
		  res, ++cnt, res->nmem, sizeof(*res->mem));
      FREE(res);
      res = nextres;
    } /* while (res) */
  }
  DRHOOK_END(0);
  return NULL;
}


PUBLIC void *
ODBc_remove_duplicates(void *Result, const int keys[], int n_keys)
{
  result_t *res = Result;
  FILE *fp_echo = ODBc_get_debug_fp();
  DRHOOK_START(ODBc_remove_duplicates);
  ODB_fprintf(fp_echo, "ODBc_remove_duplicates(): res @ %p, "
	      "res->nrows_out = %d, keys @ %p, n_keys = %d\n",
	      res, res ? res->nrows_out : -1,
	      keys, n_keys);
#if 0
  fprintf(stderr,"n_keys = %d\n",n_keys);
  fprintf(stderr,"res follows ... (i.e. before sort)\n");
  ODBc_print_data(NULL, stderr, res, 
		  "odb", "-",
		  0, 1, 0,
		  NULL);
#endif
  if (res && res->nrows_out > 0 && keys && n_keys > 0) {
    /* For now : no checks on keys[]-values */
    info_t *info = res->info;
    result_t *r1 = ODBc_sort(res, keys, n_keys); /* result set NOT row_wise */
    int nrows = r1->nrows_out;
    int ncols = r1->ncols_out;
    int poolno = r1->poolno;
    Bool row_wise = true;
    result_t *r2 = ODBc_new_res("ODBc_remove_duplicates => r2", 
				__FILE__, __LINE__,
				nrows, ncols, info, row_wise, poolno);
    int i, j;
    int ngrp = 0;
    int jj = 0;

#if 0
    fprintf(stderr,"r1 follows ... (i.e. after sort)\n");
    ODBc_print_data(NULL, stderr, r1, 
		    "odb", "-",
		    0, 1, 0,
		    NULL);
#endif

    for (i=0; i<nrows; i++) {
      Bool changed = (i > 0) ? false : true;
      
      for (j=0; j<n_keys && !changed; j++) {
	int akey = keys[j]-1;
	double djj = r1->d[akey][jj]; /* this is NOT row_wise */ 
	double di  = r1->d[akey][i]; /* this is NOT row_wise */
	if (djj != di) changed = true;
      } /* for (j=0; j<n_keys; j++) */
	  
      if (changed) {
	jj = i;
	for (j=0; j<ncols; j++) {
	  const double *din = &r1->d[j][jj]; /* this is NOT row_wise */
	  double *dout = &r2->d[ngrp][j]; /* this is indeed row_wise */
	  *dout = *din;
	}
	ngrp++;
      }
    } /* for (i=0; i<nrows; i++) */

    r2->nrows_out = ngrp;
    (void) ODBc_unget_data(r1);
    res = r2;
  }
  DRHOOK_END(0);
  return res;
}


PUBLIC int
ODBc_conv2degrees(void *Res, Bool to_degrees)
{
  int converted = 0;
  result_t *res = Res;
  info_t *info = res ? res->info : NULL;
  DRHOOK_START(ODBc_conv2degrees);
  if (res && info && info->c &&
      info->odb_lat && info->odb_lon) {
    int j;
    int nrows = res->nrows_out;
    int ncols = res->ncols_out;
    Bool row_wise = res->row_wise;
    Bool *do_this_col = NULL;
    ALLOCX(do_this_col,ncols);
    for (j=0; j<ncols; j++) {
      col_t *colthis = &info->c[j];
      do_this_col[j] = false;
      if (colthis->kind == 1 || colthis->kind == 2 || colthis->kind == 4) {
	const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	if (strequ(s, info->odb_lat->name) || strequ(s, info->odb_lon->name)) {
	  do_this_col[j] = true;
	  converted++;
	}
      }
    } /* for (j=0; j<ncols; j++) */

    if (converted == 2) {
      /* Make sure the conversion is needed by looking at the
	 prevailing latlon_rad -value that will be used in lldegrees/llradians -funcs */
      /* -- See lib/funcs.c for more:
	 latlon_rad =  1 --> values were originally in radians
	 latlon_rad =  0 --> values were originally in degrees
	   [Note: we don't allow to going back to radians, if the original was in degrees!!]
	 latlon_rad = -1 --> don't know what they were [shouldn't happen]
	 latlon_rad = -2 --> latlon_rad wasn't initialized [shouldn't happen either]
      */
      int latlon_rad = codb_change_latlon_rad_(NULL);
      if (latlon_rad == 0) {
	/* Already in degrees --> conversion to degrees abandoned */
	/* Also "back"-conversion to radians ignored; see the note above */
	converted = 0;
      }
      /* So we convert to degrees, if latlon_rad != 0, and
	 we "back"-convert to radians,  if latlon_rad != 0. Finally correct ? */
    }

    if (converted == 2) {
      for (j=0; j<ncols; j++) {
	if (do_this_col[j]) {
	  int i;
	  for (i=0; i<nrows; i++) {
	    double value = row_wise ? res->d[i][j] : res->d[j][i];
	    value = to_degrees ? ODB_lldegrees(value) : ODB_llradians(value);
	    if (row_wise)  res->d[i][j] = value; else res->d[j][i] = value;
	  } /* for (i=0; i<nrows; i++) */
	} /* if (do_this_col[j]) */
      } /* for (j=0; j<ncols; j++) */
    } /* if (converted) */
    FREEX(do_this_col);
  }
  DRHOOK_END(0);
  return (converted == 2) ? 1 : 0;
}


PRIVATE char *
GetColArrayBaseName(const col_t *col)
{
  /* Get the "column array" basename e.g. for satname[1:4] = {satname_1,satname_2,satname_3,satname_4} 
     it will be "satname_" as it would for satname[-2:1] = { satname__2,satname__1,satname_0,satname_1}.
     This infor is used when "joinstr" condition is on to compare consecutive
     common base prefixes (like "satname_" followed by a number) */

  char *basename = NULL;
  if (col) {
    char *last_uscore = NULL;
    const char *nick = col->nickname ? col->nickname : col->name;
    char *thisnick = STRDUP(nick);
    char *first_at = strchr(thisnick, '@');

    if (first_at) *first_at = '\0';
    last_uscore = strrchr(thisnick,'_');
    
    if (last_uscore) {
      /* Check that the rest of the string, after last '_', is digital */
      int is_digital = 1;
      char *x = last_uscore;
      while (is_digital && *++x) is_digital = isdigit(*x);
      if (!is_digital) last_uscore = NULL;
    }
    
    if (last_uscore) {
      /* May not be yet exactly right; f.ex. x[-1:+1] = {x__1, x_0, x_1} --> make it right */
      if (last_uscore != thisnick && last_uscore[-1] == '_') --last_uscore;
      last_uscore[1] = '\0';
      basename = STRDUP(thisnick);
    }

    FREE(thisnick);
  } /* if (col) */
  return basename;
}

#define PRINT_STRING(prt_func, v) \
{ \
  char cc[sizeof(double)+1]; \
  char *scc = cc; \
  union { \
    char s[sizeof(double)]; \
    double d; \
  } u; \
  u.d = v; \
  for (js=0; js<sizeof(double); js++) { \
    char c = u.s[js]; \
    *scc++ = isprint(c) ? c : (is_odb ? '?' : ' '); \
  } /* for (js=0; js<sizeof(double); js++) */ \
  *scc = '\0'; \
  prt_func(fp,"%s",cc); \
}

#if 0
: 2 1 2524
: VIEW="myview" on 20061107 at 114223
: Pool#1: no. of rows x cols = 2524 x 2
:                   lat                   lon
:                  @hdr                  @hdr
:                  ====                  ====
#endif

PRIVATE void *
PrintData(const char *filename, FILE *fp, void *Result, 
	  const char *format, const char *fmt_string, 
	  int konvert, int write_title, int joinstr,
	  int the_first_print, int the_final_print, 
	  int *iret)
{
  result_t *res = Result;
  info_t *info = res ? res->info : NULL;
  int rc = 0;
  DRHOOK_START(PrintData);
  if (fp && info && res && res->mem && res->d) {
    FILE *fp_echo = ODBc_get_debug_fp();
    int i, j;
    char *env;
    Bool is_default = (!format || strequ(format,"default")) ? true : false;
    Bool is_dump    = strequ(format,"dump") ? true : false;
    Bool is_bindump = strequ(format,"bindump") ? true : false;
    Bool is_binary  = ODBc_test_format_2(format,"binary");
    Bool is_odbtk   = ODBc_test_format_2(format,"odbtk");
    Bool is_odb     = ODBc_test_format_2(format,"odb");
    Bool is_odbtool = ODBc_test_format_2(format,"odbtool");
    Bool is_plotobs = ODBc_test_format_2(format,"plotobs");
    Bool warrow = ODBc_test_format_2(format,"wplotobs");
    Bool is_netcdf = ODBc_test_format_2(format,"netcdf");
    int geofmt = GeoFormat();
    Bool print_mdi = true;
    const char *delim = (is_odbtk ? "!,!" : (is_dump ? "," : " "));
    const char *Delim = is_dump ? (const char *)"" : delim; /* The actual delimiter used */
    int poolno = res->poolno;
    int nrows = res->nrows_out;
    int ncols = res->ncols_out;
    int row_wise = res->row_wise ? 1 : 0;
    int resdate, restime;
    int *width = NULL;
    int ndigits = log10(MAX(1,info->npools)) + 1; /* ~ digits in "npools" */

    env = getenv("ODB_PRINT_MDI");
    if (env) {
      int value = atoi(env);
      print_mdi = (value != 0) ? true : false;
    }

    (void) odb_datetime_(&resdate, &restime);

    if (is_dump) is_default = true;
    if (is_bindump) is_binary = true;

    if (konvert) konvert = (is_plotobs || warrow || is_odbtool || geofmt || is_netcdf) ? 
		   0 : ODBc_conv2degrees(res, true);
    if (joinstr && !is_default) joinstr = 0;

    if (is_odbtool) {
      (void) ODBc_ODBtool(fp, NULL, NULL, false, res, info);
      goto finish;
    }
    else if (is_netcdf) {
      double *d = res->mem;
      int nra = res->nra;
      uint *dtnum = NULL;
      int len_ODB_tag_delim = STRLEN(ODB_tag_delim); /* ODB_tag_delim from "privpub.h" */
      char *odb_type_tag = NULL; int len_odb_type_tag = len_ODB_tag_delim + 1;
      char *odb_name_tag = NULL; int len_odb_name_tag = len_ODB_tag_delim + 1;
      char *odb_nickname_tag = NULL; int len_odb_nickname_tag = len_ODB_tag_delim + 1;

      konvert = ODBc_conv2degrees(res, true); /* Convert to degrees, if applicable */

      ODB_fprintf(fp_echo,
		  "is_netcdf: row_wise=%d, nra=%d, nrows=%d, ncols=%d, poolno=%d, "
		  "nrows_in=%d, ncols_in=%d, res->next=%p, konvert=%d\n",
		  row_wise, nra, nrows, ncols, poolno,
		  res->nrows_in, res->nrows_out, res->next, konvert);

      CALLOC(dtnum, ncols);
      
      for (j=0; j<ncols; j++) {
	col_t *colthis = &info->c[j];
	const char *type = colthis->dtype;
	const char *name = colthis->name;
	const char *nickname = colthis->nickname ? colthis->nickname : colthis->name;
	len_odb_type_tag += STRLEN(type) + len_ODB_tag_delim;
	len_odb_name_tag += STRLEN(name) + len_ODB_tag_delim;
	len_odb_nickname_tag += STRLEN(nickname) + len_ODB_tag_delim;
	dtnum[j] = colthis->dtnum;
      }

      ALLOCX(odb_type_tag, len_odb_type_tag); strcpy(odb_type_tag,ODB_tag_delim);
      ALLOCX(odb_name_tag, len_odb_name_tag); strcpy(odb_name_tag,ODB_tag_delim);
      ALLOCX(odb_nickname_tag, len_odb_nickname_tag); strcpy(odb_nickname_tag,ODB_tag_delim);

      for (j=0; j<ncols; j++) {
	col_t *colthis = &info->c[j];
	const char *type = colthis->dtype;
	const char *name = colthis->name;
	const char *nickname = colthis->nickname ? colthis->nickname : colthis->name;
	strcat(odb_type_tag,type); strcat(odb_type_tag,ODB_tag_delim);
	strcat(odb_name_tag,name); strcat(odb_name_tag,ODB_tag_delim);
	strcat(odb_nickname_tag,nickname); strcat(odb_nickname_tag,ODB_tag_delim);
      }

      {
	const char *sql_query = ODBc_get_sql_query(info);
	const char search_str[] = "The original query follows\n\n";
	const char *matched = strstr(sql_query, search_str);
	const char title_fmt[] = "Database schema in %s/%s.sch ; Viewname=%s ; pool#%d";
	char *title = NULL;
	const char ncfile_fmt[] = "%s.nc";
	char *ncfile = NULL;
	char *viewname = NULL;
	const char namecfg[] = "/dev/null";
	int len;

	if (filename) {
	  char *pdot;
	  char *pslash = strrchr(filename,'/');
	  viewname = pslash ? STRDUP(pslash+1) : STRDUP(filename);
	  pdot = strchr(viewname, '.');
	  if (pdot) *pdot = '\0';
	  ncfile = STRDUP(filename);
	}
	else {
	  viewname = STRDUP(info->view);
	  len = STRLEN(ncfile_fmt) + STRLEN(viewname) + 20;
	  ALLOC(ncfile, len);
	  snprintf(ncfile, len, ncfile_fmt, viewname);
	}

	len = STRLEN(title_fmt) + 
	  STRLEN(info->dbcred.srcpath) + 
	  STRLEN(info->dbcred.dbname) + 
	  STRLEN(viewname) + 50;
	ALLOCX(title, len);
	snprintf(title, len, title_fmt, 
		 info->dbcred.srcpath ? info->dbcred.srcpath : "???",
		 info->dbcred.dbname ? info->dbcred.dbname : "???",
		 viewname,
		 poolno);

	if (matched) sql_query = matched + STRLEN(search_str);

	codb2netcdf_(title, ncfile, namecfg, sql_query,
		     d, &nra, &nrows, &ncols,
		     dtnum, &poolno,
		     odb_type_tag, odb_name_tag, odb_nickname_tag
		     /* Hidden arguments */
		     , STRLEN(title)
		     , STRLEN(ncfile)
		     , STRLEN(namecfg)
		     , STRLEN(sql_query)
		     , STRLEN(odb_type_tag)
		     , STRLEN(odb_name_tag)
		     , STRLEN(odb_nickname_tag)
		     );
	
	FREEX(title);
	FREE(viewname);
	FREE(ncfile);
      }

      FREEX(odb_nickname_tag);
      FREEX(odb_name_tag);
      FREEX(odb_type_tag);

      FREE(dtnum);
      goto finish;
    }
    else if (is_binary) {
      int yyyymmdd;
      int hhmmss; /* analysis date & time */
      codb_analysis_datetime_(&yyyymmdd, &hhmmss);

      if (is_bindump) {
	ll_t nbytes = (ll_t) nrows * ncols * sizeof(double);
	extern int ec_is_little_endian();
	int i_am_little = ec_is_little_endian();

	char *pq, *query = NULL;
	const char *sql_query = info->sql_query;
	const char search_str[] = "The original query follows\n\n";
	const char *matched = strstr(sql_query, search_str);
	if (matched) sql_query = matched + STRLEN(search_str);

	query = STRDUP(sql_query);
	pq = query - 1;
	while (*++pq) {
	  if (isspace(*pq) || !isprint(*pq)) *pq = ' ';
	}

	/* -f bindump dumps the title-structure (text-format) even if it is not requested */

	if (the_first_print) {
	  int jj = 0;
	  fprintf(fp,"ODB binary data dump created on %8.8d at %6.6d\n", resdate, restime);
	  fprintf(fp,"\ntitle = {\n");
	  fprintf(fp," // column_number %s type_name %s column_name %s table_name %s\n",
		  ODB_tag_delim, ODB_tag_delim, ODB_tag_delim, ODB_tag_delim);
	  for (j=0; j<ncols; j++) {
	    col_t *colthis = &info->c[j];
	    if (colthis->kind == 1 || colthis->kind == 2 || colthis->kind == 4) {
	      const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	      const table_t *tc = colthis->t;
	      const char *st = (colthis->kind == 1) ? tc->name : "Formula";
	      const char *dtype = colthis->dtype;
	      fprintf(fp, "\t%d %s %s %s %s %s %s %s\n",
		      ++jj, ODB_tag_delim,
		      dtype, ODB_tag_delim,
		      s, ODB_tag_delim,
		      st, ODB_tag_delim);
	    }
	  } /* for (j=0; j<ncols; j++) */
	  fprintf(fp,"};\n");
	}

	fprintf(fp,"\nmetadata = {\n");
	fprintf(fp,"\tpool_number = %d\n", poolno);
	fprintf(fp,"\tnumber_of_rows = %d\n", nrows);
	fprintf(fp,"\tnumber_of_columns = %d\n", ncols);
	fprintf(fp,"\tnumber_of_data_bytes = %lld\n", nbytes);
	fprintf(fp,"\tdata_endianess = %s_endian\n",i_am_little ? "little" : "big");

	if (the_first_print) {
	  fprintf(fp,"\tanalysis_date = %8.8d\n", yyyymmdd);
	  fprintf(fp,"\tanalysis_time = %6.6d\n", hhmmss);
	  fprintf(fp,"\tdata_query = %s\n", query); FREE(query);
	  fprintf(fp,"\tdatabase = %s\n",info->dbcred.dbname);
	  if (info->dbcred.srcpath)  fprintf(fp,"\tdatabase_srcpath = %s\n", info->dbcred.srcpath);
	  if (info->dbcred.datapath) fprintf(fp,"\tdatabase_datapath = %s\n", info->dbcred.datapath);
	  if (info->dbcred.idxpath)  fprintf(fp,"\tdatabase_idxpath = %s\n", info->dbcred.idxpath);
	  fprintf(fp,"\tpoolmask = %s\n", info->dbcred.poolmask ? info->dbcred.poolmask : "-1");
	  fprintf(fp,"\tlatlon_output_in_degrees = %s\n", 
		  (info->latlon_rad == 0 || konvert) ? 
		  "yes" : ((info->latlon_rad == 1) ? "no" : "unknown"));
	} /* if (the_first_print) */

	fprintf(fp,"};\n");

	/* Note: 
	   The data section for this pool starts after the form-feed character ('\f') has been found 
	   and it will be nrows x ncols x sizeof(double) -bytes long 
	 */

	fprintf(fp,fp_echo ? "\n" : "\f");

	/* bindump also is always row_wise (even is res->row_wise indicates otherwise) */
	if (!row_wise) { /* Was column-wise (Fortran) */
	  /* Note: we enforce row-wise output */
	  result_t *newres = ODBc_merge_res("PrintData [is_bindump] => newres",
					    __FILE__, __LINE__,
					    res, ncols, info, true);
	  res = newres;
	  row_wise = true;
	}

	/* Now always row-wise (C) */

	if (fp_echo) {
	  /* Note: when debugging output is on, the data will be printed in text-mode */
	  fprintf(fp, 
		  "<The following %d lines (%d columns) are normally printed in a binary %s-endian mode"
		  " and would contain %lld bytes of data>\n",
		  nrows, ncols,
		  i_am_little ? "little" : "big",
		  nbytes);
	  for (i=0; i<nrows; i++) {
	    const double *value = &res->d[i][0];
	    for (j=0; j<ncols; j++) {
	      col_t *colthis = &info->c[j];
	      if (print_mdi && colthis->dtnum != DATATYPE_STRING && ABS(value[j]) == mdi) {
		fprintf(fp, "NULL");
	      }
	      else {
		switch (colthis->dtnum) {
		case DATATYPE_STRING:
		  {
		    int js;
		    fprintf(fp, "'");
		    PRINT_STRING(fprintf, value[j]);
		    fprintf(fp, "'");
		  }
		  break;
		case DATATYPE_YYYYMMDD:
		  fprintf(fp, "%8.8d", (int)value[j]);
		  break;
		case DATATYPE_HHMMSS:
		  fprintf(fp, "%6.6d", (int)value[j]);
		  break;
		default:
		  fprintf(fp, "%.14g", value[j]);
		  break;
		}
	      }
	      fprintf(fp, "%c", (j < ncols-1) ? ',' : '\n');
	    } /* for (j=0; j<ncols; j++) */
	  } /* for (i=0; i<nrows; i++) */
	}
	else {
	  for (i=0; i<nrows; i++) {
	    const double *value = &res->d[i][0];
	    (void) fwrite(value, sizeof(*value), ncols, fp);
	  } /* for (i=0; i<nrows; i++) */
	}
      }
      else { /* !is_bindump */
	const int nhdr = 13;
	const int nversion = 1;
	const int double_precision = 1; /* 1 = values in double precision , 0 = in float, single prec. */
	int *hdr = NULL;
	ALLOCX(hdr, nhdr);
	hdr[0] = ODBX;
	hdr[1] = nhdr;
	hdr[2] = nversion;
	hdr[3] = resdate;
	hdr[4] = restime;
	hdr[5] = poolno;
	hdr[6] = nrows;
	hdr[7] = ncols;
	hdr[8] = row_wise;
	hdr[9] = double_precision;
	hdr[10] = konvert;
	hdr[11] = yyyymmdd;
	hdr[12] = hhmmss;
	(void) fwrite(hdr, sizeof(*hdr), 2, fp);
	(void) fwrite(hdr+2, sizeof(*hdr), nhdr-2, fp);
	if (row_wise) { /* Row-wise (C) */
	  for (i=0; i<nrows; i++) {
	    const double *value = &res->d[i][0];
	    (void) fwrite(value, sizeof(*value), ncols, fp);
	  } /* for (i=0; i<nrows; i++) */
	}
	else { /* Column-wise (Fortran) */
	  for (i=0; i<ncols; i++) {
	    const double *value = &res->d[i][0];
	    (void) fwrite(value, sizeof(*value), nrows, fp);
	  } /* for (i=0; i<ncols; i++) */
	}
	FREEX(hdr);
      }
      goto finish;
    }
    else if (is_plotobs || warrow) {
      /* Write for plotobs.x -executable */
      /* This will re-allocate the "res" */
      const int iswp1234 = 1234;
      float scale[6] = { 0 };
      float value_min = 0, value_max = 0;
      int iscale = 1;
      int nobs, nobs_actual;
      int latcol, loncol, colorcol;
      int ucol, vcol;
      int k, targetcols[5];
      int nt;
      Bool pseudo_color = false;
      const char *prefix = warrow ? "warrow" : "is_plotobs";
      char text1[80+1],text2[80+1],text3[80+1];
      char user_text[256+1];
      Bool cmap_ok = ODBc_get_kolor_map(&value_min, &value_max, scale, text3, sizeof(text3));
      
      {
	char *dbname = STRDUP(info->dbcred.dbname);
	char *last_slash = strrchr(dbname,'/');
	char *last_dot = strrchr(dbname,'/');
	char *p = NULL;
	int len;
	if (last_slash) {
	  *last_slash = '\0';
	  p = ++last_slash;
	}
	if (last_dot) *last_dot = '\0';
	if (p) {
	  FREE(dbname);
	  dbname = STRDUP(p);
	}
	snprintf(text1,80,"ODB database : %s       Query: %s", dbname, info->view);
	len = STRLEN(text1);
	memset(text1+len,' ',80-len);
	text1[80] = '\0';
	FREE(dbname);
      }
      
      memset(text2,' ',80); text2[80] = '\0'; /* Filled/used by Plotobs.F90 */
      if (cmap_ok) {
	iscale = (int)scale[0];
      }
      else {
	memset(text3,' ',80);
      }
      text3[80] = '\0';

      latcol = -1;
      loncol = -1;
      colorcol = -1;
      ucol = vcol = -1;
      for (j=0; j<ncols; j++) {
	col_t *colthis = &info->c[j];
	if (colthis->kind == 1 || colthis->kind == 2 || colthis->kind == 4) {
	  const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	  if (strequ(s, info->odb_lat->name)) latcol = j;
	  else if (strequ(s, info->odb_lon->name)) loncol = j;
	  else if (strequ(s, info->odb_color->name)) colorcol = j;
	  else if (warrow && strequ(s, info->odb_u->name)) ucol = j;
	  else if (warrow && strequ(s, info->odb_v->name)) vcol = j;
	}
      }

      if (warrow && (ucol == -1 || vcol == -1)) {
	ucol = vcol = -1;
	warrow = false;
	ODBc_set_format("plotobs");
      }

      targetcols[0] = latcol;
      targetcols[1] = loncol;
      if (warrow) {
	targetcols[2] = ucol;
	targetcols[3] = vcol;
	targetcols[4] = colorcol;
	nt = (colorcol == -1) ? 4 : 5;
	if (nt == 4) pseudo_color = true; /* Calculate & put wind-speed to the 5th column */
      }
      else {
	targetcols[2] = colorcol;
	targetcols[3] = -1;
	targetcols[4] = -1;
	nt = 3;
      }

      ODB_fprintf(fp_echo,"%s: poolno=%d, nrows=%d, ncols=%d\n", 
		  prefix, poolno, nrows, ncols);
      ODB_fprintf(fp_echo,"%s: latcol=%d, loncol=%d, colorcol=%d, ucol=%d, vcol=%d\n", 
		  prefix, latcol, loncol, colorcol, ucol, vcol);

      if (latcol == -1 || loncol == -1) {
	nobs = 0;
      }
      else {
	konvert = ODBc_conv2degrees(res, true); /* Convert to degrees, if applicable */
	if (!info->has_select_distinct) {
	  /* Remove duplicate (lat,lon)'s -- basically per pool to keep it cheap */
	  const int n_keys = 2;
	  int keys[2];
	  keys[0] = latcol + 1;
	  keys[1] = loncol + 1;
	  res = ODBc_remove_duplicates(res, keys, n_keys);
	}
	nobs = res->nrows_out;
      }

      fwrite(&iswp1234, sizeof(iswp1234), 1, fp);

      res->nrows_out = nobs; /* this many useful rows left !! */
      fwrite(&nobs, sizeof(int), 1, fp);
      nobs_actual = nrows;
      fwrite(&nobs_actual, sizeof(int), 1, fp);

      ODB_fprintf(fp_echo,"%s: nobs=%d, nobs_actual=%d\n",
		  prefix,nobs,nobs_actual);

      fwrite(text1, sizeof(char), 80, fp);
      fwrite(text2, sizeof(char), 80, fp);
      fwrite(text3, sizeof(char), 80, fp);

      fwrite(&value_min, sizeof(float), 1, fp);
      fwrite(&value_max, sizeof(float), 1, fp);
      fwrite(&iscale, sizeof(int), 1, fp);
      fwrite(&scale[1], sizeof(float), 5, fp);

      memset(user_text, ' ', 256);
      if (info->dbcred.dbname) {
	int len = STRLEN(info->dbcred.dbname);
	len = MIN(len, 256);
	strncpy(user_text, info->dbcred.dbname, len);
      }
      else {
	getcwd(user_text,sizeof(user_text));
      }
      user_text[256] = '\0';
      fwrite(user_text, sizeof(char), 256, fp);

      if (nobs > 0) {
	float *f = NULL;
	ALLOC(f, nobs);
	for (k=0; k<nt; k++) {
	  j = targetcols[k];
	  ODB_fprintf(fp_echo,"%s: targetcols[%d] = %d (j=%d)\n",prefix,k,targetcols[k],j);
	  if (j >= 0 && j<ncols) {
	    for (i=0; i<nobs; i++) {
	      double value = res->row_wise ? res->d[i][j] : res->d[j][i];
	      f[i] = (float)value; /* May potentially cause a SIGFPE */
	    } /* for (i=0; i<nobs; i++) */
	  }
	  else {
	    memset(f, 0, nobs*sizeof(*f));
	  }
	  {
	    int nelem = fwrite(f, sizeof(*f), nobs, fp);
	    ODB_fprintf(fp_echo,"%s: nelem's from fwrite = %d\n",prefix,nelem);
	  }
	} /* for (k=0; k<nt; k++) */
	if (warrow && pseudo_color) {
	  ODB_fprintf(fp_echo,"%s: Applying wind-speed as pseudo-color\n",prefix);
	  for (i=0; i<nobs; i++) {
	    double u = res->row_wise ? res->d[i][ucol] : res->d[ucol][i];
	    double v = res->row_wise ? res->d[i][vcol] : res->d[vcol][i];
	    double value = ODB_speed(u,v);
	    f[i] = (float)value; /* May potentially cause a SIGFPE */
	  }
	  {
	    int nelem = fwrite(f, sizeof(*f), nobs, fp);
	    ODB_fprintf(fp_echo,"%s: nelem's from fwrite = %d (pseudo-color)\n",prefix,nelem);
	  }
	}
	FREE(f);
      }
      /* Re-adjust "nrows" for return code (rc) */
      nrows = nobs;
      goto finish;
    }
    
    if (is_odb) {
      ODB_fprintf(fp,": %d %d %d\n",ncols,poolno,nrows);
      ODB_fprintf(fp,": VIEW=\"%s\" on %8.8d at %6.6d\n",info->view,resdate,restime);
      ODB_fprintf(fp,": Pool#%d: no. of rows x cols = %d x %d\n",poolno,nrows,ncols);
    }
    else if (geofmt) {
      write_title = 0;
      ODB_fprintf(fp,"#GEO\n");
      konvert = ODBc_conv2degrees(res, true); /* Convert to degrees, if applicable */
    }
      
    ALLOCX(width, ncols);

    /* Write the pure name and determine max width per column */
    if (write_title && is_odb) {
      ODB_fprintf(fp,":");
    }
    else if (write_title && is_odbtk) {
      ODB_fprintf(fp,"%sUniqueID%sPoolNum%sRowIndex",Delim,delim,delim);
    }

    Delim = is_dump ? (const char *)"" : delim; /* The actual delimiter used */
    for (j=0; j<ncols; j++) {
      col_t *colthis = &info->c[j];
      if (colthis->kind == 1 || colthis->kind == 2 || colthis->kind == 4) {
	const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	const table_t *tc = colthis->t;
	const char *st = (colthis->kind == 1) ? tc->name : "Formula";
	const char *dtype = colthis->dtype;
	int w, ws, wt, wd;
	char *sc = STRDUP(s); /* Pure column name */
	if (colthis->kind == 1) { /* A regular column variable */
	  char *p_at = strchr(sc, '@');
	  if (p_at) { *p_at = '\0'; }
	}
	ws = STRLEN(sc);
	wt = 1 + STRLEN(st); /* "1 + " due to '@' */
	wd = STRLEN(dtype);
	w = MAX(ws, wt);
	w = MAX(w, wd);
	w = MAX(4,w); /* To accomodate the "NULL" string at least */
	switch (colthis->dtnum) {
	case DATATYPE_UINT8: w = MAX(w, ndigits+12); break;
	case DATATYPE_INT4: w = MAX(w, 12); break;
	case DATATYPE_BITFIELD: w = MAX(w, MAXBITS); break;
	case DATATYPE_YYYYMMDD: w = MAX(w, 8); break;
	case DATATYPE_HHMMSS: w = MAX(w, 6); break;
	case DATATYPE_BUFR:
	case DATATYPE_GRIB: w = MAX(w, 10); break;
	case DATATYPE_STRING: w = MAX(w, 10); break;
	default: w = MAX(w,21); break;
	} /* switch (colthis->dtnum) */
	width[j] = w;
	if (write_title) {
	  if (is_odb) {
	    ODB_fprintf(fp,"%s%*.*s", Delim, w, w, sc);
	  }
	  else /* if (is_odbtk || is_default) */ {
	    ODB_fprintf(fp,"%s%s%s%s", Delim, 
			is_dump ? dtype : "", 
			is_dump ? ":" : "", 
			s);
	  }
	}
	FREE(sc);
      }
      else {
	width[j] = 0;
      }
      Delim = delim;
    } /* for (j=0; j<ncols; j++) */

    if (write_title) ODB_fprintf(fp,"\n");

    Delim = is_dump ? (const char *)"" : delim; /* The actual delimiter used */
    if (write_title && is_odb) {
      /* Write the table name for each column */
      Delim = is_dump ? (const char *)"" : delim; /* The actual delimiter used */
      ODB_fprintf(fp,":");
      for (j=0; j<ncols; j++) {
	col_t *colthis = &info->c[j];
	if (colthis->kind == 1 || colthis->kind == 2 || colthis->kind == 4) {
	  const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	  const table_t *tc = colthis->t;
	  const char *st = (colthis->kind == 1) ? tc->name : "Formula";
	  int wt = STRLEN(st);
	  int w = width[j] - 1; /* "-1" for '@' */
	  int wspaces = w - wt;
	  int js;
	  ODB_fprintf(fp,"%s",Delim);
	  for (js=0; js<wspaces; js++) ODB_fprintf(fp," ");
	  ODB_fprintf(fp,"@%s", st);
	  Delim = delim;
	}
      }
      ODB_fprintf(fp,"\n");

      /* Write the "====" for each column */
      Delim = is_dump ? (const char *)"" : delim; /* The actual delimiter used */
      ODB_fprintf(fp,":");
      for (j=0; j<ncols; j++) {
	col_t *colthis = &info->c[j];
	if (colthis->kind == 1 || colthis->kind == 2 || colthis->kind == 4) {
	  const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	  const table_t *tc = colthis->t;
	  const char *st = (colthis->kind == 1) ? tc->name : "Formula";
	  int js, w, ws, wt, wspaces;
	  char *sc = STRDUP(s); /* Pure column name */
	  if (colthis->kind == 1) { /* A regular column variable */
	    char *p_at = strchr(sc, '@');
	    if (p_at) { *p_at = '\0'; }
	  }
	  ws = STRLEN(sc);
	  wt = 1 + STRLEN(st); /* "1 + " due to '@' */
	  w = MAX(ws,wt);
	  wspaces = width[j] - w;
	  ODB_fprintf(fp,"%s",Delim);
	  for (js=0; js<wspaces; js++) ODB_fprintf(fp," ");
	  for (js=wspaces; js<width[j]; js++) ODB_fprintf(fp,"=");
	  FREE(sc);
	}
	Delim = delim;
      }
      ODB_fprintf(fp,"\n");
    } /* if (write_title && is_odb) */
    else if (write_title && is_odbtk) {
      Delim = is_dump ? (const char *)"" : delim; /* The actual delimiter used */
      ODB_fprintf(fp,"%s========%s=======%s========",Delim,delim,delim);
      for (j=0; j<ncols; j++) {
	const col_t *colthis = &info->c[j];
	const char *dtype = colthis->dtype;
	ODB_fprintf(fp,"%s%s",Delim,dtype);
	Delim = delim;
      }      
      ODB_fprintf(fp,"\n");
    } /* else if (write_title && is_odbtk) */

    if (geofmt) {
      /* For MetView GEO-points formats only */
      switch (geofmt) {
      case 1 : 
	ODB_fprintf(fp,"# lat      long       level          date      time        value\n");
	break;
      case 2 : 
	ODB_fprintf(fp,"# x/long       y/lat       value\n");
	ODB_fprintf(fp,"#FORMAT XYV\n");
	break;
      case 3 :
	ODB_fprintf(fp,"# lat        lon height      date        time        u       v\n");
	ODB_fprintf(fp,"#FORMAT XY_VECTOR\n");
	break;
      case 4 :
	ODB_fprintf(fp,"# lat        lon     height      date        time        speed   direction\n");
	ODB_fprintf(fp,"#FORMAT POLAR_VECTOR\n");
	break;
      }
      ODB_fprintf(fp,"#DATA\n");
    }

    /* Print data itself */

    for (i=0; i<nrows; i++) {

      Delim = is_dump ? (const char *)"" : delim; /* The actual delimiter used */

      if (is_odb) {
	ODB_fprintf(fp,"%s",Delim);
      }
      else if (is_odbtk) {
	double value = ODB_put_one_control_word(i, poolno);
	union { 
	  u_ll_t llu;
	  double d;
	} u;
	u.d = value;
	ODB_fprintf(fp,"%s%llu%s%d%s%d", Delim, u.llu, delim, poolno, delim, i+1);
      }
      else if (geofmt) {
	/* If any of the values to be printed is missing data, then skip this row.
	   Checking done in reverse order for speed (usually a higher probability for a match) */
	Bool is_mdi = false;
	for (j=ncols-1; j>=0; j--) {
	  double value = res->row_wise ? res->d[i][j] : res->d[j][i];
	  if (ABS(value) == mdi) {
	    is_mdi = true;
	    break;
	  }
	}
	if (is_mdi) continue; /* for (i=0; i<nrows; i++) */
      }

      Delim = is_dump ? (const char *)"" : delim; /* The actual delimiter used */
      for (j=0; j<ncols; j++) {
	col_t *colthis = &info->c[j];
	double value = res->row_wise ? res->d[i][j] : res->d[j][i];
	int w = width[j];
	if (!geofmt && print_mdi && colthis->dtnum != DATATYPE_STRING && ABS(value) == mdi) {
	  if (is_odb) {
	    ODB_fprintf(fp,"%s%*.*s",Delim,w,w,"NULL");
	  }
	  else /* if (is_odbtk || is_default) */ {
	    ODB_fprintf(fp,"%sNULL", Delim);
	  }
	}
	else {
	  switch (colthis->dtnum) {
	  case DATATYPE_UINT8: 
	    {
	      union { 
		u_ll_t llu;
		double d;
	      } u;
	      u.d = value;
	      if (is_odb) {
		ODB_fprintf(fp,"%s%*llu", Delim, w, u.llu);
	      }
	      else /* if (is_odbtk || is_default) */ {
		ODB_fprintf(fp,"%s%llu", Delim, u.llu);
	      }
	    }
	  case DATATYPE_INT4:
	    if (is_odb) {
	      ODB_fprintf(fp,"%s%*d", Delim, w, (int) value);
	    }
	    else {
	      ODB_fprintf(fp,"%s%d", Delim, (int) value);
	    }
	    break;
	  case DATATYPE_YYYYMMDD:
	    if (is_odb) {
	      ODB_fprintf(fp,"%s%*.8d", Delim, w, (int) value);
	    }
	    else {
	      ODB_fprintf(fp,"%s%8.8d", Delim, (int) value);
	    }
	    break;
	  case DATATYPE_HHMMSS:
	    if (is_odb) {
	      ODB_fprintf(fp,"%s%*.6d", Delim, w, (int) value);
	    }
	    else {
	      int theval = (int)value;
	      if (geofmt) theval /= 100;
	      ODB_fprintf(fp,geofmt ? "%s%4.4d" : "%s%6.6d", Delim, theval);
	    }
	    break;

	  case DATATYPE_BITFIELD:
	    if (is_odb || is_odbtk) {
	      /* Borrowed from lib/prt.c : codb_fill_bitfield_() */
	      char ss[MAXBITS+1]; /* MAXBITS == 32 from "privpub.h" */
	      char *c = ss;
	      unsigned int one = 1U << (MAXBITS - 1); /* 1 << 31 */
	      int j;
	      union {
		unsigned int u;
		int v;
	      } u;
	      u.v = (int) value;
	      for (j=0; j<MAXBITS; j++) {
		*c++ = (u.u & one) ? '1' : '0';
		one >>= 1;
	      }
	      *c = 0;
	      if (is_odb) {
		ODB_fprintf(fp,"%s%*.*s", Delim, w, w, ss);
	      }
	      else {
		ODB_fprintf(fp,"%s%s", Delim, ss);
	      }
	    }
	    else {
	      ODB_fprintf(fp,"%s%d", Delim, (int) value);
	    }
	    break;

	  case DATATYPE_BUFR:
	  case DATATYPE_GRIB:
	    {
	      int js;
	      ODB_fprintf(fp,"%s",Delim);
	      for (js=0; js<w-10; js++) ODB_fprintf(fp," ");
	      ODB_fprintf(fp,"0x%8.8X", (int) value);
	    }
	    break;
	  case DATATYPE_STRING:
	    {
	      col_t *colprev = (j > 0) ? &info->c[j-1] : NULL;
	      uint prevdtnum = colprev ? colprev->dtnum : 0;
	      col_t *colnext = (j-1 < ncols) ? &info->c[j+1] : NULL;
	      uint nextdtnum = colnext ? colnext->dtnum : 0;
	      int js;

	      if (joinstr && 
		  (prevdtnum == DATATYPE_STRING || nextdtnum == DATATYPE_STRING)
		  ) {
		char *thisbase = GetColArrayBaseName(colthis);
		char *prevbase = (prevdtnum == DATATYPE_STRING && thisbase) ? 
		  GetColArrayBaseName(colprev) : NULL;
		char *nextbase = (nextdtnum == DATATYPE_STRING && thisbase) ?
		  GetColArrayBaseName(colnext) : NULL;
		if (!strequ(thisbase, prevbase)) prevdtnum = 0;
		if (!strequ(thisbase, nextbase)) nextdtnum = 0;
		FREE(nextbase);
		FREE(prevbase);
		FREE(thisbase);
	      }

	      if (!joinstr || prevdtnum != DATATYPE_STRING) ODB_fprintf(fp,"%s",Delim);
	      if (is_odb) for (js=0; js<w-10; js++) ODB_fprintf(fp," ");
	      if (!joinstr || prevdtnum != DATATYPE_STRING) ODB_fprintf(fp,"'");

	      PRINT_STRING(ODB_fprintf, value);

	      if (!joinstr || nextdtnum != DATATYPE_STRING) ODB_fprintf(fp,"'");
	    }
	    break;
	  default:
	    if (is_odb) {
	      ODB_fprintf(fp,"%s%*.14g", Delim, w, value);
	    }
	    else /* if (is_odbtk || is_default) */ {
	      if (fmt_string && *fmt_string == '%') {
		ODB_fprintf(fp,"%s", Delim);
		ODB_fprintf(fp,fmt_string, value);
	      }
	      else {
		ODB_fprintf(fp,"%s%.14g", Delim, value);
	      }
	    }
	  } /* switch (colthis->dtnum) */
	}
	Delim = delim;
      } /* for (j=0; j<ncols; j++) */
      ODB_fprintf(fp,"\n");
    } /* for (i=0; i<nrows; i++) */
    FREEX(width);

  finish:
    if (konvert && !the_final_print) (void) ODBc_conv2degrees(res, false); /* Convert back */
    rc = nrows;
    fflush(fp);
  } /* if (fp && info && res && res->mem && res->d) */
  if (iret) *iret = rc;
  DRHOOK_END(0);
  return res;
}


PUBLIC void *
ODBc_print_data(const char *filename, FILE *fp, void *Result, 
		const char *format, const char *fmt_string,
		int konvert, int write_title, int joinstr,
		int the_first_print, int the_final_print, 
		int *iret)
{
  int rc = 0;
  result_t *res = Result;
  result_t *outres = res;
  DRHOOK_START(ODBc_print_data);
  if (fp && res) {
    Bool is_odb = ODBc_test_format_2(format,"odb");
    while (res) {
      int rc_local = 0;
      result_t *newres = PrintData(filename, fp, res, 
				   format, fmt_string,
				   konvert, write_title, joinstr,
				   the_first_print, the_final_print, 
				   &rc_local);
      rc += rc_local;
      if (newres == res) {
	if (!is_odb) write_title = 0;
	res = res->next;
      }
      else {
	outres = newres;
	break; /* while (res) */
      }
    } /* while (res) */
  }
  if (iret) *iret = rc;
  DRHOOK_END(0);
  return outres;
}


PUBLIC void *
ODBc_operate(void *Result, const char *whatkey)
{
  result_t *res = Result;
  info_t *info = res ? res->info : NULL;
  DRHOOK_START(ODBc_operate);
  if (res && info && whatkey) {
    if (strequ(whatkey,"count(*)")) {
      if ((info->optflags & 0x1) == 0x1) {
	/* A special case : SELECT count(*) FROM table */
	double value = 0;
	int nrows = 1;
	int ncols = 1;
	int poolno = -1;
	Bool row_wise = true; /* doesn't matter ; just one value involved */
	result_t *newres = ODBc_new_res("ODBc_operate => newres of count(*)", 
					__FILE__, __LINE__,
					nrows, ncols, info, row_wise, poolno);
	while (res) {
	  if (res->d && res->d[0] &&
	      res->nrows_out == 1 && res->ncols_out == 1) {
	    value += res->d[0][0];
	  }
	  res = res->next;
	}
	newres->d[0][0] = value;
	newres->nrows_out = 1;
	newres->ncols_out = 1;

	res = Result;
	(void) ODBc_unget_data(res);
	res = newres;
      } /* if ((info->optflags & 0x1) == 0x1) */
    } /* if (strequ(whatkey,"count(*)")) */
  }
  DRHOOK_END(0);
  return res;
}


PUBLIC void *
ODBc_sort(void *Result, const int Keys_override[], int Nkeys_override)
{
  result_t *res = Result;
  info_t *info = res ? res->info : NULL;
  DRHOOK_START(ODBc_sort);
  if (res) {
    if ((info && (info->norderby > 0 || info->has_select_distinct)) ||
	(Keys_override && Nkeys_override > 0)) { 
      int ncols = 0;

      if (info) {
	if (Keys_override && Nkeys_override > 0) {
	  ncols = info->ncols_true + info->ncols_aux;
	}
	else {
	  ncols = info->ncols_true;
	}
      }
      else {
	ncols = res->ncols_in;
      }

      if (ncols > 0) { /* Apart from insane cases this should always be true */
	result_t *newres = ODBc_merge_res("ODBc_sort => newres",
					  __FILE__, __LINE__,
					  res, ncols, info, false);
	int nrows = newres->nrows_out;
	int nra = newres->nra;
	int i, j, k;

	if (!info) newres->typeflag = res->typeflag;

	{
	  /* Use F90-routine to perform multikey-"keysort" */
	  extern int ec_is_little_endian(); /* from ifsaux/support/endian.c */
	  int *keys = NULL;
	  int nkeys = 0;
	  int *idx = NULL;
	  int nidx = 0;
	  int init_idx = 0;
	  int numabs = 0;
	  int *abscols = NULL;
	  int *swapstr = NULL;
	  int nswap = 0;
	  result_t *copyres = NULL;

	  if (Keys_override && Nkeys_override > 0) { 
	    /* (#1) The highest precedence : User supplied sorting key(s) */
	    nkeys = Nkeys_override;
	    ALLOC(keys, nkeys);
	    k = 0;
	    for (j=0; j<nkeys; j++) {
	      /* keys are assumed to be in terms of Fortran column [1..ncols] or [-ncols..-1] */
	      /* Also, no ABS()-sorting option available with this */
	      int abskey = ABS(Keys_override[j]);
	      if (abskey >= 1 && abskey <= ncols) {
		keys[k++] = Keys_override[j];
	      }
	    }
	    nkeys = k;
	  }
	  else if (info && info->norderby > 0) { 
	    /* (#2) Has precedence over the 'SELECT DISTINCT' */
	    int maxcols = info->maxcols; /* odb98.x compile-time ODB_maxcols() */
	    nkeys = info->norderby;
	    ALLOC(keys, nkeys);
	    CALLOC(abscols, nkeys);
	    k = 0;
	    for (j=0; j<nkeys; j++) {
	      col_t *colthis = &info->o[j];
	      int kind = colthis->kind;
	      int abskind = ABS(kind);
	      if (abskind >= 1 && abskind <= ncols) {
		keys[k++] = kind; /* Fortran column [1..ncols] or [-ncols..-1] */
	      }
	      else if (abskind > maxcols &&
		       abskind <= ncols + maxcols) { /* ABS-sorting */
		int colid = (kind > 0) ? ABS(kind - maxcols) : ABS(kind + maxcols);
		abscols[numabs++] = --colid;
		keys[k++] = (kind > 0) ? (kind - maxcols) : (kind + maxcols);
	      }
	    }
	    nkeys = k; /* The actual number of valid keys */
	    if (numabs > 0) {
	      /* Switch on index-sorting, when numabs > 0 */
	      nidx = nrows;
	      ALLOC(idx, nidx);
	      init_idx = 1;
	    }
	    else {
	      FREE(abscols);
	    }
	  }
	  else if (info && info->has_select_distinct) {
	    /* (#3) Lowest precedence : 'SELECT DISTINCT' */
	    nkeys = ncols;
	    ALLOC(keys, nkeys);
	    for (j=0; j<nkeys; j++) keys[j] = j+1; /* "+1" due to Fortran column indexing */
	  }

	  if (nkeys == 0) goto sort_done;

	  if (ec_is_little_endian()) {
	    /* Applies really to little-endian machines only:
	       Check if any of the columns to be sorted are strings --
	       if true, then instruct swap bytes on those columns 
	       BEFORE entering the ckeysort() and 
	       to swap then back after the sort
	    */
	    unsigned int typeflag = info ? 0 : newres->typeflag;
	    ALLOC(swapstr, nkeys);
	    nswap = 0;
	    for (j=0; j<nkeys; j++) {
	      int colid = ABS(keys[j]) - 1;
	      if (colid >= 0 && colid < ncols) {
		if (info) {
		  col_t *colthis = &info->c[colid];
		  if (colthis && colthis->dtnum == DATATYPE_STRING) swapstr[nswap++] = colid;
		}
		else {
		  int is_string = (typeflag == 0) ? 0 : 
		    ODBIT_test(&typeflag, MAXBITS, MAXBITS, colid, colid);
		  if (is_string == 1) swapstr[nswap++] = colid;
		}
	      }
	    }

	    if (nswap == 0) {
	      FREE(swapstr);
	    }
	    else { /* nswap > 0 */
	      for (j=0; j<nswap; j++) {
		int colid = swapstr[j];
		double *colptr = &newres->d[colid][0];
		swap8bytes_(colptr, &nrows);
	      } /* for (j=0; j<nswap; j++) */
	    }
	  }

	  /* Take absolute of the ABS-sorting columns, but before that
	     save a copy of the original column value */

	  if (numabs > 0 && abscols) {
	    CALLOC(copyres, 1);
	    copyres->nrows_in = nrows;
	    copyres->ncols_in = numabs;
	    copyres->nra = nra;
	    copyres->nmem = nra * numabs;
	    ALLOC(copyres->mem, copyres->nmem);
	    copyres->row_wise = false;
	    ALLOC(copyres->d, numabs);
	    k = 0;
	    for (j=0; j<numabs; j++) { /* since not row_wise */
	      copyres->d[j] = &copyres->mem[k];
	      k += nra;
	    }
	    copyres->next = NULL; /* no chain */
	    for (j=0; j<numabs; j++) {
	      int colid = abscols[j];
	      double *colptr_in = &newres->d[colid][0];
	      double *colptr_out = &copyres->d[j][0];
	      memcpy(colptr_out, colptr_in, nrows * sizeof(double));
	      for (i=0; i<nrows; i++) {
		if (colptr_in[i] < 0) colptr_in[i] = -colptr_in[i];
	      }
	    } /* for (j=0; j<numabs; j++) */
	  }

	  {
	    int iret = 0;
	    int dummy_idx = 0;
	    
	    /* Fortran90-routine from lib/ckeysort.F90 */
	    ckeysort_(newres->mem, &nra, &nrows, &ncols, 
		      keys, &nkeys,
		      idx ? idx : &dummy_idx, &nidx, &init_idx,
		      &iret);

	    if (iret != nrows) {
	      fprintf(stderr,"***Error in ODBc_sort(): "
		      "Multikeysort has failed : iret = %d, "
		      "expecting nrows=%d (ncols=%d, nra=%d, memptr=%p, nidx=%d)\n",
		      iret, nrows, ncols, nra, newres->mem, nidx);
	      RAISE(SIGABRT);
	    }
	  } /* if (nkeys > 0) */

	  if (copyres) { 
	    /* Restore values that were "ABSoluted" */
	    for (j=0; j<numabs; j++) {
	      int colid = abscols[j];
	      double *colptr_out = &newres->d[colid][0];
	      double *colptr_in = &copyres->d[j][0];
	      memcpy(colptr_out, colptr_in, nrows * sizeof(double));
	    }
	    FREE(abscols);
	    (void) ODBc_unget_data(copyres);
	  }

	  if (swapstr && nswap > 0) {
	    /* Swap string's back */
	    for (j=0; j<nswap; j++) {
	      int colid = swapstr[j];
	      double *colptr = &newres->d[colid][0];
	      swap8bytes_(colptr, &nrows);
	    } /* for (j=0; j<nswap; j++) */
	    FREE(swapstr);
	  }

	  if (idx && nidx > 0) {
	    /* Finally, if index sorting was requested, perform physical reordering */
	    /* Note that the index idx[] is a Fortran-index with range [1..nrows],
	       so we need to subtract 1 */
	    double *tmp;
	    ALLOC(tmp, nrows);
	    for (j=0; j<ncols; j++) {
	      double *colptr = &newres->d[j][0];
	      for (i=0; i<nrows; i++) {
		tmp[i] = colptr[idx[i]-1];
	      } /* for (i=0; i<nrows; i++) */
	      memcpy(colptr, tmp, nrows * sizeof(double));
	    } /* for (j=0; j<ncols; j++) */
	    FREE(tmp);
	    FREE(idx);
	  }
	  
	sort_done:
	  FREE(keys);
	}

	res = newres;
      } /* if (ncols > 0) */
    } 
  }
  DRHOOK_END(0);
  return res;
}


PRIVATE int
which_aggr(const col_t *c, int *ncols_aux)
{
  /* See also compiler/tree.c for similar function ODB_which_aggr() */
  const char *s = c ? c->name : NULL;
  int Ncols_aux = 0;
  uint aggr_flag = ODB_AGGR_NONE;
  DRHOOK_START(which_aggr);
  if (s && *s == '_') s++;
  if (s && *s) {
    char *name = STRDUP(s);
    char *p_br = strchr(name, '(');
    if (p_br) {
      *p_br = '\0';
      if (strequ(name,"min")) aggr_flag = ODB_AGGR_MIN;
      else if (strequ(name,"max")) aggr_flag = ODB_AGGR_MAX;
      else if (strequ(name,"density")) aggr_flag = ODB_AGGR_DENSITY;
      else if (strequ(name,"sum")) aggr_flag = ODB_AGGR_SUM;
      else if (strequ(name,"sum_distinct")) aggr_flag = ODB_AGGR_SUM_DISTINCT;
      else if (strequ(name,"avg")) aggr_flag = ODB_AGGR_AVG;
      else if (strequ(name,"avg_distinct")) aggr_flag = ODB_AGGR_AVG_DISTINCT;
      else if (strequ(name,"median")) aggr_flag = ODB_AGGR_MEDIAN;
      else if (strequ(name,"median_distinct")) aggr_flag = ODB_AGGR_MEDIAN_DISTINCT;
      else if (strequ(name,"stdev")) aggr_flag = ODB_AGGR_STDEV;
      else if (strequ(name,"stdev_distinct")) aggr_flag = ODB_AGGR_STDEV_DISTINCT;
      else if (strequ(name,"var")) aggr_flag = ODB_AGGR_VAR;
      else if (strequ(name,"var_distinct")) aggr_flag = ODB_AGGR_VAR_DISTINCT;
      else if (strequ(name,"rms")) aggr_flag = ODB_AGGR_RMS;
      else if (strequ(name,"rms_distinct")) aggr_flag = ODB_AGGR_RMS_DISTINCT;
      else if (strequ(name,"count")) aggr_flag = ODB_AGGR_COUNT;
      else if (strequ(name,"count_distinct")) aggr_flag = ODB_AGGR_COUNT_DISTINCT;
      else if (strequ(name,"bcount")) aggr_flag = ODB_AGGR_BCOUNT;
      else if (strequ(name,"bcount_distinct")) aggr_flag = ODB_AGGR_BCOUNT_DISTINCT;
      else if (strequ(name,"dotp")) aggr_flag = ODB_AGGR_DOTP;
      else if (strequ(name,"dotp_distinct")) aggr_flag = ODB_AGGR_DOTP_DISTINCT;
      else if (strequ(name,"norm")) aggr_flag = ODB_AGGR_NORM;
      else if (strequ(name,"norm_distinct")) aggr_flag = ODB_AGGR_NORM_DISTINCT;
      else if (strequ(name,"covar")) { aggr_flag = ODB_AGGR_COVAR; Ncols_aux = 1; }
      else if (strequ(name,"corr")) { aggr_flag = ODB_AGGR_CORR; Ncols_aux = 1; }
      else if (strequ(name,"linregr_a")) { aggr_flag = ODB_AGGR_LINREGR_A; Ncols_aux = 1; }
      else if (strequ(name,"linregr_b")) { aggr_flag = ODB_AGGR_LINREGR_B; Ncols_aux = 1; }
      else if (strequ(name,"minloc")) { aggr_flag = ODB_AGGR_MINLOC; Ncols_aux = 1; }
      else if (strequ(name,"maxloc")) { aggr_flag = ODB_AGGR_MAXLOC; Ncols_aux = 1; }
    }
    FREE(name);
  }
  if (ncols_aux) *ncols_aux = Ncols_aux;
  DRHOOK_END(0);
  return (int)aggr_flag;
}


PUBLIC void *
ODBc_aggr(void *Result, int phase_id)
{
  result_t *res = Result;
  info_t *info = res ? res->info : NULL;
  DRHOOK_START(ODBc_aggr);
  if ((phase_id == 0 || phase_id == 1) &&
      res && info && info->has_aggrfuncs && info->colaux) {
    /* This is supposed to be similar to the F90-coding in include/fodb.h */
    FILE *fp_echo = ODBc_get_debug_fp();
    int i, j, k;
    int ncols, ncols_aux, ncols_aggr, ncols_tot;
    double tmpval[2];
    Bool all_aggr_funcs;

    ncols = info->ncols_true;
    ncols_aux = info->ncols_aux;
    ncols_aggr = info->ncols_aggr_formula;
    ncols_tot = ncols + ncols_aux;

    all_aggr_funcs = (ncols_aggr == ncols) ? true : false;

    if (all_aggr_funcs) {
      /* Here all columns are aggregate functions */
      if (res->next) { /* Has chain */
	/* Note below: "ncols_tot", not "ncols" : we need both SELECT + aux -columns */
	result_t *newres = ODBc_merge_res("ODBc_aggr => newres",
					  __FILE__, __LINE__,
					  res, ncols_tot, info, false);
	res = ODBc_aggr(newres, phase_id); /* Ends up with the "else"-block below */
      }
      else { /* No chain --> plain sailing ! */
	int nrows = res->nrows_out;
	for (j=0; j<ncols; j++) { /* i.e. run up to "ncols_true" */
	  col_t *colthis = &info->c[j];
	  int aggrfuncflag = which_aggr(colthis, NULL);
	  int jj, i2nd;
	  double *d = &res->d[j][0]; /* always column-wise (i.e. not row_wise) */
	  double *daux;
	  if (phase_id == 1 || info->colaux[j] == j+1) {
	    jj = j;
	    daux = NULL;
	    i2nd = 1;
	  }
	  else { /* The sibling column */
	    jj = info->colaux[j] - 1;
	    daux = &res->d[jj][0]; 
	    i2nd = 2;
	  }
	  codb_calc_aggr_(&phase_id, &aggrfuncflag, &i2nd, tmpval, &nrows, d, daux);
	  jj = j;
	  for (i=0; i<i2nd; i++) {
	    d = &res->d[jj][0];
	    d[0] = tmpval[i];
	    jj = info->colaux[jj] - 1;
	  }
	} /* for (j=0; j<ncols; j++) */
	res->nrows_out = 1;
	res->ncols_out = ncols; /* i.e. "ncols_true" */
      } /* if (res->next) ... else ... */
    }
    else {
      /* Not all columns are aggregate functions => perform binned aggregate function calc */
      int n_keys = ncols - ncols_aggr; /* i.e. # of non-aggr. funcs */
      int *keys = NULL;
      CALLOC(keys, n_keys);
      k = 0;
      for (j=0; j<ncols; j++) { /* i.e. run up to the "ncols_true" */
	col_t *colthis = &info->c[j];
	if (colthis->kind != 4) {
	  keys[k++] = j+1; /* "+1" due to Fortran column numbering */
	}
      }
      n_keys = k;
      res = ODBc_sort(res, keys, n_keys); /* Sorted + merged into a single result-set */

      {
	result_t *tmp = NULL;
	int nrows = res->nrows_out;
	int ngrp, jj, maxlen, i2nd, ii;
	int *grpsta = NULL;

	CALLOC(grpsta, nrows+1);
	ngrp = 0;
	jj = 0;
	grpsta[ngrp] = jj;

	for (i=1; i<nrows; i++) {
	  Bool changed = false;
	  
	  for (j=0; j<n_keys; j++) {
	    int akey = keys[j]-1;
	    double djj = res->d[akey][jj]; 
	    double di  = res->d[akey][i];
	    if (djj != di) {
	      changed = true;
	      break; /* for (j=0; j<n_keys; j++) */
	    }
	  } /* for (j=0; j<n_keys; j++) */
	  
	  if (changed) {
	    jj = i;
	    grpsta[++ngrp] = jj;
	  }
	} /* for (i=1; i<nrows; i++) */
	grpsta[++ngrp] = nrows;

	FREE(keys);

	maxlen = 0;
	for (ii=0; ii<ngrp; ii++) {
	  int nr = grpsta[ii+1] - grpsta[ii];
	  maxlen = MAX(maxlen, nr);
	}

	if (phase_id == 1 || ncols_aux == 0) {
	  i2nd = 1;
	}
	else {
	  i2nd = 2;
	}

	ODB_fprintf(fp_echo, 
		    "ODBc_aggr: phase_id=%d, ncols=%d, ncols_aux=%d, ncols_aggr=%d, nrows=%d, "
		    "ngrp=%d, n_keys=%d, maxlen=%d, i2nd=%d\n",
		    phase_id, ncols, ncols_aux, ncols_aggr, nrows, 
		    ngrp, n_keys, maxlen, i2nd);
	
	tmp = ODBc_new_res("ODBc_aggr => tmp", 
			   __FILE__, __LINE__,
			   maxlen, i2nd, NULL, false, -1);

	if (tmp) {
	  result_t *newres = ODBc_new_res("ODBc_aggr => newres",
					  __FILE__, __LINE__,
					  ngrp, ncols_tot, info, false, res->poolno);
	
	  int *aggrflag = NULL;
	  ALLOCX(aggrflag, ncols);

	  for (j=0; j<ncols; j++) { /* i.e. run up to the "ncols_true" */
	    col_t *colthis = &info->c[j];
	    aggrflag[j] = which_aggr(colthis, NULL);
	  }
	  
	  for (ii=0; ii<ngrp; ii++) {
	    int nr = grpsta[ii+1] - grpsta[ii];
	    jj = grpsta[ii];
	    for (j=0; j<ncols; j++) { /* i.e. run up to the "ncols_true" */
	      col_t *colthis = &info->c[j];
	      int aggrfuncflag = aggrflag[j];

	      if (aggrfuncflag == ODB_AGGR_NONE) {
		/* A non-aggregate function column */
		double *dres  = &newres->d[j][ii];
		double *d     = &res->d[j][jj]; 
		dres[0] = d[0];
	      }
	      else {
		/* This is an aggregate function column */
		i2nd = 0;
		{
		  double *dtmp = &tmp->d[i2nd++][0];
		  double *d = &res->d[j][0]; 
		  for (k=0; k<nr; k++) {
		    dtmp[k] = d[jj+k];
		  }
		}

		{
		  if (!(phase_id == 1 || info->colaux[j] == j+1)) {
		    double *dtmp = &tmp->d[i2nd++][0];
		    int colid = info->colaux[j] - 1;
		    double *d = &res->d[colid][0]; 
		    for (k=0; k<nr; k++) {
		      dtmp[k] = d[jj+k];
		    }
		  }
		}

		{
		  int j2;
		  double *dtmp = &tmp->d[0][0]; /* always column-wise (i.e. not row_wise) */
		  double *daux = (i2nd == 2) ? &tmp->d[i2nd-1][0] : NULL;
#if 0
		  ODB_fprintf(fp_echo,
			      "ii=%d, j=%d : before codb_calc_aggr_(phase_id=%d): i2nd=%d, "
			      "info->colaux[%d] = %d, dtmp=%p, daux=%p, nr=%d\n",
			      ii, j, phase_id, i2nd, j, info->colaux[j], dtmp, daux, nr);
#endif
		  codb_calc_aggr_(&phase_id, &aggrfuncflag, &i2nd, tmpval, &nr, dtmp, daux);
		  j2 = j;
		  for (i=0; i<i2nd; i++) {
		    double *dres = &newres->d[j2][ii]; /* Note: [ii], not [i] */
		    *dres = tmpval[i];
		    j2 = info->colaux[j2] - 1;
		  }
		}
	      } /* if (aggrfuncflag == ODB_AGGR_NONE) ... else ... */
	    } /* for (j=0; j<ncols; j++) */
	  } /* for (ii=0; ii<ngrp; ii++) */
	  FREEX(aggrflag);
	  
	  (void) ODBc_unget_data(res);
	  res = newres;
	  if (phase_id == 1) res->ncols_out = ncols; /* i.e. "ncols_true" */

	  (void) ODBc_unget_data(tmp);
	} /* if (tmp) */
	FREE(grpsta);
      }
    } /* if (all_aggr_funcs) ... else ... */
  }
  DRHOOK_END(0);
  return res;
}


#define FCLOSE_FP_OUT() \
if (fp_out) { \
  fflush(fp_out); \
  /* if (use_gzip_pipe) pclose(fp_out); else fclose(fp_out); */ \
  if (iounit >= 0) cma_close_(&iounit, &iret); else fclose(fp_out); \
  fp_out = NULL; \
  iounit = -1; \
}

PUBLIC FILE *
ODBc_print_file(FILE *fp_out, int *fpunit,
		const char *outfile, const char *format,
		int poolno, const char *view,
		Bool first_time,
		Bool *Reopen_per_view,
		Bool *Reopen_per_pool,
		Bool *Use_gzip_pipe)
{
  char *gzip = getenv("ODB_GZIP");
  FILE *fp_echo = ODBc_get_debug_fp();
  Bool reopen_per_view = Reopen_per_view ? *Reopen_per_view : false;
  Bool reopen_per_pool = Reopen_per_pool ? *Reopen_per_pool : false;
  Bool use_gzip_pipe = Use_gzip_pipe ? *Use_gzip_pipe : false;
  int iounit = fpunit ? *fpunit : ((int)-ABS(mdi));
  int iret = 0;
  Bool some_plotobs = (strequ(format,"plotobs") || strequ(format,"wplotobs")) ? true : false;
  DRHOOK_START(ODBc_print_file);

  ODB_fprintf(fp_echo,
	      "<ODBc_print_file(fp_out=%p [stdout=%p], fpunit=(%d @ %p), first_time=%s\n"
	      "\toutfile='%s', format='%s', poolno=%d, view='%s')\n",
	      fp_out, stdout, iounit, fpunit,
	      first_time ? "true" : "false",
	      outfile ? outfile : NIL, format ? format : NIL,
	      poolno, view ? view : NIL);

  if (first_time) {
    if (strequ(outfile,"/dev/null")) {
      if (some_plotobs) {
	/* Prevent [w]plotobs output to end up on the "screen" */
	cma_open_(&iounit, outfile, "w", &iret, STRLEN(outfile), 1);
	fp_out = (iounit >= 0 && iret == 1) ? CMA_get_fp(&iounit) : NULL;
      }
      else {
	fp_out = stdout;
	iounit = -1;
      }
      reopen_per_view = false;
      reopen_per_pool = false;
      use_gzip_pipe = false;
    }
    else {
      int outlen = STRLEN(outfile);
      reopen_per_view = strstr(outfile,"%s") ? true : false;
      reopen_per_pool = strstr(outfile,"%d") ? true : false;
      use_gzip_pipe = (outlen >= 3 && strnequ(&outfile[outlen-3],".gz",3)) ? true : false;
      if (!reopen_per_view && !reopen_per_pool) {
	ODB_fprintf(fp_echo, "Opening file '%s'\n",outfile);
	cma_open_(&iounit, outfile, "w", &iret, STRLEN(outfile), 1);
	fp_out = (iounit >= 0 && iret == 1) ? CMA_get_fp(&iounit) : NULL;
      } /* if (!reopen_per_view && !reopen_per_pool) */
    }
  }
  else if (fp_out != stdout && poolno <= 0 && view){
    if (reopen_per_view && !reopen_per_pool) {
      char *ppfile = NULL;
      int pplen = STRLEN(outfile) + STRLEN(view) + 1;
      FCLOSE_FP_OUT();
      ALLOC(ppfile,pplen);
      snprintf(ppfile,pplen,outfile,view); /* Has just '%s' */
      ODB_fprintf(fp_echo, "Opening file '%s'\n",ppfile);
      cma_open_(&iounit, ppfile, "w", &iret, STRLEN(ppfile), 1);
      fp_out = (iounit >= 0 && iret == 1) ? CMA_get_fp(&iounit) : NULL;
      FREE(ppfile);
    } /* if (reopen_per_view && !reopen_per_pool) */
  }
  else if (fp_out != stdout && poolno > 0 && view) {
    if (reopen_per_pool) {
      char *ppfile = NULL;
      int pplen = STRLEN(outfile) + STRLEN(view) + 12 + 1;
      FCLOSE_FP_OUT();
      ALLOC(ppfile,pplen);
      if (!reopen_per_view) { /* Has just '%d' */
	snprintf(ppfile,pplen,outfile,poolno);
      }
      else { /* Has both '%d' & '%s'; need to find which comes first */
	const char *pcd = strstr(outfile,"%d");
	const char *pcs = strstr(outfile,"%s");
	if (pcd < pcs) { /* '%d' before '%s' */
	  snprintf(ppfile,pplen,outfile,poolno,view);
	}
	else { /* '%s' before '%d' */
	  snprintf(ppfile,pplen,outfile,view,poolno);
	}
      }
      ODB_fprintf(fp_echo, "Opening file '%s'\n",ppfile);
      cma_open_(&iounit, ppfile, "w", &iret, STRLEN(ppfile), 1);
      fp_out = (iounit >= 0 && iret == 1) ? CMA_get_fp(&iounit) : NULL;
      FREE(ppfile);
    } /* if (reopen_per_pool) */
  }
  else if (fp_out == stdout) {
    fflush(fp_out);
  }
  else if (fp_out) {
    FCLOSE_FP_OUT();
  }

  if (Reopen_per_view) *Reopen_per_view = reopen_per_view;
  if (Reopen_per_pool) *Reopen_per_pool = reopen_per_pool;
  if (Use_gzip_pipe) *Use_gzip_pipe = use_gzip_pipe;

  if (fpunit) *fpunit = iounit;

  ODB_fprintf(fp_echo,
	      ">ODBc_print_file(fp_out=%p [stdout=%p], fpunit=(%d @ %p) ...)\n"
	      "\t>reopen_per_view=%s, reopen_per_pool=%s, use_gzip_pipe=%s\n",
	      fp_out, stdout,
	      iounit, fpunit,
	      reopen_per_view ? "true" : "false",
	      reopen_per_pool ? "true" : "false",
	      use_gzip_pipe ? "true" : "false");

  DRHOOK_END(0);
  return fp_out;
}

static const double RMiss = 1.7e38; /* Traditional GRIB/BUFR missing data indicator that fits
				       into single precision (float) values */

#if defined(sngl)
#undef sngl
#endif

/* Provide single precision (float) conversion */
#define sngl(d) (ABS(d) > RMiss ? (float)(-RMiss) : (float)d)

#define WRITE_FORTRAN_RECLEN(fp) \
{ \
  int x = reclen; \
  if (swap_endian) swap4bytes_(&x, &oneword); \
  (void) fwrite(&x, sizeof(x), 1, fp); \
}

#define WRITE_PADDING(fp, npad) if ((npad) > 0) fprintf(fp,"%*.*s",(npad),(npad)," ")


PUBLIC int 
ODBc_ODBtool(FILE *fpin,  int *fpunit,
	     const char *file, Bool ascii, void *Result, void *Info)
{
  /* Compatible with IDL-procedure $ODB_SYSPATH/read_odb.pro */
  int rc = 0; /* No. of records (binary) or lines (ascii) written */
  DRHOOK_START(ODBc_ODBtool);
  if ((fpin || file) && Result) {
    result_t *res = Result;
    info_t *info = Info;
    const char *sql_query = ODBc_get_sql_query(info);
    const char create_view[] = "CREATE VIEW odbtool";
    const char select_stmt_start[] = "SELECT lat, lon, body.len,";
    const char end_sql[] = "END-SQL";
    int iounit = fpunit ? *fpunit : ((int)-ABS(mdi));
    int iret = 0;
    FILE *fp = NULL;
    if (fpin) {
      fp = fpin;
    }
    else {
      cma_open_(&iounit, file, "w", &iret, STRLEN(file), 1);
      fp = (iounit >= 0 && iret == 1) ? CMA_get_fp(&iounit) : NULL;
      if (fpunit) *fpunit = iounit;
    }
    if (fp) {
      if (ascii) {
	int nlcnt = 0; /* No. of newlines */
	int ncols = res->ncols_out;
	int nfrom = info ? info->nfrom : 0;
	
	fprintf(fp, "%s\n",create_view); nlcnt++;
	fprintf(fp, "%s\n",select_stmt_start); nlcnt++;

	if (info && ncols >= 4 && nfrom > 0) {
	  int j;
	  for (j=3; j<ncols; j++) {
	    const col_t *colthis = &info->c[j];
	    const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	    fprintf(fp, "%s,\n", s); nlcnt++;
	  }
	  fprintf(fp, "FROM\n"); nlcnt++;
	  for (j=0; j<nfrom; j++) {
	    const table_t *t = &info->t[j];
	    const char *s = t->name;
	    fprintf(fp, "%s,\n", s); nlcnt++;
	  }
	  if (sql_query) {
	    const char *where = strstr(sql_query, "WHERE");
	    if (!where) where = strstr(sql_query, "where");
	    if (where) {
	      char *s = STRDUP(where);
	      char *sc = strchr(s,';');
	      if (sc) *sc = '\0';
	      fprintf(fp, "%s",s);
	      FREE(s);
	      while (*where && *where != ';') {
		if (*where == '\n') nlcnt++;
		where++;
	      }
	    }
	    fprintf(fp, "\n"); nlcnt++;
	  } /* if (sql_query) */
	}

	fprintf(fp, "%s\n", end_sql); nlcnt++;

	if (res->nrows_out > 0 && ncols >= 4) {
	  /* Now write the first set of results */
	  Bool row_wise = res->row_wise;
	  int irow = 0;
	  double r_chan = row_wise ? res->d[irow][2] : res->d[2][irow];
	  r_chan = sngl(r_chan);
	  if (r_chan >= 1 && r_chan <= res->nrows_out) {
	    int k, jcol, nchan = r_chan;
	    int cnt;
	    float f;
	    double lat = row_wise ? res->d[irow][0] : res->d[0][irow];
	    double lon = row_wise ? res->d[irow][1] : res->d[1][irow];
	    lat = ODB_lldegrees(lat);
	    f = sngl(lat); fprintf(fp, "%20.12e", f);
	    lon = ODB_lldegrees(lon);
	    f = sngl(lon); fprintf(fp, "%20.12e", f);
	    fprintf(fp, "%8d\n", nchan);
	    nlcnt++;
	    cnt = 0;
	    for (k=0; k<nchan; k++) {
	      for (jcol=3; jcol<ncols; jcol++) {
		double d = row_wise ? res->d[irow][jcol] : res->d[jcol][irow];
		f = sngl(d); fprintf(fp, "%20.12e", f);
		cnt++;
		if (cnt == 12) { cnt = 0; fprintf(fp, "\n"); nlcnt++; }
	      }
	      irow++;
	    }
	    fprintf(fp, "\n"); nlcnt++;
	  } /* if (ABS(r_nchan) != mdi && r_chan >= 1 && r_chan <= res->nrows_out) */
	} /* if (res->nrows_out > 0 && ncols >= 4) */
	rc += nlcnt;
      }
      else {
	/* Binary */
	/* If at the beginning of file, then dump the SQL-query, too */
	extern int ec_is_little_endian();
	char *env_swap_endian = getenv("ODBTOOL_SWAP_ENDIAN");
	Bool swap_endian = ec_is_little_endian() ? true : false; /* we rely on big-endian files */
	Bool temp_header = (!fpin || (ftell(fpin) == 0)) ? true : false;
	int reclen;
	const int oneword = 1;

	if (env_swap_endian) {
	  /* Override the default via export ODBTOOL_SWAP_ENDIAN=0 or 1 */
	  int what = atoi(env_swap_endian);
	  swap_endian = (what == 0) ? false : true;
	}

	if (temp_header) {
	  /* For debugging etc. purposes do not write temp_header at all ? */
	  char *env_write_temp_header = getenv("ODBTOOL_WRITE_TEMP_HEADER");
	  if (env_write_temp_header) {
	    int what = atoi(env_write_temp_header);
	    temp_header = (what == 0) ? false : true;
	  }
	}

	if (temp_header) {
	  int ncols = res->ncols_out;
	  int nfrom = info ? info->nfrom : 0;
	  int len;
	  reclen = 80; /* fixed */
	  
	  /* CREATE VIEW ... */
	  WRITE_FORTRAN_RECLEN(fp);
	  len = STRLEN(create_view);
	  (void) fwrite(create_view, len, 1, fp);
	  WRITE_PADDING(fp, reclen-len);
	  WRITE_FORTRAN_RECLEN(fp);
	  rc++;

	  /* SELECT ... */
	  WRITE_FORTRAN_RECLEN(fp);
	  len = STRLEN(select_stmt_start);
	  (void) fwrite(select_stmt_start, len, 1, fp);
	  WRITE_PADDING(fp, reclen-len);
	  WRITE_FORTRAN_RECLEN(fp);
	  rc++;

	  if (info && ncols >= 4 && nfrom > 0) {
	    int j;
	    /* The rest of the columns */
	    for (j=3; j<ncols; j++) {
	      const col_t *colthis = &info->c[j];
	      const char *s = colthis->nickname ? colthis->nickname : colthis->name;
	      WRITE_FORTRAN_RECLEN(fp);
	      len = STRLEN(s);
	      (void) fwrite(s, len, 1, fp);
	      (void) fwrite(",", 1, 1, fp);
	      len++;
	      WRITE_PADDING(fp, reclen-len);
	      WRITE_FORTRAN_RECLEN(fp);
	      rc++;
	    }

	    /* FROM */
	    WRITE_FORTRAN_RECLEN(fp);
	    len = STRLEN("FROM");
	    (void) fwrite("FROM", len, 1, fp);
	    WRITE_PADDING(fp, reclen-len);
	    WRITE_FORTRAN_RECLEN(fp);
	    rc++;

	    /* FROM-tables */
	    for (j=0; j<nfrom; j++) {
	      const table_t *t = &info->t[j];
	      const char *s = t->name;
	      WRITE_FORTRAN_RECLEN(fp);
	      len = STRLEN(s);
	      (void) fwrite(s, len, 1, fp);
	      (void) fwrite(",", 1, 1, fp);
	      len++;
	      WRITE_PADDING(fp, reclen-len);
	      WRITE_FORTRAN_RECLEN(fp);
	      rc++;
	    }

	    /* Possible WHERE-stmt */
	    if (sql_query) {
	      const char *where = strstr(sql_query, "WHERE");
	      if (!where) where = strstr(sql_query, "where");
	      if (where) {
		char *s = STRDUP(where);
		char *sc = strchr(s,';');
		if (sc) *sc = '\0';
		WRITE_FORTRAN_RECLEN(fp);
		len = STRLEN(s);
		if (len > reclen) len = reclen;
		(void) fwrite(s, len, 1, fp);
		WRITE_PADDING(fp, reclen-len);
		WRITE_FORTRAN_RECLEN(fp);
		rc++;
		FREE(s);
	      }
	    } /* if (sql_query) */
	  }

	  /* END-SQL */
	  WRITE_FORTRAN_RECLEN(fp);
	  len = STRLEN(end_sql);
	  (void) fwrite(end_sql, len, 1, fp);
	  WRITE_PADDING(fp, reclen-len);
	  WRITE_FORTRAN_RECLEN(fp);
	  rc++;
	} /* if (temp_header) */

	while (res) {
	  int nrows = res->nrows_out;
	  int ncols = res->ncols_out;
	  if (nrows > 0 && ncols >= 4) {
	    Bool row_wise = res->row_wise;
	    int irow = 0;
	    double r_chan = row_wise ? res->d[irow][2] : res->d[2][irow];
        double r_chanref = r_chan;
        int nchan = r_chan;
	    int fbuflen = nchan * (ncols - 3);
	    float *fbuf=NULL, *pfbuf=NULL;
	    ALLOCX(fbuf, fbuflen);
	    while (irow < nrows) {
	      int nrows_left = nrows - irow;
	      if (r_chan >= 1 && r_chan <= nrows_left) {
		int k, jcol, tmp;
		
		float f;
		double lat = row_wise ? res->d[irow][0] : res->d[0][irow];
		double lon = row_wise ? res->d[irow][1] : res->d[1][irow];
        r_chan = row_wise ? res->d[irow][2] : res->d[2][irow];
		if (r_chan != r_chanref) {
		  /* free fbuf and reallocate fbuf */
		  
		  FREEX(fbuf);
		  nchan = r_chan;
		  fbuflen = nchan * (ncols - 3);
		  r_chanref = r_chan;
		  ALLOCX(fbuf, fbuflen);
		}
		/* (lat,lon,chan) */
		reclen = 2 * sizeof(f) + sizeof(tmp);
		WRITE_FORTRAN_RECLEN(fp);

		lat = ODB_lldegrees(lat);
		f = sngl(lat); 
		if (swap_endian) swap4bytes_(&f, &oneword);
		(void) fwrite(&f, sizeof(f), 1, fp);

		lon = ODB_lldegrees(lon);
		f = sngl(lon); 
		if (swap_endian) swap4bytes_(&f, &oneword);
		(void) fwrite(&f, sizeof(f), 1, fp);

		tmp = nchan;
		if (swap_endian) swap4bytes_(&tmp, &oneword);
		(void) fwrite(&tmp, sizeof(tmp), 1, fp);

		WRITE_FORTRAN_RECLEN(fp);
		rc++;

		/* channel data */
		reclen = fbuflen * sizeof(*fbuf);
		/*AF 27/11/08	ALLOCX(fbuf, fbuflen);  outside the loop on rows */
		pfbuf = fbuf;
		WRITE_FORTRAN_RECLEN(fp);
		for (jcol=3; jcol<ncols; jcol++) {
		  for (k=0; k<nchan; k++) {
		    double d = row_wise ? res->d[irow][jcol] : res->d[jcol][irow];
		    f = sngl(d); 
		    *pfbuf++ = f; 
		    irow++;
		    if (irow > nrows) {
		      fprintf(stderr,"***Error: Unexpected end of channel data\n");
		      RAISE(SIGABRT);
		    }
		  } /* for (k=0; k<nchan; k++) */
		  irow -= nchan;
		}
		if (swap_endian) swap4bytes_(fbuf, &fbuflen);
		(void) fwrite(fbuf, sizeof(*fbuf), fbuflen, fp);
		/*AF FREEX(fbuf); */
		WRITE_FORTRAN_RECLEN(fp);

		irow += nchan;
	      } /* if (r_chan >= 1 && r_chan <= nrows_left) */
	      else {
		/* To avoid infinite loop */
		fprintf(stderr,"***Error: Invalid value for # of channels (nchan) = %.0f\n",r_chan);
		RAISE(SIGABRT);
	      }
	    } /* while (irow < nrows) */
	    if (fbuf)
	      FREEX(fbuf);
	  } /* if (nrows > 0 && ncols >= 4) */
	  res = res->next;
	} /* while (res) */
 	
      }
      if (!fpin) {
	cma_close_(&iounit, &iret);
	if (fpunit) *fpunit = -1;
      }
    } /* if (fp) */
  }
  DRHOOK_END(0);
  return rc;
}
