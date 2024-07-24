#include "odb.h"

extern FILE *CMA_get_fp(const int *unit);


static int linecnt = 0;

PRIVATE int
readline(FILE *fp, char line[], int linelen, int skip_comment_lines)
{
  int ilen = 0;
  while (fp && !feof(fp) && fgets(line, linelen, fp)) {
    linecnt++;
#ifdef DEBUG
    if (linecnt < 30) fprintf(stderr,"dbg-line#%d:%s",linecnt,line);
#endif
    ilen = strlen(line);
    if (ilen > 0 && line[ilen-1] == '\n') { line[ilen-1] = '\0'; ilen--; }
    if (skip_comment_lines) {
      char *p = line;
      while (*p && isspace(*p)) p++;
      ilen = strlen(p);
      /* fprintf(stderr,"dbg-line[ilen=%d]#%d:<%s>\n",ilen,linecnt,p); */
      if (*p == '#' || *p == '!' || ilen == 0) continue;
    }
    if (ilen > 0) break;
  }
  return ilen;
}


PRIVATE int
findcol(const char *target, const char *from)
{
  int n = 0;
  if (target && from) {
    char *p;
    char *s = NULL;
    int len = strlen(target) + 3;
    ALLOC(s, len);
    sprintf(s,"/%s/",target);
    p = strstr(from,s);
    if (p) {
      while (*from && from <= p) {
	if (*from++ == '/')  n++;
      }
    }
    FREE(s);
  }
#ifdef DEBUG
  fprintf(stderr,"findcol: n=%d, target='%s'\n",n,target);
#endif
  return n;
}


PRIVATE double
evaluate(const char *value, double Missing_Data_Indicator, int is_string)
{
  union {
    double d;
    char s[sizeof(double)+1];
  } jack;

  const char *saved = value;

  if (*value == '"') {
    char *v = STRDUP(value);
    char *pv = v;
    int ilen, iptr, dsz=sizeof(double);
    char *p = strchr(++pv, '"');
    if (p) *p = '\0';
    ilen = strlen(pv);
    iptr = MAX(0,dsz - ilen);
    memset(jack.s,' ',dsz);
    strncpy(&jack.s[iptr],pv,dsz);
    FREE(v);
  }
  else if (strequ(value,"NULL")) {
    jack.d = Missing_Data_Indicator;
  }
  else if (is_string) {
    int dsz=sizeof(double);
    int ilen = strlen(value);
    int iptr = MAX(0,dsz - ilen);
    memset(jack.s,' ',dsz);
    strncpy(&jack.s[iptr],value,dsz);
  }
  else {
    jack.d = atof(value);
  }
  jack.s[sizeof(double)] = '\0';

  /*
  {
    char s[sizeof(double)+1];
    char *pin = jack.s;
    char *pout = s;
    memset(s,' ',sizeof(double));
    while (*pin) { 
      if (*pin >= ' ' && *pin < 127) *pout++ = *pin;
      else *pout++ = '?';
      pin++;
    }
    s[sizeof(double)] = '\0';
    fprintf(stderr,"evaluate: %s => (%.20g,'%s')\n",saved,jack.d,s);
  }
  */

  return jack.d;
}


PUBLIC void
read_basic_info_(const int *io,
		 char *tblname,
		 int *nrows,
		 int *ncols,
		 int *poolno,
		 int *width,
		 int *retcode
		 /* Hidden arguments */
		 , const int tblname_len)
{
  int i, rc = 0;
  FILE *fp = CMA_get_fp(io);
  DECL_FTN_CHAR(tblname);
  char line[80];

  ALLOC_OUTPUT_FTN_CHAR(tblname);
  *nrows = 0;
  *ncols = 0;
  *poolno = 0;
  *width = sizeof(line);

  if (fp && !feof(fp)) {
    int finish = 0;
    linecnt = 0;
    if (readline(fp,line,sizeof(line), 0) > 0) {
      char *p = line;
      while (*p && (*p == '#' || *p == '!' || *p == '@' || isspace(*p))) p++;
      strncpy(p_tblname, p, tblname_len);
    }
    while (!finish) {
      if (readline(fp,line,sizeof(line), 0) > 0) {
	char *p = line;
	while (*p && (*p == '#' || *p == '!' || isspace(*p))) p++;
	/* fprintf(stderr,"dbg(3):%s\n",p); */
	if (strnequ(p,"/nrows=",7)) {
	  if (sscanf(p+7,"%d",nrows) == 1) /*fprintf(stderr,"dbg:nrows=%d\n",*nrows)*/;
	}
	else if (strnequ(p,"/ncols=",7)) {
	  if (sscanf(p+7,"%d",ncols) == 1) /*fprintf(stderr,"dbg:ncols=%d\n",*ncols)*/;
	}
	else if (strnequ(p,"/poolno=",8)) {
	  if (sscanf(p+8,"%d",poolno) == 1) /*fprintf(stderr,"dbg:poolno=%d\n",*poolno)*/;
	}
	else if (strnequ(p,"/width=",7)) {
	  if (sscanf(p+7,"%d",width) == 1) /*fprintf(stderr,"dbg:width=%d\n",*width)*/;
	}
	else if (strnequ(p,"/END",4)) {
	  finish = 1;
	}
      }
      else if (feof(fp)) {
	finish = 1;
      }
    } 
    COPY_2_FTN_CHAR(tblname);
  }

  /* finish: */
  FREE_FTN_CHAR(tblname);

  *retcode = rc;
}


PUBLIC void
read_col_info_(const int *io,
	       const char *cvarall,
	       const double *mdi,
	       double z[],
	       const int is_string[],
	       const int *ncols_aux,
	       int colmap[],
	       const int *ncols,
	       const int *width,
	       int *retcode
	       /* Hidden arguments */
	       , int cvarall_len)
{
  int rc = 0;
  double Mdi = *mdi;
  int Ncolz = *ncols_aux;
  int Ncolmap = *ncols;
  FILE *fp = CMA_get_fp(io);
  DECL_FTN_CHAR(cvarall);

  ALLOC_FTN_CHAR(cvarall);

  if (fp && !feof(fp)) {
    int finish = 0;
    int bufsize = 1 + MAX(*width, 80);
    char *line = NULL;
    ALLOC(line,bufsize);
    while (!finish) {
      if (readline(fp, line, bufsize, 1) > 0) {
	char *p = line;
	while (*p && isspace(*p)) p++;
	if (*p) {
	  char *eq_sign = strchr(p,'=');
	  /* fprintf(stderr,"dbg(1):%s\n",p); */
	  if (eq_sign) {
	    int icol;
	    char *colname = p;
	    char *value = eq_sign + 1;
	    *eq_sign = '\0';
	    icol = findcol(colname,p_cvarall);
	    if (icol >= 1 && icol <= Ncolz) {
	      z[icol-1] = evaluate(value, Mdi, is_string[icol-1]);
	    }
	    else {
	      fprintf(stderr,
		      "***Error: Unrecognized variable/value in assignment: %s=%s\n",
		      colname,value);
	      rc++;
	    }
	  }
	  else {
	    int k = 0;
	    for (;k<Ncolmap;) {
	      int icol;
	      char *colname = p;
	      char *next = strchr(p,',');
	      /* fprintf(stderr,"dbg(2):p=<%s>,next=<%s>\n",p,next?next:"NIL"); */
	      if (next) {
		*next++ = '\0';
		while (*next && isspace(*next)) next++;
	      }
	      icol = findcol(colname,p_cvarall);
	      if (icol >= 1 && icol <= Ncolz) {
		colmap[k++] = icol;
	      }
	      else {
		fprintf(stderr,"***Error: Unrecognized column name: %s\n",colname);
		rc++;
	      }
	      if (!next) break;
	      p = next;
	    }
	    finish = 1;
	  }
	}
      }
      else
	finish = 1;
    }
    FREE(line);
  }

  /* finish: */
  FREE_FTN_CHAR(cvarall);

  *retcode = rc;
}


PUBLIC void
read_row_data_(const int *io,
	       const double *mdi,
	       double z[],
	       const int colmap[],
	       const int *ncols,
	       const int *width,
	       int *retcode)
{
  int rc = 0; 
  FILE *fp = CMA_get_fp(io);

  if (fp && !feof(fp)) {
    double Mdi = *mdi;
    int j,Nz = *ncols;  
    int bufsize = 1 + MAX(*width, 80);
    char *line = NULL;
    ALLOC(line,bufsize);
    if (readline(fp, line, bufsize, 1) > 0) {
      char *p = line;
      while (p && *p && isspace(*p)) p++;
      for (j=0; p && *p && j<Nz; j++) {
	char *next = strchr(p,',');
	if (next) {
	  *next++ = '\0';
	  while (next && *next && isspace(*next)) next++;
	}	      
	z[j] = evaluate(p, Mdi, colmap[j] < 0);
	rc++;
	p = next;
      }
    }
    FREE(line);
  }

  *retcode = rc;
}
