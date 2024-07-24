#include "odb.h"
#include "cmaio.h"
#include "cdrhook.h"

PRIVATE FILE *fpcached = NULL;
PRIVATE int fprtio = -1;
PRIVATE char *iobuf = NULL;
#define default_iobufsize  1048576
PRIVATE int iobufsize = default_iobufsize;

PUBLIC FILE *
ODB_getprt_FP()
{
  FILE *fp = (fprtio >= 0) ? CMA_get_fp(&fprtio) : stdout;
  DRHOOK_START(ODB_getprt_FP);
  fpcached = fp;
  DRHOOK_END(0);
  return fp;
}

PUBLIC void
codb_flushprt_(int *retcode)
{
  DRHOOK_START(codb_flushprt_);
  *retcode = 0;
  if (fpcached) fflush(fpcached);
  DRHOOK_END(0);
}

PUBLIC void
codb_closeprt_(int *retcode)
{
  DRHOOK_START(codb_closeprt_);
  *retcode = 0;
  if (fprtio >= 0) {
    cma_close_(&fprtio, retcode);
    FREE(iobuf);
    fprtio = -1;
    fpcached = NULL;
  }
  else if (fpcached) {
    fflush(fpcached);
  }
  DRHOOK_END(0);
}

PUBLIC void
codb_openprt_(const char *filename,
	      const int *append_mode,
	      int *retcode,
	      /* Hidden arguments */
	      int filename_len)
{
  int rc = 0;
  DRHOOK_START(codb_openprt_);
  codb_closeprt_(&rc);
  if (strnequ(filename,"stdout",filename_len)) {
    fpcached = stdout;
    rc = 1;
  }
  else if (strnequ(filename,"stderr",filename_len)) {
    fpcached = stderr;
    rc = 1;
  }
  else {
    cma_open_(&fprtio, filename, (*append_mode) ? "a" : "w", &rc, filename_len, 1);
    if (rc == 1) {
      FILE *fp = ODB_getprt_FP();
      if (fp) {
	char *env = getenv("ODB_IO_BUFSIZE");
	if (env) iobufsize = atoi(env);
	if (iobufsize < default_iobufsize) iobufsize = default_iobufsize;
	FREE(iobuf);
	ALLOC(iobuf, iobufsize + 8);
	if (iobuf) setvbuf(fp, iobuf, _IOFBF, iobufsize);
      }
    }
  }
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_lineprt_(const unsigned char *line,
	      int *retcode,
	      /* Hidden arguments */
	      int line_len)
{
  int rc = -1;
  DRHOOK_START(codb_lineprt_);
  if (fprtio >= 0) {
    cma_writeb_(&fprtio, line, &line_len, &rc);
  }
  else if (fpcached) {
    rc = fwrite(line, sizeof(*line), line_len, fpcached);
  }
  *retcode = rc;
  DRHOOK_END(line_len);
}


/* -- 
   Much faster C-based formatting for use by 
   prtdata() in module/odbprint.F90 

   See also module/odbshared.F90 for "prt_t" format definitions
   -- */

#define DELIMPRT ((delim && delim_len > 0) ? delim : " ")
#define DELIMLEN ((delim && delim_len > 0) ? ( (*delim == '\0') ? 0 : strlen(delim) ) : 1)

void
codb_fill_trueint_(const int *print_immediately,
		   const char *delim,
		   const char *fmt,
		   const int *i,
		   char *buf,
		   int *retcode
		   /* Hidden arguments (ignored) */
		   ,const int delim_len
		   ,const int fmt_len
		   ,const int buf_len)
{
  /* pint          = prt_t( 1, 13, '(1x,a12)', "%s%*lld"//char(0)) */
  int blen = buf_len - DELIMLEN;
  DRHOOK_START(codb_fill_trueint_);
  *retcode = 
    (*print_immediately && fpcached)
    ? fprintf(fpcached,fmt,DELIMPRT,blen,(long long int)*i)
    : snprintf(buf,buf_len,fmt,DELIMPRT,blen,(long long int)*i);
  DRHOOK_END(0);
}

void
codb_fill_yyyymmdd_(const int *print_immediately,
		    const char *delim,
		    const char *fmt,
		    const int *i,
		    char *buf,
		    int *retcode
		    /* Hidden arguments (ignored) */
		    ,const int delim_len
		    ,const int fmt_len
		    ,const int buf_len)
{
  /* pyyyymmdd     = prt_t( 7, 14, '(6x,i8.8)', "%s%*.8lld"//char(0)) */
  /* or with ODB_PRINT_PRETTY_DATE_TIME=1, using */
  /* pyyyymmdd_pdt = prt_t(17, 14, '(3x,i2.2,"-",a3,"-",i4.4)', "%s%s"//char(0)) */
  DRHOOK_START(codb_fill_yyyymmdd_);
  {
    int yyyymmdd = *i;
    int yyyy = (yyyymmdd/10000)%10000;
    int mm = (yyyymmdd/100)%100;
    int dd = yyyymmdd%100;
    char s[15];
    const char *mon[1+12] = 
      { 
	"???", 
	"Jan", "Feb", "Mar", "Apr", "May", "Jun",
	"Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
      };
    snprintf(s,sizeof(s),"   %2.2d-%s-%4.4d",
	     dd, (mm >= 1 && mm <= 12) ? mon[mm] : mon[0], yyyy);
    *retcode = 
      (*print_immediately && fpcached)
      ? fprintf(fpcached,fmt,DELIMPRT,s)
      : snprintf(buf,buf_len,fmt,DELIMPRT,s);
  }
  DRHOOK_END(0);
}

void
codb_fill_hhmmss_(const int *print_immediately,
		  const char *delim,
		  const char *fmt,
		  const int *i,
		  char *buf,
		  int *retcode
		  /* Hidden arguments (ignored) */
		  ,const int delim_len
		  ,const int fmt_len
		  ,const int buf_len)
{
  /* phhmmss       = prt_t( 8, 12, '(6x,i6.6)', "%s%*.6lld"//char(0)) */
  /* or with ODB_PRINT_PRETTY_DATE_TIME=1, using */
  /* phhmmss_pdt   = prt_t(18, 12, '(4x,i2.2,":",i2.2,":",i2.2)', "%s%s"//char(0)) */
  DRHOOK_START(codb_fill_hhmmss_);
  {
    int hhmmss = *i;
    int hh = (hhmmss/10000)%100;
    int mm = (hhmmss/100)%100;
    int ss = hhmmss%100;
    char s[13];
    snprintf(s,sizeof(s),"    %2.2d:%2.2d:%2.2d",hh,mm,ss);
    *retcode = 
      (*print_immediately && fpcached)
      ? fprintf(fpcached,fmt,DELIMPRT,s)
      : snprintf(buf,buf_len,fmt,DELIMPRT,s);
  }
  DRHOOK_END(0);
}

void
codb_fill_truehex_(const int *print_immediately,
		   const char *delim,
		   const char *fmt,
		   const unsigned int *u,
		   char *buf,
		   int *retcode
		   /* Hidden arguments (ignored) */
		   ,const int delim_len
		   ,const int fmt_len
		   ,const int buf_len)
{
  /* phex          = prt_t( 6, 12, '(2x,"0x",z8.8)', "%s%*s0x%8.8llx"//char(0)) */
  int blen = buf_len - DELIMLEN - 8;
  DRHOOK_START(codb_fill_truehex_);
  *retcode = 
    (*print_immediately && fpcached)
    ? fprintf(fpcached,fmt,DELIMPRT,blen," ",(unsigned long long int)*u)
    : snprintf(buf,buf_len,fmt,DELIMPRT,blen," ",(unsigned long long int)*u);
  DRHOOK_END(0);
}

void
codb_fill_ctrlw_(const int *print_immediately,
		 const char *delim,
		 const char *fmt,
		 /* In the following two: "unsigned int" rather than "int"
		    just to avoid seeing the sign (which would be wrong anyway) */
		 const unsigned int *poolno, /* the correct range [0..2147483647] */
		 const unsigned int *rownum, /* the correct range [0..2147483647] */
		 char *buf,
		 int *retcode
		 /* Hidden arguments (ignored) */
		 ,const int delim_len
		 ,const int fmt_len
		 ,const int buf_len)
{
  /* pdbidx        = prt_t( 9, 20, '(i10,i10.10)', "%s%10u%10.10u"//char(0)) */
  DRHOOK_START(codb_fill_ctrlw_);
  *retcode = 
    (*print_immediately && fpcached)
    ? fprintf(fpcached,fmt,DELIMPRT,*poolno,*rownum)
    : snprintf(buf,buf_len,fmt,DELIMPRT,*poolno,*rownum);
  DRHOOK_END(0);
}

void
codb_fill_truelonglongint_(const int *print_immediately,
			   const char *delim,
			   const char *fmt,
			   const long long int *ll,
			   char *buf,
			   int *retcode
			   /* Hidden arguments (ignored) */
			   ,const int delim_len
			   ,const int fmt_len
			   ,const int buf_len)
{
  int blen = buf_len - DELIMLEN;
  DRHOOK_START(codb_fill_truelonglongint_);
  *retcode = 
    (*print_immediately && fpcached)
    ? fprintf(fpcached,fmt,DELIMPRT,blen,*ll)
    : snprintf(buf,buf_len,fmt,DELIMPRT,blen,*ll);
  DRHOOK_END(0);
}

void
codb_fill_longlongint_(const int *print_immediately,
		       const char *delim,
		       const char *fmt,
		       const double *d,
		       char *buf,
		       int *retcode
		       /* Hidden arguments (ignored) */
		       ,const int delim_len
		       ,const int fmt_len
		       ,const int buf_len)
{
  int blen = buf_len - DELIMLEN;
  DRHOOK_START(codb_fill_longlongint_);
  *retcode = 
    (*print_immediately && fpcached)
    ? fprintf(fpcached,fmt,DELIMPRT,blen,(long long int)*d)
    : snprintf(buf,buf_len,fmt,DELIMPRT,blen,(long long int)*d);
  DRHOOK_END(0);
}

void
codb_fill_ulonglongint_(const int *print_immediately,
			const char *delim,
			const char *fmt,
			const double *d,
			char *buf,
			int *retcode
			/* Hidden arguments (ignored) */
			,const int delim_len
			,const int fmt_len
			,const int buf_len)
{
  int blen = buf_len - DELIMLEN;
  DRHOOK_START(codb_fill_ulonglongint_);
  *retcode = 
    (*print_immediately && fpcached)
    ? fprintf(fpcached,fmt,DELIMPRT,blen,(unsigned long long int)*d)
    : snprintf(buf,buf_len,fmt,DELIMPRT,blen,(unsigned long long int)*d);
  DRHOOK_END(0);
}

void
codb_fill_dble_(const int *print_immediately,
		const char *delim,
		const char *fmt,
		const double *d,
		char *buf,
		int *retcode
		/* Hidden arguments (ignored) */
		,const int delim_len
		,const int fmt_len
		,const int buf_len)
{
  /* preal         = prt_t( 3, 22, '(1x,1p,g21.14)', "%s%*.14g"//char(0)) */
  int blen = buf_len - DELIMLEN;
  DRHOOK_START(codb_fill_dble_);
  *retcode = 
    (*print_immediately && fpcached)
    ? fprintf(fpcached,fmt,DELIMPRT,blen,*d)
    : snprintf(buf,buf_len,fmt,DELIMPRT,blen,*d);
  DRHOOK_END(0);
}

void
codb_fill_varprec_(const int *print_immediately,
		   const char *delim,
		   const char *fmt,
		   const double *d,
		   char *buf,
		   int *retcode
		   /* Hidden arguments (ignored) */
		   ,const int delim_len
		   ,const int fmt_len
		   ,const int buf_len)
{
  /* pvarprec      = prt_t(10, 22, '(1x,1p,g21.14)', "%s%*.*g"//char(0)) */
  int blen = buf_len - DELIMLEN;
  DRHOOK_START(codb_fill_varprec_);
  {
    int prec = MAX(8,blen-5); /* precision in digits */
    prec = MIN(prec,blen);
    prec = MIN(prec,14);
    *retcode = 
      (*print_immediately && fpcached)
      ? fprintf(fpcached,fmt,DELIMPRT,blen,prec,*d)
      : snprintf(buf,buf_len,fmt,DELIMPRT,blen,prec,*d);
  }
  DRHOOK_END(0);
}

void
codb_fill_string_(const int *print_immediately,
		  const char *delim,
		  const char *fmt,
		  const double *d,
		  char *buf,
		  int *retcode
		  /* Hidden arguments (ignored) */
		  ,const int delim_len
		  ,const int fmt_len
		  ,const int buf_len)
{
  /* pstring       = prt_t( 4, 12, "(2x,'''',a8,'''')", "%s%*s'%8s'"//char(0)) */
  int blen = buf_len - DELIMLEN - 10;
  DRHOOK_START(codb_fill_string_);
  {
    int j;
    register char *c;
    S2D_Union u;
    u.dval = *d;
    if (u.llu == S2D_all_blanks) {
      c = &u.str[sizeof(double)];
    }
    else {
      c = u.str;
      for (j=0; j<sizeof(double); j++) {
	if (!isprint(*c)) *c = '?';
	c++;
      }
    }
    *c = 0;
    *retcode = 
      (*print_immediately && fpcached)
      ? fprintf(fpcached,fmt,DELIMPRT,blen," ",u.str)
      : snprintf(buf,buf_len,fmt,DELIMPRT,blen," ",u.str);
  }
  DRHOOK_END(0);
}

void
codb_fill_null_(const int *print_immediately,
		const char *delim,
		const int *len,
		char *buf,
		int *retcode
		/* Hidden arguments (ignored) */
		,const int delim_len
		,const int buf_len)
{
  int blen = buf_len - DELIMLEN;
  DRHOOK_START(codb_fill_null_);
  {
    *retcode = 
      (*print_immediately && fpcached)
      ? fprintf(fpcached,"%s%*s",DELIMPRT,blen,"NULL")
      : snprintf(buf,buf_len,"%s%*s",DELIMPRT,blen,"NULL");
  }
  DRHOOK_END(0);
}

void
codb_fill_unknown_(const int *print_immediately,
		   const char *delim,
		   const int *len,
		   char *buf,
		   int *retcode
		   /* Hidden arguments (ignored) */
		   ,const int delim_len
		   ,const int buf_len)
{
  int blen = buf_len - DELIMLEN;
  DRHOOK_START(codb_fill_unknown_);
  {
    *retcode = 
      (*print_immediately && fpcached)
      ? fprintf(fpcached,"%s%*s",DELIMPRT,blen,"<what>")
      : snprintf(buf,buf_len,"%s%*s",DELIMPRT,blen,"<what>");
  }
  DRHOOK_END(0);
}

void
codb_fill_bitfield_(const int *print_immediately,
		    const char *delim,
		    const int *len,
		    const unsigned int *u,
		    char *buf,
		    int *retcode
		    /* Hidden arguments (ignored) */
		    ,const int delim_len
		    ,const int buf_len)
{
  int blen = buf_len - DELIMLEN;
  DRHOOK_START(codb_fill_bitfield_);
  {
    char s[MAXBITS+1]; /* MAXBITS == 32 from "privpub.h" */
    char *c = s;
    unsigned int one = 1U << (MAXBITS - 1); /* 1 << 31 */
    int ilen = MAX(MAXBITS,blen);
    int j;
    for (j=0; j<MAXBITS; j++) {
      *c++ = (*u & one) ? '1' : '0';
      one >>= 1;
    }
    *c = 0;
    *retcode = 
      (*print_immediately && fpcached)
      ? fprintf(fpcached,"%s%*s",DELIMPRT,ilen,s)
      : snprintf(buf,buf_len,"%s%*s",DELIMPRT,ilen,s);
  }
  DRHOOK_END(0);
}
