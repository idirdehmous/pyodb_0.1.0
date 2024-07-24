
/* ae_dump.c : 
   accident & emergency dump of matrices 
   just after ODB_GET and/or before ODB_PUT
 */

#ifdef RS6K
#pragma options noextchk
#endif

#include "odb.h"
#include "cmaio.h"

#define DUMP_HDRLEN 9

static boolean first_time = 1;

static int ae_dump = 0;
static int ae_dump_proc = -1;

static int dump_no = 0;

static char *particular_dtname = NULL;

static int unit = -1;

static FILE *fp = NULL;

#define CHECK_DUMP_ERROR(code, data, datalen, col) \
if (code != datalen) { \
  fprintf(stderr, \
	  "***Warning: Unable to complete dump#%d (rc=%d) for dataname='%s', length=%d, col#%d ; disabled now\n", \
	  dump_no, code, #data, datalen, col); \
  unit = -1; \
  errflg++; \
  goto finish; \
}

void
codb_init_ae_dump_(int *retcode)
{
  if (first_time) {
    int myproc;
    char *p;
    p = getenv("ODB_AE_DUMP");
    if (p) ae_dump = atoi(p);
    ae_dump = MAX(0,ae_dump);
    p = getenv("ODB_AE_DUMP_PROC");
    if (p) ae_dump_proc = atoi(p);
    codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
    if (ae_dump_proc != -1 && ae_dump_proc != myproc) ae_dump = 0;
    if (ae_dump > 0) {
      p = getenv("ODB_AE_DUMP_DATANAME");
      if (p) particular_dtname = STRDUP(p);
    }
    first_time = 0;
  }
  *retcode = ae_dump;
}

void
codb_ae_dump_(const int *handle,
	      const int *is_get,
	      const int *poolno,
	      const char *dtname,
	      const int *datatype,
	      const int *nra,
	      const int *nrows,
	      const int *ncols,
	      const void *data
	      /* Hidden arguments */
	      , int dtname_len)
{
  int rc = 0;
  int errflg = 0;

  if (first_time) codb_init_ae_dump_(&rc);

  if (ae_dump > dump_no) {
    DECL_FTN_CHAR(dtname);

    ALLOC_FASTFTN_CHAR(dtname);

    if (particular_dtname) {
      /* if (!strequ(p_dtname, particular_dtname)) goto finish; */
      if (!strstr(particular_dtname, p_dtname)) goto finish;
    }

    dump_no++;

    if (dump_no == 1) {
      int myproc = 0;
      char *p = getenv("ODB_AE_DUMP_FILE");
      char *pfile = NULL;

      if (!p) p = "ae_dump_file.%d";
      pfile = STRDUP(p);

      codb_procdata_(&myproc, NULL, NULL, NULL, NULL);

      if (strchr(pfile,'%')) {
	ALLOC(p,strlen(pfile) + 20);
	sprintf(p,pfile,myproc);
      }
      else {
	p = STRDUP(pfile);
      }
      FREE(pfile);

      fprintf(stderr,"***Opening A&E-dumpfile='%s' for PE#%d\n",p,myproc);
      cma_open_(&unit, p, "w", &rc, strlen(p), 1);

      if (rc < 1) {
	perror(p);
	fprintf(stderr,
	"***Warning from codb_ae_dump_(): Unable to open A&E-dumpfile='%s' --> dump disabled\n",
		p);
	unit = -1;
	errflg++;
      }
      FREE(p);

      if (errflg) goto finish;

      {
	extern FILE *CMA_get_fp(const int *unit);
	fp = CMA_get_fp(&unit);
      }
    }

    { /* write dump */
      const unsigned char *out = data;
      int j, size;
      int intlen = (*datatype)%100;
      int realen = (*datatype)/100;
      int lenbytes = MAX(intlen, realen); /* a bit sloppy way to detect datatype, but must suffice right now */
      int lda = (*nra) * lenbytes;
      int Nrows = *nrows;
      int chunk = (*nrows) * lenbytes;
      int Ncols = (*ncols) + 1; /* +1 because of the 0th column, too */
      int total = lenbytes * Nrows * Ncols;
      int hdr[DUMP_HDRLEN];
      unsigned char buf[80];
      int lenbuf;

      hdr[0] = dump_no;
      hdr[1] = *handle;
      hdr[2] = *is_get;
      hdr[3] = Nrows;
      hdr[4] = Ncols;
      hdr[5] = total;
      hdr[6] = intlen;
      hdr[7] = realen;
      hdr[8] = *poolno;

      size = sizeof(hdr);
      cma_writeb_(&unit, (const unsigned char *)hdr, &size, &rc); 
      CHECK_DUMP_ERROR(rc, hdr, size, -1);

      lenbuf = sizeof(buf);
      memset(buf,' ', lenbuf);
      lenbuf = MIN(sizeof(buf), dtname_len);
      memcpy(buf, p_dtname, lenbuf);
      lenbuf = sizeof(buf);
      cma_writeb_(&unit, buf, &lenbuf, &rc); 
      CHECK_DUMP_ERROR(rc, dtname, lenbuf, -1);

      for (j=1; j<=Ncols; j++) {
	cma_writeb_(&unit, (const unsigned char *)out, &chunk, &rc);
	CHECK_DUMP_ERROR(rc, out, chunk, j);
	out += lda;
      }
    }
    
    if (dump_no > ae_dump) {
      cma_close_(&unit, &rc);
      fp = NULL;
      unit = -1;
      ae_dump = 0;
    }

  finish:
    if (fp) fflush(fp);
    if (errflg) ae_dump = 0;
    FREE_FASTFTN_CHAR(dtname);
  }
}

