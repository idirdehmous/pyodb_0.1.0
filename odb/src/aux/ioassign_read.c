#include "iostuff.h"
#include "ioassign.h"
#include <sys/types.h>
#include <sys/stat.h>
#include "memmap.h"

static int  /* Borrowed from filesize.c */
filesize_by_fp(FILE *fp, const char *path)
{
  int rc;
  struct stat buf;
  int file_size = 0;

  rc = fstat(fileno(fp),&buf);
  if (rc != 0) {
    file_size = -1;
    PERROR(path);
  }
  else {
    file_size = buf.st_size;
  }

  return file_size;
}

static char *preferred_DBNAME = NULL;

void 
preferred_dbname_ioassign_(const char *dbname
			   /* Hidden arguments */
			   , int dbname_len)
{
  if (dbname && dbname_len > 0) {
    DECL_FTN_CHAR(dbname);
    ALLOC_FTN_CHAR(dbname);
    if (preferred_DBNAME) FREE(preferred_DBNAME);
    preferred_DBNAME = STRDUP(p_dbname);
    FREE_FTN_CHAR(dbname);
  }
}

static char *incore_data = NULL;
static int   len_incore_data = 0;

void
get_incore_ioassign_(char         data[],
		     const int  *ndata, /* in bytes */
		     int *rc)
{
  static int first_time = 1;
  if (first_time) {
    FILE *fp = NULL;
    char *IOASSIGN = getenv("IOASSIGN");
    int allok = 0;
    if (!IOASSIGN) IOASSIGN = "IOASSIGN";
    fp = fopen(IOASSIGN, "r");
    if (!fp && preferred_DBNAME) {
      /* Try to find suitable IOASSIGN under preferred database directory */
      /* When found, use that as IOASSIGN-file */
      char *path, *env;
      const char base[] = "ODB_SRCPATH";
      int baselen = strlen(base);
      int len = baselen + 1 + strlen(preferred_DBNAME) + 1;
      ALLOC(env,len);
      snprintf(env,len,"%s_%s",base,preferred_DBNAME);
      path = getenv(env);
      FREE(env);
      if (!path) path = getenv(base);
      if (path) {
	/* path is now either $ODB_SRCPATH_<dbname> or $ODB_SRCPATH */
	const char also[] = "IOASSIGN";
	int alsolen = strlen(also);
	char *file;
	len = strlen(path) + 1 + strlen(preferred_DBNAME) + 1 + alsolen + 1;
	ALLOC(file,len);
	/* Check for <path>/<dbname>.IOASSIGN */
	snprintf(file,len,"%s/%s.%s",path,preferred_DBNAME,also);
	fp = fopen(file, "r");
	if (!fp) {
	  /* Check for <path>/IOASSIGN */
	  snprintf(file,len,"%s/%s",path,also);
	  fp = fopen(file, "r");
	  if (!fp) {
	    /* Check for <path>/IOASSIGN.<dbname> */
	    snprintf(file,len,"%s/%s.%s",path,also,preferred_DBNAME);
	    fp = fopen(file, "r");
	  } /* if (!fp) */
	} /* if (!fp) */
	if (fp) {
	  IOASSIGN = file;
	  allok = 1;
	}
	else {
	  FREE(file);
	}
      } /* if (path) */
    } /* if (!fp && preferred_DBNAME) */

    if (fp) {
      int nread = 0;
      int fd = fileno(fp);
      memmap_t *m = memmap_open_read(fd, NULL, NULL); /* see ifsaux/support/memmap.c */
      FREE(incore_data);
      if (m) { /* memory mapped I/O : uses mmap() */
	len_incore_data = (int)m->len;
	ALLOC(incore_data,len_incore_data+1);
	memcpy(incore_data, m->buf, len_incore_data * sizeof(*incore_data));
	(void) memmap_close(m);
      }
      else {
	len_incore_data = filesize_by_fp(fp, IOASSIGN);
	ALLOC(incore_data,len_incore_data+1);
	nread = fread(incore_data, sizeof(*incore_data), len_incore_data, fp);
	/* An overkill ? if (nread != len_incore_data) then abort ? */
      }
      incore_data[len_incore_data] = '\0';
      fclose(fp);
      if (allok) FREE(IOASSIGN);
    }
    first_time = 0;
  }
  if (incore_data && len_incore_data > 0) {
    if (data && ndata && *ndata >= len_incore_data) {
      memcpy(data,incore_data,len_incore_data);
    }
  }
  *rc = len_incore_data;
}

void
put_incore_ioassign_(const char   data[],
		     const int  *ndata, /* in bytes */
		     int *rc)
{
  if (data && ndata && *ndata > 0) {
    FREE(incore_data);
    len_incore_data = *ndata;
    ALLOC(incore_data,len_incore_data+1);
    memcpy(incore_data,data,len_incore_data);
    incore_data[len_incore_data] = '\0';
  }
  *rc = len_incore_data;
}

int 
IOassign_read(const char  *IOASSIGN, 
	             int  *pbcount_out, 
	        Ioassign **ioassign_start_out, 
	        Ioassign **ioassign_out)
{
  int rc = 0;
  Ioassign *ioassign = NULL;
  Ioassign *ioassign_start = NULL;
  int pbcount = 0;
  
  /* Open IOASSIGN datafile for input */

  FILE *fp = IOASSIGN ? fopen(IOASSIGN, "r") : NULL;
  
  if (!IOASSIGN) {
    /* stream output (-s) i.e. do not check existing IOASSIGN-file at all */
    goto finish;
  }
  else if (!fp) {
    /* Error : IOASSIGN-file could not be located */
    pbcount = -1;
    rc = -2;
  }
  else {
    /* Read IOASSIGN data */
    char oneline[MAXLINE+1];
    char file[MAXFILELEN];
    char alias[MAXFILELEN];
    
    while ( !feof(fp) && fgets(oneline, sizeof(oneline), fp) ) {
      char *s;
      Ioassign *tmp;
      int nitems;
      
      ALLOC(tmp, 1);
      
      s = strchr(oneline,'\n');
      if (s) *s = '\0';
      if (iostuff_debug) fprintf(stderr,"\n'%s'",oneline);
      
      if (ioassign) {
	ioassign->next = tmp;
	ioassign       = tmp;
	ioassign->next = NULL;
      }
      else {
	ioassign = ioassign_start = tmp;
	ioassign->next = NULL;
      }
      
      ioassign->filename  = NULL;
      ioassign->aliasname = NULL;
      ioassign->numbins  = 0;
      ioassign->readbufsize  = 0;
      ioassign->writebufsize = 0;
      ioassign->prealloc  = 0;
      ioassign->extent    = 0;
      ioassign->compression = 0;
      ioassign->blocksize = 0;
      ioassign->concat    = 0;
      ioassign->maxproc   = 0;
      ioassign->fromproc   = 0;
      ioassign->toproc   = 0;
      ioassign->pipecmd   = NULL;
      
      nitems = sscanf(oneline,"%s %s %d %d %d %d %d %d %d %d %d %d %d",
		      file, alias,
		      &ioassign->numbins,
		      &ioassign->readbufsize, &ioassign->writebufsize,
		      &ioassign->prealloc, &ioassign->extent,
		      &ioassign->compression, &ioassign->blocksize,
		      &ioassign->concat, &ioassign->maxproc,
            &ioassign->fromproc, &ioassign->toproc);
      
      if ((nitems != 11)&&(nitems != 13)) {
	/* Error : Possibly an invalid IOASSIGN-file format encountered */
	pbcount = -abs(pbcount);
	fprintf(stderr,"*** Warning: Invalid IOASSIGN-record encountered; Check format!\n");
	fprintf(stderr,"             '%s'\n",oneline);
	rc = -3;
	goto finish;
      }

      ioassign->hash = IOassign_hash(file);
      ioassign->filename  = IOstrdup(file, NULL);
      ioassign->aliasname = IOstrdup(alias, NULL);
      
      s = strchr(oneline,'|');
      if (s) ioassign->pipecmd = STRDUP(s+1);
      
      if (iostuff_debug) {
	fprintf(stderr,
		"\n==> hash=%u, file='%s', alias='%s', bins=%d, bufsizes=(%d,%d), prealloc=(%d,%d), compr=(%d,%d), concat=%d, maxproc=%d",
		ioassign->hash,
		ioassign->filename, ioassign->aliasname, ioassign->numbins,
		ioassign->readbufsize, ioassign->writebufsize,
		ioassign->prealloc, ioassign->extent,
		ioassign->compression, ioassign->blocksize,
		ioassign->concat, ioassign->maxproc);
	fprintf(stderr,
		"\n    pipe='%s'",TRIM(ioassign->pipecmd));
      }

      pbcount++;
    }

    fclose(fp);
    if (iostuff_debug) fprintf(stderr,"\n");
  }

 finish:

  *pbcount_out        = pbcount;
  *ioassign_start_out = ioassign_start;
  *ioassign_out       = ioassign;

  return rc;
}


typedef struct _incore_t {
  char *p;
  int lenp;
} incore_t;


incore_t *
Use_Incore()
{
  incore_t *px = NULL;
  static incore_t x;
  if (incore_data && len_incore_data > 0) {
    px = &x;
    px->p = incore_data;
    px->lenp = len_incore_data;
  }
  return px;
}

int
Next_Incore(char s[], int slen, incore_t *px)
{
  int rc = 0;
  if (px && px->p && px->lenp > 0) {
    char *next_nl = memchr(px->p,'\n',px->lenp);
    if (next_nl) {
      int ilen;
      rc = next_nl - px->p + 1;
      ilen = MIN(slen,rc);
      memcpy(s,px->p,ilen);
      px->p += rc;
      px->lenp -= rc;
      rc = ilen;
    }
  }
  return rc;
}

int 
IOassign_read_incore(const char  *IOASSIGN, 
	             int  *pbcount_out, 
		     Ioassign **ioassign_start_out, 
		     Ioassign **ioassign_out)
{
  int rc = 0;
  Ioassign *ioassign = NULL;
  Ioassign *ioassign_start = NULL;
  int pbcount = 0;

  /* Open IOASSIGN datafile for input */

  incore_t *fp = IOASSIGN ? Use_Incore() : NULL;
  
  if (!IOASSIGN) {
    /* stream output (-s) i.e. do not check existing IOASSIGN-file at all */
    goto finish;
  }
  else if (!fp) {
    /* Error : IOASSIGN-file could not be located */
    pbcount = -1;
    rc = -2;
  }
  else {
    /* Read IOASSIGN data */
    char oneline[MAXLINE];
    char file[MAXFILELEN];
    char alias[MAXFILELEN];
    
    while ( Next_Incore(oneline, sizeof(oneline), fp) ) {
      char *s;
      Ioassign *tmp;
      int nitems;
      
      ALLOC(tmp, 1);
      
      s = strchr(oneline,'\n');
      if (s) *s = '\0';
      if (iostuff_debug) fprintf(stderr,"\n'%s'",oneline);
      
      if (ioassign) {
	ioassign->next = tmp;
	ioassign       = tmp;
	ioassign->next = NULL;
      }
      else {
	ioassign = ioassign_start = tmp;
	ioassign->next = NULL;
      }
      
      ioassign->filename  = NULL;
      ioassign->aliasname = NULL;
      ioassign->numbins  = 0;
      ioassign->readbufsize  = 0;
      ioassign->writebufsize = 0;
      ioassign->prealloc  = 0;
      ioassign->extent    = 0;
      ioassign->compression = 0;
      ioassign->blocksize = 0;
      ioassign->concat    = 0;
      ioassign->maxproc   = 0;
      ioassign->fromproc   = 0;
      ioassign->toproc   = 0;
      ioassign->pipecmd   = NULL;
      
      nitems = sscanf(oneline,"%s %s %d %d %d %d %d %d %d %d %d %d %d",
		      file, alias,
		      &ioassign->numbins,
		      &ioassign->readbufsize, &ioassign->writebufsize,
		      &ioassign->prealloc, &ioassign->extent,
		      &ioassign->compression, &ioassign->blocksize,
		      &ioassign->concat, &ioassign->maxproc,
            &ioassign->fromproc, &ioassign->toproc);
      
      if ((nitems != 11)&&(nitems != 13)) {
	/* Error : Possibly an invalid IOASSIGN-file format encountered */
	pbcount = -abs(pbcount);
	fprintf(stderr,"*** Warning: Invalid IOASSIGN-record encountered; Check format!\n");
	fprintf(stderr,"             '%s'\n",oneline);
	rc = -3;
	goto finish;
      }

      ioassign->hash = IOassign_hash(file);
      ioassign->filename  = IOstrdup(file, NULL);
      ioassign->aliasname = IOstrdup(alias, NULL);
      
      s = strchr(oneline,'|');
      if (s) ioassign->pipecmd = STRDUP(s+1);
      
      if (iostuff_debug) {
	fprintf(stderr,
		"\n==> hash=%u, file='%s', alias='%s', bins=%d, bufsizes=(%d,%d), prealloc=(%d,%d), compr=(%d,%d), concat=%d, maxproc=%d",
		ioassign->hash,
		ioassign->filename, ioassign->aliasname, ioassign->numbins,
		ioassign->readbufsize, ioassign->writebufsize,
		ioassign->prealloc, ioassign->extent,
		ioassign->compression, ioassign->blocksize,
		ioassign->concat, ioassign->maxproc);
	fprintf(stderr,
		"\n    pipe='%s'",TRIM(ioassign->pipecmd));
      }

      pbcount++;
    }

    if (iostuff_debug) fprintf(stderr,"\n");
  }

 finish:

  *pbcount_out        = pbcount;
  *ioassign_start_out = ioassign_start;
  *ioassign_out       = ioassign;

  return rc;
}
