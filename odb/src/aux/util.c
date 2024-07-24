#include "pcma.h"
#include <signal.h>

#ifdef VPP
#pragma global noalias
#elif defined(NECSX)
#pragma cdir options -pvctl,nodep
#endif

extern int
pcma_error(const char *where,
	   int how_much,
	   const char *what,
	   const char *srcfile,
	   int srcline,
	   int perform_abort)
{
  int rc = errno;

  fprintf(stderr,
	  "pcma_error: %s failed to allocate %d bytes for variable '%s' (in %s:%d)\n",
	  where, how_much, what,
	  srcfile, srcline);

  if (perform_abort) {
    fprintf(stderr,"pcma_error: Aborting ...\n");
    RAISE(SIGABRT);
  }

  return rc;
}

void
pcma_zero_uint(unsigned int u[], int n)
{
  int j;
  for (j=0; j<n; j++) {
    u[j] = 0;
  }
}

void 
pcma_copy_uint(      unsigned int to[],
	       const unsigned int from[],
	       int n)
{
  int j;
  for (j=0; j<n; j++) {
    to[j] = from[j];
  }
}

void 
pcma_copy_dbl(      double to[],
	      const double from[],
	      int n)
{
  int j;
  for (j=0; j<n; j++) {
    to[j] = from[j];
  }
}

unsigned int *
pcma_alloc(Packbuf *pbuf, int count)
{
  unsigned int *ptr = NULL;

  if (pbuf) {
    int curlen = pbuf->len;

    if (curlen >= 0 && count >= 0) {
      int newlen = curlen + count;

      if (pbuf->allocatable && pbuf->maxalloc == 0) {
	/* Initial allocation */
	pbuf->maxalloc = MAX(count, pcma_blocksize);
	ALLOC(pbuf->p, pbuf->maxalloc);
	pbuf->counter++;
      }

      if (newlen > pbuf->maxalloc) {
	if (pbuf->allocatable) {
	  REALLOC(pbuf->p, newlen);
	  pbuf->maxalloc = newlen;
	  pbuf->counter++;
	}
	else {
	  /* Error */
	  pbuf->len = -newlen; /* A sort of an error code */
	  ptr = NULL;
	  fprintf(stderr,
		  "pcma_alloc(): No enough space in the user pack-buffer : needed=%d, available=%d int-words\n",
		  newlen, pbuf->maxalloc);
	  goto finish;
	}
      }

      if (pbuf->p) {
	ptr = &pbuf->p[curlen];
	pbuf->len = newlen;
      }
      else {
	/* Error */
	perror("pcma_alloc()");
	if (pbuf->allocatable) {
	  FREE(pbuf->p);
	}
	pbuf->len = -newlen; /* A sort of an error code */
	ptr = NULL;
	goto finish;
      }
    }
  }
  else {
    ALLOC(ptr, count);
  }

 finish:
  if (ptr) pcma_zero_uint(ptr, count);

  return ptr;
}


#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifdef VPP
#include <sys/fs/vfl.h>
#endif

#ifdef CRAYXT

#include <sys/stat.h>
#define STATVFS stat
#define STAT_FS(p,pb) stat(p,pb)
#define STAT_BSIZE st_blksize

#elif defined(CRAY) || defined(NECSX)

#include <sys/statfs.h>
#define STATVFS statfs
#define STAT_FS(p,pb) statfs(p,pb,sizeof(struct statfs),0)
#define STAT_BSIZE f_bsize

#else

#include <sys/statvfs.h>
#define STATVFS statvfs
#define STAT_FS(p,pb) statvfs(p,pb)
#define STAT_BSIZE f_bsize

#endif

static int
get_path_blocksize(const char *filename)
{
  int blk_size = 0;
  struct STATVFS buf;
  char *path = STRDUP(filename);
  char *s = strrchr(path,'/');

  if (s) {
    *s = '\0';
  }
  else {
    FREE(path);
    path = STRDUP(".");
  }
  
  if (STAT_FS(path,&buf) != -1) {
    blk_size = buf.STAT_BSIZE;
  }

  FREE(path);

  return blk_size;
}

FILE *
pcma_prealloc(const char *filename,
	      int         prealloc,
	      int         extent,
	      int        *blksize,
	      int        *retcode)
{
  int fd = -1;
  int blk_size = 0;
  FILE *fp = NULL;
  const char *write_mode = "w";
  unsigned char allocated = 0;

  if (prealloc > 0) {
    blk_size = get_path_blocksize(filename);

#ifdef VPP
    if (blk_size > 0) {
      const int vfl_mode = 0664;
      prealloc = RNDUP(prealloc,blk_size)/blk_size;
      if (extent <= 0) extent = blk_size;
      extent = RNDUP(extent,blk_size)/blk_size;

      (void) remove(filename);

      fd = vfl_create(filename, prealloc, extent,
		      VFL_ZERO, vfl_mode,
		      VFL_REPLACE);

      if (fd == -1) {
	perror(filename);
      }
      else{
	allocated = 1;
      }
    }
#endif
  }

  if (!allocated) {
    fp = fopen(filename, write_mode);
    if (fp) fd = fileno(fp);
  }
  else {
    fp = fdopen(fd, write_mode);
  }

  if (retcode)                 *retcode  = fd;
  if (blksize && blk_size > 0) *blksize = blk_size;

  return fp;
}



int 
pcma_filesize(const char *path)
{
  struct stat buf;
  long file_size = 0;

  if (stat(path,&buf) != 0) {
    file_size = -1;
  }
  else {
    file_size = buf.st_size;
  }

  return file_size;
}
 
