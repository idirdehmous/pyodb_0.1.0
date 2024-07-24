#include "iostuff.h"

#ifdef __uxppx__
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
IOget_path_blocksize(const char *filename)
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
IOprealloc(const char *filename,
	   int         prealloc,
	   int         extent,
	   int        *blksize,
	   int        *retcode)
{
  int fd = -1;
  int blk_size = 0;
  FILE *fp = NULL;
  const char *write_mode = "w";
  Boolean allocated = false;

  if (prealloc > 0) {
    blk_size = IOget_path_blocksize(filename);

#ifdef __uxppx__
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
	PERROR(filename);
      }
      else{
	allocated = true;
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
