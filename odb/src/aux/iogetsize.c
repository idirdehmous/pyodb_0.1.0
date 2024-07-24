#include "iostuff.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

int
IOgetsize(const char *path, 
	  int *filesize,
	  int *blksize)
{
  int rc = 0;
  struct stat buf;
  int file_size = 0;
  int blk_size = 0;

  rc = stat(path,&buf);
  if (rc != 0) {
    file_size = -1;
    blk_size  = -1;
  }
  else {
    file_size = buf.st_size;
    blk_size  = buf.st_blksize;
  }

  if (filesize) *filesize = file_size;
  if (blksize)  *blksize  = blk_size;

  return rc;
}

static int
recur_mkdir(const char *dir)
{
  struct stat buf;
  int rc = stat(dir, &buf);

  if (rc != 0 && errno == ENOENT) {
    char *prevdir = STRDUP(dir);
    char *last = strrchr(prevdir,'/');
    if (last) {
      *last = '\0';
      rc = recur_mkdir(prevdir);
    }
    FREE(prevdir);
    rc = mkdir(dir,S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
  }

  return rc;
}

int
IOmkdir(const char *pathname)
{
  int created = 0;

  if (strchr(pathname,'/')) {
    int rc;
#ifndef NECSX
    extern int errno;
#endif
    struct stat buf;
    char *dir = STRDUP(pathname);
    char *last = strrchr(dir,'/');
    if (last) *last = '\0';

    /* === Obsolete code ===
    rc = stat(dir, &buf);
    if (rc != 0 && errno == ENOENT) {
      char *cmd;
      int len = strlen(BIN_MKDIR) + strlen(dir) + 2;
      ALLOC(cmd,len);
      sprintf(cmd,"%s %s",BIN_MKDIR,dir);
      (void)system(cmd);
      FREE(cmd);
      rc = stat(dir, &buf);
    }
      === end of Obsolete code === */

    rc = recur_mkdir(dir);
    rc = stat(dir, &buf);
    if (rc == 0) created = S_ISDIR(buf.st_mode);

    FREE(dir);
  }

  return created;
}

