#include "cmaio.h"

void
cma_filesize_(const char *filename,
	      integer4 *retcode,
	      int len_filename)
{
  int rc = 0;
  char *tmp_filename;

  ALLOC(tmp_filename, len_filename+1);
  strncpy(tmp_filename, filename, len_filename);
  tmp_filename[len_filename] = '\0';

  (void) IOgetsize(tmp_filename, &rc, NULL);

  FREE(tmp_filename);

  *retcode = rc;
}
