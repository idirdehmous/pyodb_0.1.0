#include <stdio.h>
/* #include <malloc.h> */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "alloc.h"

void 
util_filesize_(const char *file_in, 
	         integer4 *iret, 
	       /* A hidden argument */
	              int  lenfile_in)
{
  struct stat buf;
  integer4 file_size = 0;
  char *file = NULL;

  ALLOC(file, lenfile_in + 1);

  memcpy(file, file_in, lenfile_in);
  file[lenfile_in] = '\0';

  if (stat(file,&buf) != 0) {
    perror(file);
    file_size = -1;
  }
  else {
    file_size = buf.st_size;
  }

  FREE(file);

  *iret = file_size;
}


void 
util_readraw_(const char *file_in, 
	            char  data[],
	        integer4 *lendata,
	        integer4 *iret, 
	       /* A hidden argument */
	             int  lenfile_in)
{
  int rc = 0;
  FILE *fp;
  char *file = NULL;

  ALLOC(file, lenfile_in + 1);

  memcpy(file, file_in, lenfile_in);
  file[lenfile_in] = '\0';
 
  fp = fopen(file,"r");
  if (fp) {
    rc = fread(data, sizeof(char), *lendata, fp);
    fclose(fp);
  }
  else {
    perror(file);
    rc = -1;
  }

  FREE(file);

  *iret = rc;
}


void 
util_writeraw_(const char *file_in, 
	             char  data[],
	         integer4 *lendata,
	         integer4 *iret, 
	       /* A hidden argument */
	              int  lenfile_in)
{
  int rc = 0;
  FILE *fp;
  char *file = NULL;

  ALLOC(file, lenfile_in + 1);

  memcpy(file, file_in, lenfile_in);
  file[lenfile_in] = '\0';
 
  fp = fopen(file,"w");
  if (fp) {
    rc = fwrite(data, sizeof(char), *lendata, fp);
    fclose(fp);
  }
  else {
    perror(file);
    rc = -1;
  }

  FREE(file);

  *iret = rc;
}
