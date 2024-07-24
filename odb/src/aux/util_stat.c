
/*

   UTIL_STAT: Obtains information about the size of the opened file

   Sami Saarinen, ECMWF, 27/11/95
     "              "    16/08/99 : Change to use filehandle via pbfp()            

*/



#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "privpub.h"

extern FILE *pbfp(integer4 khandle);

/* Not needed; We use -qextname while linking with Fortran (SS/23.07.2001)
#ifdef RS6K
#define util_stat_ util_stat
#endif
*/

#ifdef CRAY
#define util_stat_ UTIL_STAT
#endif

void util_stat_(integer4 *khandle,      /* Already opened file handle */
		integer4 *ifile_size)   /* Return value: size of the UTIL-file in bytes */
{
  integer4 file_size = 0;
  struct stat buf;
  FILE *fp = pbfp(*khandle);

  if (fstat(fileno(fp),&buf) != 0) {
    perror("UTIL_STAT");
    file_size = -1;
  }
  else {
    file_size = buf.st_size;
  }

  if (ifile_size) *ifile_size = file_size;
}
