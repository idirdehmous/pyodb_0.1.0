/*

   UTIL_ALLOC_IOBUF: Activates full, partial or no I/O-buffering for opened file
   UTIL_FREE_IOBUF : Frees allocated I/O-buffer space

   Sami Saarinen, ECMWF, 13/03/96
     "              "    16/08/99 : Change to use filehandle via pbfp()            

*/



#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
/* #include <malloc.h> */
#include <stdlib.h>
#include <unistd.h>

#include "alloc.h"

extern FILE *pbfp(integer4 khandle);

/* Not needed; We use -qextname while linking with Fortran (SS/23.07.2001)
#ifdef RS6K
#define util_alloc_iobuf_  util_alloc_iobuf
#define util_free_iobuf_   util_free_iobuf
#endif
*/

#ifdef CRAY
#define util_alloc_iobuf_  UTIL_ALLOC_IOBUF
#define util_free_iobuf_   UTIL_FREE_IOBUF
#endif

typedef struct _Iobuf {
  char *buffer;
  int   length;
} Iobuf;

PRIVATE Iobuf *iobuf_data = NULL;
PRIVATE int maxfdsize      = 0;

PRIVATE void allocate_iobuf_data()
{
  if (!iobuf_data) {
#ifdef CRAYXT
    maxfdsize = sysconf(_SC_OPEN_MAX);
#else
    maxfdsize = getdtablesize();
#endif
#ifdef DEBUG
    fprintf(stderr,"allocate_iobuf_data: maxfdsize = %d\n",maxfdsize);
#endif
    CALLOC(iobuf_data, maxfdsize);
  }
}

void util_alloc_iobuf_(integer4 *khandle,      /* Already opened file handle */
		 integer4 *ibufsize,     /* 0      : "no" buffering => use system defaults & return
					n  < 0 : device specific optimal buffer size is allocated 
					n  > 0 : full buffering up to n bytes
					n != 0 : User provides the buffer space for setvbuf() */
		 integer4 *kret)         /* Return value: 0 = successful */
{
  struct stat buf;
  FILE *fp = pbfp(*khandle);

  *kret = 0;

  if (fstat(fileno(fp),&buf) != 0) {    
    perror("UTIL_ALLOC_IOBUF");
    *kret = -1;
  }
  else if (*ibufsize != 0) {
    int fildes = fileno(fp);
    int length = 0;
    int bufsize = abs(*ibufsize);
    int optimal_size = (buf.st_blksize > 0) ? buf.st_blksize : BUFSIZ;
    
    if (!iobuf_data) allocate_iobuf_data();
    length = iobuf_data[fildes].length;

    if (length == 0) {
      int n = (*ibufsize > 0) ? 
	((bufsize + 2*optimal_size - 1)/optimal_size) * optimal_size : 
	  optimal_size;
      char *buffer; 

      ALLOC(buffer, n + 8); /* This is freed upon UTIL_FREE_IOBUF() */

      if (!buffer) {
	perror("UTIL_ALLOC_IOBUF");
	*kret = -1;
      }

      if (setvbuf(fp, buffer, _IOFBF, n) != 0) {
	perror("UTIL_ALLOC_IOBUF");
	*kret = -1;
      }

      iobuf_data[fildes].buffer = buffer;
      iobuf_data[fildes].length = n;
#ifdef DEBUG
      fprintf(stderr,
	      "UTIL_ALLOC_IOBUF: Allocated I/O-buffer space %d bytes for fileno %d\n",
	      n, fildes);
#endif
    }
    else {
      fprintf(stderr,
	      "UTIL_ALLOC_IOBUF: I/O-buffer space at fileno %d was already in use\n",
	      fildes);
      *kret = -1;
    }
  } /* else if (*ibufsize != 0) */
}


void util_free_iobuf_(integer4 *khandle,  /* Already opened file handle */
		      integer4 *kret)     /* Return value: 0 = successful */
{
  FILE *fp = pbfp(*khandle);
  int fildes = fileno(fp);
  *kret = 0;

  if (!iobuf_data) return;

  {
    int  *length  = &iobuf_data[fildes].length;
    char **buffer = &iobuf_data[fildes].buffer;

    if (*length > 0 && *buffer) {
#ifdef DEBUG
      int n = *length;
      fprintf(stderr,
	      "UTIL_FREE_IOBUF: Released I/O-buffer space %d bytes for fileno %d\n",
	      n, fildes);
#endif
      *length = 0;
      FREE(*buffer);
      *buffer = NULL;
    }
  }
}


