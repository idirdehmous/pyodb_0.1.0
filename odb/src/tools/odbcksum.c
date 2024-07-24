#include "odb.h"
#include <sys/stat.h>
#include <fcntl.h>
#include "odbcrc.h"
#include "memmap.h"

/* odbcksum file(s) */
/* should give identical results to Unix utility cksum */
/* provided for testing purposes of ifsaux/support/crc.c */
/* note: this utility uses odb/aux/odbcrc.c */
/* checks also read-errors */

int
main(int argc, char *argv[])
{
  char *a_out = argv[0];
  int rc = 0;
  int j;
  int numargs = argc - 1;
  
  for (j=1; (numargs == 0) || j<=numargs; j++) {
    char *fn = (numargs > 0) ? argv[j] : NULL;
    int n;
    int fd;
    long long bytes = 0;
    unsigned int filecrc = 0;
    memmap_t *m = NULL;
    
    if (fn == NULL || (fn[0] == '-' && fn[1] == '\0')) {
      fd = 0;
      fn = NULL;
    }
    else if ((fd = open(fn, O_RDONLY)) < 0) {
      fprintf(stderr, "%s: input file \"%s\": ",a_out,fn);
      perror(a_out);
      rc++;
      goto eol;
    }
    
    m = memmap_open_read(fd, NULL, NULL);
    if (m) { /* mmap() succeeded */
      char *mmbuf = m->buf;
      bytes = n = m->len;
      filecrc = ODB_cksum32(mmbuf, n, filecrc);
      memmap_close(m);
    }
    else { /* No mmap() ? */
      char buf[IO_BUFSIZE_DEFAULT];
      while ((n = read(fd, buf, sizeof(buf))) > 0) {
	bytes += n;
	filecrc = ODB_cksum32(buf, n, filecrc);
      }
    }

    if (n < 0) {
      fprintf(stderr, "%s: input file \"%s\": read-error# %d",a_out,fn,n);
      perror(a_out);
      rc++;
      close(fd);
      goto eol;
    }

    filecrc = ODB_pp_cksum32but64len(bytes, filecrc);
    close(fd);
    
    printf("%u %lld",filecrc,bytes);
    if (fn) printf(" %s",fn);
    printf("\n");
    
  eol:
    if (!fn) break;
  } /* for (j=1; !numargs || j<=numargs; j++) */
  
  return rc;
}
