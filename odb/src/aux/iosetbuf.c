#include "iostuff.h"

const int min_io_bufsize = IO_BUFSIZE_DEFAULT;

int
IOsetbuf(      FILE *fp,
	 const char *filename,
	    Boolean  read_only,
	      IObuf *iobuf,
	        int *filesize,
	        int *blksize)
{
  int rc = 0;
  int file_size = 0;
  int blk_size = 0;
  
  if (read_only) {
    /* READ-ONLY files */

    rc = IOgetsize(filename, &file_size, &blk_size);
    
    if (iostuff_debug) fprintf(stderr," : filesize=%d, blksize=%d",
			       file_size, blk_size);
    
    if (iobuf->len > 0 || iobuf->len == -1) {
      /* Adjust buflen to prevent buffer becoming too excessive in size 
	 Note: This is a file size prior to opening any pipe */
      if (iobuf->len == -1) iobuf->len = file_size;
      if (file_size >= 0) {
	iobuf->len = MIN(iobuf->len, file_size);
      }
    }

    /* Do not allow too small buffers */
    iobuf->len = MAX(iobuf->len, min_io_bufsize);

    if (iobuf->len > 0) {
      SETBUF(iobuf);
      if (iostuff_debug) {
	fprintf(stderr," : readIObuf=(%p,%d)",
		iobuf->p, iobuf->len);
      }
    }
  }
  else {
    /* WRITE-ONLY files */

    /* Do not allow too small buffers */
    iobuf->len = MAX(iobuf->len, min_io_bufsize);

    if (iobuf->len > 0) {
      SETBUF(iobuf);
      if (iostuff_debug) {
	fprintf(stderr," : writeIObuf=(%p,%d)",
		iobuf->p, iobuf->len);
      }
    }
  }

  if (filesize) *filesize = file_size;
  if (blksize)  *blksize  = blk_size;

  return rc;
}

