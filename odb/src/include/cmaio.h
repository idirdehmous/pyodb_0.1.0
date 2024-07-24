#ifndef _CMAIO_H_
#define _CMAIO_H_

#include "privpub.h"

#include "iostuff.h"
#include "pcma_extern.h"

#ifndef FORTRAN_CALL
#define FORTRAN_CALL extern
#endif

/* ------------------ */

typedef struct iocma_t {
  IOstuff  io;
  int      zinfolen;
  real8   *zinfo;
  int      ddr_read_counter;
  integer4 numddrs;
  integer4 lenddrs;
  integer4 numobs;
  integer4 maxreplen;
  integer4 cmalen;
  real8   *ddrs;
  /* For delayed unpacking */
  unsigned int  hdr[PCMA_HDRLEN];
  int           chunk;
} IOcma;

extern IOcma **cmaio;
extern int *cmaio_1stfree;
extern int *cma_perror_flag; /* flags for thread-safe perror()'ing ; used to be handled via cma_perror() */

FORTRAN_CALL void 
cma_open_(integer4       *unit, 
	  const char     *filename,
	  const char     *mode,
	  integer4       *retcode,	 
	  /* Hidden arguments to Fortran-program */
	  int             len_filename,
	  int             len_mode);

FORTRAN_CALL void 
cma_attach_(integer4       *unit, 
	    const char     *filename,
	    const char     *mode,
	    integer4       *retcode,
	    /* Hidden arguments to Fortran-program */
	    int             len_filename,
	    int             len_mode);

FORTRAN_CALL void 
cma_detach_(const integer4 *unit, integer4 *retcode);

FORTRAN_CALL void
cma_read_(const integer4 *unit,
	  real8           pbuf[],
	  const integer4 *kbufsize,
	  integer4       *retcode);

FORTRAN_CALL void 
cma_write_(const integer4 *unit,
	   const real8     pbuf[],
	   const integer4 *kbufsize,
	   integer4       *retcode);

FORTRAN_CALL void 
cma_info_(const integer4 *unit, 
	  integer4        info[],
	  const integer4 *infolen,
	  integer4       *retcode);

FORTRAN_CALL void 
cma_close_(const integer4 *unit, integer4 *retcode);

FORTRAN_CALL void 
cma_debug_(const integer4 *toggle,
	         integer4 *old_value);

FORTRAN_CALL void 
cma_stat_(const integer4 *ftn_unit,
	  const integer4 *toggle, 
   	        integer4 *old_value);

FORTRAN_CALL void
cma_get_report_(const integer4 *unit,
		real8           pbuf[],
		const integer4 *kbufsize,
		integer4       *retcode);

FORTRAN_CALL void
cma_get_ddrs_(const integer4 *unit,
	      real8           ddrs[],
	      const integer4 *lenddrs,
	      integer4       *retcode);

FORTRAN_CALL void
cma_wrapup_(integer4       *retcode);

FORTRAN_CALL void 
cma_prt_stat_(const integer4 *ftn_unit, const integer4 *cma_unit, 
	      const integer4 *binno,   const integer4 *numbins,
	      const integer4 *fileno,
	      const char     *str_open_time, const char *str_close_time,
	      const char     *logical_name,  const char *true_name,
	      const char     *pipecmd,       const char *cmd,
              const integer4 *read_only, const integer4 *packmethod, const integer4 *blocksize,
              const integer4 *numddrs,  const integer4 *lenddrs,
              const integer4 *numobs,   const integer4 *maxreplen, const integer4 *cmalen,
              const integer4 *filesize, const integer4 *filepos, const integer4 *blksize,
              const integer4 *bytes,    const integer4 *num_trans,
              const integer4 *readbuf_len,  const integer4 *readbuf_is_alloc,
	      const integer4 *writebuf_len, const integer4 *writebuf_is_alloc,
	      const integer4 *prealloc, const integer4 *extent,
	      const integer4 *mrfs_flag,
              const real8    *walltime, const real8 *xfer_speed,
              const real8    *usercpu,  const real8 *syscpu,
	      /* Hidden arguments to Fortran-program */
	      int len_str_open_time, int len_str_close_time,
	      int len_logical_name,  int len_true_name,
	      int len_pipecmd,       int len_cmd
	      );

FORTRAN_CALL void 
cma_rewind_(integer4 *unit,
	    integer4 *retcode);

FORTRAN_CALL void 
cma_seek_(const integer4 *unit,
	  const integer4 *koffset, /* 1 unit == sizeof(real8) */
	  const integer4 *kwhence, /* 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END */
	  integer4       *retcode);

FORTRAN_CALL void 
cma_seekb_(const integer4 *unit,
	   const integer4 *koffset, /* 1 unit == sizeof(byte1) */
	   const integer4 *kwhence, /* 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END */
	   integer4       *retcode);

FORTRAN_CALL void 
cma_seeki_(const integer4 *unit,
	   const integer4 *koffset, /* 1 unit == sizeof(integer4) */
	   const integer4 *kwhence, /* 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END */
	   integer4       *retcode);

FORTRAN_CALL void 
cma_seekf_(const integer4 *unit,
	   const integer4 *koffset, /* 1 unit == sizeof(integer4) */
	   const integer4 *kwhence, /* 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END */
	   integer4       *retcode);

FORTRAN_CALL void 
cma_readi_(const integer4 *unit,
	   integer4        pbuf[],
	   const integer4 *kbufsize,
	   integer4       *retcode);

FORTRAN_CALL void 
cma_readf_(const integer4 *unit,
	   real4           pbuf[],
	   const integer4 *kbufsize,
	   integer4       *retcode);

FORTRAN_CALL void 
cma_readb_(const integer4 *unit,
	   byte1           pbuf[],
	   const integer4 *kbufsize,
	   integer4       *retcode);

FORTRAN_CALL void 
cma_readc_(const integer4 *unit,
	   byte1           s[],
	   integer4       *retcode
	    /* Hidden arguments */
	   , int slen);

FORTRAN_CALL void 
cma_writei_(const integer4 *unit,
	    const integer4  pbuf[],
	    const integer4 *kbufsize,
	    integer4       *retcode);

FORTRAN_CALL void 
cma_writef_(const integer4 *unit,
	    const real4     pbuf[],
	    const integer4 *kbufsize,
	    integer4       *retcode);

FORTRAN_CALL void 
cma_writeb_(const integer4 *unit,
	    const byte1     pbuf[],
	    const integer4 *kbufsize,
	    integer4       *retcode);

FORTRAN_CALL void 
cma_writec_(const integer4 *unit,
	    const byte1     s[],
	    integer4       *retcode
	    /* Hidden arguments */
	    , int slen);

FORTRAN_CALL void
cma_bin_info_(const integer4 *unit,
	      integer4       *numbins,
	      integer4       *binfactor);

FORTRAN_CALL void 
cma_bin_file_(const integer4 *unit,
              char *filename,
              integer4 *retcode,
	      /* Hidden argument to Fortran-program */
              int len_filename);

FORTRAN_CALL void 
cma_is_packed_(const integer4 *unit,
	       integer4 *retcode);

FORTRAN_CALL void 
cma_on_mrfs_(const integer4 *unit,
	     integer4 *retcode);

FORTRAN_CALL void 
cma_get_concat_(const integer4 *unit,
		integer4 *retcode);

FORTRAN_CALL void 
cma_get_concat_byname_(const char *filename,
		       integer4 *retcode,
		       /* Hidden arguments to Fortran-program */
		       int len_filename);

FORTRAN_CALL void
cma_flpcheck_(const double d[],
	      const   int *nd,
	              int  flag[],
	      const   int *nf,
	              int *rinf,
	              int *rtiny,
	              int *rnan);

FORTRAN_CALL void
cma_get_byteswap_(const integer4 *unit, integer4 *retcode);

FORTRAN_CALL void
cma_set_byteswap_(const integer4 *unit, 
		  const integer4 *toggle,
		  integer4 *retcode);

FORTRAN_CALL void
cma_set_perror_(const integer4 *onoff, integer4 *retcode);

FORTRAN_CALL void
cma_get_perror_(integer4 *retcode);

FORTRAN_CALL void
cma_is_externally_compressed_(const char *filename,
			      const char *mode,
			      integer4 *retcode,
			      /* Hidden argument to Fortran-program */
			      int len_filename,
			      int len_mode);

void
cma_filesize_(const char *filename,
	      integer4 *retcode,
	      int len_filename);

/* Not Fortran accessible */

#define MAX_CMAIO_LOCKS 3

void cma_set_lock(int lockno);   /* lockno must be [0 .. MAX_CMAIO_LOCKS-1] */
void cma_unset_lock(int lockno); /* lockno must be [0 .. MAX_CMAIO_LOCKS-1] */

int
CMA_fetch_ddrs(const integer4 *unit,
	       IOcma *pcmaio);

FILE *
CMA_get_fp(const integer4 *unit);

int
CMA_get_MAXCMAIO(void);

void
CMA_flpchecker(const char *name,
	       int mode, /* 1=read, 2=write */
	       const char *filename,
	       const double d[], 
	       int filepos, 
	       int *retcode,
	       Boolean *check_it);

#endif
