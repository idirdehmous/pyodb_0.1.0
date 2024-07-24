#ifndef _IO_STUFF_H_
#define _IO_STUFF_H_

/**************************************************************

  iostuff.h: A header file for more organized I/O scheme.
             Fortran-interface.
             Needed only for pure C-applications.

  Author:  Sami Saarinen, ECMWF
           27-Jun-1997

 **************************************************************/


#include <stdio.h>
#include <string.h>
#ifdef USE_MALLOC_H
#include <malloc.h>
#endif
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <ctype.h>
#include <time.h>

#include "alloc.h"
#include "magicwords.h"

#define MAXLINE    4096
#define MAXFILELEN 1024

#define has_percent_sign(s)    (strchr(s,'%') != NULL)
#define has_no_percent_sign(s) (strchr(s,'%') == NULL)

#define FREEIOBUF(x) if (x.p) { FREE(x.p); x.p = NULL; } x.len = 0

#define SETBUF(iobuf) { \
        ALLOC(iobuf->p, iobuf->len+8); \
        if (iobuf->p) setvbuf(fp, (char *)iobuf->p, _IOFBF, iobuf->len); \
        else iobuf->len = 0; }

#define TRIM(s) s ? s : "<not available>"

#define MEGABYTE 1048576

#define IOBINFACTOR 1000
#define IOINDEX(u)  ((u)%IOBINFACTOR)
#define IOBIN(u)    ((u)/IOBINFACTOR)

/* The following MUST BE numeric; sizeof(type) does not do, since used in preprocessing macros */
#define byte_1      1
#define integer_4   4
#define real_8      8

typedef enum { 
  none=0x1, internal=0x2, external=0x4, 
  Zlib=0x8, Mmap=0xF /* these 2 are not implemented yet */
} Compression;

typedef struct iobuf_t {
  int   len;
  byte1 *p;
} IObuf;

typedef struct iotiming_t {
  real8 walltime;
  real8 usercpu;
  real8 syscpu;
} IOtime;

typedef struct iostat_t {
  time_t open_time;
  time_t close_time;
  integer4 bytes;
  integer4 num_trans;
  IOtime t;
} IOstat;

#define NRES 1

typedef struct iobin_t {
  char *true_name;    /* True name after conversion */
  
  FILE *fp;           /* stdio File pointer (if used) */
  integer4 fileno;    /* fileno(fp) or some other index to a file */
  
  integer4 begin_filepos; /* Offset to the actual data (skips the MR2D-section) */

  integer4 filepos;   /* File read/write position (in bytes) */
  integer4 filesize;  /* File size (read only and prior piping); in bytes */

  integer4 blksize;   /* Disk blocksize for a file */
  
  IObuf readbuf;      /* Read I/O-buffer */
  IObuf writebuf;     /* Write I/O-buffer */
  
  integer4 prealloc;  /* Preallocation (primarily used on VPP) */
  integer4 extent;    /* Extent size (primarily used on VPP) */

  IOstat stat;        /* Gather I/O-statistics (if enabled) */

  /* Extra information due to the MR2D (concatenated) file format */
  int  mr2d_infolen;
  unsigned int *mr2d_info;

  /* For external, popen()-based compression schemes only */
  char *pipecmd;      /* Unix command to perform packing/unpacking via popen() */
  char *cmd;          /* Fully blown packcmd with filename etc. substituted into it */

} IObin;


typedef struct iostuff_t {
  char *logical_name; /* File name as given in the open statement */
  char *leading_part; /* Leading part of the true_name (common with any IObin true_names) */

  Boolean is_inuse;    /* true if structure is eventually opened */
  Boolean read_only;   /* true if file is opened for read-only */
  Boolean is_detached; /* true file is left open (detached) for subsequent attachments */
  Boolean on_mrfs;     /* true if the actual file assigned to the memory resident file system */
  Boolean is_mrfs2disk;/* true if file is MR2D-type i.e. created with 'mrfs2disk' or similar */
  Boolean req_byteswap;/* true if data requires byteswap */

  unsigned char scheme; /* An OR'red list of Compression enum's; see above */

  /* For internal, non-popen(), compression schemes only */
  int packmethod;     /* Packing method for internal compression scheme */
  int blocksize;      /* Max. no. of words to pack in one go (specific to internal packing) */

  int concat;          /* Request for concatenation */
  /* int reserved[NRES]; */ /* Reserved for future use */

  integer4 numbins;   /* No. of linked bins where data resides */
  IObin *bin;         /* Chained files */
} IOstuff;

extern int IOgetattr(IOstuff *iostuff);
extern char *IOtruename(const char *name, const int *len_str);
extern int IOgetsize(const char *path, int *filesize, int *blksize);
extern void IOstuff_init(IOstuff *p);
extern void IOstuff_bin_init(IObin *pbin);
extern void IOstuff_reset(IOstuff *p);
extern Boolean IOstuff_byteswap(IOstuff *p, Boolean value);
extern void IOstuff_bin_reset(IObin *pbin);
extern void IOtimes(IOtime *p);
extern char *IOstrdup(const char *str, const int *len_str);
extern char *IOstrdup_int(const char *str, int binno);
extern char *IOstrdup_fmt(const char *fmt, int nproc);
extern char *IOstrdup_fmt2(const char *fmt, int nproc, char *table_name);
extern char *IOstrdup_fmt3(const char *fmt, char *table_name);
extern int  IOstrequ(const char *fmt, const char *str, int *nproc, char *table_name, int fromproc, int toproc);
extern char *IOresolve_env(const char *str);
extern void IOrcopy(real8 out[], const real8 in[], int len);
extern void IOircopy(integer4 out[], const real8 in[], int len);
extern void IOricopy(real8 out[], const integer4 in[], int len);
extern void IOrucopy(real8 out[], const unsigned int in[], int len);
extern void IOicopy(integer4 out[], const integer4 in[], int len);
extern char *IOknowncmd(const char *label, Boolean read_only, Boolean *has_incore, char **suffix, const IObuf *iobuf);
extern int  IOsetbuf(FILE *fp, 
		     const char *filename, 
		     Boolean read_only, 
		     IObuf *iobuf,
		     int *filesize,
		     int *blksize);
extern FILE *IOprealloc(const char *filename,
			int         prealloc,
			int         extent,
			int        *blksize,
			int        *retcode);
extern int IOconcat_byname(const char *filename, int len_filename);
extern int IOmkdir(const char *pathname);
extern Boolean  iostuff_debug;
extern Boolean  iostuff_stat;
extern integer4 iostuff_stat_ftn_unit;

#endif
