#ifndef _DCA_H_
#define _DCA_H_

/* dca.h */

#include "privpub.h"

/*
  #DCA: 
col# colname dtname dtnum file pool# offset length 
     pmethod pmethod_actual nrows nmdis avg 
     min max res1 res2 is_little
   0 seqno@hdr pk1int 279 1/hdr 1 84 6220
     1 1 2145 0 1816064.6037296 
     1 3679992 0 0 0
*/

typedef struct _dca_t {
  int colnum;           /* column number (starting from 0, C-indexing) */
  char *colname;        /* column name (in format "column@table")*/
  char *dtname;         /* data type name */
  uint  dtnum;          /* ODB data type information number (actually of odb_types_t) */
  char *filename;       /* The file name where this column resides ;
			   if begins "/", then the full path, otherwise
			   relative to the $ODB_SRCPATH_<dbname> in concern,
			   or $ODB_SRCPATH or current path, 
			   in this order */
  uint hash;            /* A hash that stems from the colname */
  int poolno;           /* Pool number in concern */
  u_ll_t offset;        /* 64-bit unsigned int offset to column data */
  int length;           /* 32-bit int length of column data */
  int pmethod;          /* Nominal packing method */
  int pmethod_actual;   /* Actual packing method in force */
  int nrows;            /* Number of rows */
  int nmdis;            /* Number of missing data values */
  double avg;           /* Column data average excluding missing data */
  double min;           /* Column data minimum excluding missing data */
  double max;           /* Column data maximum excluding missing data */
  double cr;            /* Compression ratio */
  int ncard;            /* Cardinality */
  int is_little;        /* Column is in little-endian format */
} dca_t;

typedef struct _iodca_t {
  char *file;
  int unit;
  int offset;
  int can_rewind;
} iodca_t;

typedef struct _dca_chain_t {
  /* The current active DCA I/O */
  iodca_t *iodca;
  /* To enchance lookup */
  int   handle;
  int   it;
  char  *dbname;
  char  *tblname;
  /* The DCA-file */
  char  *dcafile;
  /* DCA-datas for this file */
  dca_t *dca;
  int    dcalen;
  /* Cache maximum column number in this DCA-data chunk */
  int    ncols;
  /* Cache min & max pool numbers in this DCA-data chunk */
  int    min_poolno, max_poolno;
  /* Cache min & max hash numbers in this DCA-data chunk */
  uint   min_hash, max_hash;
  /* A pointer to next chunk of DCA-data */
  struct _dca_chain_t *next;
} dca_chain_t;

extern dca_chain_t *
DCA_alloc(int handle,
	  const char *dbname,
	  const char *tblname,
	  int *retcode);

extern int
DCA_free(int handle);

extern void
DCA_debugprt(FILE *fp,
	     int handle,
	     const char *dbname,
	     const char *tblname,
	     const char *colname,
	     int poolno);

extern int
DCA_getsize(int handle,
	    const char *dbname,
	    const char *tblname,
	    int poolno,
	    int *nrows,
	    int *ncols);

extern void *
DCA_fetch(int handle,
	  const char *dbname,
	  const char *tblname,
	  const char *colname,
	  int poolno,
	  int unpack,
	  const int idx[],
	  int idxlen,
	  int *nrows,
	  int *nbytes,
	  uint *datatype,
	  int *offset);

extern double *
DCA_fetch_double(int handle,
		 const char *dbname,
		 const char *tblname,
		 const char *colname,
		 int poolno,
		 const int idx[],
		 int idxlen,
		 int *nrows);

extern int *
DCA_fetch_int(int handle,
	      const char *dbname,
	      const char *tblname,
	      const char *colname,
	      int poolno,
	      const int idx[],
	      int idxlen,
	      int *nrows);

extern
void codb_detect_dcadir_(const char *dbname, 
			 int *io_method_env
			 /* Hidden arguments */
			 , int dbname_len);
			

#endif
