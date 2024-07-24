#ifndef _IDX_H_
#define _IDX_H_

/* idx.h */

#include "privpub.h"

/* Structures for ODB (row-)indices */

typedef struct {
  double *value; /* ncols (see below) data value(s) for this index */
  int nclusters; /* 
		    idxdata[] could be expressed in this many "clusters" when IDXT (text)-mode :
		    A cluster is either : 
		    (a) A number >= 0
		    (b) start:end (range with increment of 1; start & end inclusive)
		    (c) start:end:inc (same as (b) but increment > 1
		 */
  int idxlen;   /* Count of words occupied in idxdata[], when it's unpacked; usually == ndata */
  int ndata;
  /*
    when idxtype (see below) = 1,
    idxdata[] contains ndata rowid's with all values >= 0 i.e. between [0..nrows-1]

    when idxtype = 2,
    idxdata[] contains a bitmap of ndata unsigned int's, where
    ndata is now determined from (nrows + 32 - 1)/32 [32 = no. of bits in uint]

    if ndata is < 0, then idxdata[] denotes PCMA-packed stream of ndata-words
  */
  unsigned int *idxdata; 
} odbidx_set_t;

typedef struct { /* per pool (=pp) entries */
  ll_t backwd;   /* No. bytes to jump backwards to get to the previous pool */
  ll_t fwd;      /* No. bytes to jump forward to get to the next pool */
  int poolno;    /* pool number in concern */
  int idxtype;   /* type of index : 1=unique, 2=bitmap, -1/-2=unique/bitmap, but not active */
  int nrows;     /* no. of rows in this table (i.e. SELECT count(*) FROM table) */
  int nsets;     /* cardinality i.e. no. of distinct value-sets in this pool */
  odbidx_set_t *idxset;
} odbidx_pp_t;

typedef struct _odbidx_t {
  char *filename;    /* Index file name when file was written (stored in the file itself) */
  char *tblname;     /* table name the index belongs to */
  char *idxname;     /* name of the index */
  char *colnames;    /* comma separated column names */
  char *wherecond;   /* WHERE-condition : NULL if the default i.e. "1" */

  /* Timestamp info */
  yyyymmdd creation_date;
  hhmmss creation_time;

  yyyymmdd modification_date;
  hhmmss modification_time;

  int ncols;         /* How many columns in this index >= 0 && <= MAXBITS (32) */
  int npools;        /* Number of pp-entries */
  unsigned int typeflag; /* 
			    Type flag FOR EACH COLUMN (--> up to MAXBITS-columns) :
			     0 = if not a string
			     1 = if a string (--> no byteswapping)
			  */

  /* The following two, when ncols > 0 */
  int ncard;     /* Global cardinality over *ALL POOLS* (= SELECT DISTINCT cols ... WHERE cond) */
  double *dcard; /* Distinct column values, in row-major order (total ncard rows) ;
		    in total ncols * ncard -values */

  odbidx_pp_t *pp;

  struct _odbidx_t *next; /* for chaining (filekind = 2) */
} odbidx_t;

typedef struct {
  int handle;
  char *dbname;
  int nidx;
  odbidx_t *idx;
} odbidx_main_t;

/* 
   ODB rows index binary (IDXB) / text (IDXT) file format :

   One index file per table covering *ALL* the pools
   (may need some message passing to read/write this file ; not a big problem ?)

   Start of IDXF-block:

     Format is text until the first '\f' (form-feed) is reached

     "IDXB"       - 4-bytes magic word : used also to detect the need for byte-swapping of the block
      or
     "IDXT"       - 4-bytes magic word, if text format is to follow

     Index-file format version number - int (4-bytes)
     Index-file kind - int (4 bytes)
        1 = normal index-file with pool entries etc.
	2 = chained index-file with other index-files as entries (like in iomap-file)

     if (index_file_kind == 2) then

       other_index_file_name_1
       other_index_file_name_2
         ...
       other_index_file_name_N
       End-Of-File

      (each of these text fields precede their length in format "%d")

     else if (index_file_kind == 1) then

     "filename"   - Full name of the index-file, when it was written
     "tblname"    - table name (length up to the next \n)
     "idxname"    - index name (length up to the next \n)
     "colnames"   - comma separated column names (length up to the next \n)

     "WHERE-condition" - "1" if no particular WHERE-condition applied (up to the next form feed \f)

     (each of these text fields precede their length in format "%d")

     index creation date & time     - 2 x 4-byte words
     index modification date & time - 2 x 4-byte words

     ncols        - number of columns in this index >= 0 && <= MAXBITS (32)
     npools       - number of pools covered by this index
     typeflag     - type flag for each column to denote whether it's a string (=1) or not (=0)

     if (ncols > 0) then
       ncard - global cardinality of the columns (i.e. SELECT DISTINCT cols) over all pools
       dcard[] - 8-byte vector of cardinal values : ncols * ncard ; row-major; ncard-rows
     endif

     For the binary file only: ncard is always written, even if it is 0 (i.e. ncols = 0)

     POOL_LOOP: Repeat until poolno below is -1

       The following 4 entries are 4-byte ints all:

       poolno       - pool number in concern
       idxtype      - index type : 1=unique, 2=bitmap, -1 or -2 the same, but not active
       nrows        - number of data rows in the table (in this poolno), SELECT count(*) FROM table
       nsets        - cardinality (no. of distinct value-sets)

       2 x 8-byte integers : for binary these are meaningful, for text -- just for information

         backwd - no. of bytes to jump backward to reach the previous pool in pp-structure
	 fwd    - no. of bytes to jump forward to reach the next pool in pp-structure

       SET_LOOP: Repeat for each set :

         if (ncols > 0) then
           value[]      - ncols values : 
                          all 8-byte long (64-bit flps [b/swap] or 8-byte '<string>' [no b/swap])
	 endif

         ndata        - length of index data vector (4-byte int)
         idxlen       - Original length of idxdata[] before is was (possibly) packed
                        When idxdata[] not packed, idxlen == ndata
	 nclusters    - identifies how many chuncks of idxdata[] grouped together (text-mode only)
         if < 0, indicates that idxdata[] below is PCMA-packed of up to 
	         ABS(ndata) packed words !!
         idxdata[]    - index data vector, unsigned int (when unpacked, its length is idxlen-elements)
   
       End SET_LOOP

     End POOL_LOOP

     -1           - To indicate end of pools has been reached

     endif

   End of IDXF-block

*/

extern int codb_IDXF_drop(void *Info);

extern char *codb_IDXF_filename(Bool create_name,
				const char *idxpath,
				const char *tblname,
				const char *idxname,
				int ncols,
				const char *colnames,
				const char *wherecond,
				const char *filesuffix);

extern char *codb_IDXF_open(int *io_odx, const char *filename, const char *mode);
extern int   codb_IDXF_close(int io_idx, odbidx_t *idx);

extern int codb_IDXF_read(int io_idx, odbidx_t *idx, 
			  int handle, int *poolno_offset, int recur,
			  int oper,
			  const double *rhs);

extern int codb_IDXF_write(int io_idx, odbidx_t *idx, int binary, int pmethod, int use_idxtype);

extern odbidx_pp_t *codb_IDXF_pack(odbidx_t *idx, int poolno, int pmethod, int idxtype, int recur);
extern odbidx_pp_t *codb_IDXF_unpack(odbidx_t *idx, int poolno, int recur);
extern odbidx_pp_t *codb_IDXF_locate_pp(odbidx_t *idx, int poolno, int recur);

extern odbidx_t *
codb_IDXF_create_index(int handle,
		       void *Idx, int poolno, 
		       int idxtype, 
		       void *Info, int *Nrows);

extern void *
codb_IDXF_freeidx(odbidx_t *idx, int recur);

extern odbidx_t *
codb_IDXF_fetch(int handle,
		const char *idxpath,
		const char *tblname,
		const char *idxname,
		int ncols,
		const char *colnames,
		const char *wherecond,
		int oper,
		const double *rhs);

extern void
codb_cardinality_(const int *ncols, const int *nrows, const int *lda,
		  const double a[], /* A Fortran matrix A(LDA,1:NCOLS), LDA >= NROWS */
		  int *rc,          /* cardinality i.e. number of distinct values <= NROWS */
		  int unique_idx[], /* if not NULL, 
				       then Fortran-indices to unique rows of A upon output */
		  const int *nunique_idx, /* Number of elements allocated for unique_idx[] */
		  const int *bailout /* Bailout from cardinality 
					study after this many distinct entries found */
		  );
   
#endif
