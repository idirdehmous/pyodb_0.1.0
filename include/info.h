#ifndef _INFO_H_
#define _INFO_H_

/* info.h */

#include "alloc.h"
#include "pcma_extern.h"
#include "idx.h"

/* Since IBM Power5+ was complaining about "col_t" being already defined,
   then lets not fall into the same trap on any other platforms */

#define DB_t            ODB_DB_t
#define set_t           ODB_set_t
#define str_t           ODB_str_t
#define col_t           ODB_col_t
#define table_t         ODB_table_t
#define simple_where_t  ODB_simple_where_t
#define dyn_t           ODB_dyn_t
#define info_t          ODB_info_t
#define dbcred_t        ODB_dbcred_t

static const double mdi = ABS(RMDI);

//#define NLINKCASES 6
//static const char *linkcase_name[1+NLINKCASES] = {
//  "<undef>"
//  ,"(1) ONELOOPER - link"            /* 1 */
//  ,"(2) ALIGNed table"               /* 2 */
//  ,"(3) ALIGNed, but orphaned table" /* 3 */
//  ,"(4) A regular link"              /* 4 */
//  ,"(5) First table in FROM-stmt"    /* 5 */
//  ,"(6) Lonely, disconnected table"  /* 6 */
//};

#define BEGIN_ROW "$begin_row#"
#define END_ROW "$end_row#"

typedef struct _DB_t {
  int h;
  char *dbname;
  char *srcpath;
  char *datapath;
  char *idxpath;
  char *poolmask;
  int npools;
  int ntables;
  char **tblname;
} DB_t;


typedef struct _table_t {
  struct _info_t *info;
  int table_id;
  char *name;
  char *b;
  char *e;
  int linkparent_id;
  int offset_parent_id;
  struct _table_t *prev;
  struct _table_t *next;
  struct _table_t *linkparent;
  struct _table_t *offset_parent;
  char *linkoffset;
  char *linklen;
  int jr;
  int lo;
  int ob;
  int linkcase;
  linkoffset_t *offset;
  linklen_t *len;
  int nrows, ncols;
  int *idx;
  char *wherecond_and;
  void *wherecond_and_ptree;
  Bool simple_wherecond;
  byte *skiprow;
  int nwl;
  struct _simple_where_t *wl;
  int stored_idx_count;
  Bool in_select_clause;
} table_t;

typedef struct _dyn_t {
  char *name;
  double *data;
  int ndata;
  int colid;
  table_t *parent;
  table_t *child;
} dyn_t;

typedef struct _col_t {
  char *name;
  char *fetch_name;
  char *nickname;
  char *dtype;
  uint dtnum;
  int kind;
  int bitpos;
  int bitlen;
  int ioffset;
  int table_id;
  table_t *t;
  double *dsym;
  double *dinp;
  int dinp_len;
  Bool dinp_alloc;
  void *formula_ptree;
  Bool formula_ptree_owner;
} col_t;

typedef struct _simple_where_t {
  char *condstr;
  col_t *wcol;
  char *lhs;
  int oper;
  double rhs;
  odbidx_t *stored_idx; /* via CREATE INDEX ... ON table ... , if found */
} simple_where_t;

typedef struct _set_t {
  char *name;
  double value;
} set_t;

typedef struct _str_t {
  char *name;
  char *value;
  S2D_Union u;
} str_t;

typedef struct _dbcred_t {
  char *dbname;   /* current database name <dbname> */
  char *srcpath;  /* ODB_SRCPATH_<dbname>           */
  char *datapath; /* ODB_DATAPATH_<dbname>          */
  char *idxpath;  /* ODB_IDXPATH_<dbname>           */
  char *poolmask; /* ODB_PERMANENT_POOLMASK         */
} dbcred_t;

typedef struct _info_t {
  char *dir;
  char *host;
  char *tstamp;

  dbcred_t dbcred;

  char *view;
  char *sql_query;

  int create_index; /* 
		       -1 = DROP INDEX
		        0 = Normal SELECT stmt, 
		        1 = CREATE UNIQUE INDEX, 
		        2 = CREATE BITMAP INDEX 
		    */
  Bool binary_index; /* If binary index (file) is meant to be created */
  Bool use_indices;
  char *use_index_name; /* Usually "*" i.e. any */

  Bool has_select_distinct;
  Bool has_count_star;
  Bool has_aggrfuncs;
  Bool has_thin;
  Bool has_ll; /* True if has refs to lldegrees()/llradians() */

  Bool need_global_view; /* Usually an orderby/select distinct/aggregate-func view */
  Bool need_hash_lock; /* Has select distinct or unique by keywords */
  Bool is_bc; /* Basic calculator : no table refs and just one line of output expected */

  uint optflags; /* 
		    0x1 : SELECT count(*) FROM table
		 */

  int latlon_rad; /* 
		     Affects values returned by lldegrees()/llradians()
		     Legal values of $ODB_LATLON_RAD :
		     -1 : undefined
		          -> lldegrees() behaves like degrees() i.e. does conversion to degrees
		          -> llradians() behaves like radians() i.e. does conversion to radians
		      0 : $ODB_LAT/$ODB_LON assumed to be in degrees
		          -> lldegrees() does no conversion
		          -> llradians() does conversion to radians
		      1 : $ODB_LAT/$ODB_LON assumed to be in radians
		          -> lldegrees() does conversion to degrees
		          -> llradians() does no conversion
		  */

  col_t *odb_lat;   /* getenv("ODB_LAT"); */
  col_t *odb_lon;   /* getenv("ODB_LON"); */
  col_t *odb_date;  /* getenv("ODB_DATE"); */
  col_t *odb_time;  /* getenv("ODB_TIME"); */
  col_t *odb_color; /* getenv("ODB_COLOR"); */
  col_t *odb_u;     /* getenv("ODB_U"); */
  col_t *odb_v;     /* getenv("ODB_V"); */

  int error_code;
  int maxcols;
  int npools;

  Bool dca_alloc;

  int idxalloc;
  int idxlen;
  double *idxlen_dsym; /* aka ROWNUM */

  double *__maxcount__; /* Traces ODBTk's $__maxcount__ efficiently */

  int nset;
  set_t *s;

  int ns2d;
  str_t *s2d;

  col_t *c;
  int ncols; /* total number of columns to be dealt with when executing SELECT */
  int ncols_true; /* True number of columns i.e. ncols_pure + ncols_formula + ncols_aggr_formula */
  int ncols_pure; /* no formula, pure column (kind=1) */
  int ncols_formula; /* simple formula (kind=2) */
  int ncols_aggr_formula; /* aggregate formula (kind=4) */
  int ncols_aux; /* auxiliary cols needed due to aggregate functions (kind=8) */
  int ncols_nonassoc; /* column variables needed to be prefetched due to formula(s) (kind=0) */ 

  int ncolaux; /* Should be == ncols_true + ncols_aux */
  int *colaux;

  col_t *u;
  int nuniqueby;

  table_t *t; /* full FROM-hierarchy; chain length = nfrom */
  int nfrom;

  char *wherecond;
  void *wherecond_ptree; /* Used for table-less WHERE only */

  col_t *w;
  int nwhere; /* no. of columns that need to be fetched due to WHERE-condition */

  col_t *o;
  int norderby;

  col_t *p;
  int nprefetch; /* additional columns to be prefetched due to LINKs */

  /* dynamic @LINK-lengths for use by "$<parent_tblname>.<child_tblname>#" -variables */
  int ndyn;
  dyn_t *dyn;

  DB_t *ph; /* A back-reference to DB_t structure this info_t belongs to */

  struct _info_t *next; /* Another SQL info */
} info_t;


extern int ODBc_open(const char *dbname, 
		     const char *mode, int *npools, int *ntables,
		     const char *poolmask); /* Returns handle >= 1 if all ok */
extern int ODBc_close(int handle); /*  Returns 0 if all ok */
extern const char **ODBc_get_tablenames(int handle, int *ntables);

extern FILE *ODBc_debug_fp(FILE *fp_debug); /* Returns old debug file pointer */
extern FILE *ODBc_get_debug_fp(); /* Returns current debug file pointer */

extern void *ODBc_sql_prepare(int handle, const char *sql_query, const set_t *setvar, int nsetvar); /* Returns ptr to info_t upon success */
extern void *ODBc_sql_prepare_via_sqlfile(int handle, const char *sql_query_file, const set_t *setvar, int nsetvar); /* Returns ptr to info_t upon success */

extern void *ODBc_create_index_prepare(int handle, /* A tool to handle CREATE INDEX */
				       const void *Info, const void *Infoaux);  
extern int ODBc_fetch_indices(table_t *t, info_t *info);

extern int  /* Returns no. of rows >= 0 matched if all ok */
ODBc_sql_exec(int handle, void *Info, int poolno, int *Begin_row, int *End_row);

extern void *ODBc_get_info(const char *info_file_or_pipe, const set_t *setvar, int nsetvar); /* Returns ptr to info_t upon success */
extern void  ODBc_print_info(FILE *fp, void *Info);
extern void *ODBc_reset_info(void *Info); /* Returns Info */
extern void *ODBc_sql_cancel(void *Info); /* Returns NULL */

extern const char *ODBc_get_sql_query(const void *Info); /* Returns SQL-query in concern */

extern void *ODBc_make_setvars(const char *varvalue, int *nsetvar);

extern void *ODBc_next(void *Info); /* Returns the next SQL info in chain (or NULL) */

extern void ODBc_vget_bits(double x[], int nx, int pos, int len); /* Gets bits pos:pos+len-1 */

extern void *ODBc_get_free_handles(int *Maxhandle); /* returns ptr to free_handles */

extern int ODBc_get_handle(const char *db);

#endif /* _INFO_H_ */

