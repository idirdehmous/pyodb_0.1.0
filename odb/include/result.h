#ifndef _RESULT_H_
#define _RESULT_H_

/* result.h */

#include "info.h"

/* See info.h for reason why we re-#define the result_t */

#define result_t ODB_result_t
#define uresult_t ODB_uresult_t
#define colinfo_t ODB_colinfo_t

typedef struct _result_t {
  double *mem; /* Allocated (contiguous) memory */
  int nmem;    /* Number of words allocated for *mem :
		  nra * nrows_in for row_wise == true
		  nra * ncols_in for row_wise == false */
  double **d;  /* 2D-array representation of "mem" */
  Bool row_wise; /* data in **d row-wise major when "true", else column-wise (Fortran) */
  int nrows_in;  /* This many rows upon entering ODBc_get_data, usually info->idxlen */
  int ncols_in;  /* This many columns upon entering ODBc_get_data, usually info->ncols */
  int nra;       /* Leading dimension of **d
		    ODBc_lda(ncols_in) for row_wise == true
		    ODBc_lda(nrows_in) for row_wise == false */
  int nrows_out; /* Number of rows upon exiting ODBc_get_data <= nrows_in */
  int ncols_out; /* Number of cols upon exiting ODBc_get_data <= ncols_in */
  int poolno;    /* The pool number in concern */
  info_t *info;  /* Based on this SQL info-structure */
  Bool backup;   /* if "true", then each individual report goes to a backup_file */
  char *backup_file;
  unsigned int typeflag; /* up to MAXBITS columns ; used in context of typeflag of idx.h */
  struct _result_t *next; /* Next result in chain, if applicable */
} result_t;


typedef union {
  void *parse_tree; /* Parse-tree that replaces EvalMe(S2D_<n>) */
  result_t *res;
  double alias;
  int i[2];
} uresult_t;


typedef struct _colinfo_t {
  int id;
  char *name;
  char *nickname;
  char *type_name; /* 19/01/2009 AF typename cannot be used in C++ */
  uint dtnum;
} colinfo_t;

extern void * /* Returns possibly modified dataset & no. of rows printed in *iret */
ODBc_print_data(const char *filename, FILE *fp, void *Result, 
		const char *format, const char *fmt_string,
		int konvert, int write_title, int joinstr,
		int the_first_print, int the_final_print, 
		int *iret);

extern FILE *
ODBc_print_file(FILE *fp_out, int *fpunit,
		const char *outfile, const char *format,
		int poolno, const char *view,
		Bool first_time,
		Bool *Reopen_per_view,
		Bool *Reopen_per_pool,
		Bool *Use_gzip_pipe); /* Returns old fp_out, re-opened fp_out or NULL */

extern void *  /* Returns a ptr to result_t upon success */
ODBc_get_data(int handle, void *Result, void *Info, int poolno, 
	      int begin_row, int end_row, 
	      Bool do_sort, const Bool *row_wise_preference);

extern void *ODBc_get_data_from_binary_file(const char *filename, Boolean sort_unique);

extern void *ODBc_unget_data(void *Result); /* Returns NULL */

extern int ODBc_lda(int m, int method); /* Returns "leading dimension of 'a'" i.e. value >= m */

extern void *   /* sorts + merges the result-chain and returns new result */
ODBc_sort(void *Result, const int Keys_override[], int Nkeys_override);

extern void *   /* Manipulates the result-chain according to the whatkey */
ODBc_operate(void *Result, const char *whatkey);

extern void *ODBc_aggr(void *Result, int phase_id); /* creates result set for aggregate funcs */

extern void * /* Removes duplicates from the given key ("Fortran") -columns and returns
		 a reduced working set */
ODBc_remove_duplicates(void *Result, const int keys[], int n_keys);

extern Bool ODBc_set_kolor_map(const char *cmapfile);
extern Bool ODBc_get_kolor_map(float *value_min, float *value_max, float scale[], 
			       char *text, int textlen);

extern void ODBc_set_format(const char *format);
extern Bool ODBc_test_format_1(const char *test_format);
extern Bool ODBc_test_format_2(const char *format, const char *test_format);
extern Bool ODBc_test_format_3(const char *format, const char *test_format, int len);

extern Bool ODBc_nothing_to_plot(void *Info);
extern int ODBc_ODBtool(FILE *fpin, int *fpunit,
			const char *file, Bool ascii, void *Result, void *Info);

extern void ODBc_DebugPrintRes(const char *when, const result_t *r);

/* Create new result_t set */
extern void *ODBc_new_res(const char *label, const char *file, int lineno,
			  int nrows, int ncols, void *Info, Bool row_wise, int poolno);

/* Merge one or more result_t sets into one and make them "row_wise" */
extern void *ODBc_merge_res(const char *label, const char *file, int lineno,
			    void *Res, int ncols, void *Info, Bool row_wise);

/* Convert to degrees, if applicable */

extern int ODBc_conv2degrees(void *Res, Bool to_degrees);

/* from lib/eq_regions.c */

extern int ODBc_bsearch(const double key,
			const int n, 
			const double x[ /* with n elements */ ],
			const double sign /* +1 forward and -1 for reverse search */);

extern int ODBc_interval_bsearch(const double key, 
				 const int n, 
				 const double x[ /* with n+1 elements */ ],
				 const double *delta, /* if present, only x[0] will be used */
				 const double add,
				 const double sign /* +1 forward and -1 for reverse search */);

#endif /* _RESULT_H_ */

