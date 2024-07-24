#include "odb.h"
#include "info.h"
#include "result.h"
#include "idx.h"
#include "evaluate.h"
#include "cdrhook.h"
#include "cargs.h" /* from ifsaux/include/ */

#define USAGE_1 "Usage: %s /dir/database.sch ODB/SQL-query_file begin_row end_row {debug_on=1|off=0} \\\n"
#define USAGE_2 "       {konvert_latlon_to_degrees=1|no_conversion=0} output_format output_file\n"
#define USAGE_3 "       {write_title_header=1|do_not_write=0} {global_row_count_on=1|off=0}\n"
#define USAGE_4 "       {progress_bar_on=1|off=0} {launch_report_viewer_on=1|off=0} cmap-file\n"
#define USAGE_5 "       {fmt_string=[-|%%15.7g (for example)]} {joinstr_on=1|off=0} {comma_separated_varvalues|-}\n"

extern double util_walltime_();

/* The following unit number must be 6 (stdout) and cannot be 0 (for stderr), 
   since stderr is normally re-directed to /dev/null by the odbsql-script */
#define PROGRESS_BAR_IOUNIT 6

#define PROGRESS_BEGIN(zwall) \
{ \
  if (progress_bar) { \
    zwall[0] = util_walltime_(); \
  } \
}

#define PROGRESS_END(zwall, ipoolno) \
{ \
  if (progress_bar) { \
    const int iounit = PROGRESS_BAR_IOUNIT; \
    double wtime; \
    zwall[1] = util_walltime_(); \
    wtime = zwall[1] - zwall[0]; \
    totalrows += nrows; \
    codb_progress_bar_(&iounit, \
		       info->view, \
		       &ipoolno, \
		       &npools, \
		       &nrows, \
		       &totalrows, \
		       &wtime, \
		       NULL \
		       , STRLEN(info->view)); \
  } \
}

#define PROGRESS_INIT() \
{ \
  if (progress_bar) { \
    int ipoolno = 0; \
    int nrows = 0; \
    double zwall[2]; \
    PROGRESS_BEGIN(zwall); \
    codb_set_progress_bar_("totalrows", &totalrows, STRLEN("totalrows")); \
    PROGRESS_END(zwall, ipoolno); \
  } \
}

#define PROGRESS_NL() \
{ \
  if (progress_bar) { \
    const int iounit = PROGRESS_BAR_IOUNIT; \
    codb_progress_nl_(&iounit); \
  } \
}


#define PROGRESS_FINISH() \
{ \
  if (progress_bar) { \
    const int iounit = PROGRESS_BAR_IOUNIT; \
    const int newline = 1; \
    double wtime = util_walltime_(); \
    wtime -= walltot[0]; \
    codb_progress_bar_(&iounit, \
		       info->view, \
		       &npools, \
		       &npools, \
		       &totalrows, \
		       &totalrows, \
		       &wtime, \
		       &newline \
		       , STRLEN(info->view)); \
  } \
}


PRIVATE void
Launch(const char *outfile, const char *viewname, Bool reporter, Bool warrow)
{
  char *env = reporter ? getenv("ODB_REPORTER") : getenv("ODB_PLOTTER");
  if (!env) {
    if (reporter) 
      env = "$ODB_FEBINPATH/b4.x";
    else if (warrow)
      env = "$ODB_FEBINPATH/plotobs.x -s -D$MAGICS_DEVICE -W -b";
    else
      env = "$ODB_FEBINPATH/plotobs.x -s -D$MAGICS_DEVICE -b";
  }
  if (!strequ(env,"0") && !strequ(env,"/dev/null")) {
    int len = STRLEN(env) + STRLEN(outfile) + 10;
    int viewlen = STRLEN(viewname);
    if (viewlen == 0) {
      if (access(outfile,R_OK) == 0) {
	char *cmd = NULL;
	ALLOC(cmd, len);
	snprintf(cmd, len, "%s%s%s &", env, reporter ? " " : "", outfile);
	if (!reporter) {
	  char *magics_device = getenv("MAGICS_DEVICE");
	  if (!magics_device) {
	    magics_device = STRDUP("MAGICS_DEVICE=JPEG");
	    putenv(magics_device);
	    /* do NOT FREE(magics_device) !! */
	  }
	} /* if (!reporter) */
	(void) system(cmd);
	FREE(cmd);
      }
      else {
	fprintf(stderr,
		"***Warning in Launch(outfile=%s, viewname=%s, reporter=%s): "
		"Cannot access the file '%s'\n",
		outfile, viewname ? viewname : NIL, reporter ? "true" : "false",
		outfile);
      }
    }
    else {
      char *ppfile = NULL;
      len += viewlen;
      ALLOC(ppfile, len);
      snprintf(ppfile, len, outfile, viewname);
      Launch(ppfile, NULL, reporter, warrow);
      FREE(ppfile);
    }
  }
}


PUBLIC void
odbsql_(int *iret)
{
  int rc = 0;
  DRHOOK_START(odbsql);
  {
    int argc = ec_argc();
    char **argv = ec_argv();
    char *dbname = NULL;
    char *sql_query_file = NULL;
    int begin_row = 1;
    int end_row = 0;
    int debug_on = false;
    int konvert = 0;
    char *format = "default";
    char *outfile = "/dev/null";
    int write_title = 1;
    int global_row_count = 0;
    int progress_bar = 0;
    int launch_b4x = 0;
    int launch_plotobs = 0;
    char *cmapfile = "/dev/null";
    char *fmt_string = NULL;
    int joinstr = 0;
    char *varvalue = NULL;
    int nsetvar = 0;
    set_t *setvar = NULL;

    int numargs = argc-1;
    
    if (numargs != 16) {
      fprintf(stderr,USAGE_1,argv[0]);
      fprintf(stderr,USAGE_2);
      fprintf(stderr,USAGE_3);
      fprintf(stderr,USAGE_4);
      fprintf(stderr,USAGE_5);
      rc = -argc;
      goto finish;
    }
    else {
      int npools = 0;
      int ntables = 0;
      int handle = 0;
      int save_begin_row;
      int save_end_row;
      int save_write_title;
      int save_global_row_count;
      int save_progress_bar;
      Bool need_global_view;
      Bool is_odb, is_plotobs, is_binary, is_odbtool, warrow, is_geo;
      Bool is_netcdf;
      Bool trigger_odbtool_write = false;

      dbname = argv[1];
      sql_query_file = argv[2];
      begin_row = atoi(argv[3]);
      end_row = atoi(argv[4]);
      debug_on = atoi(argv[5]);
      konvert = atoi(argv[6]);
      format = argv[7];
      outfile = argv[8];
      write_title = atoi(argv[9]);
      global_row_count = atoi(argv[10]);
      progress_bar = atoi(argv[11]);
      launch_b4x = atoi(argv[12]);
      cmapfile = argv[13];
      fmt_string = argv[14];
      joinstr = atoi(argv[15]);
      varvalue = strequ(argv[16],"-") ? NULL : argv[16];

      setvar = ODBc_make_setvars(varvalue, &nsetvar);

      save_begin_row = begin_row;
      save_end_row = end_row;
      save_write_title = write_title;
      save_global_row_count = global_row_count;
      save_progress_bar = progress_bar;

      if (debug_on) (void) ODBc_debug_fp(stderr);
      
      handle = ODBc_open(dbname, "r", &npools, &ntables, NULL);
      
      ODB_fprintf(ODBc_get_debug_fp(),
		  "%s: handle = %d, npools = %d, ntables = %d\n",
		  dbname, handle, npools, ntables);

      ODBc_set_format(format);

      is_odb = ODBc_test_format_1("odb");
      is_plotobs = ODBc_test_format_1("plotobs");
      warrow = ODBc_test_format_1("wplotobs");
      is_binary = (ODBc_test_format_1("binary") || ODBc_test_format_1("bindump"));
      is_odbtool = ODBc_test_format_1("odbtool");
      is_geo = ODBc_test_format_3(NULL, "geo", 3);
      is_netcdf = ODBc_test_format_1("netcdf");

      if (handle >= 1) {
	int npools_saved = npools;
	int *poolnos = NULL;
	Bool reopen_per_view = false;
	Bool reopen_per_pool = false;
	Bool use_gzip_pipe = false;
	FILE *fp_out = NULL;
	int fpunit = -1;
	int ip;
	info_t *info = ODBc_sql_prepare_via_sqlfile(handle, sql_query_file, setvar, nsetvar);

	/* The following returns the effective list of pools in poolmask
	   and the length of this list == npools i.e. npools gets changed */
	poolnos = ODB_get_permanent_poolmask(handle, &npools);

	if (npools == 0) {
	  rc = ODBc_close(handle);
	  goto finish;
	}

	/* Obtain initial values for fp_out and for Bool-variables
	   reopen_per_view, reopen_per_pool & use_gzip_pipe */

	fp_out = ODBc_print_file(fp_out, &fpunit,
				 outfile, format,
				 -1, NULL,
				 true,
				 &reopen_per_view,
				 &reopen_per_pool,
				 &use_gzip_pipe);

	if (launch_b4x) {
	  if (fp_out == stdout || 
	      reopen_per_pool || use_gzip_pipe ||
	      is_binary || is_plotobs || warrow ||
	      strequ(outfile,"/dev/null")) {
	    launch_b4x = 0;
	  }
	}

	launch_plotobs = 0;
	if ((is_plotobs || warrow) && fp_out != stdout &&
	    !reopen_per_pool && !reopen_per_view &&
	    !strequ(outfile,"/dev/null")) {
	  launch_plotobs = 1;
	}

	if ((is_plotobs || warrow) && !strequ(cmapfile,"/dev/null")) {
	  (void) ODBc_set_kolor_map(cmapfile);
	}

	/* Ignore ORDERBY-clause when plotting */ 
	if ((is_plotobs || warrow) && info->o && info->norderby > 0) info->norderby  = -info->norderby;

	if (info->need_global_view ||
	    is_netcdf) {
	  need_global_view = true;
	}
	else {
	  need_global_view = false;
	}

	if (progress_bar) {
	  if (debug_on) progress_bar = 0;
	  else if (fp_out == stdout && !need_global_view) progress_bar = 0;
	  save_progress_bar = progress_bar;
	}

	if (progress_bar) {
	  codb_set_progress_bar_("maxpoolno", &npools, STRLEN("maxpoolno"));
	}
      
	while (info) {
	  int the_first_print = 1;
	  double walltot[2];
	  void *next_info = ODBc_next(info);
	  result_t *res = NULL;
	  int totalrows = 0;
	  const char *ascii_file = is_odbtool ? "temp_ascii.000001" : NULL;
	  info_t *infoaux = NULL;
	  int create_index = info->create_index;
	  odbidx_t *stored_idx = NULL;
	  Bool is_bc = false; /* In case used as basic calculator (bc) e.g. SELECT 1+2*3 */

	  walltot[0] = util_walltime_();

	  ODBc_print_info(ODBc_get_debug_fp(), info);

	  if ((is_plotobs || warrow || is_odbtool || is_geo) && ODBc_nothing_to_plot(info)) {
	    /* Check if $ODB_LAT & $ODB_LON present in SELECT-columns */
	    goto next;
	  }

	  if (is_odbtool && info->ncols_true < 4) {
	    /* Consider nothing to output for ODB-tool */
	    goto next;
	  }

	  if (create_index < 0) {
	    /* 'DROP INDEX name' or 'DROP INDEX *' */
	    (void) codb_IDXF_drop(info);
	    goto next;
	  }
	  else if (create_index > 0) {
	    /* 'CREATE [UNIQUE|DISTINCT|BITMAP] INDEX name ...' */
	    infoaux = ODBc_create_index_prepare(handle, info, NULL);
	    if (infoaux) {
	      stored_idx = ODBc_create_index_prepare(handle, info, infoaux);
	    }
	  }

	  if (is_odbtool) trigger_odbtool_write = true; /* Triggers write of the ASCII-file */

	  fp_out = ODBc_print_file(fp_out, &fpunit,
				   outfile, format,
				   -1, info->view,
				   false,
				   &reopen_per_view,
				   &reopen_per_pool,
				   &use_gzip_pipe);

	  /* Ignore ORDERBY-clause when plotting */ 
	  if ((is_plotobs || warrow) && info->o && info->norderby  > 0) 
	    info->norderby  = -info->norderby;

	  if (info->need_global_view ||
	      is_netcdf) {
	    need_global_view = true;
	  }
	  else {
	    need_global_view = false;
	  }

	  begin_row = save_begin_row;
	  end_row = save_end_row;
	  write_title = save_write_title;
	  global_row_count = save_global_row_count;
	  progress_bar = save_progress_bar;

	  if (info->is_bc) {
	    /* This must be a basic calculator (bc) mode !! */
	    is_bc = true;
	    progress_bar = 0; /* Switch progress bar off in bc-mode */
	    begin_row = end_row = global_row_count = 1; /* Expect just one result row */
	  }
	  else {
	    is_bc = false;
	  }

	  if (info->need_hash_lock) {
	    /* UNIQUEBY-clause present (or has SELECT DISTINCT) */
	    codb_hash_set_lock_();
	    codb_hash_init_();
	  }

	  if (global_row_count && need_global_view && !is_netcdf) global_row_count = 0;
	  
	  PROGRESS_INIT();

	  for (ip=0; ip<npools; ip++) {
	    /* This is a loop over effective pools i.e. those which
	       were left in the poolmask */
	    int jp = poolnos[ip];
	    DEF_IT; /* defines variable "it" */
	    double wall[2];
	    PROGRESS_BEGIN(wall);
	    if (create_index == 0) {
	      int nrows = 0;
	      if (info->has_thin) codb_thin_reset_(&it);
	      nrows = ODBc_sql_exec(handle, info, jp, &begin_row, &end_row);
	      if (nrows > 0) {
		const Bool row_wise_preference = true;
		res = ODBc_get_data(handle, res, info, jp, begin_row, end_row, false, &row_wise_preference);
		PROGRESS_END(wall, ip);
		if (!need_global_view && res) {
		  fp_out = ODBc_print_file(fp_out, &fpunit,
					   outfile, format,
					   jp, info->view,
					   false,
					   &reopen_per_view,
					   &reopen_per_pool,
					   &use_gzip_pipe);
		  res = ODBc_print_data(outfile, fp_out, res,
					format, fmt_string,
					konvert, write_title, joinstr,
					the_first_print, (ip == npools-1) ? 1 : 0, 
					NULL);
		  the_first_print = 0;
		  if (is_odbtool && trigger_odbtool_write) {
		    (void) ODBc_ODBtool(NULL, NULL, ascii_file, true, res, info);
		    trigger_odbtool_write = false;
		  }
		  else if (!is_odb) write_title = 0;
		  res = ODBc_unget_data(res);
		}
		if (global_row_count) end_row -= nrows; /* This many rows left to retrieve */
	      } /* if (nrows > 0) */
	      else if (nrows == 0) {
		PROGRESS_END(wall, ip);
	      }
	      if (info->has_thin) codb_thin_reset_(&it);
	      info = ODBc_reset_info(info);
	      if (global_row_count && end_row <= 0) break; /* Stop scanning now */
	    }
	    else if (stored_idx) { /* Handling of CREATE [UNIQUE|BITMAP] INDEX -stuff */
	      int nrows = 0;
	      stored_idx = codb_IDXF_create_index(handle,
						  stored_idx, jp, 
						  create_index, 
						  infoaux, &nrows);
	      infoaux = ODBc_reset_info(infoaux);
	      PROGRESS_END(wall, ip);
	    }
	  } /* for (ip=0; ip<npools; ip++) */

	  if (stored_idx) {
	    int io_idx = -1;
	    char *wherecond = S2D_fix(info, stored_idx->wherecond);
	    char *filename = codb_IDXF_filename(true,
						info->ph->idxpath, 
						stored_idx->tblname, 
						stored_idx->idxname,
						info->ncols_true,
						stored_idx->colnames,
						wherecond,
						".gz");
	    char *true_filename = codb_IDXF_open(&io_idx, filename, "w");
	    if (true_filename && io_idx >= 0) {
	      int rc_idx;
	      stored_idx->filename = true_filename;
	      FREE(stored_idx->wherecond);
	      stored_idx->wherecond = wherecond;
	      rc_idx = codb_IDXF_write(io_idx, stored_idx, 
				       info->binary_index ? 1 : 0,
				       0, /* PCMA-packing method; 0 = no packing */
				       0  /* idxtype; change idxtype from 1 to 2 or vice versa */
				       );
	      (void) codb_IDXF_close(io_idx, NULL);
	      stored_idx = codb_IDXF_freeidx(stored_idx, 0);
	    }
	    else {
	      fprintf(stderr, 
		      "***Warning: Unable to open index file '%s' for writing\n",
		      filename);
	    }
	    FREE(filename);
	  }

	  if (info->need_hash_lock) {
	    /* UNIQUEBY-clause present (or has SELECT DISTINCT) */
	    codb_hash_init_();
	    codb_hash_unset_lock_();
	  }

	  if (need_global_view && res) {
	    if (info->has_aggrfuncs) res = ODBc_aggr(res, 1);
	    res = ODBc_operate(res, "count(*)");
	    res = ODBc_sort(res, NULL, 0);
	    if (is_netcdf) res = ODBc_merge_res("Final merge for NetCDF",
						__FILE__, __LINE__,
						res, res->ncols_out, info, false);
	    if (!launch_b4x) PROGRESS_NL();
	    res = ODBc_print_data(outfile, fp_out, res,
				  format, fmt_string,
				  konvert, write_title, joinstr,
				  the_first_print, 1, 
				  NULL);
	    if (progress_bar) {
	      int nrows = res->nrows_out;
	      totalrows = 0;
	      PROGRESS_END(walltot, npools);
	    }
	    if (is_odbtool && trigger_odbtool_write) {
	      (void) ODBc_ODBtool(NULL, NULL, ascii_file, true, res, info);
	      trigger_odbtool_write = false;
	    }
	    res = ODBc_unget_data(res);
	  }

	  PROGRESS_FINISH();

	  if (launch_b4x && reopen_per_view) {
	    if (fp_out) fflush(fp_out);
	    Launch(outfile, info->view, true, false);
	  }
	  
	next:

	  if (infoaux) (void) ODBc_sql_cancel(infoaux);

	  info = ODBc_sql_cancel(info);
	  info = next_info;
	} /* while (info) */

	/* Close the print file */
	fp_out = ODBc_print_file(fp_out, &fpunit,
				 outfile, format,
				 -1, NULL,
				 false,
				 &reopen_per_view,
				 &reopen_per_pool,
				 &use_gzip_pipe);

	if (launch_b4x && !reopen_per_view) {
	  Launch(outfile, NULL, true, false);
	}

	if (launch_plotobs) {
	  Launch(outfile, NULL, false, warrow);
	}

	FREE(poolnos);

	rc = ODBc_close(handle);
      }
      else { /* if (handle >= 1) ... */
	rc = -255;
      }
    }
  }
 finish:
  (void) ODBc_set_kolor_map(NULL);
  DRHOOK_END(rc);
  if (iret) *iret = rc;
}
  
void odbsql(int *iret) { odbsql_(iret); }

