/* odbi_direct_main.c */

/* Note: This is a common code with odbi_client_main.c 
         with only a few exceptions via #ifdef's */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <math.h>
#include <unistd.h>
#include <sys/types.h>

/* Choose between true direct-mode or client[/server]-mode */

#if !defined(ODBI_DIRECT)
#define ODBI_DIRECT 1
#endif

#include "odbi.h"
#include "privpub.h"
#include "odbcsdefs.h"
#include "cdrhook.h"

int ODBI_errno = 0;

#define DEVNULL "/dev/null"

char *ncpus_nchunk = NULL; /* global */

#if ODBI_DIRECT == 1

#define FLAGS "b:B:CDf:Fl:L:mn:Nq:Q:Sv:V:#:"

static const int direct = 1;
int fast_odbsql = 1; /* global */
int odb_version = 0; /* global */
int csopt = 0; /* global */

#define USAGE \
  "Usage: %s\n" \
  "       [-b format]        (output data in binary format; default:text)\n" \
  "       [-B bufsize]       (working buffer size; default: at least no. of columns)\n" \
  "       [-D]               (return data only i.e. from ODBI_fetch_row_array())\n" \
  "       [-l database_name] (default: ECMA)\n" \
  "       [-L start_row,maxrows] (default: 1,-1 i.e. all via SQL)\n" \
  "       [-m]               (obtain metadata only)\n" \
  "       [-N]               (no NULLs, but the value of missing data [in text-mode]; default: print NULL)\n" \
  "       [-S]               (show row numbers -- in text-mode output only)\n" \
  "       [-v viewname]      (default:myview)\n" \
  "       [-V var=value]     (supply changes to the default $-variables; may appear multiple times)\n" \
  "       [-F]               (do *NOT* use Fast odbsql-approach [by default it is used])\n" \
  "       [-n numeric_odb_version] (as produced by command odb_version -numeric)\n" \
  "       [-q sql_data_query]\n" \
  "       [-Q sql_data_query_file]\n" \
  "       [-f output_file_name] (default: /dev/null i.e. do not use files)\n" \
  "       [-C]               (odbi_direct.x invoked via c/s-infrastructure)\n" \
  "       [-# ncpus[.nchunk]] (Parallelism (and pool-chunking), default: ncpus=1)\n" \
  "\n"

#else /* ODBI_DIRECT == 0 */

#define FLAGS "b:B:Df:H:i:l:L:mNO:p:P:q:Q:T:Sv:V:#:"

static const int direct = 0;
static int fast_odbsql = 1; /* local; dummy */
static int odb_version = 0; /* local; dummy */
static int csopt = 0; /* local; dummy */

#define USAGE \
  "Usage: %s\n" \
  "       [-b format]        (output data in binary format; default:text)\n" \
  "       [-B bufsize]       (working buffer size; default: at least no. of columns)\n" \
  "       [-D]               (return data only i.e. from ODBI_fetch_row_array())\n" \
  "       [-l database_name] (default: ECMA)\n" \
  "       [-L start_row,maxrows] (default: 1,-1 i.e. all via SQL)\n" \
  "       [-m]               (obtain metadata only)\n" \
  "       [-N]               (no NULLs, but the value of missing data [in text-mode]; default: print NULL)\n" \
  "       [-S]               (show row numbers -- in text-mode output only)\n" \
  "       [-v viewname]      (default:myview)\n" \
  "       [-V var=value]     (supply changes to the default $-variables; may appear multiple times)\n" \
  "== Specific to client/server-version only:\n" \
  "       [-l [host:][/datapath/]database_name]\n" \
  "       [-H {hostname|ip-address}]      (default: {localhost|127.0.0.1})\n" \
  "       [-P server_port_number]         (default: mod(uid,10000)+10000)\n" \
  "       [-T timeout_in_secs]            (default: 3600)\n" \
  "       [-i [host:]/datapath]           (default: /tmp)\n" \
  "       [-q sql_data_query]\n" \
  "       [-Q sql_data_query_file]\n" \
  "       [-p poolmask]\n" \
  "       [-O options_to_be_propagated_to_ODBI_connect]\n" \
  "       [-f output_file_name] (default: /dev/null i.e. do not use files)\n" \
  "       [-# ncpus[.nchunk]] (Parallelism (and pool-chunking), default: ncpus=1)\n" \
  "\n"

#endif

typedef struct _varvalue_t {
  char *var;
  double value;
  struct _varvalue_t *next;
} varvalue_t;

static varvalue_t *varlist_start = NULL;
static varvalue_t *varlist = NULL;


PRIVATE char *Slurp(const char *path) /* As in odb/compiler/odb98.c */
{
  extern int IOgetsize(const char *path, int *filesize, int *blksize);
  int filesize = 0;
  int rc = IOgetsize(path, &filesize, NULL);
  char *s = NULL;
  if (rc == 0 && filesize > 0) {
    FILE *fp = fopen(path,"r");
    if (fp) {
      s = malloc(sizeof(*s) * (filesize+20));
      if (s) {
	rc = fread(s, sizeof(*s), filesize, fp); /* slurp!! */
	if (rc == filesize) { /* Success */
	  s[filesize] = '\0'; 
	  strcat(s," ;\n");
	}
	else free(s); /* Failure */
      }
      fclose(fp);
    }
  }
  if (!s) {
    fprintf(stderr,"***Error: Unable to inline non-existent (or empty) ODB/SQL-file '%s'\n",
	    path ? path : NIL);
    raise(SIGABRT);
    _exit(1);
  }
  return s;
}

int
main(int argc, char *argv[])
{
  int c, len, errflg = 0;
  char *p;
  int is_binary = 0;
  char *output_file = strdup(DEVNULL);
  char *output_format = strdup("text");
  int bufsize = 0; /* will be set to at least "ncols" */
  int data_only = 0;
  char *dbname = NULL;
  int print_nulls = 1;
  char *viewname = NULL;
  int ntot = 0;
  int start_row = 1;
  int maxrows = 2147483647;
  int showrows = 0;
  int obtain_metadata_only = 0;
  char *server_host = NULL;
  int server_port = 0;
  int server_timeout = TIMEOUT_DEFAULT;
  int server_timeout_given = 0;
  char *server_datapath = NULL;
  char *data_query = NULL;
  char *options = /* compulsory parameters for C/S */
    strdup("fp=%F;host=%s;port=%d;timeout=%d;datapath=%s;poolmask=%s;ncpus=%s"); 
  char *poolmask = strdup("");
  FILE *fp = stdout;
  FILE *fp_data = fp;
  double time_start = ODBI_timer(NULL);
  void *db = NULL;
  int verbose = 0;
  DRHOOK_START(main);

  while ((c = getopt(argc, argv, FLAGS)) != -1) {
    switch (c) {
    case 'b': /* binary mode */
      p = optarg;
      if (output_format) free(output_format);
      output_format = strdup(p);
      is_binary = (strcmp(output_format,"text") != 0) ? 1 : 0;
      break;
    case 'B': /* buffer size */
      p = optarg;
      bufsize = atoi(p);
      break;
    case 'C': /* invoked via c/s-infrastructure */
      csopt = 1;
      break;
    case 'D': /* return data only */
      data_only = 1;
      break;
    case 'f':
      p = optarg;
      if (output_file) free(output_file);
      output_file = strdup(p);
      break;
    case 'F': /* switch off fast odbsql */
      fast_odbsql = 0;
      break;
    case 'H': /* server host */
      p = optarg;
      if (server_host) free(server_host);
      server_host = strdup(p);
      break;
    case 'i': /* server datapath */
      p = optarg;
      if (server_datapath) free(server_datapath);
      server_datapath = strdup(p);
      break;
    case 'l': /* database name */
      p = optarg;
      if (dbname) free(dbname);
      dbname = strdup(p);
      break;
    case 'L': /* start_row,nrows */
      p = optarg;
      {
	int num = sscanf(p,"%d,%d",&start_row,&maxrows);
	if (num != 2) {
	  start_row = 1;
	  maxrows = 2147483647;
	}
      }
      break;
    case 'm': /* database metadata only (implies also that no data rows are returned) */
      obtain_metadata_only = 1;
      break;
    case 'n': /* odb_version -numeric */
      p = optarg;
      odb_version = atoi(p);
      break;
    case 'N': /* switch off NULLs in text-mode */
      print_nulls = 0;
      break;
    case 'O': /* Other options (accumulative) */
      p = optarg;
      if (strcmp(p,"verbose") == 0) verbose = 1;
      len = strlen(options) + strlen(p) + 2;
      options = realloc(options, len);
      strcat(options,";");
      strcat(options,p);
      break;
    case 'p': /* poolmask */
      p = optarg;
      len = strlen(poolmask) + strlen(p) + 2;
      poolmask = realloc(poolmask, len);
      if (strlen(poolmask) > 0) strcat(poolmask,",");
      strcat(poolmask,p);
      break;
    case 'P': /* server port */
      p = optarg;
      server_port = atoi(p);
      break;
    case 'q': /* data query */
      p = optarg;
      if (data_query) free(data_query);
      data_query = strdup(p);
      break;
    case 'Q': /* data query file */
      p = optarg;
      if (data_query) free(data_query);
      data_query = Slurp(p);
      break;
    case 'S': /* show row numbers */
      showrows = 1;
      break;
    case 'T': /* server timeout */
      p = optarg;
      server_timeout = atoi(p);
      server_timeout_given = 1;
      break;
    case 'v': /* viewname */
      p = optarg;
      if (viewname) free(viewname);
      viewname = strdup(p);
      if (strcmp(viewname,"@") == 0) { /* a NULL query */
	free(viewname);
	viewname = NULL;
      }
      break;
    case 'V': /* var=value */
      p = optarg;
      if (!varlist_start) {
	varlist = varlist_start = malloc(sizeof(varvalue_t));
      }
      else {
	varlist->next = malloc(sizeof(varvalue_t));
	varlist = varlist->next;
      }
      varlist->var = strdup(p);
      p = strchr(varlist->var,'=');
      if (p && p[1] != '\0') {
	*p++ = '\0';
	varlist->value = atof(p);
      }
      else {
	varlist->value = 0;
      }
      varlist->next = NULL;
      break;
    case '#':
      p = optarg;
      if (ncpus_nchunk) free(ncpus_nchunk);
      ncpus_nchunk = strdup(p);
      break;
    default:
      fprintf(stderr,"***Error: Unrecognized switch '-%c'\n",c);
      errflg++;
      break;
    } /* switch (c) */
  } /* while ((c = getopt(argc, argv, FLAGS)) != -1) */

  if (errflg) {
    fprintf(stderr,USAGE,argv[0]);
    goto finish;
  }

  if (!dbname)   dbname   = strdup(DB_DEFAULT);

  if (strcmp(output_file,DEVNULL) != 0) {
    data_only = 1;
    if (strcmp(output_format,"netcdf") == 0 || strcmp(output_format,"unetcdf") == 0) {
      /* Try to allocate at least 2 cpus/cores for NetCDF-file creation, pool-chunking of 16 */
      if (!ncpus_nchunk) ncpus_nchunk = strdup("2.16");
    }
  }
  if (!ncpus_nchunk) ncpus_nchunk = strdup("1");

  if (data_only) {
    fp_data = stdout;
    fp = fopen(DEVNULL,"w");
  }

  if (!direct) {
    if (!server_datapath) {
      server_datapath = strdup(DATAPATH_DEFAULT);
    }
    if (!server_host) {
      /* check dbname first */
      p = strchr(dbname,':');
      if (!p) {
	/* then check server_datapath */
	p = strchr(server_datapath,':');
	if (p) server_host = strdup(server_datapath);
      }
      else {
	server_host = strdup(dbname);
      }
      if (server_host) {
	p = strchr(server_host, ':');
	if (p) *p = '\0';
      }
      else {
	server_host = strdup(HOST_DEFAULT);
      }
    }
    if (server_datapath) {
      p = strchr(server_datapath,':');
      if (p) {
	char *tmp = strdup(p+1);
	free(server_datapath);
	server_datapath = strdup(tmp);
	free(tmp);
      }
    }
    if (server_port <= MIN_PORT) {
      server_port = PORT_DEFAULT;
    }

#if 0
    if (strlen(poolmask) > 0) {
      char *p = poolmask;
      len = strlen(options) + strlen("poolmask=") + strlen(p) + 2;
      options = realloc(options, len);
      strcat(options,";poolmask=");
      strcat(options,p);
    }
#endif
  } /* if (!direct) */


  if (is_binary) showrows = 0;
  if (obtain_metadata_only) maxrows = 0;
  if (!server_timeout_given) {
    char *env = getenv("ODBCS_TIMEOUT");
    if (!env) server_timeout = env ? atoi(env) : TIMEOUT_DEFAULT;
    if (server_timeout <= 0) server_timeout = TIMEOUT_DEFAULT;
    server_timeout_given = 1;
  }

  {
    static char newenv[40]; /* static because of "putenv()" */
    snprintf(newenv,sizeof(newenv),"ODBCS_TIMEOUT=%d",server_timeout);
    putenv(newenv);
  }

  if (direct) {
    /* fprintf(stderr,"odbi_direct_main: fp=%p, fileno=%d\n",fp,fileno(fp)); */
    db = ODBI_connect(dbname,"fp=%F",fp);
  }
  else {
    if (verbose) {
      fprintf(stderr,
	      "ODBI_connect(%s,"
	      "\n\toptions=%s,"
	      "\n\tfp=0x...,host=%s,port=%d,timeout=%d,"
	      "\n\tdatapath=%s,poolmask=%s,ncpus=%s)\n",
	      dbname,
	      options
	      , server_host
	      , server_port
	      , server_timeout
	      , server_datapath
	      , poolmask ? poolmask : ""
	      , ncpus_nchunk
	      );
    }
    db = ODBI_connect(dbname,
		      options
		      , fp
		      , server_host
		      , server_port
		      , server_timeout
		      , server_datapath
		      , poolmask
		      , ncpus_nchunk
		      );
  }

  if (db) {
    ODBI_print_db_metadata(fp,db,obtain_metadata_only);
    if (direct) {
      /* Additional metadata */
      fprintf(fp,"output_format=%s\n",output_format);
      fprintf(fp,"obtain_metadata=%d\n",obtain_metadata_only);
      fprintf(fp,"show_row_numbers=%d\n",showrows);
      fprintf(fp,"print_nulls=%d\n",print_nulls);
      if (strlen(poolmask) > 0) fprintf(fp,"permanent_poolmask=%s\n",poolmask);
    }
  }
  else {
    errflg = -2;
    fprintf(fp,"error_code=%d\n",errflg);
    fprintf(fp,"reason=cannot_open_database:%s\n",dbname);
    goto finish;
  } /* if (db) ... else ... */

  if (db && viewname) {
    void *q = ODBI_prepare(db,viewname,data_query);
    if (q) {
      int rc_exec;

      if (varlist_start) {  /* SET $var = value */
	varvalue_t *vl = varlist_start;
	while (vl) {
	  varvalue_t *this = vl;
	  (void) ODBI_bind_param(q, this->var, this->value);
	  free(this->var);
	  vl = this->next;
	  free(this);
	} 
	varlist_start = varlist = NULL; /* since already free'd them above */
      } /* varlist_start */

      ODBI_print_query_metadata(fp,q);

      if (obtain_metadata_only) {
	double time_delta = ODBI_timer(&time_start);
	fprintf(fp,"elapsed_time=%.6f\n", time_delta);
	fprintf(fp,"error_code=0\n");
	goto finish;
      }

      ODBI_limits(q, &start_row, &maxrows, &bufsize);

      if (strcmp(output_file,DEVNULL) != 0) {
	fprintf(fp,"output_file=%s\n",output_file);
	fprintf(fp,"output_format=%s\n",output_format);
	rc_exec = ODBI_fetchfile(q, output_file, output_format);
      }
      else {
	if ((rc_exec = ODBI_execute(q)) >= 0) {
	  int total_rows = 0;
	  double *data = NULL;
	  int rc;
	  int jcnt = start_row;
	  int jrow, jcol;
	  int nrows, ncols = ODBI_ncols(q);
	
	  data = malloc(bufsize * sizeof(*data));
	  if (!data) {
	    fprintf(fp,"buffer_size=%d\n",-bufsize);
	    fprintf(stderr,
		    "***Error: Unable to allocate bufsize of %d x %d = %lld bytes\n",
		    bufsize, (int)sizeof(*data),
		    (long long int)bufsize * sizeof(*data));
	    errflg = -3;
	    goto finish;
	  }
	  nrows=-1;
	  rc = ODBI_fetchonerow_array(q, &nrows, &ncols,&data[is_binary], bufsize);
	  while ( rc > 0) {
	    total_rows += nrows;
	    if (is_binary) {
	      data[0] = rc; /* Note: the actual data length information at the beginning */
	      fwrite(data, sizeof(*data), 1+rc, fp_data);
	    }
	    else {
	      double *d = &data[is_binary];
	      fprintf(fp,"number_of_rows=%d\n",nrows);
	      for (jrow=0; jrow<nrows; jrow++) {
		char *delim = " ";
		if (showrows) {
		  fprintf(fp_data,"#%d",jcnt++);
		  delim=",";
		}
		for (jcol=0; jcol<ncols; jcol++) {
		  ODBI_printcol(fp_data,q,jcol+1,*d,delim,print_nulls);
		  d++;
		  delim = ",";
		} /* for (jcol=0; jcol<ncols; jcol++) */
		fprintf(fp_data,"\n");
	      } /* for (jrow=0; jrow<nrows; jrow++) */
	    } /* if (is_binary) ... else ... */
	    fflush(fp_data);
	    nrows=-1;
	    rc = ODBI_fetchonerow_array(q, &nrows, &ncols,&data[is_binary], bufsize);
	  } /*  while ((rc = ODBI_fetchonerow_array(...))) */

	  ntot = total_rows;
	  if (is_binary) {
	    data[0] = rc;
	    fwrite(data, sizeof(*data), 1, fp_data);
	    data[0] = total_rows;
	    fwrite(data, sizeof(*data), 1, fp);
	  }
	  free(data);

	  if (rc < 0) {
	    fprintf(stderr,
		    "***Error: Unable to fetchonerow_array: rc=%d, bufsize=%d, nrows=%d, ncols=%d, total_rows=%d\n",
		    rc, bufsize, nrows, ncols, total_rows);
	    errflg = -4;
	    goto finish;
	  }
	  if (!is_binary) fprintf(fp,"total_number_of_rows=%d\n", total_rows);
	  (void) ODBI_finish(q);
	} /* if ((rc_exec = ODBI_execute(q)) >= 0) */
      } /* if (strcmp(output_file,DEVNULL) != 0) */

      if (rc_exec < 0) {
	errflg = -3;
	fprintf(fp,"error_code=%d\n",errflg);
	fprintf(fp,"reason=failing_to_execute_query:%s\n",viewname);
      }
    } /* if (q) ... */
    else {
      errflg = -1;
      fprintf(fp,"error_code=%d\n",errflg);
      fprintf(fp,"reason=cannot_process_query:%s\n",viewname);
    }  /* if (q) ... else ... */
    
    if (errflg == 0) {
      double time_delta = ODBI_timer(&time_start);
      if (is_binary) {
	fwrite(&time_delta, sizeof(time_delta), 1, fp);
      }
      else {
	fprintf(fp,"elapsed_time=%.6f\n", time_delta);
	fprintf(fp,"error_code=0\n");
      }
    } /* if (errflg == 0) */
  } /* if (db && viewname) */
  else {
    double time_delta = ODBI_timer(&time_start);
    fprintf(fp,"elapsed_time=%.6f\n", time_delta);
    fprintf(fp,"error_code=0\n"); /* a NULL query */
  }

 finish:
  (void) ODBI_disconnect(db);

  fflush(fp);
  if (fp_data != fp) fflush(fp_data);
  DRHOOK_END(ntot);
  return errflg;
}
