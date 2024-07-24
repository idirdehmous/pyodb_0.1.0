
/* odbi_direct.c */

#include "privpub.h"
#include "odb.h"
#define ODBI_DIRECT 1
#include "odbi.h"
#include "pcma_extern.h"
#include "iostuff.h"
#include "cdrhook.h"

#include "odbi_struct.h"
#include "vparam.h"

#define QDB q->db

#include "info.h"
#include "result.h"

extern int fast_odbsql; /* =1, if -F was *not* supplied, otherwise =0 */


static int version_threshold = 321533; /* ODB-version (CY32R1.533) after which
					  the client version number becomes available */

extern int odb_version; /* As supplied via -n option in the main-program */

/* Local subroutines */

PRIVATE void print_paths(FILE *fp,
			 const char *dbname)
{  
  const char fmt[] = "ODB_%sPATH%s%s";
  char *env = NULL;
  int slen = STRLEN(fmt) + 4 + STRLEN(dbname) + 1; /* 4 for max("DATA","SRC","IDX") */
  char *s;
  DRHOOK_START(print_paths);
  ALLOC(s, slen);

  snprintf(s,slen,fmt,"SRC","_",dbname);
  env = getenv(s);
  if (!env) {
    snprintf(s,slen,fmt,"SRC","","");
    env = getenv(s);
  }
  if (env) fprintf(fp,"srcpath=%s\n",env);

  snprintf(s,slen,fmt,"DATA","_",dbname);
  env = getenv(s);
  if (!env) {
    snprintf(s,slen,fmt,"DATA","","");
    env = getenv(s);
  }
  if (env) fprintf(fp,"datapath=%s\n",env);

  snprintf(s,slen,fmt,"IDX","_",dbname);
  env = getenv(s);
  if (!env) {
    snprintf(s,slen,fmt,"IDX","","");
    env = getenv(s);
  }
  if (env) fprintf(fp,"idxpath=%s\n",env);
  FREE(s);
  DRHOOK_END(0);
}


PRIVATE void print_consider_tables(FILE *fp,
				   const char *dbname)
{
  const char fmt[] = "ODB_CONSIDER_TABLES%s%s";
  char *env = NULL;
  int slen = STRLEN(fmt) + 1 + STRLEN(dbname) + 1;
  char *s;
  DRHOOK_START(print_consider_tables);
  ALLOC(s, slen);
  snprintf(s,slen,fmt,"_",dbname);
  env = getenv(s);
  if (!env) {
    snprintf(s,slen,fmt,"","");
    env = getenv(s);
  }
  if (env) fprintf(fp,"consider_tables=%s\n",env);
  FREE(s);
  DRHOOK_END(0);
}


PRIVATE void print_permanent_poolmask(FILE *fp,
				      const char *dbname)
{
  const char fmt[] = "ODB_PERMANENT_POOLMASK%s%s";
  char *env = NULL;
  int slen = STRLEN(fmt) + 1 + STRLEN(dbname) + 1;
  char *s;
  DRHOOK_START(print_permanent_poolmask);
  ALLOC(s, slen);
  snprintf(s,slen,fmt,"_",dbname);
  env = getenv(s);
  if (!env) {
    snprintf(s,slen,fmt,"","");
    env = getenv(s);
  }
  if (env) fprintf(fp,"permanent_poolmask=%s\n",env);
  FREE(s);
  DRHOOK_END(0);
}


PRIVATE void print_setvars(FILE *fp,
			   const char *dbname,
			   const char *viewname,
			   void *vq)
{
  Boolean mdi_found = false;
  double missing_value = 2147483647;
  DRHOOK_START(print_setvars);
  if (fp && dbname) { /* Print values of the SET-variables */
    int it = viewname ? 1 : 0; /* A "feature" from OpenMP-port ;-( */
    char *true_ddfile = NULL;
    int nsetvar;
    info_t *info = NULL;
    if (fast_odbsql) {
      ODBI_query_t *q = vq;
      info = q ? q->info : NULL;
      nsetvar = info ? info->nset : 0;
      if (!viewname && nsetvar == 0 && dbname) {
	/* Prepare to read dd-file directly */
	int len = STRLEN(dbname);
	true_ddfile = IOtruename(dbname, &len); /* $ODB_SRCPATH_<dbname>/<dbname>.dd */
      }
    }
    else {
      nsetvar = ODB_get_vars(dbname, viewname, it, 0, NULL);
    }
#if 0
    {
      fprintf(stderr,
	      "print_setvars: dbname='%s', viewname='%s', it=%d : nsetvar=%d, true_ddfile = '%s'\n",
	      dbname ? dbname : NIL,
	      viewname ? viewname : NIL,
	      it, nsetvar,
	      true_ddfile ? true_ddfile : NIL);
    }
#endif
    if (nsetvar > 0 || true_ddfile) {
      int rc, j;
      ODB_Setvar *setvar = NULL;

      if (!true_ddfile) ALLOC(setvar, nsetvar);
      else rc = nsetvar = 0;
      
      if (true_ddfile) {
	FILE *fp = fopen(true_ddfile,"r");
	if (fp) {
	  char line[MAXLINE+1];
	  while (fp && !feof(fp) && fgets(line, MAXLINE, fp)) {
	    if (line[0] == '$') ++nsetvar;
	  }
	  rewind(fp);
	  ALLOC(setvar, nsetvar);
	  j = 0;
	  while (fp && !feof(fp) && fgets(line, MAXLINE, fp)) {
	    if (line[0] == '$') {
	      char *blank = strchr(line, ' ');
	      if (blank) {
		*blank++ = '\0';
		setvar[j].symbol = STRDUP(line+1);
		setvar[j].value = atof(blank);
		++j;
	      }
	    }
	  }
	  rc = nsetvar = j;
	  fclose(fp);
	} /* fp */
	FREE(true_ddfile);
      }
      else if (info) {
	rc = nsetvar;
	for (j=0; j<rc; j++) {
	  setvar[j].symbol = STRDUP(info->s[j].name);
	  setvar[j].value = info->s[j].value;
	}
      }
      else {
	rc = ODB_get_vars(dbname, viewname, it, nsetvar, setvar);
      }
      fprintf(fp,"number_of_set_variables=%d\n",rc);
      for (j=0; j<rc; j++) {
	char *s = setvar[j].symbol;
	if (s && *s == '$') s++;
	if (s) {
	  if (!mdi_found && strequ(s,"mdi")) {
	    mdi_found = true;
	    missing_value = setvar[j].value;
	  }
	  fprintf(fp,"set_%s=%.14g\n",s,setvar[j].value);
	  FREE(setvar[j].symbol);
	}
      } /* for (j=0; j<nsetvar; j++) */
      FREE(setvar);
    } /* if (nsetvar > 0) */
    else {
      fprintf(fp,"number_of_set_variables=0\n");
    }
  }
  fprintf(fp,"missing_value=%.14g\n", missing_value);
  DRHOOK_END(0);
}


/* Maximum name length (can be *INCREASED* via ODBI_maxnamelen(newlength)) */
PRIVATE int max_name_len = 64;

int ODBI_maxnamelen(int new_len)
{
  int old_len = max_name_len;
  DRHOOK_START(ODBI_maxnamelen);
  if (new_len >= max_name_len) max_name_len = new_len;
  DRHOOK_END(0);
  return old_len;
}

/* Max int */
#define ODBI_MAX_INT 2147483647

/* For db->poolstat[] */
#define ODBI_POOL_UNLOADED   0
#define ODBI_POOL_LOADED     1
#define ODBI_POOL_RELEASED   2


void ODBI_print_db_metadata(FILE *fp,
			    void *vdb,
			    int complete_info)
{
  ODBI_db_t *db = vdb;
  DRHOOK_START(ODBI_print_db_metadata);

  if (db) { /* Database metadata */
    if (!fp) fp = db->fp;
    fprintf(fp,"ODBI\n");
    fprintf(fp,"database=%s\n",db->name);
    fprintf(fp,"open_mode=%s\n",db->mode);
    fprintf(fp,"number_of_pools=%d\n",db->npools);
    fprintf(fp,"io_method=%d\n",db->io_method);
    print_paths(fp, db->name);
    print_consider_tables(fp, db->name);
    print_permanent_poolmask(fp, db->name);
    if (complete_info) { /* behind a flag, since we don't always need this */
      int jt, jcol;
      fprintf(fp,"number_of_tables=%d\n",db->ntables);
      for (jt=0; jt<db->ntables; jt++) {
	fprintf(fp,"table_%d=%s\n",jt+1,db->tables[jt]);
      }
      for (jt=0; jt<db->ntables; jt++) {
	ODBI_query_t *tq = ODBI_prepare(db,db->tables[jt],NULL);
	ODBI_print_query_metadata(fp,tq);
      }
      print_setvars(fp, db->name, NULL, NULL);
    }
  } /* if (db) */
  DRHOOK_END(0);
}


void ODBI_print_query_metadata(FILE *fp,
			       void *vq)
{
  ODBI_query_t *q = vq;
  DRHOOK_START(ODBI_print_query_metadata);

  if (q) { /* Query metadata */
    int jcol;
    /* Keep compatibility with older clients (before version_threshold);
       however, formulas with division ('/') lead to a problem unless
       client is relinked with newer libraries */
    char *delim = (odb_version >= version_threshold) ? ODB_tag_delim : "/";
    if (!fp) fp = QDB->fp;
    fprintf(fp,"%s=%s\n",
	    /* (q->is_table && is_a_query == 2) ? "table" : "query", q->name); [fix this] */
	    (q->is_table) ? "table" : "query", q->name);
    fprintf(fp,"number_of_columns=%d\n",q->ncols_fixed);

    for (jcol=0; jcol<q->ncols_fixed; jcol++) {
      unsigned int dtnum = ODBI_coltypenum(q,jcol+1);
      const char *coltype = ODBI_coltype(q,jcol+1);
      const char *colname = ODBI_colname(q,jcol+1);
      const char *colnickname = ODBI_colnickname(q,jcol+1);
      fprintf(fp,"column_%d=%s%s%s%s%s%u%s%s%s\n",
	      jcol+1,
	      delim, coltype,
	      delim, colname,
	      delim, dtnum,
	      delim, colnickname,
	      delim);
    } /* for (jcol=0; jcol<q->ncols_fixed; jcol++) */

    if (q->is_table && /* complete_info && */
	q->ncols_all > q->ncols_fixed) { 
      int jextra = 0;
      fprintf(fp,"number_of_extra_columns=%d\n",q->ncols_all-q->ncols_fixed);
      /* Print excess columns (valid for tables only) */
      for (jcol=q->ncols_fixed; jcol<q->ncols_all; jcol++) {
	unsigned int dtnum = ODBI_coltypenum(q,jcol+1);
	const char *coltype = ODBI_coltype(q,jcol+1);
	const char *colname = ODBI_colname(q,jcol+1);
	int bitpos = q->cols[jcol].bitpos;
	int bitlen = q->cols[jcol].bitlen;
	fprintf(fp,"extra_column_%d=%s%s%s%s%s%u%s%d%s%d%s\n",
		++jextra,
		delim, coltype,
		delim, colname,
		delim, dtnum,
		delim, bitpos,
		delim, bitlen,
		delim);
      } /* for (jcol=0; jcol<q->ncols_fixed; jcol++) */
    }
    if (!q->is_table /* && complete_info */) {
      print_setvars(fp, QDB->name, q->name, q);
    }
  } /* if (q) */

  DRHOOK_END(0);
}


void *ODBI_connect(const char *dbname,
		   const char *params,
		   ...)
{
  FILE *fp = NULL;
  ODBI_db_t *db = NULL;
  DRHOOK_START(ODBI_connect);

  if (dbname) {
    int npools = 0;
    int ntables = 0;
    const char *mode = "READONLY";

    CALLOC(db,1);
    db->cli = NULL; /* this is *not* for client/server-model */

    if (params) { /* Not implemented yet */
      va_list ap;
      va_start(ap, params);
#if 0
      fprintf(stderr,"direct: params='%s'\n",params);
#endif
      {
	char *s, *fmt;
	int fmtlen = STRLEN(params) + 1;
	ALLOC(fmt,fmtlen);
	s = fmt;
	while (*params) {
	  if (!isspace(*params)) *s++ = *params;
	  params++;
	}
	*s = '\0';
	Vparam(db, 0, fmt, ap);
	FREE(fmt);
      }
      va_end(ap);
    }
    fp = db->fp;

    db->fp_opened_here = 0;
    if (!fp) {
      db->fp = fopen("/dev/null","w");
      if (db->fp) db->fp_opened_here = 1;
      else db->fp = stdout;
    }
    db->name = STRDUP(dbname);
    db->mode = STRDUP(mode);

    if (fast_odbsql) {
      db->handle = ODBc_open(dbname, "r", &npools, &ntables, NULL);
      db->io_method = 5;
    }
    else {
      f_odb_open_(dbname, mode, &npools, &db->handle
		  /* Hidden arguments */
		  , STRLEN(dbname), STRLEN(mode)
		  );

      f_odb_io_method_(&db->handle, &db->io_method);
    }
    if (db-> handle <=0) { /* database not found */
      FREE(db);
      db = NULL;
    } else {
      db->npools = npools;
      CALLOC(db->poolstat, 1+npools); /* Implies ODBI_POOL_UNLOADED; 
					 The "1+" is just to allow direct accessing with poolno=[1..npools] */
      db->nquery = 0;
      db->query = NULL;
      db->swapbytes = 0;
      db->tables = NULL;
      
      if (fast_odbsql) {
	const char **tblname = ODBc_get_tablenames(db->handle, &ntables);
	if (tblname && ntables > 0) {
	  int jt;
	  CALLOC(db->tables, ntables);
	  for (jt=0; jt<ntables; jt++) db->tables[jt] = STRDUP(tblname[jt]);
	}
      }
      else {
	char *token = NULL;
	char *saved = NULL;
	int maxcols = ODB_maxcols();
	int jt, len;
	len = max_name_len * maxcols;
	
	ALLOC(saved, len * maxcols + 1);
	f_odb_get_tablenames_(&db->handle, saved, &ntables
			      /* Hidden arguments */
			      , len
			      );
	
	CALLOC(db->tables, ntables);
	
	token = strtok(saved,ODB_tag_delim);
	jt = 0;
	while (token) {
	  db->tables[jt++] = STRDUP(token);
	  token = strtok(NULL,ODB_tag_delim);
	}
	FREE(saved);
      }
      
      db->ntables = ntables;
    }  
  } /* if (dbname) */
    
  DRHOOK_END(0);
  return db;
}


void *ODBI_prepare(void *vdb,
		   const char *viewname,
		   const char *query_string)
{
  ODBI_db_t *db = vdb;
  ODBI_query_t *q = NULL;
  DRHOOK_START(ODBI_prepare);
  if (db) {
    db->nquery++;
    REALLOC(db->query, db->nquery);
    q = &db->query[db->nquery-1];
    q->name = STRDUP(viewname);
    q->is_table = (*q->name == '@') ? 1 : 0;
    q->query_string = NULL;
    if (!q->is_table && query_string) q->query_string = STRDUP(query_string);
    q->fpcache = NULL; /* for now */
    q->a = NULL;
    q->poolno = 0;
    q->nrows = 0;
    q->ncols = 0;
    q->ncols_fixed = 0;
    q->ncols_all = 0;
    q->start_row = 1;
    q->maxrows = ODBI_MAX_INT;
    q->ntot = 0;
    q->nra = 0;
    q->row_offset = 0;
    q->string_count = 0;
    q->info = NULL;
    q->need_global_picture = -1;
    q->first_time = 1;
    q->all_processed = 0;
    q->re_eval_info = 0;
    QDB = db;

    if (fast_odbsql) {
      if (q->is_table) {
	const char fmt[] = "CREATE VIEW %s SELECT * FROM %s ;";
	int query_len = STRLEN(fmt) + 2 * STRLEN(q->name+1) + 1;
	ALLOC(q->query_string, query_len);
	snprintf(q->query_string, query_len, fmt, q->name+1, q->name+1);
      }
      q->info = ODBc_sql_prepare(QDB->handle, q->query_string, NULL, 0);
    }
    else {
      f_odb_addview_(&QDB->handle, q->name, &q->vhandle
		     /* Hidden arguments */
		     , STRLEN(q->name)
		     );
    }

    if (fast_odbsql) {
      int jcol;
      info_t *info = q->info;
      int ncols = info->ncols_true;
      int nadd = q->is_table ? (MAXBITS * ncols) : 0; /* each column could be have up to maxbits sub-members */
      char *bitfields = NULL;
      CALLOC(q->cols, ncols + nadd);
      for (jcol=0; jcol<ncols; jcol++) {
	const char *name = info->c[jcol].name;
	const char *nickname = info->c[jcol].nickname ? info->c[jcol].nickname : info->c[jcol].name;
	q->cols[jcol].name = STRDUP(name);
	q->cols[jcol].nickname = STRDUP(nickname);
	q->cols[jcol].type = STRDUP(info->c[jcol].dtype);
	q->cols[jcol].dtnum = info->c[jcol].dtnum;
	q->cols[jcol].bitpos = 0;
	q->cols[jcol].bitlen = 0;

	if (q->is_table && info->c[jcol].dtnum == DATATYPE_BITFIELD) {
	  /* Constructing columns for additional query to get 
	     the Bitfield members for "number_of_extra_columns" */
	  char *pure_name = STRDUP(info->c[jcol].name);
	  char *has_at = strchr(pure_name,'@');
	  if (has_at) *has_at = '\0';
	  {
	    const char *table_name = q->name; /* Note: starting with '@' */
	    char *field = NULL;
	    /* The +6 accounts for the following 6 chars ".*", and \0 */
	    int field_len = STRLEN(pure_name) + STRLEN(table_name) + 6; 
	    ALLOC(field, field_len);
	    snprintf(field, field_len, "\"%s.*%s\",", pure_name, table_name);
	    if (!bitfields) {
	      bitfields = STRDUP(field);
	    }
	    else {
	      char *copy = NULL;
	      int copylen = STRLEN(bitfields) + STRLEN(field) + 1;
	      ALLOC(copy, copylen);
	      snprintf(copy, copylen, "%s%s", bitfields, field);
	      FREE(bitfields);
	      bitfields = copy;
	    }
	    FREE(field);
	  }
	  FREE(pure_name);
	} /* if (q->is_table && info->c[jcol].dtnum == DATATYPE_BITFIELD) */
      }

      q->ncols_fixed = ncols;
      if (!bitfields) {
	q->ncols_all = q->ncols_fixed;
      }
      else {
	info_t *meminfo = NULL;
	int jcols, mem_ncols = 0;
	const char fmt[] = "CREATE VIEW meminfo_%s SELECT %s FROM %s ;";
	int query_len = STRLEN(fmt) + STRLEN(bitfields) + 2 * STRLEN(q->name+1) + 1;
	char *query_string = NULL;
	ALLOC(query_string, query_len);
	snprintf(query_string, query_len, fmt, q->name+1, bitfields, q->name+1);
	meminfo = ODBc_sql_prepare(QDB->handle, query_string, NULL, 0);
	mem_ncols = meminfo->ncols_true;
	for (jcol=ncols; jcol<ncols+mem_ncols; jcol++) {
	  int jj_col = jcol - ncols;
	  const char *name = meminfo->c[jj_col].name;
	  q->cols[jcol].name = STRDUP(name);
	  q->cols[jcol].nickname = STRDUP(name);
	  q->cols[jcol].type = STRDUP(meminfo->c[jj_col].dtype);
	  q->cols[jcol].dtnum = meminfo->c[jj_col].dtnum;
	  q->cols[jcol].bitpos = meminfo->c[jj_col].bitpos;
	  q->cols[jcol].bitlen = meminfo->c[jj_col].bitlen;
	}
	q->ncols_all = q->ncols_fixed + mem_ncols;
	meminfo = ODBc_sql_cancel(meminfo);
	FREE(query_string);
	FREE(bitfields);
      }
    }
    else {
      char *token = NULL;
      char *saved = NULL;
      int jcol, len;
      int extname;
      int ncols = ODB_maxcols(); /* Quite big ? */

      len = max_name_len * ncols + 1;
      ALLOC(saved, len);

      extname = 0;
      f_odb_get_colnames_(&QDB->handle, q->name, saved, &extname, &q->ncols_fixed
			  /* Hidden arguments */
			  , STRLEN(q->name), len
			  );

      if (q->is_table) {
	extname = 1;
	f_odb_get_colnames_(&QDB->handle, q->name, saved, &extname, &q->ncols_all
			    /* Hidden arguments */
			    , STRLEN(q->name), len
			    );
      }
      else {
	q->ncols_all = q->ncols_fixed;
      }

      CALLOC(q->cols, q->ncols_all);

      token = strtok(saved,ODB_tag_delim);
      jcol = 0;
      while (token) {
	q->cols[jcol].name = STRDUP(token);
	q->cols[jcol].nickname = STRDUP(token);
	jcol++;
	token = strtok(NULL,ODB_tag_delim);
      }

      f_odb_get_typenames_(&QDB->handle, q->name, saved, &extname, &q->ncols_all
			  /* Hidden arguments */
			  , STRLEN(q->name), len
			   );

      token = strtok(saved,ODB_tag_delim);
      jcol = 0;
      while (token) {
	q->cols[jcol].type = STRDUP(token);
	q->cols[jcol].dtnum = get_dtnum(token);
	jcol++;
	token = strtok(NULL,ODB_tag_delim);
      }

      FREE(saved);
    }

  }
  /*  finish: */
  DRHOOK_END(0);
  return q;
}


PRIVATE int Res2Q(const result_t *res,
		  ODBI_query_t *q)
{
  const int method = 1; /* Changed from 2 to 1 by SS/25-Feb-2014 */
  int n = 0;
  if (res) {
    int i,j,k;
    int nrows, ncols;
    q->nrows = nrows = res->nrows_out;
    q->ncols = ncols = res->ncols_out;
    q->nra = ODBc_lda(q->nrows, method);
    
    n = q->nra * (1 + q->ncols);
    ALLOC(q->a, n);
    
    if (res->row_wise) {
      for (k=0; k<nrows; k++) {
	i = q->nra + k;
	for (j=0; j<ncols; j++) {
	  q->a[i] = res->d[k][j];
	  i += q->nra;
	} /* for (j=0; j<ncols; j++) */
      } /* for (k=0; k<nrows; k++) */
    }
    else {
      for (j=0; j<ncols; j++) {
	i = (j+1) * q->nra;
	/* for (k=0; k<nrows; k++) q->a[i++] = res->d[j][k]; */
	memcpy(&q->a[i], &res->d[j][0], nrows * sizeof(*q->a));
      } /* for (j=0; j<ncols; j++) */
    }
  }
  else {
    q->nrows = 0;
    q->nra = ODBc_lda(q->nrows, method);
    n = q->nra * (1 + q->ncols);
    CALLOC(q->a, n);
  }
  return n;
}

int ODBI_execute(void *vq)
{
  ODBI_query_t *q = vq;
  int rc = 0;
  DRHOOK_START(ODBI_execute);
  if (q) {
#if 0
    fprintf(stderr,"ODBI_execute(%s):start; fast_odbsql=%d, q->need_global_picture=%d\n", 
	    q->name,fast_odbsql,q->need_global_picture);
#endif

    if (fast_odbsql) {
      info_t *info = q->info;

      if (!info) q->re_eval_info = 1;

      if (q->re_eval_info) {
	int j, nset = 0;
	set_t *s = NULL;
	if (info && info->nset > 0 && info->s) {
	  nset = info->nset;
	  ALLOC(s, nset);
	  for (j=0; j<nset; j++) {
	    s[j].name = STRDUP(info->s[j].name);
	    s[j].value = info->s[j].value;
	  }
	}
	info = q->info = ODBc_sql_cancel(q->info);
	info = q->info = ODBc_sql_prepare(QDB->handle, q->query_string, s, nset);
	if (nset > 0 && s) {
	  for (j=0; j<nset; j++) {
	    FREE(s[j].name);
	  }
	  FREE(s);
	}
	q->first_time = 1;
	q->re_eval_info = 0;
      }

      if (q->first_time) {
	/* Usually the very first AND standalone ODBI_execute() i.e. not called from ODB_fetchrow_array() */
	q->first_time = 0;
	rc = 0;
	goto finish;
      }
      else if (q->all_processed) {
	rc = 0;
	ODBI_finish(q);
	goto finish;
      }

      if (q->need_global_picture == -1) {
	if ((info->u && info->nuniqueby > 0) ||
	    info->has_select_distinct ||
	    info->has_aggrfuncs ||
	    (info->o && info->norderby > 0) ||
	    ((info->optflags & 0x1) == 0x1) ||
	    info->create_index > 0) {
	  q->need_global_picture = 1;
	}
	else if (info->has_select_distinct &&
		 !info->has_aggrfuncs &&
		 !(info->o && info->norderby > 0) &&
		 (info->create_index == 0) &&
		 (info->ncols == info->ncols_true) &&
		 (info->nwhere == 0) &&
		 (info->wherecond && strnequ(info->wherecond,"Unique(",7))) {
	  /* f.ex. SELECT 1+2*3 */
	  q->need_global_picture = 2;
	}
	else {
	  q->need_global_picture = 0;
	}
      }
#if 0
    fprintf(stderr,
	    "q->need_global_picture=%d now, q->row_offset=%d, q->start_row=%d, q->poolno/QDB->npools=%d/%d\n", 
	    q->need_global_picture, q->row_offset, q->start_row, q->poolno, QDB->npools);
#endif

    } /* if (fast_odbsql) */

    if (q->need_global_picture >= 1 && fast_odbsql) {
      const Bool row_wise_preference = true;
      int iret = -99999;
      if (q->need_global_picture == 1) {
	/* make sure you do this only once */
	/* loop over all pools and gather data to the result-chain */
	/* then copy all that data to the current "q" */
	info_t *info = q->info;
	int jp, nrows, npools = QDB->npools;
	result_t *res = NULL;
	for (jp=1; jp<=npools; jp++) {
	  /* Accumulate res for further postprocessing & merge */
	  q->poolno = jp;
	  nrows = ODBc_sql_exec(QDB->handle, q->info, jp, NULL, NULL);
	  QDB->poolstat[jp] = ODBI_POOL_LOADED;
	  res = ODBc_get_data(QDB->handle, res, q->info, jp, 1, nrows, false, &row_wise_preference);
	  info = q->info = ODBc_reset_info(q->info);
	}
	if (info->has_aggrfuncs) res = ODBc_aggr(res, 1);
	res = ODBc_operate(res, "count(*)");
	res = ODBc_sort(res, NULL, 0);
	iret = Res2Q(res, q);
	res = ODBc_unget_data(res);
	q->row_offset = q->start_row - 1;
	q->need_global_picture = 101; /* done */
      }
      else if (q->need_global_picture == 2) {
	/* f.ex. SELECT 1+2*3 */
 	/* make sure you do this only once */
	/* loop over all pools until at least one row found */
	/* then copy all that data to the current "q" */
	info_t *info = q->info;
	int jp, nrows, npools = QDB->npools;
	result_t *res = NULL;
	for (jp=1; !res && jp<=npools; jp++) {
	  /* Accumulate res for further postprocessing & merge */
	  q->poolno = jp;
	  nrows = ODBc_sql_exec(QDB->handle, q->info, jp, NULL, NULL);
	  QDB->poolstat[jp] = ODBI_POOL_LOADED;
	  if (nrows == 1) {
	    res = ODBc_get_data(QDB->handle, NULL, q->info, jp, 1, nrows, false, &row_wise_preference);
	  }
	  info = q->info = ODBc_reset_info(q->info);
	}
	iret = Res2Q(res, q);
	res = ODBc_unget_data(res);
	q->row_offset = q->start_row - 1;
	q->need_global_picture = 201; /* done */
	q->all_processed = 1;
      }
      rc = q->nrows - q->row_offset;
#if 0
      fprintf(stderr,
	      "Changing[pool#%d,glbpic=%d]> row_offset=%d, start_row=%d, ntot=%d, nrows=%d (rc=%d, iret=%d)\n",
	      q->poolno, q->need_global_picture,
	      q->row_offset, q->start_row, q->ntot, q->nrows, rc, iret);
#endif
      if (rc <= 0) {
	ODBI_finish(q);
      }
    } /* if (q->need_global_picture >= 1 && fast_odbsql) */
    else {
      do {
	int iret = 0;
	(void) ODBI_finish(q);
	
	if (q->poolno <= QDB->npools && 
	    QDB->poolstat[q->poolno] == ODBI_POOL_UNLOADED) {
	  
	  if (fast_odbsql) {
	    const int method = 1; /* Changed from 2 to 1 by SS/25-Feb-2014 */
	    rc = q->nrows = ODBc_sql_exec(QDB->handle, q->info, q->poolno, NULL, NULL);
	    q->ncols = q->ncols_fixed;
	    q->nra = ODBc_lda(q->nrows, method);
	  }
	  else {
	    f_odb_select_(&QDB->handle, q->name, 
			  &q->nrows, &q->ncols, &q->nra, &q->poolno,
			  &rc
			  /* Hidden arguments */
			  , STRLEN(q->name)
			  );
	  }
	
	  QDB->poolstat[q->poolno] = ODBI_POOL_LOADED;
	
	  if (q->fpcache) {
	    fprintf(q->fpcache,"%d %d\n",q->poolno,q->nrows);
	  }
	
#if 0
	  fprintf(stderr,"Changing[#%d]> row_offset=%d, start_row=%d, ntot=%d, nrows=%d\n",
		  q->poolno, q->row_offset, q->start_row, q->ntot, q->nrows);
#endif
	  if (q->nrows > 0 && 
	      q->ntot + 1        <= q->start_row &&
	      q->ntot + q->nrows >= q->start_row) {
	    int n;

	    if (fast_odbsql) {
	      const Bool row_wise_preference = false; /* We prefer Fortran-wise major column order */
	      result_t *res = 
		ODBc_get_data(QDB->handle, NULL, q->info, q->poolno, 1, q->nrows, 
			      false, &row_wise_preference);
	      n = Res2Q(res, q);
	      res = ODBc_unget_data(res);
	    }
	    else { /* ! fast_odbsql */
	      n = q->nra * (1 + q->ncols);
	      ALLOC(q->a, n);
	      f_odb_get_(&QDB->handle, q->name, q->a, 
			 &q->nrows, &q->ncols, &q->nra, &q->poolno, 
			 &iret
			 /* Hidden arguments */
			 , STRLEN(q->name)
			 );
	    }

	    q->row_offset = q->start_row - q->ntot - 1;
	  }
	  else {
	    q->row_offset = q->nrows;
	    rc = 0;
	  } /* if (q->nrows > 0 ...) else ... */
	  q->ntot += q->nrows;
#if 0
	  fprintf(stderr,"Changing[#%d]< row_offset=%d, start_row=%d, ntot=%d, nrows=%d\n",
		  q->poolno, q->row_offset, q->start_row, q->ntot, q->nrows);
#endif

	  if (fast_odbsql) {
	    q->info = ODBc_reset_info(q->info);
	  }
	  else {
	    f_odb_cancel_(&QDB->handle, q->name, &q->poolno, 
			  &iret
			  /* Hidden arguments */
			  , STRLEN(q->name)
			  );

	    f_odb_release_(&QDB->handle, &q->poolno, &iret);
	  }

	  QDB->poolstat[q->poolno] = ODBI_POOL_RELEASED;
	} /* if (q->poolno <= QDB->npools && ...) */
      } while (rc == 0 && q->poolno < QDB->npools);
    } /* if (q->need_global_picture == 1 && ...) ... else ... */

#if 0
    {
      fprintf(stderr,"ODBI_execute(%s) : rc=%d ; nrows=%d, ncols=%d, nra=%d, poolno=%d, row_offset=%d, ntot=%d\n",
	      q->name, rc, q->nrows, q->ncols, q->nra, q->poolno, q->row_offset, q->ntot);
    }
#endif
  }
 finish:
  DRHOOK_END(rc);
  return rc;
}


int ODBI_fetchonerow_array(void *vq,
			int *nrows,
			int *ncols,
			double d[],
			int nd)
{
  ODBI_query_t *q = vq;
  int rc = 0;
  DRHOOK_START(ODBI_fetchonerow_array);
  if (nrows) *nrows = 0;
  if (ncols) *ncols = 0;
  if (q && d) {

    if (q->all_processed) {
      if (ncols) *ncols = q->ncols;
      rc = 0;
      goto finish;
    }

    if (q->row_offset == q->nrows) {
      int rc_exec = 0;
      q->first_time = 0;
      rc_exec = ODBI_execute(q);
      if (rc_exec < 0) {
	rc = rc_exec;
	goto finish;
      }
    }

    if (q->a) {
      int maxrows   = nd/q->ncols;
      int row_chunk = MIN(maxrows, q->nrows - q->row_offset);
      row_chunk = MIN(row_chunk, q->maxrows);
      f_odb_copydata_(d, &row_chunk, &q->row_offset, 
		      q->a, &q->nrows, &q->ncols, &q->nra,
		      &QDB->swapbytes,
		      &rc);
#if 0
      {
	fprintf(stderr,
		"ODBI_fetchonerow_array@f_odb_copydata_[1]> rc=%d, row_chunk=%d, maxrows=%d, nrows=%d, row_offset=%d\n",
		rc, row_chunk, maxrows, q->nrows, q->row_offset);
      }
#endif
      rc *= q->ncols;
      q->row_offset += row_chunk;
      q->start_row += row_chunk;
      q->maxrows -= row_chunk;
      if (nrows) *nrows = row_chunk;
      if (ncols) *ncols = q->ncols;
#if 0
      {
	fprintf(stderr,
		"ODBI_fetchonerow_array@f_odb_copydata_[2]< rc=%d, row_chunk=%d, maxrows=%d, nrows=%d, row_offset=%d\n",
		rc, row_chunk, maxrows, q->nrows, q->row_offset);
      }
#endif
    }
  }
 finish:
  DRHOOK_END(nrows ? *nrows : 0);
  return rc;
}

int ODBI_fetchrow_array(void *vq,
			int *nrows,
			int *ncols,
			double d[],
			int nd)
{
  ODBI_query_t *q = vq;
  int total_rows=0;
  int local_rows=-1;
  int local_cols=0;
  int bufsize;
  int rc = 0;

  DRHOOK_START(ODBI_fetchrow_array);
  if (q && d) {
    if (*nrows) local_rows = *nrows;
    if (*ncols) local_cols = *ncols;
 
    if (local_rows < 0 ) { /* fetch pool by pool in direct mode (default mode) */
      rc=ODBI_fetchonerow_array(vq,nrows,ncols,d,nd);
      /* fprintf(stderr, "pool nbrows to be retrieved: %d %d\n", q->poolno, q->nrows);*/
      total_rows += q->nrows;
    } else {
      bufsize = nd;
      rc=ODBI_fetchonerow_array(vq,nrows,ncols,&d[0],bufsize);
      while (rc > 0) {
/*AF 16/03/09 wrong number of rows
	total_rows += q->nrows;
	bufsize -= q->nrows * *ncols;
*/
	total_rows += *nrows;
	bufsize -= *nrows * *ncols;
	/*	fprintf(stderr, "all nbrows to be retrieved: %d %d\n", q->poolno, q->nrows);*/
	rc=ODBI_fetchonerow_array(vq,nrows,ncols,&d[total_rows* *ncols],bufsize);
      }
    }
  }
  if (nrows) *nrows = total_rows;
  if (ncols) *ncols = local_cols;
  DRHOOK_END(rc);
  return total_rows;
}

int ODBI_finish(void *vq)
{
  ODBI_query_t *q = vq;
  int rc = 0;
  DRHOOK_START(ODBI_finish);
  if (q) {
    FREE(q->a);
    if (q->poolno >= 1 && q->poolno <= QDB->npools) {
#if 0
      {
	fprintf(stderr,
		"ODBI_finish(%s at pool#%d) ; nrows=%d, ncols=%d, nra=%d, row_offset=%d, ntot=%d\n",
		q->name, q->poolno, q->nrows, q->ncols, q->nra, q->row_offset, q->ntot);
      }
#endif
      if (!fast_odbsql) {
	if (QDB->poolstat[q->poolno] == ODBI_POOL_LOADED) {
	  /* To circumvent a bug in ODB ; the bug naturally to be fixed soon ! */
	  f_odb_release_(&QDB->handle, &q->poolno, &rc);
	}
      } 
    }
    if (q->poolno <= QDB->npools) q->poolno++;
    q->nrows = 0;
    q->ncols = 0;
    q->nra = 0;
    q->row_offset = 0;
    if (fast_odbsql) {
      q->need_global_picture = -1;
    }
  }
  DRHOOK_END(0);
  return rc;
}


int ODBI_disconnect(void *vdb)
{
  ODBI_db_t *db = vdb;
  int rc = 0;
  DRHOOK_START(ODBI_disconnect);
  if (db) {
    int j;
    const int save = 0;
    if (fast_odbsql) {
      rc = ODBc_close(db->handle);
    }
    else {
      f_odb_close_(&db->handle, &save, &rc);
    }
    FREE(db->name);
    FREE(db->mode);
    for (j=0; j<db->nquery; j++) {
      ODBI_query_t *q = &db->query[j];
      (void) ODBI_finish(q);
      FREE(q->name);
      FREE(q->query_string);
      if (q->cols) {
	int jcol;
	for (jcol=0; jcol<q->ncols_all; jcol++) {
	  FREE(q->cols[jcol].name);
	  FREE(q->cols[jcol].nickname);
	  FREE(q->cols[jcol].type);
	}
	FREE(q->cols);
      }
      if (fast_odbsql) {
	q->info = ODBc_sql_cancel(q->info);
      }
    }
    FREE(db->query);
    FREE(db->poolstat);
    if (db->fp_opened_here) fclose(db->fp);
    FREE(db);
  }
  DRHOOK_END(0);
  return rc;
}


double ODBI_bind_param(void *vq, 
		       const char *param_name, 
		       double new_value)
{
  ODBI_query_t *q = vq;
  double old_value = 0;
  DRHOOK_START(ODBI_bind_param);
  if (param_name && q && q->name) {
    if (fast_odbsql) {
      info_t *info = q->info;
      if (info) {
	int j, nset = info->nset;
	Boolean found = false;
	if (*param_name == '$') ++param_name;
	for (j=0; j<nset && !found; ++j) {
	  const char *varname = info->s[j].name;
	  if (varname && *varname == '$') ++varname;
	  if (strequ(varname, param_name)) {
	    old_value = info->s[j].value;
	    info->s[j].value = new_value;
	    found = 1;
	  }
	} /* for (j=0; j<nset && !found; j++) */
	if (!found) {
	  int len = STRLEN(param_name) + 2;
	  j = nset++;
	  REALLOC(info->s, nset);
	  ALLOC(info->s[j].name, len);
	  snprintf(info->s[j].name, len, "$%s", param_name);
	  info->s[j].value = new_value;
	}
	fprintf(QDB->fp,"set_%s=%.14g\n",param_name,new_value);
	q->re_eval_info = 1; /* Next time ODBI_execute() is run, re-evaluate the q->info */
      }
    }
    else {
      f_odb_setval_(&QDB->handle, q->name, param_name, 
		    &new_value, &old_value
		    /* Hidden arguments */
		    , STRLEN(q->name), STRLEN(param_name)
		    );
      if (*param_name == '$') ++param_name;
      fprintf(QDB->fp,"set_%s=%.14g\n",param_name,new_value);
    }
  }
  DRHOOK_END(0);
  return old_value;
}


void ODBI_limits(void *vq,
		 int *start_row,
		 int *maxrows,
		 int *bufsize)
{
  ODBI_query_t *q = vq;
  DRHOOK_START(ODBI_limits);
  if (q) {
    if (start_row) {
      if (*start_row <= 0) *start_row = 1;
      q->start_row = *start_row;
      fprintf(QDB->fp,"start_row=%d\n",q->start_row);
    }
    if (maxrows) {
      if (*maxrows < 0) *maxrows = ODBI_MAX_INT;
      q->maxrows = *maxrows;
      fprintf(QDB->fp,"maxrows=%d\n",q->maxrows);
    }
    if (bufsize) {
      if (*bufsize < q->ncols_fixed) *bufsize = q->ncols_fixed;
      (*bufsize)++; /* Accomodate one slot for binary data */
      fprintf(QDB->fp,"buffer_size=%d\n",*bufsize);
    }
  }
  DRHOOK_END(0);
}


int ODBI_fetchfile(void *vq,
		   const char *filename,
		   const char *fileformat)
{
#if 0
  const char dbgfile[] = "/tmp/fetchfile.direct";
#else
  const char dbgfile[] = "/dev/null";
#endif
  FILE *dbgfp = fopen(dbgfile, "w");
  int rc = 0;
  FILE *outfp = stdout; /* That's the "dump" for ODBI_DIRECT */
  ODBI_query_t *q = vq;
  DRHOOK_START(ODBI_fetchfile);
  setvbuf(dbgfp, (char *)NULL, _IONBF, 0);
  if (q && outfp && filename && fileformat) {
    info_t *info = q->info;
    int filesize = 0;
    if (fast_odbsql && info) {
      const char fmt[] = "$ODB_FEBINPATH/odbsql -q '%s' -f %s -o %s -B -i '%s' -m%d,%d -n%s";
      const char *srcpath = info->dbcred.srcpath;
      const char *sql_query = info->sql_query;
      const char search_str[] = "The original query follows\n\n";
      const char *matched = strstr(sql_query, search_str); /* see also aux/result.c & aux/info.c */
      char *cmd = NULL;
      char *nl;
      int iret, len, dummy;
      char *fname = NULL;
      extern int csopt; /* Flag for indicating that c/s-infra is on, if == 1 i.e. when odbi_direct.x -C */
      extern char *ncpus_nchunk; /* Parallelism (and pool-chunking) */

      if (strcmp(fileformat,"netcdf") == 0 || strcmp(fileformat,"unetcdf") == 0) {
	/* Try to allocate at least 2 cpus/cores for NetCDF-file creation, pool-chunking of 16 */
	if (!ncpus_nchunk) ncpus_nchunk = strdup("2.16");
      }
      if (!ncpus_nchunk) ncpus_nchunk = strdup("1");

      fprintf(dbgfp,"csopt = %d [c/s-infra], ncpus[.nchunk]=%s\n",
	      csopt,ncpus_nchunk);

      if (csopt) {
	int pid = (int)getpid();
	const char *last_slash = strrchr(filename,'/');
	char *env = getenv("TMPDIR");
	if (!env) env = ".";
	if (last_slash) filename = last_slash + 1;
	len = STRLEN(env) + STRLEN(filename) + 30;
	ALLOC(fname, len);
	snprintf(fname, len, "%s/__.%s.%d", env, filename, pid);
      }
      else {
	fname = STRDUP(filename);
      }

      if (matched) sql_query = matched + STRLEN(search_str);
      len = 
	STRLEN(fmt) + STRLEN(sql_query) + STRLEN(fileformat) + 
	STRLEN(fname) + STRLEN(srcpath) + STRLEN(ncpus_nchunk) + 100;
      ALLOC(cmd, len);
      snprintf(cmd, len, fmt, sql_query, fileformat, fname, srcpath, q->start_row, q->maxrows,
	       ncpus_nchunk);

      /* Replace newlines with blanks */
      nl = cmd-1;
      while (*++nl) if (*nl == '\n') *nl = ' ';

      fprintf(dbgfp,"cmd=[%s]\n",cmd);

      codb_remove_file_(fname, &dummy, STRLEN(fname));
      iret = system(cmd);
      fprintf(dbgfp,"iret = %d, dummy = %d\n",iret,dummy);

      if (iret == 0) {
	/* Now copy file to outfp, unless a terminal */
	FILE *fp = NULL;

	codb_filesize_(fname, &filesize, STRLEN(fname));
	fprintf(dbgfp,"filesize = %d\n",filesize);

	if (!csopt) {
	  rc = filesize;
	  goto finish;
	}

	fwrite(&filesize, sizeof(filesize), 1, outfp);
	fflush(outfp);

	if (filesize > 0) fp = fopen(fname, "r");
	fprintf(dbgfp,"fname = '%s', open fp = %p,%d [outfp = %p,%d]\n", 
		fname, fp, fileno(fp), outfp, fileno(outfp));

	if (fp) {
	  int nread = 0;
	  char buf[IO_BUFSIZE_DEFAULT];
	  while (nread < filesize) {
	    int nbytes = fread(buf, sizeof(*buf), sizeof(buf), fp);
	    fprintf(dbgfp,"\tnbytes = %d\n",nbytes);
	    if (nbytes > 0) {
	      int nwritten = fwrite(buf, sizeof(*buf), nbytes, outfp);
	      if (nwritten == nbytes) {
		nread += nwritten;
		fprintf(dbgfp,"\t\tnread = %d bytes so far (nwritten = %d [incr]); filesize = %d bytes\n",
			nread,nwritten,filesize);
	      }
	      else {
		fprintf(stderr,"direct: ODBI_fetchfile() failed to write from file '%s' to pipe [rc=-2]\n",fname);
		fprintf(dbgfp,"direct: ODBI_fetchfile() failed to write from file '%s' to pipe [rc=-2]\n",fname);
		rc = -2;
		break;
	      }
	    }
	    else {
	      if (nbytes < 0) {
		fprintf(stderr,"direct: ODBI_fetchfile() failed to read from file '%s' [rc=-3]\n",fname);
		fprintf(dbgfp,"direct: ODBI_fetchfile() failed to read from file '%s' [rc=-3]\n",fname);
		rc = -3;
	      }
	      break;
	    }
	  } /* while (nread < filesize) */
	  fclose(fp);
	  if (rc == 0) rc = filesize;
	}
	else {
	  fprintf(stderr,"direct: ODBI_fetchfile() unable to open file '%s' for reading [rc=-4]\n",fname);
	  fprintf(dbgfp,"direct: ODBI_fetchfile() unable to open file '%s' for reading [rc=-4]\n",fname);
	  rc = -4;
	}
      }
      else {
	fwrite(&filesize, sizeof(filesize), 1, outfp);
	fprintf(stderr,"direct: ODBI_fetchfile() failed in command '%s' [rc=-1]\n",cmd);
	fprintf(dbgfp,"direct: ODBI_fetchfile() failed in command '%s' [rc=-1]\n",cmd);
	rc = -1;
      }

      fflush(outfp);
      
      codb_remove_file_(fname, &dummy, STRLEN(fname));

    finish:
      FREE(cmd);
      FREE(fname);
    }
    else {
      fwrite(&filesize, sizeof(filesize), 1, outfp);
      fprintf(stderr,"direct: ODBI_fetchfile() implemented only via odbsql i.e. do NOT use askodb -F [rc=-5]\n");
      fprintf(dbgfp,"direct: ODBI_fetchfile() implemented only via odbsql i.e. do NOT use askodb -F [rc=-5]\n");
      rc = -5;
    }
  }
  ODBI_finish(q);
  DRHOOK_END(rc >= 0 ? rc : 0);
  fprintf(dbgfp,"rc = %d\n",rc);
  fclose(dbgfp);
  return rc;
}

			  
#include "odbi_shared.c"
