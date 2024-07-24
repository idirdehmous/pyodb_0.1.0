
/* tree.c */

#include "defs.h"
#include "magicwords.h"
#include "pcma_extern.h"

extern Boolean verbose;
extern int safeGuard;
extern int LEX_in_where;

extern int bailout_level;
extern char *bailout_string;
extern int ddl_piped;
extern Boolean debug;

PUBLIC  Boolean ODB_tables_done = 0;

PUBLIC  Boolean ODB_in_tabledef = 0;
PUBLIC  Boolean has_USD_symbols = 0;
PUBLIC  Boolean has_OFFSET_func = 0;

PUBLIC  Boolean has_count_star = 0;
PUBLIC  Boolean has_usddothash = 0;
PUBLIC  Boolean no_from_stmt = 0;

PUBLIC int ODB_nsymbols = 0;
PUBLIC int ODB_nUSDsymbols = 0;
PUBLIC int ODB_nHASHsymbols = 0;
PUBLIC int ODB_nBSNUMsymbols = 0;
PUBLIC int ODB_ntypes = 0;
PUBLIC int ODB_ntables = 0;
PUBLIC int ODB_nviews = 0;
PUBLIC int ODB_nnodes = 0;
PUBLIC int ODB_hollerith_strings = 0;
PUBLIC Boolean ODB_has_links = 0;


/* Used in ODB_link_massage() */

typedef struct cmptable_t {
  double wt;
  ODB_Table *p;
} Cmptable_t;

PRIVATE int cmptable(const Cmptable_t *a, const Cmptable_t *b) {
  if      ( a->wt < b->wt ) return -1;
  else if ( a->wt > b->wt ) return  1;
  else			    return  0;
}

/* Used in ODB_reorder_tables() */

typedef struct cmpfromtable_t {
  double wt;
  int rank;
  int flag;
} Cmpfromtable_t;

PRIVATE int cmpfromtable(const Cmpfromtable_t *a, const Cmpfromtable_t *b) {
  if      ( a->wt < b->wt ) return -1;
  else if ( a->wt > b->wt ) return  1;
  else			    return  0;
}

PRIVATE ODB_Tree *first_oper      = NULL;
PRIVATE ODB_Tree *last_oper = NULL;

PRIVATE ODB_Symbol *first_symbol      = NULL;
PRIVATE ODB_Symbol *last_symbol = NULL;

PRIVATE ODB_Type *first_type      = NULL;
PRIVATE ODB_Type *last_type = NULL;

PRIVATE ODB_Table *first_table      = NULL;
PRIVATE ODB_Table *last_table = NULL;

PUBLIC ODB_Table **table_hier = NULL;

PRIVATE ODB_View *first_view      = NULL;
PRIVATE ODB_View *last_view = NULL;

/* Registered functions */

static double ftrunc(double x); /* As in funcs.h included later in this file */

static
double lldegrees(double d)
{
  extern int latlon_rad;
  if (latlon_rad != 0) d = R2D(d);
  /* if (d > 180) d -= 360; */
  return d;
}

static
double llradians(double d)
{
  extern int latlon_rad;
  if (latlon_rad != 1) d = D2R(d);
  /* if (d > pi) d -= 2*pi; */
  return d;
}

static double dint(double d); /* from funcs.h */

static
double timestamp(double indate, double intime)
{ /* Merge "YYYYMMDD" and "HHMMSS" into "YYYYMMDDHHMMSS" */
  double outstamp = RMDI; /* Missing data indicator : here indicates error */
  if (indate >= 0 && indate <= INT_MAX &&
      intime >= 0 && intime <= 240000) {
    long long int lldate = (long long int)indate;
    long long int lltime = (long long int)intime;
    long long int tstamp = lldate * 1000000ll + lltime;
    outstamp = tstamp;
    outstamp = dint(outstamp);
  }
  return outstamp;
}


static
double basetime(double indate, double intime)
{ /* Merge "YYYYMMDD" and "HHMMSS" into "YYYYMMDDHH" */
  double outstamp = RMDI; /* Missing data indicator : here indicates error */
  if (indate >= 0 && indate <= INT_MAX &&
      intime >= 0 && intime <= 240000) {
    long long int lldate = (long long int)indate;
    long long int lltime = (long long int)intime;
    long long int tstamp = lldate * 1000000ll + (lltime/10000ll);
    outstamp = tstamp;
    outstamp = dint(outstamp);
  }
  return outstamp;
}


static
int boxid(double x, double deltax, double xmin, double xmax,
	  int wrap_around_ok)
{
  int id = NMDI;
  if (deltax < 0) deltax = -deltax;
  if (deltax != 0 && xmin < xmax) {
    double xspan = xmax - xmin;
    int n = (int)(xspan/deltax);
    id = (int)((x - xmin)/deltax);
    if (id < 0 || id >= n) {
      if (wrap_around_ok) {
	if (id < 0) id = n-1;
	else if (id >= n) id = 0;
      }
      else {
	if (id < 0) id = 0;
	else if (id >= n) id = n-1;
      }
    }
  }
  return id;
}

static
double box(double x, double deltax, double xmin, double xmax,
	   int wrap_around_ok)
{
  double value = RMDI;
  if (deltax < 0) deltax = -deltax;
  if (deltax != 0 && xmin < xmax) {
    int id = boxid(x,deltax,xmin,xmax,wrap_around_ok);
    value = xmin + id * deltax + 0.5 * deltax;
  }
  return value;
}

static
double boxid_lat(double lat, double deltalat)
{
  const double latmax = 90;
  return boxid(lat,deltalat,-latmax,+latmax,0);
}

static
double boxlat(double lat, double deltalat)
{
  const double latmax = 90;
  return box(lat,deltalat,-latmax,+latmax,0);
}

static
double boxid_lon(double lon, double deltalon)
{
  const double lonmax = 180;
  return boxid(lon,deltalon,-lonmax,+lonmax,1);
}

static
double boxlon(double lon, double deltalon)
{
  const double lonmax = 180;
  return box(lon,deltalon,-lonmax,+lonmax,1);
}

#include "funcs.h"

typedef struct _kw_t {
  char *name;
  int   token_def;
} kw_t;

static int Nkw = 0;

PRIVATE kw_t 
keyword[] = {
  "number", ODB_NUMBER,
  "symref", ODB_NAME,
  "$symref", ODB_USDNAME,
  "#symref", ODB_HASHNAME,
  "\\bsnum", ODB_BSNUM,
  "nickname", ODB_NICKNAME,
  "arrname", ODB_ARRNAME,
  "hollerith", ODB_STRING,
  "wildcard", ODB_WC_STRING,
  "LIKE", ODB_LIKE,
  "NOTLIKE", ODB_NOTLIKE,
  "==", ODB_EQ,
  "+", ODB_ADD,
  "-", ODB_SUB,
  "*", ODB_STAR,
  "/", ODB_DIV,
  "", ODB_UNARY_PLUS,
  "-", ODB_UNARY_MINUS,
  "||", ODB_OR,
  "&&", ODB_AND,
  "!", ODB_NOT,
  ">", ODB_GT,
  ">=", ODB_GE,
  "<", ODB_LT,
  "<=", ODB_LE,
  "!=", ODB_NE,
  "<=>", ODB_CMP,
  "set", ODB_SET,
  "typeref", ODB_TYPE,
  "tableref", ODB_TABLE,
  "viewref", ODB_VIEW,
  "function", ODB_FUNC,
  "string_function", ODB_STRFUNC,
  "inside_function", ODB_INSIDE,
  "inside_polygon_function", ODB_INSIDE_POLYGON,
  "near_function", ODB_NEAR,
  "str1_function", ODB_STRFUNC1,
  "aggregate_function", ODB_FUNCAGGR,
  "where_symbol",ODB_WHERE_SYMBOL,
  "query",ODB_QUERY,
  "match_function",ODB_MATCH,
  "?", ODB_QMARK,
  ":", ODB_COLON,
  "|", ODB_NORM,
  NULL
};


PRIVATE int
cmpstr_kw(const void *A, const void *B)
{
  const kw_t *a = A;
  const kw_t *b = B;
  if      ( a->token_def < b->token_def ) return -1;
  else if ( a->token_def > b->token_def ) return  1;
  else			                  return  0;
}


PUBLIC char *
ODB_keymap(int what)
{
  if (Nkw == 0) { /* Very first time only */
    const kw_t *pkw = keyword;
    while (pkw->name) {
      Nkw++;
      pkw++;
    }
    qsort(keyword, Nkw, sizeof(kw_t), cmpstr_kw);
  } /* if (Nkw == 0) */
  {
    static char unknown[] = "<unknown>";
    kw_t *pkw = NULL;
    kw_t key;
    key.token_def = what;
    pkw = bsearch(&key, keyword, Nkw, sizeof(kw_t), cmpstr_kw);
    return pkw ? pkw->name : unknown;
  }
}

PUBLIC ODB_Tree *
ODB_start_oper() { return first_oper; }

PUBLIC ODB_Symbol *
ODB_start_symbol() { return first_symbol; }

PUBLIC ODB_Type *
ODB_start_type() { return first_type; }

PUBLIC ODB_Table *
ODB_start_table() { return first_table; }

PUBLIC ODB_View *
ODB_start_view() { return first_view; }

PUBLIC char *
ODB_extract(const char *in, int left_delim, int right_delim)
{
  char *rc = NULL;
  if (in) {
    char *p = STRDUP(in);
    int lenp = strlen(p);
    char *pleft = 
      BETWEEN(left_delim,0,255) ? strchr(p, left_delim) : p - 1;
    rc = STRDUP(in);
    if (pleft) {
      char *pright = NULL;
      pleft++;
      pright = 
	BETWEEN(right_delim,0,255) ? strchr(pleft, right_delim) : &p[lenp];
      if (pright) {
	*pright = '\0';
	FREE(rc);
	rc = STRDUP(pleft);
      }
    } /* if (pleft) */
    FREE(p);
  }

  /*
  if (rc) fprintf(stderr,
		  "ODB_extract: from='%s' with delims %c and %c gives '%s'\n",
		  in, left_delim, right_delim, rc);
		  */

  return rc;
}


PRIVATE int
cmpstr_funcs(const void *A, const void *B)
{
  const funcs_t *a = A;
  const funcs_t *b = B;
  return strcmp(a->name, b->name);
}


PRIVATE const funcs_t *
get_func(const char *name)
{
  if (NfuncS == 0) { /* Very first time only */
    const funcs_t *pf = Func;
    while (pf->name) {
      NfuncS++;
      pf++;
    }
    qsort(Func, NfuncS, sizeof(funcs_t), cmpstr_funcs);
  } /* if (NfuncS == 0) */

  {
    funcs_t *pf = NULL;
    funcs_t key;
    key.name = (char *)name;
    pf = bsearch(&key, Func, NfuncS, sizeof(funcs_t), cmpstr_funcs);
    if (pf) return pf;
  }
  /* Ending up here ? ==> Error */
  SETMSG1("Unrecognized function '%s'",name);
  YYerror(msg);
  return NULL;
}


PRIVATE const funcs_t *
check_func(const char *name, int numargs, 
	   const int *min_numargs, const int *max_numargs)
{
  const funcs_t *pf = get_func(name);
  if (pf) {
    if (pf->numargs == -1) { /* Variable no. of arguments */
      Boolean will_abort = 0;
      if (min_numargs && (numargs < *min_numargs)) {
	SETMSG3("Incorrect no. of args (%d) passed to the function '%s'. Expecting at least %d.",
		numargs,name,*min_numargs);
	YYwarn(1,msg); /* "1" to get "*** Error" message printed ; no abort yet */
	will_abort = 1;
      }
      if (max_numargs && (numargs > *max_numargs)) {
	SETMSG3("Incorrect no. of args (%d) passed to the function '%s'. Expecting no more than %d.",
		numargs,name,*max_numargs);
	YYwarn(1,msg); /* "1" to get "*** Error" message printed ; no abort yet */
	will_abort = 1;
      }
      if (will_abort) exit(1);
      return pf;
    }
    if (pf->numargs == numargs) return pf;
    SETMSG3("Incorrect no. of args (%d) passed to the function '%s'. Expecting %d.",
	    numargs,name,pf->numargs);
    YYerror(msg);
  } /* if (pf) */
  /* Ending up here ? ==> Error */
  SETMSG1("Unrecognized function '%s'",name);
  YYerror(msg);
  return NULL;
}


PRIVATE char *
symref(const ODB_Symbol *p)
{
  char *s, *name = NULL;
  int len = 20;
  if (p) {
    if (p->name) {
      name = p->name;
      len += strlen(name);
    }
  }
  ALLOC(s, len);
  snprintf(s,len,"[%p,'%s']",p,name ? name : NIL);
  return s;
}


PUBLIC void
ODB_print_symbols(FILE *fp)
{
  int j;
  if (!fp) fp = stderr;

  fprintf(fp,"*** ODB_print_symbols() ***\n");

  fprintf(fp,"\tNo. of symbols = %d : kind = ODB_NAME = %d\n",
	  ODB_nsymbols, ODB_NAME);
  fprintf(fp,"\tNo. of $-symbols = %d : kind = ODB_USDNAME = %d\n",
	  ODB_nUSDsymbols, ODB_USDNAME);
  fprintf(fp,"\tNo. of #-symbols = %d : kind = ODB_HASHNAME = %d\n",
	  ODB_nHASHsymbols, ODB_HASHNAME);
  fprintf(fp,"\tNo. of \\-symbols = %d : kind = ODB_BSNUM = %d\n",
	  ODB_nBSNUMsymbols, ODB_BSNUM);
  fprintf(fp,"\tNo. of holleriths = %d : kind = ODB_STRING = %d\n",
	  ODB_hollerith_strings, ODB_STRING);
  fprintf(fp,"\tNo. of types   = %d : kind = ODB_TYPE = %d\n",
	  ODB_ntypes, ODB_TYPE);
  fprintf(fp,"\tNo. of tables  = %d : kind = ODB_TABLE = %d\n",
	  ODB_ntables, ODB_TABLE);
  fprintf(fp,"\tNo. of views   = %d : kind = ODB_VIEW = %d\n",
	  ODB_nviews, ODB_VIEW);

  { /* Symbol table */
    ODB_Symbol *p = ODB_start_symbol();
    for (; p != NULL; p = p->next) {
      fprintf(fp,"symref=%p: name='%s', kind=(%d,%s), dval=%g, dname='%s', flag=0x%x\n",
	      p, p->name?p->name:NIL, 
	      p->kind, ODB_keymap(p->kind),
	      p->dval, p->dname ? p->dname : NIL,
	      p->flag);
    }
  }

  { /* Type definitions */
    ODB_Type *p = ODB_start_type();
    for (; p != NULL; p = p->next) {
      char *s = symref(p->type);
      fprintf(fp,"typeref=%p: symref=%s, nsym=%d, no_members=%d, bitstream=%d\n",
	      p, s, p->nsym, (int)p->no_members, (int)p->bitstream);
      FREE(s);

      if (p->nsym > 0) {
	fprintf(fp,"  : symrefs & members=\n");
	for (j=0; j<p->nsym; j++) {
	  s = symref(p->sym[j]);
	  fprintf(fp,"\t%s",s);
	  FREE(s);
	  s = symref(p->member[j]);
	  fprintf(fp," & %s",s);
	  FREE(s);
	  if (p->bitstream) {
	    fprintf(fp," : bit (pos=%d,len=%d)",p->pos[j],p->len[j]);
	  }
	  fprintf(fp,"\n");
	}
      } /* if (p->nsym > 0) */

    }
  }

  { /* Table definitions */
    ODB_Table *p = ODB_start_table();
    for (; p != NULL; p = p->next) {
      char *s = symref(p->table);
      fprintf(fp,"tableref=%p: symref=%s, nsym=%d\n",
	      p, s, p->nsym);
      FREE(s);

      if (p->nsym > 0) {
	char **expname = p->expname;
	fprintf(fp,"  : typerefs & symrefs=\n");
	for (j=0; j<p->nsym; j++) {
	  ODB_Type *type = p->type[j];
	  int linkmode;
	  s = symref(type ? type->type : NULL);
	  fprintf(fp,"\t%p : %s",type,s);
	  FREE(s);
	  if (expname) {
	    s = expname[j] ? expname[j] : NULL;
	    fprintf(fp," & expname='%s'",s?s:NIL);
	  }
	  s = symref(p->sym[j]);
	  linkmode = p->linkmode[j];
	  fprintf(fp," & %s & linkmode = %d\n",s,linkmode);
	  FREE(s);
	}
      } /* if (p->nsym > 0) */

      if (p->nlink > 0) {
	int nlink = p->nlink; 
	fprintf(fp,"  : nlink=%d\n",nlink);
	for (j=0; j<nlink; j++) {
	  char *s = p->linkname[j];
	  fprintf(fp,"\tlink#%d, linkname='%s' ",j,s?s:NIL);
	  if (p->link[j]) {
	    s = symref(p->link[j]->table);
	    fprintf(fp,": tableref=%p, symref=%s\n",
		    p->link[j],s);
	    FREE(s);
	  }
	  else {
	   fprintf(fp,": tableref=UNDEFINED\n"); 
	  }
	}
      } /* if (p->nlink > 0) */

    }
  }

  { /* View definitions */
    ODB_View *p = ODB_start_view();
    for (; p != NULL; p = p->next) {
      char *s = symref(p->view);
      fprintf(fp,"viewref=%p: symref=%s\n",p, s);
      FREE(s);
      fprintf(fp,"  : nselect=%d, nselect_all=%d, nfrom=%d, nwhere=%d, norderby=%d",
	      p->nselect, p->nselect_all, p->nfrom, p->nwhere, p->norderby);
      fprintf(fp,", WHERE-code is %s",
	      p->cond ? "present" : "NOT present");
      if (p->cond) fprintf(fp," at %p",p->cond);

      if (p->nselect > 0) {
	fprintf(fp,"  : symrefs(SELECT)=\n");
	for (j=0; j<p->nselect; j++) {
	  s = symref(p->select[j]);
	  fprintf(fp,"\t%s",s);
	  FREE(s);
	  if (p->table_index) {
	    fprintf(fp," : tid = %d", p->table_index[j]);
	  }
	  if (p->tag) {
	    fprintf(fp," : tag = '%s'",p->tag[j] ? p->tag[j] : NIL);
	  }
	  if (p->call_arg) {
	    fprintf(fp," : arg = '%s'",p->call_arg[j] ? p->call_arg[j] : NIL);
	  }
	  if (p->def_put && p->alias_put) {
	    fprintf(fp,"\n\t  : #define %s(d,i) %s",
		    p->def_put[j], p->alias_put[j]);
	  }
	  if (p->def_get && p->alias_get) {
	    fprintf(fp,"\n\t  : #define %s(d,i) %s",
		    p->def_get[j], p->alias_get[j]);
	  }
	  fprintf(fp,"\n");
	}
      } /* if (p->nselect > 0) */

      if (p->nfrom > 0) {
	fprintf(fp,"  : tablerefs(FROM)=\n");
	for (j=0; j<p->nfrom; j++) {
	  ODB_Table *table = p->from[j];
	  s = symref(table ? table->table : NULL);
	  fprintf(fp,"\t%p : %s\n",table,s);
	  FREE(s);
	}
      } /* if (p->nfrom > 0) */

      if (p->nwhere > 0) {
	int k;
	fprintf(fp,"  : symrefs(WHERE)=\n");
	for (j=0, k=p->nselect_all; j<p->nwhere; j++, k++) {
	  s = symref(p->where[j]);
	  fprintf(fp,"\t%s",s);
	  FREE(s);
	  if (p->table_index) {
	    fprintf(fp," : table_index = %d", p->table_index[k]);
	  }
	  if (p->tag) {
	    fprintf(fp," : tag = '%s'",p->tag[k] ? p->tag[k] : NIL);
	  }
	  if (p->def_put && p->alias_put) {
	    fprintf(fp,"\n\t  : #define %s(i,d) %s",
		    p->def_put[k]   ? p->def_put[k]   : NIL, 
		    p->alias_put[k] ? p->alias_put[k] : NIL);
	  }
	  if (p->def_get && p->alias_get) {
	    fprintf(fp,"\n\t  : #define %s(i,d) %s",
		    p->def_get[k]   ? p->def_get[k]   : NIL,
		    p->alias_get[k] ? p->alias_get[k] : NIL);
	  }
	  fprintf(fp,"\n");
	}
      } /* if (p->nwhere > 0) */

      if (p->norderby > 0) {
	int k;
	fprintf(fp,"  : symrefs(ORDERBY)=\n");
	for (j=0, k=p->nselect_all+p->nwhere; j<p->norderby; j++, k++) {
	  int key = p->mkeys[j];
	  s = symref(p->orderby[j]);
	  fprintf(fp,"\t%s : sorting key#%d",s,key);
	  FREE(s);
	  if (p->tag) {
	    fprintf(fp," : tag = '%s'",p->tag[k] ? p->tag[k] : NIL);
	  }
	  fprintf(fp,"\n");
	}
      } /* if (p->norderby > 0) */

    }
  }

  fprintf(fp,"*** End of ODB_print_symbols() ***\n");
}

PRIVATE ODB_dbcred *AssignDBCred()
{
  /* from yacc.y */
  extern char *YACC_current_dbname;
  extern char *YACC_current_srcpath;
  extern char *YACC_current_datapath;
  extern char *YACC_current_idxpath;
  extern char *YACC_current_poolmask;
  ODB_dbcred *p = NULL;
  CALLOC(p, 1);
  p->dbname   = STRDUP(YACC_current_dbname);
  p->srcpath  = STRDUP(YACC_current_srcpath);
  p->datapath = STRDUP(YACC_current_datapath);
  p->idxpath  = STRDUP(YACC_current_idxpath);
  p->poolmask = STRDUP(YACC_current_poolmask);
  return p;
}


PRIVATE char *
hash_massage(const char *s)
{
  char *p;
  int len = strlen(s) + 2;
  ALLOC(p, len);
  snprintf(p,len,"#%s",s);
  return p;
}

PRIVATE char *
s2d_join(const char *s, int count)
{
  char *p;
  int len = strlen(s) + 2;
  ALLOC(p,len);
  snprintf(p,len,"%s%d",s,count);
  return p;
}

PRIVATE char *
s2d_massage(const char *s, int kind)
{
  char *psm, *sm;

  if (kind == ODB_STRING) {
    /* Leading blank removal */
    while (*s) {
      if (*s != ' ') break;
      s++;
    }
  }

  {
    int len = S2DLEN + strlen(s) + 1;
    ALLOC(sm, len);
    snprintf(sm,len,"%s%s",S2D,s);
  }

  if (kind == ODB_STRING) {
    psm = sm + S2DLEN;
    while (*psm) {
      /* Replace non-printables with a question mark */
      if (!isprint(*psm)) *psm = '?';
      psm++;
    }
  }

  return sm;
}

PRIVATE double
s2d(const char *s, int kind)
{
  const int ns = sizeof(double);
  union {
    double dval;
    char   s[sizeof(double)+1];
  } u;
  int len = strlen(s);

  memset(u.s,' ',ns);

  if (len < ns && kind == ODB_STRING) {
    /* Right justify */
    int offset = ns - len;
    strncpy(&u.s[offset],s,len);
  }
  else {
    strncpy(u.s, s, ns);
  }
  u.s[ns] = '\0';

  /* fprintf(stderr,"s2d: in='%s', out='%s'\n",s,u.s); */

  return u.dval;
}


PRIVATE char *
d2s(double d)
{
  const int ns = sizeof(double);
  union {
    double dval;
    char   s[sizeof(double)+1];
  } u;
  u.dval = d;
  u.s[ns] = '\0';
  return STRDUP(u.s);
}


PUBLIC Boolean
ODB_is_oper(void *p)
{
  Boolean is_oper = 0;
  if (p) {
    ODB_Tree *ptest = p;
    ODB_Tree *pnode = ODB_start_oper();
    for (; pnode != NULL; pnode = pnode->next) {
      if (pnode == ptest) {
	is_oper = 1;
	break;
      }
    }
  }
  return is_oper;
}


PUBLIC Boolean
ODB_is_symbol(void *p)
{
  Boolean is_symbol = 0;
  if (p) {
    ODB_Symbol *ptest = p;
    ODB_Symbol *psym = ODB_start_symbol();
    for (; psym != NULL; psym = psym->next) {
      if (psym == ptest) {
	is_symbol = 1;
	break;
      }
    }
  }
  return is_symbol;
}


PUBLIC ODB_Symbol *
ODB_lookup(int kind, const char *name, ODB_Symbol *start_symbol)
{
  if (name) {
    ODB_Symbol *psym = start_symbol ? start_symbol : ODB_start_symbol();
    if (verbose) fprintf(stderr,
			 "ODB_lookup: kind=(%d,%s), name='%s'\n",
			 kind, ODB_keymap(kind), name);
    if (kind == ODB_STRING || kind == ODB_WC_STRING) {
      for (; psym != NULL; psym = psym->next) {
	if (psym->kind == kind && psym->dname) {
	  char *s = s2d_massage(name, kind);
	  if (strequ(psym->dname, &s[S2DLEN])) {
	    FREE(s);
	    return psym;
	  }
	  FREE(s);
	}
      }
    }
    else if (LEX_in_where && kind == ODB_USDNAME && IS_USDDOTHASH(name)) {
      /* Disallow use of "$<parent_tblname>.<child_tblname>#" in WHERE-stmt */
      SETMSG1("It is forbidden to use '$parent.child#' -variable (now '%s') in WHERE-statement",name);
      YYwarn(1,msg); /* Warn only ,but will most likely abort later due to this */
      return NULL; 
    }
    else { /* Speeded up lookup */
      uint hash = ODB_hash(kind, name);
      for (; psym != NULL; psym = psym->next) {
	if (psym->hash == hash && psym->kind == kind && strequ(psym->name, name)) return psym;
      }
    }
  }
  return NULL; /* No symbol found */
}

PUBLIC ODB_Type *
ODB_lookup_type(const char *name, ODB_Type *start_type)
{
  ODB_Type *ptype = start_type ? start_type : ODB_start_type();

  for (; ptype != NULL; ptype = ptype->next) {
    ODB_Symbol *psym = ptype->type;
    if (strequ(psym->name, name)) return ptype;
  }

  return NULL; /* No type found */
}

PUBLIC ODB_Table *
ODB_lookup_table(const char *name, ODB_Table *start_table)
{
  ODB_Table *ptable = start_table ? start_table : ODB_start_table();
  uint hash = ODB_hash(ODB_TABLE, name);
  for (; ptable != NULL; ptable = ptable->next) {
    ODB_Symbol *psym = ptable->table;
    if (psym->hash == hash && strequ(psym->name, name)) return ptable;
  }

  return NULL; /* No table found */
}


PUBLIC Boolean
ODB_in_table(int kind, const char *name, const ODB_Table *ptable, int *index)
{
  /* "name" can be a (a) string "#table_name"
                     (b) column entry name, when kind = ODB_NAME
		     (c) column type name, when kind = ODB_TYPE
  */
  Boolean in_table = 0;

  if (index) *index = -1; /* Not found (or a HASH-symbol) */

  if (name && ptable) {
    if (IS_HASH(name)) {
      char *p = hash_massage(ptable->table->name);
      in_table = strequ(p,name);
      FREE(p);
    }
    else {
      int j, nsym = ptable->nsym;
      for (j=0; j<nsym; j++) {
	ODB_Symbol *psym = (kind == ODB_TYPE) ? ptable->type[j]->type : ptable->sym[j];
	if (psym && strequ(psym->name, name)) {
	  if (index) *index = j;
	  in_table = 1;
	  break;
	}
      } /* for (j=0; j<nsym; j++) */
    }
  }

  return in_table;
}


PUBLIC Boolean
ODB_in_type(const char *name, const ODB_Type *ptype, int *index)
{
  Boolean in_type = 0;

  if (index) *index = -1; /* Not found */

  if (name && ptype) {
    int j, nsym = ptype->nsym;

    for (j=0; j<nsym; j++) {
      ODB_Symbol *psym = ptype->member[j];
      if (strequ(psym->name, name)) {
	if (index) *index = j;
	in_type = 1;
	break;
      }
    } /* for (j=0; j<nsym; j++) */
  }

  return in_type;
}


PUBLIC ODB_View *
ODB_lookup_view(const char *name, ODB_View *start_view)
{
  ODB_View *pview = start_view ? start_view : ODB_start_view();

  for (; pview != NULL; pview = pview->next) {
    ODB_Symbol *psym = pview->view;
    if (strequ(psym->name, name)) return pview;
  }

  return NULL; /* No type found */
}


PUBLIC ODB_Symbol *
ODB_new_symbol(int kind, const char *s)
{
  ODB_Symbol *psym = ODB_lookup(kind, s, NULL);

  if (!psym) {
    Boolean okay = 0;
    okay |= (kind == ODB_NAME);
    okay |= (kind == ODB_NICKNAME);
    okay |= (kind == ODB_STRING);
    okay |= (kind == ODB_WC_STRING);
    okay |= (ODB_in_tabledef && (kind == ODB_TYPE));
    okay |= (ODB_in_tabledef && (kind == ODB_USDNAME));

    if (!okay) {
      if (strchr(s, '@') || strchr(s, '.')) {
	SETMSG3("Symbol '%s' of kind=(%d,%s) cannot contain \"@\" or \".\"",
		s, kind, ODB_keymap(kind));
	YYerror(msg);
      }
    }

    if (verbose) fprintf(stderr,
			 "ODB_new_symbol: kind=(%d,%s), name='%s'\n",
			 kind, ODB_keymap(kind), s);

    CALLOC(psym, 1);

    if (first_symbol)  
      last_symbol->next = psym;
    else 
      first_symbol = psym;
    last_symbol = psym;

    psym->kind = kind;

    if (kind == ODB_STRING || kind == ODB_WC_STRING) {
      char *sm = s2d_massage(s, kind);
      psym->name = s2d_join(S2D,ODB_hollerith_strings);
      psym->dname = STRDUP(&sm[S2DLEN]);
      psym->dval = s2d(psym->dname, kind);
      psym->sorig = STRDUP(s);
      FREE(sm);
    }
    else if (kind == ODB_HASHNAME) {
      psym->name = hash_massage(s+1);
      psym->dname = NULL;
      psym->dval = 0;
      psym->sorig = NULL;
    }
    else {
      psym->name = STRDUP(s);
      psym->dname = NULL;
      psym->dval = 0;
      psym->sorig = NULL;
    }
    psym->hash = ODB_hash(psym->kind, psym->name);

    psym->flag = 0;
    psym->only_view = 0;
    psym->next = NULL;

    if (kind == ODB_USDNAME || IS_DOLLAR(s)) {
      has_USD_symbols = 1;
      ODB_nUSDsymbols++;
      psym->only_view = (ODB_tables_done || ODB_ntables > 0);
    }
    else if (kind == ODB_STRING || kind == ODB_WC_STRING)
      ODB_hollerith_strings++;
    else if (kind == ODB_HASHNAME)
      ODB_nHASHsymbols++;
    else if (kind == ODB_BSNUM)
      ODB_nBSNUMsymbols++;
    else 
      ODB_nsymbols++;
  }

  return psym;
}


PUBLIC ODB_Symbol *
ODB_symbol_copy(ODB_Symbol *psym)
{
  ODB_Symbol *p = psym ? ODB_new_symbol(psym->kind, psym->name) : NULL;
  return p;
}


PUBLIC ODB_Type *
ODB_new_type(const char *s, Boolean reuse_okay)
{
  ODB_Type *ptype = ODB_lookup_type(s, NULL);

  if (ptype && !reuse_okay) {
    SETMSG1("An attempt to redefine an existing type '%s'",s);
    YYerror(msg);
  }

  if (!ptype) {
    ODB_Symbol *psym = ODB_new_symbol(ODB_TYPE, s);

    ALLOC(ptype, 1);
    
    if (first_type)  
      last_type->next = ptype;
    else 
      first_type = ptype;
    last_type = ptype;
    
    ptype->type = psym;
    ptype->nsym = 0;
    ptype->sym  = NULL;
    ptype->member = NULL;
    
    ptype->bitstream = 0;
    ptype->no_members = 0;
    ptype->processed = 0;
    ptype->pos = NULL;
    ptype->len = NULL;
    
    ptype->next = NULL;

    ODB_ntypes++;
  }    

  return ptype;
}

PUBLIC ODB_Table *
ODB_copy_table(const ODB_Table *from, const char *name)
{
  ODB_Table *p = ODB_lookup_table(name, NULL);

  if (p && name) {
    ODB_Table *ptable = ODB_start_table();
    ODB_Table *prev = NULL;
    
    for (; ptable != NULL; ptable = ptable->next) {
      if (ptable == p) {
	/* The "ptable" becomes obsolete and will be replaced by the "new" */
	ODB_Table *new;
	ALLOC(new, 1);
	/* Copy "from" to "new" ; as-is */
	memcpy(new, from, sizeof(*new));
	/*
	  fprintf(stderr,
		"$$$ copy_table: from=('%s' at %p), to='%s' <i.e. '%s' at %p>\n",
		from->table->name, from,
		name,
		ptable->table->name, ptable);
	*/
	/* .. but retain some old entries */
	new->table = ptable->table;
	new->tableno = ptable->tableno;
	new->rank = ptable->rank;
	new->wt = ptable->wt;
	/* correct expanded names */
	{
	  int nsym = new->nsym;
	  int j;
	  ALLOC(new->expname, nsym);
	  for (j=0; j<nsym; j++) {
	    char *x;
	    char *oldname = STRDUP(from->expname[j]);
	    x = strchr(oldname,'@');
	    if (x) {
	      char *newname;
	      int newlen;
	      *++x = '\0';
	      newlen = strlen(oldname) + strlen(name) + 1;
	      ALLOC(newname, newlen);
	      snprintf(newname,newlen,"%s%s",oldname,name);
	      new->expname[j] = newname;
	    }
	    else {
	      new->expname[j] = STRDUP(oldname);
	    }
	    FREE(oldname);
	  } /* for (j=0; j<nsym; j++) */
	}
	/* Don't forget to update the chain */
	new->next = ptable->next;
	if (prev) prev->next = new;
	if (ptable == first_table) first_table = new;
	if (ptable == last_table)  last_table = new;
	p = new;
	break;
      }
      prev = ptable;
    } /* for (; ptable != NULL; ptable = ptable->next) */
  } /* if (p) */

  return p;
}

PUBLIC ODB_Table *
ODB_new_table(const char *s, Boolean reuse_okay)
{
  ODB_Table *ptable = ODB_lookup_table(s, NULL);

  if (ptable && !reuse_okay) {
    SETMSG1("An attempt to redefine an existing table '%s'",s);
    YYerror(msg);
  }

  if (ODB_tables_done) {
    SETMSG1("No more table defs allowed after view defs. Occured at table '%s'",s);
    YYerror(msg);
  }

  if (!ptable) {
    ODB_Symbol *psym = ODB_new_symbol(ODB_TABLE, s);

    ALLOC(ptable, 1);

    if (first_table)  
      last_table->next = ptable;
    else 
      first_table = ptable;
    last_table = ptable;

    ptable->table = psym;
    ptable->tableno = ODB_ntables++;
    ptable->rank = ptable->tableno; /* the default in absence of a better value so far */
    ptable->nsym = 0;
    ptable->wt = 0;
    ptable->type = NULL;
    ptable->sym  = NULL;
    ptable->expname = NULL;
    ptable->linkslavemask = NULL;
    ptable->nlink = 0;
    ptable->link  = NULL;
    ptable->sharedlink = NULL;
    ptable->sharedlinkname = NULL;
    ptable->any_sharedlinks = 0;

    ptable->db = AssignDBCred();

    ptable->next = NULL;
  }    

  return ptable;
}

PUBLIC const char *
ODB_NickName(int colnum)
{
  /* The default nickname for each SQL-column : 
     un-addressable  "<a><number>" where <a> is ARTIFICIAL_NICKCHAR = \a in defs.h */
  static char s[20];
  snprintf(s,sizeof(s),"%c%d",ARTIFICIAL_NICKCHAR,colnum);
  return s;
}

PUBLIC ODB_View *
ODB_new_view(const char *s, Boolean reuse_okay)
{
  extern int use_indices;
  extern char *use_index_name;
  extern int reorder_tables;
  extern int insert_tables;
  extern Boolean LEX_binary_index;
  ODB_View *pview = ODB_lookup_view(s, NULL);

  if (pview && !reuse_okay) {
    SETMSG1("An attempt to redefine an existing view '%s'",s);
    YYerror(msg);
  }

  if (!pview) {
    ODB_Symbol *psym = ODB_new_symbol(ODB_VIEW, s);

    CALLOC(pview, 1);

    if (first_view)  
      last_view->next = pview;
    else 
      first_view = pview;
    last_view = pview;

    pview->view    = psym;

    pview->db = AssignDBCred();

    pview->create_index = 0;
    pview->binary_index = LEX_binary_index;

    pview->nselect = 0;
    pview->nselect_all = 0;
    pview->select  = NULL;

    pview->sel = NULL;
    pview->is_formula = NULL;
    pview->has_formulas = 0;
    pview->nselsym = 0;
    pview->selsym = NULL;

    pview->ncols_aux = 0;

    pview->select_distinct = 0;
    pview->usddothash = 0;
    pview->use_indices = use_indices;
    pview->use_index_name = use_index_name ? STRDUP(use_index_name) : STRDUP("*");
    pview->reorder_tables = reorder_tables;
    pview->insert_tables = insert_tables;
    pview->has_count_star = 0;
    pview->safeGuard = 0;

    pview->select_aggr_flag = ODB_AGGR_NONE;

    pview->no_from_stmt = 0;
    pview->nfrom_start = 0;
    pview->nfrom   = 0;
    pview->from    = NULL;
    pview->from_attr = NULL;
    pview->maxfrom = INT_MAX;
    pview->active  = NULL;
    pview->merged_with = NULL;

    pview->nwhere  = 0;
    pview->where   = NULL;

    pview->nuniqueby = 0;
    pview->uniqueby = NULL;

    pview->has_thin = 0;

    pview->norderby = 0;
    pview->orderby = NULL;
    pview->mkeys = NULL;

    pview->table_index = NULL;
    pview->tag = NULL;

    pview->call_arg = NULL;

    pview->def_put = NULL;
    pview->alias_put = NULL;

    pview->def_get = NULL;
    pview->alias_get = NULL;

    pview->poslen = NULL;

    pview->cond = NULL;
    pview->andlist = NULL;
    pview->andlen = 0;

    pview->next = NULL;

    ODB_nviews++;
  }    

  return pview;
}

PUBLIC int
ODB_remove_duplicates(SymbolParam_t *in, 
		      int  *table_index,
		      char **tag,
		      char **call_arg,
		      char **def_put,
		      char **alias_put,
		      char **def_get,
		      char **alias_get,
		      char **poslen,
		      char **offset)
{
  int ndrem = 0;
  if (in) {
    int j;
    int nsym = in->nsym;

    init_list(NULL);
    for (j=0; j<nsym; j++) {
      char *s = tag ? tag[j] : in->sym[j]->name;
      char *p;
      int len = strlen(s) + 3;
      ALLOC(p, len);
      snprintf(p, len, "/%s/", s);
      if (!in_list(p)) {
	add_list(p);
	in->sym[ndrem] = in->sym[j];
	in->readonly[ndrem] = in->readonly[j];
	if (in->sel) in->sel[ndrem] = in->sel[j];
	if (table_index) table_index[ndrem] = table_index[j];
	if (tag) tag[ndrem] = STRDUP(tag[j]);
	if (call_arg) call_arg[ndrem] = STRDUP(call_arg[j]);
	if (def_put) def_put[ndrem] = STRDUP(def_put[j]);
	if (alias_put) alias_put[ndrem] = STRDUP(alias_put[j]);
	if (def_get) def_get[ndrem] = STRDUP(def_get[j]);
	if (alias_get) alias_get[ndrem] = STRDUP(alias_get[j]);
	if (poslen) poslen[ndrem] = STRDUP(poslen[j]);
	if (offset) offset[ndrem] = STRDUP(offset[j]);
	ndrem++;
      }
      FREE(p);
    } /* for (j=0; j<nsym; j++) */
    destroy_list();

    if (table_index) {
      for (j=ndrem; j<nsym; j++) {
	table_index[j] = RMDI;
      } /* for (j=ndrem; j<nsym; j++) */
    } /* if (table_index) */

    in->nsym = ndrem;
  }
  return ndrem;
}


PUBLIC Boolean 
ODB_dupl_symbols(ODB_Symbol **psym, int nsym, Boolean extract)
{
  Boolean dupl_exist = 0;
  int dupl_count = 0;
  int j;

  init_list(NULL);

  if (extract) {
    for (j=0; j<nsym; j++) {
      char *name = psym[j]->name;
      if (!strnequ(name,"LINKLEN(",8)) {
	char *s = ODB_extract(name,'(',')');
	char *p;
	int len = strlen(s) + 3;

	ALLOC(p, len);
	snprintf(p, len, "/%s/", s);
	
	if (!in_list(p)) {
	  add_list(p);
	  FREE(p);
	}
	else {
	  FREE(p);
	  dupl_count++;
	}

	FREE(s);
      }
    } /* for (j=0; j<nsym; j++) */
  }
  else {
    for (j=0; j<nsym; j++) {
      char *s = psym[j]->name;
      char *p;
      int len = strlen(s) + 3;

      ALLOC(p, len);
      snprintf(p, len, "/%s/", s);
      
      if (!in_list(p)) {
	add_list(p);
	FREE(p);
      }
      else {
	FREE(p);
	dupl_count++;
      }
    } /* for (j=0; j<nsym; j++) */
  }

  destroy_list();

  dupl_exist = (dupl_count > 0);

  return dupl_exist;
}


PUBLIC void
ODB_link_massage()
{
  if (!ODB_tables_done) {
    /* Resolve any outstanding links to the other tables */

    ODB_Table *ptable;
    Boolean on_error = 0;
    ODB_Table **all_tables = NULL;

    if (debug) {
      int j = 0;
      CALLOC(all_tables, ODB_ntables);
      for (ptable = ODB_start_table() ; ptable != NULL; ptable = ptable->next) {
	all_tables[j++] = ptable;
      }
    }
    
    for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
      int nlink = ptable->nlink;

      CALLOC(ptable->linkslavemask,ODB_ntables);
      
      if (nlink > 0) {
	int j;
	char *tablename = ptable->table->name;

	ODB_has_links = 1;
	
	for (j=0; j<nlink; j++) {
	  char *xx = ptable->linkname[j];
	  char *yy = ptable->sharedlinkname[j];
	  /* fprintf(stderr,"ODB_link_massage(): <%s> : j/nlink=%d,%d : xx='%s', yy='%s'\n",
		  tablename, j,nlink, xx, yy); */
	  if (!ptable->link[j]) {
	    char *linkname = ptable->linkname[j];
	    ODB_Table *p = ODB_lookup_table(linkname, NULL);
	    if (!p) {
	      fprintf(stderr,
		      "Unable to establish a link from table '%s' to the undefined table '%s'\n",
		      tablename, linkname);
	      on_error = 1;
	    }
	    ptable->link[j] = p;
	  } /* if (!ptable->link[j]) */

	  /* Shareable links (if any) */
	  if (ptable->sharedlinkname[j] && !ptable->sharedlink[j]) {
	    char *linkname = ptable->sharedlinkname[j];
	    ODB_Table *p = ODB_lookup_table(linkname, NULL);
	    if (!p) {
	      fprintf(stderr,
		      "Unable to establish shareable link from table '%s' to a non-existent table '%s'\n",
		      tablename, linkname);
	      on_error = 1;
	    }
	    ptable->sharedlink[j] = p;
	  } /* Shareable links (if any) */
	} /* for (j=0; j<nlink; j++) */
      } /* if (nlink > 0) */
    } /* for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) */
    
    if (on_error) YYerror("Table linking failed");

    process_one_tables(NULL,NULL,NULL);

    { 
      /* 
	 Assign weights for use by FROM-stmt based on how the links span 
         Fill in the linkslavemask, too 
      */

      ODB_linklist *list = manage_linklist(FUNC_LINKLIST_START,NULL,NULL,0);
      int j;
      const double delta_link = 1;
      const double delta_align = 1;
      const double delta_onelooper = 1;

      for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	int nlink = ptable->nlink;
	if (nlink > 0) {
	  int jtbl = ptable->tableno;
	  for (j=0; j<nlink; j++) {
	    ODB_Table *p = ptable->link[j];
	    p->linkslavemask[jtbl] = 4; /* Is behind a link */
	    p->wt += delta_link; /* Increase weight by delta for direct links (on slave-side only) */
	  } /* for (j=0; j<nlink; j++) */
	} /* if (nlink > 0) */
      }

      if (debug) {
	int j;
	fprintf(stderr,"Initial weights :\n");
	for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	  fprintf(stderr,"[%d] ptable->table->name = '%s' : initial weight = %.6f\n",
		  ptable->tableno, ptable->table->name, ptable->wt);
	  for (j=0; j<ODB_ntables; j++) {
	    fprintf(stderr," %s=%d",
		    all_tables[j]->table->name,
		    (int)ptable->linkslavemask[j]);
	  }
	  fprintf(stderr,"\n");
	}
      }

      if (list) { /* Indirect LINKs (via ALIGN/ONELOOPER) */
	ODB_Table **ptcache = NULL;
	CALLOC(ptcache,ODB_ntables);

	if (debug) fprintf(stderr,
			   "ptcache(ODB_ntables=%d) : Indirect LINKs (via ALIGN/ONELOOPER) : list=%p\n",
			   ODB_ntables, list);

	while (list) {
	  if (debug) fprintf(stderr,
			     "list : n_rhs=%d, lhs=%s, type=%d (only #1, ONELOOPER or #2, ALIGN considered)\n",
			     list->n_rhs, list->lhs ? list->lhs : NIL, list->type);
	  if (list->n_rhs > 0 && 
	      (list->type == 1 || list->type == 2)) {
	    int n = 0;

	    { /* Calculate n (= count of existing tables) and 
		 fill ptcache (= participating & existing tables) */
	      struct _rhs_t *first_rhs = list->rhs;
	      ptable = ODB_lookup_table(list->lhs, NULL);
	      if (ptable) ptcache[n++] = ptable;
	      while (first_rhs) {
		ptable = ODB_lookup_table(first_rhs->name, NULL);
		if (ptable) ptcache[n++] = ptable;
		first_rhs = first_rhs->next;
	      } /* while (first_rhs) */
	    }

	    if (debug) fprintf(stderr, "\tptcache(n=%d)\n",n);

	    if (n > 1) {
	      int j1, j2, jtbl;
	      if (list->type == 2) { /* ALIGN'ed (or -A) tables */
		/* 
		   Add delta-weight to BOTH tables' weights, but make sure
		   you do this only once, since there maybe redundant
		   -A or ALIGN-statements for a subset of tables; like
		   -Aa=(b,c,d)
		   -Ab=(c,d)
		*/
		for (j1=0; j1<n; j1++) {
		  ptable = ptcache[j1];
		  jtbl = ptable->tableno;
		  for (j2=j1+1; j2<n; j2++) {
		    ODB_Table *p = ptcache[j2];
		    int jj = p->tableno;
		    if ((ptable->linkslavemask[jj] & list->type) != list->type) { /* "master" */
		      ptable->wt += delta_align;
		      ptable->linkslavemask[jj] |= list->type; /* No more updates for this */
		    }
		    if ((p->linkslavemask[jtbl] & list->type) != list->type) { /* "slave" */
		      p->wt += delta_align;
		      p->linkslavemask[jtbl] |= list->type; /* No more updates for this */
		    }
		  } /* for (j2=j1+1; j2<n; j2++) */
		} /* for (j1=0; j1<n; j1++) */
	      }
	      else if (list->type == 1) { /* ONELOOPER's (or -1) tables */
		/*
		  Add delta-weight to slave-table only and also make sure
		  this is done only once (the reason is the same as with ALIGN).
		  Also add delta-weight only if master & slave have a true "LINK-manship"
		  i.e. linktype[] & 4 at slave is equal to 4
		*/
		for (j1=0; j1<n; j1++) {
		  ptable = ptcache[j1];
		  jtbl = ptable->tableno;
		  for (j2=j1+1; j2<n; j2++) {
		    ODB_Table *p = ptcache[j2];
		    int jj = p->tableno;
		    if ((ptable->linkslavemask[jj] & list->type) != list->type) {
		      if ((ptable->linkslavemask[jj] & 4) == 4) { /* I'm a slave */
			ptable->wt += delta_onelooper;
			ptable->linkslavemask[jj] |= list->type; /* No more updates for this */
		      }
		    }
		    if ((p->linkslavemask[jtbl] & list->type) != list->type) {
		      if ((p->linkslavemask[jtbl] & 4) == 4) { /* I'm a slave */
			p->wt += delta_onelooper;
			p->linkslavemask[jtbl] |= list->type; /* No more updates for this */
		      }
		    }
		  } /* for (j2=j1+1; j2<n; j2++) */
		} /* for (j1=0; j1<n; j1++) */
	      } /* if (list->type == 2) ... else if (list->type == 1) ... */
	    } /* if (n > 1) */
	  } /* if (list->n_rhs > 0 && ... */
	  list = list->next;
	} /* while (list) */
	FREE(ptcache);
      } /* if (list) */

      if (debug) {
	int j;
	fprintf(stderr,"Intermediate, stage#2 weights :\n");
	for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	  fprintf(stderr,"[%d] ptable->table->name = '%s' : intermed. weight = %.6f : mask=\n",
		  ptable->tableno, ptable->table->name, ptable->wt);
	  for (j=0; j<ODB_ntables; j++) {
	    fprintf(stderr," %s=%d",
		    all_tables[j]->table->name,
		    (int)ptable->linkslavemask[j]);
	  }
	  fprintf(stderr,"\n");
	}
      }

      /* Now make sure that ALIGNed tables have themselves the same weight
	 Iterate until convergence (as in "final order" below) */
      {
	int changed;
	int iter = 0;
	const int maxiter = 2 * ODB_ntables;
	ODB_linklist *saved_list = manage_linklist(FUNC_LINKLIST_START,NULL,NULL,0);
	ODB_Table **ptcache = NULL;
	CALLOC(ptcache,ODB_ntables);

	if (debug) fprintf(stderr,
			   "ptcache(ODB_ntables=%d) : Iterate until convergence : saved_list=%p\n",
			   ODB_ntables, saved_list);
	do {
	  changed = 0;
	  list = saved_list;
	  while (list) {
	    if (debug) fprintf(stderr,
			       "list : n_rhs=%d, lhs=%s, type=%d (only #2, ALIGN considered)\n",
			       list->n_rhs, list->lhs ? list->lhs : NIL, list->type);
	    if (list->n_rhs > 0 && 
		list->type == 2) { /* 2 --> consider ALIGNed (or -A) tables only */
	      int n = 0;
	      
	      { /* Calculate n (= count of existing tables) and 
		   fill ptcache (= participating & existing tables) */
		struct _rhs_t *first_rhs = list->rhs;
		ptable = ODB_lookup_table(list->lhs, NULL);
		if (ptable) ptcache[n++] = ptable;
		while (first_rhs) {
		  ptable = ODB_lookup_table(first_rhs->name, NULL);
		  if (ptable) ptcache[n++] = ptable;
		  first_rhs = first_rhs->next;
		} /* while (first_rhs) */
	      }
	      
	      if (debug) fprintf(stderr, "\tptcache(n=%d)\n",n);

	      if (n > 1) {
		int jtbl;
		double maxwt = 0; /* Puts tables lower in the hierarchy */
		for (jtbl=0; jtbl<n; jtbl++) {
		  ODB_Table *p = ptcache[jtbl];
		  maxwt = MAX(maxwt,p->wt);
		} /* for (jtbl=0; jtbl<n; jtbl++) */
		for (jtbl=0; jtbl<n; jtbl++) {
		  ODB_Table *p = ptcache[jtbl];
		  if (p->wt != maxwt) {
		    p->wt = maxwt;
		    changed++;
		  }
		} /* for (jtbl=0; jtbl<n; jtbl++) */
	      } /* if (n > 1) */
	    } /* if (list->n_rhs > 0 && ... */
	    list = list->next;
	  } /* while (list) */
	} while (changed && ++iter < maxiter);

	FREE(ptcache);

	if (changed && iter >= maxiter) { /* Abort */
	  SETMSG1("Cannot resolve ALIGNed tables' ordering in %d iterations. Check for possible table-recursion",iter);
	  YYerror(msg);
	}
      }

      if (debug) {
	int j;
	fprintf(stderr,"Intermediate, stage#2 weights :\n");
	for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	  fprintf(stderr,"[%d] ptable->table->name = '%s' : intermed. weight = %.6f : mask=\n",
		  ptable->tableno, ptable->table->name, ptable->wt);
	  for (j=0; j<ODB_ntables; j++) {
	    fprintf(stderr," %s=%d",
		    all_tables[j]->table->name,
		    (int)ptable->linkslavemask[j]);
	  }
	  fprintf(stderr,"\n");
	}
      }

      /* The semi-final order : a tie-breaker
	 Add "seed" to weight from initial table ordering (1 millionth)
       */

      {
	const double eps = 0.000001;
	for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	  ptable->wt += eps * ptable->tableno;
	}
      }

      /* The final order : 
	 Adjust weights so that master (parent) table can never have 
	 larger weight than the slave (child) table.
	 This examination and alteration goes on over the genuine links only.
	 As result, the smaller the weight, the higher in the hierarchy
	 the FROM-table ends up being placed.
      */

      {
	const double eps = 0.001;
	int changed;
	int iter = 0;
	const int maxiter = 2 * ODB_ntables;
	do {
	  changed = 0;
	  for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	    int nlink = ptable->nlink;
	    if (nlink > 0) { /* Check only genuine links (for now) */
	      double parent_wt = ptable->wt;
	      for (j=0; j<nlink; j++) {
		ODB_Table *p = ptable->link[j];
		if (p->wt <= parent_wt) {
		  /* child receives parent weight 
		     (+ epsilon to make it really bigger i.e. lower in the hierarchy) */
		  p->wt = parent_wt + eps;
		  changed++;
		}
	      } /* for (j=0; j<nlink; j++) */
	    } /* if (nlink > 0) */
	  }
	} while (changed && ++iter < maxiter);

	if (changed && iter >= maxiter) { /* Abort */
	  SETMSG1("Cannot resolve table ordering in %d iterations. Check for possible table-recursion",iter);
	  YYerror(msg);
	}
      }

      { /* Calculate rank order number in the hierarchy */
	int j;
	Cmptable_t *cmp = NULL;
	ALLOC(cmp, ODB_ntables);
	for (ptable = ODB_start_table(), j=0; ptable != NULL; ptable = ptable->next, j++) {
	  cmp[j].wt = ptable->wt;
	  cmp[j].p  = ptable;
	}
	qsort(cmp, ODB_ntables, sizeof(*cmp), 
	      (int (*)(const void *, const void *))cmptable);
	ALLOC(table_hier, ODB_ntables);
	for (j=0; j<ODB_ntables; j++) {
	  table_hier[j] = cmp[j].p;
	  cmp[j].p->rank = j;
	}
	FREE(cmp);
      }

      if (debug) {
	int j;
	fprintf(stderr,"Final weights & rank :\n");
	for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	  fprintf(stderr,"[%d] [rank=%d] ptable->table->name = '%s' : final weight = %.6f, mask=\n",
		  ptable->tableno, ptable->rank, ptable->table->name, ptable->wt);
	  for (j=0; j<ODB_ntables; j++) {
	    fprintf(stderr," %s=%d",
		    all_tables[j]->table->name,
		    (int)ptable->linkslavemask[j]);
	  }
	  fprintf(stderr,"\n");
	}
      }
    }

    FREE(all_tables);

    ODB_tables_done = 1;
  }
}


/* === PRIVATE routines === */

PUBLIC int GetSign(const char *poffset)
{
  int sign = 0;
  if (poffset && !strequ(poffset,"0")) {
    if (*poffset == '-' || *poffset == '_') sign = -1;
    else if (*poffset == '+') sign = +1;
  }
  return sign;
}


PRIVATE char *ZZtop(const char *poffset, int sign) {
  char *zz = NULL;
  if (poffset && !strequ(poffset, "0")) {
    int len = 3 + STRLEN(poffset) + 1;
    ALLOC(zz, len);
    snprintf(zz, len, "_Z%s%s",
	     (sign < 0) ? "_" : "",
	     poffset+ABS(sign));
  }
  else
    zz = STRDUP("");
  return zz;
}

PRIVATE char *TheWho(const char *poffset, int sign) {
  char *zz = NULL;
  if (poffset && !strequ(poffset, "0")) {
    int len = 1 + STRLEN(poffset) + 1;
    ALLOC(zz, len);
    snprintf(zz, len, "%s%s",
	     sign ? "" : "+",
	     poffset+ABS(sign));
  }
  else
    zz = STRDUP("");
  return zz;
}

PRIVATE char *GreenDay(const char *poffset) {
  char *zz = NULL;
  if (poffset && !strequ(poffset, "0")) {
    int len = STRLEN(ODB_OFFSET_CHAR) + STRLEN(poffset) + 1;
    ALLOC(zz, len);
    snprintf(zz, len, "%s%s",
	     ODB_OFFSET_CHAR, poffset);
  }
  else
    zz = STRDUP("");
  return zz;
}

PRIVATE ODB_Tree *
new_node(int what)
{
  ODB_Tree *pnode;

  CALLOC(pnode, 1);

  if (first_oper)  
    last_oper->next = pnode;
  else 
    first_oper = pnode;
  last_oper = pnode;

  pnode->what    = what;
  pnode->dval    = 0;
  pnode->argc    = 0;
  pnode->argv    = NULL;
  pnode->level   = -1;
  pnode->joffset = 0;
  pnode->next    = NULL;

  ODB_nnodes++;

  return pnode;
}


PUBLIC ODB_Tree *
ODB_oper_copy(ODB_Tree *expr, Boolean recursive)
{
  ODB_Tree *pnode = NULL;
  if (expr) {
    pnode = new_node(expr->what);
    pnode->dval    = expr->dval;
    pnode->argc    = expr->argc;
    pnode->level   = expr->level;
    if (pnode->argc > 0) {
      int j, n = pnode->argc;
      ALLOC(pnode->argv, n);
      for (j=0; j<n; j++) {
	void *p = expr->argv[j];
	if (ODB_is_symbol(p)) {
	  pnode->argv[j] = ODB_symbol_copy(p);
	}
	else if (ODB_is_oper(p)) {
	  pnode->argv[j] = recursive ? ODB_oper_copy(expr->argv[j],1) : expr->argv[j];
	}
	else { /* Something else : do not copy */
	  pnode->argv[j] = expr->argv[j];
	}
      }
    }
    else {
      pnode->argv = NULL;
    }
  }
  return pnode;
}

typedef struct Alias_Type_t {
  char *true_type;
  char *your_type;
  struct Alias_Type_t *next;
} AliasType;

PRIVATE AliasType *alias_type_start = NULL;
PRIVATE AliasType *alias_type = NULL;

PRIVATE Boolean
add_alias(const char *true_type, const char *your_type)
{
  Boolean already_there = 0;
  AliasType *p;

  for (p = alias_type_start; p != NULL; p = p->next) {
    if (strequ(p->your_type, your_type)) {
      already_there = 1;
      break;
    }
  }

  if (!already_there) {
    ALLOC(p, 1);

    if (!alias_type_start) {
      /* Get started */
      alias_type_start = p;
      alias_type_start->true_type = NULL;
      alias_type_start->your_type = NULL;
      alias_type_start->next      = NULL;
    }

    /* Add it */
    p->true_type = STRDUP(true_type);
    p->your_type = STRDUP(your_type);
    p->next      = NULL;
    if (alias_type) alias_type->next = p;
    alias_type = p;
  }

  return already_there;
}

PRIVATE const char *
getalias(const char *your_type)
{
  const char *true_type = your_type;
  AliasType *p;

  for (p = alias_type_start; p != NULL; p = p->next) {
    if (strequ(p->your_type, your_type)) {
      true_type = p->true_type;
      break;
    }
  }
  return true_type;
}


PUBLIC int 
ODB_evaluate(ODB_Tree *pnode, double *dval)
     /* For compile-time expression evalution only */
{
  int irc = 0;

  *dval = 0;

  if (pnode) {
    switch (pnode->what) {

    case ODB_NUMBER:
      *dval = pnode->dval;
      irc = 1;
      break;

    case ODB_USDNAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *name = psym->name;
	if (!IS_POOLNO(name)) { /* Applicable to non-$# only */
	  *dval = psym->dval; /* current (compile-time) value of $-variable */
	  irc = 1;
	}
      }
      break;

    case ODB_ADD: 
    case ODB_SUB: 
    case ODB_STAR: 
    case ODB_DIV: 
    case ODB_UNARY_PLUS:
    case ODB_UNARY_MINUS:
      {
	double arg[2];
	int i, numargs = MIN(pnode->argc,2);

	if (numargs <= 0) {
	  SETMSG1("Zero number of args for operation '%s'",ODB_keymap(pnode->what));
	  YYerror(msg);
	  break;
	}

	irc = 1;
	for (i=0; i<numargs; i++) {
	  irc *= ODB_evaluate(pnode->argv[i], &arg[i]);
	}

	if (!irc) break;

	switch (pnode->what) {
	case ODB_ADD: 
	  *dval = arg[0] + arg[1];
	  break;
	case ODB_SUB: 
	  *dval = arg[0] - arg[1];
	  break;
	case ODB_STAR: 
	  *dval = arg[0] * arg[1];
	  break;
	case ODB_DIV: 
	  {
	    if (arg[1] != 0) 
	      *dval = arg[0] / arg[1];
	    else {
	      SETMSG0("Division by zero");
	      YYerror(msg);
	    }
	  }
	  break;

	case ODB_UNARY_PLUS:
	  *dval = arg[0];
	  break;

	case ODB_UNARY_MINUS:
	  *dval = -arg[0];
	  break;

	} /* switch (pnode->what) [inner] */
      }
      break;

    case ODB_FUNC:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *fname = psym->name;
	const funcs_t *f = get_func(fname);
	if (f && f->u.compile_time) {
	  int joffset = (f->numargs < 0) ? f->joffset : 0;
	  int i, numargs = pnode->argc - 1 - joffset;
	  Boolean arg_to_radians = (f->deg2rad > 0 && ((f->deg2rad & 0x1) == 0x1)) ? 1 : 0;
	  double *arg = NULL;
	  ALLOC(arg, numargs);
	  irc = 1;
	  for (i=0; i<numargs; i++) {
	    irc *= ODB_evaluate(pnode->argv[1+joffset+i], &arg[i]);
	    if (irc && arg_to_radians) arg[i] = D2R(arg[i]);
	    else if (irc && f->deg2rad < 0 && ABS(f->deg2rad) != i+1) { /* [hack] */
	      arg[i] = D2R(arg[i]);
	    }

	  }
	  if (irc) { /* Still ok --> now call the function itself */
	    double res = 0;
	    Boolean funcresult_to_degrees = (f->deg2rad > 0 && ((f->deg2rad & 0x2) == 0x2)) ? 1 : 0;
	    if (joffset == 0) {
	      if (f->numargs < 0) {
		res = f->u.vararg(numargs, arg); /* Whatever these functions might be */
	      }
	      else {
		switch (numargs) {
		case 0: res = f->u.zeroarg (); break;
		case 1: res = f->u.onearg  (arg[0]); break;
		case 2: res = f->u.twoarg  (arg[0], arg[1]); break;
		case 3: res = f->u.threearg(arg[0], arg[1], arg[2]); break;
		case 4: res = f->u.fourarg (arg[0], arg[1], arg[2], arg[3]); break;
		case 5: res = f->u.fivearg (arg[0], arg[1], arg[2], arg[3], arg[4]); break;
		case 6: res = f->u.sixarg  (arg[0], arg[1], arg[2], arg[3], arg[4], arg[5]); break;
		default: res = f->u.vararg (numargs, arg); break;
		}
	      }
	    }
	    else if (joffset == 1) { /* Usually min/max function */
	      res = f->u.vararg(numargs, arg); /* Note: Here "numargs" could even be 1 !! */
	    }
	    else { /* After all the hassle, cannot evaluate, since unrecognized joffset */
	      irc = 0;
	    }

	    if (irc && funcresult_to_degrees) res = R2D(res);
	    if (irc) *dval = res;
	  }
	  FREE(arg);
	}
	else {
	  irc = 0; /* not an evaluable function */
	}
      }
      break;

    case ODB_NAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *dname = psym->name;
        irc = ODB_has_define(dname);
        if (irc) *dval = ODB_get_define(dname);
      }
      break;

    } /* switch (pnode->what) [outer] */
  } /* if (pnode) */

  return irc;
}

PRIVATE double 
dval_assign(double *dval) { return *dval; }

PRIVATE int
ival_assign(int *ival) { return *ival; }

PUBLIC Boolean
ODB_split(const char *s, char **type, char **var, char **member, char **table, char **offset)
{
  Boolean rc = 0;
  char *p = STRDUP(s);
  char *pcolon = strchr(p, ':');
  char *pdot = IS_USDDOTHASH(p) ? NULL : strchr(p, '.');
  char *pat  = strchr(p, '@');
  char *ptype, *pvar, *pmember, *ptable;
  char *poffset = GET_OFFSET(p);
  if (poffset) ++poffset;

  if (type)   *type   = NULL;
  if (var)    *var    = NULL;
  if (member) *member = NULL;
  if (table)  *table  = NULL;
  if (offset) *offset = NULL;

  if (pdot && pat) {
    if (pat < pdot) {
      SETMSG1("An '@'-sign precedes a '.'-sign in SELECT/WHERE/ORDERBY-symbol '%s'\n",s);
      PRTMSG(msg);
      rc = 1;
      goto finish;
    }
  }

  ptype = pcolon ? p : NULL;
  pvar = pcolon ? pcolon + 1 : p;
  if (pcolon) *pcolon = '\0';
  ptable = pat ? pat + 1 : NULL;
  if (ptable && poffset) {
    char *offset_char = GET_OFFSET(ptable);
    if (offset_char) *offset_char = '\0';
  }
  else if (pvar && poffset) {
    char *offset_char = GET_OFFSET(pvar);
    if (offset_char) *offset_char = '\0';
  }
  if (pat) *pat = '\0';
  pmember = pdot ? pdot + 1 : NULL;
  if (pdot) *pdot = '\0';

  if (ptable) {
    char *pbrL = strchr(ptable,'[');
    char *pbrR = pbrL ? strchr(ptable,']') : NULL;

    if (pbrR - pbrL > 0) {
      char *x1, *x2;
      int len = (pbrL - ptable) + (pbrR - pbrL) + 1;
      char *ptable_actual = NULL;
      ALLOC(ptable_actual, len);
      x1 = ptable;
      x2 = ptable_actual;
      while (*x1 && x1 < pbrL) {
	*x2++ = *x1++;
      }
      *x2++ = '_';
      x1++;
      while (*x1 && x1 < pbrR) {
	*x2++ = *x1++;
      }
      *x2++ = '\0';
      ptable = STRDUP(ptable_actual);
      FREE(ptable_actual);
    } /* if (pbrR - pbrL > 0) */
  }

  if (type)   *type   = ptype   ? STRDUP(ptype)   : NULL;
  if (var)    *var    = STRDUP(pvar);
  if (member) *member = pmember ? STRDUP(pmember) : NULL;
  if (table)  *table  = ptable  ? STRDUP(ptable)  : NULL;
  if (offset) *offset = poffset ? STRDUP(poffset) : STRDUP("0");

 finish:

  FREE(p);
  return rc;
}


PRIVATE char *
join(int icase, const char *var, const char *member, 
     const ODB_Table *t, const ODB_Type *type,
     Boolean is_where, Boolean is_selectexpr, 
     const ODB_Symbol *psym, const char *offset)
{
  char *p = NULL;
  
  if (!IS_HASH(var)) {
    const char *poffset = offset ? offset : "0";
    int sign = 0;
    int len = 100;
    char *table = t ? t->table->name : NULL;
    Boolean bitstream = type ? type->bitstream : 0;
    char *typename = "undefined";
    Boolean not_valid_char = 0;
    int j;
    const char *c;
    char *xvar = NULL;
    
    ALLOC(xvar, strlen(var) + 1);
    c = var;
    j = 0;
    while ( *c ) {
      if (IS_VARCHAR(c)) xvar[j++] = *c;
      else not_valid_char = 1;
      c++;
    }
    xvar[j] = '\0';
    
    if (xvar) len += strlen(xvar);
    if (member) len += strlen(member);
    if (table) len += strlen(table);
    if (type) {
      typename = bitstream ? BITFIELD : type->type->name;
    }
    if (typename) len += strlen(typename);
    len += strlen(poffset) + 10;
    sign = GetSign(poffset);

    ALLOC(p, len);
    *p = '\0';

    switch (icase) {
    case -1: /* def_put */
      {
	if (is_selectexpr) { /* Must be consistent with ODB_expand_sym */
	  FREE(p);
	  p = ODB_expand_sym(psym, "F", table, poffset);
	}
	else {
	  char *ptable = ODB_uppercase(table);
	  char *pvar = not_valid_char ? STRDUP(xvar) : ODB_capitalize(xvar);
	  char *pmember = member ? ODB_capitalize(member) : STRDUP("all");
	  char *zz = ZZtop(poffset, sign);

	  snprintf(p, len, "%s%s_%s_%s_%s", ptable, zz,
		   pvar, pmember, 
		   is_where ? "EXPR" : "PUT");
	  
	  FREE(zz);
	  FREE(pmember);
	  FREE(pvar);
	  FREE(ptable);
	}
      }
      break;
      
    case -2: /* alias_put */
      {
	char *zz = TheWho(poffset, sign);
	if (bitstream && member) {
	  int index = -1;
	  Boolean member_present = ODB_in_type(member, type, &index);
	  
	  if (is_where) {
	    snprintf(p, len, "GET_BITS(T_%sX%s[i%s], %d, %d)", 
		     table, xvar, 
		     zz,
		     type->pos[index], type->len[index]);
	  }
	  else if (is_selectexpr) {
	    snprintf(p, len, "GET_BITS(T_%sX%s[P->Index_%s[i%s]], %d, %d)", 
		     table, xvar, table, 
		     zz,
		     type->pos[index], type->len[index]);
	  }
	  else {
	    snprintf(p, len, "PUT_BITS(T_%sX%s[i%s], d, %d, %d)", 
		     table, xvar,
		     zz,
		     type->pos[index], type->len[index]);
	  }
	}
	else {
	  if (is_where) {
	    if (member) 
	      snprintf(p, len, "T_%sX%s[i%s].%s", table, xvar, 
		       zz, member);
	    else
	      snprintf(p, len, "T_%sX%s[i%s]", table, xvar,
		       zz);
	  }
	  else if (is_selectexpr) {
	    if (member) 
	      snprintf(p, len, "T_%sX%s[P->Index_%s[i%s]].%s", table, xvar, table, 
		       zz, member);
	    else
	      snprintf(p, len, "T_%sX%s[P->Index_%s[i%s]]", table, xvar, table,
		       zz);
	  }
	  else {
	    if (member) 
	      snprintf(p, len, "T_%sX%s[i%s].%s = d", table, xvar, 
		       zz, member);
	    else
	      snprintf(p, len, "T_%sX%s[i%s] = d", table, xvar,
		       zz);
	  }
	}
	FREE(zz);
      }
      break;
      
    case 0: /* Tag */
      {
	char *zz = GreenDay(poffset);
	if (member)
	  snprintf(p, len, "%s:%s.%s@%s%s", 
		  typename, var, member, table, zz);
	else {
	  const char *actual_type = getalias(typename);
	  snprintf(p, len, "%s:%s@%s%s", actual_type, var, table, zz);
	}
	FREE(zz);
      }
      break;
      
    case 1: /* def_get */
      {
	char *ptable = ODB_uppercase(table);
	char *pvar = not_valid_char ? STRDUP(xvar) : ODB_capitalize(xvar);
	char *pmember = member ? ODB_capitalize(member) : NULL;
	char *zz = ZZtop(poffset, sign);
	
	if (pmember) 
	  snprintf(p, len, "%s%s_%s_%s_GET", ptable, zz,
		   pvar, pmember);
	else
	  snprintf(p, len, "%s%s_%s_all_GET", ptable, zz,
		   pvar);

	FREE(zz);
	FREE(pmember);
	FREE(pvar);
	FREE(ptable);
      }
      break;
      
    case 2: /* alias_get */
      {
	char *zz = TheWho(poffset, sign);
	if (bitstream && member) {
	  int index = -1;
	  Boolean member_present = ODB_in_type(member, type, &index);
	  
	  snprintf(p, len, "d = GET_BITS(T_%sX%s[i%s], %d, %d)", 
		   table, xvar, 
		   zz,
		   type->pos[index], type->len[index]);
	}
	else {
	  if (member) 
	    snprintf(p, len, "d = T_%sX%s[i%s].%s", table, xvar, zz, member);
	  else
	    snprintf(p, len, "d = T_%sX%s[i%s]", table, xvar, zz);
	}
	break;
	FREE(zz);
      }
      
    case 3: /* call_arg */
      {
	char *zz = ZZtop(poffset, sign);
	snprintf(p, len, "%s:T_%sX%s", typename, table, xvar);
	FREE(zz);
      }
      break;

    case 4: /* poslen */
      if (bitstream && member) {
	int index = -1;
	Boolean member_present = ODB_in_type(member, type, &index);
	snprintf(p, len, "%d %d", type->pos[index], type->len[index]);
      }
      else {
	snprintf(p, len, "0 0");
      }
      break;
      
    }
    
    FREE(xvar);
  }

  return p;
}


/* === PUBLIC routines === */


PUBLIC char *
ODB_expand_sym(const ODB_Symbol *psym, const char *suffix, 
	       const char *table, const char *offset)
{
  char *s = NULL;
  if (psym) { 
    int slen = 40;
    char *pvar = NULL;
    char *pmember = NULL;
    char *ptable = NULL;
    char *poffset = NULL;
    int sign = 0;
    (void) ODB_split(psym->name, NULL, &pvar, &pmember, &ptable, &poffset);
    if (!ptable && table) ptable = STRDUP(table);
    if (ptable) {
      char *p = ptable;
      while (*p) { if (islower(*p)) *p = toupper(*p); p++; }
    }
    else {
      ptable = STRDUP("no_table");
    }
    if (!poffset && offset) poffset = STRDUP(offset);
    if (!poffset) poffset = STRDUP("0");
    sign = GetSign(poffset);

    if (pvar) {
      int in_bracket = 0;
      char *p = pvar;
      char *newpvar = STRDUP(pvar);
      char *newp = newpvar;
      while (*p) { 
	if (!in_bracket && islower(*p)) *newp++ = toupper(*p);
	else if (IS_VARCHAR(p)) *newp++ = *p;
	else if (*p == '(') in_bracket++;
	else if (*p == ')') in_bracket--;
	p++;
      }
      *newp = '\0';
      FREE(pvar);
      pvar = STRDUP(newpvar);
    }
    else {
      pvar = STRDUP("no_var");
    }
    if (pmember) {
      char *p = pmember;
      while (*p) { if (islower(*p)) *p = toupper(*p); p++; }
    }
    else {
      pmember = STRDUP("all");
    }
    {
      char *zz = ZZtop(poffset, sign);
      slen += STRLEN(ptable) + STRLEN(zz) + STRLEN(pvar) + STRLEN(pmember) + STRLEN(suffix);
      ALLOC(s,slen);
      snprintf(s,slen,"%s%s_%s_%s%s%s",
	       ptable,zz,
	       pvar,pmember,
	       suffix ? "_" : "",
	       suffix ? suffix : "");
      FREE(zz);
    }

    FREE(pvar);
    FREE(pmember);
    FREE(ptable);
    FREE(poffset);
  }
  return s;
}


PRIVATE ODB_Stack *stack = NULL;

PRIVATE int stack_debug = 0;
PRIVATE int stack_first_time = 1;

PRIVATE void check_stack_debug()
{
  if (stack_first_time) {
    char *env = getenv("ODB_STACK_DEBUG");
    if (env) stack_debug = atoi(env);
    stack_first_time = 0;
  }
}

PRIVATE void stack_error(const char *errmsg)
{
  fprintf(stderr,"***Stack handling error: %s\n",errmsg);
  fprintf(stderr,"\tFor more info: Please re-run the compilation with -v option and ODB_STACK_DEBUG=1\n");
  raise(SIGABRT);
  exit(1);
}

#define POPSTACK(x,f) \
ODB_Stack *this = NULL; \
if (stack) { \
  x = stack->u.x; \
  f = stack->flag; \
  this = stack; \
  stack = stack->prev; \
} \
FREE(this);

PUBLIC char *
ODB_popstr()
{
  if (stack_first_time) check_stack_debug();
  {
    char *s = NULL;
    unsigned int flag = 0;
    const unsigned int flag_expected = 0x1;
    POPSTACK(s,flag);
    if (stack_debug) {
      fprintf(stderr,
	      "<<< ODB_popstr() = %p ('%s') : flag=0x%x, flag_expected=0x%x\n",
	      s?s:NULL,s?s:"(nil)",flag,flag_expected);
    }
    if (flag != flag_expected) stack_error("ODB_popstr(): flag != flag_expected");
    return s;
  }
}

PUBLIC int
ODB_popi()
{
  if (stack_first_time) check_stack_debug();
  {
    int i = 0;
    unsigned int flag = 0;
    const unsigned int flag_expected = 0x2;
    POPSTACK(i,flag);
    if (stack_debug) {
      fprintf(stderr,"<<< ODB_popi() = %d : flag=0x%x, flag_expected=0x%x\n",i,flag,flag_expected);
    }
    if (flag != flag_expected) stack_error("ODB_popi(): flag != flag_expected");
    return i;
  }
}

PUBLIC ODB_Tree *
ODB_popexpr()
{
  if (stack_first_time) check_stack_debug();
  {
    ODB_Tree *expr = NULL;
    unsigned int flag = 0;
    const unsigned int flag_expected = 0x4;
    POPSTACK(expr,flag);
    if (stack_debug) {
      fprintf(stderr,
	      "<<< ODB_popexpr() = %p : flag=0x%x, flag_expected=0x%x\n",
	      expr?expr:NULL,flag,flag_expected);
    }
    if (flag != flag_expected) stack_error("ODB_popexpr(): flag != flag_expected");
    return expr;
  }
}

PUBLIC FILE *
ODB_popFILE()
{
  if (stack_first_time) check_stack_debug();
  {
    FILE *fp = NULL;
    unsigned int flag = 0;
    const unsigned int flag_expected = 0x8;
    POPSTACK(fp,flag);
    if (stack_debug) {
      fprintf(stderr,
	      "<<< ODB_popFILE() = %p : flag=0x%x, flag_expected=0x%x\n",
	      fp?fp:NULL,flag,flag_expected);
    }
    if (flag != flag_expected) stack_error("ODB_popFILE(): flag != flag_expected");
    return fp;
  }
}

PUBLIC ODB_Symbol *
ODB_popSYMBOL()
{
  if (stack_first_time) check_stack_debug();
  {
    ODB_Symbol *psym = NULL;
    unsigned int flag = 0;
    const unsigned int flag_expected = 0x10;
    POPSTACK(psym,flag);
    if (stack_debug) {
      fprintf(stderr,
	      "<<< ODB_popSYMBOL() = %p : flag=0x%x, flag_expected=0x%x\n",
	      psym?psym:NULL,flag,flag_expected);
    }
    if (flag != flag_expected) stack_error("ODB_popSYMBOL(): flag != flag_expected");
    return psym;
  }
}

PUBLIC ODB_SelectExpr *
ODB_popSELECTEXPR()
{
  if (stack_first_time) check_stack_debug();
  {
    ODB_SelectExpr *sel = NULL;
    unsigned int flag = 0;
    const unsigned int flag_expected = 0x20;
    POPSTACK(sel,flag);
    if (stack_debug) {
      fprintf(stderr,
	      "<<< ODB_popSELECTEXPR() = %p : flag=0x%x, flag_expected=0x%x\n",
	      sel?sel:NULL,flag,flag_expected);
    }
    if (flag != flag_expected) stack_error("ODB_popSELECTEXPR(): flag != flag_expected");
    return sel;
  }
}

#define PUSHSTACK(x,f) \
{ \
  ODB_Stack *prev = NULL; \
  ODB_Stack *this = stack; \
  if (this) { \
    prev = this; \
    ALLOC(this->next,1); \
    this = this->next; \
  } \
  else { \
    ALLOC(this,1); \
  } \
  this->u.x = x; \
  this->flag = f; \
  this->prev = prev; \
  this->next = NULL; \
  stack = this; \
}

PUBLIC void 
ODB_pushstr(char *s)
{
  const unsigned int flag = 0x1;
  if (stack_first_time) check_stack_debug();
  if (stack_debug) {
    fprintf(stderr,">>> ODB_pushstr(%p) '%s'\n",s?s:NULL,s?s:"(nil)");
  }
  PUSHSTACK(s,flag);
}

PUBLIC void 
ODB_pushi(int i)
{
  const unsigned int flag = 0x2;
  if (stack_first_time) check_stack_debug();
  if (stack_debug) {
    fprintf(stderr,">>> ODB_pushi(%d)\n",i);
  }
  PUSHSTACK(i,flag);
}

PUBLIC void 
ODB_pushexpr(ODB_Tree *expr)
{
  const unsigned int flag = 0x4;
  if (stack_first_time) check_stack_debug();
  if (stack_debug) {
    fprintf(stderr,">>> ODB_pushexpr(%p)\n",expr?expr:NULL);
  }
  PUSHSTACK(expr,flag);
}

PUBLIC void 
ODB_pushFILE(FILE *fp)
{
  const unsigned int flag = 0x8;
  if (stack_first_time) check_stack_debug();
  if (stack_debug) {
    fprintf(stderr,">>> ODB_pushFILE(%p)\n",fp?fp:NULL);
  }
  PUSHSTACK(fp,flag);
}

PUBLIC void 
ODB_pushSYMBOL(ODB_Symbol *psym)
{
  const unsigned int flag = 0x10;
  if (stack_first_time) check_stack_debug();
  if (stack_debug) {
    fprintf(stderr,">>> ODB_pushSYMBOL(%p)\n",psym?psym:NULL);
  }
  PUSHSTACK(psym,flag);
}

PUBLIC void 
ODB_pushSELECTEXPR(ODB_SelectExpr *sel)
{
  const unsigned int flag = 0x20;
  if (stack_first_time) check_stack_debug();
  if (stack_debug) {
    fprintf(stderr,">>> ODB_pushSELECTEXPR(%p)\n",sel?sel:NULL);
  }
  PUSHSTACK(sel,flag);
}

#define VERBOSE_REORDER_PRINT() \
	char *s = pview->from[j]->table->name; \
	int flag = pview->from_attr[j]; \
	int tblno = pview->from[j]->tableno; \
	int rank = pview->from[j]->rank; \
	int i, nlink = pview->from[j]->nlink; \
	fprintf(stderr,"[%d] = %s [flag=0x%x:tbl#%d:rank=%d] : # of links = %d ",j,s,flag,tblno,rank,nlink); \
	for (i=0; i<nlink; i++) { \
	  char *linktablename = pview->from[j]->link[i]->table->name; /* vow !! */ \
	  int itstblno = pview->from[j]->link[i]->tableno; \
	  int itsrank = pview->from[j]->link[i]->rank; \
	  fprintf(stderr,"%s%s<tbl#%d:rank=%d>",(i>0) ? "," : "\n(",linktablename,itstblno,itsrank); \
	}  \
	fprintf(stderr,"%s\n",(nlink > 0) ? ")" :"")


PUBLIC int
ODB_reorder_tables(ODB_View *pview)
{
  int nfrom = pview ? pview->nfrom : 0;
  if (pview && nfrom >= 1) {
    int j;
    const char *vname = pview->view->name;

    if (verbose) {
      fprintf(stderr,
	      "ODB_reorder_tables(): Reordering FROM-tables in view '%s'\n",vname);
    }

    if (verbose) {
      fprintf(stderr,"ODB_reorder_tables(): ** Old sequence -->\n");
      for (j=0; j<nfrom; j++) {
	VERBOSE_REORDER_PRINT();
      }
    } /* if (verbose) */

    if (nfrom > 1) {
      int *newflag = NULL;
      ODB_Table **newfrom = NULL;
      Cmpfromtable_t *cmp = NULL;

      ALLOC(cmp, nfrom);

      for (j=0; j<nfrom; j++) {
	char *s = pview->from[j]->table->name;
	ODB_Table *ptable = ODB_lookup_table(s, NULL);
	if (verbose) {
	  fprintf(stderr,"ODB_reorder_tables(): [%d] table=%s : wt=%.6f\n",j,s,ptable->wt);
	}
	cmp[j].wt = ptable->wt;
	cmp[j].rank = j;
	cmp[j].flag = pview->from_attr[j];
      }

      qsort(cmp, nfrom, sizeof(*cmp), 
	    (int (*)(const  void *, const void *))cmpfromtable);

      ALLOC(newflag, nfrom);
      ALLOC(newfrom, nfrom);
      for (j=0; j<nfrom; j++) {
	newflag[j] = cmp[j].flag;
	newfrom[j] = pview->from[cmp[j].rank];
      }

      FREE(pview->from_attr);
      pview->from_attr = newflag;
      FREE(pview->from);
      pview->from = newfrom;

      FREE(cmp);
    } /* if (nfrom > 1) */

    if (verbose) {
      fprintf(stderr,"ODB_reorder_tables(): ** New sequence -->\n");
      for (j=0; j<nfrom; j++) {
	VERBOSE_REORDER_PRINT();
      }
      for (j=0; j<nfrom; j++) {
	char *s = pview->from[j]->table->name;
	ODB_Table *ptable = ODB_lookup_table(s, NULL);
	fprintf(stderr,"ODB_reorder_tables()> [%d] table=%s : wt=%.6f\n",j,s,ptable->wt);
      }
    } /* if (verbose) */

  } /* if (pview && nfrom >= 1) */
  return nfrom;
}


PRIVATE int
Unroll_Links(ODB_Table **from, int nfrom, Boolean touched[])
{
  int j, rc = 0;
  for (j=0; j<nfrom; j++) {
    ODB_Table *ptable = from[j];
    int tblno = ptable->tableno;
    if (!touched[tblno]) { /* Avoids infinite loop when recursive links between tables */
      touched[tblno] = 1;
      rc++;
      rc += Unroll_Links(ptable->link, ptable->nlink, touched);
    }
  }
  return rc;
}

PUBLIC int
ODB_insert_tables(ODB_View *pview)
{
  int nfrom = pview ? pview->nfrom : 0;
  if (pview && nfrom > 1) {
    int saved_nfrom = nfrom;
    int n_touched, tblno, j;
    ODB_Table **newfrom = NULL; /* new from-list */
    int newlen = 0;
    int lenstr = ODB_ntables;
    int more, total_more = 0;
    ODB_Table **all_tables = NULL; /* All tables; original order */
    int *picked = NULL; /* 0 = if not picked, 1 = if already picked, 2 = if inserted */
    Boolean *touched = NULL; /* True if table in unrolled "hierarchy-path" */
    ODB_Table *ptable = NULL; 

    CALLOC(all_tables, ODB_ntables);
    j = 0;
    for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
      if (verbose) {
	tblno = ptable->tableno;
	fprintf(stderr,">>all_tables[%d] = '%s' : tableno = %d\n", 
		j, ptable->table->name, tblno);
      }
      lenstr += STRLEN(ptable->table->name);
      all_tables[j++] = ptable;
    }

    CALLOC(newfrom, ODB_ntables);
    CALLOC(picked, ODB_ntables);
    for (j=0; j<nfrom; j++) {
      ptable = pview->from[j];
      tblno = ptable->tableno;
      picked[tblno] = 1;
      newfrom[j] = ptable;
    }
    newlen = nfrom;
    
    CALLOC(touched, ODB_ntables);
    n_touched = Unroll_Links(pview->from, nfrom, touched); /* Could this be an infinite loop ? */
    if (verbose) {
      fprintf(stderr,"n_touched = %d\n", n_touched);
      for (j=0; j<ODB_ntables; j++) {
	if (touched[j]) {
	  ptable = all_tables[j];
	  fprintf(stderr,"[%d] : '%s'\n", j, ptable->table->name);
	}
      }
    }

    do {
      more = 0;
      for (j=0; j<nfrom; j++) {
	int i;
        ptable = newfrom[j];
	for (i=0; i<ODB_ntables; i++) {
	  if (!picked[i] && touched[i] && ptable->linkslavemask[i]) {
	    /* insert this table */
	    picked[i] = 2;
	    ODB_pushi(i);
	    more++;
	    total_more++;
	  }
	}
      }
      if (more > 0) {
	for (j=more-1; j>=0; j--) {
	  int i = ODB_popi();
	  newfrom[newlen++] = all_tables[i];
	}
	nfrom = newlen;
      }
    } while (more > 0);

    if (verbose) {
      fprintf(stderr,"--> New picked-list\n");
      for (j=0; j<ODB_ntables; j++) {
	if (picked[j]) {
	  ptable = all_tables[j];
	  fprintf(stderr,"###[%d] : '%s' : picked=%d\n", j, ptable->table->name,picked[j]);
	}
      }
    }

    FREE(all_tables);
    FREE(touched);
    FREE(picked);

    FREE(pview->from);
    pview->from = newfrom;
    pview->nfrom = nfrom;

    if (total_more > 0) { /* Update from_attr -flags */
      int *newflag = NULL;
      ALLOC(newflag, nfrom);
      for (j=0; j<saved_nfrom; j++) {
	newflag[j] = pview->from_attr[j];
      }
      for (j=saved_nfrom; j<nfrom; j++) {
	newflag[j] = ODB_FROM_ATTR_INSERT;
      }
      FREE(pview->from_attr);
      pview->from_attr = newflag;
    }

    if (verbose && total_more > 0) { /* Report the change */
      char *oldfromlist = NULL;
      char *newfromlist = NULL;
      ALLOC(oldfromlist, lenstr); *oldfromlist = '\0';
      ALLOC(newfromlist, lenstr); *newfromlist = '\0';
      more = 0;
      for (j=0; j<saved_nfrom; j++) {
	ptable = pview->from[j];
	if (j > 0) strcat(oldfromlist,",");
	strcat(oldfromlist,ptable->table->name);
      }
      more = 0;
      for (j=0; j<nfrom; j++) {
	ptable = pview->from[j];
	if (more++ > 0) strcat(newfromlist,",");
	strcat(newfromlist,ptable->table->name);
      }
      SETMSG2("Potentially missing tables inserted to FROM-statement\n"
	      "  before '%s' and\n  after '%s'\n",
	      oldfromlist, newfromlist);
      PRTMSG(msg);
      FREE(oldfromlist);
      FREE(newfromlist);
    } /* if (verbose && total_more > 0) */
  } /* if (pview && nfrom > 1) */
  return nfrom;
}


PUBLIC int
ODB_pick_tables(int nfrom)
{
  int flag = ODB_FROM_ATTR_AUTO;
  int count = 0;
  no_from_stmt = 0;
  if (nfrom <= 0) {
    ODB_Table *ptable = ODB_start_table();
    for ( ; ptable != NULL; ptable = ptable->next) {
      ODB_pushstr(ptable->table->name);
      ODB_pushi(flag);
      count++;
    }
    if (nfrom == -2) no_from_stmt = 1; /* FROM-less SELECT */
  }
  else {
    int j;
    char **fromlist = NULL;
    ALLOC(fromlist, nfrom);
    /* Empty stack first */
    for (j=nfrom-1; j>=0; j--) {
      char *s = ODB_popstr(); /* a character string from the stack */
      fromlist[j] = s;
    }
    /* Redo the stack + add the flag */
    flag = ODB_FROM_ATTR_USER;
    for (j=0; j<nfrom; j++) {
      char *s = fromlist[j];
      ODB_pushstr(s);
      ODB_pushi(flag);
      count++;
    }
    FREE(fromlist);
  }
  return count;
}

PUBLIC int
ODB_pick_symbols(ODB_Table **from, const int *from_attr, int nfrom)
{
  int count = 0;

  if (from && nfrom > 0) {
    int i;
    for (i=0; i<nfrom; i++) {
      if ((from_attr[i] & ODB_FROM_ATTR_INSERT) == ODB_FROM_ATTR_INSERT) {
	/* Skip this table if table was auto-inserted */
	continue;
      }
      else {
	ODB_Table *ptable = from[i];
	char *tname = ptable->table->name;
	int tname_len = strlen(tname);
	int j, nsym = ptable->nsym;
	
	for (j=0; j<nsym; j++) {
	  ODB_Symbol *psym = ptable->sym[j];
	  if (psym) {
	    char *symname = psym->name;
	    char *s;
	    int len = strlen(symname) + tname_len + 2;
	    ALLOC(s, len);
	    snprintf(s, len, "%s@%s", symname, tname);
	    ODB_pushstr(s);
	    count++;
	  }
	} /* for (j=0; j<nsym; j++) */
      }
    } /* for (i=0; i<nfrom; i++) */
  }

  return count;
}


PRIVATE Boolean
check_string2(Boolean is_error, char **tag, const ODB_Tree *p1, const ODB_Tree *p2)
{
  /* Check that if a string expression is present, then
     the counterpart argument (or expr) must be either
     another string expression or an ODB_NAME with a correct
     datatype ("string") */

  Boolean on_error = 0;
  char *name = NULL;

  if (p1) {
    Boolean is_string = (p1->what == ODB_STRING ||
			 p1->what == ODB_WC_STRING);

    if (p2) {
      is_string |= (p2->what == ODB_STRING ||
		    p2->what == ODB_WC_STRING);

      if (is_string) {
	Boolean in_business = 
	  (
	   ((p1->what == ODB_STRING || p1->what == ODB_WC_STRING)
	    && p2->what == ODB_WHERE_SYMBOL) ||
	   ((p2->what == ODB_STRING || p2->what == ODB_WC_STRING)
	    && p1->what == ODB_WHERE_SYMBOL));

	if (in_business) {
	  char *swhere     = (p1->what == ODB_WHERE_SYMBOL) ? p1->argv[0] : p2->argv[0];
	  char *ptype = NULL;
	  char *pvar = NULL;
	  char *ptable = NULL;
	  int isym;
	  ODB_Symbol *psym = (p1->what == ODB_STRING || p1->what == ODB_WC_STRING) 
	    ? p1->argv[0] : p2->argv[0];

	  (void) ODB_split(swhere, NULL, &pvar, NULL, NULL, NULL);
	  isym = atoi(pvar); /* Order no. of a view symbol */
	  
	  (void) ODB_split(tag[isym], &ptype, &pvar, NULL, &ptable, NULL);

	  if (!strequ(ptype,STRING)) {
	    SETMSG4("Can't compare non-string symbol '%s' (type='%s', table='%s') against string symbol '%s'",
		    pvar, ptype, ptable, psym->name);
	    YYerror(msg);
	  }

	  FREE(ptype);
	  FREE(pvar);
	  FREE(ptable);
	}

      } /* if (is_string) */
    }
  }
    
  if (on_error && is_error) {
    SETMSG1("Cannot compare a non-string variable '%s' against a string expression",
	    name);
    YYerror(msg);
  }

  return on_error;
}


PUBLIC int
ODB_trace_symbols(ODB_Tree *pnode, ODB_Tracesym *t, Boolean is_selectexpr)
{
  int rc = 0;

  if (pnode) {
    int what = pnode->what;

    switch (what) {
    case ODB_WHERE_SYMBOL:
      if (t) {
	if (t->flag == 0) {
	  t->next++;
	  rc = 1; /* Already resolved, but must be accounted for */
	}
	else if (t->flag == 2) {
	  t->maxfrom = MAX(t->maxfrom, pnode->level);
	}
      }
      break;
      
    case ODB_NAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *name = psym->name;

	if (t) {
	  if (t->flag == 0) {
	    int j = t->next++;
	    int ifrom = t->table_index[j];
	    char *tag = t->tag[j];
	    char *s = t->where[j];
	    int len = strlen(s) + 50;
	    char *p;
	    
	    t->maxfrom = MAX(t->maxfrom, ifrom);
	    
	    ALLOC(p, len);
	    snprintf(p, len, "double:%d.%d@%s", j, ifrom, s);
	    
	    {
	      int numargs = 3;
	      void **tmp;
	      ALLOC(tmp, numargs);
	      tmp[0] = p;
	      tmp[1] = psym;
	      tmp[2] = tag;
	      pnode->argc = numargs;
	      FREE(pnode->argv);
	      pnode->argv = tmp;
	    }

	    pnode->level = ifrom;
	    pnode->what = ODB_WHERE_SYMBOL;
	  }
	  else if (t->flag == 2) {
	    t->maxfrom = MAX(t->maxfrom, pnode->level);
	  }
	}
	else if (!t) {
	  ODB_pushstr(name);
	}
      }
      rc = 1;
      break;

    case ODB_HASHNAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *name = psym->name;

	if (t) {
	  if (t->flag == 0) {
	    int j = t->next++;
	    int ifrom = t->table_index[j];
	    /* fprintf(stderr,"TRACE_SYMBOLS: HASHNAME='%s' --> ifrom=%d\n",name,ifrom); */
	    t->maxfrom = MAX(t->maxfrom, ifrom);
	    pnode->level = ifrom;
	  }
	  else if (t->flag == 2) {
	    t->maxfrom = MAX(t->maxfrom, pnode->level);
	  }
	}
	else if (!t) {
	  ODB_pushstr(name);
	}
      }
      rc = 1;
      break;

    case ODB_BSNUM:
      rc = 0;
      break;

    case ODB_USDNAME:
      if (is_selectexpr && !t) {
	ODB_Symbol *psym = pnode->argv[0];
	char *name = psym->name;
	ODB_pushstr(name);
	rc = 1;
      }
      else {
	rc = 0;
      }
      break;

    case ODB_COND:
      {
	int j, numargs = pnode->argc;
	for (j=0; j<numargs; j++) 
	  rc += ODB_trace_symbols(pnode->argv[j], t, is_selectexpr);
      }
      break;

    case ODB_FUNC:
    case ODB_FUNCAGGR:
      {
	int j, numargs = pnode->argc - 1;

	if (t) {
	  ODB_Symbol *psym = pnode->argv[0];
	  char *name = psym->name;
	  if (t->flag == 0) {
	    t->has_maxcount |= strequ(name,"maxcount");
	    t->has_Unique |= strequ(name,"Unique");
	    t->has_thin |= strequ(name,"thin");
	  }
	  else if (t->flag == 2) {
	    t->has_maxcount |= strequ(name,"maxcount");
	    t->has_Unique |= strequ(name,"Unique");
	    t->maxfrom = MAX(t->maxfrom, pnode->level);
	  }
	}

	for (j=1; j<=numargs; j++) 
	  rc += ODB_trace_symbols(pnode->argv[j], t, is_selectexpr);
      }
      break;

    case ODB_FILE:
      {
	int numargs = pnode->argc - 1;

	if (t) {
	  if (t->flag == 0) {
	    /* t->has_infile = 1; */
	  }
	  else if (t->flag == 2) {
	    /* t->has_infile = 1; */
	    t->maxfrom = MAX(t->maxfrom, pnode->level);
	  }
	}

	rc += ODB_trace_symbols(pnode->argv[2], t, is_selectexpr);
      }
      break;

    case ODB_NE:
    case ODB_EQ:
    case ODB_GT:
    case ODB_GE:
    case ODB_LE:
    case ODB_LT:
    case ODB_AND:
    case ODB_OR:
    case ODB_ADD:
    case ODB_SUB:
    case ODB_STAR:
    case ODB_DIV:
      rc += ODB_trace_symbols(pnode->argv[0], t, is_selectexpr);
      rc += ODB_trace_symbols(pnode->argv[1], t, is_selectexpr);
      break;

    case ODB_UNARY_PLUS:
    case ODB_UNARY_MINUS:
    case ODB_NOT:
      rc += ODB_trace_symbols(pnode->argv[0], t, is_selectexpr);
      break;

    case ODB_MATCH:
      {
	int j, nummatch = pnode->argc; /* Ought to be 1 */
	for (j=0; j<nummatch; j++) {
	  ODB_Match_t *match = pnode->argv[j];
	  rc += ODB_trace_symbols(match->expr, t, is_selectexpr);
	}
      }
      break;
  
    default:
      rc = 0;
      break;
    } /* switch (what) */
  }

  return rc;
}


PRIVATE char *Voffset(const char *poffset)
{
  char *s = NULL;
  if (poffset && !strequ(poffset,"0")) {
    s = STRDUP(poffset);
    if (*s == '_') *s = '-';
  }
  else
    s = STRDUP("0");
  return s;
}


#define PROCESS(SYMBOL, symbol, nsymbol, ifrom, in_flag, is_where, is_selectexpr, is_select) \
for (j=0; j<nsymbol; j++) { \
  Boolean is_select_formula = (is_select && v->is_formula && v->is_formula[j]) ? 1 : 0; \
  if (!is_select_formula) { \
   ODB_Symbol *psym = v->symbol[j]; \
   const char *s = psym->name; \
   Boolean found = 0; \
   char *pvar = NULL; \
   char *pmember = NULL; \
   char *ptable = NULL; \
   char *poffset = NULL; \
   if (ODB_split(s, NULL, &pvar, &pmember, &ptable, &poffset) == 0) { \
    int index = -1; \
    found = ODB_in_table(ODB_NAME, pvar, t, &index); \
    if (found && ptable) found = strequ(tname, ptable); \
    if (found && pmember && index >= 0) { \
      ODB_Type *type = t->type[index]; \
      Boolean member_present = ODB_in_type(pmember, type, NULL); \
      if (!member_present) { \
	char *tyname = type->type->name; \
	SETMSG3("Member '%s' is not present in type '%s' (variable '%s')\n", \
		pmember, tyname, pvar); \
	PRTMSG(msg); \
	SETMSG2("  Error occurred while processing table '%s' in view '%s'\n", \
		tname, vname); \
	PRTMSG(msg); \
	error_count++; \
      } \
    } /* if (found && pmember) */ \
    if (found) { \
      ODB_Type *type = (index >= 0) ? t->type[index] : NULL; \
      v->table_index[k] = ifrom; \
      v->tag[k] = join(0, pvar, pmember, t, type, is_where, is_selectexpr, psym, poffset); \
      v->call_arg[k] = join(3, pvar, pmember, t, type, is_where, is_selectexpr, psym, poffset); \
      v->def_put[k] = join(-1, pvar, pmember, t, type, is_where, is_selectexpr, psym, poffset); \
      v->alias_put[k] = join(-2, pvar, pmember, t, type, is_where, is_selectexpr, psym, poffset); \
      v->def_get[k] = join(1, pvar, pmember, t, type, is_where, is_selectexpr, psym, poffset); \
      v->alias_get[k] = join(2, pvar, pmember, t, type, is_where, is_selectexpr, psym, poffset); \
      v->poslen[k] = join(4, pvar, pmember, t, type, is_where, is_selectexpr, psym, poffset); \
      v->offset[k] = Voffset(poffset); \
    } \
   } \
   else { \
    SETMSG3("Invalid %s-symbol '%s' encountered in view '%s'\n", \
	    SYMBOL, s, vname);  \
    PRTMSG(msg);  \
    error_count++; \
   } \
   if (found) { refsym[k]++; reftab[ifrom] = 1; } \
   FREE(pvar); \
   FREE(pmember); \
   FREE(ptable); \
   FREE(poffset); \
  } \
  in_flag[k++] = 1; \
} /* for (j=0; j<nsymbol; j++) */


PRIVATE Boolean
check_string(Boolean is_error, int what, const ODB_Tree *p1, const ODB_Tree *p2)
{
  /* A string can only be part of the ODB_EQ or ODB_NE or ODB_LIKE or ODB_NOTLIKE
     expressions, and can only appear in either LHS or RHS of the full expression.
     Furthermore if ODB_EQ/ODB_NE expr, then the other side of the
     expr must be an ODB_NAME */

  Boolean on_error = 0;

  if (p1) {
    Boolean is_string = (p1->what == ODB_STRING || p1->what == ODB_WC_STRING);

    if (p2) {
      is_string |= (p2->what == ODB_STRING || p2->what == ODB_WC_STRING);

      if (is_string) {
	on_error = ((what != ODB_EQ) && (what != ODB_NE) && 
		    (what != ODB_LIKE) && (what != ODB_NOTLIKE));
	if (!on_error) {
	  on_error = !(
		       ((p1->what == ODB_STRING || p1->what == ODB_WC_STRING)
			&& 
			(p2->what == ODB_STRING || p2->what == ODB_NAME))
		       ||
		       ((p2->what == ODB_STRING || p2->what == ODB_WC_STRING)
			 && 
			(p1->what == ODB_STRING || p1->what == ODB_NAME)));
	}
      } /* if (is_string) */
    }
    else {
      /* No second parameter ==> a unary operation or so */
      on_error = is_string; /* No unary operations available for ODB_STRING */
    }
  }
    
  if (on_error && is_error) {
    SETMSG0("Invalid presence of a string expression");
    YYerror(msg);
  }

  return on_error;
}


PRIVATE int
remap_expr_symbols(ODB_Tree *pnode, int nsym, ODB_Symbol **psym, 
		   char **tag, ODB_Symbol **psym_updated)
{
  int rc = 0;
  static int stklen = 0;
  Boolean Master = 0;

  if (tag && psym && nsym > 0) {
    int j;
    for (j=nsym-1; j>=0; j--) {
      if (tag[j] && psym[j]) {
	char *atag = tag[j];
	char *name = psym[j]->name;
	if (IS_HASH(name)) continue;
	if (IS_DOLLAR(name)) continue;
	/* fprintf(stderr,"remap_expr_symbols: pushing symbol[%d] = '%s' to stack\n",j,psym[j]->name); */
	ODB_pushSYMBOL(psym[j]);
	/* fprintf(stderr,"remap_expr_symbols: pushing tag[%d] = '%s' to stack\n",j,atag); */
	ODB_pushstr(atag);
	ODB_pushi(j);
	stklen++;
      }
    }
    Master = 1;
  }

  if (pnode && stklen > 0) {
    int what = pnode->what;
    
    switch (what) {
      
    case ODB_EQ:
    case ODB_NE:
    case ODB_GT:
    case ODB_GE:
    case ODB_LE:
    case ODB_LT:
    case ODB_AND:
    case ODB_OR:
    case ODB_ADD:
    case ODB_SUB:
    case ODB_STAR:
    case ODB_DIV:
      rc += remap_expr_symbols(pnode->argv[0], 0, NULL, NULL, psym_updated);
      rc += remap_expr_symbols(pnode->argv[1], 0, NULL, NULL, psym_updated);
      break;
      
    case ODB_UNARY_PLUS:
    case ODB_UNARY_MINUS:
    case ODB_NOT:
      rc += remap_expr_symbols(pnode->argv[0], 0, NULL, NULL, psym_updated);
      break;
      
    case ODB_NAME:
      { /* Note the reverse order of these 'pops' */
	int j = ODB_popi();
	char *atag = ODB_popstr();
	ODB_Symbol *psym = ODB_popSYMBOL();
	ODB_Symbol *newsym = NULL;
	char *pcolon = strchr(atag,':');
	if (pcolon) {
	  pcolon++;
	  if (!strequ(psym->name,pcolon)) {
	    newsym = ODB_new_symbol(ODB_NAME, pcolon);
	    if (psym_updated) psym_updated[j] = newsym;
	  }
	}
	pnode->argv[0] = newsym ? newsym : psym; /* Changed the symbol pointer !! */
	rc++;
	stklen--;
      }
      break;

    case ODB_COND:
      {
	int j, numargs = pnode->argc;
	for (j=0; j<numargs; j++) {
	  rc += remap_expr_symbols(pnode->argv[j], 0, NULL, NULL, psym_updated);
	}
      }
      break;
      
    case ODB_FUNC:
    case ODB_FUNCAGGR:
      {
	int j, numargs = pnode->argc - 1;
	if (numargs > 0) {
	  for (j=1; j<=numargs; j++) {
	    rc += remap_expr_symbols(pnode->argv[j], 0, NULL, NULL, psym_updated);
	  }
	}
      }
      break;

    case ODB_NUMBER:
      /* Do nothing ? */
      break;
      
    default:
      fprintf(stderr,
	      "***Warning in remap_expr_symbols() :"
	      "what = %d alias '%s' : Should be here ?\n", what, ODB_keymap(what));
      break;
    } /* switch (what) */
  } /* if (pnode && stklen > 0) */

  if (Master && stklen != 0) {
    SETMSG1("A serious programming error in remap_expr_symbols(): stklen != 0 (stklen=%d)",stklen);
    YYerror(msg);
  }

  return rc;
}

PUBLIC int
ODB_setup_selectexpr(ODB_View *pview, int k, int j)
{
  int rc = 0;
  if (pview && pview->sel && k >= 0 && j >= 0) {
    int nselect_all = pview->nselect_all;
    int nwhere = pview->nwhere;
    int norderby = pview->norderby;
    int i = nselect_all+nwhere+norderby + k;
    int n = remap_expr_symbols(pview->sel[j]->expr, 
			       pview->sel[j]->nsym, pview->sel[j]->sym, &pview->tag[i],
			       pview->sel[j]->sym);
    rc = pview->sel[j]->nsym;
  }
  return rc;
}

PUBLIC uint
ODB_which_aggr(const ODB_Tree *pnode, int *ncols_aux)
{
  int Ncols_aux = 0;
  uint aggr_flag = ODB_AGGR_NONE;
  if (pnode && pnode->what == ODB_FUNCAGGR) {
    const ODB_Symbol *psym = pnode->argv[0];
    const char *name = psym->name;
    if (strequ(name,"min")) aggr_flag = ODB_AGGR_MIN;
    else if (strequ(name,"max")) aggr_flag = ODB_AGGR_MAX;
    else if (strequ(name,"density")) aggr_flag = ODB_AGGR_DENSITY;
    else if (strequ(name,"sum")) aggr_flag = ODB_AGGR_SUM;
    else if (strequ(name,"sum_distinct")) aggr_flag = ODB_AGGR_SUM_DISTINCT;
    else if (strequ(name,"avg")) aggr_flag = ODB_AGGR_AVG;
    else if (strequ(name,"avg_distinct")) aggr_flag = ODB_AGGR_AVG_DISTINCT;
    else if (strequ(name,"median")) aggr_flag = ODB_AGGR_MEDIAN;
    else if (strequ(name,"median_distinct")) aggr_flag = ODB_AGGR_MEDIAN_DISTINCT;
    else if (strequ(name,"stdev")) aggr_flag = ODB_AGGR_STDEV;
    else if (strequ(name,"stdev_distinct")) aggr_flag = ODB_AGGR_STDEV_DISTINCT;
    else if (strequ(name,"var")) aggr_flag = ODB_AGGR_VAR;
    else if (strequ(name,"var_distinct")) aggr_flag = ODB_AGGR_VAR_DISTINCT;
    else if (strequ(name,"rms")) aggr_flag = ODB_AGGR_RMS;
    else if (strequ(name,"rms_distinct")) aggr_flag = ODB_AGGR_RMS_DISTINCT;
    else if (strequ(name,"count")) aggr_flag = ODB_AGGR_COUNT;
    else if (strequ(name,"count_distinct")) aggr_flag = ODB_AGGR_COUNT_DISTINCT;
    else if (strequ(name,"bcount")) aggr_flag = ODB_AGGR_BCOUNT;
    else if (strequ(name,"bcount_distinct")) aggr_flag = ODB_AGGR_BCOUNT_DISTINCT;
    else if (strequ(name,"dotp")) aggr_flag = ODB_AGGR_DOTP;
    else if (strequ(name,"dotp_distinct")) aggr_flag = ODB_AGGR_DOTP_DISTINCT;
    else if (strequ(name,"norm")) aggr_flag = ODB_AGGR_NORM;
    else if (strequ(name,"norm_distinct")) aggr_flag = ODB_AGGR_NORM_DISTINCT;
    else if (strequ(name,"covar")) { aggr_flag = ODB_AGGR_COVAR; Ncols_aux = 1; }
    else if (strequ(name,"corr")) { aggr_flag = ODB_AGGR_CORR; Ncols_aux = 1; }
    else if (strequ(name,"linregr_a")) { aggr_flag = ODB_AGGR_LINREGR_A; Ncols_aux = 1; }
    else if (strequ(name,"linregr_b")) { aggr_flag = ODB_AGGR_LINREGR_B; Ncols_aux = 1; }
    else if (strequ(name,"minloc")) { aggr_flag = ODB_AGGR_MINLOC; Ncols_aux = 1; }
    else if (strequ(name,"maxloc")) { aggr_flag = ODB_AGGR_MAXLOC; Ncols_aux = 1; }
  }
  if (ncols_aux) *ncols_aux = Ncols_aux;
  return aggr_flag;
}

PUBLIC int
ODB_RemoveDuplicateTables(ODB_View *v)
{
  int new_nfrom = v ? v->nfrom : 0;
  if (new_nfrom > 0) {
    ODB_Table **from = NULL;
    int *from_attr = NULL;
    int i, nfrom = new_nfrom;
    int *refcount = NULL;
    CALLOC(refcount, ODB_ntables);
    for (i=0; i<nfrom; i++) {
      const char *tblname = v->from[i]->table->name;
      ODB_Table *t = ODB_lookup_table(tblname, NULL);
      int num = t->tableno;
      if (num >= 0 && num < ODB_ntables) refcount[num]++;
    }
    CALLOC(from, nfrom);
    CALLOC(from_attr, nfrom);
    new_nfrom = 0;
    for (i=0; i<nfrom; i++) {
      const char *tblname = v->from[i]->table->name;
      ODB_Table *t = ODB_lookup_table(tblname, NULL);
      int num = t->tableno;
      if (num >= 0 && num < ODB_ntables && refcount[num] >= 1) {
	int flag = v->from_attr[i];
	from[new_nfrom] = v->from[i];
	from_attr[new_nfrom] = v->from_attr[i];
	new_nfrom++;
	refcount[num] = 0;
      }
    }
    FREE(refcount);
    v->from = from;
    v->nfrom = new_nfrom;
  }
  return new_nfrom;
}


PUBLIC ODB_Tree *
ODB_oper(int what, void *p1, void *p2, void *p3, void *p4,
	 const char *filename, int lineno)
{		
  ODB_Tree *pnode = new_node(what);

  if (debug) {
    fprintf(stderr,
	    "%p = ODB_oper(what=%d i.e. %s [called from %s:%d], p1=%p, p2=%p, p3=%p, p4=%p)\n",
	    pnode, what, ODB_keymap(what), filename, lineno, p1, p2, p3, p4);
  }

  switch (what) {

  case ODB_MATCH:
    {
      ODB_Match_t *match = p1;
      int nummatch = 1;
      pnode->argc = nummatch;
      ALLOC(pnode->argv, nummatch);
      pnode->argv[0] = match;
    }
    break;

  case ODB_NUMBER:
    pnode->dval = dval_assign(p1);  /* "p1" used to be a ptr to yylval.dval */
    break;

  case ODB_ARRNAME:
    {
      int j, nsym = ival_assign(p1);  /* "p1" is the number of symbols */
      what = pnode->what = ODB_NAME; /* Renamed */
      pnode->argc = nsym;
      ALLOC(pnode->argv,nsym);
      /* fprintf(stderr,"ODB_ARRNAME: argc = %d\n",nsym); */
      for (j=nsym-1; j>=0; j--) {
	char *s = ODB_popstr();
	ODB_Symbol *psym = ODB_new_symbol(what, s);
	pnode->argv[j] = psym;
	/* fprintf(stderr,"ODB_ARRNAME: argv[%d] -> '%s'\n",j,s); */
      }
    }
    break;

  case ODB_USDNAME:
    {
      char *pname = p1;
      ODB_Symbol *psym = ODB_lookup(ODB_USDNAME,pname,NULL);
      if (!psym) {
	SETMSG1("Attempt to use undefined SET-variable '%s'",pname);
	YYerror(msg);
      }
    }

    /* no break; fall through */
  case ODB_HASHNAME:
  case ODB_NAME:
  case ODB_BSNUM:
  case ODB_STRING:
  case ODB_WC_STRING:
    {
      ODB_Symbol *psym = ODB_new_symbol(what, p1);
      pnode->argc = 1;
      ALLOC(pnode->argv,1);
      pnode->argv[0] = psym;
    }
    break;

  case ODB_COND:
    /* x ? y : z */
    {
      int numargs = 3;
      pnode->argc = numargs;
      ALLOC(pnode->argv,numargs);
      pnode->argv[0] = p1;
      pnode->argv[1] = p2;
      pnode->argv[2] = p3;
    }
    break;

  case ODB_IN:
    /* 
       target IN ( a, b, c, ... ) 

         is converted to 

       ((target == a) || (target == b) || (target == c) || ...)
     */
    {
      ODB_Tree *target = p1;
      int j, numargs = p2 ? ival_assign(p2) : 0;
      ODB_Tree **args = NULL;
      CALLOC(args, numargs);
      for (j=numargs-1; j>=0; j--) {
	ODB_Tree *expr = ODB_popexpr();
	args[j] = ODBOPER2(ODB_EQ,target,expr);
      }
      if (numargs == 1) {
	pnode = args[0]; /* i.e. just simply "target == a" */
      }
      else { /* numargs > 1 */
	ODB_Tree *chain = NULL;
	pnode->argc = 2;
	ALLOC(pnode->argv,2);
	pnode->argv[0] = args[0];  /* left expr : "target == a" */
	chain = args[1];
	for (j=2; j<numargs; j++) {
	  chain = ODBOPER2(ODB_OR,chain,args[j]);
	}
	pnode->argv[1] = chain;  /* right expr : "((target == b) || (target == c) || ...)" */
	pnode->what = ODB_OR;    /* we end up with : left || right */
      }
      FREE(args);
    }
    break;

  case ODB_NOTIN:
    /* 
       target NOT IN ( a, b, c, ... ) 

         is converted to 

       ((target != a) && (target != b) && (target != c) && ...)
     */
    {
      ODB_Tree *target = p1;
      int j, numargs = p2 ? ival_assign(p2) : 0;
      ODB_Tree **args = NULL;
      CALLOC(args, numargs);
      for (j=numargs-1; j>=0; j--) {
	ODB_Tree *expr = ODB_popexpr();
	args[j] = ODBOPER2(ODB_NE,target,expr);
      }
      if (numargs == 1) {
	pnode = args[0]; /* i.e. just simply "target != a" */
      }
      else { /* numargs > 1 */
	ODB_Tree *chain = NULL;
	pnode->argc = 2;
	ALLOC(pnode->argv,2);
	pnode->argv[0] = args[0];  /* left expr : "target != a" */
	chain = args[1];
	for (j=2; j<numargs; j++) {
	  chain = ODBOPER2(ODB_AND,chain,args[j]);
	}
	pnode->argv[1] = chain;  /* right expr : "((target != b) && (target != c) && ...)" */
	pnode->what = ODB_AND;   /* we end up with : left && right */
      }
      FREE(args);
    }
    break;

  case ODB_QUERY:
    {
      int j, numargs = p4 ? ival_assign(p4) : 0;
      char *fname = (numargs > 0) ? "SubQuery" : "RunOnceQuery";
      char *poolmask = p1 ? p1 : "-1";
      ODB_Tree *poolmask_expr = ODBOPER1(ODB_STRING, poolmask);
      char *db = p2 ? p2 : "";
      ODB_Tree *db_expr = ODBOPER1(ODB_STRING, db);
      char *query = p3;
      ODB_Tree *query_expr = ODBOPER1(ODB_STRING, query);
      double make_sort_unique = 1;
      ODB_Tree *make_sort_unique_expr =  ODBOPER1(ODB_NUMBER, &make_sort_unique);
      ODB_Tree **tmpargv = NULL;
      if (numargs > 0) {
	ALLOC(tmpargv, numargs);
	for (j=numargs-1; j>=0; j--) {
	  tmpargv[j] = ODB_popexpr();
	}
      }
      ODB_pushexpr(poolmask_expr);
      ODB_pushexpr(db_expr);
      ODB_pushexpr(query_expr);
      ODB_pushexpr(make_sort_unique_expr);
      if (numargs > 0) {
	for (j=0; j<numargs; j++) ODB_pushexpr(tmpargv[j]);
	/* FREE(tmpargv); */
      }
      if (debug) {
	fprintf(stderr,"what=ODB_QUERY (%d) near end ; fname='%s' :\n",what,fname);
	fprintf(stderr,"p1 = %p : poolmask = %s\n",p1,poolmask);
	fprintf(stderr,"p2 = %p : db = '%s'\n",p2,db);
	fprintf(stderr,"p3 = %p : query = '%s'\n",p3,query?query:NIL);
	fprintf(stderr,"make_sort_unique = %d\n",(int)make_sort_unique);
	fprintf(stderr,"p4 = %p : numargs = %d (numargs+4 = %d)\n",p4,numargs,numargs+4);
      }
      numargs += 4;
      pnode = ODBOPER2(ODB_FUNC, fname, &numargs); /* changed pnode !!!! */
    }
    break;

  case ODB_STRFUNC1:
    {
      /* 
	 lat or latitude("helsinki");
	 lon or longitude("stockholm");
	 alt or altitude("paris");
      */
      char *fname = p1;
      char *place = p2;
    back_here:
      if (strequ(fname,"latitude")   || strequ(fname,"lat")  ||
	  strequ(fname,"longitude")  || strequ(fname,"lon")  ||
	  strequ(fname,"altitude")   || strequ(fname,"alt")  || 
	  strequ(fname,"population") || strequ(fname,"popul") || strequ(fname,"pop")) {
	int nargs = 1;
	ODB_Tree *arg1 = ODBOPER1(ODB_STRING,place);
	char f[4];
	strncpy(f,fname,3); /* Retain just the first 3 letterz */
	f[3] = '\0';
	ODB_pushexpr(arg1);
	pnode = ODBOPER2(ODB_FUNC, f, &nargs); /* changed pnode !!!! */
      }
      else if (strequ(fname,"elevation")  || strequ(fname,"elev")) {
	fname = "alt";
	goto back_here;
      }
      else {
	SETMSG2("Unrecognized string function %s(\"%s\")",fname,place);
	YYerror(msg);
      }
    }
    break;

  case ODB_INSIDE:
  case ODB_INSIDE_POLYGON:
  case ODB_NEAR:
    {
      /* 
	 For example:
	   inside("finland",lat,lon) [returns 1 if inside, 0 otherwise]
	 or
	   polygon("/polygon/filename", lat, lon) [returns 1 if inside polygon, 0 otherwise]
	 or
	   near("helsinki",lat,lon) [returns distance in meters]

	 You can also leave out the (lat,lon); 
	 they will be assumed "lat@hdr" and "lon@hdr"

	 These functions also don't need degrees(lat),degrees(lon)
	 as it is done internally (note also that noawadays degrees() := lldegrees())
      */
      ODB_Tree *region = p1; /* code for the region or place name */
      ODB_Tree *lat = p2;
      ODB_Tree *lon = p3;
      char *fname = (what == ODB_INSIDE) ? "Inside" : 
	((what == ODB_INSIDE_POLYGON) ? "InPolygon" : "Near");
      int nargs = 3;
      ODB_pushexpr(region);
      ODB_pushexpr(lat);
      ODB_pushexpr(lon);
      pnode = ODBOPER2(ODB_FUNC, fname, &nargs); /* changed pnode !!!! */
    }
    break;

  case ODB_STRFUNC:
    {
      /* lhs OPER "str", where OPER ODB_{EQ,LIKE} */
      /* This becomes a vararg function --> {strequal|wildcard}(nargs,str,nargs-1 ARRNAMEs) */
      ODB_Tree *lhs = p1; /* code for ODB_ARRNAME (renamed to ODB_NAME) */
      int nsym = lhs->argc;
      ODB_Tree *str = p2; /* code for ODB_STRING or ODB_WC_STRING */
      int oper = ival_assign(p3);
      char *fname = (oper == ODB_EQ) ? "StrEqual" : "WildCard";
      int j, nargs = 1 + nsym;
      ODB_pushexpr(str);
      for (j=0; j<nsym; j++) {
	ODB_Symbol *psym = lhs->argv[j];
	ODB_Tree *expr = ODBOPER1(ODB_NAME, psym->name);
	ODB_pushexpr(expr);
      }
      pnode = ODBOPER2(ODB_FUNC, fname, &nargs); /* changed pnode !!!! */
    }
    break;

  case ODB_FUNC:
    {
      char *fname = p1;
      Boolean free_fname = 0;
      int j, numargs = p2 ? ival_assign(p2) : 0;
      Boolean has_distinct = p3 ? ival_assign(p3) : 0;
      Boolean distinct_allowed = 0;
      ODB_Match_t *match = (p4 && strequ(fname, "InQuery")) ? p4 : NULL;
      int nummatch = 0;
      int joffset = 0;
      Boolean is_thin = 0;
      Boolean is_varargs = 0;
      Boolean is_offset = 0;
      int key = 0;
      int keylen = 0;
      const int onearg = 1;
      const funcs_t *f = NULL;

      /* check first if this is LINKOFFSET or LINKLEN -case 
	 and numargs == 1 and the "onlys" arg-expression is ODB_NAME : if true then
	 LINKOFFSET(xxx[@table]) will be changed to xxx.offset[@table]
	 LINKLEN(xxx[@table]) will be changed to xxx.len[@table]
	 the [@table] is optional and could be either blank or @table
      */

      if (numargs == 1 && (strequ(fname,"linklen") || strequ(fname,"linkoffset"))) {
	Boolean linklen = strequ(fname,"linklen") ? 1 : 0;
	ODB_Tree *expr = ODB_popexpr();
	if (expr && expr->what == ODB_NAME && expr->argv) { /* still cool !! */
	  ODB_Symbol *psym = expr->argv[0];
	  char *s = psym->name;
	  char *pvar = NULL;
	  char *pmember = NULL;
	  char *ptable = NULL;
	  (void) ODB_split(s, NULL, &pvar, &pmember, &ptable, NULL);
	  if (pvar && !pmember) {
	    char *snew;
	    int slen = strlen(pvar) + STRLEN(ptable) + 20;
	    ALLOC(snew, slen);
	    snprintf(snew,slen,"%s.%s%s%s",
		     pvar,
		     linklen ? "len" : "offset",
		     ptable ? "@" : "",
		     ptable ? ptable : "");
	    FREE(pvar);
	    FREE(pmember);
	    FREE(ptable);
	    pnode = ODBOPER1(ODB_NAME, snew);
	    break; /* changed pnode !!!! */
	  }
	  FREE(pvar);
	  FREE(pmember);
	  FREE(ptable);
	}
	/* No success --> push the last expression back to stack */
	ODB_pushexpr(expr);
      }

      /* check if count(expression), in which case turn it to bcount-function
	 (boolean count), which is similar to Fortran90's count(logical_expression) function */

      if (numargs == 1 && strequ(fname,"count")) {
	ODB_Tree *expr = ODB_popexpr();
	if (!ODB_IsSimpleExpr(expr,0)) fname = "bcount"; /* now a boolean counter */
	ODB_pushexpr(expr); /* restore expression stack */
      }

      if (strequ(fname,"atan") && numargs == 2) {
	/* Means atan2(arg1,arg2) */
	fname = "atan2";
      }
      else if (strequ(fname,"degrees") || strequ(fname,"rad2deg")) {
	fname = "degrees";
      }
      else if (strequ(fname,"radians") || strequ(fname,"deg2rad")) {
	fname = "radians";
      }
      else if (strequ(fname,"lldegrees") || strequ(fname,"ll_degrees") ||
	       strequ(fname,"lldeg")) {
	fname = "lldegrees";
      }
      else if (strequ(fname,"latdegrees") || strequ(fname,"latrad2deg") ||
	       strequ(fname,"lat_degrees") || strequ(fname,"lat_rad2deg")) {
	fname = "lldegrees";
      }
      else if (strequ(fname,"londegrees") || strequ(fname,"lonrad2deg") ||
	       strequ(fname,"lon_degrees") || strequ(fname,"lon_rad2deg")) {
	fname = "lldegrees";
      }
      else if (strequ(fname,"llradians") || strequ(fname,"ll_radians") ||
	       strequ(fname,"llrad")) {
	fname = "llradians";
      }
      else if (strequ(fname,"latradians") || strequ(fname,"latdeg2rad") ||
	       strequ(fname,"lat_radians") || strequ(fname,"lat_deg2rad")) {
	fname = "llradians";
      }
      else if (strequ(fname,"lonradians") || strequ(fname,"londeg2rad") ||
	       strequ(fname,"lon_radians") || strequ(fname,"lon_deg2rad")) {
	fname = "llradians";
      }
      else if (strequ(fname,"invector")) {
	fname = "in_vector";
      }
      else if (strequ(fname,"minval")) {
	fname = "min";
      }
      else if (strequ(fname,"maxval")) {
	fname = "max";
      }
      else if (strequ(fname,"cov") || strequ(fname,"covariance") ||
	       strequ(fname,"covarians")) {
	fname = "covar";
      }
      else if (strequ(fname,"correlation") || strequ(fname,"correl")) {
	fname = "corr";
      }
      else if (strequ(fname,"lina") || strequ(fname,"linregra") || 
	       strequ(fname,"linrega") || strequ(fname,"linreg_a")) {
	fname = "linregr_a";
      }
      else if (strequ(fname,"linb") || strequ(fname,"linregrb") || 
	       strequ(fname,"linregb") || strequ(fname,"linreg_b")) {
	fname = "linregr_b";
      }
      else if (strequ(fname,"std") || strequ(fname,"stddev")) {
	fname = "stdev";
      }
      else if (strequ(fname,"variance") || strequ(fname,"varians")) {
	fname = "var";
      }
      else if (strequ(fname,"average") || strequ(fname,"mean")) {
	fname = "avg";
      }
      else if (strequ(fname,"rand")) {
	fname = "random";
      }
      else if (strequ(fname,"datenow")) {
	fname = "date_now";
      }
      else if (strequ(fname,"timenow")) {
	fname = "time_now";
      }
      else if (strequ(fname, "thinn") ||
	       strequ(fname, "thinned") || strequ(fname, "thinning")) {
	fname = "thin";
      }
      else if (strequ(fname, "celsius")) {
	fname = "k2c";
      }
      else if (strequ(fname, "fahrenheit")) {
	fname = "k2f";
      }
      else if (strequ(fname, "dotproduct") || 
	       strequ(fname, "dot_product") ||
	       strequ(fname, "dot_prod")) {
	fname = "dotp";
      }
      else if (strequ(fname, "timestamp")) {
	fname = "tstamp";
      }
      else if (strequ(fname, "time_angle")) {
	fname = "ta";
      }
      else if (strequ(fname, "hour_angle")) {
	fname = "ha";
      }
      else if (strequ(fname, "daynum")) {
	fname = "datenum";
      }
      else if (strequ(fname, "solar_declination") || strequ(fname, "solar_declination_angle")) {
	fname = "sda";
      }
      else if (strequ(fname, "solar_elevation") || strequ(fname, "solar_elevation_angle") ||
	       strequ(fname, "solar_altitude") || strequ(fname, "solar_altitude_angle")) {
	fname = "sela";
      }
      else if (strequ(fname, "solar_zenith") || strequ(fname, "solar_zenith_angle")) {
	fname = "sza";
      }
      else if (strequ(fname, "solar_azimuth") || strequ(fname, "solar_azimuth_angle")) {
	fname = "saza";
      }

      if (strequ(fname,"count")) {
	if (numargs == 0 && !has_distinct) { /* count(*) or just count() */
	  /* note: count(distinct *) not possible due to yacc.y grammar defs, however
	     count(distinct) is possible and thus must not be accepted */
	  double one = 1;
	  ODB_Tree *expr = ODBOPER1(ODB_NUMBER,&one);
	  ODB_pushexpr(expr);
	  numargs = 1;
	  /* Need to flag presence of count(*) to trigger failure in case of "No active tables" */
	  /* Happens with 'SELECT count(*) FROM hdr', which should have been either
	     'SELECT count(#hdr) FROM hdr' or 'SELECT count(lat) FROM hdr' */
	  has_count_star = 1; /* will be reset back to 0 (false) after ODBOPER(ODB_VIEW,...) in yacc.y */
	}
	if (numargs == 1) { 
	  pnode->what = ODB_FUNCAGGR;
	  distinct_allowed = has_distinct;
	  f = NULL;
	}
	else {
	  f = check_func(fname, numargs, NULL, NULL);
	}
      }
      else if (strequ(fname,"sum")   || strequ(fname,"avg") || strequ(fname,"median")  ||
	       strequ(fname,"stdev") || strequ(fname,"rms") || strequ(fname,"density") ||
	       strequ(fname,"var")   || strequ(fname,"bcount") ) {
	if (numargs == 1) { 
	  pnode->what = ODB_FUNCAGGR;
	  distinct_allowed = has_distinct;
	  f = NULL;
	}
	else {
	  f = check_func(fname, numargs, NULL, NULL);
	}
      }
      else if (strequ(fname,"dotp") || strequ(fname,"norm")) {
	if (numargs == 2) { 
	  pnode->what = ODB_FUNCAGGR;
	  distinct_allowed = has_distinct;
	  f = NULL;
	}
	else {
	  f = check_func(fname, numargs, NULL, NULL);
	}
      }
      else if (strequ(fname,"corr") || strequ(fname,"covar")  ||
	       strequ(fname,"linregr_a") || strequ(fname,"linregr_b") ||
	       strequ(fname,"minloc") || strequ(fname,"maxloc")) {
	if (numargs == 2) { 
	  pnode->what = ODB_FUNCAGGR;
	  f = NULL;
	}
	else {
	  f = check_func(fname, numargs, NULL, NULL);
	}
      }
      else if (strequ(fname,"min") || strequ(fname,"max")) {
	if (numargs == 1) { 
	  pnode->what = ODB_FUNCAGGR;
	  /* fname = strequ(fname,"min") ? "Min" : "Max"; */
	  f = NULL;
	}
	else {
	  /* Allocate more space for one hidden arg (which is # of actual args) */
	  f = check_func(fname, numargs, &onearg, NULL); /* Requires at least 1 argument */
	  is_varargs = 1;
	}
      }
      else if (strequ(fname, "InQuery")) {
	const int fivearg = 5;
	f = check_func(fname, numargs, &fivearg, NULL); /* Requires at least 5 arguments */
	if (match) nummatch = 1; /* Just for clarify; not used for anything here */
	is_varargs = 1;
      }
      else if (strequ(fname, "cksum")) {
	f = check_func(fname, numargs, NULL, NULL); /* Requires no args */
	is_varargs = 1;
      }
      else if (strequ(fname, "StrEqual") || strequ(fname, "WildCard")) {
	const int twoarg = 2;
	f = check_func(fname, numargs, &twoarg, NULL); /* Requires 2 or more args */
	is_varargs = 1;
      }
      else if (strequ(fname, "SubQuery") || strequ(fname, "RunOnceQuery")) {
	const int fourarg = 4;
	f = check_func(fname, numargs, &fourarg, NULL); /* Requires 4 or more args */
	is_varargs = 1;
      }
      else if (strequ(fname, "thin")) {
	/* Allocate more space for the two first,hidden, args {key,n} */
	/* The following separates multiple occurences of the same thin()-functions */
	static int thincnt = 0; 
	++thincnt;
	is_thin = 1;
	key = ODB_cksum32((const char *)&thincnt, sizeof(thincnt), key);
	keylen += sizeof(thincnt);
	f = check_func(fname, numargs, &onearg, NULL); /* Require at least 1 argument */
      }
      else if (strequ(fname, "offset")) {
	/* offset()-function : 2 args of which
	   the 1st one must be column@table, but
	   the 2nd one must be an evaluable (constant) expression
	*/
	is_offset = 1;
	has_OFFSET_func = 1;
	f = check_func(fname, numargs, NULL, NULL);
      }
      else {
	f = check_func(fname, numargs, NULL, NULL);
      }

      if (f) joffset = f->joffset;

      if (distinct_allowed) {
	int plen = strlen(fname) + strlen("_distinct") + 1;
	char *p;
	ALLOC(p,plen);
	snprintf(p,plen,"%s_distinct",fname);
	fname = p;
	free_fname = 1;
	f = check_func(fname, numargs, NULL, NULL);
      }

      pnode->joffset = joffset;
      pnode->argc = 1 + joffset + numargs;
      ALLOC(pnode->argv,pnode->argc);
      pnode->argv[0] = ODB_new_symbol(what, fname); /* function name */
      if (free_fname) FREE(fname);

      /* Flush the expression stack */

      srand(numargs);
      for (j=numargs; j>=1; j--) {
	ODB_Tree *addr = ODB_popexpr();
	pnode->argv[joffset+j] = addr;

	if (is_thin) {
	  if (addr && addr->argc > 0) {
	    double random_number = (double)rand()/(double)RAND_MAX;
	    double number = random_number;
	    switch (addr->what) {
	    case ODB_FUNC:
	      number = addr->argc;
	      key = ODB_cksum32((const char *)&number, sizeof(number), key);
	      keylen += sizeof(number);
	      /* fall through */
	    case ODB_BSNUM:
	    case ODB_NAME:
	    case ODB_USDNAME:
	    case ODB_HASHNAME:
	    case ODB_STRING:
	    case ODB_WC_STRING:
	      {
		const ODB_Symbol *psym = addr->argv[0];
		const char *s = psym ? psym->name : NULL;
		int slen = STRLEN(s);
		if (slen > 0) {
		  key = ODB_cksum32(s, slen, key);
		  keylen += slen;
		}
		else { /* A fallback */
		  number = random_number;
		  key = ODB_cksum32((const char *)&number, sizeof(number), key);
		  keylen += sizeof(number);
		}
	      }
	      break;
	    case ODB_NUMBER:
	      number = addr->dval;
	      /* fall through */
	    default:
	      key = ODB_cksum32((const char *)&number, sizeof(number), key);
	      keylen += sizeof(number);
	      break;
	    } /* switch (addr->what) */
	  }
	}
      } /* for (j=numargs; j>=1; j--) */

      if (is_thin) { /* thinning function */
	double d1, d2;
	d1 = numargs + 1; /* The number of actual args/expressions after this arg "n" */
	pnode->argv[1] = ODBOPER1(ODB_NUMBER,&d1);
	key = ODB_pp_cksum32(keylen, key);
	d2 = key;
	pnode->argv[2] = ODBOPER1(ODB_NUMBER,&d2);
      }
      else if (is_offset) { /* offset(expr_1, expr_2) -function */
	int error_count = 0;
	ODB_Tree *expr_1 = pnode->argv[1];
	ODB_Tree *expr_2 = pnode->argv[2];
	double offset = 0;
	{
	  /* The 1st arg must be ODB_NAME */
	  if (expr_1->what != ODB_NAME) {
            error_count++;
	    SETMSG1("The 1st argument in offset()-function must be a column name (ODB_NAME). Was %s",
		    ODB_keymap(expr_1->what));
	    YYwarn(error_count, msg); /* No abort yet */
	  }
	}
        {
          /* 2nd arg must be a constant expression (i.e. evaluable) */
          int irc = ODB_evaluate(expr_2,&offset);
          if (!irc) {
            error_count++;
            SETMSG1("The 2nd argument in offset()-function must be a constant expression. Was %s",
                    ODB_keymap(expr_2->what));
            YYwarn(error_count, msg); /* No abort yet */
          }
	  if (error_count == 0) {
	    /* Replace the result of evaluation with a constant number */
	    expr_2->dval = offset;
	    expr_2->what = ODB_NUMBER;
	  }
        }
        if (error_count > 0) {
          SETMSG0("Invalid argument(s) in offset()-function");
          YYerror(msg);
        }
	/* Finally : Turn this pnode into a ODB_NAME, with offset appended
	   into the name itself with OFFSET_CHAR as a separator */
	{
	  int ioffset = (offset >= -INT_MAX && offset <= INT_MAX) ? (int)offset : 0;
	  ODB_Symbol *psym = expr_1->argv[0];
	  char *name = psym->name;
	  char *newname = NULL;
	  if (ioffset != 0) {
	    int len = STRLEN(name) + 30;
	    ALLOC(newname,len);
	    snprintf(newname,len,"%s%s%s%d",
		     name,ODB_OFFSET_CHAR,
		     (ioffset > 0) ? "" : "_",
		     ABS(ioffset));
	  }
	  else {
	    newname = STRDUP(name);
	  }
	  pnode = ODBOPER1(ODB_NAME,newname);
	}
      }
      else if (is_varargs) { /* variable no. of args */
	double nargs = numargs;
	pnode->argv[1] = ODBOPER1(ODB_NUMBER,&nargs);
      }
    }
    break;

  case ODB_FILE:
    {
      ODB_Symbol *pfunc = ODB_new_symbol(what, p1); /* function name */
      int numargs = 2;

      (void) check_func(pfunc->name, numargs, NULL, NULL);

      pnode->argc = numargs + 1;
      ALLOC(pnode->argv,pnode->argc);
      pnode->argv[0] = pfunc;
      pnode->argv[1] = p2; /* filename */
      pnode->argv[2] = p3; /* expression */
    }
    break;

  case ODB_EQ:
  case ODB_ADD:
  case ODB_SUB:
  case ODB_STAR:
  case ODB_DIV:
  case ODB_GT:
  case ODB_GE:
  case ODB_LE:
  case ODB_LT:
  case ODB_NE:
  case ODB_AND:
  case ODB_OR:
    check_string(1,what,p1,p2);
    pnode->argc = 2;
    ALLOC(pnode->argv,2);
    pnode->argv[0] = p1;   /* left expr */
    pnode->argv[1] = p2;  /* right expr */
    break;

  case ODB_UNARY_PLUS: 
  case ODB_UNARY_MINUS: 
  case ODB_NOT: 
    check_string(1,what,p1,NULL);
    pnode->argc = 1;
    ALLOC(pnode->argv,1);
    pnode->argv[0] = p1;   /* unary expr */
    break;

  case ODB_GTGT:
  case ODB_GTGE:
  case ODB_GEGE:
  case ODB_GEGT:
  case ODB_LTLT:
  case ODB_LTLE:
  case ODB_LELE:
  case ODB_LELT:
    {
      int XX = what / ODB_SCALE;
      int YY = what % ODB_SCALE;
      pnode->argc = 2;
      ALLOC(pnode->argv,2);
      what = pnode->what = ODB_AND;   /* Switch 'what' to ODB_AND */
      pnode->argv[0] = ODBOPER2(XX, p1, p2);  /* left & middle expr */
      pnode->argv[1] = ODBOPER2(YY, p2, p3);  /* middle & right expr */
    }
    break;   

  case ODB_SET:
    {
      ODB_Symbol *psym = p1;
      ODB_Tree *expr = p2;
      Boolean is_string = check_string(0, what, expr, NULL);
      if (!is_string) {
	double dval = 0;
	int irc = ODB_evaluate(expr, &dval);
	
	if (irc) psym->dval = dval;
	
	pnode->argc = 1;
	ALLOC(pnode->argv,1);
	pnode->argv[0] = psym;
      }
      else {
	if (expr->what == ODB_STRING && expr->argc == 1) {
	  ODB_Symbol *psym1 = expr->argv[0];
	  double dval = s2d(psym1->name, expr->what);
	  char *str = d2s(dval);
	  ODB_Symbol *psym2 = ODB_new_symbol(expr->what, str);
	  pnode->argc = 1;
	  ALLOC(pnode->argv,1);
	  pnode->argv[0] = psym2;
	  FREE(str);
	}
	else {
	  SETMSG1("Invalid expression in SET-variable '%s' declaration",psym->name);
	  YYerror(msg);
	}
      }
    }
    break;

  case ODB_TYPE:
    pnode->argc = 1;
    ALLOC(pnode->argv,1);
    pnode->argv[0] = p1;
    
    if (p1) {
      ODB_Type *type = p1;
      char *tyname = type->type->name;
      int nsym = ival_assign(p2);
      int j, bitcount;
      
      /* Note overallocation for use by UNUSED if applicable */

      type->nsym = nsym;
      ALLOC(type->sym, nsym + 1);
      ALLOC(type->member, nsym + 1);
      
      for (j=nsym-1; j>=0; j--) {
	char *s;
	s = ODB_popstr(); /* member name from stack */
	type->member[j] = ODB_lookup(ODB_NAME, s, NULL);
	s = ODB_popstr(); /* type string from stack */
	type->sym[j] = ODB_lookup(ODB_TYPE, s, NULL);
      }

      type->no_members = (nsym == 1 && strequ(type->member[0]->name,EMPTY_MEMBER));

      if (type->no_members) {
	Boolean already_in_list;

	type->member[0] = ODB_lookup(ODB_TYPE, tyname, NULL);
	already_in_list = add_alias(type->sym[0]->name, type->member[0]->name);

	if (already_in_list) {
	  SETMSG2("Attempt to supersede already defined type '%s' with '%s'",
		  type->member[0]->name, type->sym[0]->name);
	  YYerror(msg);
	}
      }
      
      /* Check if solely a bit stream */

      ALLOC(type->pos, nsym + 1);
      ALLOC(type->len, nsym + 1);

      for (j=0; j<nsym; j++) {
	type->pos[j] = 0;
	type->len[j] = 0;
      }

      type->bitstream = (!type->no_members);
      bitcount = 0;

      if (type->bitstream) {
	for (j=0; j<nsym; j++) {
	  char *s = type->sym[j]->name;
	  
	  if (strnequ(s,"bit",3)) {
	    int nbits = atoi(s+3);
	    type->pos[j] = bitcount;
	    type->len[j] = nbits;
	    bitcount += nbits;
	  }
	  else {
	    type->bitstream = 0;
	    break;
	  }
	} /* for (j=0; j<nsym; j++) */
      }

      if (type->bitstream) {
	if (bitcount > MAXBITS) {
	  SETMSG2("Exceeded maximum no. of bits (%d) allowed per data struct in type '%s'",
		  MAXBITS, tyname);
	  YYerror(msg);
	}

#ifdef EXCLUDE_THIS
	if (bitcount < MAXBITS) {
	  /* Allocate an extra field called UNUSED */

	  int nbits = MAXBITS - bitcount;
	  /* char unused[] = UNUSED; */
	  char pbit[10];
	  snprintf(pbit,sizeof(pbit),"bit%d",nbits);

	  type->member[nsym] = ODB_new_symbol(ODB_NAME, UNUSED);
	  type->sym[nsym] = ODB_new_symbol(ODB_TYPE, pbit);
	  type->pos[nsym] = bitcount;
	  type->len[nsym] = nbits;

	  type->nsym = ++nsym;

	  bitcount += nbits; /* Now this should equal to MAXBITS */
	}
#endif

      } /* if (type->bitstream) */
    }
    break;

  case ODB_TABLE:
    pnode->argc = 1;
    ALLOC(pnode->argv,1);
    pnode->argv[0] = p1;
    break;

  case ODB_VIEW:
    pnode->argc = 1;
    ALLOC(pnode->argv,1);
    pnode->argv[0] = p1;
    
    if (p1) {
      ODB_View *v = p1;
      char *vname = v->view->name;
      int nselect = v->nselect;
      int nselect_all = v->nselect_all;
      int nfrom   = v->nfrom;
      int nwhere  = v->nwhere;
      int norderby = v->norderby;
      int nselsym = v->nselsym;
      int nsym = nselect_all + nwhere + norderby + nselsym;
      int *refsym = NULL;
      int i, j, k;
      int error_count = 0;
      int prtsyms = 0;
      Boolean redundant_tables = (v->create_index >= 0) ? 1 : 0;
      Boolean *in_select = NULL;
      Boolean *in_where = NULL;
      Boolean *in_orderby = NULL;
      Boolean *in_selsym = NULL;
      
      v->has_count_star = has_count_star;
      v->safeGuard = safeGuard;

      ODB_link_massage();

      nfrom = ODB_RemoveDuplicateTables(v);
      
      CALLOC(in_select, nsym);
      CALLOC(in_where, nsym);
      CALLOC(in_orderby, nsym);
      CALLOC(in_selsym, nsym);

      ALLOC(v->table_index, nsym);
      for (k=0; k<nsym; k++) v->table_index[k] = RMDI;

      CALLOC(v->tag, nsym);
      CALLOC(v->call_arg, nsym);
      CALLOC(v->def_get, nsym);
      CALLOC(v->alias_get, nsym);
      CALLOC(v->def_put, nsym);
      CALLOC(v->alias_put, nsym);
      CALLOC(v->poslen, nsym);
      CALLOC(v->offset, nsym);
      
      while (redundant_tables) {
	Boolean *reftab = NULL;
	ODB_Table **from = NULL;
	int *from_attr = NULL;
	int new_nfrom = 0;

	ALLOC(reftab, nfrom);
	ALLOC(from, nfrom);
	ALLOC(from_attr, nfrom);
	
	CALLOC(refsym, nsym);

	for (i=0; i<nfrom; i++) {
	  ODB_Table *t = v->from[i];
	  char *tname = t->table->name;
	  int nlink = t->nlink;
	  
	  /* We must accept traversal over all FROM-tables if count(*) was present ;
	     However, do NOT accept those tables which were auto-inserted by the program 
	     (see INSERT; or ODB_INSERT_TABLES) */

	  reftab[i] =
	    (((v->from_attr[i] & ODB_FROM_ATTR_INSERT) == ODB_FROM_ATTR_INSERT) || 
	     (!v->has_count_star)) ? 0 : 1;
	  
	  k = 0;
	  PROCESS("SELECT", select, nselect, i, in_select, 0, 0, 1);
	  
	  k = nselect_all;
	  PROCESS("WHERE", where, nwhere, i, in_where, 1, 0, 0);

	  k = nselect_all + nwhere;
	  PROCESS("ORDERBY", orderby, norderby, i, in_orderby, 0, 0, 0);

	  k = nselect_all + nwhere + norderby;
	  PROCESS("SELECTEXPR", selsym, nselsym, i, in_selsym, 0, 1, 0);

	  if (!reftab[i] && nlink > 0) {
	    /* Check if LINKs to any tables that follow are found */
	    int jj;
	    for (jj=0; jj<nlink && !reftab[i]; jj++) {
	      char *linkname = t->linkname[jj];
	      int ii;
	      for (ii=i+1; ii<nfrom && !reftab[i]; ii++) {
		ODB_Table *tii = v->from[ii];
		char *tiiname = tii->table->name;
		reftab[i] = strequ(tiiname, linkname);
	      } /* for (ii=i+1; ii<nfrom && !reftab[i]; ii++) */
	    } /* for (jj=0; jj<nlink && !reftab[i]; jj++) */
	  }
	} /* for (i=0; i<nfrom; i++) */

	redundant_tables = 0;
	new_nfrom = 0;

	for (i=0; i<nfrom; i++) {
	  int flag = v->from_attr[i];
	  if (reftab[i]) {
	    from[new_nfrom] = v->from[i];
	    from_attr[new_nfrom] = v->from_attr[i];
	    new_nfrom++;
	  } 
	  else {
	    ODB_Table *t = v->from[i];
	    char *tname = t->table->name;
	    if ((flag & ODB_FROM_ATTR_INSERT) != ODB_FROM_ATTR_INSERT) {
	      /* Report only those tables which compiler has *not* inserted itself */
	      if (!v->no_from_stmt) { /* Assume FROM-stmt was indeed present */
	        SETMSG3("Table '%s' is not referenced in view '%s' [0x%x]\n", tname, vname,flag);
	        PRTMSG(msg);
	      }
	    }
	    redundant_tables = 1;
	  }
	}

	if (nfrom == 0) {
	  SETMSG1("No active tables in view '%s'\n",vname);
	  PRTMSG(msg); /* Changed from error to warning */
	  v->nfrom = 0;
	  redundant_tables = 0;
	}
	else if (redundant_tables) {
	  for (i=0; i<new_nfrom; i++) {
	    v->from[i] = from[i];
	    v->from_attr[i] = from_attr[i];
	  }
	  nfrom = v->nfrom = new_nfrom;
	  FREE(refsym);
	}

	FREE(from);
	FREE(reftab);
      } /* while (redundant_tables) */
      
      for (k=0; k<nsym; k++) {
	int count = refsym[k];
	
	if (count != 1) {
	  char *w;
	  ODB_Symbol *psym = NULL;
	  char *name;
	  Boolean is_error = 0;
	  Boolean is_where = 0;
	  Boolean is_selsym = 0;
	  Boolean is_formula = 0;
	  
	  if (in_select[k]) {
	    w = "SELECT";
	    psym = v->select[k];
	    is_formula = (v->is_formula && v->is_formula[k]);
	  }
	  else if (in_where[k]) {
	    w = "WHERE";
	    psym = v->where[k-(nselect_all)];
	    is_where = 1;
	  }
	  else if (in_orderby[k]) {
	    w = "ORDERBY";
	    psym = v->orderby[k-(nselect_all+nwhere)];
	    is_formula = IS_FORMULA(psym->name);
	  }
	  else if (in_selsym[k]) {
	    w = "SELECTEXPR";
	    psym = v->selsym[k-(nselect_all+nwhere+norderby)];
	    is_selsym = 1;
	  }
	  else {
	    psym = NULL; /* Impossible ? */
	  }

	  name = psym ? psym->name : NULL;

	  if (!name) {
	    SETMSG2("No active table for empty %s-variable in view '%s'\n",
		    w, vname);
	    is_error = 1;
	  }
	  else if ((is_where || is_selsym)  && (IS_DOLLAR(name) || IS_HASH(name))) {
	    is_error = 0;
	  }
	  else if (count < 1 && !is_formula) {
	    SETMSG3("No active table for %s-variable '%s' in view '%s'\n",
		    w, name, vname);
	    is_error = 1;
	  }
	  else if (count > 1) {
	    SETMSG3("%s-variable '%s' specified more than once in view '%s'\n",
		    w, name, vname);
	    is_error = 1;
	  }

	  if (is_error) {
	    PRTMSG(msg);
	    error_count++;
	    ++prtsyms;
	  }
	}
      }
      
      if (error_count == 0) { 
	/* Re-assign SELECT/WHERE/ORDERBY-variables to the exact symbol entries */
	
	for (k=0; k<nsym; k++) {
	  ODB_Symbol *psym = NULL;
	  char *s;
	  char *pvar = NULL;
	  Boolean is_formula = 0;

	  if (in_select[k]) {
	    psym = v->select[k];
	    is_formula = (v->is_formula && v->is_formula[k]);
	  }
	  else if (in_where[k]) {
	    psym = v->where[k-(nselect_all)];
	  }
	  else if (in_orderby[k]) {
	    psym = v->orderby[k-(nselect_all+nwhere)];
	    is_formula = IS_FORMULA(psym->name);
	  }
	  else if (in_selsym[k]) {
	    psym = v->selsym[k-(nselect_all+nwhere+norderby)];
	  }

	  s = psym->name;

	  if (IS_HASH(s)) {
	    pvar = STRDUP(s);
	    psym = ODB_lookup(ODB_HASHNAME, pvar, NULL);
	  }
	  else if (is_formula) {
	    if (in_select[k]) {
	      psym = ODB_lookup(ODB_NAME, v->sel[k]->formula, NULL);
	    }
	  }
	  else {
	    (void) ODB_split(s, NULL, &pvar, NULL, NULL, NULL);
	    psym = ODB_lookup(ODB_NAME, pvar, NULL);
	  }

	  if (!psym) {
	    if (pvar) {
	      SETMSG3("Undefined symbol '%s' (now as '%s') in view '%s'\n",s,pvar,vname);
	    }
	    else {
	      SETMSG2("Undefined symbol '%s' in view '%s'\n",s,vname);
	    }
	    PRTMSG(msg);
	    error_count++;
	    ++prtsyms;
	  }

	  if (in_select[k]) {
	    v->select[k] = psym;
	  }
	  else if (in_where[k]) {
	    v->where[k-(nselect_all)] = psym;
	  }
	  else if (in_orderby[k]) {
	    v->orderby[k-(nselect_all+nwhere)] = psym;
	  }
	  else if (in_selsym[k]) {
	    v->selsym[k-(nselect_all+nwhere+norderby)] = psym;
	  }

	  FREE(pvar);
	} /* for (k=0; k<nsym; k++) */
	
      }
      
      FREE(refsym);

      
      if (error_count == 0) {
	/* Check for duplicate symbols in SELECT */

	Boolean dupl_exist = 1;
	int n = nselect_all;

	while (dupl_exist) {
	  ODB_Symbol **seltag;

	  ALLOC(seltag, n);

	  for (k=0; k<n; k++) {
	    ODB_Symbol *psym;
	    ALLOC(psym, 1);
	    if (!v->tag[k] && v->is_formula && v->is_formula[k]) {
	      int taglen = strlen("Formula:") + strlen(v->sel[k]->formula) + 1 + strlen("Formula") + 1;
	      ALLOC(v->tag[k], taglen);
	      snprintf(v->tag[k], taglen, "Formula:%s@Formula", 
		       /* In the next +1 since the first item is formula_char 
			  (see yacc.y; search for first_char) */
		       v->sel[k]->formula+1);
	    }
	    psym->name = v->tag[k];
	    /* fprintf(stderr,"k=%d :  v->tag[k] = '%s'\n",k, v->tag[k]); */
	    seltag[k] = psym;
	  }

	  dupl_exist = ODB_dupl_symbols(seltag, n, 0);

	  /*
	  if (dupl_exist) {
	    for (k=0; k<n; k++) {
	      fprintf(stderr,"k=%d: '%s' ('%s')\n",k,seltag[k]->name,v->select[k]->name);
	    }
	  }
	  */
	  
	  for (k=0; k<n; k++) {
	    FREE(seltag[k]);
	  }
	  FREE(seltag);

	  if (dupl_exist) {
	    SymbolParam_t *sp;

	    ALLOC(sp,1);
	    sp->nsym = n;
	    sp->sym = v->select;
	    sp->readonly = v->readonly;
	    sp->sel = v->sel;

	    SETMSG1("Duplicate SELECT-variables encountered in the view '%s' => removing\n", 
		    vname);
	    PRTMSG(msg);
	    
	    
	    /* error_count++; */
	    
	    n = ODB_remove_duplicates(sp,
				      v->table_index,
				      v->tag,
				      v->call_arg,
				      v->def_put,
				      v->alias_put,
				      v->def_get,
				      v->alias_get,
				      v->poslen,
				      v->offset);
	  }
	} /* while (dupl_exist) */

	v->nselect = n;
      }

      if (error_count == 0 && norderby > 0) {
	/* Check for duplicate symbols in ORDERBY */

	ODB_Symbol **seltag;
	Boolean dupl_exist;

	ALLOC(seltag, norderby);
	for (k=0; k<norderby; k++) {
	  int kk = k + (nselect_all + nwhere);
	  ODB_Symbol *psym = v->orderby[k];
	  if (!v->tag[kk] && IS_FORMULA(psym->name)) {
	    int taglen = strlen("Formula:") + strlen(psym->name) + 1 + strlen("Formula") + 1;
	    ALLOC(v->tag[kk], taglen);
	    snprintf(v->tag[kk], taglen, "Formula:%s@Formula", 
		     /* In the next +1 since the first item is formula_char 
			(see yacc.y; search for first_char) */
		     psym->name+1);
	  }
	  ALLOC(psym, 1);
	  psym->name = v->tag[kk];
	  seltag[k] = psym;
	}

	dupl_exist = ODB_dupl_symbols(seltag, norderby, 0);

	if (dupl_exist) {
	  for (k=0; k<norderby; k++) {
	    fprintf(stderr,"k=%d: '%s'\n",k,seltag[k]->name);
	  }

	  SETMSG1("Duplicate ORDERBY-variables encountered in the view '%s'\n", vname);
	  PRTMSG(msg);

	  error_count++;
	  ++prtsyms;
	}

	for (k=0; k<norderby; k++) {
	  FREE(seltag[k]);
	}
	FREE(seltag);
      }

      FREE(in_select);
      FREE(in_where);
      FREE(in_orderby);
      FREE(in_selsym);
      
      if (error_count > 0) {
	if (prtsyms) ODB_print_symbols(NULL);
	YYerror("Syntax error");
      }
    }
    break;

  default:
    SETMSG2("Operation '%s' (%d) not supported/implemented",ODB_keymap(what),what);
    YYerror(msg);
    break;
  }

  return pnode;
}


PUBLIC ODB_Symbol **
ODB_resolve_relations(const char *label,
		      int bailout_key,
		      SymbolParam_t *inout,
		      const char *vname,
		      ODB_Table **table, 
		      const int *from_attr,
		      int ntable,
		      ODB_Symbol **sym, 
		      int *sign,
		      int nsym,
		      Boolean allow_dollar,
		      Boolean allow_hash,
		      int *nsym_out,
		      int **sign_out)
{
  ODB_Symbol **newsym = NULL;
  Boolean has_symbol_param = (inout != NULL);
  Boolean may_have_select_expr = (has_symbol_param && inout->sel);
  int on_error = 0;
  Boolean has_sign = (sign != NULL && sign_out != NULL);
  int linkmode_1 = 1;
  int linkmode_2 = 2;
  int ReadOnly = 1;
  int *refcount = NULL;
  int *linkmode = NULL;
  int i,j;
  int more_than_one;
  int less_than_one;
  int zero = 0;
  int redo = 0;

#if 0
  fprintf(stderr,"ODB_resolve_relations('%s')\n",label);
#endif

  *nsym_out = 0;

  ALLOC(refcount, nsym);
  ALLOC(linkmode, nsym);

 re_do:
  redo = 0;

  for (j=0; j<nsym; j++) {
    char *s = sym[j]->name;
    char *pvar = NULL;
    char *pmember = NULL;
    char *ptable = NULL;
    char *poffset = NULL;
    char *xpvar = NULL;
    Boolean is_hash = IS_HASH(s);
    Boolean linkoffset = 0;
    Boolean linklen = 0;
    Boolean is_formula = 
      (IS_FORMULA(s) ||
       (may_have_select_expr && inout->sel[j] && inout->sel[j]->formula));

#if 0
    fprintf(stderr,"ODB_resolve_relations(A): name[%d]='%s' : is_formula = %d\n",
	    j,s,(int)is_formula);
#endif

    refcount[j] = 0;
    linkmode[j] = 0;

    if (is_formula) continue; /* skip formulas */

    if (!allow_hash && is_hash) {
      fprintf(stderr,
	      "Hash symbols ('%s') cannot be present in %s-clause; view '%s'\n",
	      s, label, vname);
      on_error++;
      continue;
    }

    if (allow_dollar && IS_DOLLAR(s)) continue;

    if (is_hash) {
      pvar = STRDUP(s);
      ptable = STRDUP(s+1);
    }
    else {
      (void) ODB_split(s, NULL, &pvar, &pmember, &ptable, &poffset);
    }

    if (pmember) {
      if (strequ(pmember,"off") || strequ(pmember,"offset")) {
	int len = strlen(pvar) + 20;
	ALLOC(xpvar, len);
	snprintf(xpvar,len,"LINKOFFSET(%s)",pvar);
	linkoffset = 1;
      }
      else if (strequ(pmember,"len") || strequ(pmember,"length")) {
	int len = strlen(pvar) + 20;
	ALLOC(xpvar, len);
	snprintf(xpvar,len,"LINKLEN(%s)",pvar);
	linklen = 1;
      }
    }

    for (i=0; i<ntable; i++) {
      extern char *one_tables;
      ODB_Table *t = table[i];
      char *tname = t->table->name;
      int index = -1;
      Boolean found = 0;
      Boolean retry = 0;

      /* Ignore searches from automatically inserted tables, since they
	 were not considered by the user in the first place */
      if ((from_attr[i] & ODB_FROM_ATTR_INSERT) == ODB_FROM_ATTR_INSERT) continue;

    retry_again:
      index = -1;
      found = ODB_in_table(ODB_NAME, pvar, t, &index);

      if (found && ptable) found = strequ(tname, ptable);

      if (!found) {
	/* No progress; Try with links if applicable ... */

	if (t->nlink > 0 && pmember && xpvar) {
	  found = ODB_in_table(ODB_NAME, xpvar, t, &index);

	  if (found && ptable) found = strequ(tname, ptable);
	  if (found) found = (t->linkmode[index] > 0);
	} /* if (t->nlink > 0 && ... */
      } /* if (!found) */

      if (!found && one_tables && t->any_sharedlinks && !retry) {
	/* Still no progress; Try sharedlinks if applicable ... */
	char *x = xpvar ? ODB_extract(xpvar, '(',')') : STRDUP(pvar);
	char *y = strchr(x,'.');
	retry = 1;
	if (!y) {
	  char *master = ODB_get_sharedlinkname(x, one_tables);
	  FREE(x);
	  if (master) {
	    char *new_s;
	    int len_new_s = 0;
	    int len_master = strlen(master);
	    FREE(pvar);
	    if (linkoffset || linklen) {
	      ALLOC(pvar,len_master+20);
	      sprintf(pvar,"%s.%s",master,linkoffset?"offset":"length");
	      FREE(xpvar);
	      ALLOC(xpvar, len_master + 20);
	      sprintf(xpvar,"%s(%s)",linkoffset?"LINKOFFSET":"LINKLEN",master);
	    }
	    else {
	      ALLOC(pvar,len_master+1);
	      sprintf(pvar,"%s",master);
	    }
	    len_new_s += strlen(pvar);
	    if (ptable)  len_new_s += strlen(ptable) + 1;
	    if (poffset) len_new_s += strlen(poffset) + 1;
	    ++len_new_s;
	    ALLOC(new_s, len_new_s);
	    if (poffset && !strequ(poffset,"0")) {
	      int sign = GetSign(poffset);
	      snprintf(new_s, len_new_s, "%s%s%s%s%s%s",
		       pvar, 
		       ptable  ? "@" : "", 
		       ptable  ? ptable  : "",
		       ODB_OFFSET_CHAR,
		       (sign < 0) ? "_" : "",
		       poffset+ABS(sign)
		       );
	    }
	    else {
	      snprintf(new_s, len_new_s, "%s%s%s",
		       pvar, 
		       ptable  ? "@" : "", 
		       ptable  ? ptable  : "");
	    }
	    sym[j] = ODB_new_symbol(ODB_NAME, new_s); /* change sym[j] !!!! */
	    FREE(new_s);
	    FREE(master);
	    goto retry_again;
	  }
	}
	FREE(x);
      }

      if (found) {
	refcount[j]++;
	linkmode[j] = (index >= 0) ? t->linkmode[index] : 0;
	/*
	{
	  char *typename = (index >= 0) ? t->type[index]->type->name : NIL;
	  int klinkmode = (index >= 0) ? t->linkmode[index] : -1;
	  fprintf(stderr,"resolve(j=%d,i=%d): refcount=%d, linkmode=%d",
		  j,i,refcount[j],linkmode[j]);
	  fprintf(stderr,
		  ", pvar='%s', xpvar='%s', pmember='%s', ptable='%s', index=%d, type='%s', linkmode=%d\n",
		  pvar, xpvar ? xpvar : NIL, pmember ? pmember : NIL,
		  ptable ? ptable : NIL, index, typename, klinkmode);
	}
	*/
      }
    } /* for (i=0; i<ntable; i++) */

    FREE(xpvar);
    FREE(pvar);
    FREE(pmember);
    FREE(ptable);
    FREE(poffset);
  } /* for (j=0; j<nsym; j++) */

  /* Check if any "refcount[]" greater than one.
     That means ambiguous tables found i.e.
     a symbol can be present in more than one table.
     This is an error. */

  more_than_one = 0;
  for (j=0; j<nsym; j++) {
    char *s = sym[j]->name;
    Boolean is_formula = 
      (IS_FORMULA(s) ||
       (may_have_select_expr && inout->sel[j] && inout->sel[j]->formula));
    if (is_formula) continue; /* skip formulas */
    more_than_one |= (refcount[j] > 1);
  }

  if (more_than_one) {
    on_error++;

    for (j=0; j<nsym; j++) {
      if (refcount[j] > 1) {
	char *s = sym[j]->name;
	fprintf(stderr,"Ambiguous %s-symbol '%s' encountered in view '%s'. ",
		label, s, vname);
	fprintf(stderr,"Related to more than one FROM-table.\n");
      }
    } /* for (j=0; j<nsym; j++) */
  }

  /* Check if any "refcount[]" equal to zero.
     That may be mean that one of LINKOFFSET or LINKLEN 
     or even both is present.
     Alternatively it means that a symbol is not present 
     in any given table, which is an error (except of course
     when a $-symbol is in concern in the WHERE-part) */

  less_than_one = 0;
  for (j=0; j<nsym; j++) {
    char *s = sym[j]->name;
    Boolean is_formula = 
      (IS_FORMULA(s) ||
       (may_have_select_expr && inout->sel[j] && inout->sel[j]->formula));
    if (is_formula) continue; /* skip formulas */
    if (allow_dollar && IS_DOLLAR(s)) continue; /* Disregard, if dollar */
    less_than_one |= (refcount[j] < 1);
  }

  /* Check if any links are present in the given tables */
      
  if (!on_error) {
    if (less_than_one) {
      int links_present = 0;

      for (i=0; i<ntable; i++) {
	ODB_Table *t = table[i];
	/* Ignore searches from automatically inserted tables, since they
	   were not considered by the user in the first place */
	if ((from_attr[i] & ODB_FROM_ATTR_INSERT) == ODB_FROM_ATTR_INSERT) continue;
	links_present |= (t->nlink > 0);
	if (links_present) break;
      }

      if (!links_present) {
	/* No reason to search for LINKLEN or so */
	int add = 0;
	/* Display message(s) */
	for (j=0; j<nsym; j++) {
	  char *s = sym[j]->name;
	  Boolean is_formula = 
	    (IS_FORMULA(s) ||
	     (may_have_select_expr && inout->sel[j] && inout->sel[j]->formula));
	  if (is_formula) continue; /* skip formulas */
	  if (allow_dollar && IS_DOLLAR(s)) continue; /* Disregard, if dollar */
	  if (refcount[j] < 1) {
	    int an_error = 0;
	    SETMSG3("Table reference count < 1 for column#%d in %s : '%s'",j+1,label,s);
	    if (bailout_level == 0 || 
		( (bailout_level & bailout_key) != bailout_key) ||
		STRLEN(bailout_string) == 0) {
	      an_error++;
	      add++;
	    }
	    else {
	      /* Make it a (simple) formula according to BAILOUT_STRING;
		 We support (a) A number, like "0" for zero
		            (b) A dollar variable, like "$mdi" */
	      ODB_Tree *expr = NULL;
	      s = bailout_string;
	      while (isspace(*s)) s++;
	      if (ODB_is_integer(s)) {
		int numba = atoi(s);
		expr = ODBOPER1(ODB_NUMBER,&numba);
	      }
	      else if (ODB_is_dollar(s)) {
		if (allow_dollar && bailout_key == ODB_BAILOUT_WHERE) {
		  ODB_Symbol *psym = ODB_new_symbol(ODB_USDNAME, s);
		  sym[j] = psym;
		  redo++;
		  continue;
		}
		else {
		  expr = ODBOPER1(ODB_USDNAME,s);
		}
	      }
	      if (expr) {
		char *first_char = STRDUP(FORMULA_CHAR); /* first_char free'd inside the dump_s */
		char *formula = dump_s(first_char,expr,ddl_piped ? 2 : 0,NULL);
		ODB_Symbol *psym = ODB_new_symbol(ODB_NAME, formula);
		sym[j] = psym;
		if (may_have_select_expr) {
		  int nsym;
		  char *f1 = dump_s(NULL,expr,3,NULL);
		  ODB_Symbol *nick = ODB_new_symbol(ODB_NICKNAME, f1);
		  inout->sel[j]->nicksym = nick;
		  inout->sel[j]->expr = expr;
		  inout->sel[j]->aggr_flag = ODB_AGGR_NONE;
		  inout->sel[j]->formula = formula;
		  inout->sel[j]->formula_out = NULL;
		  inout->sel[j]->ncols_aux = 0;
		  inout->sel[j]->nsym = nsym = ODB_trace_symbols(expr,NULL,1);
		  if (nsym > 0) {
		    ALLOC(inout->sel[j]->sym, nsym);
		    for (j=nsym-1; j>=0; j--) {
		      char *s = ODB_popstr();
		      /* The following symbol may still have "@"'s or "."'s [if ODB_NAME] */
		      int what = IS_HASH(s) ? ODB_HASHNAME : (IS_DOLLAR(s) ? ODB_USDNAME : ODB_NAME);
		      inout->sel[j]->sym[j] = ODB_new_symbol(what, s);
		    }
		  }
		  else { /* No symbols; just scalars (or functions) etc. */
		    inout->sel[j]->nsym = 0;
		    inout->sel[j]->sym = NULL;
		  }
		  if (inout->readonly) inout->readonly[j] = 1; /* Always unconditionally R/O */
		  redo++;
		}
	      }
	      else { /* An error after all ;-( */
		an_error++;
		add++;
	      }
	    }
	    YYwarn(an_error,msg);
	  } /* if (refcount[j] < 1) */
	} /* for (j=0; j<nsym; j++) */
	if (add) on_error++; /* Truly an error i.e. no bail outs possible */
      } /* if (!links_present) */
    } /* if (less_than_one) */
  }

  if (!on_error && redo) goto re_do;

  /* Search tables for a matching LINKOFFSET/LINKLEN or name.offset/name.len 
     Expand partial names */

  if (!on_error) {
    int count = 0;

    for (j=0; j<nsym; j++) {
      Boolean is_readonly = (has_symbol_param && inout->readonly[j]);
      char *s = sym[j]->name;
      char *pvar = NULL;
      char *pmember = NULL;
      char *ptable = NULL;
      char *poffset = NULL;
      Boolean is_hash = IS_HASH(s);
      Boolean is_formula = 
	(IS_FORMULA(s) ||
	 (may_have_select_expr && inout->sel[j] && inout->sel[j]->formula));
      ODB_SelectExpr *sel = may_have_select_expr ? inout->sel[j] : NULL;

#if 0
      fprintf(stderr,"ODB_resolve_relations(B): name[%d]='%s'\n",j,s);
#endif

      ODB_pushi(has_sign ? sign[j] : zero);

      if (is_formula) {
	ODB_pushi(linkmode[j]);
	ODB_pushstr(s);
	ODB_pushi(ReadOnly);
	ODB_pushSELECTEXPR(sel);
	count++;
	continue; /* next j */
      }

      if (refcount[j] == 1 || 
	  (allow_dollar && IS_DOLLAR(s))) {
	char *st = s;
	ODB_pushi(linkmode[j]);

	if (!(allow_dollar && IS_DOLLAR(s))) {
	  if (is_hash) {
	    pvar = STRDUP(s);
	    ptable = STRDUP(s+1);
	  }
	  else {
	    (void) ODB_split(s, NULL, &pvar, &pmember, &ptable, &poffset);
	  }
	  if (!ptable) {
	    char *xtable = NULL;
	    for (i=0; i<ntable; i++) {
	      ODB_Table *t = table[i];
	      char *tname = t->table->name;
	      Boolean found = 0;
	      /* Ignore searches from automatically inserted tables, since they
		 were not considered by the user in the first place */
	      if ((from_attr[i] & ODB_FROM_ATTR_INSERT) == ODB_FROM_ATTR_INSERT) continue;
	      found = ODB_in_table(ODB_NAME, pvar, t, NULL);
	      if (found) { /* The only occurence, since refcount[j] == 1 is checked */
		int len = strlen(pvar) + STRLEN(pmember) + strlen(tname) + 10;
		len += STRLEN(poffset);
		ALLOC(st, len);
		if (poffset && !strequ(poffset,"0")) {
		  int sign = GetSign(poffset);
		  sprintf(st, "%s%s%s@%s%s%s%s", 
			  pvar, 
			  pmember ? "." : "", 
			  pmember ? pmember : "",
			  tname,
			  ODB_OFFSET_CHAR, 
			  (sign < 0) ? "_" : "",
			  poffset+ABS(sign)
			  );
		}
		else {
		  sprintf(st, "%s%s%s@%s", 
			  pvar, 
			  pmember ? "." : "", 
			  pmember ? pmember : "",
			  tname);
		}
		break;
	      }
	    } /* for (i=0; i<ntable; i++) */
	    FREE(xtable);
	  }
	  FREE(pvar);
	  FREE(pmember);
	  FREE(ptable);
	  FREE(poffset);
	}

	ODB_pushstr(st);
	ODB_pushi(is_readonly ? ReadOnly : zero);
	ODB_pushSELECTEXPR(sel);
	count++;
	continue; /* next j */
      }

      if (is_hash) {
	pvar = STRDUP(s);
	ptable = STRDUP(s+1);
      }
      else {
	(void) ODB_split(s, NULL, &pvar, &pmember, &ptable, &poffset);
      }
      
      if (!ptable) {
	char *xtable = NULL;
	char *xpvar = NULL;

	ALLOC(xpvar, strlen(pvar) + 20);
	sprintf(xpvar,"LINKOFFSET(%s)",pvar);

	refcount[j] = 0;

	for (i=0; i<ntable; i++) {
	  ODB_Table *t = table[i];
	  char *tname = t->table->name;
	  Boolean found = 0;
	  /* Ignore searches from automatically inserted tables, since they
	     were not considered by the user in the first place */
	  if ((from_attr[i] & ODB_FROM_ATTR_INSERT) == ODB_FROM_ATTR_INSERT) continue;
	  found = ODB_in_table(ODB_NAME, xpvar, t, NULL);
	  if (found && ptable) found = strequ(tname, ptable);
	  if (found) refcount[j]++;
	  if (found && !xtable && refcount[j] == 1) xtable = STRDUP(tname);
	} /* for (i=0; i<ntable; i++) */

	if (refcount[j] == 1) {
	  /* Bingo ! */
	  char *soffset = NULL;
	  char *slen = NULL;
	  char *zz = GreenDay(poffset);

	  ODB_pushi(linkmode_1);
	  if (xtable) {
	    int len = strlen(xpvar) + strlen(xtable) + STRLEN(zz) + 10;
	    ALLOC(soffset, len);
	    snprintf(soffset, len, "%s@%s%s", xpvar,xtable,zz);
	  }
	  else {
	    soffset = STRDUP(xpvar);
	  }
	  ODB_pushstr(soffset);
	  ODB_pushi(is_readonly ? ReadOnly : zero);
	  ODB_pushSELECTEXPR(sel);

	  if (xtable) {
	    int len = strlen(pvar) + strlen(xtable) + STRLEN(zz) + 20;
	    ALLOC(slen, len);
	    snprintf(slen,len,"LINKLEN(%s)@%s%s",pvar,xtable,zz);
	  }
	  else {
	    int len = strlen(pvar) + STRLEN(zz) + 20;
	    ALLOC(slen, len);
	    snprintf(slen,len,"LINKLEN(%s)%ss",pvar,zz);
	  }

	  FREE(zz);

	  ODB_pushi(has_sign ? sign[j] : zero);
	  ODB_pushi(linkmode_2);
	  ODB_pushstr(slen);
	  ODB_pushi(is_readonly ? ReadOnly : zero);
	  ODB_pushSELECTEXPR(sel); /* This is a bit wrong, since linkoffset already got this "sel" */

	  count += 2;
	}
	else {
	  /* No luck */	
	  SETMSG2("Variable '%s' is not uniquely found from any FROM-tables; view '%s'",
		  s, vname);
	  on_error++;
	  YYwarn(on_error, msg);
	}

	FREE(xpvar);
	FREE(xtable);
      }
      else {
	SETMSG3("Error in variable '%s', view '%s'. No such table '%s' in FROM-list",
		s, vname, ptable);
	on_error++;
	YYwarn(on_error, msg);
      }
      
      FREE(pvar);
      FREE(pmember);
      FREE(ptable);
      FREE(poffset);
    } /* for (j=0; j<nsym; j++) */

    if (!on_error) {
      SymbolParam_t *out = NULL;
      ALLOC(newsym, count);
      if (has_sign) ALLOC(*sign_out, count);
      if (has_symbol_param) {
	CALLOC(out, 1);
	out->nsym = count;
	CALLOC(out->sym, count);
	CALLOC(out->readonly, count);
	CALLOC(out->sel, count);
      }

      *nsym_out = count;

      for (j=count-1; j>=0; j--) {
	ODB_SelectExpr *sel = ODB_popSELECTEXPR();
	int dummy = ODB_popi(); /* this is the readonly guy if applicable */
	char *s = ODB_popstr();
	Boolean is_hash = IS_HASH(s);
	int  lnmode = ODB_popi();
	int  key = ODB_popi();

	if (has_sign) (*sign_out)[j] = key;

	if (lnmode == 0) {
	  newsym[j] = ODB_new_symbol(is_hash ? ODB_HASHNAME : ODB_NAME, s);
	}
	else if (lnmode == 1) {
	  char *tname = strrchr(s,'@');
	  char *x = ODB_extract(s,'(',')');
	  char *y = strchr(x,'.');
	  if (y) *y = '\0';
	  if (!tname) tname = "";
	  ALLOC(y, strlen(x) + strlen(tname) + 20);
	  sprintf(y,"LINKOFFSET(%s)%s",x,tname);
	  newsym[j] = ODB_new_symbol(ODB_NAME, y);
	  FREE(x);
	}
	else if (lnmode == 2) {
	  char *tname = strrchr(s,'@');
	  char *x = ODB_extract(s,'(',')');
	  char *y = strchr(x,'.');
	  if (y) *y = '\0';
	  if (!tname) tname = "";
	  ALLOC(y, strlen(x) + strlen(tname) + 20);
	  sprintf(y,"LINKLEN(%s)%s",x,tname);
	  newsym[j] = ODB_new_symbol(ODB_NAME, y);
	  FREE(x);
	}
	if (has_symbol_param) {
	  out->sym[j] = newsym[j];
	  out->readonly[j] = dummy;
	  out->sel[j] = sel;
	}
      } /* for (j=count-1; j>=0; j--) */

      if (has_symbol_param) {
	int jj;
	/* fprintf(stderr,"<before> inout->nsym = %d\n",inout->nsym); 
	for (jj=0; jj<inout->nsym; jj++) {
	  fprintf(stderr,"@%d: '%s', readonly=%d\n",
		  jj,inout->sym[jj]->name,inout->readonly[jj]);
	}
	*/
	inout->nsym = out->nsym;
	inout->sym = out->sym;
	inout->readonly = out->readonly;
	inout->sel = out->sel;
	/*
	fprintf(stderr,"<afterr> inout->nsym = %d\n",inout->nsym);
	for (jj=0; jj<inout->nsym; jj++) {
	  fprintf(stderr,"@%d: '%s', readonly=%d\n",
		  jj,inout->sym[jj]->name,inout->readonly[jj]);
	}
	*/
      }
    } /* if (!on_error) */
  }
  
  /* Errors ? */

  if (on_error) {
    SETMSG3("Error in building table-to-%s-symbol relations in view '%s' (err. count = %d)",
	    label, vname, on_error);
    YYerror(msg);
  }

  /*
  if (newsym) {
    int count = *nsym_out;
    for (j=0; j<count; j++) {
      fprintf(stderr,"resolve(%s, %s): %d : symref=%s\n",
	      vname,label,j,symref(newsym[j]));
    }
  }
  */

  FREE(refcount);
  FREE(linkmode);

  return newsym;
}


Boolean
ODB_matchup_sym(Boolean report_error,
		const char *target_label,
		ODB_Symbol **target,
		int ntarget,
		const char *with_label,
		ODB_Symbol **with,
		int nwith,
		int *key)
{
  int i, j, errcnt = 0;
  Boolean has_key = (key != NULL);

  for (j=0; j<ntarget; j++) {
    char *st = target[j]->name;
    Boolean found = 0;

    for (i=0; i<nwith; i++) {
      char *sw = with[i]->name;
      if (strequ(st,sw)) {
	found = 1;
	/* if (has_key) key[j] = key[j] * (i + 1); */
	if (has_key) {
	  int maxcols = ODB_maxcols();
	  if (ABS(key[j]) != maxcols) {
	    key[j] = key[j] * (i + 1);
	  }
	  else if (key[j] ==  maxcols) { /* ABS-sorting, ascending */
	    key[j] =  maxcols + i + 1;
	  }
	  else if (key[j] == -maxcols) { /* ABS-sorting, descending */
	    key[j] = -(maxcols + i + 1);
	  }
	}
	break;
      }
    } /* for (i=0; i<nwith; i++) */

    if (!found) {
      if (report_error) {
	fprintf(stderr,
		"%s-variable '%s' is not specified in the %s-list\n",
		target_label, st, with_label);
      }
      if (has_key) key[j] = 0;
      errcnt++;
    }
  } /* for (j=0; j<ntarget; j++) */

  if (report_error && errcnt > 0) {
    fprintf(stderr,"%s-variables:\n",target_label);
    for (j=0; j<ntarget; j++) {
      char *st = target[j]->name;
      fprintf(stderr,"\t%d:\t%s\n",j,st);
    }
    fprintf(stderr,"%s-variables:\n",with_label);
    for (i=0; i<nwith; i++) {
      char *sw = with[i]->name;
      fprintf(stderr,"\t%d:\t%s\n",i,sw);
    }
  } /* if (report_error && errcnt > 0) */

  return (errcnt > 0);
}


int
ODB_wildcard(const char *s,
	     ODB_Table **from,
	     const int *from_attr,
	     int nfrom)
{
  int j, count = 0;
  Boolean found;
  int table_idx = -1;
  int name_idx = -1;
  char *pvar = NULL;
  char *pmember = NULL;
  char *ptable = NULL;

  (void) ODB_split(s, NULL, &pvar, &pmember, &ptable, NULL);

  if (!ptable) {
    SETMSG1("Table not speficied in SELECT-wildcard definition '%s'",s);
    YYerror(msg);
  }
  else {
    /* Cross-check that the ptable is a valid one */
    found = 0;
    for (j=0; j<nfrom; j++) {
      found = strequ(ptable,from[j]->table->name);
      if (found) {
	table_idx = j;
	break;
      }
    }
    if (!found) {
      SETMSG2("Invalid table name '%s' in SELECT-wildcard definition '%s'",
	      ptable,s);
      YYerror(msg);
    }
  }

  if (!pvar) {
    SETMSG1("Variable name not speficied in SELECT-wildcard definition '%s'",s);
    YYerror(msg);
  }
  
  if (!pmember) {
    /* A Bitfield member is NOT to be scanned */
    if (!strequ(pvar,"*")) {
      SETMSG1("Invalid SELECT-wildcard definition '%s'. Use '*@table'",s);
      YYerror(msg);
    }
    /* All okay */
    count = from[table_idx]->nsym;
    for (j=0; j<count; j++) {
      char *p = from[table_idx]->sym[j]->name;
      char *c;
      ALLOC(c, strlen(p) + 1 + strlen(ptable) + 1);
      sprintf(c,"%s@%s",p,ptable);
      ODB_pushstr(c);
    }
  }
  else {
    /* Bitfield members are to be scanned */
    if (!strequ(pmember,"*")) {
      SETMSG1("Invalid SELECT-wildcard definition '%s'. Use 'name.*@table'",s);
      YYerror(msg);
    }
    /* Cross-check that the pvar exists in the ptable i.e. from[table_idx] */
    found = 0;
    for (j=0; j<from[table_idx]->nsym; j++) {
      found = strequ(pvar, from[table_idx]->sym[j]->name);
      if (found) {
	name_idx = j;
	break;
      }
    }
    if (!found) {
      SETMSG3("Error in SELECT-wildcard '%s': Variable '%s' is not present in table '%s'",
	      s, pvar, ptable);
      YYerror(msg);
    }
    /* Make sure it is a Bitfield */
    if (!from[table_idx]->type[name_idx]->bitstream) {
      SETMSG2("Error in SELECT-wildcard '%s': Variable '%s' is not a bitfield",s,pvar);
      YYerror(msg);
    }
    /* All okay */
    count = from[table_idx]->type[name_idx]->nsym;
    for (j=0; j<count; j++) {
      char *p = from[table_idx]->type[name_idx]->member[j]->name;
      char *c;
      ALLOC(c, strlen(pvar) + 1 + strlen(p) + 1 + strlen(ptable) + 1);
      sprintf(c,"%s.%s@%s",pvar,p,ptable);
      ODB_pushstr(c);
    }
  }
  

  FREE(pvar);
  FREE(pmember);
  FREE(ptable);

  return count;
}


PUBLIC Boolean
ODB_is_integer(const char *s)
{
  Boolean is_integer = 0;
  if (s && (isdigit(*s) || (*s == '+') || (*s == '-'))) {
    double d = atof(s);
    double flr = floor(d);
    is_integer = (d == flr && flr >= -INT_MAX && flr <= INT_MAX);
  }
  /*
  fprintf(stderr,
	  "ODB_is_integer('%s'): is_integer = %d\n",
	  s ? s : NIL,(int)is_integer);
  */
  return is_integer;
}


PUBLIC Boolean
ODB_is_dollar(const char *s)
{
  Boolean is_dollar = 0;
  if (IS_DOLLAR(s)) {
    int ok = 1;
    while (*++s && ok) {
      char c = *s;
      if (isalnum(c) || c == '_') continue; /* still ok */
      ok = 0;
      break;
    }
    is_dollar = ok ? 1 : 0;
  }
  return is_dollar;
}



PRIVATE void
Form_AND_expr(ODB_View *pview, ODB_Tree *pcond)
{
  if (pcond) {
    int what = pcond->what;
    if (what == ODB_AND) {
      ODB_Tree *left = pcond->argv[0];
      ODB_Tree *right = pcond->argv[1];

      if (left && left->what != ODB_AND) {
	ODB_pushexpr(left);
	pview->andlen++;
      } 
      else {
	Form_AND_expr(pview, left);
      }

      if (right && right->what != ODB_AND) {
	ODB_pushexpr(right);
	pview->andlen++;
      }
      else {
	Form_AND_expr(pview, right);
      }
    } /* if (what == ODB_AND) */
  }
}


PUBLIC void
ODB_setup_where(ODB_View *pview)
{
  int j, nfrom;
  ODB_Tracesym t;
  
  t.flag = 0;
  t.next = pview->nselect_all;
  t.maxfrom = 0;
  t.table_index = pview->table_index;
  t.where = pview->def_put;
  t.tag = pview->tag;
  t.has_maxcount = 0;
  t.has_Unique = 0;
  t.has_thin = 0;
  
  (void) ODB_trace_symbols(pview->cond, &t, 0);
  
  nfrom =  pview->nfrom;

  /* Check if the special maxcount() function was referred to */
  if (t.has_maxcount) t.maxfrom = nfrom - 1;
  pview->maxfrom = t.maxfrom;

  /* Check if thin-function was referred to */
  pview->has_thin = t.has_thin;
  
  /* fprintf(stderr,"ODB_VIEW(%s): maxfrom = %d\n",name,maxfrom); */
  
  /* Check "string"'s */
  
  t.flag = 1;
  t.next = pview->nselect_all;
  t.maxfrom = 0;
  t.table_index = pview->table_index;
  t.where = pview->def_put;
  t.tag = pview->tag;
  t.has_maxcount = 0;
  t.has_Unique = 0;
  t.has_thin = 0;
  
  (void) ODB_trace_symbols(pview->cond, &t, 0);

  /* Active TABLEs */

  CALLOC(pview->active, nfrom);
  for (j=0; j<pview->nselect; j++) {
    int tidx = pview->table_index[j];
    if (tidx >= 0 && tidx < nfrom) pview->active[tidx] |= 0x1;
  }

  if (pview->nselsym > 0) { /* Right place ? */
    int nselect_all = pview->nselect_all;
    int nwhere = pview->nwhere;
    int norderby = pview->norderby;
    int nselsym = pview->nselsym;
    for (j=nselect_all+nwhere+norderby; j<nselect_all+nwhere+norderby+nselsym; j++) {
      int tidx = pview->table_index[j];
      if (tidx >= 0 && tidx < nfrom) pview->active[tidx] |= 0x1;
    }
  }

  /* Prepare for merged table indices */

  CALLOC(pview->merged_with, nfrom);
  for (j=0; j<pview->nselect; j++) {
    int tidx = pview->table_index[j];
    if (tidx >= 0 && tidx < nfrom) pview->merged_with[tidx] = NULL;
  } /* for (j=0; j<pview->nselect; j++) */

  /* Form the AND-expression list */

  pview->andlen = 0;
  FREE(pview->andlist);

  if (pview->cond) {
    Boolean *has_Unique;

    Form_AND_expr(pview, pview->cond);
    
    if (pview->andlen == 0) {
      ODB_pushexpr(pview->cond);
      pview->andlen = 1;
    }
    
    ALLOC(pview->andlist, pview->andlen);
    ALLOC(has_Unique, pview->andlen);
    
    for (j=pview->andlen-1; j>=0; j--) {
      ODB_Tree *expr = ODB_popexpr();
      pview->andlist[j].expr = expr;
      t.flag = 2;
      t.next = 0;
      t.maxfrom = 0;
      t.table_index = NULL;
      t.where = NULL;
      t.tag = NULL;
      t.has_maxcount = 0;
      t.has_Unique = 0;
      t.has_thin = 0;
      
      (void) ODB_trace_symbols(expr, &t, 0);
      
      if (t.has_maxcount) t.maxfrom = nfrom - 1;
      has_Unique[j] = t.has_Unique;
      
      pview->andlist[j].maxfrom = t.maxfrom;
    }

    /* Unhoist the Unique() AND-expr to the most inner loop (i.e. pview->maxfrom) */
    for (j=0; j<pview->andlen; j++) {
      if (has_Unique[j]) pview->andlist[j].maxfrom = pview->maxfrom;
    }

    FREE(has_Unique);

#if 0
    fprintf(stderr,"*** DEBUG print from ODB_setup_where ***\n");
    fprintf(stderr,"View='%s' : No. of AND-expressions = %d\n",
	    pview->view->name,pview->andlen);
    for (j=0; j<pview->andlen; j++) {
      ODB_Tree *expr = pview->andlist[j].expr;
      fprintf(stderr,"\t(%d) : addr=%p : maxfrom = %d (nfrom-1=%d, Views maxfrom=%d)\n",
	      j,
	      expr, pview->andlist[j].maxfrom,
	      nfrom - 1, pview->maxfrom);
      dump_c(stderr, 0, expr);
      fprintf(stderr,"\n");
    }
    fprintf(stderr,"*** End of DEBUG print ***\n");
#endif
  }
}


PUBLIC Boolean
ODB_fixconv(ODB_Tree *pnode)
{
  Boolean fixed = 0;
  extern int LEX_convflag;
  extern double LEX_unconv_value;
  if (LEX_convflag) {
    if (pnode->what == ODB_NUMBER) {
      double dval = pnode->dval;
      switch (LEX_convflag) {
      case 1: /* Celsius */
	dval = c2k(-LEX_unconv_value);
	fixed = 1;
	break;
      case 2: /* Fahrenheit */
	dval = f2k(-LEX_unconv_value);
	fixed = 1;
	break;
      }
      pnode->dval = dval;
    }
    LEX_convflag = 0;
    LEX_unconv_value = 0;
  }
  else if (pnode->what == ODB_NUMBER) {
    pnode->dval = -pnode->dval; /* Unary minus */
    fixed = 1;
  }
  return fixed;
}
