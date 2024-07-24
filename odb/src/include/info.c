
/* info.c */

/* Reads ODB INFO-file and returns query parameters
   back in a more digested form */

#include "odb.h"
#include "info.h"
#include "result.h"
#include "evaluate.h"
#include "magicwords.h"
#include "idx.h"
#include "cdrhook.h"

#define IN_DATABASE              0x00000001ull
#define IN_VIEWNAME              0x00000002ull
#define IN_FROM                  0x00000004ull
#define IN_SELECT                0x00000008ull
#define IN_WHERESYM              0x00000010ull
#define IN_ORDERBY               0x00000020ull
#define IN_UNIQUEBY              0x00000040ull
#define IN_WHERECOND             0x00000080ull
#define IN_PREFETCH              0x00000100ull
#define IN_LINKS                 0x00000200ull
#define IN_SET                   0x00000400ull
#define IN_WHERECOND_AND         0x00000800ull
#define IN_HAS_SELECT_DISTINCT   0x00001000ull
#define IN_HAS_AGGRFUNCS         0x00002000ull
#define IN_NICKNAME              0x00004000ull
#define IN_ERROR_CODE            0x00008000ull
#define IN_MAXCOLS               0x00010000ull
#define IN_STRINGS               0x00020000ull
#define IN_COLAUX                0x00040000ull
#define IN_HAS_COUNT_STAR        0x00080000ull
#define IN_SQL_QUERY             0x00100000ull
#define IN_HAS_THIN              0x00200000ull
#define IN_CREATE_INDEX          0x00400000ull
#define IN_USE_INDICES           0x00800000ull
#define IN_USE_INDEX_NAME        0x01000000ull
#define IN_BINARY_INDEX          0x02000000ull
#define IN_SRCPATH               0x04000000ull
#define IN_DATAPATH              0x08000000ull
#define IN_IDXPATH               0x10000000ull
#define IN_POOLMASK              0x20000000ull

PRIVATE FILE *fp_echo = NULL; /* Not thread-safe yet */

PUBLIC FILE *
ODBc_debug_fp(FILE *fp_debug)
{ /* Not thread-safe yet */
  FILE *old_fp = fp_echo;
  fp_echo = fp_debug;
  return old_fp;
}

PUBLIC FILE *
ODBc_get_debug_fp()
{ /* Not thread-safe yet */
  return fp_echo;
}

PRIVATE int CountChars(const char *s, char ch)
{
  int cnt = 0;
  if (s && ch) {
    while (*s) {
      if (*s == ch) ++cnt;
      ++s;
    }
  }
  return cnt;
}

PRIVATE char *
MassageLinkName(const char *s, Bool linklen)
{
  char *name = STRDUP(s);
  char *p = name;
  char *child = NULL;
  char *parent = NULL;
  DRHOOK_START(MassageLinkName);
  p = name;
  while (*p) {
    char ch = *p;
    if (ch == '(' && !child) {
      child = ++p;
    }
    else if (ch == ')' && child) {
      *p++ = '\0';
    }
    else if (ch == '@' && child && !parent) {
      parent = ++p;
    }
    else {
      ch = ToLower(ch);
      *p++ = ch;
    }
  }
  if (child && parent) {
    int len = STRLEN(child) + STRLEN(parent) + 3;
    len += (linklen ? STRLEN("len") : STRLEN("offset"));
    ALLOC(p, len);
    snprintf(p, len, "%s.%s@%s", child, linklen ? "len" : "offset", parent);
    FREE(name);
    name = STRDUP(p);
  }
  else { /* No luck */
    FREE(name);
    name = STRDUP(s);
  }
  DRHOOK_END(0);
  return name;
}

PRIVATE Bool
has_ll(const char *s)
{
  Bool haz = (s && strstr(s, "degrees(")) ? true : false;
  if (!haz && s) haz = (strstr(s, "radians(")) ? true : false;
  if (!haz && s) haz = (strstr(s, "Inside(")) ? true : false;
  if (!haz && s) haz = (strstr(s, "InPolygon(")) ? true : false;
  if (!haz && s) haz = (strstr(s, "Near(")) ? true : false;
  return haz;
}

PRIVATE Bool
has_links(const char *s)
{
  Bool haz = (s && strstr(s, "LINKLEN(")) ? true : false;
  if (!haz && s) haz = (strstr(s, "LINKOFFSET(")) ? true : false;
  return haz;
}

PRIVATE char *
CheckAgainstLinks(char *s, const info_t *info)
{
  DRHOOK_START(CheckAgainstLinks);
  if (s && info) {
    if (has_links(s)) {
      char *name = STRDUP(s); /* Note, that the length of 's' does never INCREASE in the process */
      int j, n;

      /* Check against prefetched links first */
      n = info->nprefetch;
      for (j=0; j<n; j++) {
	if (has_links(name)) { /* still has links */
	  col_t *colthis = &info->p[j];
	  /* Using function 'ReplaceSubStrings' from lib/evaluate.c */
	  char *new = ReplaceSubStrings(name,                /* 'haystack' */
					colthis->fetch_name, /* 'needle' */
					colthis->name,       /* 'repl_with' */
					true,                /* 'all_occurences' */
					false,               /* 'remove_white_space' */
					false);              /* 'ignore_case' */
	  FREE(name);
	  name = new;
	}
	else
	  goto done;
      } /* for (j=0; j<n; j++) */

      /* Check against links in select-symbols */
      n = info->ncols;
      for (j=0; j<n; j++) {
	if (has_links(name)) { /* still has links */
	  col_t *colthis = &info->c[j];
	  if (colthis->dtnum == DATATYPE_LINKOFFSET || 
	      colthis->dtnum == DATATYPE_LINKLEN) {
	    /* Using function 'ReplaceSubStrings' from lib/evaluate.c */
	    char *new = ReplaceSubStrings(name,                /* 'haystack' */
					  colthis->fetch_name, /* 'needle' */
					  colthis->name,       /* 'repl_with' */
					  true,                /* 'all_occurences' */
					  false,               /* 'remove_white_space' */
					  false);              /* 'ignore_case' */
	    FREE(name);
	    name = new;
	  }
	}
	else
	  goto done;
      } /* for (j=0; j<n; j++) */

      /* Check against links in where-symbols */
      n = info->nwhere;
      for (j=0; j<n; j++) {
	if (has_links(name)) { /* still has links */
	  col_t *colthis = &info->w[j];
	  if (colthis->dtnum == DATATYPE_LINKOFFSET || colthis->dtnum == DATATYPE_LINKLEN) {
	    /* Using function 'ReplaceSubStrings' from lib/evaluate.c */
	    char *new = ReplaceSubStrings(name,                /* 'haystack' */
					  colthis->fetch_name, /* 'needle' */
					  colthis->name,       /* 'repl_with' */
					  true,                /* 'all_occurences' */
					  false,               /* 'remove_white_space' */
	                                  false);              /* 'ignore_case' */
	    FREE(name);
	    name = new;
	  }
	}
	else
	  goto done;
      } /* for (j=0; j<n; j++) */

    done:
      strcpy(s, name);
    }
  }
  DRHOOK_END(0);
  return s;
}

#define CHECK_SIZE(ds, x, num, cnt) \
if (ds->num < cnt) { \
  REALLOC(ds->x, cnt); \
  ds->num = cnt; \
}

#define INVALID_INPUT(p, nelem, got) { \
  fprintf(stderr,"***Error in %s:%d: Invalid input line = '%s'\n",__FILE__,__LINE__,p); \
  fprintf(stderr,"                   Expecting %d elements, but got %d.\n",nelem,got); \
  RAISE(SIGABRT); \
}

#define GET_THESE(x, num) \
CHECK_SIZE(info, x, num, cnt); \
{ \
  /* nelem = sscanf(p,"%d %s\t%d %d %d",&flag,s,&bitpos,&bitlen,&ioffset); */ \
  char *p_space = NULL; \
  char *p_tab = NULL; \
  nelem = 0; \
  p_space = strchr(p,' '); \
  p_tab = strchr(p,'\t'); \
  nelem = sscanf(p,"%d",&flag); \
  if (nelem == 1 && p_space && p_tab) { \
    while(p_space < p_tab && isspace(*p_space)) p_space++; \
    if (p_space < p_tab) { \
      *p_tab++ = '\0'; \
      strcpy(s,p_space); nelem++; \
      nelem += sscanf(p_tab,"%d %d %d",&bitpos,&bitlen,&ioffset); \
    } \
  } \
}\
if (nelem == 5) { \
  uint dtnum; \
  col_t *colthis = &info->x[j]; \
  char *dtype = s; \
  p = strchr(s,':'); \
  if (p) { *p++ = '\0'; } else { p = s; dtype = "Formula"; } \
  colthis->dtype = STRDUP(dtype); \
  dtnum = get_dtnum(dtype); \
  if (dtnum == DATATYPE_BITFIELD && bitlen > 0) { \
    /* remove the member part, since all we need is in (bitpos,bitlen) */ \
    char *tmp = STRDUP(p); \
    char *ptmp = tmp; \
    char *pp = p; \
    Bool take = true; \
    while (*pp) { \
      if (*pp == '.') take = false; \
      else if (*pp == '@') take = true; \
      if (take) *ptmp++ = *pp; \
      pp++; \
    } \
    *ptmp = '\0'; \
    colthis->fetch_name = STRDUP(tmp); \
    FREE(tmp); \
    dtnum = DATATYPE_INT4; \
    colthis->name = STRDUP(p); \
  } \
  else if (dtnum == DATATYPE_LINKOFFSET || dtnum == DATATYPE_LINKLEN) { \
    colthis->fetch_name = STRDUP(p); \
    colthis->name = MassageLinkName(p, (dtnum == DATATYPE_LINKLEN) ? true : false); \
  } \
  else { \
    colthis->fetch_name = STRDUP(p); \
    colthis->name = STRDUP(p); \
    if (IS_(UNIQNUM,colthis->name)) dtnum = DATATYPE_UINT8; \
  } \
  colthis->dtnum = dtnum; \
  colthis->nickname = NULL; \
  colthis->kind = flag; \
  colthis->bitpos = bitpos; \
  colthis->bitlen = bitlen; \
  colthis->ioffset = ioffset; \
  colthis->table_id = -1; \
  colthis->t = NULL; \
  colthis->dsym = NULL; \
  colthis->dinp = NULL; \
  colthis->dinp_len = 0; \
  colthis->dinp_alloc = false; \
  colthis->formula_ptree = NULL; \
  colthis->formula_ptree_owner = false; \
} \
else { \
  INVALID_INPUT(p, 5, nelem); \
}

#define COPY_THESE(x,j,cin) \
{ \
  col_t *c = &info->x[j]; \
  c->name = STRDUP(cin->name); \
  c->fetch_name = cin->fetch_name ? STRDUP(cin->fetch_name) : STRDUP(cin->name); \
  c->nickname = cin->nickname ? STRDUP(cin->nickname) : NULL; \
  c->dtnum = (cin->dtnum != DATATYPE_UNDEF) ? cin->dtnum : get_dtnum("double"); \
  c->dtype = STRDUP(cin->dtype); \
  c->kind = 1; \
  c->bitpos = 0; \
  c->bitlen = 0; \
  c->ioffset = 0; \
  c->table_id = cin->table_id; \
  c->t = cin->t; \
  c->dsym = NULL; \
  c->dinp = NULL; \
  c->dinp_len = 0; \
  c->dinp_alloc = false; \
  c->formula_ptree = NULL; \
  c->formula_ptree_owner = false; \
}

#define HARDCOPY_THESE(x,j,Cin) \
{ \
  col_t *c = &info->x[j]; \
  col_t *cin = Cin; \
  memcpy(c, cin, sizeof(*c)); \
  cin->name = NULL; \
  cin->fetch_name = NULL; \
  cin->nickname = NULL; \
  cin->dtype = NULL; \
  cin->dsym = NULL; \
  cin->dinp = NULL; \
  cin->dinp_len = 0; \
  cin->dinp_alloc = false; \
  cin->formula_ptree = NULL; \
  cin->formula_ptree_owner = false; \
}

#define FILL_TABLE_ID(x, num, do_test, is_select) \
{ \
  int j; \
  for (j=0; j<num; j++) { \
    int i; \
    col_t *colthis = &info->x[j]; \
    const char *hash = IS_HASH(colthis->name) ? colthis->name : NULL; \
    const char *at = colthis->name ? strchr(colthis->name,'@') : NULL; \
    colthis->table_id = -1; /* Initialization */ \
    colthis->t = NULL; /* Initialization */ \
    if ((!at && !hash) || (do_test && (colthis->kind == 2 || colthis->kind == 4))) { \
      continue; \
    } \
    else if (at++) { \
      const char *poffset = GET_OFFSET(at); \
      int len = poffset ? (int)(poffset - at) : STRLEN(at); \
      for (i=0; i<info->nfrom; i++) { \
	if (strnequ(info->t[i].name,at,len)) { \
	  colthis->table_id = i; \
	  colthis->t = &info->t[i]; \
          if (is_select) colthis->t->in_select_clause = true; \
	  break; \
	} \
      } /* for (i=0; i<info->nfrom; i++) */ \
    } \
    else if (hash) { \
      ++hash; \
      for (i=0; i<info->nfrom; i++) { \
	if (strequ(info->t[i].name,hash)) { \
	  colthis->table_id = i; \
	  colthis->t = &info->t[i]; \
          if (is_select) colthis->t->in_select_clause = true; \
	  break; \
	} \
      } /* for (i=0; i<info->nfrom; i++) */ \
    } \
  } /* for (j=0; j<num; j++) */ \
}


typedef struct _RunOnce_t {
  char *cmd;
  uresult_t u;
  int refcount;
  struct _RunOnce_t *next;
} RunOnce_t;


PRIVATE RunOnce_t *
Init_RunOnceCheck()
{
  RunOnce_t *ro;
  CALLOC(ro, 1);
  return ro;
}

PRIVATE RunOnce_t *
Destroy_RunOnceCheck(RunOnce_t *ro)
{
  while (ro) {
    RunOnce_t *ro_next = ro->next;
    FREE(ro->cmd);
    FREE(ro);
    ro = ro_next;
  }
  return NULL;
}

PRIVATE RunOnce_t *
Find_RunOnceCheck(RunOnce_t *ro, const char *cmd)
{
  while (ro) {
    if (strequ(ro->cmd, cmd)) return ro;
    ro = ro->next;
  }
  return NULL;
}

PRIVATE char *
Do_RunOnceCheck(RunOnce_t *ro, 
		char *w /* note: must be FREE'able !! */,
		Bool EvalMe)
{
  if (ro && w) {
    const char *key = EvalMe ? "EvalMe(" : "RunOnceQuery(";
    int keylen = STRLEN(key);
    for (;;) {
      const char *p = w;
      const char *str = strstr(p, key);
      if (str) {
	const char *pstart = str;
	const char *pend = NULL;
	int paren = 1;
	p = str + keylen;
	while (*p && paren > 0) {
	  if (*p == '(') paren++;
	  else if (*p == ')') paren--;
	  if (paren == 0) pend = p;
	  p++;
	}
	if (pend) {
	  int iret = 0;
	  double value = 0;
	  void *parse_tree = NULL;
	  char valstr[100];
	  char *cmd = NULL;
	  int cmdlen = pend - pstart + 1;
	  RunOnce_t *found = NULL;
	  ALLOCX(cmd, cmdlen + 1);
	  snprintf(cmd,cmdlen+1,"%*.*s",cmdlen,cmdlen,pstart);
	  found = Find_RunOnceCheck(ro, cmd);

	  if (found) {
	    /* Already ran once !! */
	    if (EvalMe) {
	      parse_tree = found->u.parse_tree;
	    }
	    else {
	      value = found->u.alias;
	    }
	    found->refcount++;
	  }
	  else {
	    RunOnce_t *ro_save = ro;
	    if (EvalMe) {
	      /* Evalme(<expr>) --> only <expr> will be passed to the ParseTree */
	      char *expr = STRDUP(cmd + keylen);
	      char *closing_paren = strrchr(expr, ')');
	      if (closing_paren) *closing_paren = '\0';
	      parse_tree = ParseTree(expr, &iret);
	      ODB_fprintf(fp_echo,
			  "Do_RunOnceCheck for Evalme of '%s' [iret = %d] : parse_tree = %p\n",
			  expr, iret, parse_tree);
	      FREE(expr);
	    }
	    else {
	      value = Run(cmd, &iret, NULL, NULL, NULL, true);
	    }
	    if (iret != 0) {
	      /* Should not happen with */
	      fprintf(stderr, 
		      "***Error in Do_RunOnceCheck() [iret = %d] : Could not %s '%s'\n",
		      iret,
		      EvalMe ? "create parse-tree on" : "evaluate",
		      cmd);
	      RAISE(SIGABRT);
	    }
	    while (ro) {
	      if (!ro->cmd) {
		ro->cmd = STRDUP(cmd);
		if (EvalMe) {
		  ro->u.parse_tree = parse_tree;
		}
		else {
		  ro->u.alias = value;
		}
		ro->refcount = 1;
		found = ro;
		break;
	      }
	      else if (!ro->next) {
		ro->next = Init_RunOnceCheck();
	      }
	      ro = ro->next;
	    } /* while (ro) */
	  }

	  if (found->u.i[0] == 0 && found->u.i[1] == 0) {
	    strcpy(valstr,"0");
	  }
	  else {
	    snprintf(valstr, sizeof(valstr), 
		     "Conv_llu2double(%d, %d)",
		     found->u.i[0], found->u.i[1]);
	  }
	  
	  {
	    /* Replace {RunOnceQuery(...)|EvalMe(...)} with Conv_llu2double(...) */
	    char *new = ReplaceSubStrings(w,                /* 'haystack' */
					  cmd,              /* 'needle' */
					  valstr,           /* 'repl_with' */
					  true,             /* 'all_occurences' */
					  false,            /* 'remove_white_space' */
					  false);           /* 'ignore_case' */
	    FREE(w);
	    w = new;
	  }

	  FREEX(cmd);
	} /* if (pend) */
      }
      else
	break; /* for (;;) */
    } /* for (;;) */
  } /* if (ro && w) */
  return w;
}


PUBLIC void *
ODBc_make_setvars(const char *varvalue, int *nsetvar)
{
  int n = 0;
  set_t *setvar = NULL;
  DRHOOK_START(ODBc_make_setvars);

  if (varvalue && !strequ(varvalue,"-") && strchr(varvalue,'=')) {
    const char *delim = ",";
    char *var = STRDUP(varvalue);
    char *token = strtok(var,delim);
    while (token) {
      char *ss = STRDUP(token);
      char *t = strchr(ss,'=');
      if (t) {
	int j = n++;
	char *lhs = ss;
	char *rhs = t + 1;
	char *comma = strchr(rhs,',');
	int len_lhs, len_rhs;
	*t = '\0';
	if (comma) *comma = '\0';
	if (*lhs == '$') ++lhs;
	len_lhs = STRLEN(lhs);
	len_rhs = STRLEN(rhs);
	REALLOC(setvar, n);
	ALLOC(setvar[j].name, len_lhs + 2);
	snprintf(setvar[j].name, len_lhs + 2, "$%s", lhs);
	setvar[j].value = STRLEN(rhs) > 0 ? atof(rhs) : 0;
      } /* if (t) */
      FREE(ss);
      token = strtok(NULL,delim);
    } /* while (token) */
    FREE(var);
  }

  if (nsetvar) *nsetvar = n;
  DRHOOK_END(0);
  return setvar;
}


#if defined(MAXLINE)
#undef MAXLINE
#endif
#define MAXLINE    65536

static Bool MakePlotColumns(info_t *info_inout, Bool warrow,
			    int latcol, int loncol, int colorcol, int ucol, int vcol);

static Bool HasSimpleWHEREcond(const info_t *info, table_t *t, col_t *w, int nwhere, const set_t *set, int nset);

PUBLIC void *
ODBc_get_info(const char *info_file_or_pipe, const set_t *setvar, int nsetvar)
{
  info_t *info_chain = NULL;
  DRHOOK_START(ODBc_get_info);
  {
    Bool is_stdin = strequ(info_file_or_pipe,"stdin") ? true : false;
    Bool is_pipe = (info_file_or_pipe && (info_file_or_pipe[0] == '|')) ? true : false;
    FILE *fp = NULL;
    if (is_stdin) {
      fp = stdin;
    }
    else if (is_pipe) {
      fp = popen(++info_file_or_pipe,"r");
    }
    else {
      fp = fopen(info_file_or_pipe,"r");
    }
    if (fp) {
      Bool is_plotobs = ODBc_test_format_1("plotobs");
      Bool warrow = ODBc_test_format_1("wplotobs");
      info_t *info = NULL;
      char *dir = NULL;
      char *host = NULL;
      char *tstamp = NULL;
      u_ll_t in = 0;
      int cnt = 0;
      table_t *twa = NULL;
      Bool first_and = false;
      int errflg = 0;
      char buf[MAXLINE];
      while ( fgets(buf,sizeof(buf),fp) ) {
	char *nl = strchr(buf,'\n');
	if (nl) *nl = '\0';
	ODB_fprintf(fp_echo,"%s\n",buf);
	
	if (!in && strnequ(buf,"INFO",4)) {
	  char *env = NULL;
	  if (info) {
	    CALLOC(info->next, 1);
	    info = info->next;
	  }
	  else {
	    CALLOC(info,1);
	    info_chain = info;
	  }
	  info->has_ll = false;
	  info->latlon_rad = -1; /* undefined */
	  env = getenv("ODB_LATLON_RAD");
	  if (env) {
	    info->latlon_rad = atoi(env);
	    codb_change_latlon_rad_(&info->latlon_rad);
	  }

	  env = getenv("ODB_LAT");
	  CALLOC(info->odb_lat, 1);
	  info->odb_lat->name = env ? STRDUP(env) : STRDUP("lat@hdr");

	  env = getenv("ODB_LON");
	  CALLOC(info->odb_lon, 1);
	  info->odb_lon->name = env ? STRDUP(env) : STRDUP("lon@hdr");

	  env = getenv("ODB_DATE");
	  CALLOC(info->odb_date, 1);
	  info->odb_date->name = env ? STRDUP(env) : STRDUP("date@hdr");

	  env = getenv("ODB_TIME");
	  CALLOC(info->odb_time, 1);
	  info->odb_time->name = env ? STRDUP(env) : STRDUP("time@hdr");

	  env = getenv("ODB_COLOR");
	  CALLOC(info->odb_color, 1);
	  if (warrow) {
	    info->odb_color->name = env ? STRDUP(env) : STRDUP("speed");
	  }
	  else {
	    info->odb_color->name = env ? STRDUP(env) : STRDUP("obsvalue@body");
	  }

	  env = getenv("ODB_U");
	  CALLOC(info->odb_u, 1);
	  info->odb_u->name = env ? STRDUP(env) : STRDUP("obsvalue@body");

	  env = getenv("ODB_V");
	  CALLOC(info->odb_v, 1);
	  info->odb_v->name = env ? STRDUP(env) : STRDUP("obsvalue@body#1"); /* offset(obsvalue@body,1) */

	  in = 0;
	  cnt = 0;
	  twa = NULL;
	  first_and = false;
	}
	else if (!in && buf[0] != '/') {
	  continue;
	}
	else if (strnequ(buf,"/end ",5)) {
	  in = 0;
	  cnt = 0;
	  twa = NULL;
	  first_and = false;
	}
	else if (!in && buf[0] == '/') {
	  char *eq = strchr(buf,'=');
	  if (eq) { /* /xxx=yyy */
	    char *key = buf;
	    *eq++ = '\0';
	    while (isspace(*eq)) eq++;
	    if (strequ(key,"/dir")) {
	      if (dir) FREE(dir);
	      dir = STRDUP(eq);
	    }
	    else if (strequ(key,"/host")) {
	      if (host) FREE(host);
	      host = STRDUP(eq);
	    }
	    else if (strequ(key,"/tstamp")) {
	      if (tstamp) FREE(tstamp);
	      tstamp = STRDUP(eq);
	    }
	    else if (strequ(key,"/database")) {
	      info->dbcred.dbname = STRDUP(eq);
	    }
	    else if (strequ(key,"/srcpath")) {
	      info->dbcred.srcpath = STRDUP(eq);
	    }
	    else if (strequ(key,"/datapath")) {
	      info->dbcred.datapath = STRDUP(eq);
	    }
	    else if (strequ(key,"/idxpath")) {
	      info->dbcred.idxpath = STRDUP(eq);
	    }
	    else if (strequ(key,"/poolmask")) {
	      info->dbcred.poolmask = STRDUP(eq);
	    }
	    else if (strequ(key,"/viewname")) {
	      info->view = STRDUP(eq);
	    }
	    else if (strequ(key,"/use_index_name")) {
	      info->use_index_name = STRDUP(eq);
	    }
	    else if (strequ(key,"/has_select_distinct")) {
	      info->has_select_distinct = strequ(eq,"1") ? true : false;
	    }
	    else if (strequ(key,"/has_count_star")) {
	      info->has_count_star = strequ(eq,"1") ? true : false;
	    }
	    else if (strequ(key,"/has_aggrfuncs")) {
	      info->has_aggrfuncs = strequ(eq,"1") ? true : false;
	    }
	    else if (strequ(key,"/has_thin")) {
	      info->has_thin = strequ(eq,"1") ? true : false;
	    }
	    else if (strequ(key,"/use_indices")) {
	      info->use_indices = strequ(eq,"1") ? true : false;
	    }
	    else if (strequ(key,"/binary_index")) {
	      info->binary_index = strequ(eq,"1") ? true : false;
	    }
	    else if (strequ(key,"/maxcols")) {
	      int maxcols = atoi(eq);
	      if (maxcols <= 0) maxcols = ODB_maxcols(); /* Shouldn't happen */
	      info->maxcols = maxcols;
	    }
	    else if (strequ(key,"/create_index")) {
	      int ci = atoi(eq);
	      info->create_index = ci;
	    }
	    else if (strequ(key,"/error_code")) {
	      int error_code = atoi(eq);
	      if (info) {
		info->error_code = error_code;
	      }
	      if (error_code != 0) errflg++;
	    }
	    in = 0;
	    cnt = 0;
	    twa = NULL;
	    first_and = false;
	  }
	  else { /* /xxx number */
	    char key[20];
	    int num;
	    int nelem = sscanf(buf,"%s %d",key,&num);
	    if (nelem == 2) {
	      int nalloc = (num > 0) ? num : 1;
	      cnt = 0;
	      if (strequ(key,"/from")) {
		/* Here "num" must be >= 0 , with "0" meaning a table-less query */
		if (num < 0) num = 0;
		info->nfrom = num;
		if (num > 0) {
		  CALLOC(info->t, nalloc);
		}
		in = IN_FROM;
	      }
	      else if (strequ(key,"/select")) {
		if (num < 0) num = 0;
		info->ncols = num;
		info->ncols_true = 0;
		CALLOC(info->c, nalloc);
		in = IN_SELECT;
	      }
	      else if (strequ(key,"/wheresym")) {
		/* Column variables referenced in the WHERE-condition */
		if (num < 0) num = 0;
		info->nwhere = num;
		CALLOC(info->w, nalloc);
		in = IN_WHERESYM;
	      }
	      else if (strequ(key,"/wherecond")) {
		/* WHERE-condition */
		if (num < 0) num = 0;
		CALLOC(info->wherecond, nalloc);
		in = IN_WHERECOND;
	      }
	      else if (strequ(key,"/wherecond_and")) {
		/* WHERE AND-condition : one or more per table */
		/* num is now table id reference [0..nfrom-1] */
		int idx = num;
		twa = (info->t && idx >= 0 && idx < info->nfrom) ? &info->t[idx] : NULL;
		if (twa) {
		  in = IN_WHERECOND_AND;
		  first_and = true;
		}
	      }
	      else if (strequ(key,"/orderby")) {
		if (num < 0) num = 0;
		info->norderby = num;
		CALLOC(info->o, nalloc);
		in = IN_ORDERBY;
	      }
	      else if (strequ(key,"/uniqueby")) {
		if (num < 0) num = 0;
		info->nuniqueby = num;
		CALLOC(info->u, nalloc);
		in = IN_UNIQUEBY;
	      }
	      else if (strequ(key,"/strings")) {
		if (num < 0) num = 0;
		info->ns2d = num;
		CALLOC(info->s2d, nalloc);
		in = IN_STRINGS;
	      }
	      else if (strequ(key,"/prefetch")) {
		if (num < 0) num = 0;
		info->nprefetch = num;
		CALLOC(info->p, nalloc);
		in = IN_PREFETCH;
	      }
	      else if (strequ(key,"/links")) {
		in = IN_LINKS;
	      }
	      else if (strequ(key,"/colaux")) {
		if (num < 0) num = 0;
		info->ncolaux = num;
		CALLOC(info->colaux, nalloc);
		in = IN_COLAUX;
	      }
	      else if (strequ(key,"/set")) {
		if (num < 0) num = 0;
		info->nset = num;
		CALLOC(info->s, nalloc);
		in = IN_SET;
	      }
	      else if (strequ(key,"/nickname")) {
		/* Assume that this comes always LATER than "/select" */
		in = IN_NICKNAME;
	      }
	      else if (strequ(key,"/sql_query")) {
		/* SQL-query string */
		if (num < 0) num = 0;
		CALLOC(info->sql_query, nalloc);
		in = IN_SQL_QUERY;
	      }
	    }
	    else {
	      INVALID_INPUT(buf, 2, nelem);
	    } /* if (nelem == 2) ... else ... */
	  } /* if (eq) ... else ... */
	}
	else if (in) {
	  int j;
	  char *p = buf;
	  int len, nelem, flag, idx;
	  int bitpos, bitlen, ioffset;
	  char *s = NULL;
	  while (isspace(*p)) p++;
	  len = strlen(p);
	  j = cnt++;
	  ALLOC(s, len+1);
	  switch (in) {
	  case IN_FROM:
	    CHECK_SIZE(info, t, nfrom, cnt);
	    nelem = sscanf(p,"%d %s",&idx,s);
	    if (nelem == 2 && idx >= 0 && idx < info->nfrom) {
	      table_t *t = &info->t[idx];
	      t->name = STRDUP(s);
	      t->table_id = idx;
	      t->linkparent_id = -1;
	      t->offset_parent_id = -1;
	      t->b = NULL;
	      t->e = NULL;
	      t->prev = NULL;
	      t->next = NULL;
	      t->linkparent = NULL;
	      t->offset_parent = NULL;
	      t->linkoffset = NULL;
	      t->linklen = NULL;
	      t->jr = -1;
	      t->lo =  0;
	      t->ob = -1;
	      t->linkcase = -1;
	      t->offset = NULL;
	      t->len = NULL;
	      t->info = NULL;
	      t->nrows = 0;
	      t->ncols = 0;
	      t->idx = NULL;
	      t->wherecond_and = NULL;
	      t->wherecond_and_ptree = NULL;
	      t->in_select_clause = false;
	      t->simple_wherecond = false;
	      t->skiprow = NULL;
	      t->nwl = 0;
	      t->wl = NULL;
	      t->stored_idx_count = 0;
	    }
	    else {
	      INVALID_INPUT(p, 2, nelem);
	    }  /* if (nelem == 2) ... else ... */
	    break;
	  case IN_SELECT:
	    GET_THESE(c, ncols);
	    info->ncols_pure += (info->c[j].kind == 1);
	    info->ncols_formula += (info->c[j].kind == 2);
	    info->ncols_aggr_formula += (info->c[j].kind == 4);
	    info->ncols_aux += (info->c[j].kind == 8);
	    info->ncols_nonassoc += (info->c[j].kind == 0);
	    break;
	  case IN_NICKNAME:
	    {
	      const char *nickname = strchr(p, ' ');
	      nelem = sscanf(p,"%d ",&idx);
	      if (nelem == 1 && nickname && idx >= 0 && idx < info->ncols) {
		col_t *colthis = &info->c[idx];
		nickname++;
		FREE(colthis->nickname);
		colthis->nickname = STRDUP(nickname);
	      }
	      else {
		INVALID_INPUT(p, 1, nelem);
	      }
	    }
	    break;
	  case IN_WHERESYM:
	    GET_THESE(w, nwhere);
	    break;
	  case IN_PREFETCH:
	    GET_THESE(p, nprefetch);
	    break;
	  case IN_ORDERBY:
	    GET_THESE(o, norderby);
	    break;
	  case IN_UNIQUEBY:
	    GET_THESE(u, nuniqueby);
	    break;
	  case IN_STRINGS:
	    CHECK_SIZE(info, s2d, ns2d, cnt);
	    {
	      int lenstr = 0;
	      nelem = sscanf(p, "%d,%d ", &idx, &lenstr);
	      if (nelem == 2 && idx >= 0 && idx < info->ns2d && lenstr >= 0) {
		const char *s2d_value = strchr(p, ' ');
		str_t *s2d = &info->s2d[idx];
		int len = S2DlcLEN + 30;
		ALLOC(s2d->name, len);
		snprintf(s2d->name,len,"%s%d",S2Dlc,idx); /* symbol name "s2d_<number>" */
		s2d_value++;
		s2d->value = STRDUP(s2d_value);
		s2d->u.saddr = s2d->value; /* ==> "s2d->u.dval" is aliased address to the value-char */
		putsym(s2d->name, s2d->u.dval);
	      }
	      else {
		INVALID_INPUT(p, 2, nelem);
	      }
	    }
	    break;
	  case IN_LINKS:
	    CHECK_SIZE(info, t, nfrom, cnt);
	    {
	      int linkparent_id = -1;
	      int offset_parent_id = -1;
	      char *b, *e, *off;
	      ALLOCX(b, len+1);
	      ALLOCX(e, len+1);
	      ALLOCX(off, len+1);
	      nelem = sscanf(p,"%d %s %d %s %s %d %s",
			     &idx,s,&linkparent_id,b,e,
			     &offset_parent_id, off);
	      if (nelem == 7 && idx >= 0 && idx < info->nfrom) {
		table_t *t = &info->t[idx];
		/* t->name = STRDUP(s); */
		t->linkparent_id = linkparent_id;
		t->offset_parent_id = offset_parent_id;
		t->b = STRDUP(b);
		t->e = STRDUP(e);
	      }
	      else {
		INVALID_INPUT(p, 7, nelem);
	      }  /* if (nelem == 7) ... else ... */
	      FREEX(b);
	      FREEX(e);
	      FREEX(off);
	    }
	    break;
	  case IN_COLAUX:
	    CHECK_SIZE(info, colaux, ncolaux, cnt);
	    {
	      int col_aux;
	      nelem = sscanf(p, "%d %d", &idx, &col_aux);
	      if (nelem == 2 && idx >= 0 && idx < info->ncolaux) {
		info->colaux[idx] = col_aux; /* if > 0, Fortran index to a sibling column in multi-arg aggr */
	      }
	      else {
		INVALID_INPUT(p, 2, nelem);
	      }
	    }
	  case IN_SET:
	    CHECK_SIZE(info, s, nset, cnt);
	    {
	      double value = 0;
	      nelem = sscanf(p,"%s %lf", s, &value);
	      if (nelem == 2) {
		info->s[j].name = STRDUP(s);
		info->s[j].value = value;
	      }
	      else {
		INVALID_INPUT(p, 2, nelem);
	      } /* if (nelem == 2) ... else ... */
	    }
	    break;
	  case IN_WHERECOND:
	    {
	      int curlen = STRLEN(info->wherecond);
	      int newlen = curlen + len + 1; /* "+1" for extra blank */
	      REALLOC(info->wherecond, newlen + 1);
	      info->wherecond[curlen] = '\0';
	      strncat(info->wherecond, p, len);
	      info->wherecond[newlen] = '\0';
	    }
	    break;
	  case IN_SQL_QUERY:
	    {
	      int curlen = STRLEN(info->sql_query);
	      int newlen = curlen + len + 1; /* "+1" for extra newline */
	      REALLOC(info->sql_query, newlen + 1);
	      info->sql_query[curlen] = '\0';
	      strncat(info->sql_query, p, len);
	      strcat(info->sql_query, "\n");
	      info->sql_query[newlen] = '\0';
	    }
	    break;
	  case IN_WHERECOND_AND:
	    if (twa && !strequ(p,"1")) {
	      int jj = twa->nwl;
	      const char andies[] = " && ";
	      const int n_andies = 4;
	      Bool add_andies = false;
	      int curlen = STRLEN(twa->wherecond_and);
	      int newlen = curlen + len;
	      if (first_and && curlen > 0) {
		/* we already have previous WHERE-condition here */
		newlen += n_andies;
		add_andies = true;
	      }
	      CHECK_SIZE(twa, wl, nwl, jj+1);
	      {
		simple_where_t *wl = &twa->wl[jj];
		wl->condstr = STRDUP(p);
		wl->wcol = NULL; /* WHERE-variable of "lhs" */
		wl->lhs = NULL;
		wl->oper = UNKNOWN; /* == Kind_t UNKNOWN as in include/evaluate.h */
		wl->rhs = 0;
		wl->stored_idx = NULL;
	      }
	      REALLOC(twa->wherecond_and, newlen + 1);
	      twa->wherecond_and[curlen] = '\0';
	      if (add_andies) strncat(twa->wherecond_and, andies, n_andies);
	      strncat(twa->wherecond_and, p, len);
	      twa->wherecond_and[newlen] = '\0';
	      first_and = false;
	    }
	    break;
	  } /* switch (in) */
	  FREE(s);
	}
      } /* while ( fgets(buf,sizeof(buf),fp) ) */

      if (is_pipe) {
	int iret = pclose(fp);
	if (iret == -1) {
	  fprintf(stderr,
		  "***Error: Couldn't close the pipe. Maybe compilation has failed ?\n");
	  exit(SIGABRT);
	}
      }
      else if (!is_stdin) fclose(fp);

      if (!errflg && info_chain) {
	int j;
	info = info_chain;
	while (info) {
	  int nfrom = info->nfrom;

	  /* Make sure dir, hots & tstamp are filled in */

	  info->dir = STRDUP(dir);
	  info->host = STRDUP(host);
	  info->tstamp = STRDUP(tstamp);

	  info->ncols_true = info->ncols_pure + info->ncols_formula + info->ncols_aggr_formula;
	  
	  /* Fill in table ids */
	  FILL_TABLE_ID(c, info->ncols, 1, true);
	  FILL_TABLE_ID(o, info->norderby, 0, false);
	  FILL_TABLE_ID(p, info->nprefetch, 1, false);

	  FILL_TABLE_ID(odb_lat, 1, 0, false);
	  FILL_TABLE_ID(odb_lon, 1, 0, false);
	  FILL_TABLE_ID(odb_color, 1, 0, false);
	  FILL_TABLE_ID(odb_u, 1, 0, false);
	  FILL_TABLE_ID(odb_v, 1, 0, false);

	  FILL_TABLE_ID(u, info->nuniqueby, 1, false);
	  FILL_TABLE_ID(w, info->nwhere, 1, false);

	  /* Update SET-variables (IN_SET) */

	  if (setvar && nsetvar > 0) {
	    int js;
	    if (!info->s) {
	      /* Copy all */
	      ALLOC(info->s, nsetvar);
	      for (js=0; js<nsetvar; js++) {
		info->s[js].name = STRDUP(setvar[js].name);
		info->s[js].value = setvar[js].value;
	      } /* for (js=0; js<nsetvar; js++) */
	      info->nset = nsetvar;
	    }
	    else {
	      /* Update selectively & add completely new ones */
	      int jj, njj = info->nset;
	      int updcnt = 0;
	      int *is_updated = NULL;
	      CALLOC(is_updated, nsetvar);
	      for (js=0; js<nsetvar; js++) {
		const char *svname = setvar[js].name;
		double value = setvar[js].value;
		for (jj=0; jj<njj; jj++) {
		  if (strequ(info->s[jj].name, svname)) {
		    info->s[jj].value = value;
		    is_updated[js] = 1;
		    ++updcnt;
		    break;
		  }
		} /* for (jj=0; jj<njj; jj++) */
	      } /* for (js=0; js<nsetvar; js++) */
	      if (updcnt > 0) {
		updcnt += njj;
		CHECK_SIZE(info, s, nset, updcnt);
		for (js=0; js<nsetvar; js++) {
		  if (!is_updated[js]) {
		    const char *svname = setvar[js].name;
		    double value = setvar[js].value;
		    info->s[njj].name = STRDUP(svname);
		    info->s[njj].value = value;
		    ++njj;
		  }
		} /* for (js=0; js<nsetvar; js++) */
	      } /* if (updcnt > 0) */
	      FREE(is_updated);
	    }
	  }

	  if (is_plotobs || warrow) {
	    /* When plotting is concerned, then only the following SELECT-columns are needed
	       $ODB_LAT, $ODB_LON and (possibly) $ODB_COLOR 
	       and if warrow == true, check also that BOTH $ODB_U & $ODB_V are there
	       otherwise reset warrow to false
	    */
	    int ncols = info->ncols_true;
	    int latcol = -1, loncol = -1, colorcol = -1;
	    int ucol = -1, vcol = -1;

	    for (j=0; j<ncols; j++) {
	      col_t *colthis = &info->c[j];
	      if (colthis->kind == 1 || colthis->kind == 2 || colthis->kind == 4) {
		const char *s = colthis->nickname ? colthis->nickname : colthis->name;
		if (strequ(s, info->odb_lat->name)) latcol = j;
		else if (strequ(s, info->odb_lon->name)) loncol = j;
		else if (strequ(s, info->odb_color->name)) colorcol = j;
		else if (strequ(s, info->odb_u->name)) ucol = j;
		else if (strequ(s, info->odb_v->name)) vcol = j;
	      }
	    }
	    warrow = MakePlotColumns(info, warrow,
				     latcol, loncol, colorcol, ucol, vcol);
	    if (!warrow) {
	      FREE(info->odb_u->name); info->odb_u->name = STRDUP("\t<N/A>");
	      FREE(info->odb_v->name); info->odb_v->name = STRDUP("\t<N/A>");
	    }
	  } /* if (is_plotobs || warrow) */

	  /* Create ParseTree for SELECT-column entries that are formulas;
	     Remember to replace occurences of LINKLEN(child)@parent & LINKOFFSET(child)@parent
	     since the parser in evaluate.c cannot currently stand this syntax !! */
	  
	  for (j=0; j<info->ncols; j++) {
	    col_t *colthis = &info->c[j];
	    int flag = colthis->kind;
	    if (flag == 2 || flag == 4) {
	      int iret = 0;
	      colthis->name = CheckAgainstLinks(colthis->name, info);
	      colthis->formula_ptree = ParseTree(colthis->name, &iret);
	      if (iret != 0 || !colthis->formula_ptree) {
		fprintf(stderr,
			"***Error: Could not parse formula '%s' : iret = %d\n",
			colthis->name, iret);
		RAISE(SIGABRT);
	      }
	      colthis->formula_ptree_owner = true;
	      if (flag == 4 && info->colaux && j < info->ncolaux) {
		/* Adjust multi-arg. aggregate function's 2nd (or more) argument */
		int jj = info->colaux[j];
		if (jj > info->ncols_true) {
		  col_t *cs = &info->c[--jj]; /* column of a sibling */
		  if (cs->kind == 8) {
		    FREE(cs->name);
		    cs->name = STRDUP(colthis->name);
		    cs->formula_ptree = colthis->formula_ptree;
		    cs->formula_ptree_owner = false;
		  }
		}
	      }
	      if (!info->has_ll && has_ll(colthis->name)) info->has_ll = true;
	    }
	  } /* for (j=0; j<info->ncols; j++) */

	  {
	    /* Handle EvalMe(S2D_<n>)'s first 
	       Note: We share the RunOnce_t data structure due to similarities */
	     RunOnce_t *ro = Init_RunOnceCheck();
	     char *w = info->wherecond = Do_RunOnceCheck(ro, info->wherecond, true);
	     if (nfrom > 0) {
	       /* Also kill all table-specific WHERE-stmts that equal to "1" */
	       for (j=0; j<nfrom; j++) {
		 table_t *this = &info->t[j];
		 char *w = this->wherecond_and = Do_RunOnceCheck(ro, this->wherecond_and, true);
	       }
	     }
	     ro = Destroy_RunOnceCheck(ro);
	  }

	  {
	    RunOnce_t *ro = Init_RunOnceCheck();

	    if (info->wherecond) {
	      /* If the master WHERE-cond equals to "1" i.e. default, kill it */
	      char *w = info->wherecond = Do_RunOnceCheck(ro, info->wherecond, false);
	      while (isspace(*w)) w++;
	      if (strequ(w,"1")) { FREE(info->wherecond); w = NULL; }
	      if (w && info->nuniqueby == 0
		  && !strchr(w, '$')
		  && !strstr(w, "maxrows(")
		  && !strstr(w, "maxcount(")
		  && !strstr(w, "Inside(")
		  && !strstr(w, "InPolygon(")
		  && !strstr(w, "Near(")
		  && !strstr(w, "Query(")
		  ) {
		/* Check for trivial WHERE, which can be evaluated into a constant value */
		int iret = 0;
		double value = Run(w, &iret, NULL, NULL, NULL, true);
		if (iret == 0) { /* Yes, indeed !! */
		  FREE(info->wherecond); /* After this always true */
		  if (value == 0) info->wherecond = STRDUP("0"); /* i.e. never true */
		}
	      } /* if (w && info->nuniqueby == 0 & ...) */
	    }
	  
	    if (nfrom > 0) {
	      /* Also kill all table-specific WHERE-stmts that equal to "1" */
	      for (j=0; j<nfrom; j++) {
		table_t *this = &info->t[j];
		char *w = this->wherecond_and = Do_RunOnceCheck(ro, this->wherecond_and, false);
		if (w) {
		  while (isspace(*w)) w++;
		  if (strequ(w,"1")) { FREE(this->wherecond_and); w = NULL; }
		  if (w && !strchr(w, '$')
 		      && !strstr(w, "Unique(")
		      && !strstr(w, "maxrows(")
		      && !strstr(w, "maxcount(")
		      && !strstr(w, "Inside(")
		      && !strstr(w, "InPolygon(")
		      && !strstr(w, "Near(")
		      && !strstr(w, "Query(")
		      ) {
		    /* Check for trivial WHERE, which can be evaluated into a constant value */
		    int iret = 0;
		    double value = Run(w, &iret, NULL, NULL, NULL, true);
		    if (iret == 0) { /* Yes, indeed !! */
		      FREE(this->wherecond_and); /* After this always true */
		      if (value == 0) this->wherecond_and = STRDUP("0"); /* i.e. never true */
		    }
		  } /* if (w && !strchr(w, '$') && ...) */
		} /* if (w) */
	      } /* for (j=0; j<nfrom; j++) */
	    } /* if (nfrom > 0) */

	    ro = Destroy_RunOnceCheck(ro);
	  }

	  if (nfrom == 0 && info->wherecond) {
	    /* Needed for table-less SELECT/WHERE only */
	    int iret = 0;
	    info->wherecond = CheckAgainstLinks(info->wherecond, info);
	    info->wherecond_ptree = ParseTree(info->wherecond, &iret);
	    if (iret != 0 || !info->wherecond_ptree) {
	      fprintf(stderr,
		      "***Error: Could not parse (table-less) WHERE-condition '%s' : iret = %d\n",
		      info->wherecond, iret);
	      RAISE(SIGABRT);
	    }
	  }
	  else {
	    info->wherecond_ptree = NULL;
	  }

	  for (j=0; j<nfrom; j++) {
	    int len = 0;
	    table_t *this = &info->t[j];
	    int linkparent_id = this->linkparent_id;
	    int offset_parent_id = this->offset_parent_id;
	    table_t *tp, *tp_off;

	    if (this->wherecond_and) {
	      int iret = 0;
	      this->wherecond_and = CheckAgainstLinks(this->wherecond_and, info);
	      this->wherecond_and_ptree = ParseTree(this->wherecond_and, &iret);
	      if (iret != 0 || !this->wherecond_and_ptree) {
		fprintf(stderr,
			"***Error: Could not parse WHERE-condition '%s' of table '%s' : iret = %d\n",
			this->wherecond_and, this->name, iret);
		RAISE(SIGABRT);
	      }
	      if (!info->has_ll && has_ll(this->wherecond_and)) info->has_ll = true;
	    }
	    else {
	      this->wherecond_and_ptree = NULL;
	    }

	    if (linkparent_id >= 0) {
	      tp = this->linkparent = &info->t[linkparent_id];
	    }
	    else {
	      tp = this->linkparent = NULL;
	    }

	    if (offset_parent_id >= 0) {
	      tp_off = this->offset_parent = &info->t[offset_parent_id];
	    }
	    else {
	      tp_off = this->offset_parent = NULL;
	    }

	    if (tp_off) {
	      /* Note: No MassageLinkName here, since linkoffset/linklen below are used as "fetch_name" 
	               and these will never endup in symbol table */
	      len = strlen("LINKOFFSET()@") + strlen(this->name) + strlen(tp_off->name) + 1;
	      ALLOC(this->linkoffset, len);
	      ALLOC(this->linklen, len);
	      snprintf(this->linkoffset, len, "LINKOFFSET(%s)@%s", this->name, tp_off->name);
	      snprintf(this->linklen, len, "LINKLEN(%s)@%s", this->name, tp_off->name);
	    }
	    else {
	      this->linkoffset = NULL;
	      this->linklen = NULL;
	    }

	    if (*this->e == '1') { 
	      /* ONELOOPER i.e. one-to-one relationship between parent & child */
	      this->linkcase = 1;
	    }
	    else if (*this->e == 'A') { 
	      /* ALIGNed tables child1 & child2 : the same number of rows;
		 may have different parent-tables though */
	      this->linkcase = 2;
	    }
	    else if (*this->e == '=') { 
	      /* ALIGNed as in 'A', but orphaned from parent; 
		 child2 shares common row# with the aligned child1 */
	      this->linkcase = 3;
	    }
	    else if (*this->b != '0' && *this->e == 'N') {
	      /* A regular link between parent & child ; no ONELOOPERs or ALIGNments */
	      this->linkcase = 4;
	    }
	    else { 
	      /* The very first table in FROM-statement or 
		 a completely disconnected/isolated table w.r.t. other tables in FROM-stmt */
	      /* *this->b == '0' && *this->e == 'N' */
	      this->linkcase = (j == 0) ? 5 : 6;
	    }

	    this->offset = NULL;
	    this->len = NULL;
	    this->nrows = 0;
	    this->ncols = 0;
	    this->idx = NULL;
	    this->prev = (j   >     0) ? &info->t[j-1] : NULL;
	    this->next = (j+1 < nfrom) ? &info->t[j+1] : NULL;
	    this->info = info;
	  } /* for (j=0; j<nfrom; j++) */

	  /* Fill optimization flags */

	  info->optflags = 0;
	  if (info->has_count_star && !info->wherecond &&
	      info->nfrom == 1 && info->ncols == 1 &&
	      info->has_aggrfuncs) {
	    info->optflags |= 0x1;
	    info->has_aggrfuncs = false;
	  }

	  /* Index lengths */

	  info->idxalloc = 0;
	  info->idxlen = 0;
	  info->idxlen_dsym = putsym(_ROWNUM, info->idxlen);
	  info->__maxcount__ = NULL; /* Address for ODBTk's $__maxcount__ */

    	  /* In case of "$<parent_tblname>.<child_tblname>#" -variables exist ... */

	  info->dyn = NULL;
	  info->ndyn = 0;
	  if (info->t && info->ncols_nonassoc > 0) {
	    int jj = 0;
	    int nalloc = 1; /* just to get the logic in CHECK_SIZE() working */
	    CALLOC(info->dyn, nalloc); /* but keep info->ndyn still = 0 */
	    for (j=0; j<info->ncols; j++) {
	      col_t *colthis = &info->c[j];
	      char *name = colthis->name;
	      if (colthis->kind == 0 && IS_USDDOTHASH(name)) {
		table_t *t = info->t;
		char *s = STRDUP(name);
		char *parent = s+1;
		char *child = strchr(s,'.');
		char *hash = strchr(s,'#');
		dyn_t *dyn;
		*child++ = '\0';
		*hash = '\0';
		info->ndyn = jj + 1;
		CHECK_SIZE(info, dyn, ndyn, cnt);
		dyn = &info->dyn[jj];
		dyn->parent = NULL;
		dyn->child = NULL;
		while (t) {
		  if (strequ(t->name, parent)) dyn->parent = t;
		  else if (strequ(t->name, child)) dyn->child = t;
		  t = t->next;
		}
		putsym(name, 0); /* Regardless whether parent & child were present or not */
		if (dyn->parent && dyn->child) {
		  dyn->name = STRDUP(name);
		  dyn->data = NULL;
		  dyn->ndata = 0;
		  dyn->colid = j;
		  dyn->parent->in_select_clause = true;
		  dyn->child->in_select_clause = true;
		  jj = info->ndyn;
		}
		else {
		  info->ndyn--;
		}
		FREE(s);
	      } /* if (colthis->kind == 0 && IS_USDDOTHASH(name)) */
	    } /* for (j=0; j<info->ncols; j++) */
	  } /* if (info->ncols_nonassoc > 0) */

	  /* Check for simple WHERE-conditions for each table */
	  {
	    table_t *t = info->t;
	    while (t) {
	      t->simple_wherecond = HasSimpleWHEREcond(info, t, info->w, info->nwhere, info->s, info->nset);
	      t = t->next;
	    } /* while (t) */
	  }

	  if (info->create_index < 0) {
	    /* A backstitch that perhaps should have done at odb98.x -stage ... */
	    FREE(info->use_index_name);
	    info->use_index_name = STRDUP(info->view);
	  }

	  /* Redo the SQL-query */

	  {
	    char *q = NULL;
	    const char Exit[] = ";\nEXIT; // The original query follows\n\n";
	    int len = STRLEN(Exit) + STRLEN(info->sql_query) + 1;

	    if (info->create_index < 0) {
	      /* 'DROP INDEX name' or 'DROP INDEX *' etc. */
	      len += STRLEN("DROP INDEX ") + STRLEN(info->view);
	      if (info->nfrom > 0) {
		table_t *t = info->t;
		len += STRLEN(" ON ");
		while (t) {
		  len += STRLEN(t->name) + 1;
		  t = t->next;
		}
	      }
	      ALLOC(q, len);
	      snprintf(q, len, "DROP INDEX %s", info->view);
	      if (info->nfrom > 0) {
		table_t *t = info->t;
		strcat(q, " ON ");
		while (t) {
		  strcat(q, t->name);
		  t = t->next;
		  if (t) strcat(q, ",");
		}
	      }
	    }
	    else if (info->create_index == 1 || info->create_index == 2) {
	      /* 
		 CREATE UNIQUE INDEX name ON table ... 
		   or
		 CREATE BITMAP INDEX name ON table ...
	       */
	      int ncols = info->ncols_true;
	      int lencols = 0;
	      len += STRLEN("USING BINARY INDEX") + 2;
	      len += STRLEN("CREATE UNIQUE INDEX ") + STRLEN(info->view);
	      len += STRLEN(" ON ") + STRLEN(info->t->name);
	      if (ncols > 0) {
		lencols = 1;
		for (j=0; j<ncols; j++) {
		  col_t *colthis = &info->c[j];
		  int inclen = STRLEN(colthis->name) + 1;
		  len += inclen;
		  lencols += inclen;
		}
		len += 6; /* " (  ) " */
	      }
	      len += STRLEN(" WHERE ") + (info->wherecond ? STRLEN(info->wherecond) : 1);
	      ALLOC(q, len);
	      if (ncols > 0) {
		char *cols;
		ALLOC(cols, lencols);
		*cols = 0;
		for (j=0; j<ncols; j++) {
		  col_t *colthis = &info->c[j];
		  if (j>0) strcat(cols,",");
		  strcat(cols, colthis->name);
		}
		snprintf(q, len, "USING %sINDEX;\nCREATE %s INDEX %s\nON %s\n( %s )\nWHERE %s",
			 info->binary_index ? "BINARY " : "",
			 (info->create_index == 1) ? "UNIQUE" : "BITMAP",
			 info->view,
			 info->t->name,
			 cols,
			 info->wherecond ? info->wherecond : "1");
		FREE(cols);
	      }
	      else {
		snprintf(q, len, "USING %sINDEX;\nCREATE %s INDEX %s\nON %s\nWHERE %s",
			 info->binary_index ? "BINARY " : "",
			 (info->create_index == 1) ? "UNIQUE" : "BITMAP",
			 info->view,
			 info->t->name,
			 info->wherecond ? info->wherecond : "1");
	      }
	    }
	    else { /* if (info->create_index > 0) */
	      /*
		SELECT [DISTINCT] columns
		[UNIQUEBY columns]
		[FROM tables]
		[WHERE cond]
		[ORDERBY columns]
	      */
	      table_t *t = info->t;
	      int ncols = info->ncols_true;
	      int lencols = 0;
	      int uniq_ncols = (info->nuniqueby > 0 && !info->has_select_distinct) ?
		info->nuniqueby : 0;
	      int uniq_lencols = 0;
	      int sort_ncols = info->norderby;
	      int sort_lencols = 0;
	      char *where = info->wherecond ? STRDUP(info->wherecond) : STRDUP("1");
	      char *punique = strstr(where,"Unique(");
	      const char no_indices[] = "NO INDICES;\n";

	      if (punique) {
		int cnt_lb = CountChars(punique,'('); /* Count of left brackets '(' */
		int cnt_rb = CountChars(punique,')'); /* Count of right brackets ')' */
		if (cnt_lb == cnt_rb) {
		  strcpy(punique,"1");
		}
		else if (cnt_lb < cnt_rb) {
		  strcpy(punique,"1)");
		}
		else {
		  fprintf(stderr,
			  "***Error: Impossible to have more '(':s than ')':s ; in %s\n",
			  punique);
		  RAISE(SIGABRT);
		}
	      }

	      if (!info->use_indices) len += STRLEN(no_indices);

	      len += 
		STRLEN("CREATE VIEW ") + STRLEN(info->view) +
		STRLEN(" AS SELECT ") +
		(info->has_select_distinct ? STRLEN("DISTINCT ") : 0);

	      if (ncols > 0) {
		lencols = 1;
		for (j=0; j<ncols; j++) {
		  col_t *colthis = &info->c[j];
		  char *name = colthis->nickname ? colthis->nickname : colthis->name;
		  int inclen = STRLEN(name) + 1;
		  len += inclen;
		  lencols += inclen;
		}
	      }

	      if (uniq_ncols > 0) {
		uniq_lencols = 1;
		len += STRLEN(" UNIQUEBY ");
		for (j=0; j<uniq_ncols; j++) {
		  col_t *colthis = &info->u[j];
		  int inclen = STRLEN(colthis->name) + 1;
		  len += inclen;
		  uniq_lencols += inclen;
		}
	      }

	      t = info->t;
	      if (t) {
		len += STRLEN(" FROM ");
		while (t) {
		  len += STRLEN(t->name) + 1;
		  t = t->next;
		}
	      }

	      len += STRLEN(" WHERE ") + STRLEN(where);

	      if (sort_ncols > 0) {
		int maxcols = info->maxcols + 1;
		sort_lencols = 1;
		len += STRLEN(" ORDERBY ");
		for (j=0; j<sort_ncols; j++) {
		  col_t *colthis = &info->o[j];
		  int inclen = STRLEN(colthis->name) + 1;
		  if (colthis->kind < 0) inclen += STRLEN(" DESC");
		  if (ABS(colthis->kind) > maxcols) inclen += STRLEN("ABS()");
		  len += inclen;
		  sort_lencols += inclen;
		}
	      }

	      ALLOC(q, len);
	      snprintf(q, len, "%sCREATE VIEW %s AS\nSELECT %s",
		       (!info->use_indices) ? no_indices : "",
		       info->view, 
		       info->has_select_distinct ? "DISTINCT " : "");

	      if (ncols > 0) {
		for (j=0; j<ncols; j++) {
		  col_t *colthis = &info->c[j];
		  char *name = colthis->nickname ? colthis->nickname : colthis->name;
		  if (j>0) strcat(q, ",");
		  strcat(q, name);
		}
	      }

	      if (uniq_ncols > 0) {
		strcat(q,"\nUNIQUEBY ");
		for (j=0; j<uniq_ncols; j++) {
		  col_t *colthis = &info->u[j];
		  char *name = colthis->name;
		  if (j>0) strcat(q, ",");
		  strcat(q, name);
		}
	      }

	      t = info->t;
	      if (t) {
		strcat(q, "\nFROM ");
		while (t) {
		  if (t != info->t) strcat(q, ",");
		  strcat(q, t->name);
		  t = t->next;
		}
	      }
	      
	      strcat(q, "\nWHERE ");
	      strcat(q, where);
	      FREE(where);

	      if (sort_ncols > 0) {
		int maxcols = info->maxcols + 1;
		strcat(q,"\nORDERBY ");
		for (j=0; j<sort_ncols; j++) {
		  col_t *colthis = &info->o[j];
		  char *name = colthis->name;
		  if (j>0) strcat(q, ",");
		  if (ABS(colthis->kind) > maxcols) {
		    strcat(q,"ABS(");
		    strcat(q, name);
		    strcat(q,")");
		  }
		  else {
		    strcat(q, name);
		  }
		  if (colthis->kind < 0) strcat(q," DESC");
		}
	      }
	    }

	    strcat(q, Exit);
	    strcat(q, info->sql_query);

	    FREE(info->sql_query);
	    info->sql_query = q;
	  }

	  if ((info->u && info->nuniqueby > 0) || 
	      info->has_select_distinct ||
	      info->has_aggrfuncs || 
	      (info->o && info->norderby > 0) ||
	      ((info->optflags & 0x1) == 0x1) ||
	      info->create_index > 0) {
	    info->need_global_view = true;
	  }
	  else {
	    info->need_global_view = false;
	  }

	  if ((info->u && info->nuniqueby > 0) || info->has_select_distinct) {
	    /* UNIQUEBY-clause present (or has SELECT DISTINCT) */
	    info->need_hash_lock = true;
	  }
	  else {
	    info->need_hash_lock = false;
	  }

	  if (info->has_select_distinct &&
	      !info->has_aggrfuncs &&
	      !(info->o && info->norderby > 0) &&
	      (info->create_index == 0) &&
	      (info->ncols == info->ncols_true) &&
	      (info->nwhere == 0) &&
	      (info->wherecond && strnequ(info->wherecond,"Unique(",7))) {
	    /* This must be a basic calculator (bc) mode !! */
	    info->is_bc = true; /* Implies also that just one result line is expected */
	  }
	  else {
	    info->is_bc = false;
	  }

	  info = info->next;
	} /* while (info) */

      } /* if (info_chain) */
      else {
	fprintf(stderr,
		"***Error: Cannot extract all compilation metadata. Has compilation failed ?\n");
	exit(SIGABRT);
      }

      FREE(dir);
      FREE(host);
      FREE(tstamp);
    } /* if (fp) */
  }
  DRHOOK_END(0);
  return info_chain;
}


PUBLIC void *
ODBc_next(void *Info)
{
  info_t *info = Info;
  return info ? info->next : NULL;
}


PUBLIC void
ODBc_print_info(FILE *fp, void *Info)
{
  info_t *info = Info;
  DRHOOK_START(ODBc_print_info);
  if (info && fp) { /* Print contents of the info */    
    int j;
    const char colfmt[] = "  [%d] = '%s' : fetch_name = '%s' : "
      "kind = %d : (pos,len) = (%d,%d) : ioffset = %d : table_id = %d @ %p\n";
    const char nickcolfmt[] = "  [%d] = '%s'%s%s%s: fetch_name = '%s' : "
      "kind = %d : (pos,len) = (%d,%d) : ioffset = %d : table_id = %d @ %p\n";
    fprintf(fp,"Directory = '%s'\n",info->dir ? info->dir : NIL);
    fprintf(fp,"Host = '%s'\n",info->host ? info->host : NIL);
    fprintf(fp,"Timestamp = '%s'\n",info->tstamp ? info->tstamp : NIL);
    fprintf(fp,"Database = '%s'\n",info->dbcred.dbname ? info->dbcred.dbname : NIL);
    fprintf(fp,"Npools = %d\n",info->npools);
    fprintf(fp,"Viewname = '%s'\n",info->view ? info->view : NIL);
    fprintf(fp,"The SQL-query in concern:\n%s\n",info->sql_query ? info->sql_query : NIL);
    fprintf(fp,"use_indices = %d\n",info->use_indices);
    fprintf(fp,"create_index = %d\n",info->create_index);
    if (info->create_index == 1) {
      fprintf(fp,"The query serves the CREATE UNIQUE INDEX %s\n",
	      info->view ? info->view : NIL);
    }
    else if (info->create_index == 2) {
      fprintf(fp,"The query serves the CREATE BITMAP INDEX %s\n",
	      info->view ? info->view : NIL);
    }
    else if (info->create_index < 0) {
      fprintf(fp,"The query serves the DROP INDEX %s\n",
	      info->view ? info->view : NIL);
    }
    fprintf(fp,"Has SELECT DISTINCT = %s\n", info->has_select_distinct ? "true" : "false");
    fprintf(fp,"Has SELECT COUNT(*) = %s\n", info->has_count_star ? "true" : "false");
    fprintf(fp,"Has aggregate functions = %s\n", info->has_aggrfuncs ? "true" : "false");
    fprintf(fp,"Calls thin()-function = %s\n", info->has_thin ? "true" : "false");
    fprintf(fp,"Optflags = 0x%x\n",info->optflags);
    fprintf(fp,"Maxcols = %d\n",info->maxcols);
    fprintf(fp,"No. of relevant $-variables : %d\n",info->nset);
    for (j=0; j<info->nset; j++) {
      fprintf(fp,"\t[%d] : '%s' = %.14g\n", j, info->s[j].name, info->s[j].value);
    }
    fprintf(fp,"No. of 'Hollerith'-strings : %d\n",info->ns2d);
    for (j=0; j<info->ns2d; j++) {
      fprintf(fp,"\t[%d] : '%s' = '%s' (saddr=%p)\n", 
	      j, info->s2d[j].name, info->s2d[j].value, info->s2d[j].u.saddr);
    }
    fprintf(fp,"No. of tables in FROM-statement : %d\n", info->nfrom);
    for (j=0; j<info->nfrom; j++) {
      table_t *t = &info->t[j];
      fprintf(fp,
	      "\n"
	      "\ttable_id = %d (%d) @ %p : '%s' : linkcase=%d ('%s') >\n"
	      "\t  prev=%p, next=%p, linkparent=%p, offset_parent=%p\n"
	      "\t  linkparent_id=%d ('%s'), offset_parent_id=%d ('%s')  %s\n"
	      "\t  [b,offset]=['%s','%s'],\n"
	      "\t  [e,len]=['%s','%s'] :\n"
	      "\t  wherecond_and [unless '%s'] =%s%s\n"
	      "\t  referenced in SELECT-clause ? %s\n"
	      "\t  has simple WHERE-condition ? %s\n",
	      t->table_id, j, t,
	      t->name ? t->name : NIL, t->linkcase,
	      (t->linkcase >= 1 && t->linkcase <= NLINKCASES) ? linkcase_name[t->linkcase] : NIL,
	      t->prev, t->next, t->linkparent, t->offset_parent,
	      t->linkparent_id, 
	      (t->linkparent && t->linkparent->name) ? t->linkparent->name : NIL,
	      t->offset_parent_id, 
	      (t->offset_parent && t->offset_parent->name) ? t->offset_parent->name : NIL,
	      (t->linkparent_id < 0) ? "" :
	      ((t->linkparent_id == t->offset_parent_id) ? "--> THE SAME" : "--> DIFFER"),
	      t->b ? t->b : NIL,
	      t->linkoffset ? t->linkoffset : NIL,
	      t->e ? t->e : NIL,
	      t->linklen ? t->linklen : NIL,
	      NIL, 
	      t->wherecond_and ? "\n" : "",
	      t->wherecond_and ? t->wherecond_and : NIL,
	      t->in_select_clause ? "yes" : "no",
	      t->simple_wherecond ? "yes" : "no"
	      );

      if (t->simple_wherecond && t->wl && t->nwl > 0) {
	int i, nwl = t->nwl;
	simple_where_t *wl = t->wl;
	fprintf(fp,"  %d simple WHERE-condition%s %sfollow for table '%s':\n", 
		nwl, 
		(nwl > 1) ? "s" : "",
		(nwl > 1) ? "(separated by ANDs) " : "",
		t->name);
	for (i=0; i<nwl; i++) {
	  fprintf(fp,"\t%s --> %s %s %.14g\n", 
		  wl->condstr,
		  wl->lhs, KindStr((Kind_t)wl->oper), wl->rhs);
	  wl++;
	}
      }

      if (t->stored_idx_count > 0) {
	int i, nwl = t->nwl;
	int cnt = 0;
	simple_where_t *wl = t->wl;
	fprintf(fp,"  %d stored indices were found for the following WHERE-stmts:\n",
		t->stored_idx_count);
	for (i=0; i<nwl; i++) {
	  if (wl->stored_idx) {
	    fprintf(fp,"\t(%d) : %s : index-file=\n  %s\n",
		    ++cnt,wl->condstr,
		    wl->stored_idx->filename);
	  }
	  wl++;
	}
      }
      else {
	fprintf(fp,"  0 stored indices were found for table '%s'\n",t->name);
      }
    }
    fprintf(fp,"No. of select columns : %d (total), %d (true), %d (pure)",
	    info->ncols, info->ncols_true, info->ncols_pure);
    fprintf(fp,", %d (aggr), %d (aux), %d (nonassoc)\n",
	    info->ncols_aggr_formula, info->ncols_aux, info->ncols_nonassoc);
    
    for (j=0; j<info->ncols; j++) {
      col_t *colthis = &info->c[j];
      fprintf(fp, nickcolfmt,
	      j, colthis->name, 
	      colthis->nickname ? " (as '" : "",
	      colthis->nickname ? colthis->nickname : "", 
	      colthis->nickname ? "') " : "",
	      colthis->fetch_name, colthis->kind, 
	      colthis->bitpos, colthis->bitlen, colthis->ioffset,
	      colthis->table_id, colthis->t);
    }

    fprintf(fp,"Assuming export ODB_LAT=%s\n",info->odb_lat->name);
    fprintf(fp,"         export ODB_LON=%s\n",info->odb_lon->name);
    fprintf(fp,"         export ODB_COLOR=%s\n",info->odb_color->name);
    fprintf(fp,"         export ODB_U=%s\n",info->odb_u->name);
    fprintf(fp,"         export ODB_V=%s\n",info->odb_v->name);

    fprintf(fp,"No. of UNIQUEBY columns : %d\n", info->nuniqueby);
    for (j=0; j<info->nuniqueby; j++) {
      col_t *colthis = &info->u[j];
      fprintf(fp, colfmt,
	      j, colthis->name,
	      colthis->fetch_name, colthis->kind, 
	      colthis->bitpos, colthis->bitlen, colthis->ioffset,
	      colthis->table_id, colthis->t);
    }
    if (info->wherecond) {
      fprintf(fp,"WHERE-statement follows:\n");
      fprintf(fp,"%s\n",info->wherecond);
    }
    fprintf(fp,"No. of WHERE-columns to be prefetched : %d\n",info->nwhere);
    for (j=0; j<info->nwhere; j++) {
      col_t *colthis = &info->w[j];
      fprintf(fp, colfmt,
	      j, colthis->name,
	      colthis->fetch_name, colthis->kind, 
	      colthis->bitpos, colthis->bitlen, colthis->ioffset,
	      colthis->table_id, colthis->t);
    }
    fprintf(fp,"No. of ORDERBY columns : %d\n", info->norderby);
    for (j=0; j<info->norderby; j++) {
      col_t *colthis = &info->o[j];
      fprintf(fp, colfmt,
	      j, colthis->name,
	      colthis->fetch_name, colthis->kind, 
	      colthis->bitpos, colthis->bitlen, colthis->ioffset,
	      colthis->table_id, colthis->t);
    }
    fprintf(fp,"No. of columns to be prefetched due to LINKs : %d\n",info->nprefetch);
    for (j=0; j<info->nprefetch; j++) {
      col_t *colthis = &info->p[j];
      fprintf(fp, colfmt,
	      j, colthis->name,
	      colthis->fetch_name, colthis->kind, 
	      colthis->bitpos, colthis->bitlen, colthis->ioffset,
	      colthis->table_id, colthis->t);
    }
    fprintf(fp,"No. of dynamic @LINK-lengths : %d\n", info->ndyn);
    for (j=0; j<info->ndyn; j++) {
      dyn_t *dyn = &info->dyn[j];
      fprintf(fp,"\t%s : parent = '%s' (table_id = %d), child = '%s' (table_id = %d)\n",
	      dyn->name, 
	      dyn->parent->name, dyn->parent->table_id, 
	      dyn->child->name, dyn->child->table_id);
    }
  } /* if (info && fp) */
  DRHOOK_END(0);
}


#define FREE_THIS_SET() { \
  int j; \
  for (j=0; j<info->nset; j++) { \
    set_t *s = &info->s[j]; \
    (void) delsym(s->name, it); \
    FREE(s->name); \
  } \
  FREE(info->s); \
}

#define FREE_THIS_S2D() { \
  int j; \
  for (j=0; j<info->ns2d; j++) { \
    str_t *s2d = &info->s2d[j]; \
    (void) delsym(s2d->name, it); \
    FREE(s2d->name); \
    FREE(s2d->value); \
  } \
  FREE(info->s2d); \
}

#define FREE_THIS_COL(x,num) { \
  int j; \
  if (info->x) { \
    int nx = num; \
    for (j=0; j<nx; j++) { \
      col_t *colthis = &info->x[j]; \
      (void) delsym(colthis->name, it); \
      FREE(colthis->name); \
      (void) delsym(colthis->fetch_name, it); \
      FREE(colthis->fetch_name); \
      FREE(colthis->nickname); \
      FREE(colthis->dtype); \
      if (colthis->dinp_alloc) FREE(colthis->dinp); \
      colthis->dinp = NULL; \
      colthis->dinp_len = 0; \
      colthis->dinp_alloc = false; \
      if (colthis->formula_ptree && colthis->formula_ptree_owner) DelParseTree(colthis->formula_ptree); \
      colthis->formula_ptree = NULL; \
    } \
  } \
  FREE(info->x); \
}

#define FREE_HER_TABLES(x,num) { \
  int j, nx=info->num; \
  for (j=0; j<nx; j++) { \
    table_t *this = &info->x[j]; \
    FREE(this->name); \
    FREE(this->b); \
    FREE(this->e); \
    FREE(this->linkoffset); \
    FREE(this->linklen); \
    FREE(this->offset); \
    FREE(this->len); \
    FREE(this->idx); \
    DelParseTree(this->wherecond_and_ptree); \
    this->wherecond_and_ptree = NULL; \
    FREE(this->wherecond_and); \
    if (this->wl) { \
      int i, nwl = this->nwl; \
      for (i=0; i<nwl; i++) { \
        simple_where_t *wl = &this->wl[i]; \
	FREE(wl->condstr); \
	FREE(wl->lhs); \
        wl->stored_idx = codb_IDXF_freeidx(wl->stored_idx, 0); \
      } \
      FREE(this->wl); \
    } \
  } \
  FREE(info->x); \
}

#define FREE_DYN() \
{ \
  int j, ndyn = info->ndyn; \
  for (j=0; j<ndyn; j++) { \
    dyn_t *dyn = &info->dyn[j]; \
    (void) delsym(dyn->name, it); \
    FREE(dyn->name); \
    FREE(dyn->data); \
  } \
  FREE(info->dyn); \
}

PUBLIC void *
ODBc_sql_cancel(void *Info)
{
  DRHOOK_START(ODBc_sql_cancel);
  if (Info) {
    DEF_IT;
    info_t *info = Info;
    FREE(info->dir);
    FREE(info->host);
    FREE(info->tstamp);
    FREE(info->dbcred.dbname);
    FREE(info->dbcred.srcpath);
    FREE(info->dbcred.datapath);
    FREE(info->dbcred.idxpath);
    FREE(info->dbcred.poolmask);
    FREE(info->view);
    FREE(info->sql_query);
    FREE(info->wherecond);
    DelParseTree(info->wherecond_ptree);
    info->wherecond_ptree = NULL;
    FREE_THIS_SET();
    FREE_THIS_S2D();
    FREE_THIS_COL(c,info->ncols);
    FREE_THIS_COL(u,info->nuniqueby);
    FREE_THIS_COL(w,info->nwhere);
    FREE_THIS_COL(o,ABS(info->norderby));
    FREE_THIS_COL(p,info->nprefetch);
    FREE_THIS_COL(odb_lat,1);
    FREE_THIS_COL(odb_lon,1);
    FREE_THIS_COL(odb_color,1);
    FREE_THIS_COL(odb_u,1);
    FREE_THIS_COL(odb_v,1);
    FREE_HER_TABLES(t,nfrom);
    FREE_DYN();
    FREE(info->colaux);
    FREE(info);
  }
  DRHOOK_END(0);
  return NULL;
}

#define SWAPINFO(n, old, new) tmp = old->n; old->n = new->n; new->n = tmp

PRIVATE Bool
MakePlotColumns(info_t *info_inout, Bool warrow,
		int latcol, int loncol, int colorcol, int ucol, int vcol)
{
  DRHOOK_START(MakePlotColumns);
  if (info_inout) {
    int nc = info_inout->ncols_true;
    ODB_fprintf(fp_echo,
		"MakePlotColumns(warrow=%s)< nc = %d, latcol = %d, loncol = %d, colorcol = %d"
		", ucol = %d, vcol = %d\n",
		warrow ? "true" : "false", nc, latcol, loncol, colorcol, ucol, vcol);
    if (latcol >= 0 && latcol < nc && loncol >= 0 && loncol < nc) {
      info_t *info = NULL;
      int j, k, cnt = 2; /* 2 == at least two columns i.e. latcol & loncol present */
      
      if (warrow) {
	if (ucol >= 0 && ucol < nc &&
	    vcol >= 0 && vcol < nc) cnt += 2; /* Both 3rd & 4th present */
	else {
	  ODB_fprintf(fp_echo,"MakePlotColumns: No (u,v)-available --> abandoning wind-arrow plot\n");
	  ucol = vcol = -1;
	  warrow = false;
	  ODBc_set_format("plotobs"); /* Reset back to "just" plotobs */
	}
      }
      else {
	ucol = vcol = -1;
      }

      if (colorcol >= 0 && colorcol < nc) cnt++; /* 3rd (5th for warrow) column present */

      ODB_fprintf(fp_echo,
		  "MakePlotColumns(warrow=%s)> nc = %d, latcol = %d, loncol = %d, colorcol = %d"
		  ", ucol = %d, vcol = %d\n",
		  warrow ? "true" : "false", nc, latcol, loncol, colorcol, ucol, vcol);

      CALLOC(info, 1); /* All zeroed, too */

      k = cnt; /* "cnt" saved in "k" for future use */
      for (j=0; j<info_inout->ncols; j++) {
	info->ncols_pure += (info_inout->c[j].kind == 1);
	info->ncols_formula += (info_inout->c[j].kind == 2);
	info->ncols_aggr_formula += (info_inout->c[j].kind == 4);
	info->ncols_aux += (info_inout->c[j].kind == 8);
	info->ncols_nonassoc += (info_inout->c[j].kind == 0);
	if (warrow) {
	  if (j != latcol && j != loncol && j != colorcol && j != ucol && j != vcol) cnt++;
	}
	else {
	  if (j != latcol && j != loncol && j != colorcol) cnt++;
	}
      }
      info->ncols_true = info->ncols_pure + info->ncols_formula + info->ncols_aggr_formula;

      CHECK_SIZE(info, c, ncols, cnt); /* no. of columns == cnt */

      ODB_fprintf(fp_echo,"MakePlotColumns: info->ncols = %d\n",info->ncols);
      ODB_fprintf(fp_echo,"MakePlotColumns: info->ncols_true = %d\n",info->ncols_true);
      ODB_fprintf(fp_echo,"MakePlotColumns: info->ncols_pure = %d\n",info->ncols_pure);
      ODB_fprintf(fp_echo,"MakePlotColumns: info->ncols_formula = %d\n",info->ncols_formula);
      ODB_fprintf(fp_echo,"MakePlotColumns: info->ncols_aggr_formula = %d\n",info->ncols_aggr_formula);
      ODB_fprintf(fp_echo,"MakePlotColumns: info->ncols_aux = %d\n",info->ncols_aux);
      ODB_fprintf(fp_echo,"MakePlotColumns: info->ncols_nonassoc = %d\n",info->ncols_nonassoc);

      /* 
	 Remap/copy columns so that 

	   $ODB_LAT becomes column#1
	   $ODB_LON becomes column#2

	   if warrow == true, then

	   $ODB_U becomes column#3
	   $ODB_V becomes column#4
	   $ODB_COLOR becomes column#5 (if available)

	   else

	   $ODB_COLOR becomes column#3 (if available)
	   
       */

      {
	int jj = 0;
	HARDCOPY_THESE(c,0,&info_inout->c[latcol]); ++jj;
	HARDCOPY_THESE(c,1,&info_inout->c[loncol]); ++jj;
	if (warrow) {
	  HARDCOPY_THESE(c,jj,&info_inout->c[ucol]); ++jj;
	  HARDCOPY_THESE(c,jj,&info_inout->c[vcol]); ++jj;
	}
	if (colorcol >= 0 && colorcol < nc) {
	  HARDCOPY_THESE(c,jj,&info_inout->c[colorcol]); ++jj;
	}
      }

      for (j=0; j<info_inout->ncols; j++) {
	if (j != latcol && j != loncol && j != colorcol &&
	    j != ucol && j != vcol) {
	  HARDCOPY_THESE(c,k,&info_inout->c[j]);
	  k++;
	}
      } /* for (j=0; j<info_inout->ncols; j++) */

      /* "k" should now be == "cnt" */

      if (k != cnt) {
	fprintf(stderr,
		"***Error: Programming fatality in MakePlotColumns : k != cnt (k=%d, cnt=%d)\n",
		k, cnt);
	RAISE(SIGABRT);
      }
      
      /* Swap info->c with info_inout->c */
      {
	col_t *tmp;
	SWAPINFO(c, info, info_inout);
      }

      /* redo c's dimensions and pass them to info_inout */
      {
	int tmp;
	SWAPINFO(ncols, info, info_inout);
	SWAPINFO(ncols_true, info, info_inout);
	SWAPINFO(ncols_pure, info, info_inout);
	SWAPINFO(ncols_formula, info, info_inout);
	SWAPINFO(ncols_aggr_formula, info, info_inout);
	SWAPINFO(ncols_aux, info, info_inout);
	SWAPINFO(ncols_nonassoc, info, info_inout);
      }

      /* Get rid of (temporary) info */
      (void) ODBc_sql_cancel(info);
    } /* f (latcol >= 0 && latcol < nc && loncol >= 0 && loncol < nc) */
  }
  DRHOOK_END(0);
  return warrow;
}


PRIVATE Bool
HasSimpleWHEREcond(const info_t *info, table_t *t, col_t *w, int nwhere, const set_t *set, int nset)
{
  Bool is_simple = false;
  simple_where_t *wl = t ? t->wl : NULL;
  int nwl = t ? t->nwl : 0;
  int table_id = t ? t->table_id : -1;
  char *env = getenv("ODB_SIMPLE_WHERECOND_LIMIT");
  int limit = env ? atoi(env) : (int)ABS(mdi);
  DRHOOK_START(HasSimpleWHEREcond);

  if (t && wl && nwl >= 1 && nwl <= limit &&
      w && nwhere > 0 && table_id >= 0) {
    int i, j, nhits = 0;
    const char *tblname = t->name;
    for (i=0; i<nwl; i++) {
      wl->condstr = CheckAgainstLinks(wl->condstr, info);
      {
	const char *s = wl->condstr;
	char *lhs = GetLHSwhenSimpleExpr(s, &wl->oper, &wl->rhs, set, nset); /* STRDUP'ped lhs */
	if (lhs) {
	  /* For the moment ignore all kind of '$...#'-variables and such
	     '#table'-variables that are not referring to the current table */
	  if (IS_USDHASH(lhs) || (IS_HASH(lhs) && !strequ(lhs+1, tblname))) FREE(lhs);
	  if (lhs) {
	    nhits++;
	    wl->lhs = lhs;
	  }
	}
      }
      wl++;
    } /* for (i=0; i<nwl; i++) */

    if (nhits == nwl) {
      /* Indeed a simple WHERE-condition ;
	 Now associate "lhs" with a WHERE-variable */
      nhits = 0;
      wl = t->wl;
      for (i=0; i<nwl; i++) {
	for (j=0; j<nwhere; j++) {
	  col_t *colthis = &w[j];
	  const char *s = colthis->name;
	  table_t *tc = colthis->t;
	  if (tc && tc->table_id == table_id && strequ(s, wl->lhs)) {
	    wl->wcol = colthis;
	    nhits++;
	    break; /* for (j=0; j<nwhere; j++) */
	  }
	} /* for (j=0; j<nwhere; j++) */
	wl++;
      } /* for (i=0; i<nwl; i++) */
    }

    if (nhits == nwl) is_simple = true;
  } /* if (t && wl && nwl > 0 && w && nwhere > 0 && table_id >= 0) */

  DRHOOK_END(0);
  return is_simple;
}


PUBLIC int
ODBc_fetch_indices(table_t *t, info_t *info)
{
  int cnt = 0;
  DRHOOK_START(ODBc_fetch_indices);

  if (t && info) {
    int use = (int)info->use_indices;
    char *env = getenv("ODB_USE_INDICES");

    /* 
       Normally by default use indices if ODB_USE_INDICES=1 
       Could have been overridden by info->use_indices = false,
       coming from odb98.x
     */
    
    if (use && env) use = atoi(env);
    
    if (info->create_index != 0) use = 0;

    if (use) {
      simple_where_t *wl = t ? t->wl : NULL;
      int nwl = t ? t->nwl : 0;
      
      if (wl && nwl >= 1) {
	int i;
	int handle = info->ph ? info->ph->h : -1; 
	const char *idxpath = info->ph->idxpath;
	const char *tblname = t->name;
	const char *idxname = info->use_index_name;

	/* The strategy : 
	   ============
	   For each simple WHERE-condition "wl", try to find the best 
	   possible match from among the various index-files
	   that may lie around. The match is found using the following
	   precedence (the highest first, and when matched, ignore the rest):

	   (#1) Search for indices that directly satisfy the wl-triplet 
	        by looking at the WHERE-statement itself.

	   Assuming the previous example : "press@body < 10", we look
	   for any index-files that satisfy criterias :

	   CREATE INDEX name ON body WHERE press@body < 10;

	   or

	   CREATE INDEX name on body ( some_other_column_names ) WHERE press@body < 10;

	   i.e. "ncols" is irrelevant, as is "colnames"


	   (#2) If the (#1) didn't match any index-files (indices), then
	        we can try the next candidate. 
		Look for such indices, which have wl->lhs as column name
	        and WHERE-condition "WHERE 1". Then search for those
	        index-sets only, which satisfy the wl-triplet (lhs,oper,rhs).

	   E.g. you have created an index like the following:
	     CREATE INDEX name ON body ( press );
	   which translates into :
	     CREATE INDEX name ON body ( press@body ) WHERE 1;

	   And your simple WHERE-condition is "press@body < 10", thus
	   the wl-triplet is (press@body, '<', 10).

	   So we will search for each index-set that has an occurence of
	   "press@body" that satisfies "less than ten" condition.

	   ---
	   
	   If neither of the two found any match, we abandon use of indices for
	   this wl-triplet.
	   
	   NB: It is assumed that an empty wherecond'ition is the same as 
	   always true i.e. "WHERE 1"
	*/
	   
	   
	for (i=0; i<nwl; i++) {
	  int ncols;
	  char *wherecond;
	  const char *lhs =  wl->wcol ? wl->wcol->name : NULL;
	  int oper = wl->oper;
	  double rhs = wl->rhs;
	  /* For the moment ignore all kind of '$...#'-variables and such
	     '#table'-variables that are not referring to the current table */
	  const char *colnames = NULL;

	  wl->stored_idx = NULL;

	  {
	    /* The (#1) */
	    ncols = -1;
	    wherecond = S2D_fix(info, wl->condstr);
	    colnames = NULL;
	    wl->stored_idx = codb_IDXF_fetch(handle,
					     idxpath,
					     tblname,
					     idxname,
					     ncols,
					     colnames,
					     wherecond,
					     oper,
					     NULL);
	    FREE(wherecond);
	  }

	  if (!wl->stored_idx) {
	    /* The (#2) */
	    ncols = 1;
	    wherecond = NULL;
	    if (lhs) {
	      if (IS_USDHASH(lhs) || (IS_HASH(lhs) && !strequ(lhs+1, tblname)))
		colnames = NULL;
	      else
		colnames = lhs;
	    }
	    if (colnames) {
	      wl->stored_idx = codb_IDXF_fetch(handle,
					       idxpath,
					       tblname,
					       idxname,
					       ncols,
					       colnames,
					       wherecond,
					       oper,
					       &rhs);
	    }
	  }
	  if (wl->stored_idx) cnt++;
	  wl++;
	} /* for (i=0; i<nwl; i++) */
      }
    }
  }
  DRHOOK_END(0);
  return cnt;
}



PUBLIC const char *
ODBc_get_sql_query(const void *Info)
{
  const info_t *info = Info;
  return info ? info->sql_query : NULL;
}


#ifdef STANDALONE_TEST
int main(int argc, char *argv[])
{
  int j;
  (void) ODBc_debug_fp(stdout);
  for (j=1; j<argc; j++) {
    info_t *info = ODBc_get_info(argv[j]);
    ODBc_print_info(Info, stdout);
    ODBc_unget_info(info);
  }
  return 0;
}
#endif
