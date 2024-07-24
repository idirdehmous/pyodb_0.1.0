%{
#define THIS_IS_YACC_FILE
#include "defs.h"
#include "pcma_extern.h"

#undef yywrap

#define ODBURO_EMPTY 0x1
#define ODBREADONLY  0x2
#define ODBUPDATED   0x4
#define ODBAS        0x8

#define ODB_LAT "lat@hdr"
#define ODB_LON "lon@hdr"

extern int yywrap();
extern void LEX_prev_state();
extern Boolean verbose;
extern int use_indices;
extern int reorder_tables;
extern int insert_tables;
extern int safeGuard;
extern FILE *LEX_open_include(const char *filename);
extern int ODB_maxcols();

extern Boolean readonly_mode;
extern Boolean has_count_star;
extern Boolean has_usddothash;
extern Boolean no_from_stmt;
extern int ddl_piped;
extern int LEX_convflag;
extern double LEX_unconv_value;
extern int LEX_create_index;
extern int bailout_level;
extern char *bailout_string;

PRIVATE const char YACC_any_index[] = "*";
PUBLIC char *use_index_name = NULL;

PRIVATE Boolean YACC_select_distinct = 0;

PRIVATE int tmp_view_num = 0;

PRIVATE const int YACC_MinusOne = -1;
PRIVATE const int YACC_PlusOne  =  1;
PRIVATE double YACC_IncOne = 1;
PRIVATE	double YACC_TruE = 1;
PRIVATE int YACC_distinct = 1;

PUBLIC ODB_Tree *YACC_dummy_cond = NULL;

PUBLIC char *YACC_current_dbname = NULL;
PUBLIC char *YACC_current_srcpath = NULL;
PUBLIC char *YACC_current_datapath = NULL;
PUBLIC char *YACC_current_idxpath = NULL;
PUBLIC char *YACC_current_poolmask = NULL;

PRIVATE char *YACC_join(unsigned int code, const char *s2)
{
  int len = STRLEN(s2) + 20;
  char *s = NULL;
  ALLOC(s, len);
  snprintf(s,len,"%u%s",code,s2);
  return s;
}

PRIVATE char *YACC_join_by_expr(const char *s, int odb_name,
	const ODB_Tree *exprlat, const ODB_Tree *exprlon)
{
  unsigned int code = 0;
  unsigned int j;
  for (j=1; j<=2; j++) {
    const ODB_Tree *expr = (j==1) ? exprlat : exprlon;
    if (expr) {
      const char *compare_with = (j==1) ? ODB_LAT : ODB_LON;
      int what = expr->what;
      if (what == odb_name) {
        ODB_Symbol *psym = expr->argv[0];
	char *name = psym->name;
	if (strequ(name, compare_with)) code |= j;
      }
    }
  } /* for (j=1; j<=2; j++) */
  /* The rational behind the "code"
     When a bit (1st or 2nd or both) are set, then 1st argument or 2nd argument or 
     both, respectively, will be subject to ODB_lldegrees() in ODB_inside/inpolygon/near functions.

     This complicated matter is needed to avoid applying ODB_lldegrees() twice per argument:
     once when entering, and once when in ODB_inside/inpolygon/near functions.

     After this for example the following works ok and should give the same results:
     WHERE inside("finland")
     WHERE inside("finland",lat,lon)
     WHERE inside("finland",degrees(lat),degrees(lon))
     WHERE inside("finland",degrees(lat),lon)
     WHERE inside("finland",lat,degrees(lon))

     The code (values 0,1,2 or 3) will be prepended in front of supplied string argument
     and stripped away in ODB_inside/inpolygon/near functions to extract the code.
   */
  return YACC_join(code, s);
}

/* We do not accept the following table names */

#define N_INVTABNAM  12

PRIVATE const char *invalid_table_names[N_INVTABNAM] = {
	"core", /* Unix core-file */
	        /* The following are specific to Windows/CYGWIN-based databases 
		   See more : http://support.microsoft.com/?id=74496 */
	"con",  /* Keyboard and display */
	"prn",  /* System list device, usually a parallel port */
	"aux",  /* Auxiliary device, usually a serial port */
	"nul",  /* Bit-bucket device */
	"com1", /* First serial communications port */
	"com2", /* Second serial communications port */
	"com3", /* Third serial communications port */
	"com4", /* Fourth serial communications port */
	"lpt1", /* First parallel printer port */
	"lpt2", /* Second parallel printer port */
	"lpt3"  /* Third parallel printer port */
};

#define DEF_USDHASH(s, a_value) \
 pname = s; \
 value = a_value; \
 { \
   ODB_Tree *expr = ODBOPER1(ODB_NUMBER,&value); \
   ODB_Tree *p; \
   name = ODB_new_symbol(ODB_USDNAME,pname); \
   p = ODBOPER2(ODB_SET, name, expr); \
   (void) ODB_new_cmd(p); \
 }

#define DEF_USDDOTHASH(parent_tblname, child_tblname) \
 { \
   /* Create r/o variable "$<parent_tblname>.<child_tblname>#" to \
      denote dynamic length between two tables during SQL-query. \
      After this you can do the following: \
      SELECT lat,lon,$hdr.body# AS "body.len",press,obsvalue \
      FROM hdr,body WHERE press < 100; */ \
   ODB_Symbol *name; \
   char *pname; \
   int len = STRLEN(parent_tblname) + STRLEN(child_tblname) + 4; \
   ALLOC(pname, len); \
   snprintf(pname, len, "$%s.%s#", parent_tblname, child_tblname); \
   name = ODB_new_symbol(ODB_USDNAME,pname); \
   FREE(pname); \
   { \
     double value = 0; \
     ODB_Tree *expr = ODBOPER1(ODB_NUMBER,&value); \
     ODB_Tree *ptree = ODBOPER2(ODB_SET, name, expr); \
     (void) ODB_new_cmd(ptree); \
   } \
 }

%}

%union {
  double         dval;
  int            numargs;
  int	         id;
  int	         sign;
  char	         onechar;
  char          *str;
  ODB_Symbol    *sym;
  ODB_Type      *type;
  Tabledef_t    *table;
  Viewdef_t     *view;
  Selectdef_t   *select;
  ODB_Tree      *node;
  ODB_Cmd       *cmd;
  ODB_Arridx    *arridx;
  ODB_SelAttr_t *attr;
  ODB_Match_t   *match;
};

%token <dval> ODB_NUMBER
%token <str>  ODB_NAME ODB_USDNAME ODB_HASHNAME ODB_BSNUM
%token <str>  ODB_STRING ODB_WC_STRING ODB_FILE ODB_EQNE_STRING
%token <id>   ODB_INSIDE ODB_OUTSIDE
%token <id>   ODB_INSIDE_POLYGON ODB_OUTSIDE_POLYGON

%type  <cmd>     stmt stmtlist
%type  <node>    expr cond pred where selexpr num_expr selpred formula query
%type  <node>    opt_inc opt_selinc strcmpre inside selinside near selnear strfunc1
%type  <node>	 other_siders other_near
%type  <numargs> as star arrname
%type  <numargs> decllist fromlist uniqlist sortlist arglist alignlist selarglist num_arglist
%type  <numargs> decl from uniqueby orderby arrsort opt_arglist
%type  <numargs> exprlist
%type  <numargs> opt_ontable ontable indexcols indexlist indexname
%type  <match>   matchfunc
%type  <str>     name USDname extUSDname HASHname BSnum type string opt_table_name 
%type  <str>     filename opt_idxname idxname dbcred
%type  <id>	 opt_sel_param is_null_or_is_not_null opt_as_nickname in_notin opt_using
%type  <id>	 eqne like_notlike infile_notinfile
%type  <id>      insiders outsiders
%type  <attr>    select2
%type  <type>    typedef
%type  <table>   tabledef
%type  <view>    viewdef indexdef dropindex
%type  <dval>    number const_expr
%type  <arridx>  arrindex
%type  <onechar> align
%type  <select>  select sellist selname
%type  <sign>    opt_ascdesc ascdesc

%token ODB_FUNC ODB_FUNCAGGR ODB_STRFUNC ODB_NEAR
%token ODB_STRFUNC1 ODB_QUERY
%token ODB_INCLUDE
%token ODB_SET
%token ODB_TYPE
%token ODB_TABLE
%token ODB_VIEW ODB_SELECT ODB_FROM ODB_WHERE ODB_UNIQUEBY ODB_ORDERBY
%token ODB_CREATEINDEX ODB_ON ODB_DROPINDEX
%token ODB_SEMICOLON ODB_COMMA
%token ODB_LP ODB_RP
%token ODB_LB ODB_RB
%token ODB_EXIT ODB_BETWEEN
%token ODB_WHERE_SYMBOL
%token ODB_ASC ODB_DESC ODB_ABS
%token ODB_ALIGN ODB_ONELOOPER ODB_SHAREDLINK ODB_RESET
%token ODB_LIKE ODB_NOTLIKE
%token ODB_IS ODB_NULL
%token ODB_SELECT_DISTINCT ODB_SELECT_ALL
%token ODB_AS ODB_READONLY ODB_UPDATED
%token ODB_REORDER ODB_NOREORDER
%token ODB_INDEX ODB_NOINDEX
%token ODB_SAFEGUARD ODB_NOSAFEGUARD
%token ODB_INSERT ODB_NOINSERT
%token ODB_TYPEOF
%token ODB_ARRNAME ODB_NICKNAME
%token ODB_DISTINCT
%token ODB_COND ODB_NORM
%token ODB_USING
%token ODB_DATABASE ODB_SRCPATH ODB_DATAPATH ODB_IDXPATH ODB_POOLMASK
%token ODB_MATCH

%left  ODB_OR
%left  ODB_AND
%left  ODB_IN ODB_NOTIN
%left  ODB_INFILE ODB_NOTINFILE
%left  ODB_NE ODB_EQ ODB_CMP
%left  ODB_GE ODB_LE ODB_GT ODB_LT
%left  ODB_DOTP
%left  ODB_ADD ODB_SUB
%left  ODB_STAR ODB_DIV ODB_MODULO
%left  ODB_UNARY_MINUS ODB_UNARY_PLUS ODB_NOT
%right ODB_POWER
%right ODB_QMARK ODB_COLON

%start odbsql

%%

odbsql  : { /* a hack to get "$mdi" always defined */
	    char *pname = "$mdi";
	    double value = ABS(RMDI);
	    ODB_Symbol *name = ODB_lookup(ODB_USDNAME,pname,NULL);
	    if (!name) {
	      ODB_Tree *expr = ODBOPER1(ODB_NUMBER,&value);
	      ODB_Tree *p;
	      name = ODB_new_symbol(ODB_USDNAME,pname);
	      p = ODBOPER2(ODB_SET, name, expr);
	      (void) ODB_new_cmd(p); 
	    }
	    /* For internal use only; None of the following cannot be redefined by the user */
	    /* Install '$#' which defines the current pool number in order to
	       reference it from SELECT-expressions or WHERE-stmt */
	    DEF_USDHASH("$#", 0);
	    /* Install also other $-variables that end with '#'.
	       Note also that _POOLNO i.e. '$pool#' is an alias to '$#' */
	    DEF_USDHASH(_POOLNO, 0);
	    DEF_USDHASH(_NPOOLS, 0);
	    DEF_USDHASH(_NTABLES, 0);
	    DEF_USDHASH(_UNIQNUM, 0);
	    DEF_USDHASH(_ROWNUM, 0);
	    DEF_USDHASH(_COLNUM, 0);
	    DEF_USDHASH(_NROWS, 0);
	    DEF_USDHASH(_NCOLS, 0);
	    { /* Initialize the dummy WHERE-condition */
	      if (!YACC_dummy_cond) YACC_dummy_cond = ODBOPER1(ODB_NUMBER,&YACC_TruE);
	    }	    
	} stmtlist
	;

stmtlist: stmt				{ $$ = $1; }
	| stmtlist ODB_SEMICOLON stmt	{ $$ = $1; }
	| error				{ yyclearin; yyerrok; }
	;

stmt	: /* empty */			{ $$ = NULL; }
	| ODB_INCLUDE filename {
		char *filename = STRDUP($2);
		(void) LEX_open_include(filename);

		/* Help LEX to bail out from the INCLUDE-block */
		LEX_prev_state(); 

		return 0;
	}
	| ODB_EXIT { return 1; }
	| ODB_READONLY	{ readonly_mode = 1; }
	| ODB_UPDATED	{ readonly_mode = 0; }
	| opt_using ODB_INDEX as opt_idxname { 
	  const char *idxname = $4 ? $4 : YACC_any_index;
	  use_index_name = STRDUP(idxname); 
	  use_indices = 1;
	}
	| opt_using ODB_DATABASE as dbcred {
	  char *s = $4;
	  while(isspace(*s)) ++s;
	  {
	    char *cred = STRDUP(s);
	    char *last_slash = strchr(cred, '/');
	    char *basename = last_slash ? last_slash+1 : cred;
	    char *dot = strchr(basename,'.');
	    char *dirname;
	    if (last_slash) *last_slash = '\0';
	    dirname = (STRLEN(cred) > 0) ? cred : ".";
	    FREE(YACC_current_srcpath);
	    YACC_current_srcpath = STRDUP(dirname);
	    FREE(YACC_current_datapath);
	    YACC_current_datapath = STRDUP(dirname);
	    FREE(YACC_current_dbname);
	    if (dot) *dot = '\0';
	    YACC_current_dbname = ODB_uppercase(basename);
	    FREE(cred);
	  }
	}
	| opt_using ODB_SRCPATH as dbcred {
	  char *s = $4;
	  while(isspace(*s)) ++s;
	  {
	    char *cred = STRDUP(s);
	    FREE(YACC_current_srcpath);
	    YACC_current_srcpath = cred;
	  }
	}
	| opt_using ODB_DATAPATH as dbcred {
	  char *s = $4;
	  while(isspace(*s)) ++s;
	  {
	    char *cred = STRDUP(s);
	    FREE(YACC_current_datapath);
	    YACC_current_datapath = cred;
	  }
	}
	| opt_using ODB_IDXPATH as dbcred {
	  char *s = $4;
	  while(isspace(*s)) ++s;
	  {
	    char *cred = STRDUP(s);
	    FREE(YACC_current_idxpath);
	    YACC_current_idxpath = cred;
	  }
	}
	| opt_using ODB_POOLMASK as dbcred {
	  char *s = $4;
	  while(isspace(*s)) ++s;
	  {
	    char *cred = STRDUP(s);
	    FREE(YACC_current_poolmask);
	    YACC_current_poolmask = cred;
	  }
	}
	| ODB_NOINDEX	{ use_indices = 0; FREE(use_index_name); }
	| ODB_REORDER	{ reorder_tables = 1; }
	| ODB_NOREORDER	{ reorder_tables = 0; }
	| ODB_INSERT	{ insert_tables = 1; }
	| ODB_NOINSERT	{ insert_tables = 0; }
	| ODB_SAFEGUARD	  { safeGuard = 1; }
	| ODB_NOSAFEGUARD { safeGuard = 0; }
	| ODB_SET extUSDname ODB_EQ expr {
		char *pname = $2;
		ODB_Symbol *name;
		/* Prevent user from redefining the '$#' variable */
		if (strequ(pname,"$#")) {
		  SETMSG0("Pool number ($#-variable) cannot be re-defined by the user");
		  YYerror(msg);
		}
		/* Prevent redefinition of *ANY* variables starting with '$' and
		   ending with '#'. These are for internal use only */
		if (IS_USDHASH(pname)) {
		  SETMSG1("Illegal '%s': it is forbidden to redefine $-variables ending with '#'",pname);
		  YYerror(msg);
		}
		name = ODB_lookup(ODB_USDNAME,pname,NULL);
		if (!name) {
		  ODB_Tree *expr = $4;
		  ODB_Tree *p;

		  name = ODB_new_symbol(ODB_USDNAME,pname);
		  p = ODBOPER2(ODB_SET, name, expr);

		  $$ = ODB_new_cmd(p); 
		}
		else {
		  double oldvalue = name->dval;
		  double newvalue = 0;
		  if (!(ODB_evaluate($4,&newvalue) && newvalue == oldvalue)) {
		    SETMSG2("Variable '%s' was already set ; previous value (%.20g) retained\n",
		           pname, oldvalue);
		    YYwarn(0,msg);
		  }
		}
	}
	| ODB_RESET align {
		extern char *one_tables;
		char c = $2;
		if (one_tables) {
		  char *p = one_tables;
		  while (*p) { if (*p == c) *p = '#'; p++; }
		}
		if (c == '=') {
		  extern Boolean reset_align_issued;
		  reset_align_issued = 1;
		}
		else if (c == '@') {
		  extern Boolean reset_onelooper_issued;
		  reset_onelooper_issued = 1;
		}
	}
	| align ODB_LP alignlist ODB_RP {
		char key = $1;
		int nsym = $3;
		extern char *one_tables;
		extern int optlevel;
		extern Boolean ODB_tables_done; /* defined in tree.c */
		if (ODB_tables_done) {
		  SETMSG0("ALIGN/ONELOOPER/SHAREDLINK statements must be situated in data definition layout (ddl)\n");
		  YYerror(msg);
		}
		else if (key == '&') { /* Shareable link; regardless of optimization level */
		  char c = key;
		  int j;
		  char *master;
		  char *tmp_one_tables = NULL;
		  char **list;
  		  ALLOC(list, nsym);
		  for (j=nsym-1; j>=0; j--) {
		    list[j] = ODB_popstr();
		  }
		  master = STRDUP(list[0]);
		  if (one_tables) {
		    int len;
		    tmp_one_tables = STRDUP(one_tables+1);
		    len = strlen(tmp_one_tables);
		    tmp_one_tables[len-1] = '\0';
		    FREE(one_tables);
		  }
		  init_list(tmp_one_tables);
		  for (j=1; j<nsym; j++) {
		    int nelem, len_all;
		    char *all;
		    len_all = strlen(master) + 1 + strlen(list[j]) + 1;
		    ALLOC(all, len_all);
		    sprintf(all,"%s%c%s",master,c,list[j]);
		    nelem = ODB_addrel(all,c);
		    FREE(all);
		  }
		  {
		    char *plist = add_list(NULL);
		    if (plist) one_tables = STRDUP(plist);
		  }
		  destroy_list();
		  FREE(list);
		  FREE(master);
		}
		else if (nsym >= 2 && optlevel >=3) {
		  char c = key;
		  int j, nelem;
		  char *master;
		  char *tmp_one_tables = NULL;
		  char **list;
		  int len_all;
		  char *all;
  		  ALLOC(list, nsym);
		  for (j=nsym-1; j>=0; j--) {
		    list[j] = ODB_popstr();
		  }
		  master = STRDUP(list[0]);
		  if (one_tables) {
		    int len;
		    tmp_one_tables = STRDUP(one_tables+1);
		    len = strlen(tmp_one_tables);
		    tmp_one_tables[len-1] = '\0';
		    FREE(one_tables);
		  }
		  init_list(tmp_one_tables);
		  len_all = strlen(master) + 1 + 1;
		  for (j=1; j<nsym; j++) {
		    len_all += strlen(list[j]) + 1;
		  }
		  len_all += 1;
		  ALLOC(all, len_all);
		  sprintf(all,"%s%c(",master,c);
		  for (j=1; j<nsym; j++) {
		   strcat(all,list[j]);
		   strcat(all,(j<nsym-1) ? "," : ")");
		  }
		  nelem = ODB_addrel(all,c);
		  {
		    char *plist = add_list(NULL);
		    if (plist) one_tables = STRDUP(plist);
		  }
		  destroy_list();
		  FREE(list);
		  FREE(master);
		  FREE(all);
		}
	} 
	| typedef ODB_LP decllist ODB_RP {
		int nsym = $3;
		ODB_Type *type = $1;
		ODB_Tree *p;
		p = ODBOPER2(ODB_TYPE, type, &nsym);
		$$ = ODB_new_cmd(p); 
	}
	| typedef name {
		int nsym = 1;
		ODB_Type *type = $1;
		char *empty_member = STRDUP(EMPTY_MEMBER);
		ODB_Tree *p;

		(void) ODB_new_type($2, 1);
		(void) ODB_new_symbol(ODB_NAME, empty_member);

		ODB_pushstr($2);         /* type string to stack */
		ODB_pushstr(empty_member);  /* member name string to stack */

		p = ODBOPER2(ODB_TYPE, type, &nsym);

		$$ = ODB_new_cmd(p); 
	}
	| tabledef ODB_LP decllist ODB_RP { 
	  extern Boolean ODB_in_tabledef;
	  ODB_Cmd *pcmd = NULL;
	  Tabledef_t *list = $1;
	  int nt = list->num_tables, jlist = 0;
	  {
		int j, k, nsym = $3, ksym, nsym_actual = 0;
		ODB_Table *table = list->tables[jlist];
		char *tname = table ? table->table->name : NIL;
		ODB_Tree *p;
		Boolean dupl_exist;
		ODB_Symbol **psym;
		ODB_Type **ptype;
		int *linkmode;
		int nlink = 0;

		ALLOC(ptype, 2 * nsym);
		ALLOC(psym, 2 * nsym);
		ALLOC(linkmode, 2 * nsym);

		ksym = 2*nsym - 1;
		for (j=nsym-1; j>=0; j--) {
		  char *s;
		  ODB_Type *type;
		  char *typename;

		  s = ODB_popstr(); /* name string from stack */
		  psym[ksym]  = ODB_lookup(ODB_NAME, s, NULL);

		  s = ODB_popstr(); /* type string from stack */
		  ptype[ksym] = ODB_lookup_type(s, NULL);

		  type = ptype[ksym];
		  typename = type->type->name;

		  if (*typename == '@') { 
		    /* We have a link; adjust entries; note reverse order */
		    char *len, *offset;

		    linkmode[ksym  ] = 2; /* Length */
		    linkmode[ksym-1] = 1; /* Offset */
		    nlink++;

		    ALLOC(len, strlen(psym[ksym]->name) + 30);
		    sprintf(len, "LINKLEN(%s)",psym[ksym]->name);

		    ALLOC(offset, strlen(psym[ksym]->name) + 30);
		    sprintf(offset, "LINKOFFSET(%s)",psym[ksym]->name);

		    psym[ksym] = ODB_new_symbol(ODB_NAME, len);
		    psym[ksym-1] = ODB_new_symbol(ODB_NAME, offset);

		    FREE(offset);
		    FREE(len);

		    ptype[ksym] = ODB_new_type(LINKLENTYPE, 1);
		    ptype[ksym-1] = ODB_new_type(LINKOFFSETTYPE, 1);
		    nsym_actual += 2;
		  }
		  else {
		    linkmode[ksym] = linkmode[ksym-1] = 0; /* Not a link */
		    psym[ksym-1]  = NULL;
		    ptype[ksym-1] = NULL;
		    nsym_actual++;
		  }
		  ksym -= 2;
		}

		table->nsym = nsym_actual;
		ALLOC(table->type, nsym_actual);
		ALLOC(table->sym, nsym_actual);
		ALLOC(table->linkmode, nsym_actual);
		
		ksym = 2 * nsym;
		j = 0;
		for (k=0; k<ksym; k++) {
		  if (ptype[k] && psym[k]) {
		    table->type[j] = ptype[k];
		    table->sym[j] = psym[k];
		    table->linkmode[j] = linkmode[k];
		    j++;
		  }
		  k++;
		  if (ptype[k] && psym[k]) {
		    table->type[j] = ptype[k];
		    table->sym[j] = psym[k];
		    table->linkmode[j] = linkmode[k];
		    j++;
		  }
		}

		nsym = nsym_actual;
		
		table->nlink = nlink;
		ALLOC(table->link, nlink);
		ALLOC(table->linkname, nlink);
		ALLOC(table->sharedlink, nlink);
		ALLOC(table->sharedlinkname, nlink);

		if (nlink > 0) {
		  Boolean *sharedlink_mask;
		  int num_sharedlink = 0;
		  CALLOC(sharedlink_mask, nsym);
		  k = 0;
		  for (j=0; j<nsym; j++) {
		    if (table->linkmode[j] == 1) { /* LINK-offset */
		      extern char *one_tables;
		      char *p = ODB_extract(table->sym[j]->name,'(',')');
		      ODB_Table *link_table = ODB_lookup_table(p,NULL);
		      table->link[k] = link_table; /* Can still point to a NULL */
		      table->linkname[k] = p; /* ... just in case the link[k] was a NULL */
		      table->sharedlink[k] = NULL; /* ... for the moment; may change */
		      table->sharedlinkname[k] = ODB_get_sharedlinkname(p, one_tables);
		      if (table->sharedlinkname[k]) {
			table->sharedlinkname[k] = STRDUP(table->sharedlinkname[k]);
		        num_sharedlink++;
			sharedlink_mask[j]   = 1; /* link-offset */
			sharedlink_mask[j+1] = 1; /* link-length */
		      }
		      k++;
		      DEF_USDDOTHASH(tname, p);
		    } /* if (table->linkmode[j] == 1) */
		  }
		  if (num_sharedlink > 0) {
		    /* Remove shared link from symbol list of this table */
		    ODB_Type **shl_type;
		    ODB_Symbol **shl_sym;
		    int *shl_linkmode;
	            nsym_actual = nsym - 2 * num_sharedlink; /* "2" is for offset & length */
		    ALLOC(shl_type, nsym_actual);
		    ALLOC(shl_sym, nsym_actual);
		    ALLOC(shl_linkmode, nsym_actual);
		    k = 0;
		    for (j=0; j<nsym; j++) {
			if (sharedlink_mask[j]) continue;
			shl_type[k] = table->type[j];
			shl_sym[k] = table->sym[j];
			shl_linkmode[k] = table->linkmode[j];
			k++;
		    }
		    FREE(table->type);     table->type = shl_type;
		    FREE(table->sym);      table->sym = shl_sym;
		    FREE(table->linkmode); table->linkmode = shl_linkmode;
		    table->nsym = nsym = nsym_actual; /* no. of symbols reset again */
		    table->any_sharedlinks = 1;
		  }
		  FREE(sharedlink_mask);
		} /* if (nlink > 0) */

		FREE(linkmode);

		dupl_exist = ODB_dupl_symbols(table->sym, nsym, 1);

		if (dupl_exist) {
		  SETMSG1("Duplicate variables encountered in the table '%s'\n", tname);
		  YYerror(msg);
		}

		ALLOC(table->expname, nsym);
		for (j=0; j<nsym; j++) {
		  char *s = NULL;
		  int slen = 3;
		  Boolean bitstream = table->type[j]->bitstream;
		  slen += strlen(bitstream ? BITFIELD : table->type[j]->type->name);
		  slen += strlen(table->sym[j]->name);
		  slen += strlen(table->table->name);
		  ALLOC(s, slen);
		  sprintf(s,"%s:%s@%s",
			bitstream ? BITFIELD : table->type[j]->type->name,
			table->sym[j]->name,
			table->table->name);
		  table->expname[j] = s;
		}

		p = ODBOPER1(ODB_TABLE, table);

		pcmd = ODB_new_cmd(p);
	  }
	  if (nt > 1) {
	    for (jlist=1; jlist<nt; jlist++) {
		ODB_Tree *p;
		ODB_Table *table0 = list->tables[0];
		ODB_Table *table = list->tables[jlist];
		char *tname = table ? table->table->name : NIL;
		table = list->tables[jlist] = ODB_copy_table(table0, tname);
		p = ODBOPER1(ODB_TABLE, table);
		pcmd = ODB_new_cmd(p);
	    }
	  }
	  $$ = pcmd;
	  ODB_in_tabledef = 0;
	}
	| dropindex opt_ontable {
	  ODB_Cmd *pcmd = NULL;
	  Viewdef_t *list = $1;
	  int nt = list->num_views, jlist = 0;
	  ODB_link_massage();
	  if (nt > 1) {
	    ODB_View *view = list->views[0];
	    char *vname = STRDUP(view->view->name);
	    char *vp = strrchr(vname,'_');
	    if (!vp) vp = vname; else *vp = '\0';
	    SETMSG1("The array syntax '%s[<low>:<high>]' in name definition not supported with CREATE INDEX",vp);
	    YYerror(msg);
	  }
	  {
	    int j;
	    int error = 0;
	    int nfrom = $2;
	    int nselect = 0;
	    ODB_View *view = list->views[jlist];
	    char *vname = view->view->name;
	    view->cond = NULL;
	    view->create_index = LEX_create_index;

	    if (nfrom > 0) {
	      view->no_from_stmt = no_from_stmt = 0;
	      view->nfrom = nfrom;
	      ALLOC(view->from, nfrom);
	      ALLOC(view->from_attr, nfrom);
	      for (j=nfrom-1; j>=0; j--) {
		int flag = ODB_popi(); /* from-attribute */
		char *s = ODB_popstr(); /* a character string from the stack */
		ODB_Table *ptable = ODB_lookup_table(s, NULL);
		if (!ptable) {
		  SETMSG4("Undefined table '%s' in DROP INDEX %s ON %s (from_attr=0x%x)\n",
			  s,vname,s,flag);
		  error++;
		  YYwarn(error,msg);
		}
		view->from_attr[j] = flag;
		view->from[j] = ptable;
	      }

	      ALLOC(view->active, 1);
	      view->active[0] = 1;
	    }

	    if (error) { 
	      SETMSG1("Syntax error in DROP INDEX '%s' [ON table(s)] definition",vname);
	      YYerror(msg);
	    }

	    {
	      ODB_Tree *p = ODBOPER1(ODB_VIEW, view);
	      pcmd = ODB_new_cmd(p);
	    }
	  }
	  $$ = pcmd;
	  LEX_create_index = 0;
	}
	| indexdef ontable indexcols where {
	  ODB_Cmd *pcmd = NULL;
	  Viewdef_t *list = $1;
	  int nt = list->num_views, jlist = 0;
	  int nfrom = $2; /* must be 1 */
	  ODB_link_massage();
	  if (nfrom != 1) {
	    SETMSG0("Currently exactly one 'tablename' needed in 'CREATE INDEX name ON tablename' ...");
	    YYerror(msg);
	  }
	  if (nt > 1) {
	    ODB_View *view = list->views[0];
	    char *vname = STRDUP(view->view->name);
	    char *vp = strrchr(vname,'_');
	    if (!vp) vp = vname; else *vp = '\0';
	    SETMSG1("The array syntax '%s[<low>:<high>]' in name definition not supported with CREATE INDEX",vp);
	    YYerror(msg);
	  }
	  {
	    int j;
	    int nselect = $3; /* ncols (could be == 0) */ 
	    ODB_Tree *cond = $4;
	    int nwhere = ODB_trace_symbols(cond, NULL, 0);
	    ODB_View *view = list->views[jlist];
	    char *vname = view->view->name;
	    int error = 0;
	    Boolean relate_where  = (nwhere > 0);
	    Boolean relate_select = (nselect > 0);
	    ODB_Symbol **pwhere= NULL;
	    ODB_Symbol **pselect=NULL;

	    /* Reverse processing order : WHERE, COLUMNS, ON TABLE */

	    /* === WHERE === */
	    ALLOC(pwhere, nwhere);
	    for (j=nwhere-1; j>=0; j--) {
	      char *s = ODB_popstr(); /* a character string from the stack */
	      /* The following symbol may still have "@"'s or "."'s [if ODB_NAME] */
	      int what = IS_HASH(s) ? ODB_HASHNAME : 
		  (IS_DOLLAR(s) ? ODB_USDNAME : (IS_BSNUM(s) ? ODB_BSNUM : ODB_NAME));
	      pwhere[j] = ODB_new_symbol(what, s);
	    }

	    /* "select" COLUMNS */
	    ALLOC(pselect, nselect);
	    view->nselect = nselect;
	    view->nselect_all = nselect;
	    view->all_readonly = 1;
	    ALLOC(view->readonly, nselect);
	    CALLOC(view->is_formula, nselect);
	    for (j=nselect-1; j>=0; j--) {
	      char *s = ODB_popstr(); /* a character string from the stack */
	      pselect[j] = ODB_new_symbol(ODB_NAME, s);
	      view->readonly[j] = 1;
	    }
	    view->select = pselect;

	    /* "from" TABLE */

	    if (nfrom != 1) error++;

	    view->no_from_stmt = no_from_stmt = 0;
	    view->nfrom = nfrom;
	    ALLOC(view->from, nfrom);
	    ALLOC(view->from_attr, nfrom);

	    for (j=nfrom-1; j>=0; j--) {
	      int flag = ODB_popi(); /* from-attribute */
	      char *s = ODB_popstr(); /* a character string from the stack */
	      ODB_Table *ptable = ODB_lookup_table(s, NULL);
	      if (!ptable) {
		SETMSG3("Undefined table '%s' in CREATE INDEX '%s' (from_attr=0x%x)\n",s,vname,flag);
		error++;
		YYwarn(error,msg);
	      }
	      view->from_attr[j] = flag;
	      view->from[j] = ptable;
	    }

	    ALLOC(view->active, 1);
	    view->active[0] = 1;

	    /* === Check SELECT relations === */
	    
	    if (!error && relate_select) {
	      int count = 0;
	      SymbolParam_t *inout;
	      
	      /*** SELECT ***/
	      
	      ALLOC(inout, 1);
	      inout->nsym = nselect;
	      inout->sym = pselect;
	      inout->readonly = view->readonly;
	      inout->sel = NULL;

	      (void)ODB_resolve_relations("SELECT", ODB_BAILOUT_SELECT, inout, 
      		       vname, view->from, view->from_attr, nfrom, pselect, NULL, nselect, 
					  0, 0,
					  &count, NULL);

	      FREE(pselect);

	      relate_select = 0;

	      /* === Remove duplicates (if any) === */

	      nselect = ODB_remove_duplicates(inout, 
					      NULL,
					      NULL,
					      NULL,
					      NULL,
					      NULL,
					      NULL,
					      NULL,
					      NULL,
					      NULL);
	      
	      view->nselect_all = nselect;
	      view->nselect = nselect;
	      view->select = pselect = inout->sym;
	      view->readonly = inout->readonly;

	      if (nselect > MAXBITS) {
		SETMSG1("Only up to %d columns allowed in CREATE INDEX ... ON TABLE ( columns )",MAXBITS);
		error++;
		YYwarn(error,msg);
	      }

	      FREE(inout);
	    }
	    
	    if (!error && relate_where) {
	      /*** WHERE ***/
	      int count = 0;

	      view->where = 
		ODB_resolve_relations("WHERE", ODB_BAILOUT_WHERE, NULL, 
		   vname, view->from, view->from_attr, nfrom, pwhere, NULL, nwhere, 
				      1, 1,
				      &count, NULL);
	      view->nwhere = nwhere = count;
	      relate_where = 0;
	    }

	    if (!error && !cond && nselect == 0) {
	      /* Make sure the user DOES supply columns and/or WHERE-condition,
		 but does not leave out BOTH !! */
	      SETMSG0("Either column(s) or WHERE-condition must be present in CREATE INDEX ... ON TABLE ( columns ) WHERE cond");
	      error++;
	      YYwarn(error,msg);
	    }

	    if (error) { 
	      SETMSG1("Syntax error in CREATE INDEX '%s' definition",vname);
	      YYerror(msg);
	    }

	    /* The rest */

	    view->active[0] = 1;
	    view->cond = cond;

	    view->create_index = LEX_create_index;

	    {
	      ODB_Tree *p = ODBOPER1(ODB_VIEW, view);
	      /* Setup WHERE-symbols and check string comparisons in WHERE */
	      ODB_setup_where(view);
	      pcmd = ODB_new_cmd(p);
	    }
	  }
	  $$ = pcmd;
	  LEX_create_index = 0;
	}
	| viewdef select uniqueby from where orderby {
	  /* New: if orderby is present, then push its symbols to select-list, too,
	          in case they were missing => the following becomes possible:

		  select a,b,c from t orderby d

		  as it is in fact the same as

		  select a,b,c,d from t orderby d !! 

		  Note: This technique doesn't work if a sorting column number is given, i.e.
		  the following doesn't make sense:

		  select a,b,c, from t orderby 4

		  But the following is still legal:

		  select a,b,c from t orderby 3

		  since column#3 i.e. c is already present in select
		  */
	  ODB_Cmd *pcmd = NULL;
	  Viewdef_t *list = $1;
	  int nt = list->num_views, jlist = 0;
	  ODB_link_massage();
	  if (nt > 1) {
	    ODB_View *view = list->views[0];
	    char *vname = STRDUP(view->view->name);
	    char *vp = strrchr(vname,'_');
	    if (!vp) vp = vname; else *vp = '\0';
	    SETMSG1("The array syntax '%s[<low>:<high>]' in name definition not supported with CREATE VIEW",vp);
	    YYerror(msg);
	  }
	  {
		int j;
		Selectdef_t *sdef = $2;
		int nselect = sdef->nselect;
		Boolean *readonly = sdef->readonly;
		int nfrom = $4;
		int norderby = $6;
		int nuniqueby = $3;
		ODB_Tree *cond = $5;
		int nwhere = ODB_trace_symbols(cond, NULL, 0);
		int iwhere = nwhere;
		ODB_View *view = list->views[jlist];
		char *vname = view->view->name;
		ODB_Tree *p;
		int error = 0;
		Boolean relate_where  = (nwhere > 0);
		Boolean relate_orderby = (norderby > 0 || norderby == -1);
		Boolean relate_select = 1;
		Boolean has_wildcard = 0;
		Boolean has_regex = 0;
		Boolean has_number_orderby = 0;
		Boolean has_number_uniqueby = 0;
		int *sign = NULL;
		ODB_Symbol **pwhere= NULL;
		ODB_Symbol **porderby=NULL;
		ODB_Symbol **puniqueby=NULL;
		ODB_Symbol **pselect=NULL;
		Boolean *p_readonly=NULL;
		int more_from_orderby = 0;
		char **from_orderby = NULL;
		ODB_SelectExpr **p_sel = NULL;
		Boolean maybe_sd = 0; /* maybe a SELECT DISTINCT ? */

		/* Note the reverse(-ish) processing order :
		   {WHERE,ORDERBY,FROM,UNIQUEBY,SELECT} */

		/* === WHERE === */

		ALLOC(pwhere, nwhere);

		for (j=nwhere-1; j>=0; j--) {
		  char *s = ODB_popstr(); /* a character string from the stack */
		  /* The following symbol may still have "@"'s or "."'s [if ODB_NAME] */
		  int what = IS_HASH(s) ? ODB_HASHNAME : 
		      (IS_DOLLAR(s) ? ODB_USDNAME : (IS_BSNUM(s) ? ODB_BSNUM : ODB_NAME));
		  pwhere[j] = ODB_new_symbol(what, s);
		}

		/* === ORDERBY === */

		if (norderby > 0) {
		  ALLOC(from_orderby, norderby);
		  ALLOC(porderby, norderby);
		  ALLOC(sign, norderby);
		}
		more_from_orderby = 0;

		for (j=norderby-1; j>=0; j--) {
		  char *s = ODB_popstr();     /* a character string from the stack */
		  int  xsign = ODB_popi(); /* the sign : +1 or -1 or 9999 (=ABS) */
		  sign[j] = xsign;
		  /* The following symbol may still have "@"'s or "."'s */
		  porderby[j] = ODB_new_symbol(ODB_NAME, s);
		  if (ODB_is_integer(s)) {
		    has_number_orderby = 1;
		  }
		  else {
		    from_orderby[more_from_orderby++] = s;
		  }
		}
		
		/*
		fprintf(stderr,"more_from_orderby is now = %d\n",more_from_orderby);
		fprintf(stderr,"norderby = %d\n",norderby);
		*/

		/* === FROM === */

		view->nfrom = nfrom;
		ALLOC(view->from_attr, nfrom);
		ALLOC(view->from, nfrom);

		{
		  int new_nfrom = 0;
		  for (j=nfrom-1; j>=0; j--) {
		    int flag = ODB_popi(); /* from-attribute */
		    char *s = ODB_popstr(); /* a character string from the stack */
		    ODB_Table *ptable = ODB_lookup_table(s, NULL);
		    if (!ptable) {
		      SETMSG3("Undefined table '%s' in view '%s' (from_attr=0x%x)\n",s,vname,flag);
		      if (bailout_level == 0 || 
			  ( (bailout_level & ODB_BAILOUT_FROM) != ODB_BAILOUT_FROM) ) error++;
		      YYwarn(error,msg);
		    }
		    else {
		      new_nfrom++;
		    }
		    view->from_attr[j] = flag;
		    view->from[j] = ptable;
		  } /* for (j=nfrom-1; j>=0; j--) */
		  
		  if (!error && new_nfrom < nfrom) {
		    /* We are here due to bailout_level NOT being == 0 (which is the default) */
		    ODB_Table **from;
		    int *from_attr;
		    int jj = 0;
		    ALLOC(from, new_nfrom);
		    ALLOC(from_attr, new_nfrom);
		    for (j=0; j<nfrom; j++) {
		      int flag = view->from_attr[j];
		      ODB_Table *ptable = view->from[j];
		      if (ptable) {
			jj++;
			from_attr[jj] = flag;
			from[jj] = ptable;
		      }
		    } /* for (j=0; j<nfrom; j++) */
		    FREE(view->from);
		    FREE(view->from_attr);
		    view->from = from;
		    view->from_attr = from_attr;
		    view->nfrom = nfrom = new_nfrom;
		    if (nfrom == 0) {
		      /* 
			 Could this be a problem ?
			 FROM bad_table1,bad_table2 gets converted into FROM * ???
		      */
		      no_from_stmt = 1;
		    }
		  }
		}

		view->no_from_stmt = no_from_stmt;

		if (!YACC_select_distinct &&
		    no_from_stmt &&
		    norderby == 0 && 
		    nselect > 0 &&
		    nuniqueby == 0) maybe_sd = 1;

		/* == Remove duplicate FROM-tables == */

		if (!no_from_stmt) {
		  nfrom = ODB_RemoveDuplicateTables(view);
		}

		/* == Insert missing FROM-tables == */

		if (view->insert_tables) { 
		  /* 'INSERT' keyword is used (is the default [see also env ODB_INSERT_TABLES]) */
		  nfrom = ODB_insert_tables(view);
		}
		else {
		  /* don't perform insertion */
		  if (verbose) 
			fprintf(stderr,
				"yacc: Insertion of missing tables to FROM-stmt bypassed in view '%s'\n",vname);
		} /* if (view->insert_tables) */

		/* == Reorder FROM-tables == */
		
		if (view->reorder_tables) { 
		  /* 'REORDER' keyword used/active (the default [see also env ODB_REORDER_TABLES]) */
		  (void) ODB_reorder_tables(view);
		}
		else {
		  /* don't perform reordering */
		  if (verbose) 
			fprintf(stderr,
				"yacc: Reordering of FROM-tables bypassed in view '%s'\n",vname);
		} /* if (view->reorder_tables) */

		nfrom = view->nfrom;

		/* === UNIQUEBY === */

		if (nuniqueby != 0 && YACC_select_distinct) {
		  SETMSG3("The keywords in '%s' and '%s' have to be mutually exclusive in view '%s'\n",
			  "SELECT UNIQUE", "UNIQUE BY", vname);
		  error++;
		  YYwarn(error,msg);
		}

		if (!YACC_select_distinct) {
		  if (nuniqueby < 0) {
		    nuniqueby = ODB_pick_symbols(view->from, view->from_attr, nfrom);
		  }
		  ALLOC(puniqueby, nuniqueby);
		  for (j=nuniqueby-1; j>=0; j--) {
		    char *s = ODB_popstr(); /* a character string from the stack */
		    /* The following symbol may still have "@"'s or "."'s */
		    puniqueby[j] = ODB_new_symbol(ODB_NAME, s); 
		    has_number_uniqueby |= ODB_is_integer(s);
		  }
		}

		/* === SELECT === */

		if (nselect < 0) { /* Means : 'SELECT *' */
		  int j;
		  Boolean readonly_flag = sdef->readonly[0];
		  sdef->nselect = nselect = 
		    ODB_pick_symbols(view->from, view->from_attr, nfrom) + more_from_orderby;
		  ALLOC(sdef->select_items, nselect);
		  FREE(sdef->readonly);
		  ALLOC(sdef->readonly, nselect);
		  for (j=more_from_orderby-1; j>=0 ; j--) {
		    char *s = from_orderby[j];
		    ODB_pushstr(s);
		  }
		  for (j=nselect-1; j>=0; j--) {
		    char *s = ODB_popstr();
		    sdef->select_items[j] = s;
		    sdef->readonly[j] = readonly_flag;
		  }
		  readonly = sdef->readonly;
		  if (more_from_orderby == 0) {
		    relate_select = 0; /* Symbol relations to the tables are now fully resolved */
		  }
		}
		else if (more_from_orderby > 0) {
		  /* expand "sdef" with items from orderby */
		  int j, jj;
		  Selectdef_t *new_sdef = NULL;
		  CALLOC(new_sdef,1);
		  /* fprintf(stderr,"<begin>nselect=%d\n",nselect); */
		  nselect += more_from_orderby;
		  /* fprintf(stderr,"<end>nselect=%d\n",nselect); */
		  new_sdef->nselect = nselect;
		  ALLOC(new_sdef->select_items, nselect);
		  ALLOC(new_sdef->readonly, nselect);
		  CALLOC(new_sdef->sel, nselect);
		  new_sdef->next = NULL;
		  new_sdef->nselect = nselect;
		  for (j=0; j<sdef->nselect; j++) {
		    new_sdef->select_items[j] = sdef->select_items[j];
		    new_sdef->readonly[j] = sdef->readonly[j];
		    if (sdef->sel) new_sdef->sel[j] = sdef->sel[j];
		  }
		  jj = more_from_orderby;
		  for (j=sdef->nselect; j<new_sdef->nselect; j++) {
		    new_sdef->select_items[j] = from_orderby[--jj];
		    new_sdef->readonly[j] = readonly_mode;
		  }
		  readonly = new_sdef->readonly;
		  FREE(sdef->select_items);
		  FREE(sdef->readonly);
		  FREE(sdef->sel);
		  FREE(sdef);
		  sdef = new_sdef;
		}

		FREE(from_orderby);

		ALLOC(pselect, nselect);
		ALLOC(p_readonly, nselect);
		CALLOC(p_sel, nselect);

		for (j=0; j<nselect; j++) {
		  char *s = STRDUP(sdef->select_items[j]);
		  int slen = strlen(s);
		  /* The following symbol may still have "@"'s or "."'s */
		  pselect[j] = ODB_new_symbol(ODB_NAME, s); 
		  p_readonly[j] = readonly[j];
		  if (sdef->sel) p_sel[j] = &sdef->sel[j];
		  if (IS_REGEX(s, slen)) {
			has_regex = 1;
		  }
		  else if (IS_WILDCARD(s)) {
			has_wildcard = 1;
		  }
		  /*
		  fprintf(stderr,
		             "$#@ sdef->select_items[%d] = '%s' (readonly=%d, wildcard=%d/%d, regex=%d/%d)\n",
			     j,s,p_readonly[j],IS_WILDCARD(s),has_wildcard,IS_REGEX(s, slen),has_regex); */
		}

		/* Expand partial wildcards ('*@table' or 'var.*@table') in SELECT */

		if (has_regex || has_wildcard) {
		  int errcnt = 0;
		  int i, count = 0;
		  int *p_count;
		  CALLOC(p_count, nselect);
		  for (j=0; j<nselect; j++) {
		    ODB_Symbol *psym = pselect[j];
		    char *s = psym->name;
		    int slen = strlen(s);
		    if (IS_REGEX(s, slen)) {
		      /* fprintf(stderr,"$$$ IS_REGEX: pselect[%d]->name = '%s'\n",j,s); */
		      p_count[j] = ODB_regex(s, view->from, view->from_attr, nfrom);
		      if (p_count[j] <= 0) {
		        SETMSG2("Regular expression '%s' in SELECT-clause (column#%d) did not match any columns",
			s,j+1);
			++errcnt;
			YYwarn(errcnt,msg); /* Do not abort -- yet */
		      }
		    }
		    else if (IS_WILDCARD(s)) {
		      /* fprintf(stderr,"$$$ IS_WILDCARD: pselect[%d]->name = '%s'\n",j,s); */
		      s = ODB_lowercase(psym->name);
		      /* ODB_wildcard will fail inside if no matches found */
		      p_count[j] = ODB_wildcard(s, view->from, view->from_attr, nfrom);
		      FREE(s);
		    }
		    else {
		      /* fprintf(stderr,"$$$ regular: pselect[%d]->name = '%s'\n",j,s); */
		      ODB_pushstr(s);
		      p_count[j] = 1;
		    }
		    count += p_count[j];
	            /* fprintf(stderr,"===>p_count[%d] = %d\n",j,p_count[j]); */
		  } /* for (j=0; j<nselect; j++) */

		  if (errcnt > 0) ODB_exit(errcnt);

		  /* fprintf(stderr,"### nselect=%d => count=%d\n",nselect,count); */

		  {		  
		    ODB_SelectExpr **p_sel_expanded;
		    Boolean *p_readonly_expanded;
		    CALLOC(p_sel_expanded, count);
		    ALLOC(p_readonly_expanded, count);
		    i = count;
		    for (j=nselect-1; j>=0; j--) {
		      int k;
		      for (k=0; k<p_count[j]; k++) {
		        --i;
		        p_readonly_expanded[i] = p_readonly[j];
		        p_sel_expanded[i] = p_sel[j];
		      } /* for (k=0; k<p_count[j]; k++) */
		    } /* for (j=nselect-1; j>=0; j--) */
  		    FREE(p_readonly);
		    p_readonly = p_readonly_expanded;
		    FREE(p_sel);
		    p_sel = p_sel_expanded;
		  }

		  FREE(pselect);
		  nselect = count;

		  ALLOC(pselect, nselect);

		  for (j=nselect-1; j>=0; j--) {
		    char *s = ODB_popstr(); /* a character string from the stack */
		    /* The following symbol may still have "@"'s or "."'s */
		    /* fprintf(stderr,"$$$ from stack %d = '%s' (readonly=%d)\n",j,s,p_readonly[j]); */
		    pselect[j] = ODB_new_symbol(ODB_NAME, s); 
		  }

		} /* if (has_regex || has_wildcard) */

		/* === Check SELECT relations === */

		if (relate_select) {
		  int count = 0;
		  SymbolParam_t *inout;

		  /*** SELECT ***/

		  ALLOC(inout, 1);
		  inout->nsym = nselect;
		  inout->sym = pselect;
		  inout->readonly = p_readonly;
		  inout->sel = p_sel;

		  (void)ODB_resolve_relations("SELECT", ODB_BAILOUT_SELECT, inout, 
			vname, view->from, view->from_attr, nfrom, pselect, NULL, nselect, 
			0, 0,
			&count, NULL);

		  FREE(pselect);
		  FREE(p_readonly);

		  relate_select = 0;

		  /* === Remove duplicates (if any) === */

		  nselect = ODB_remove_duplicates(inout, 
						  NULL,
						  NULL,
						  NULL,
						  NULL,
						  NULL,
						  NULL,
						  NULL,
						  NULL,
						  NULL);

		  pselect = inout->sym;
		  p_readonly = inout->readonly;
		  p_sel = inout->sel;

		  FREE(inout);
		}

		view->nselect_all = nselect;
		view->nselect = nselect;
		view->select = pselect;
		view->readonly = p_readonly;

		view->all_readonly = 0;
		for (j=0; j<nselect; j++) {
		  view->all_readonly += view->readonly[j];
	          /* fprintf(stderr,"$$$ select col#%d = '%s'\n",j,view->select[j]->name); */
		}
		view->all_readonly = (view->all_readonly == nselect);

		if (nselect == 0) {
		  SETMSG1("Empty SELECT-clause in view '%s'",vname);
		  YYerror(msg);
		}

		/* Find symbol references in select expressions/formulas */

		view->sel = p_sel;
		CALLOC(view->is_formula, nselect);
		view->nselsym = 0;
		view->selsym = NULL;
		for (j=0; j<nselect; j++) {
		  if (view->sel && view->sel[j] && view->sel[j]->formula) {
		    /*** SELECT expression (for each column separately) ***/
		    ODB_Tree *expr = view->sel[j]->expr;
		    char *formula = view->sel[j]->formula;
		    int count = 0;
		    ODB_Symbol **psym = view->sel[j]->sym;
		    int nsym = view->sel[j]->nsym;
		    /* fprintf(stderr,"SELECTEXPR[col#%d:%s]< nsym=%d\n",j+1,formula,nsym); */
		    psym = view->sel[j]->sym = 
			  ODB_resolve_relations("SELECTEXPR", ODB_BAILOUT_SELECTEXPR, NULL, 
			  vname, view->from, view->from_attr, nfrom, psym, NULL, nsym, 
			  1, 1,
			  &count, NULL);
		    /* fprintf(stderr,"SELECTEXPR[col#%d:%s]> nsym=%d\n",j+1,formula,count); */
		    if (nsym != count) {
		      SETMSG1("Tried to access the whole link-word (= 2 columns) in your SELECT-expression '%s' ?",
		      formula+1);
		      YYwarn(1,msg);
		      SETMSG2("A serious programming error after ODB_resolve_relations(SELECTEXPR): nsym=%d != count=%d",
		              nsym, count);
		      YYerror(msg); /* Fails on this */
		    }
		    view->nselsym += count;
		    view->is_formula[j] = 1;

		    if (maybe_sd) {
		      /* Still in SELECT DISTINCT -route ? */
		      ODB_Tree *expr = view->sel[j]->expr;
		      double numba = 0;
		      int irc = ODB_evaluate(expr,&numba);
		      if (!irc) maybe_sd = 0;
		    }
		  }
		  else
		    maybe_sd = 0;
		}

		if (maybe_sd) YACC_select_distinct = 1;

		if (view->nselsym > 0) {
		  int k, nselsym = view->nselsym;
		  CALLOC(view->selsym, nselsym);
		  for (j=0, k=0; j<nselect; j++) {
		    if (view->is_formula[j] && view->sel[j]->nsym > 0) {
		      int kk, nsym = view->sel[j]->nsym;
		      for (kk=0; kk<nsym; kk++) {
		        view->selsym[k+kk] = view->sel[j]->sym[kk];
		      }
		      k += nsym;
		    }
		  }
		}

		/* Take UNIQUEBY elements from the SELECT-list directly */

		if (YACC_select_distinct) {
		  nuniqueby = nselect;
		  ALLOC(puniqueby, nuniqueby);
		  for (j=0; j<nuniqueby; j++) {
		    puniqueby[j] = ODB_symbol_copy(pselect[j]);
		  }		  
		  view->select_distinct = 1;
		}

		/* === Adjust UNIQUEBY references, if integers were found === */
		/* Not applicable to SELECT DISTINCT, since an integer is also an expression */

		if (has_number_uniqueby && !YACC_select_distinct) {
		  for (j=0; j<nuniqueby; j++) {
		    char *s = puniqueby[j]->name;
		    if (ODB_is_integer(s)) {
			int val = atoi(s);
			if (val >= 1 && val <= view->nselect) {
			  puniqueby[j] = view->select[val-1];
			}
			else {
			  SETMSG2("No such SELECT-column#%d. UNIQUEBY out of bounds in view '%s'",
				 val, vname);
			  YYerror(msg); 
			}
		    } /* if (ODB_is_integer(s)) */
		  }
		}

		/* === Append UNIQUEBY variables to the WHERE-condition === */

		if (nuniqueby > 0) {
		  double Dnuniqueby = nuniqueby;
		  int numargs = 1 + nuniqueby;
		  ODB_Tree *func, *expr;

		  /*** Add expression with AND ***/

		  expr = ODBOPER1(ODB_NUMBER,&Dnuniqueby);
		  ODB_pushexpr(expr);

		  if (YACC_select_distinct) {
		    /* "puniqueby[j]->name" can be a formula */
		    for (j=0; j<nuniqueby; j++) {
		      /* Note: right now 'nuniqueby == nselect' */
		      char *s = puniqueby[j]->name;
		      if (view->is_formula[j]) {
			expr = ODB_oper_copy(view->sel[j]->expr, 1);
		      }
		      else {
		        expr = ODBOPER1(ODB_NAME,puniqueby[j]->name);
		      }
		      ODB_pushexpr(expr);
		    }
		  }
		  else {
		    for (j=0; j<nuniqueby; j++) {
		      expr = ODBOPER1(ODB_NAME,puniqueby[j]->name);
		      ODB_pushexpr(expr);
		    }
		  }
		  func = ODBOPER2(ODB_FUNC,"Unique",&numargs);

		  if (cond)  cond = ODBOPER2(ODB_AND,cond,func);
		  else       cond = func;

		  /*** Re-create list of WHERE-symbols added by the UNIQUEBY symbols ***/

		  FREE(pwhere);
		  nwhere = ODB_trace_symbols(cond, NULL, 0);
		  ALLOC(pwhere, nwhere);

		  for (j=nwhere-1; j>=0; j--) {
		    char *s = ODB_popstr(); /* a character string from the stack */
		    /* The following symbol may still have "@"'s or "."'s */
		    pwhere[j] = ODB_new_symbol(ODB_NAME, s);
		  }

		  relate_where = 1;
		}

		/* === Check WHERE relations === */

		if (relate_where) {
		  /*** WHERE ***/
		  int count = 0;
		  view->where = 
			ODB_resolve_relations("WHERE", ODB_BAILOUT_WHERE, NULL, 
			vname, view->from, view->from_attr, nfrom, pwhere, NULL, nwhere, 
			1, 1,
			&count, NULL);

		  view->nwhere = nwhere = count;

		  relate_where = 0;
		}
		else {
		  view->nwhere = nwhere = 0;
		  view->where = NULL;
		}

		/* === Adjust ORDERBY references, if integers were found === */

		if (has_number_orderby) {
		  for (j=0; j<norderby; j++) {
		    char *s = porderby[j]->name;
		    if (ODB_is_integer(s)) {
			int val = atoi(s);
			if (val >= 1 && val <= view->nselect) {
			  porderby[j] = view->select[val-1];
			}
			else {
			  SETMSG2("No such SELECT-column#%d. ORDERBY out of bounds in view '%s'",
				 val, vname);
			  YYerror(msg); 
			}
		    } /* if (ODB_is_integer(s)) */
		  }
		}

		/* === Fill in wildcard ORDERBY relations === */

		if (norderby == -1) {
		  norderby = view->nselect;
		  ALLOC(porderby, norderby);
		  ALLOC(sign, norderby);
		  for (j=0; j<norderby; j++) {
		    ODB_Symbol *psym = view->select[j];
		    char *s = psym->name;
		    porderby[j] = ODB_new_symbol(ODB_NAME, s);
		    sign[j] = +1;
		  }
		}

		/* === Check ORDERBY relations === */

		if (relate_orderby) {
		  /*** ORDERBY ***/
		  int count = 0;
		  view->orderby = 
			ODB_resolve_relations("ORDERBY", ODB_BAILOUT_ORDERBY, NULL,
			vname, view->from, view->from_attr, nfrom, porderby, sign, norderby, 
			0, 0,
			&count, &view->mkeys);

		  view->norderby = norderby = count;
		  relate_orderby = 0;

		  /* Match up ORDER BY-symbols with the specified SELECT-symbols */
		  error |= ODB_matchup_sym(1,
				"ORDERBY", view->orderby, view->norderby, 
				"SELECT", view->select, view->nselect,
				view->mkeys);
		}
		else {
		  view->norderby = 0;
		  view->orderby = NULL;
		  view->mkeys = NULL;
		}

		if (error) { 
		  SETMSG1("Syntax error in view '%s' definition",vname);
		  YYerror(msg);
		}

		FREE(puniqueby);
		FREE(pwhere);
		FREE(porderby);
		FREE(sign);

		view->cond = cond;
		view->nuniqueby = nuniqueby;

		/* === Wrap up === */

		has_usddothash = 0;
		p = ODBOPER1(ODB_VIEW, view);
		has_count_star = 0;

		/* Get the fully resolved UNIQUEBY symbols from WHERE-symbols */
		{
		  int count = view->nwhere - iwhere;
		  if (count > 0) {
		    int k = 0;
		    ALLOC(view->uniqueby, count);
		    for (j=iwhere; j<nwhere; j++) {
		      view->uniqueby[k++] = view->where[j];
		    }
		    view->nuniqueby = count;
		  }
		}

		/* Setup WHERE-symbols and check string comparisons in WHERE */
		ODB_setup_where(view);

		/* Remap symbols in SELECT-expressions */
		if (view->nselsym > 0) {
		  int k=0, nselsym = view->nselsym;
		  for (j=0; j<nselect; j++) {
		    if (view->is_formula[j]) {
		      /*** SELECT expression (for each column separately) ***/
		      ODB_Tree *expr = view->sel[j]->expr;
		      char *formula = view->sel[j]->formula;
		      char *formula_out = NULL;
		      int count = 0;
		      ODB_Symbol **psym = view->sel[j]->sym;
		      int nsym = view->sel[j]->nsym;
		      k += ODB_setup_selectexpr(view, k, j);
		      view->sel[j]->formula_out = formula_out = dump_s(NULL,expr,ddl_piped ? 2 : 0,NULL);
		      /* fprintf(stderr,"SELECTEXPR[col#%d:%s]> nsym=%d\n",j+1,formula_out,nsym); */
		    }
		  }
		  FREE(view->selsym);
		  /* Redo selsym's */
		  CALLOC(view->selsym, nselsym);
		  for (j=0, k=0; j<nselect; j++) {
		    if (view->is_formula[j] && view->sel[j]->nsym > 0) {
		      int kk, nsym = view->sel[j]->nsym;
		      for (kk=0; kk<nsym; kk++) {
		        ODB_Symbol *psym = view->selsym[k+kk] = view->sel[j]->sym[kk];
			if (!has_usddothash && psym && psym->name) {
			  /* Flag presence of "$<parent_table>.<child_table>" -variables in SELECT */
			  has_usddothash = IS_USDDOTHASH(psym->name) ? 1 : 0;
			}
		      } /* for (kk=0; kk<nsym; kk++) */
		      k += nsym;
		    }
		  } /* for (j=0, k=0; j<nselect; j++) */
		}

		/* Set 'has_formulas' if any SELECT colums involves formulas */
		if (view->sel) {
	          for (j=0; j<nselect; j++) {
		    if (view->is_formula[j]) { view->has_formulas = 1; break; }
                  }
		}

		if (view->has_formulas) {
		  /* Update select aggregate list for each element SELECT-list 
		     (constant/common for all) */
		  view->select_aggr_flag = 0;
		  view->ncols_aux = 0;
		  for (j=0; j<nselect; j++) {
		    if (view->is_formula[j]) {
		      view->select_aggr_flag |= view->sel[j]->aggr_flag;
		      view->ncols_aux += view->sel[j]->ncols_aux;
		    }
		  }
		}

		view->usddothash = has_usddothash;

		has_usddothash = 0;
		YACC_select_distinct = 0;
		LEX_convflag = 0;
		LEX_unconv_value = 0;
		pcmd = ODB_new_cmd(p); 
		if (verbose) 
			fprintf(stderr,"yacc: VIEW='%s' done.\n",vname);
	  }
	  $$ = pcmd;
	}
	;

as	: /* empty */	{ $$ = 0; }
	| ODB_AS	{ $$ = 1; }
	| ODB_EQ	{ $$ = 2; }
	| ODB_COLON	{ $$ = 3; }
	;

typedef	: ODB_TYPE name as { 
		ODB_Type *type = ODB_new_type($2, 0);
		$$ = type; 
	}
	;

tabledef: ODB_TABLE arrname as {
		extern Boolean ODB_in_tabledef;
		int errflg = 0;
		int jlist;
		int nt = $2;
		char **table_name = NULL;
		Tabledef_t *list;
		ALLOC(list,1);
		list->num_tables = nt;
		ALLOC(list->tables, nt);
		ODB_in_tabledef = 1;
		ALLOC(table_name, nt);
		for (jlist=nt-1; jlist>=0; jlist--) {
		  table_name[jlist] = ODB_popstr();
		}
		/* Check that the table name is NOT called "core" to avoid confusion with Unix core-files */
		/* ECMWF had one such case in Dec'2005, where tar-command failed (!) on IBM
		   and produced a core-dump in one of the ODB data directories, which bound to cause
		   havoc what it comes to autodetecting table names for compilation etc. */
		/* Now (5-May-2006) we test also against Windows/CYGWIN special files, too */
		for (jlist=0; jlist<nt; jlist++) {
		  int jt;
		  for (jt=0; jt<N_INVTABNAM; jt++) {
		    const char *pname = invalid_table_names[jt];
		    if (strequ(table_name[jlist], pname)) {
		      SETMSG1("ODB/SQL-compiler does not accept table names that are called '%s'.",pname);
		      errflg++;
		      YYwarn(errflg,msg);
		      break; /* for (jt=0; jt<N_INVTABNAM; jt++) */
		    }
		  }
		}
		if (errflg > 0) { /* Invalid table names encountered */
		  SETMSG0("Invalid table names encountered");
		  YYerror(msg);
		}
		for (jlist=0; jlist<nt; jlist++) {
		  list->tables[jlist] = ODB_new_table(table_name[jlist], 0);
		}
		FREE(table_name);
		$$ = list;
	}
	;

indexdef : ODB_CREATEINDEX idxname {
		const int nt = 1;
		char *idxname = $2;
		{
		  Viewdef_t *list;
		  CALLOC(list,1);
		  list->num_views = nt;
		  CALLOC(list->views, nt);
		  list->views[0] = ODB_new_view(idxname, 0);
		  $$ = list;
		}
	 }

dropindex : ODB_DROPINDEX opt_idxname {
		const int nt = 1;
		char *idxname = $2;
		if (!idxname) idxname = use_index_name;
		if (!idxname) idxname = STRDUP("*");
		{
		  Viewdef_t *list;
		  CALLOC(list,1);
		  list->num_views = nt;
		  CALLOC(list->views, nt);
		  list->views[0] = ODB_new_view(idxname, 0);
		  $$ = list;
		}
	 }
	 ;

viewdef	: ODB_VIEW arrname as { 
		int jlist;
		int nt = $2;
		char **view_name = NULL;
		Viewdef_t *list;
		ALLOC(list,1);
		list->num_views = nt;
		ALLOC(list->views, nt);
		ALLOC(view_name, nt);
		for (jlist=nt-1; jlist>=0; jlist--) {
		  view_name[jlist] = ODB_popstr();
		}
		for (jlist=0; jlist<nt; jlist++) {
		  list->views[jlist] = ODB_new_view(view_name[jlist], 0);
		  tmp_view_num++;		  
		}
		FREE(view_name);
		$$ = list;
	}
	| /* empty */ {
	        extern char *odb_source;
	        const int nt = 1;
	        Viewdef_t *list;
		char *viewname = NULL;
		ALLOC(list,1);
		list->num_views = nt;
		ALLOC(list->views, nt);
		if (tmp_view_num > 0 || !odb_source) {
		  if (!odb_source && tmp_view_num == 0) tmp_view_num++;
		  ALLOC(viewname,50);
		  sprintf(viewname,"%s_%d","_tmp",tmp_view_num++);
		  list->views[0] = ODB_new_view(viewname, 0);
		}
		else {
		  char *p = viewname = ODB_lowercase(odb_source);
		  char *find = strrchr(viewname,'/');
		  if (find) p = find + 1;
		  find = strchr(p,'.');
		  if (find) *find = '\0';
		  list->views[0] = ODB_new_view(p, 0);
		  tmp_view_num++;		  
		}
		FREE(viewname);
		$$ = list;
	}
	;

select	: select2 sellist	{ 
	        ODB_SelAttr_t *attr = $1;
		Selectdef_t *sdef = NULL;
		Selectdef_t *p_sdef = $2;
		int i, n = 0;
		while (p_sdef) {
		  n += p_sdef->nselect;
		  p_sdef = p_sdef->next;
		}
		/* fprintf(stderr,">>ODB_SELECT sellist: n=%d\n",n); */
		CALLOC(sdef, 1);
		sdef->nselect = n;
		ALLOC(sdef->select_items, n);
		ALLOC(sdef->readonly, n);
		CALLOC(sdef->sel, n);
		sdef->next = NULL;
		if (n > 0) {
		  int chunk = 0;
		  i = n;
		  p_sdef = $2;
		  while (p_sdef) {
		    int j, nselect = p_sdef->nselect;
		    ++chunk;
		    /* fprintf(stderr,"** nselect=%d (chunk#%d)\n",nselect,chunk); */
		    for (j=nselect-1; j>=0; j--) {
		      --i;
		      sdef->select_items[i] = p_sdef->select_items[j];
		      sdef->readonly[i] = p_sdef->readonly[j];
		      if (p_sdef->sel) sdef->sel[i] = p_sdef->sel[j];
		      /* fprintf(stderr,"**** i=%d, j=%d: '%s' [r/o=%d]\n",
			      i,j,sdef->select_items[i],sdef->readonly[i]); */
		    }
		    p_sdef = p_sdef->next;
		  }
		}
		/* fprintf(stderr,"<<ODB_SELECT sellist\n"); */
		YACC_select_distinct = attr ? attr->distinct : 0;
		$$ = sdef;
	}
	| select2 star opt_sel_param opt_comma { 
	        ODB_SelAttr_t *attr = $1;
		int mode = $3;
		Boolean readonly_flag = 
		  IS_SET(mode,ODBURO_EMPTY) ? readonly_mode : IS_SET(mode,ODBREADONLY);
		Selectdef_t *sdef;
		CALLOC(sdef,1);
		sdef->nselect = -1;
		sdef->select_items = NULL;
		ALLOC(sdef->readonly, 1);
		sdef->readonly[0] = readonly_flag;
		sdef->next = NULL;
		YACC_select_distinct = attr ? attr->distinct : 0;
		if (IS_SET(mode,ODBAS)) { /* Disregard, but warn */
		  char *s = ODB_popstr();
		  SETMSG1("Nickname not applicable in 'SELECT * AS %s'",s);
		  YYerror(msg);
		}
		$$ = sdef;
	}
	;

opt_comma : /* empty */
	  | ODB_COMMA
	  ;

select2	: ODB_SELECT_DISTINCT { ODB_SelAttr_t *attr; CALLOC(attr,1); attr->distinct = 1; $$ = attr; }
	| ODB_SELECT_ALL      { $$ = NULL; /* interpreted as distinct=0 */ }
	;

opt_sel_param 	: opt_as_nickname 		{ $$ = (ODBURO_EMPTY | $1); }
		| opt_as_nickname ODB_READONLY	{ $$ = (ODBREADONLY  | $1); }
		| opt_as_nickname ODB_UPDATED	{ $$ = (ODBUPDATED   | $1); }
		;

opt_as_nickname	: /* empty */ { $$ = 0; }
	      	| ODB_AS name   { ODB_pushstr($2); $$ = ODBAS; }
	      	| ODB_AS string { ODB_pushstr($2); $$ = ODBAS; }
		;

uniqueby: /* empty */			{ $$ = 0; }
	| ODB_UNIQUEBY uniqlist		{ $$ = $2; }
	| ODB_UNIQUEBY star		{ $$ = -1; }
	;

from	: /* empty */			{ $$ = ODB_pick_tables(-2); /* Automatic "pick up" */ }
	| ODB_FROM star			{ $$ = ODB_pick_tables(-1); /* Automatic "pick up" */}
	| ODB_FROM fromlist		{ $$ = ODB_pick_tables($2); /* User defined : add attribute */}
	;

ontable : ODB_ON fromlist { 
	    int count = ODB_pick_tables($2);
	    $$ = count;
          }
	;

opt_ontable : /* empty */ { $$ =  0; }
	    | ontable	  { $$ = $1; }
	    ;

where	: /* empty */			{ $$ = NULL; }
	| ODB_WHERE cond		{ $$ = $2;   }
	;

orderby	: /* empty */			{ $$ = 0; }
	| ODB_ORDERBY sortlist		{ $$ = $2; }
	| ODB_ORDERBY star		{ $$ = -1; }
	;

star	: ODB_STAR			{ $$ = -1; }
	| ODB_LP star ODB_RP		{ $$ = -1; }
	;

cond	: expr				{ $$ = $1; }
	| pred				{ $$ = $1; }
	;

insiders : ODB_INSIDE		{ $$ = ODB_INSIDE; }
	 | ODB_INSIDE_POLYGON   { $$ = ODB_INSIDE_POLYGON; }
	 ;

outsiders : ODB_OUTSIDE		{ $$ = ODB_OUTSIDE; }
	  | ODB_OUTSIDE_POLYGON { $$ = ODB_OUTSIDE_POLYGON; }
	  ;

other_siders : insiders string opt_comma ODB_RP {
          int what = $1;
	  char *s = YACC_join(3,$2);
	  ODB_Tree *region = ODBOPER1(ODB_STRING,s);
	  ODB_Tree *lat = ODBOPER1(ODB_NAME,ODB_LAT);
	  ODB_Tree *lon = ODBOPER1(ODB_NAME,ODB_LON);
	  $$ = ODBOPER3(what, region, lat, lon);
	}
	| outsiders string opt_comma ODB_RP {
	  char *s = YACC_join(3,$2);
	  ODB_Tree *region = ODBOPER1(ODB_STRING,s);
	  ODB_Tree *lat = ODBOPER1(ODB_NAME,ODB_LAT);
	  ODB_Tree *lon = ODBOPER1(ODB_NAME,ODB_LON);
	  int what = ($1 == ODB_OUTSIDE) ? ODB_INSIDE : ODB_INSIDE_POLYGON;
	  ODB_Tree *tmp = ODBOPER3(what, region, lat, lon);
	  $$ = ODBOPER1(ODB_NOT, tmp);
	}
	;

other_near : ODB_NEAR string opt_comma ODB_RP {
	  char *s = YACC_join(3,$2);
	  ODB_Tree *place = ODBOPER1(ODB_STRING,s);
	  ODB_Tree *lat = ODBOPER1(ODB_NAME,ODB_LAT);
	  ODB_Tree *lon = ODBOPER1(ODB_NAME,ODB_LON);
	  $$ = ODBOPER3(ODB_NEAR, place, lat, lon);
	}
	;

selinside : insiders string ODB_COMMA selexpr ODB_COMMA selexpr opt_comma ODB_RP {
          int what = $1;
	  char *s = YACC_join_by_expr($2, ODB_NAME, $4, $6);
	  ODB_Tree *region = ODBOPER1(ODB_STRING,s);
	  $$ = ODBOPER3(what, region, $4, $6);
	}
	| outsiders string ODB_COMMA selexpr ODB_COMMA selexpr opt_comma ODB_RP {
	  char *s = YACC_join_by_expr($2, ODB_NAME, $4, $6);
	  ODB_Tree *region = ODBOPER1(ODB_STRING,s);
	  int what = ($1 == ODB_OUTSIDE) ? ODB_INSIDE : ODB_INSIDE_POLYGON;
	  ODB_Tree *tmp = ODBOPER3(what, region, $4, $6);
	  $$ = ODBOPER1(ODB_NOT, tmp);
	}
	| other_siders { $$ = $1; }
	;

selnear	: ODB_NEAR string ODB_COMMA selexpr ODB_COMMA selexpr opt_comma ODB_RP {
	  char *s = YACC_join_by_expr($2, ODB_NAME, $4, $6);
	  ODB_Tree *place = ODBOPER1(ODB_STRING,s);
	  $$ = ODBOPER3(ODB_NEAR, place, $4, $6);
	}
	| other_near { $$ = $1; }
	;

inside	: insiders string ODB_COMMA expr ODB_COMMA expr opt_comma ODB_RP {
	  char *s = YACC_join_by_expr($2, ODB_NAME, $4, $6);
	  ODB_Tree *region = ODBOPER1(ODB_STRING,s);
	  $$ = ODBOPER3($1, region, $4, $6);
	}
	| outsiders string ODB_COMMA expr ODB_COMMA expr opt_comma ODB_RP {
	  char *s = YACC_join_by_expr($2, ODB_NAME, $4, $6);
	  ODB_Tree *region = ODBOPER1(ODB_STRING,s);
	  int what = ($1 == ODB_OUTSIDE) ? ODB_INSIDE : ODB_INSIDE_POLYGON;
	  ODB_Tree *tmp = ODBOPER3(what, region, $4, $6);
	  $$ = ODBOPER1(ODB_NOT, tmp);
	}
	| other_siders { $$ = $1; }
	;

near	: ODB_NEAR string ODB_COMMA expr ODB_COMMA expr opt_comma ODB_RP {
	  char *s = YACC_join_by_expr($2, ODB_NAME, $4, $6);
	  ODB_Tree *place = ODBOPER1(ODB_STRING,s);
	  $$ = ODBOPER3(ODB_NEAR, place, $4, $6);
	}
	| other_near { $$ = $1; }
	;

strcmpre: arrname			{ $$ = ODBOPER1(ODB_ARRNAME,&$1); }
	| arrname ODB_EQNE_STRING	{ /* A serious hack ;-( */
	    char *s = $2;
	    int eqne = (*s++ == '1') ? ODB_EQ : ODB_NE;
	    ODB_Tree *lhs = ODBOPER1(ODB_ARRNAME,&$1);
	    ODB_Tree *rhs = ODBOPER1(ODB_STRING,s);
	    int oper = ODB_EQ;
	    ODB_Tree *t = ODBOPER3(ODB_STRFUNC, lhs, rhs, &oper);
	    $$ = (eqne == oper) ? t : ODBOPER1(ODB_NOT, t);
	}	
	| arrname like_notlike string	{ 
	    ODB_Tree *lhs = ODBOPER1(ODB_ARRNAME,&$1);
	    ODB_Tree *rhs = ODBOPER1(ODB_WC_STRING,$3);
	    int like = $2 ? ODB_LIKE : ODB_NOTLIKE;
	    int oper = ODB_LIKE;
	    ODB_Tree *t = ODBOPER3(ODB_STRFUNC, lhs, rhs, &oper);
	    $$ = (like == oper) ? t : ODBOPER1(ODB_NOT, t);
	}
	;

eqne	: ODB_EQ	{ $$ = 1; }
	| ODB_NE	{ $$ = 0; }
	;

like_notlike	: ODB_LIKE	{ $$ = 1; }
		| ODB_NOTLIKE	{ $$ = 0; }
		;

pred	: ODB_LP pred ODB_RP		{ $$ = $2; }
	| expr is_null_or_is_not_null		{
	  int is_what = $2 ? ODB_EQ : ODB_NE;
	  char *pname = "$mdi";
	  ODB_Symbol *name = ODB_lookup(ODB_USDNAME,pname,NULL);
	  double mdi = name ? ABS(name->dval) : ABS(RMDI);
	  ODB_Tree *arg = $1;
	  if (is_what == ODB_EQ) {
	    /* WHERE x is NULL 
	       will effectively be translated to the following:
	       WHERE abs(x) = $mdi 
	     */
	    int nargs = 1;
	    ODB_pushexpr(arg);
	    {
	      ODB_Tree *lhs = ODBOPER2(ODB_FUNC,"abs",&nargs);
	      ODB_Tree *rhs = ODBOPER1(ODB_NUMBER,&mdi);
	      $$ = ODBOPER2(is_what, lhs, rhs);
	    }
	  }
	  else {
	    /* WHERE x is not NULL 
	       will effectively be translated to the following:
	       WHERE x != $mdi && x != -$mdi
	     */
	    ODB_Tree *rhspos = ODBOPER1(ODB_NUMBER,&mdi); /* $mdi */
	    double mdineg = -mdi;
	    ODB_Tree *rhsneg = ODBOPER1(ODB_NUMBER,&mdineg); /* -$mdi */
	    ODB_Tree *left  = ODBOPER2(ODB_NE, arg, rhspos); /* x !=  $mdi */
	    ODB_Tree *right = ODBOPER2(ODB_NE, arg, rhsneg); /* x != -$mdi */
	    $$ = ODBOPER2(ODB_AND, left, right); /* left AND right */
	  }
	}
	| ODB_NOT cond			{ $$ = ODBOPER1(ODB_NOT,$2); }
	| cond ODB_AND cond		{ $$ = ODBOPER2(ODB_AND,$1,$3); }
	| cond ODB_OR cond		{ $$ = ODBOPER2(ODB_OR,$1,$3); }
	| expr ODB_BETWEEN expr ODB_AND expr { $$ = ODBOPER3(ODB_LELE,$3,$1,$5); }
	| expr ODB_GT expr		{ $$ = ODBOPER2(ODB_GT,$1,$3); }
	| expr ODB_GE expr		{ $$ = ODBOPER2(ODB_GE,$1,$3); }
	| expr eqne expr		{ $$ = ODBOPER2($2 ? ODB_EQ : ODB_NE,$1,$3); }
	| expr ODB_IS expr		{ $$ = ODBOPER2(ODB_EQ,$1,$3); }
	| expr ODB_LE expr		{ $$ = ODBOPER2(ODB_LE,$1,$3); }
	| expr ODB_LT expr		{ $$ = ODBOPER2(ODB_LT,$1,$3); }
	| expr ODB_CMP expr		{ 
	  int numargs = 2;
	  ODB_pushexpr($1); ODB_pushexpr($3);
	  $$ = ODBOPER2(ODB_FUNC,"cmp",&numargs); 
	}
	| expr ODB_IS ODB_NOT expr	{ $$ = ODBOPER2(ODB_NE,$1,$4); }
	| expr ODB_GT expr ODB_GT expr	{ $$ = ODBOPER3(ODB_GTGT,$1,$3,$5); }
	| expr ODB_GT expr ODB_GE expr	{ $$ = ODBOPER3(ODB_GTGE,$1,$3,$5); }
	| expr ODB_GE expr ODB_GE expr	{ $$ = ODBOPER3(ODB_GEGE,$1,$3,$5); }
	| expr ODB_GE expr ODB_GT expr	{ $$ = ODBOPER3(ODB_GEGT,$1,$3,$5); }
	| expr ODB_LT expr ODB_LT expr	{ $$ = ODBOPER3(ODB_LTLT,$1,$3,$5); }
	| expr ODB_LT expr ODB_LE expr	{ $$ = ODBOPER3(ODB_LTLE,$1,$3,$5); }
	| expr ODB_LE expr ODB_LE expr	{ $$ = ODBOPER3(ODB_LELE,$1,$3,$5); }
	| expr ODB_LE expr ODB_LT expr	{ $$ = ODBOPER3(ODB_LELT,$1,$3,$5); }
	| ODB_ALIGN ODB_LP fromlist ODB_RP {
		int numargs = $3;
		if (numargs >= 2) {
		  int j, nequ = numargs - 1;
		  char **hs;
		  ODB_Tree **hexpr, **equexpr;
		  ODB_Tree *target, *andchain;
		  ALLOC(hs, numargs);
		  ALLOC(hexpr, numargs);
		  for (j=numargs-1; j>=0; j--) {
			char *t = NULL;
			char *s = ODB_popstr();
			int len = strlen(s) + 2;
			ALLOC(t,len);
			sprintf(t,"#%s",s);
			hexpr[j] = ODBOPER1(ODB_HASHNAME, t);
			FREE(t);
		  }
		  target = hexpr[0];
		  ALLOC(equexpr, nequ);
		  for (j=0; j<nequ; j++) {
			equexpr[j] = ODBOPER2(ODB_EQ, target, hexpr[j+1]);
		  }
		  andchain = equexpr[0];
		  for (j=1; j<nequ; j++) {
			andchain = ODBOPER2(ODB_AND, andchain, equexpr[j]);
		  }
		  $$ = andchain;
		}
		else { /* No args ==> error */
		  SETMSG0("ALIGN ( )-statement expects at least two arguments.");
		  YYerror(msg);
		}
	}
	| expr in_notin ODB_LP expr ODB_COLON expr opt_inc ODB_RP {
	  ODB_Tree *target = $1;
	  Boolean is_in = $2;
	  ODB_Tree *begin = $4;
	  ODB_Tree *end = $6;
	  ODB_Tree *step = $7;
	  int j, n = 4;
	  ODB_Tree **args;
	  ALLOC(args, n);
	  args[0] = target;
	  args[1] = begin;
	  args[2] = end;
	  args[3] = step;
	  for (j=0; j<n; j++) ODB_pushexpr(args[j]);
	  { 
	    ODB_Tree *f = ODBOPER2(ODB_FUNC,"InGenList",&n);
	    $$ = is_in ? f : ODBOPER1(ODB_NOT,f);
	  }
	  FREE(args);
	}
	| expr in_notin query {
	  int numexpr = 1;
	  double dnumexpr = numexpr;
	  ODB_Tree *numexpr_expr = ODBOPER1(ODB_NUMBER, &dnumexpr);
	  double dnummatch = 0;
	  ODB_Tree *nummatch_expr = ODBOPER1(ODB_NUMBER, &dnummatch);
	  ODB_Tree *expr = $1;
	  Boolean is_in = $2;
	  ODB_Tree *query = $3;
	  ODB_Symbol *query_funcsym = query->argv[0];
	  char *query_funcname = query_funcsym->name;
	  double runonce = strequ(query_funcname, "RunOnceQuery") ? 1 : 0;
	  ODB_Tree *runonce_expr = ODBOPER1(ODB_NUMBER, &runonce);
	  int numargs = numexpr + 4;
	  ODB_pushexpr(numexpr_expr);
	  ODB_pushexpr(nummatch_expr);
	  ODB_pushexpr(query);
	  ODB_pushexpr(runonce_expr);
	  ODB_pushexpr(expr);
	  { 
	    ODB_Tree *f = ODBOPER4(ODB_FUNC, "InQuery", &numargs, NULL, NULL);
	    $$ = is_in ? f : ODBOPER1(ODB_NOT, f);
	  }
	}
	| ODB_MATCH expr ODB_COMMA exprlist opt_comma ODB_RP in_notin query {
	  int j,numexpr = 1 + $4;
	  double dnumexpr = numexpr;
	  ODB_Tree *numexpr_expr = ODBOPER1(ODB_NUMBER, &dnumexpr);
	  double dnummatch = 0;
	  ODB_Tree *nummatch_expr = ODBOPER1(ODB_NUMBER, &dnummatch);
	  ODB_Tree **expr = NULL;
	  CALLOC(expr, numexpr);
	  for (j=numexpr-1; j>0; j--) expr[j] = ODB_popexpr();
	  expr[0] = $2;
	  {
	    Boolean is_in = $7;
	    ODB_Tree *query = $8;
	    ODB_Symbol *query_funcsym = query->argv[0];
	    char *query_funcname = query_funcsym->name;
	    double runonce = strequ(query_funcname, "RunOnceQuery") ? 1 : 0;
	    ODB_Tree *runonce_expr = ODBOPER1(ODB_NUMBER, &runonce);
	    double dnumexpr = numexpr;
	    ODB_Tree *numexpr_expr = ODBOPER1(ODB_NUMBER, &dnumexpr);
	    int numargs = numexpr + 4;
	    ODB_pushexpr(numexpr_expr);
	    ODB_pushexpr(nummatch_expr);
	    ODB_pushexpr(query);
	    ODB_pushexpr(runonce_expr);
	    for (j=0; j<numexpr; j++) ODB_pushexpr(expr[j]);
	    FREE(expr);
	    { 
	      ODB_Tree *f = ODBOPER4(ODB_FUNC, "InQuery", &numargs, NULL, NULL);
	      $$ = is_in ? f : ODBOPER1(ODB_NOT, f);
	    }
	  }
	}
	| matchfunc in_notin query {
	  ODB_Match_t *match = $1;
	  const char *formula = match->formula;
	  int nummatch = strchr(formula,'\\') ? 1 : 0; /* If any \<number>'s present ==> nummatch = 1 */
	  ODB_Tree *match_expr = (nummatch == 1) ? ODBOPER1(ODB_MATCH, match) : match->expr;
	  {
	    double dnummatch = nummatch;
	    ODB_Tree *nummatch_expr = ODBOPER1(ODB_NUMBER, &dnummatch);
	    Boolean is_in = $2;
	    ODB_Tree *query = $3;
	    ODB_Symbol *query_funcsym = query->argv[0];
	    char *query_funcname = query_funcsym->name;
	    double runonce = strequ(query_funcname, "RunOnceQuery") ? 1 : 0;
	    ODB_Tree *runonce_expr = ODBOPER1(ODB_NUMBER, &runonce);
	    int numexpr = 1;
	    double dnumexpr = numexpr;
	    ODB_Tree *numexpr_expr = ODBOPER1(ODB_NUMBER, &dnumexpr);
	    int numargs = numexpr + 4;
	    ODB_pushexpr(numexpr_expr);
	    ODB_pushexpr(nummatch_expr);
	    ODB_pushexpr(query);
	    ODB_pushexpr(runonce_expr);
	    ODB_pushexpr(match_expr);
	    { 
	      ODB_Tree *f = ODBOPER4(ODB_FUNC, "InQuery", &numargs, NULL, (nummatch == 1) ? match : NULL);
	      $$ = is_in ? f : ODBOPER1(ODB_NOT, f);
	    }
	  }
	}
	| expr in_notin ODB_LP arglist ODB_RP {
	  Boolean is_in = $2;
	  int numargs = $4;
	  if (numargs >= 1) {
	    ODB_Tree *target = $1;
	    $$ = ODBOPER2(is_in?ODB_IN:ODB_NOTIN,target,&numargs);
	  }
	  else { /* No args ==> warning */
	    double numba = is_in ? 0 : 1; /* always false/true */
	    SETMSG1("expression %s ( )-statement expects at least one argument.",
		    is_in ? "IN" : "NOTIN");
	    YYwarn(0,msg);
	    $$ = ODBOPER1(ODB_NUMBER, &numba);
	  }
	}
	| expr infile_notinfile filename {
	  ODB_Tree *expr = $1;
	  char *filename = STRDUP($3);
	  $$ = ODBOPER3(ODB_FILE,$2 ? "InFile" : "NotInFile",filename,expr);
	}
	;

infile_notinfile	: ODB_INFILE			{ $$ = 1; }
			| ODB_NOTINFILE			{ $$ = 0; }
			;

in_notin: ODB_IN			{ $$ = 1; }
	| ODB_NOTIN			{ $$ = 0; }
	;

opt_inc : /* empty */			{ $$ = ODBOPER1(ODB_NUMBER,&YACC_IncOne);  }
	| ODB_COLON expr		{ $$ = $2; }
	; 

opt_selinc : /* empty */		{ $$ = ODBOPER1(ODB_NUMBER,&YACC_IncOne);  }
	| ODB_COLON formula		{ $$ = $2; }
	; 

filename: ODB_STRING			{ $$ = $1; }
	| ODB_LP ODB_STRING ODB_RP	{ $$ = $2; }
	;

uniqlist: /* empty */			{ $$ = 0; }
	| arrname			{ $$ = $1; }
	| const_expr			{ 
		char *s; ALLOC(s,50); sprintf(s,"%.20g",$1); ODB_pushstr(s);
		$$ = 1;
	}
	| uniqlist ODB_COMMA arrname	{ $$ = $1 + $3; }
	| uniqlist ODB_COMMA const_expr	{ 
		char *s; ALLOC(s,50); sprintf(s,"%.20g",$3); ODB_pushstr(s);
		$$ = $1 + 1; 
	}
	| uniqlist ODB_COMMA		{ $$ = $1; }
	;

fromlist: /* empty */			{ $$ = 0; }
	| arrname			{ $$ = $1; }
	| fromlist ODB_COMMA arrname	{ $$ = $1 + $3; }
	| fromlist ODB_COMMA		{ $$ = $1; }
	;

indexname : name			{ ODB_pushstr($1); $$ = 1; }
	  ;

indexlist : /* empty */				{ $$ = 0;  }
	  | indexname				{ $$ = $1; }
	  | indexlist ODB_COMMA indexname	{ $$ = $1 + $3; }
	  | indexlist ODB_COMMA			{ $$ = $1; }
	  ;

indexcols : /* empty */		        { $$ = 0;  }
	  | ODB_LP indexlist ODB_RP	{ $$ = $2; }
	  ;

sellist : selname			{ $$ = $1; }
	| sellist ODB_COMMA selname { 
		Selectdef_t *sdef = $3;
		sdef->next = $1;
		$$ = sdef;
	}
	| sellist ODB_COMMA		{ $$ = $1; }
	;

selname : formula opt_sel_param { 
                static int colid = 0;
		int mode = $2;
		Boolean readonly_flag =
		  IS_SET(mode,ODBURO_EMPTY) ? readonly_mode : IS_SET(mode,ODBREADONLY);
		ODB_Tree *expr = $1;
		Boolean is_name = (expr && expr->what == ODB_NAME);
		Boolean is_aggr = (expr && expr->what == ODB_FUNCAGGR);
		Selectdef_t *sdef;
		int j, n = is_name ? expr->argc : 1;
		ODB_Symbol *nick = NULL;
		/* fprintf(stderr,"selexpr opt_sel_param : is_name = %d, n=%d\n",(int)is_name, n); */
		CALLOC(sdef, 1);
		sdef->nselect = n;
		ALLOC(sdef->select_items, n);
		ALLOC(sdef->readonly, n);
		CALLOC(sdef->sel, n);
		if (IS_SET(mode,ODBAS)) { /* Nicknames */
		  char *s = ODB_popstr();
		  if (n > 1) { /* not applicable/bad if n > 1 */
		    SETMSG1("Multiple colnames in 'SELECT arrayname[lo:hi] AS %s' cannot have the same nickname",s);
		    YYerror(msg); /* Abort */
		  }
		  /* Replace possible ODB_tag_delim's with blanks */
		  {
		    const char *tag_delim = ODB_tag_delim;
		    if (tag_delim) {
		      char *ss = s;
		      while (*ss) {
			if (*ss == *tag_delim) *ss = ' ';
			++ss;
		      }
		    }
		  }
		  nick = ODB_new_symbol(ODB_NICKNAME, s);
		  for (j=0; j<n; j++) sdef->sel[j].nicksym = nick;
		}
		if (!is_name) {
		  char *first_char = STRDUP(FORMULA_CHAR); /* first_char free'd inside the dump_s */
		  Boolean subexpr_is_aggrfunc = 0;
		  /* Search also subexpression for ODB_FUNCAGGR, since that's disallowed */ 
		  char *formula = dump_s(first_char,expr,ddl_piped ? 2 : 0,&subexpr_is_aggrfunc);
		  char *f;
		  int nsym = 0;
		  int ncols_aux = 0;
		  /* filter formula : change xxx_distinct(z) into xxx(distinct z) */
		  while ( (f = strstr(formula,"_distinct(")) != NULL ) { f[0] = '('; f[9] = ' '; }
		  /* filter formula : change count(1) into count(*) */
		  while ( (f = strstr(formula,"count(1)")) != NULL ) { f[6] = '*'; }
		  sdef->sel[0].expr = expr;
		  sdef->sel[0].aggr_flag = is_aggr ? ODB_which_aggr(expr, &ncols_aux) : ODB_AGGR_NONE;
		  sdef->sel[0].formula = formula;
		  sdef->sel[0].ncols_aux = ncols_aux;
		  if (subexpr_is_aggrfunc) {
		    SETMSG1("Sub-expression in SELECT-column '%s' cannot be an aggregate function",
			    formula+1);
		    YYerror(msg);
		  }
		  sdef->sel[0].nsym = nsym = ODB_trace_symbols(expr,NULL,1);
		  if (nsym > 0) {
		    ALLOC(sdef->sel[0].sym, nsym);
		    for (j=nsym-1; j>=0; j--) {
		      char *s = ODB_popstr();
		      /* The following symbol may still have "@"'s or "."'s [if ODB_NAME] */
		      int what = IS_HASH(s) ? ODB_HASHNAME : 
			  (IS_DOLLAR(s) ? ODB_USDNAME : (IS_BSNUM(s) ? ODB_BSNUM : ODB_NAME));
		      sdef->sel[0].sym[j] = ODB_new_symbol(what, s);
		    }
		  }
		  else { /* No symbols; just scalars (or functions) etc. */
		    sdef->sel[0].nsym = 0;
		    sdef->sel[0].sym = NULL;
		  }
		  if (!nick) {
		    char *f1 = dump_s(NULL,expr,3,NULL);
		    /* Skip over "_" if an aggregate function was in concern */
		    int add = is_aggr ? 1 : 0;
		    /* filter formula : change xxx_distinct(z) into xxx(distinct z) */
		    while ( (f = strstr(f1,"_distinct(")) != NULL ) { f[0] = '('; f[9] = ' '; }
		    /* filter formula : change count(1) into count(*) */
		    while ( (f = strstr(f1,"count(1)")) != NULL ) { f[6] = '*'; }
		    nick = ODB_new_symbol(ODB_NICKNAME, f1 + add);
		    sdef->sel[0].nicksym = nick;
		  }
		  sdef->readonly[0] = 1; /* Always unconditionally R/O */
		  sdef->select_items[0] = formula;
		}
		else {
		  for (j=0; j<n; j++) {
		    ODB_Symbol *psym = expr->argv[j];
		    char *s = psym->name;
		    /* fprintf(stderr,"YACC(from ARRNAME): argv[%d] -> '%s'\n",j,s); */
		    sdef->select_items[j] = STRDUP(s);
		    sdef->readonly[j] = readonly_flag;
		    sdef->sel[j].expr = NULL; /* by default */
		    sdef->sel[j].nicksym = nick ? nick : psym;
		  }
		}
		sdef->next = NULL;
		++colid;
		$$ = sdef;
	}
        | string opt_sel_param  {
                int mode = $2;
		Boolean readonly_flag =
		  IS_SET(mode,ODBURO_EMPTY) ? readonly_mode : IS_SET(mode,ODBREADONLY);
                Selectdef_t *sdef;
                const int n = 1;
                CALLOC(sdef, 1);
                sdef->nselect = n;
                ALLOC(sdef->select_items, n);
                ALLOC(sdef->readonly, n);
                CALLOC(sdef->sel, n);
                sdef->select_items[0] = STRDUP($1);
                sdef->readonly[0] = readonly_flag;
		if (IS_SET(mode,ODBAS)) { /* Nicknames (... but watch for regular expressions ) */
		  char *s = ODB_popstr();
		  ODB_Symbol *nick = ODB_new_symbol(ODB_NICKNAME, s);
		  sdef->sel[0].nicksym = nick;	    
		}
		else {
		  sdef->sel[0].nicksym = ODB_new_symbol(ODB_NICKNAME,ODB_NickName(1));
		}
		/* fprintf(stderr,"---> string='%s',  opt_sel_param=%d\n",$1,$2); */
                sdef->next = NULL;
                $$ = sdef;
        }
	;

number	: ODB_NUMBER		{ $$ = $1; }
	| USDname		{
		char *pname = $1;
		ODB_Symbol *name = ODB_lookup(ODB_USDNAME,pname,NULL);
		if (!name) {
		  SETMSG1("Variable '%s' has not been set",pname);
		  YYerror(msg);
		}
		$$ = name->dval;
	}
	;

const_expr	: num_expr { /* Must be able to evaluate into a number */
		  double numba = 0;
		  int irc = ODB_evaluate($1,&numba);
		  if (!irc) {
		     SETMSG0("Unable to evaluate an expression, which was assumed constant");
		     YYerror(msg);
		  }
		  $$ = numba;
		}
		;

arrindex: ODB_LB const_expr ODB_RB {
		double d = $2; double flr = floor(d);
		ODB_Arridx *a; ALLOC(a,1);
		if (d != flr || flr > INT_MAX || flr < -INT_MAX) {
		  SETMSG1("Invalid array index %g",d);
		  YYerror(msg);
		}
		a->low = flr;
		a->high = flr;
		a->inc = 1;
		a->only_low = 0;
		a->only_high = 1;
		$$ = a;
	}
	|  ODB_LB const_expr ODB_COLON const_expr ODB_RB {
		double low = $2; double flrlow = floor(low);
		double high = $4; double flrhigh = floor(high);
		ODB_Arridx *a; ALLOC(a,1);
		if (low  != flrlow  || flrlow  > INT_MAX || flrlow  < -INT_MAX ||
		    high != flrhigh || flrhigh > INT_MAX || flrhigh < -INT_MAX) {
		  SETMSG2("Invalid array indices %g:%g found",low,high);
		  YYerror(msg);
		}
		a->low = flrlow;
		a->high = flrhigh;
		a->inc = (flrhigh >= flrlow) ? 1 : -1;
		a->only_low = 0;
		a->only_high = 0;
		$$ = a;
	}
	|  ODB_LB const_expr ODB_COLON const_expr ODB_COLON const_expr ODB_RB {
		double low = $2; double flrlow = floor(low);
		double high = $4; double flrhigh = floor(high);
		double inc = $6; double flrinc = floor(inc);
		ODB_Arridx *a; ALLOC(a,1);
		if (low  != flrlow  || flrlow  > INT_MAX || flrlow  < -INT_MAX ||
		    high != flrhigh || flrhigh > INT_MAX || flrhigh < -INT_MAX ||
		    inc  != flrinc  || flrinc  > INT_MAX || flrinc  < -INT_MAX ||
		    flrinc == 0) {
		  SETMSG3("Invalid array indices %g:%g:%g found",low,high,inc);
		  YYerror(msg);
		}
		a->low = flrlow;
		a->high = flrhigh;
		a->inc = flrinc;
		a->only_low = 0;
		a->only_high = 0;
		$$ = a;
	}
	;

opt_table_name	:	/* empty */	{ $$ = STRDUP(""); }
		| name			{ $$ = $1;	   }
		;

arrname	: name			{ ODB_pushstr($1); $$ = 1; }
	| name arrindex	opt_table_name {
		ODB_Arridx *a = $2;
		int low = a->low, high = a->high, inc = a->inc;
		int j, count;
		int namelen = strlen($1) + strlen($3);
		count = 0;
		if (low <= high) {
		  inc = ABS(inc);
		  for (j=low; j<=high; j+=inc) {
		    char *s;
		    ALLOC(s, namelen + 30);
		    sprintf(s,"%s_%s%d%s",$1,(j<0) ? "_" : "", ABS(j),$3);
		    ODB_pushstr(s);
		    count++;
		  }
		} else {
		  inc = ABS(inc);
		  for (j=low; j>=high; j-=inc) {
		    char *s;
		    ALLOC(s, namelen + 30);
		    sprintf(s,"%s_%s%d%s",$1,(j<0) ? "_" : "", ABS(j), $3);
		    ODB_pushstr(s);
		    count++;
		  }
		}
		if (count == 0) {
		  SETMSG5("An empty array section definition : %s[%d:%d:%d]%s\n",
			  $1,a->low,a->high,a->inc,$3);
		  YYerror(msg);
		}
		$$ = count;
	}
	;

sortlist: /* empty */			{ $$ = 0; }
	| arrsort			{ $$ = $1; }
	| sortlist ODB_COMMA arrsort	{ $$ = $1 + $3; }
	| sortlist ODB_COMMA		{ $$ = $1; }
	;

arrsort	: arrname opt_ascdesc { 
		int j, n = $1;
		char **s; ALLOC(s,n);
		for (j=n-1; j>=0; j--) s[j] = ODB_popstr();
		for (j=0; j<n; j++) {
		  ODB_pushi($2 * YACC_PlusOne);
		  ODB_pushstr(s[j]); 
		}
		FREE(s);
		$$ = n; 
	}
	| ODB_ADD arrname {  
		int j, n = $2;
		char **s; ALLOC(s,n);
		for (j=n-1; j>=0; j--) s[j] = ODB_popstr();
		for (j=0; j<n; j++) {
		  ODB_pushi(YACC_PlusOne);
		  ODB_pushstr(s[j]); 
		}
		FREE(s);
		$$ = n; 
	}
	| ODB_SUB arrname {  
		int j, n = $2;
		char **s; ALLOC(s,n);
		for (j=n-1; j>=0; j--) s[j] = ODB_popstr();
		for (j=0; j<n; j++) {
		  ODB_pushi(YACC_MinusOne);
		  ODB_pushstr(s[j]); 
		}
		FREE(s);
		$$ = n; 
	}
	| arrname ODB_ABS opt_ascdesc {  
		int j, n = $1;
		char **s; ALLOC(s,n);
		for (j=n-1; j>=0; j--) s[j] = ODB_popstr();
		for (j=0; j<n; j++) {
		  ODB_pushi($3 * ODB_maxcols());
		  ODB_pushstr(s[j]); 
		}
		FREE(s);
		$$ = n; 
	}
	| ODB_ABS ODB_LP arrname ODB_RP opt_ascdesc {  
		int j, n = $3;
		char **s; ALLOC(s,n);
		for (j=n-1; j>=0; j--) s[j] = ODB_popstr();
		for (j=0; j<n; j++) {
		  ODB_pushi($5 * ODB_maxcols());
		  ODB_pushstr(s[j]); 
		}
		FREE(s);
		$$ = n; 
	}
	| const_expr {
		double d = $1;
		char *s; ALLOC(s,50); sprintf(s,"%.20g",ABS(d)); 
		ODB_pushi((d >= 0) ? YACC_PlusOne : YACC_MinusOne); 
		ODB_pushstr(s);
		$$ = 1;
	}
	| const_expr ODB_ABS {
		double d = $1;
		char *s; ALLOC(s,50); sprintf(s,"%.20g",ABS(d)); 
		ODB_pushi(((d >= 0) ? YACC_PlusOne : YACC_MinusOne) * ODB_maxcols()); 
		ODB_pushstr(s);
		$$ = 1;
	}
	| ODB_ABS ODB_LP const_expr ODB_RP {
		double d = $3;
		char *s; ALLOC(s,50); sprintf(s,"%.20g",ABS(d)); 
		ODB_pushi(((d >= 0) ? YACC_PlusOne : YACC_MinusOne) * ODB_maxcols()); 
		ODB_pushstr(s);
		$$ = 1;
	}
	| const_expr ascdesc {
		double d = $1;
		int ad = $2;
		char *s; 
		if (d < 0) {
		  SETMSG2("The value itself (%g) in ORDERBY-list cannot be negative when using %s",
			  d, (ad == 1) ? "ASC" : "DESC");
		  YYerror(msg);
		}
		ALLOC(s,50); sprintf(s,"%.20g",d); 
		ODB_pushi((ad == 1) ? YACC_PlusOne : YACC_MinusOne);
		ODB_pushstr(s);
		$$ = 1;
	}
	| const_expr ODB_ABS ascdesc {
		double d = $1;
		int ad = $3;
		char *s; 
		if (d < 0) {
		  SETMSG2("The value itself (%g) in ORDERBY-list cannot be negative when using %s",
			  d, (ad == 1) ? "ASC" : "DESC");
		  YYerror(msg);
		}
		ALLOC(s,50); sprintf(s,"%.20g",d); 
		ODB_pushi(((ad == 1) ? YACC_PlusOne : YACC_MinusOne) * ODB_maxcols());
		ODB_pushstr(s);
		$$ = 1;
	}
	| ODB_ABS ODB_LP const_expr ODB_RP ascdesc {
		double d = $3;
		int ad = $5;
		char *s; 
		if (d < 0) {
		  SETMSG2("The value itself (%g) in ORDERBY-list cannot be negative when using %s",
			  d, (ad == 1) ? "ASC" : "DESC");
		  YYerror(msg);
		}
		ALLOC(s,50); sprintf(s,"%.20g",d); 
		ODB_pushi(((ad == 1) ? YACC_PlusOne : YACC_MinusOne) * ODB_maxcols());
		ODB_pushstr(s);
		$$ = 1;
	}
	;

opt_ascdesc  : /* empty */ { $$ =  1; }
	     | ascdesc	   { $$ = $1; }	
	     ;

ascdesc	  : ODB_ASC	   { $$ =  1; }	
	  | ODB_DESC	   { $$ = -1; }
	  ;

decllist: /* empty */			{ $$ = 0; }
	| decl				{ $$ = $1; }
	| decllist ODB_COMMA decl	{ $$ = $1 + $3; }
	| decllist ODB_COMMA		{ $$ = $1; }
	| ODB_LP decllist ODB_RP	{ $$ = $2; }
	; 

decl	: name type		{
		ODB_Type   *type = ODB_new_type($2, 1);
		ODB_Symbol *name = ODB_new_symbol(ODB_NAME, $1);
		ODB_pushstr($2); /* type string to stack */
		ODB_pushstr($1); /* member name string to stack */
		$$ = 1; 
	}
	| name arrindex type {
		ODB_Arridx *a = $2;
		int low = a->low, high = a->high, inc = a->inc;
		int j, count;
		int  namelen = strlen($1);
		ODB_Type *type = ODB_new_type($3, 1);
		count = 0;
		if (a->only_high) { /* A special case */
		  if (high > 0)      { low =  1; inc =  1; }
		  else if (high < 0) { low = -1; inc = -1; }
		  else /* error */   { 
		    SETMSG2("A zero length type definition with array section : %s %s[0]\n",
			   $3, $1);
		    YYerror(msg);
		  }
		}
		if (low <= high) {
		  inc = ABS(inc);
		  for (j=low; j<=high; j+=inc) {
		    char *s;
		    ALLOC(s, namelen + 30);
		    sprintf(s,"%s_%s%d",$1,(j<0) ? "_" : "", ABS(j));
		    (void) ODB_new_symbol(ODB_NAME, s);
		    ODB_pushstr($3); /* type string to stack */
		    ODB_pushstr(s);  /* member name string to stack */
		    count++;
		  }
		} else {
		  inc = ABS(inc);
		  for (j=low; j>=high; j-=inc) {
		    char *s;
		    ALLOC(s, namelen + 30);
		    sprintf(s,"%s_%s%d",$1,(j<0) ? "_" : "", ABS(j));
		    (void) ODB_new_symbol(ODB_NAME, s);
		    ODB_pushstr($3); /* type string to stack */
		    ODB_pushstr(s);  /* member name string to stack */
		    count++;
		  }
		}
		if (count == 0) {
		  SETMSG2("An empty type definition with array section : %s %s[???]\n",
			  $3, $1);
		  YYerror(msg);
		}
		$$ = count;
	}
	;

expr	: ODB_LP expr ODB_RP    { $$ = $2; }
	| ODB_NUMBER		{ $$ = ODBOPER1(ODB_NUMBER,&$1); }
        | USDname		{ $$ = ODBOPER1(ODB_USDNAME,$1); }
	| inside		{ $$ = $1; }
	| near			{ $$ = $1; }
	| strcmpre		{ $$ = $1; }
        | HASHname		{ $$ = ODBOPER1(ODB_HASHNAME,$1); }
        | BSnum			{ $$ = ODBOPER1(ODB_BSNUM,$1); }
	| strfunc1		{ $$ = $1; }
	| name ODB_LP arglist ODB_RP opt_table_name { 
	  /* looking for possible linkoffset/-len(var)@table_name */
	  char *tblname = $5;
	  int tblen = STRLEN(tblname);
	  int numargs = $3;
	  if (tblen > 0) {
	    char *fname = $1;
	    int errcnt = 0;
	    if (strequ(fname,"linklen") || strequ(fname,"linkoffset")) {
	      ODB_Tree *expr = ODB_popexpr();
	      if (numargs == 1 && expr && expr->what == ODB_NAME && expr->argv) {
		ODB_Symbol *psym = expr->argv[0];
		char *s = psym->name;
		char *snew;
		int slen = strlen(s) + 1 + tblen + 1;
		ALLOC(snew, slen);
		snprintf(snew,slen,"%s%s%s",s,IS_TABLE(tblname) ? "" : "@",tblname);
		expr = ODBOPER1(ODB_NAME, snew);
		ODB_pushexpr(expr); /* back to stack */
	      }
	      else
		++errcnt;
	    }
	    else
	      ++errcnt;
	    if (errcnt > 0) {
	      SETMSG2("If parent table name is given, then the syntax must be : %s or %s",
		      "linkoffset(child_table)@parent",
		      "linklen(child_table)@parent");
	      YYerror(msg);
	    }
	  }
	  $$ = ODBOPER2(ODB_FUNC,$1,&numargs); 
        }
        | expr ODB_ADD expr	{ $$ = ODBOPER2(ODB_ADD,$1,$3); }
        | expr ODB_SUB expr	{ $$ = ODBOPER2(ODB_SUB,$1,$3); }
        | expr ODB_STAR expr	{ $$ = ODBOPER2(ODB_STAR,$1,$3); }
        | expr ODB_DIV expr	{ $$ = ODBOPER2(ODB_DIV,$1,$3); }
	| expr ODB_MODULO expr { 
		int numargs = 2;
		ODB_pushexpr($1); ODB_pushexpr($3);
		$$ = ODBOPER2(ODB_FUNC,"mod",&numargs); }
	| expr ODB_POWER expr { 
		int numargs = 2;
		ODB_pushexpr($1); ODB_pushexpr($3);
		$$ = ODBOPER2(ODB_FUNC,"pow",&numargs); }
	| ODB_ADD expr %prec ODB_UNARY_PLUS  { 
		$$ = $2;
	}
	| ODB_SUB expr %prec ODB_UNARY_MINUS { 
		if (ODB_fixconv($2)) {
		  $$ = $2;
		}
		else {
		  $$ = ODBOPER1(ODB_UNARY_MINUS,$2); 
		}
	}
        ;

arglist	: /* empty */		 { $$ = 0; }
	| cond			 { ODB_pushexpr($1);   $$ = 1; }
	| arglist ODB_COMMA cond { ODB_pushexpr($3);   $$ = $1 + 1; }
	;

exprlist: expr			  { ODB_pushexpr($1);   $$ = 1; }
	| exprlist ODB_COMMA expr { ODB_pushexpr($3);   $$ = $1 + 1; }
	;

matchfunc : ODB_MATCH cond ODB_RP {
	    ODB_Match_t *match = NULL;
	    CALLOC(match, 1);
	    match->expr = $2;
	    match->formula = dump_s(NULL,match->expr,0,NULL);
	    match->symexpr = ODBOPER1(ODB_STRING,match->formula);
	    $$ = match;
	  }
	  ;

num_expr: ODB_LP num_expr ODB_RP    { $$ = $2; }
	| number		    { $$ = ODBOPER1(ODB_NUMBER,&$1); }
	| name ODB_LP num_arglist ODB_RP { $$ = ODBOPER2(ODB_FUNC,$1,&$3); }
        | num_expr ODB_ADD num_expr	{ $$ = ODBOPER2(ODB_ADD,$1,$3); }
        | num_expr ODB_SUB num_expr	{ $$ = ODBOPER2(ODB_SUB,$1,$3); }
        | num_expr ODB_STAR num_expr	{ $$ = ODBOPER2(ODB_STAR,$1,$3); }
        | num_expr ODB_DIV num_expr	{ $$ = ODBOPER2(ODB_DIV,$1,$3); }
	| num_expr ODB_MODULO num_expr { 
		int numargs = 2;
		ODB_pushexpr($1); ODB_pushexpr($3);
		$$ = ODBOPER2(ODB_FUNC,"mod",&numargs); }
	| num_expr ODB_POWER num_expr { 
		int numargs = 2;
		ODB_pushexpr($1); ODB_pushexpr($3);
		$$ = ODBOPER2(ODB_FUNC,"pow",&numargs); }
	| ODB_ADD num_expr %prec ODB_UNARY_PLUS  { 
		$$ = $2;
	}
	| ODB_SUB num_expr %prec ODB_UNARY_MINUS { 
		if (ODB_fixconv($2)) {
		  $$ = $2;
		}
		else {
		  $$ = ODBOPER1(ODB_UNARY_MINUS,$2); 
		}
	}
        ;

num_arglist : /* empty */		 { $$ = 0; }
	| num_expr			 { ODB_pushexpr($1);   $$ = 1; }
	| num_arglist ODB_COMMA num_expr { ODB_pushexpr($3);   $$ = $1 + 1; }
	;

formula	: selexpr			{ $$ = $1; }
	| selpred			{ $$ = $1; }
	;

selpred	: ODB_LP selpred ODB_RP		{ $$ = $2; }
	| selexpr is_null_or_is_not_null		{
		int is_what = $2 ? ODB_EQ : ODB_NE;
		ODB_Tree *arg = $1;
		int nargs = 1;
		ODB_pushexpr(arg);
		{
		  ODB_Tree *lhs = ODBOPER2(ODB_FUNC,"abs",&nargs);
		  char *pname = "$mdi";
		  ODB_Symbol *name = ODB_lookup(ODB_USDNAME,pname,NULL);
		  double mdi = name ? ABS(name->dval) : ABS(RMDI);
		  ODB_Tree *rhs = ODBOPER1(ODB_NUMBER,&mdi);
		  $$ = ODBOPER2(is_what, lhs, rhs);
		}
	}
	| ODB_NOT formula			{ $$ = ODBOPER1(ODB_NOT,$2); }
	| formula ODB_AND formula		{ $$ = ODBOPER2(ODB_AND,$1,$3); }
	| formula ODB_OR formula		{ $$ = ODBOPER2(ODB_OR,$1,$3); }
	| selexpr ODB_BETWEEN selexpr ODB_AND selexpr { $$ = ODBOPER3(ODB_LELE,$3,$1,$5); }
	| selexpr ODB_GT selexpr		{ $$ = ODBOPER2(ODB_GT,$1,$3); }
	| selexpr ODB_GE selexpr		{ $$ = ODBOPER2(ODB_GE,$1,$3); }
	| selexpr eqne selexpr			{ $$ = ODBOPER2($2 ? ODB_EQ : ODB_NE,$1,$3); }
	| selexpr ODB_IS selexpr		{ $$ = ODBOPER2(ODB_EQ,$1,$3); }
	| selexpr ODB_LE selexpr		{ $$ = ODBOPER2(ODB_LE,$1,$3); }
	| selexpr ODB_LT selexpr		{ $$ = ODBOPER2(ODB_LT,$1,$3); }
	| selexpr ODB_CMP selexpr		{ 
	  int numargs = 2;
	  ODB_pushexpr($1); ODB_pushexpr($3);
	  $$ = ODBOPER2(ODB_FUNC,"cmp",&numargs); 
	}
	| selexpr ODB_IS ODB_NOT selexpr	{ $$ = ODBOPER2(ODB_NE,$1,$4); }
	| selexpr ODB_GT selexpr ODB_GT selexpr	{ $$ = ODBOPER3(ODB_GTGT,$1,$3,$5); }
	| selexpr ODB_GT selexpr ODB_GE selexpr	{ $$ = ODBOPER3(ODB_GTGE,$1,$3,$5); }
	| selexpr ODB_GE selexpr ODB_GE selexpr	{ $$ = ODBOPER3(ODB_GEGE,$1,$3,$5); }
	| selexpr ODB_GE selexpr ODB_GT selexpr	{ $$ = ODBOPER3(ODB_GEGT,$1,$3,$5); }
	| selexpr ODB_LT selexpr ODB_LT selexpr	{ $$ = ODBOPER3(ODB_LTLT,$1,$3,$5); }
	| selexpr ODB_LT selexpr ODB_LE selexpr	{ $$ = ODBOPER3(ODB_LTLE,$1,$3,$5); }
	| selexpr ODB_LE selexpr ODB_LE selexpr	{ $$ = ODBOPER3(ODB_LELE,$1,$3,$5); }
	| selexpr ODB_LE selexpr ODB_LT selexpr	{ $$ = ODBOPER3(ODB_LELT,$1,$3,$5); }
	| selexpr in_notin ODB_LP formula ODB_COLON formula opt_selinc ODB_RP {
	  ODB_Tree *target = $1;
	  Boolean is_in = $2;
	  ODB_Tree *begin = $4;
	  ODB_Tree *end = $6;
	  ODB_Tree *step = $7;
	  int j, n = 4;
	  ODB_Tree **args;
	  ALLOC(args, n);
	  args[0] = target;
	  args[1] = begin;
	  args[2] = end;
	  args[3] = step;
	  for (j=0; j<n; j++) ODB_pushexpr(args[j]);
	  {
	    ODB_Tree *f = ODBOPER2(ODB_FUNC,"InGenList",&n);
	    $$ = is_in ? f : ODBOPER1(ODB_NOT,f);
	  }
	  FREE(args);
	}
	| selexpr in_notin ODB_LP selarglist ODB_RP {
	  Boolean is_in = $2;
	  int numargs = $4;
	  if (numargs >= 1) {
	    ODB_Tree *target = $1;
	    $$ = ODBOPER2(is_in?ODB_IN:ODB_NOTIN,target,&numargs);
	  }
	  else { /* No args ==> warning */
	    double numba = is_in ? 0 : 1; /* always false/true */
	    SETMSG1("expression %s ( )-statement expects at least one argument.",
		    is_in ? "IN" : "NOTIN");
	    YYwarn(0,msg);
	    $$ = ODBOPER1(ODB_NUMBER, &numba);
	  }
	}
	| formula ODB_QMARK formula ODB_COLON formula { $$ = ODBOPER3(ODB_COND,$1,$3,$5); }
	;

strfunc1: name ODB_LP string ODB_RP { $$ = ODBOPER2(ODB_STRFUNC1, $1, $3); }
	;

selexpr	: ODB_LP selexpr ODB_RP	        { $$ = $2; }
	| selinside			{ $$ = $1; }
	| selnear			{ $$ = $1; }
	| strcmpre			{ $$ = $1; }
	| strfunc1			{ $$ = $1; }
	| name ODB_LP selarglist ODB_RP	opt_table_name { 
	  /* looking for possible linkoffset/-len(var)@table_name */
	  char *tblname = $5;
	  int tblen = STRLEN(tblname);
	  int numargs = $3;
	  if (tblen > 0) {
	    char *fname = $1;
	    int errcnt = 0;
	    if (strequ(fname,"linklen") || strequ(fname,"linkoffset")) {
	      ODB_Tree *expr = ODB_popexpr();
	      if (numargs == 1 && expr && expr->what == ODB_NAME && expr->argv) {
		ODB_Symbol *psym = expr->argv[0];
		char *s = psym->name;
		char *snew;
		int slen = strlen(s) + 1 + tblen + 1;
		ALLOC(snew, slen);
		snprintf(snew,slen,"%s%s%s",s,IS_TABLE(tblname) ? "" : "@",tblname);
		expr = ODBOPER1(ODB_NAME, snew);
		ODB_pushexpr(expr); /* back to stack */
	      }
	      else
		++errcnt;
	    }
	    else
	      ++errcnt;
	    if (errcnt > 0) {
	      SETMSG2("If parent table name is given, then the syntax must be : %s or %s",
		      "linkoffset(child_table)@parent",
		      "linklen(child_table)@parent");
	      YYerror(msg);
	    }
	  }
	  $$ = ODBOPER2(ODB_FUNC,$1,&numargs); 
	}
	| name ODB_LP ODB_DISTINCT selarglist ODB_RP { 
	    $$ = ODBOPER3(ODB_FUNC,$1,&$4,&YACC_distinct); }
	| name ODB_LP ODB_STAR ODB_RP   { 
	    int numargs = 0;
	    $$ = ODBOPER2(ODB_FUNC,$1,&numargs); }
        | USDname			{ $$ = ODBOPER1(ODB_USDNAME,$1); }
        | HASHname			{ $$ = ODBOPER1(ODB_HASHNAME,$1); }
        | BSnum				{ $$ = ODBOPER1(ODB_BSNUM,$1); }
        | ODB_NUMBER			{ $$ = ODBOPER1(ODB_NUMBER,&$1); }
        | selexpr ODB_ADD selexpr	{ $$ = ODBOPER2(ODB_ADD,$1,$3); }
        | selexpr ODB_SUB selexpr	{ $$ = ODBOPER2(ODB_SUB,$1,$3); }
        | selexpr ODB_STAR selexpr	{ $$ = ODBOPER2(ODB_STAR,$1,$3); }
        | selexpr ODB_DIV selexpr	{ $$ = ODBOPER2(ODB_DIV,$1,$3); }
	| selexpr ODB_MODULO selexpr { 
		int numargs = 2;
		ODB_pushexpr($1); ODB_pushexpr($3);
		$$ = ODBOPER2(ODB_FUNC,"mod",&numargs); }
	| selexpr ODB_POWER selexpr { 
		int numargs = 2;
		ODB_pushexpr($1); ODB_pushexpr($3);
		$$ = ODBOPER2(ODB_FUNC,"pow",&numargs); }
	| ODB_ADD selexpr %prec ODB_UNARY_PLUS  { 
		$$ = $2;
	}
	| ODB_SUB selexpr %prec ODB_UNARY_MINUS { 
		if (ODB_fixconv($2)) {
		  $$ = $2;
		}
		else {
		  $$ = ODBOPER1(ODB_UNARY_MINUS,$2); 
		}
	}
	| selexpr ODB_DOTP selexpr {
		int numargs = 2;
		ODB_pushexpr($1); ODB_pushexpr($3);
		$$ = ODBOPER2(ODB_FUNC,"dotp",&numargs);
	}	
	| ODB_NORM selexpr ODB_NORM { /* | x | i.e. sqrt(dotp(x,x)) */
		int numargs = 2;
		ODB_pushexpr($2); ODB_pushexpr($2);
		$$ = ODBOPER2(ODB_FUNC,"norm",&numargs);
	}	
	| ODB_NORM selexpr ODB_COMMA selexpr ODB_NORM { /* | x, y | or i.e. sqrt(x .* x) */
		int numargs = 2;
		ODB_pushexpr($2); ODB_pushexpr($4);
		$$ = ODBOPER2(ODB_FUNC,"norm",&numargs);
	}	
        ;

selarglist : /* empty */		 { $$ = 0; }
	| formula			 { ODB_pushexpr($1);   $$ = 1; }
	| selarglist ODB_COMMA formula	 { ODB_pushexpr($3);   $$ = $1 + 1; }
	;

alignlist : /* empty */			{ $$ = 0; }
	| arrname			{ $$ = $1; }
	| alignlist ODB_COMMA arrname	{ $$ = $1 + $3; }
	;

USDname	: ODB_USDNAME		{ $$ = $1; }
	;

extUSDname : USDname		{ $$ = $1; }
	   | name		{
	     char *s = $1;
	     int slen;
	     /* Check that no '.' or '@' exist in the "name",
	        since they are invalid in USDname-proper */
	     if (strchr(s,'.') || strchr(s,'@')) {
		SETMSG1("Parameter '%s' cannot contain '.' or '@' characters in this context",s);
		YYerror(msg);
	     }
	     /* Now prepend with a 'dollar'-sign and return */
	     slen = strlen(s);
	     ALLOC(s,slen+2);
	     sprintf(s,"$%s",$1);
	     $$ = s; 
	   }
	;

HASHname: ODB_HASHNAME		{ $$ = $1; }
	;

BSnum	: ODB_BSNUM		{ $$ = $1; }
	;

string	: ODB_STRING		{ $$ = $1; }
	| string ODB_STRING	{
	  int len = STRLEN($1) + STRLEN($2) + 1;
	  char *s = NULL;
	  ALLOC(s, len);
	  snprintf(s,len,"%s%s",$1,$2);
	  $$ = s;	  
	}
	;

opt_arglist : /* empty */       { $$ =  0; }
	    | ODB_COMMA	arglist { $$ = $2; }
	    ;

query	: ODB_SELECT string ODB_RP {
	  /* Usage f.ex. : obstype IN ( SELECT "distinct obstype FROM hdr" ) */
	  int len = STRLEN("SELECT ") + STRLEN($2) + 1;
	  char *poolmask = "-1";
	  char *db = getenv("ODB98_DBPATH"); /* see odb/scripts/odbsql */
	  char *query = NULL;
	  int numargs = 0;
	  ALLOC(query, len);
	  snprintf(query,len,"SELECT %s",$2);
	  $$ = ODBOPER4(ODB_QUERY, poolmask, db, query, &numargs);
	}
	| ODB_QUERY string opt_arglist ODB_RP {
	  /* Usage f.ex. : a IN query("set $x=1; select a from t where c = $x") */
	  char *poolmask = "-1";
	  char *db = getenv("ODB98_DBPATH"); /* see odb/scripts/odbsql */
	  char *query = $2;
	  int numargs = $3;
	  $$ = ODBOPER4(ODB_QUERY, poolmask, db, query, &numargs);
	}
	| ODB_QUERY string ODB_COMMA string opt_arglist ODB_RP {
	  /* Usage f.ex. : codetype IN query("/data/base/XYZ", "select codetype from hdr") */
	  /*      or     : codetype IN query("1-10,16,20", "select codetype from hdr") */
	  char *arg2 = $2;
	  Boolean poolmask_given = (*arg2 == '-' || *arg2 == ',' || isdigit(*arg2)) ? 1 : 0;
	  char *poolmask = poolmask_given ? arg2 : "-1";
	  char *db = poolmask_given ? getenv("ODB98_DBPATH") : arg2;
	  char *query = $4;
	  int numargs = $5;
	  $$ = ODBOPER4(ODB_QUERY, poolmask, db, query, &numargs);
	}
	| ODB_QUERY const_expr ODB_COMMA string ODB_COMMA string opt_arglist ODB_RP {
	  /* Usage f.ex. : sensor IN query(7, "/data/base/XYZ", "select distinct sensor from hdr") */
	  char poolmask[80];
	  double poolno = $2;
	  char *db = $4;
	  char *query = $6;
	  int numargs = $7;
	  snprintf(poolmask,sizeof(poolmask),"%.0f", poolno);
	  $$ = ODBOPER4(ODB_QUERY, poolmask, db, query, &numargs);
	}
	| ODB_QUERY string ODB_COMMA string ODB_COMMA string opt_arglist ODB_RP {
	  /* Usage f.ex. : sensor IN query("1-10,16,20", "/data/base/XYZ", "select distinct sensor from hdr") */
	  char *poolmask = $2;
	  char *db = $4;
	  char *query = $6;
	  int numargs = $7;
	  $$ = ODBOPER4(ODB_QUERY, poolmask, db, query, &numargs);
	}
	| ODB_QUERY const_expr ODB_COMMA string opt_arglist ODB_RP {
	  /* Usage f.ex. : a IN query(7, "set $x=1; select a from t where c = $x") */
	  char poolmask[80];
	  double poolno = $2;
	  char *db = getenv("ODB98_DBPATH"); /* see odb/scripts/odbsql */
	  char *query = $4;
	  int numargs = $5;
	  snprintf(poolmask,sizeof(poolmask),"%.0f", poolno);
	  $$ = ODBOPER4(ODB_QUERY, poolmask, db, query, &numargs);
	}
	;

type	: ODB_NAME		{ $$ = $1; }
	| ODB_TYPEOF ODB_LP name ODB_RP {
	  char *arg = $3;
	  char *x = NULL;
	  char *s = STRDUP(arg);
	  char *tblname = strchr(s,'@');
	  ODB_Table *ptable = NULL;
	  if (tblname) {
	    *tblname++ = '\0';
	    ptable = ODB_lookup_table(tblname,NULL);
	    if (!ptable) {
	      SETMSG2("Reference to undefined table '%s' in TYPEOF(%s)",tblname,arg);
	      YYerror(msg);
	    }
	    else {
	      int index = -1;
	      if (ODB_in_table(ODB_NAME, s, ptable, &index) && index >= 0) {
	        x = STRDUP(ptable->type[index]->type->name);
	      }
	      else {
	        SETMSG3("Unable to locate item '%s' from table '%s' in TYPEOF(%s)",s,tblname,arg);
	        YYerror(msg);
	      }
	    }
	  }
	  if (!ptable) { /* Find table */
	    int count = 0;
	    for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	      if (ODB_in_table(ODB_NAME, s, ptable, NULL)) count++;
	    }
	    if (count == 0) {
	      SETMSG2("Unable to locate table for item '%s' in TYPEOF(%s)",s,arg);
	      YYerror(msg);
	    }
	    if (count > 1) {
	      SETMSG3("Item '%s' in TYPEOF(%s) found in %d tables, but must be unambiguous",s,arg,count);
	      YYerror(msg);
	    }
	    for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	      int index = -1;
	      if (ODB_in_table(ODB_NAME, s, ptable, &index) && index >= 0) {
	        x = STRDUP(ptable->type[index]->type->name);
	        break;
	      }
	    }
	    if (!ptable) {
	      SETMSG2("Unable to locate table for item '%s' in TYPEOF(%s)",s,arg);
	      YYerror(msg);
	    }
	  }
	  FREE(s);
	  if (!x) {
	    SETMSG1("Cannot resolve item or table in TYPEOF(%s)",arg);
	    YYerror(msg);
	  }
	  $$ = x;
	}
	;

name	: ODB_NAME		{ $$ = STRDUP($1); }
	;

align	: ODB_ALIGN		{ $$ = '@'; }
	| ODB_ONELOOPER		{ $$ = '='; }
	| ODB_SHAREDLINK	{ $$ = '&'; }
	;

is_null_or_is_not_null	: ODB_IS ODB_NULL	  { $$ = 1; }
			| ODB_IS ODB_NOT ODB_NULL { $$ = 0; }
			;

idxname : name	      { $$ = $1; }
	;

opt_idxname : /* empty */ { $$ = NULL; }
	    | idxname     { $$ = $1; }
	    | ODB_STAR    { $$ = STRDUP("*"); }
	    | string      { $$ = $1; /* Already STRDUP'ped in lex.l */ }
	    ;

opt_using   : /* empty */ { $$ = 0; }
	    | ODB_USING	  { $$ = 1; }
	    ;

dbcred : string      { $$ = $1; /* Already STRDUP'ped in lex.l */ }
       | name	     { $$ = $1; }
       ;
