
/* defs.h */

#include <stdio.h>
#include <ctype.h>
#include <string.h>
#ifdef USE_MALLOC_H
#include <malloc.h>
#endif
#include "ecstdlib.h"
#include <stdarg.h>
#include <unistd.h>
#include <math.h>
#include <limits.h>
#include <signal.h>
#ifdef HPPA
#include <sys/time.h>
#else
#include <sys/select.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>

#include "alloc.h"

#include "odbcrc.h"
#include "odbmd5.h"

#if 0

/* These are here to *FOOL* the make-depend system
   and to enforce rebuild of ODB/SQL-compiler,
   when one of these files below has changed */

#include "odb.h"
#include "odb_macros.h"
#include "dca.h"
#include "idx.h"
#include "info.h"
#include "odb98.h"

#endif


#define ODB_H "$ODB_SYSPATH/odb.h"

#define exit ODB_exit

#define FOPEN(name, mode)    ODB_fopen(name, mode, __FILE__, __LINE__)
#define FCLOSE(fp)           if (fp) { ODB_fclose(fp, __FILE__, __LINE__); fp = NULL; }
#define FREWIND(fp)          if (fp) { rewind(fp); }

#define UINT "uint"
#define BITFIELD "Bitfield"
#define STRING "string"
#define UNUSED "Unused"

#ifndef MAXLINE
#define MAXLINE    1024
#endif

#define ODB_FROM_ATTR_USER   0x1
#define ODB_FROM_ATTR_AUTO   0x2
#define ODB_FROM_ATTR_INSERT 0x4

#define ODB_BAILOUT_ALL      (-1)

#define ODB_BAILOUT_SELECT     0x01 /*  1 */
#define ODB_BAILOUT_SELECTEXPR 0x02 /*  2 */
#define ODB_BAILOUT_UNIQUEBY   0x04 /*  4 */
#define ODB_BAILOUT_FROM       0x08 /*  8 */
#define ODB_BAILOUT_WHERE      0x10 /* 16 */
#define ODB_BAILOUT_ORDERBY    0x20 /* 32 */

#define ODB_SCALE 10000

#define ODB_GTGT  ODB_GT * ODB_SCALE + ODB_GT
#define ODB_GTGE  ODB_GT * ODB_SCALE + ODB_GE
#define ODB_GEGE  ODB_GE * ODB_SCALE + ODB_GE
#define ODB_GEGT  ODB_GE * ODB_SCALE + ODB_GT

#define ODB_LTLT  ODB_LT * ODB_SCALE + ODB_LT
#define ODB_LTLE  ODB_LT * ODB_SCALE + ODB_LE
#define ODB_LELE  ODB_LE * ODB_SCALE + ODB_LE
#define ODB_LELT  ODB_LE * ODB_SCALE + ODB_LT

#define IS_SET(mode,val) ( ((mode)&(val)) == (val) )

#define IS_LINK(s)    strequ(s,"Link")
#define IS_VARCHAR(s) (isalnum(*s) || (*s == '_'))
#define IS_WILDCARD(s) (strstr(s,"*@") != NULL)
#define IS_REGEX(s, slen)  ( \
			    ((slen > 2) && (s[0] == '/' && s[slen-1] == '/')) || \
			    ((slen > 3) && (strnequ(s,"!/",2) && s[slen-1] == '/')) || \
			    ((slen > 3) && (strnequ(s,"~/",2) && s[slen-1] == '/')) || \
			    ((slen > 3) && (s[0] == '/' && strncaseequ(&s[slen-2],"/i",2))) || \
			    ((slen > 4) && (strnequ(s,"!/",2) && strncaseequ(&s[slen-2],"/i",2))) || \
			    ((slen > 4) && (strnequ(s,"~/",2) && strncaseequ(&s[slen-2],"/i",2))) \
			   )

#define ARTIFICIAL_NICKCHAR '\a'
#define HAS_NICKNAME(sym) (sym && sym->name && (*sym->name != ARTIFICIAL_NICKCHAR) \
                               && (sym->kind == ODB_NICKNAME))

static const char FORMULA_CHAR[] = "\b";
#define IS_FORMULA(s) (s && *s == *FORMULA_CHAR)

#define LINKOFFSETTYPE "linkoffset_t"
#define LINKLENTYPE    "linklen_t"

#define BETWEEN(x,a,b) ( ((x) >= (a)) && ((x) <= (b)) )

#define ZEROIT(x,n1,n2) { int l; for(l=(n1); l<(n2); l++) (x)[l] = 0; }

#define EMPTY_MEMBER "/* an empty member */"

PUBLIC void odb_warn(char *errormsg, int rc);
PUBLIC void yyerror(char *errormsg);

PRIVATE char msg[MAXLINE];

#define PRTMSG(s) \
{ \
  extern char *odb_source;\
  extern int ODB_lineno; \
  if (odb_source && ODB_lineno > 0) { \
    fprintf(stderr,"\"%s\":%d [%s:%d] : ",odb_source, ODB_lineno, __FILE__, __LINE__); \
  } else { \
    fprintf(stderr,"[%s:%d] : ", __FILE__, __LINE__); \
  } \
  fprintf(stderr,"%s",s);			\
}

#define PRTMSG3(s,filename,lineno) \
{ \
  extern char *odb_source;\
  extern int ODB_lineno; \
  if (odb_source && ODB_lineno > 0) { \
    fprintf(stderr,"\"%s\":%d [%s:%d called from %s:%d] : ",odb_source, ODB_lineno, \
            __FILE__, __LINE__, \
            filename, lineno); \
  } else { \
    fprintf(stderr,"[%s:%d called from %s:%d] : ", \
            __FILE__, __LINE__, \
            filename, lineno); \
  } \
  fprintf(stderr,"%s",s);			\
}

#define YYwarn(rc,s) { \
  if (s != NULL) { \
    int len=strlen(s); \
    PRTMSG(s); \
    if (s[len-1] != '\n') fprintf(stderr,"\n"); \
  } \
  odb_warn(NULL,rc); \
}

#define YYwarn3(rc,s,filename,lineno) { \
  if (s != NULL) { \
    int len=strlen(s); \
    PRTMSG3(s,filename,lineno); \
    if (s[len-1] != '\n') fprintf(stderr,"\n"); \
  } \
  odb_warn(NULL,rc); \
}

#define YYerror(s) { \
  if (s != NULL) { \
    int len=strlen(s); \
    PRTMSG(s); \
    if (s[len-1] != '\n') fprintf(stderr,"\n"); \
  } \
  yyerror(NULL); \
}

#define YYerror3(s,filename,lineno) { \
  if (s != NULL) { \
    int len=strlen(s); \
    PRTMSG3(s,filename,lineno); \
    if (s[len-1] != '\n') fprintf(stderr,"\n"); \
  } \
  yyerror(NULL); \
}

#define SETMSG0(s) sprintf(msg,(s))
#define SETMSG1(s,var1) sprintf(msg,(s),(var1))
#define SETMSG2(s,var1,var2) sprintf(msg,(s),(var1),(var2))
#define SETMSG3(s,var1,var2,var3) sprintf(msg,(s),(var1),(var2),(var3))
#define SETMSG4(s,var1,var2,var3,var4) sprintf(msg,(s),(var1),(var2),(var3),(var4))
#define SETMSG5(s,var1,var2,var3,var4,var5) sprintf(msg,(s),(var1),(var2),(var3),(var4),(var5))

#define ODBOPER0(what)                 ODB_oper(what, NULL, NULL, NULL, NULL, __FILE__, __LINE__)
#define ODBOPER1(what, p1)             ODB_oper(what, p1, NULL, NULL, NULL, __FILE__, __LINE__)
#define ODBOPER2(what, p1, p2)         ODB_oper(what, p1, p2, NULL, NULL, __FILE__, __LINE__)
#define ODBOPER3(what, p1, p2, p3)     ODB_oper(what, p1, p2, p3, NULL, __FILE__, __LINE__)
#define ODBOPER4(what, p1, p2, p3, p4) ODB_oper(what, p1, p2, p3, p4, __FILE__, __LINE__)

#define FUNC_LINKLIST_ADD         1
#define FUNC_LINKLIST_START       2
#define FUNC_LINKLIST_QUERY       3

typedef struct _ODB_linklist {
  char *lhs;
  struct _rhs_t {
    char *name;
    struct _rhs_t *next;
  } *rhs;
  struct _rhs_t *last_rhs;
  int n_rhs;
  int type; /* 1 : -1 (onelooper), 2 : -A (align), 4 : a regular link, 8 : shared link (N/A) */
  struct _ODB_linklist *next;
} ODB_linklist;

typedef struct _ODB_Arridx {
  int low;
  int high;
  int inc;
  Boolean only_low;
  Boolean only_high;
} ODB_Arridx;

typedef struct _ODB_Filelist {
  char *filename;
  Boolean create_so;
  struct _ODB_Filelist *next;
} ODB_Filelist;

typedef struct _ODB_Tree {
  int    what;      /* What sort of command */
  double dval;      /* Direct numeric value (if applicable) */
  int    argc;      /* No of args */
  void **argv;      /* Argument list */
  int   level;      /* FROM-level nesting (0,1,2,...) or uninitialized = -1 */
  int joffset;      /* Arg-offset (usually 0) as in funcs_t in funcs.h */
  struct _ODB_Tree *next; /* Next in chain */
} ODB_Tree;

typedef struct _ODB_Cmd {
  int lineno;                 /* ODB-source line number */
  ODB_Tree *node;             /* Pointer to the next expression */
  struct _ODB_Cmd *next;
} ODB_Cmd;

typedef struct _ODB_Match_t {
  ODB_Tree *expr;
  char *formula; /* expression as a character string */
  ODB_Tree *symexpr; /* formula as a symexpr */
} ODB_Match_t;

typedef struct selattr_t {
  Boolean distinct;
} ODB_SelAttr_t;

typedef struct _ODB_dbcred {
  char *dbname;   /* current database name <dbname> */
  char *srcpath;  /* ODB_SRCPATH_<dbname>           */
  char *datapath; /* ODB_DATAPATH_<dbname>          */
  char *idxpath;  /* ODB_IDXPATH_<dbname>           */
  char *poolmask; /* ODB_PERMANENT_POOLMASK         */
} ODB_dbcred;

typedef struct _ODB_Symbol {
  uint hash;        /* A hash of {kind,name} */
  int kind;         /* The kind of an entry : ODB_{TYPE|NAME|USDNAME|STRING|TABLE|VIEW} */
  double dval;      /* Direct numeric value (if applicable) */
  char *name;       /* Name of the symbol in concern */
  char *dname;      /* "True" value of the name (kind == ODB_STRING or ODB_WC_STRING) */
  char *sorig;      /* Original value (as input) of the string, if kind == ODB_STRING or ODB_WC_STRING */
  uint  flag;       /* Various flags needed during the processing */
  Boolean only_view; /* A dollar symbol that exist for VIEW only */
  struct _ODB_Symbol *next;
} ODB_Symbol;

#define RESET_SYM(x,num) ((x) &= ~(1U << ((num)-1)))
#define   SET_SYM(x,num) ((x) |=  (1U << ((num)-1)))
#define  DONE_SYM(x,num) ((x) &   (1U << ((num)-1)))

typedef struct _ODB_Type {
  ODB_Symbol *type;    /* Ptr to a symbol to describe the type itself */
  int nsym;            /* No. of members */
  ODB_Symbol **sym;    /* Pointers to type symbols */
  ODB_Symbol **member; /* Pointers to member symbols */
  Boolean no_members;  /* If a raw typedef : TYPE mytype = uint; */
  Boolean processed;   /* If ODB_TYPE has been processed to the C-language already */
  Boolean bitstream;   /* Solely a bit stream (up to MAXBITS allowed) */
  int *pos;            /* Start of a bit position */
  int *len;            /* No. of bits in that position */
  struct _ODB_Type *next;
} ODB_Type;

typedef struct _ODB_Table {
  ODB_Symbol *table; /* Ptr to a symbol to describe the table itself */
  int nsym;          /* No. of types/symbols in this table */
  int tableno;       /* Sequence number of this table : [0..ODB_ntables-1] */
  int rank;          /* Table's rank (order# in hierarchy) after final weight (wt) has been applied */
  double wt;         /* Precedence weight for use by ordering the FROM-stmt */
  ODB_Type   **type; /* Pointers to types */
  ODB_Symbol **sym;  /* Pointers to variable symbols */
  char    **expname; /* Fully expanded names ("datatype:varname@table") */
  int     *linkmode; /* 0 = not a  link; 1 = link-offset; 2 = link-length */
  int         nlink; /* No. of links to the other tables, if any */
  char   **linkname; /* Link names (before are the link-tables are available */
  struct _ODB_Table **link; /* Links to the other tables */
  uchar *linkslavemask; /* A mask against every table to denote if the current table is slave for
			   given tableno, so that linkslavemask[tableno] > 0
			   Array dimension [0..ODB_ntables-1] */
  struct _ODB_Table **sharedlink; /* Detects shareable links (saves dbase-space) */
  char   **sharedlinkname;        /* Name of the possible master shareable table */
  Boolean any_sharedlinks; /* 1 if any sharedlinkname[] points to non-NULL */
  ODB_dbcred *db;      /* Associated database credentials */
  struct _ODB_Table *next;
} ODB_Table;

typedef struct tabledef_t {
  int num_tables;
  ODB_Table **tables;
} Tabledef_t;

typedef struct _ODB_AndList {
  ODB_Tree *expr;
  int maxfrom;
} ODB_AndList;

typedef struct _ODB_SelectExpr {
  ODB_Tree *expr;       /* Expression tree in concern */
  uint aggr_flag;       /* Non-zero if any of aggregate funcs {SUM|MIN|MAX|AVG|STDEV|COUNT} used */
  int ncols_aux;        /* Number of auxiliary columns needed (if aggregate {CORR|COVAR|...}) */
  char *formula;        /* Expression as a formula/character string (upon input) */
  char *formula_out;    /* Expression as a formula/character string (upon output) */
  ODB_Symbol *nicksym;  /* Expression's nickname symbols */
  int nsym;             /* No. of symbols in this expressions */
  ODB_Symbol **sym;     /* Ptrs to expression symbols */
} ODB_SelectExpr;

typedef struct _ODB_View {
  ODB_Symbol *view;    /* Ptr to a symbol to describe the view itself */

  ODB_dbcred *db;      /* Associated database credentials */

  int create_index;     /* 0=Normal SELECT stmt, 1=CREATE UNIQUE INDEX, 2=CREATE BITMAP INDEX */
  Boolean binary_index; /* True if binary index (file) is meant to be created */

  int nselect;         /* No. of symbols in SELECT-clause (after dupl. removal) */
  int nselect_all;     /* Original no. of symbols in SELECT (before dupl. removal) */
  Boolean *readonly;   /* Read/Only symbols in SELECT-clause */
  Boolean all_readonly;/* when true, then do not generate code for put()-routines at all */
  ODB_Symbol **select; /* Ptrs to SELECT-symbols */

  ODB_SelectExpr **sel; /* SELECT-expressions i.e. "formulas" (when present) & their symbols */
  Boolean *is_formula;  /* Flag for each SELECT-column to denote if it is a formula (=1) or not (=0) */
  Boolean has_formulas; /* true if any SELECT colums involves formulas */
  int nselsym;          /* Total number of symbols in SELECT-expressions **sel */
  ODB_Symbol **selsym;  /* SELECT-expression symbols as a one list (length = nselsym) */

  int ncols_aux;        /* Total number of auxiliary columns needed; derived from SELECT-expressions' ncols_aux */

  Boolean select_distinct; /* SELECT DISTINCT (or SELECT UNIQUE) was used */

  Boolean usddothash; /* Has "$<parent_table>.<child_table>" -variables in SELECT */

  Boolean use_indices; /* If true (default) then prefer to use CREATE INDEX-indices */
  char *use_index_name;

  Boolean reorder_tables; /* View-specific reordering of FROM-tables */

  Boolean insert_tables;  /* When true (default), ODB/SQL will insert missing FROM-tables 
			     by looking into database layout table-hierarchy */

  Boolean has_count_star; /* True if any of the SELECT-expressions have count(*) */

  Boolean safeGuard;      /* True if -G option or SAFEGUARD keyword active */

  Boolean no_from_stmt; /* True if FROM-statement wasn't explicitly present */

  uint select_aggr_flag; /* Non-zero if any of aggregate funcs {SUM|MIN|MAX|AVG|STDEV|COUNT} used */

  int nfrom_start;     /* Starting table (deferred implementation) */
  int nfrom;           /* No. of tables in FROM-clause */
  ODB_Table **from;    /* Ptrs to FROM-tables */
  int *from_attr;      /* ODB_FROM_ATTR_xxx to FROM-tables indicate how table was supplied in FROM-stmt */

  int maxfrom;         /* The most inner loop of [0..nfrom-1], where to put the WHERE-test */
  int *active;         /* Table is active (|= 0x1), if in SELECT i.e. Index_Table[] is needed 
			  active[i] is [also] |= 0x2, if "i" is is one of values of table_index[j]
			*/
  char **merged_with;  /* Table maybe aliased with some other table, i.e. Index_Table[] merge can occur */

  int nwhere;          /* No. of symbols in WHERE-clause */
  ODB_Symbol **where;  /* Ptrs to WHERE-symbols */

  int nuniqueby;       /* No. of UNIQUE BY symbols; naturally > 0 if UNIQUE BY present */
  ODB_Symbol **uniqueby; /* UNIQUEBY symbols */

  Boolean has_thin;    /* thin-function is present in WHERE */

  int norderby;        /* No. of symbols in ORDER BY-clause */
  ODB_Symbol **orderby;/* Ptrs to ORDER BY-symbols */
  int *mkeys;          /* Resolved (SELECT-matched) sorting columns 
			  from ORDER BY-clause for use by (multi-)KEYSORT() */

  int  *table_index;   /* Index to from[] -table for each SELECT/WHERE-symbol */
  char **tag;          /* An array of symbol tags of the  SELECT/WHERE-variables */

  char **call_arg;     /* A symbol of the SELECT/WHERE-variables when passed to the help routines */

  char **def_put;      /* LHS name to be used when addressing SELECT/WHERE-variables in C-code */
  char **alias_put;    /* Alias or the actual name as in #define for LHS */

  char **def_get;      /* RHS name to be used when addressing SELECT/WHERE-variables in C-code */
  char **alias_get;    /* Alias or the actual name as in #define for RHS */

  char **poslen;       /* Bitfield pos & len, if applicable */

  char **offset;       /* The constant expression part from offset(var,const_expr)-"function" */

  ODB_Tree *cond;      /* Pointer to WHERE-condition code */
  ODB_AndList *andlist;  /* A list of AND-expressions */
  int andlen;            /* No. of AND-expressions */

  struct _ODB_View *next;
} ODB_View;

typedef struct symbolparam_t {
  int nsym;
  ODB_Symbol **sym;
  Boolean *readonly;
  ODB_SelectExpr **sel;
} SymbolParam_t;

typedef struct viewdef_t {
  int num_views;
  ODB_View **views;
} Viewdef_t;

typedef struct selectdef_t {
  int nselect;
  char **select_items; /* array */
  Boolean *readonly; /* array */
  ODB_SelectExpr *sel; /* array */
  struct selectdef_t *next;
} Selectdef_t;

typedef struct _ODB_Tracesym {
  int  flag;
  int  next;
  int  maxfrom;
  int  *table_index;
  char **where;
  char **tag;
  Boolean has_maxcount; /* The maxcount() function has been referred (=1), not referred (=0) */
  Boolean has_Unique;   /* The Unique() function has been referred (=1), not referred (=0) */
  Boolean has_thin;     /* The thin() function has been referred (=1), not referred (=0) */
} ODB_Tracesym;

typedef struct _ODB_Stack {
  unsigned int flag;
  union {
    char *s;
    int i;
    ODB_Tree *expr;
    FILE *fp;
    ODB_Symbol *psym;
    ODB_SelectExpr *sel;
  } u;
  struct _ODB_Stack *prev;
  struct _ODB_Stack *next;
} ODB_Stack;

extern ODB_Symbol   *ODB_new_symbol(int kind, const char *s);
extern ODB_Symbol   *ODB_symbol_copy(ODB_Symbol *psym);
extern Boolean       ODB_is_symbol(void *p);
extern ODB_Type     *ODB_new_type(const char *p, Boolean reuse_okay);
extern ODB_Table    *ODB_new_table(const char *p, Boolean reuse_okay);
extern ODB_Table    *ODB_copy_table(const ODB_Table *from, const char *table_name);
extern ODB_View     *ODB_new_view(const char *p, Boolean reuse_okay);
extern ODB_Cmd      *ODB_new_cmd(ODB_Tree *pnode);
extern ODB_Tree     *ODB_oper(int what, void *p1, void *p2, void *p3, void *p4,
			      const char *filename, int lineno);
extern ODB_Tree     *ODB_oper_copy(ODB_Tree *expr, Boolean recursive);
extern Boolean       ODB_is_oper(void *p);
extern void          ODB_pushSELECTEXPR(ODB_SelectExpr *sel);
extern void          ODB_pushSYMBOL(ODB_Symbol *psym);
extern void          ODB_pushFILE(FILE *fp);
extern void          ODB_pushexpr(ODB_Tree *expr);
extern void          ODB_pushstr(char *s);
extern void          ODB_pushi(int i);

extern ODB_SelectExpr *ODB_popSELECTEXPR();
extern ODB_Symbol     *ODB_popSYMBOL();
extern FILE           *ODB_popFILE();
extern ODB_Tree       *ODB_popexpr();
extern char           *ODB_popstr();
extern int             ODB_popi();

extern char *ODB_keymap(int what);

extern char *ODB_lowercase(const char *s);
extern char *ODB_uppercase(const char *s);
extern char *ODB_capitalize(const char *s);

extern ODB_Symbol  *ODB_lookup(int kind, const char *s, ODB_Symbol *start_symbol);
extern ODB_Type    *ODB_lookup_type(const char *name, ODB_Type *start_type);
extern ODB_Table   *ODB_lookup_table(const char *name, ODB_Table *start_table);
extern ODB_View    *ODB_lookup_view(const char *name, ODB_View *start_view);

extern Boolean      ODB_in_table(int kind, const char *name, const ODB_Table *ptable, int *index);
extern Boolean      ODB_in_type(const char *name, const ODB_Type *ptype, int *index);

extern ODB_Symbol **ODB_resolve_relations(const char *label,
					  int bailout_key,
					  SymbolParam_t *sp,
					  const char *vname,
					  ODB_Table **table, 
					  const int *from_attr,
					  int ntable,
					  ODB_Symbol **sym, 
					  int *sign_in,
					  int nsym,
					  Boolean allow_dollar,
					  Boolean allow_hash,
					  int *nsym_out,
					  int **sign_out);

extern void ODB_setup_where(ODB_View *pview);
extern int ODB_setup_selectexpr(ODB_View *pview, int k, int j);
extern Boolean ODB_IsSimpleExpr(const ODB_Tree *pnode, Boolean funcs_are_simple);

extern Boolean ODB_matchup_sym(Boolean report_error,
			       const char *target_label,
			       ODB_Symbol **target,
			       int ntarget,
			       const char *with_label,
			       ODB_Symbol **with,
			       int nwith,
			       int *key);

extern FILE *ODB_fopen(const char *file, const char *mode, const char *srcfile, int srcline);
extern int   ODB_fclose(FILE *fp, const char *srcfile, int srcline);

extern ODB_Tree    *ODB_start_oper();
extern ODB_Symbol  *ODB_start_symbol();
extern ODB_Type    *ODB_start_type();
extern ODB_Table   *ODB_start_table();
extern ODB_View    *ODB_start_view();
extern ODB_Cmd     *ODB_start_cmd();

extern void ODB_print_symbols(FILE *fp);
extern int ODB_pick_tables(int nfrom);
extern int ODB_pick_symbols(ODB_Table **from, const int *from_attr, int nfrom);
extern int ODB_trace_symbols(ODB_Tree *pnode, ODB_Tracesym *t, Boolean is_selectexpr);
extern int ODB_wildcard(const char *s, ODB_Table **from, const int *from_attr, int nfrom);
extern int ODB_regex(const char *s, ODB_Table **from, const int *from_attr, int nfrom);
extern Boolean ODB_is_integer(const char *s);
extern Boolean ODB_is_dollar(const char *s);
extern void ODB_where_massage(ODB_View *pview);
extern int ODB_reorder_tables(ODB_View *pview);
extern int ODB_insert_tables(ODB_View *pview);
extern char *ODB_get_sharedlinkname(const char *table_name, const char *target);
extern int ODB_remove_duplicates(SymbolParam_t *in, 
				 int  *table_index,
				 char **tag,
				 char **call_arg,
				 char **def_put,
				 char **alias_put,
				 char **def_get,
				 char **alias_get,
				 char **poslen,
				 char **offset);
extern Boolean ODB_dupl_symbols(ODB_Symbol **psym, int nsym, Boolean extract);
extern Boolean ODB_split(const char *s, 
			 char **type, char **var, char **member, char **table, char **offset);
extern char *ODB_expand_sym(const ODB_Symbol *psym, const char *suffix, const char *table, const char *offset);
extern uint ODB_which_aggr(const ODB_Tree *pnode, int *ncols_aux);

extern   char *ODB_extract(const char *in, int left_delim, int right_delim);
extern void ODB_link_massage();
extern int ODB_addrel(const char *p, char separ);

extern ODB_Filelist *genc(Boolean views_only);

extern void ODB_exit(int status);
extern void ODB_sigexit(int status);
extern int ODB_evaluate(ODB_Tree *pnode, double *dval);

extern void     destroy_list();
extern char    *init_list(const char *p);
extern Boolean  in_list(const char *p);
extern Boolean in_mylist(const char *p, const char *mylist, char delim);
extern Boolean  in_extlist(const char *p, const char *extlist);
extern char    *in_extlist1(const char *p, const char *extlist);
extern char    *add_list(const char *p);
extern char    *get_list();
extern char     get_list_delim();
extern int      get_list_elemcount();

extern void  dump_c(FILE *cfp, int lineno, ODB_Tree *pnode);
extern char *dump_s(char *in, ODB_Tree *pnode, int flag, Boolean *subexpr_is_aggrfunc);
extern const char *ODB_NickName(int colnum);

extern void   ODB_put_define(const char *s, double default_value);
extern double ODB_get_define(const char *s);
extern Boolean ODB_has_define(const char *s);

extern int ODB_copyfile(FILE *fpout, const char *file,
			const char *start_mark, const char *end_mark,
			Boolean backslash_dblquote,
			Boolean keep_backslash);

extern int ODB_grepfile(FILE *fpout, const char *pattern, const char *file,
			const char *start_mark, const char *end_mark,
			Boolean backslash_dblquote,
			Boolean keep_backslash);

extern Boolean ODB_fixconv(ODB_Tree *pnode);

extern uint ODB_hash(int kind, const char *s);
extern ODB_linklist *manage_linklist(int function,
				     const char *lhs, const char *rhs,
				     int type);
extern void process_one_tables(FILE *cfp,
			       const char *start, const char *end);

extern Boolean is_dummy_table(const char *name);

extern int ODB_RemoveDuplicateTables(ODB_View *v);

#ifndef THIS_IS_YACC_FILE
#include "y.tab.h"
#endif

/* Prevent warnings from -Wmissing-prototypes.  */

#ifdef YYPARSE_PARAM
#if defined (__STDC__) || defined (__cplusplus)
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined (__STDC__) || defined (__cplusplus)
int yyparse (void);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */

#ifdef YYLEX_PARAM
extern int yylex (YYLEX_PARAM);
#else /* ! YYLEX_PARAM */
extern int yylex ();
#endif /* ! YYLEX_PARAM */


