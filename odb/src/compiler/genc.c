
/* genc.c */

#define SETMSG 1
#include "defs.h"
#include "magicwords.h"
#include "pcma_extern.h"

PRIVATE ODB_Filelist *flist_start = NULL;
PRIVATE ODB_Filelist *flist_last = NULL;
PRIVATE ODB_Filelist *flist = NULL;

PRIVATE const int linelen_threshold = 50;

PRIVATE char **tmpsym = NULL;
PRIVATE int ntmpsym = 0;

extern int ODB_lineno;
extern char *odb_label;
extern int optlevel;
extern int genstatic;
extern int incremental;
extern int merge_table_indices;
extern Boolean verbose;
extern char *one_tables;
extern Boolean reset_align_issued;
extern Boolean reset_onelooper_issued;
extern Boolean has_USD_symbols;
extern ODB_Table **table_hier;
extern int ODB_ntables;
extern Boolean readonly_mode;
extern Boolean insert_drhook;
extern int ddl_piped;
extern char *ARGH;

extern FILE *fpdevnull;
PRIVATE FILE *fphdr     = NULL;
PRIVATE FILE *fptable   = NULL;

extern int filtered_info;
PUBLIC FILE *fpinf  = NULL; /* Provides filtered (-F<level>) compilation information for generic select */
PRIVATE int info_dump = 0;

extern int GetSign(const char *poffset); /* from tree.c */

static const char usd_maxcount[] = "$__maxcount__";
static const double mdi = ABS(RMDI);

#define FPINF_MAGIC()             if (fpinf) ODB_fprintf(fpinf, "INFO\n");
#define FPINF_KEYLINE(name,item)  if (fpinf) ODB_fprintf(fpinf, "\n/%s=%s\n", #name, item)
#define FPINF_KEYNUM(name,num)    if (fpinf) ODB_fprintf(fpinf, "\n/%s %d\n", #name, num)
#define FPINF_KEY(name)           FPINF_KEYNUM(name,-1)
#define FPINF_ITEM(x)             if (fpinf) ODB_fprintf(fpinf, "%s\n", x)
#define FPINF_NUM(id,val)         if (fpinf) ODB_fprintf(fpinf, "%d %d\n", id, val)
#define FPINF_KEYSTR(num,str)     if (fpinf) ODB_fprintf(fpinf, "%d,%d %s\n", num, STRLEN(str), str)
#define FPINF_NUMITEM_TYPE_VAR_MEM_TABLE(num, ptype, pvar, pmem, ptable, poslen) \
if (fpinf) \
 pmem ? ODB_fprintf(fpinf, "%d %s:%s.%s@%s\t%s 0\n", \
                    num, ptype, pvar, pmem, ptable, poslen ? poslen : "0 0") \
      : ODB_fprintf(fpinf, "%d %s:%s@%s\t%s 0\n"   , \
                    num, ptype, pvar      , ptable, poslen ? poslen : "0 0")
#define FPINF_NUMITEM_TYPE_LINK_VAR_TABLE(num, ptype, plink, pvar, ptable) \
if (fpinf) ODB_fprintf(fpinf, "%d %s:%s(%s)@%s\t0 0 0\n", num, ptype, #plink, pvar, ptable)
#define FPINF_ITEMFLP(x,val)      if (fpinf) ODB_fprintf(fpinf, "%s %.14g\n", x, val)
#define FPINF_NICKNAME(num,x)     if (fpinf) ODB_fprintf(fpinf, "%d %s\n", num, x)
#define FPINF_NUMITEM(num,x,poslen,offset) \
if (fpinf) { int _sign = GetSign(offset); const char *_tmp = offset; \
  ODB_fprintf(fpinf, "%d %s\t%s %s%s\n", \
	      num, x, \
	      poslen ? poslen : "0 0", \
              (_sign < 0) ? "-" : "", \
	      _tmp ? _tmp+ABS(_sign) : "0"); }
#define FPINF_NUMITEM_WT(num,x,wt)if (fpinf) ODB_fprintf(fpinf, "%d %s %.6f\n", num, x, wt)
#define FPINF_FMT2(fmt, x1, x2)   if (fpinf) ODB_fprintf(fpinf, fmt, x1, x2)
#define FPINF_KEYEND(name)        if (fpinf) ODB_fprintf(fpinf, "/end %s\n", #name)
#define FPINF_LINK(child_id, child, parent_id, \
                   parent_or_offset, kind_of_link_or_length, \
                   offset_parent_id, offset_parent_name) \
if (fpinf) ODB_fprintf(fpinf, "%d %s %d %s %s %d %s\n", \
		       child_id, child, parent_id, parent_or_offset, kind_of_link_or_length,\
                       offset_parent_id, offset_parent_name)

#define NL(n)  { if (cfp) { int l; for(l=0; l<(n); l++) fprintf(cfp,"\n"); }}
#define TAB(n) { if (cfp) { int l; for(l=0; l<(n); l++) fprintf(cfp,"  "); }}
#define LBDQ(n){ if (cfp) { TAB(1); fprintf(cfp,"\""); TAB(n); }}
#define LB(n)  { if (cfp) { TAB(1); fprintf(cfp,"{\""); TAB(n); }}
#define RB(n)  { if (cfp) { int l, lmax=(n); for(l=1; l<=lmax; l++) { \
	            fprintf(cfp,"\"},"); NL(1); if (l<lmax) LB(0); } }}
#define RBDQ(n){ if (cfp) { int l, lmax=(n); for(l=1; l<=lmax; l++) { \
	            fprintf(cfp,"\","); NL(1); if (l<lmax) LBDQ(0); } }}
#define WR(s)  { if (cfp) { fprintf(cfp,"%s",s); }}
#define WRCOMMA(s)  { if (cfp) { WR(s); fprintf(cfp,", "); }}
#define LP     { if (cfp) { fprintf(cfp,"("); }}
#define RP     { if (cfp) { fprintf(cfp,")"); }}

#define PrtAggrFlag(x) \
  TAB(1); ODB_fprintf(cfp,"P->aggr_func_flag[%d] = %s; /* '%s' */\n",i,#x,pview->tag[i]); ++numaggr

#define POOLNO "POOLNO"

#define PREFETCH_CODE(ntabs, what, fplink) \
{ \
  for (i=0; i<nfrom; i++) { \
    if (i < nfrom - 1) { \
      Boolean link_found = 0; \
      char *snext = pview->from[i+1]->table->name; \
      char *snext_sharedlinkname = snext; \
      char *sthis = NULL; \
      for (j=0; j<=i; j++) { \
	ODB_Table *t = pview->from[j]; \
	if (t->link) { \
	  int l; \
	  for (l=0; l<t->nlink; l++) { \
	    char *s = t->link[l]->table->name; \
	    link_found = strequ(s, snext); \
	    if (link_found) { \
	      sthis = t->table->name; \
              if (t->sharedlink[l]) snext_sharedlinkname = t->sharedlinkname[l]; \
	      goto what##_prefetch_break; \
	    } \
	  } /* for (l=0; l<t->nlink; l++) */ \
	} /* if (t->link) */ \
      } /* for (j=0; j<=i; j++) */ \
    what##_prefetch_break: \
      if (link_found) { \
	TAB(ntabs); \
	ODB_fprintf(cfp,"%s *P%s_off = UseDSlong(P->T_%s, %s, %s, P->T_%s->LINKOFFSET(%s));\n", \
		LINKOFFSETTYPE, snext, sthis, \
		odb_label,LINKOFFSETTYPE,sthis,snext_sharedlinkname); \
	TAB(ntabs); \
	ODB_fprintf(cfp,"%s *P%s_len = UseDSlong(P->T_%s, %s, %s, P->T_%s->LINKLEN(%s));\n", \
		LINKLENTYPE, snext, sthis, \
		odb_label,LINKLENTYPE,sthis,snext_sharedlinkname); \
        if (fplink && fplink == fpinf) { \
	  int flag = 1; \
	  FPINF_NUMITEM_TYPE_LINK_VAR_TABLE(flag, LINKOFFSETTYPE, LINKOFFSET, snext_sharedlinkname, sthis); \
	  FPINF_NUMITEM_TYPE_LINK_VAR_TABLE(flag, LINKLENTYPE, LINKLEN, snext_sharedlinkname, sthis); \
	} \
      } \
    } /* if (i < nfrom - 1) */ \
  } /* for (i=0; i<nfrom; i++) */ \
}

#define CONDCODE(cfp_io,rc) \
rc = 0; \
(void) tmp_symbols(cfp_io, pcond, done, i, 1+i+2, nfrom_start); \
if (i < maxfrom) { \
  double dval = 0; \
  int evalok = (optlevel > 0) ? ODB_evaluate(pcond, &dval) : 0; \
  int count = 0; \
  if (optlevel > 0 && !evalok) { \
    /* Perform optimization */ \
    for (j=0; j<andlen; j++) { \
      if (andlist[j].maxfrom == i) { \
	ODB_Tree *expr = andlist[j].expr; \
             if (count == 0) { TAB(1+i+2); ODB_fprintf(cfp_io,"int cond%d = ",i); } \
	else if (count  > 0) { NL(1); TAB(6); ODB_fprintf(cfp_io,"&& "); } \
	dump_c(cfp_io,lineno,expr); \
	count++; \
      } \
    } /* for (j=0; j<andlen; j++) */ \
  } \
  if (count > 0) { \
    ODB_fprintf(cfp_io,";\n"); \
    TAB(1+i+2); ODB_fprintf(cfp_io,"if (cond%d == 0) continue;\n",i); \
    rc = count; \
  } else if (evalok) { \
    if (dval == 0) { TAB(1+i+2); ODB_fprintf(cfp_io,"continue; /* Always false */\n"); } \
    rc = -1; \
  } \
} \
else if (i == maxfrom) { \
  double dval = 0; \
  int evalok = (optlevel > 0) ? ODB_evaluate(pcond, &dval) : 0; \
  int count = 0; \
  if (optlevel > 0 && !evalok) { \
    /* Perform optimization */ \
    for (j=0; j<andlen; j++) { \
      if (andlist[j].maxfrom == i) { \
	ODB_Tree *expr = andlist[j].expr; \
	     if (count == 0) { TAB(1+i+2); ODB_fprintf(cfp_io,"int cond%d = ",i); } \
	else if (count > 0) { NL(1); TAB(6); ODB_fprintf(cfp_io,"&& "); } \
	dump_c(cfp_io,lineno,expr); \
	count++; \
      } \
    } /* for (j=0; j<andlen; j++) */ \
  } \
  else if (!evalok) { \
    dump_c(cfp_io,lineno,pcond); \
  } \
  if (count > 0) { \
    ODB_fprintf(cfp_io,";\n"); \
    TAB(1+i+2); ODB_fprintf(cfp_io,"if (cond%d != 0)\n",i); \
    rc = count; \
  } else if (evalok) { \
    if (dval == 0) { TAB(1+i+2); ODB_fprintf(cfp_io,"continue; /* Always false */\n"); } \
    rc = -1; \
  } \
  TAB(1+i+2); ODB_fprintf(cfp_io,"{ /* if-block start */\n"); \
  oneif++; \
}

#define NTYPES 1
static const int ntypes = NTYPES;
static char *Key[NTYPES]         = { "d"       };
static char *ExtType[NTYPES]     = { "double"  };
static char *KeyInfo[NTYPES]     = { "REAL(8)" };

PRIVATE void
TagStrip(FILE *cfp, int ntabs, const char *tag, const char *endstr)
{
  const char *tag_delim = ODB_tag_delim;
  if (tag_delim && tag && cfp && cfp != fpdevnull) {
    const char dblquote = '"';
    const char backslash = '\\';
    Boolean need_endquote = 0;
    const char *p = tag;
    int cnt = 0;
    while (*p) {
      if (cnt == 0) {
	NL(1);
	TAB(ntabs);
	fputc(dblquote, cfp); /* Opening quote */
	need_endquote = 1;
      }
      if (*p == dblquote) fputc(backslash, cfp);
      fputc(*p, cfp);
      ++cnt;
      if (*p == *tag_delim && cnt >= linelen_threshold) {
	fputc(dblquote, cfp); /* Closing quote */
	need_endquote = 0;
	cnt = 0;
      }
      ++p;
    }
    if (need_endquote) fputc(dblquote, cfp); /* Closing quote */
    if (endstr) ODB_fprintf(cfp, "%s", endstr);
  }
}

PRIVATE void
vector_loop(FILE *cfp, Boolean on)
{
  /*
  if (cfp && on) {
    NL(1);
    ODB_fprintf(cfp,"#ifdef VPP\n");
    ODB_fprintf(cfp,"#pragma loop noalias\n");
    ODB_fprintf(cfp,"#pragma loop novrec\n");
    ODB_fprintf(cfp,"#pragma loop vector\n");
    ODB_fprintf(cfp,"#elif defined(NECSX)\n");
    ODB_fprintf(cfp,"#pragma cdir nodep\n");
    ODB_fprintf(cfp,"#endif\n");
  }
  */
}

PRIVATE void
set_optlevel(FILE *cfp, int opt)
{
  /*
  if (cfp && opt >= 0) {
    NL(1);
    ODB_fprintf(cfp,"#ifdef RS6K\n");
    ODB_fprintf(cfp,"#pragma options optimize=%d\n",opt);
    ODB_fprintf(cfp,"#endif\n");
    NL(1);
  }
  */
}

PRIVATE char *
dblquotes(const char *s)
{
  char *p = NULL;
  if (s) {
    char *c;
    int len = 2*strlen(s) + 1;
    ALLOC(p,len);
    c = p;
    while (*s) {
      if (*s == '"') *c++ = '\\';
      *c++ = *s++;
    }
    *c = '\0';
  }
  return p ? p : STRDUP("");
}

PRIVATE void
free_tmpsym(Boolean all)
{
  if (tmpsym) {
    int j;
    for (j=0; j<ntmpsym; j++) {
      FREE(tmpsym[j]);
    }
    if (all) FREE(tmpsym);
    ntmpsym = 0;
  }
}


PUBLIC void
process_one_tables(FILE *cfp, const char *start, const char *end)
{
  ODB_linklist *list = manage_linklist(FUNC_LINKLIST_START,NULL,NULL,0);
  const char *p = one_tables;
  if (!list && p) {
    char tmp_delim = *p++;
    char delim[2];
    char *saved = STRDUP(p);
    char *s = saved;
    char *token = NULL;
    delim[0] = tmp_delim;
    delim[1] = '\0';
    token = strtok(s,delim);
    while (token) {
      char *t = STRDUP(token);
      char *onelooper = strchr(t,'=');
      char *align = strchr(t,'@');
      if (onelooper) {
	*onelooper++ = '\0'; /* sets end of string t implicitly to '\0' */
	if (ODB_lookup_table(onelooper,NULL) &&
	    !manage_linklist(FUNC_LINKLIST_QUERY,t,onelooper,1)) {
	  (void) manage_linklist(FUNC_LINKLIST_ADD,t,onelooper,1);
	}
      }
      else if (align) {
	*align++ = '\0'; /* sets end of string t implicitly to '\0' */
	if (ODB_lookup_table(align,NULL) &&
	    !manage_linklist(FUNC_LINKLIST_QUERY,t,align,2)) {
	  (void) manage_linklist(FUNC_LINKLIST_ADD,t,align,2);
	}
      }
      FREE(t);
      token = strtok(NULL,delim);
    }
    FREE(saved);
    list = manage_linklist(FUNC_LINKLIST_START,NULL,NULL,0);
  }
  if (list && cfp) {
    ODB_linklist *plinklist = list;
    while (plinklist) {
      if (plinklist->n_rhs > 0) {
	if (start) ODB_fprintf(cfp,"%s",start);
	if (plinklist->type == 1) {
	  ODB_fprintf(cfp,"-1%s=",plinklist->lhs);
	}
	else if (plinklist->type == 2) {
	  ODB_fprintf(cfp,"-A%s=",plinklist->lhs);
	}
	if (plinklist->n_rhs > 1) ODB_fprintf(cfp,"(");
	{ /* Loop over all rhs-(table)names */
	  struct _rhs_t *first_rhs = plinklist->rhs;
	  while (first_rhs) {
	    ODB_fprintf(cfp,"%s",first_rhs->name);
	    if (first_rhs != plinklist->last_rhs) ODB_fprintf(cfp,",");
	    first_rhs = first_rhs->next;
	  } /* while (first_rhs) */
	}
	if (plinklist->n_rhs > 1) ODB_fprintf(cfp,")");
	if (end) ODB_fprintf(cfp,"%s",end);
      }
      plinklist = plinklist->next;
    } /* while (plinklist) */
  } /* if (list && cfp) */
}


PRIVATE	int
is_master(const char *master, const char *slave)
{ /* This is not completely right ;-( .. but practically not used at all */
  int rc = 0;
  if (master && slave) {
    /* Precedence over ALIGN-table (=2) testing */
    /* "master@slave" */
    if (manage_linklist(FUNC_LINKLIST_QUERY,master,slave,2))
      rc = 1; /* master is a master against the given slave */
  }
  return rc;
}


PRIVATE int
onegrep(const char *master, const char *slave, int lineno)
{
  int icase = 0;
  if (master && slave) {
    int match = 0;
    ODB_Table *pm = ODB_lookup_table(master,NULL);
    ODB_Table *ps = ODB_lookup_table(slave,NULL);
    int m_tableno = pm ? pm->tableno : -1;
    int s_tableno = ps ? ps->tableno : -1;

    match = (pm && ps && ps->linkslavemask[m_tableno]);

    /* Precedence over ALIGN-table (=2) testing */
    if (match) {
      if ((ps->linkslavemask[m_tableno] & 2) == 2) icase = 2;
      else if ((ps->linkslavemask[m_tableno] & 1) == 1) icase = 1;
      else match = 0;
    }

    if (!match) {
      /* Swap master & slave with each other so that the table order doesn't matter */
      match = (ps && pm && pm->linkslavemask[s_tableno]);
      if (match) {
	if ((pm->linkslavemask[s_tableno] & 2) == 2) icase = 2;
	else if ((pm->linkslavemask[s_tableno] & 1) == 1) icase = 1;
	else match = 0;
      }
    }

    if (verbose) fprintf(stderr,"onegrep(%s,%s,#%d) = %d : match = %d\n",master,slave,lineno,icase,match);
  }
  return icase;
}

PUBLIC char *
ODB_get_sharedlinkname(const char *slave, const char *target)
{
  char *master = NULL;
  if (target && slave) {
    char *p;
    int len = strlen(slave) + 2;
    ALLOC(p,len);

    /* locate "&slave" */
    sprintf(p,"&%s",slave);
    master = in_extlist1(p,target);
    FREE(p);
  }
  return master;
}

PRIVATE int
get_PKmethod(const char *type, char **ftype, char **datatype, char **ctype)
{
  int method = 0;

  if (strequ(type,"pk2real")) {
    method = 2;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk3real")) {
    method = 3; /* Method#3 now fixed (13/12/2000) */
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"string")) {
    method = 3; /* Method#3 now fixed (13/12/2000) */
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("STRING");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk4real")) {
    method = 4;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk5real")) {
    method = 5;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk9real")) {
    method = 9;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk2int")) {
    method = 2;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("INT4");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"pk3int")) {
    method = 3;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("INT4");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"pk4int")) {
    method = 4;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("INT4");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"pk5int")) {
    method = 5;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("INT4");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"pk1int")) {
    method = 1;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("INT4");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"yyyymmdd")) {
    method = 1;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("YYYYMMDD");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"hhmmss")) {
    method = 1;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("HHMMSS");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,BITFIELD)) {
    /* method = 3; Used to be 9 before 16/10/2000; SS */
    /* method = 9; Back to 9 since pcma_3 has failed ; SS 29/11/2000 */
    method = 1; /* Brand new method since 13/12/2000 */
    /* if (ftype) *ftype = STRDUP("UINT"); -- commented out on 29/05/2002 by SS */
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("BITFIELD");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"linkoffset_t")) {
    method = 1;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("LINKOFFSET");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"linklen_t")) {
    method = 1;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("LINKLEN");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"pk9int")) {
    method = 9;
    if (ftype)    *ftype = STRDUP("INT");
    if (datatype) *datatype = STRDUP("INT4");
    if (ctype)    *ctype = STRDUP("int");
  }
  else if (strequ(type,"pk11real")) {
    method = 11;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk12real")) {
    method = 12;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk13real")) {
    method = 13;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk14real")) {
    method = 14;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk15real")) {
    method = 15;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk16real")) {
    method = 16;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk17real")) {
    method = 17;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk18real")) {
    method = 18;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk19real")) {
    method = 19;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk21real")) {
    method = 21;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk22real")) {
    method = 22;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk23real")) {
    method = 23;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk24real")) {
    method = 24;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk25real")) {
    method = 25;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk26real")) {
    method = 26;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk27real")) {
    method = 27;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk28real")) {
    method = 28;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk29real")) {
    method = 29;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk31real")) {
    method = 31;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk32real")) {
    method = 32;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk33real")) {
    method = 33;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk34real")) {
    method = 34;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk35real")) {
    method = 35;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk36real")) {
    method = 36;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk37real")) {
    method = 37;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk38real")) {
    method = 38;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else if (strequ(type,"pk39real")) {
    method = 39;
    if (ftype)    *ftype = STRDUP("DBL");
    if (datatype) *datatype = STRDUP("REAL8");
    if (ctype)    *ctype = STRDUP("double");
  }
  else {
    method = 0;
    if (ftype) *ftype = STRDUP(type);
    if (datatype) {
      if (strequ(type,"double")) *datatype = STRDUP("REAL8");
      else if (strequ(type,"float")) *datatype = STRDUP("REAL4");
      else if (strequ(type,"int")) *datatype = STRDUP("INT4");
      else if (strequ(type,"uint")) *datatype = STRDUP("UINT4");
      else if (strequ(type,"char")) *datatype = STRDUP("INT1");
      else if (strequ(type,"uchar")) *datatype = STRDUP("UINT1");
      else if (strequ(type,"short")) *datatype = STRDUP("INT2");
      else if (strequ(type,"ushort")) *datatype = STRDUP("UINT2");
      else if (strequ(type,"bufr")) *datatype = STRDUP("BUFR");
      else if (strequ(type,"grib")) *datatype = STRDUP("GRIB");
      else if (strequ(type,"real4")) *datatype = STRDUP("REAL4");
      else if (strequ(type,"real8")) *datatype = STRDUP("REAL8");
      else if (strequ(type,"real")) *datatype = STRDUP("REAL8");
      else if (strequ(type,"integer1")) *datatype = STRDUP("INT1");
      else if (strequ(type,"integer2")) *datatype = STRDUP("INT2");
      else if (strequ(type,"integer4")) *datatype = STRDUP("INT4");
      else if (strequ(type,"integer8")) *datatype = STRDUP("INT8");
      else if (strequ(type,"longlong")) *datatype = STRDUP("INT8");
      else if (strequ(type,"ulonglong")) *datatype = STRDUP("UINT8");
      else *datatype = STRDUP("UNDEF");
    }
    if (ctype) {
      if (strequ(type,"double")) *ctype = STRDUP("double");
      else if (strequ(type,"float")) *ctype = STRDUP("float");
      else if (strequ(type,"int")) *ctype = STRDUP("int");
      else if (strequ(type,"uint")) *ctype = STRDUP("unsigned int");
      else if (strequ(type,"char")) *ctype = STRDUP("char");
      else if (strequ(type,"uchar")) *ctype = STRDUP("unsigned char");
      else if (strequ(type,"short")) *ctype = STRDUP("short int");
      else if (strequ(type,"ushort")) *ctype = STRDUP("unsigned short int");
      else if (strequ(type,"bufr")) *ctype = STRDUP("bufr");
      else if (strequ(type,"grib")) *ctype = STRDUP("grib");
      else if (strequ(type,"real4")) *ctype = STRDUP("float");
      else if (strequ(type,"real8")) *ctype = STRDUP("double");
      else if (strequ(type,"real")) *ctype = STRDUP("double");
      else if (strequ(type,"integer1")) *ctype = STRDUP("char");
      else if (strequ(type,"integer2")) *ctype = STRDUP("short int");
      else if (strequ(type,"integer4")) *ctype = STRDUP("int");
      else if (strequ(type,"integer8")) *ctype = STRDUP("long long int");
      else if (strequ(type,"longlong")) *ctype = STRDUP("long long int");
      else if (strequ(type,"ulonglong")) *ctype = STRDUP("unsigned long long int");
      else *ctype = STRDUP("undefined_ctype");
    }
  }

  return method;
}


PRIVATE char *
get_typedef(const char *type, const char *name, const char *ptr)
{
  char *s = NULL;
  const char *p = ptr ? ptr : "";

  if (strnequ(type,"bit",3)) {
    char *t = "unsigned";
    int nbits = atoi(type+3);
    ALLOC(s,strlen(t) + strlen(name) + 20);
    sprintf(s,"%s %s : %d",t,name,nbits);
  }
  else {
    ALLOC(s,strlen(type) + strlen(name) + strlen(p) + 2);
    sprintf(s,"%s %s%s",type,p,name);
  }

  return s;
}


PRIVATE char *
get_typename(ODB_Type *ptype)
{
  char *tyname;
  if (ptype->bitstream) {
    tyname = BITFIELD;
  }
  else if (ptype->nsym == 1 && ptype->no_members) {
    tyname = ptype->sym[0]->name;
  }
  else {
    tyname = ptype->type->name;
  }
  return tyname;
}
	      

PRIVATE int
assign_USD_symbols(FILE *cfp, 
		   ODB_Tree *pnode, 
		   const int numtabs, 
		   const int phase,
		   const char *name,
		   const char *viewname)
{
  int rc = 0;
  if (!has_USD_symbols) return rc;

  if (pnode) {
    int what = pnode->what;
    
    switch (what) {
    case ODB_WHERE_SYMBOL:
      break;

    case ODB_MATCH:
      {
	int j, nummatch = pnode->argc; /* Ought to be 1 */
	for (j=0; j<nummatch; j++) {
	  ODB_Match_t *match = pnode->argv[j];
	  ODB_Tree *expr = match->expr;
	  rc += assign_USD_symbols(cfp, expr, numtabs, phase, name, viewname);
	} /* for (j=0; j<nummatch; j++) */
      }
      break;

    case ODB_USDNAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *s = psym->name;
	double value = psym->dval;
	uint flag = psym->flag;
	uint done = DONE_SYM(flag, phase);
	
	if (/* cfp && */ (phase >= 1 && phase <= 3) && !done) {
	  if (phase == 1) { /* Declare local USD-variable */
	    TAB(numtabs);
	    if (IS_POOLNO(s)) {/* '$#' or '$pool#' */
	      ODB_fprintf(cfp,"double lc_USD_%s = P->PoolNo; /* A fixed value */\n", POOLNO);
	    }
	    else if (!IS_USDHASH(s)) {
	      ODB_fprintf(cfp,"double lc_USD_%s = 0;\n",s+1);
	    }
	    else if (IS_USDHASH(s)) {
	      char *sh = STRDUP(s);
	      int sh_len = STRLEN(sh);
	      char *dot = strchr(sh,'.');
	      if (dot) *dot = 'D'; /* IS_USDDOTHASH(s) */
	      sh[sh_len-1] = '\0'; /* replace '#' with '\0' */
	      ODB_fprintf(cfp,"double lc_USDHASH_%s = 0;\n",sh+1);
	      FREE(sh);
	    }
	  }
	  else if (phase == 2) { /* Get the most recent value of the USD-variable */
	    if (!IS_POOLNO(s)) {
	      if (!IS_USDHASH(s)) {
		TAB(numtabs);
		if (name) {
		  ODB_fprintf(cfp,
			      "ODBMAC_ADDR_TRIGGER(%s, %s, %s, %s%s%s);\n",
			      odb_label,name,s+1,
			      viewname ? "\"" : "", 
			      viewname ? viewname : "NULL", viewname ? "\"" : "");
		}
		else { /* !name */
		  ODB_fprintf(cfp,
			      "ODBMAC_LC_GETVAL(%s, %s, %s%s%s);\n",
			      odb_label,s+1,
			      viewname ? "\"" : "", 
			      viewname ? viewname : "NULL", viewname ? "\"" : "");
		}
	      }
	      else if (IS_USDHASH(s)) {
		char *eq = NULL;
		if (IS_(ROWNUM,s)) eq = "tmpcount+1";
		if (IS_(UNIQNUM,s)) eq = "ODB_put_one_control_word(tmpcount, P->PoolNo)";
		else if (IS_(NROWS,s)) eq = "P->Nrows";
		else if (IS_(NCOLS,s)) eq = "P->Ncols";
		else if (IS_(COLNUM,s)) eq = "0"; /* Don't know the column ? */
		else if (IS_USDDOTHASH(s)) eq = "0";
		if (eq) {
		  char *sh = STRDUP(s);
		  int sh_len = STRLEN(sh);
		  char *dot = strchr(sh,'.');
		  if (dot) *dot = 'D'; /* IS_USDDOTHASH(s) */
		  sh[sh_len-1] = '\0'; /* replace '#' with '\0' */
		  TAB(numtabs);
		  ODB_fprintf(cfp,"lc_USDHASH_%s = %s;\n",sh+1,eq);
		  FREE(sh);
		}
	      }
	    } /* if (!IS_POOLNO(s)) */
	  }
	  else if (phase == 3) { /* Add variable to the known variable list */
	    if (!IS_POOLNO(s) && !IS_USDHASH(s)) {
	      TAB(numtabs);
	      ODB_fprintf(cfp,"Pool->add_var(\"%s\", \"%s\", \"%s\", it, USD_%s_%s);\n",
			  odb_label, s, viewname ? viewname : "???", s+1, odb_label);
	    }
	    add_list(s);
	    FPINF_ITEMFLP(s, value);
	  }
	  
	  SET_SYM(psym->flag, phase);
	}
	else if (phase == 4) { /* $-symbol found in WHERE */
	  SET_SYM(psym->flag, 4);
	}
	else if (phase == 5) { /* $-symbol found in SELECT-expression */
	  SET_SYM(psym->flag, 5);
	}
	else if (phase == 0) { /* Reset phase flags to zero */
	  RESET_SYM(psym->flag, 1);
	  RESET_SYM(psym->flag, 2);
	  RESET_SYM(psym->flag, 3);
	}
      }
      rc = 1;
      break;
      
    case ODB_COND:
      {
	int j, numargs = pnode->argc;
	for (j=0; j<numargs; j++) 
	  rc += assign_USD_symbols(cfp, pnode->argv[j], numtabs, phase, name, viewname);
      }
      break;

    case ODB_FUNC:
    case ODB_FUNCAGGR:
      {
	int j, numargs = pnode->argc - 1;
	for (j=1; j<=numargs; j++) 
	  rc += assign_USD_symbols(cfp, pnode->argv[j], numtabs, phase, name, viewname);
      }
      break;

    case ODB_FILE:
      {
	rc += assign_USD_symbols(cfp, pnode->argv[2], numtabs, phase, name, viewname);
      }
      break;
      
    case ODB_GT:
    case ODB_GE:
    case ODB_EQ:
    case ODB_LE:
    case ODB_LT:
    case ODB_NE:
    case ODB_AND:
    case ODB_OR:
    case ODB_ADD:
    case ODB_SUB:
    case ODB_STAR:
    case ODB_DIV:
      rc += assign_USD_symbols(cfp, pnode->argv[0], numtabs, phase, name, viewname);
      rc += assign_USD_symbols(cfp, pnode->argv[1], numtabs, phase, name, viewname);
      break;
      
    case ODB_UNARY_PLUS:
    case ODB_UNARY_MINUS:
    case ODB_NOT:
      rc += assign_USD_symbols(cfp, pnode->argv[0], numtabs, phase, name, viewname);
      break;
      
    default:
      rc = 0;
      break;
    } /* switch (what) */
  } /* if (pnode) */

  return rc;
}


PRIVATE int
tmp_symbols(FILE *cfp, ODB_Tree *pnode, Boolean done[], 
	    int level, int numtabs, int nfrom_start)
{
  int rc = 0;

  if (pnode) {
    int what = pnode->what;

    switch (what) {
    case ODB_WHERE_SYMBOL:
      {
	char *s = pnode->argv[0];
	char *ptype = NULL;
	char *pvar = NULL;
	char *pmember = NULL;
	char *ptable = NULL;
	int ifrom, isym;

	(void) ODB_split(s, &ptype, &pvar, &pmember, &ptable, NULL);
	ifrom = atoi(pmember) - nfrom_start;

	if (ifrom == level) {
	  isym = atoi(pvar);
	  if (!done[isym]) {
	    int idx = -1;
	    int j;
	    int n = ntmpsym;
	    int len = strlen(ptype) + strlen(ptable) + 100;
	    char *s = NULL;
	    char *ps;

	    ALLOC(s,len);
	    snprintf(s,len,"%d = %s(K%d)",isym,ptable,ifrom);
	    ps = strchr(s,'=');

	    if (ps) {
	      for (j=0; j<n; j++) {
		/* Check if already declared */
		char *c = tmpsym[j];
		char *pc = strchr(c,'=');
		if (pc) {
		  if (strequ(ps,pc)) {
		    idx = j;
		    break;
		  }
		}
	      } /* for (j=0; j<n; j++) */
	    }
	    
	    TAB(numtabs);
	    if (idx == -1) {
	      n = ntmpsym;
	      tmpsym[n] = s;
	      ntmpsym++;
	      ODB_fprintf(cfp,"%s tmp%s",ptype,s);
	      ODB_fprintf(cfp,";\n");
	    }
	    else {
	      char *c = STRDUP(tmpsym[idx]);
	      char *p = strchr(c,'=');
	      n = 0;
	      if (p) {
		*p = '\0';
		n = atoi(c);
	      }
	      ODB_fprintf(cfp,"%s tmp%d = tmp%d;\n",ptype,isym,n);
	      FREE(c);
	      FREE(s);
	    }

	    done[isym] = 1;
	  }
	}

	FREE(ptype);
	FREE(pvar);
	FREE(pmember);
	FREE(ptable);
      }
      break;

    case ODB_MATCH:
      {
	int j, nummatch = pnode->argc; /* Ought to be 1 */
	for (j=0; j<nummatch; j++) {
	  ODB_Match_t *match = pnode->argv[j];
	  ODB_Tree *expr = match->expr;
	  rc += tmp_symbols(cfp, expr, done, level, numtabs, nfrom_start);
	} /* for (j=0; j<nummatch; j++) */
      }
      break;

    case ODB_COND:
      {
	int j, numargs = pnode->argc;
	for (j=0; j<numargs; j++) {
	  rc += tmp_symbols(cfp, pnode->argv[j], done, level, numtabs, nfrom_start);
	}
      }
      break;

    case ODB_FUNC:
    case ODB_FUNCAGGR:
      {
	int j, numargs = pnode->argc - 1;
	for (j=1; j<=numargs; j++) {
	  rc += tmp_symbols(cfp, pnode->argv[j], done, level, numtabs, nfrom_start);
	}
      }
      break;

    case ODB_FILE:
      {
	rc += tmp_symbols(cfp, pnode->argv[2], done, level, numtabs, nfrom_start);
      }
      break;

    case ODB_GT:
    case ODB_GE:
    case ODB_EQ:
    case ODB_LE:
    case ODB_LT:
    case ODB_NE:
    case ODB_AND:
    case ODB_OR:
    case ODB_ADD:
    case ODB_SUB:
    case ODB_STAR:
    case ODB_DIV:
      rc += tmp_symbols(cfp, pnode->argv[0], done, level, numtabs, nfrom_start);
      rc += tmp_symbols(cfp, pnode->argv[1], done, level, numtabs, nfrom_start);
      break;

    case ODB_UNARY_PLUS:
    case ODB_UNARY_MINUS:
    case ODB_NOT:
      rc += tmp_symbols(cfp, pnode->argv[0], done, level, numtabs, nfrom_start);
      break;

    default:
      rc = 0;
      break;
    } /* switch (what) */
  }

  return rc;
}


#define CHECK_LENOFS(pvar) { \
const int linklen =  8; /* strlen("LINKLEN(") */ \
const int linkofs = 11; /* strlen("LINKOFFSET(") */ \
if (strnequ(pvar,"LINKLEN(",linklen)) { \
  char *p = strchr(pvar,')'); \
  if (p) *p = '\0'; \
  p = STRDUP(pvar+linklen); \
  FREE(pvar); \
  ALLOC(pvar,strlen(p)+10); \
  sprintf(pvar,"%s.len",p); \
  FREE(p); \
} \
else if (strnequ(pvar,"LINKOFFSET(",linkofs)) { \
  char *p = strchr(pvar,')'); \
  if (p) *p = '\0'; \
  p = STRDUP(pvar+linkofs); \
  FREE(pvar); \
  ALLOC(pvar,strlen(p)+10); \
  sprintf(pvar,"%s.offset",p); \
  FREE(p); \
} }


PRIVATE void
where_cond(FILE *cfp, const ODB_Tree *pnode)
{
  if (cfp && pnode) {
    int what = pnode->what;

    switch (what) {
    case ODB_MATCH:
      {
	int j, nummatch = pnode->argc; /* Ought to be 1 */
	ODB_fprintf(cfp,"MATCH(");
	for (j=0; j<nummatch; j++) {
	  ODB_Match_t *match = pnode->argv[j];
	  ODB_Tree *expr = match->expr;
	  ODB_fprintf(cfp,"%s",(j>0) ? "," : ",");
	  where_cond(cfp, expr);
	} /* for (j=0; j<nummatch; j++) */
	ODB_fprintf(cfp,")");
      }
      break;

    case ODB_FUNC:
      {
	int j, jstart, numargs = pnode->argc - 1;
	ODB_Symbol *psym = pnode->argv[0];
	char *name = psym->name;
	ODB_fprintf(cfp,"%s(",name);
	jstart = 1;
	for (j=jstart; j<=numargs; j++) {
	  where_cond(cfp, pnode->argv[j]);
	  ODB_fprintf(cfp,"%s",(j==numargs) ? ")" : ","); 
	}
      }
      break;

    case ODB_FILE:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *name = psym->name;
	char *filename = dblquotes(pnode->argv[1]);
	if (strequ(name,"InFile")) {
	  where_cond(cfp, pnode->argv[2]);
	  ODB_fprintf(cfp," IN FILE ('%s')",filename);
	}
	else if (strequ(name,"NotInFile")) {
	  where_cond(cfp, pnode->argv[2]);
	  ODB_fprintf(cfp," NOT IN FILE ('%s')",filename);
	}
	FREE(filename);
      }
      break;

    case ODB_AND:
    case ODB_OR:
    case ODB_NE:
    case ODB_EQ:
    case ODB_GT:
    case ODB_GE:
    case ODB_LE:
    case ODB_LT:
    case ODB_ADD:
    case ODB_SUB:
    case ODB_STAR:
    case ODB_DIV:
      LP;
      where_cond(cfp, pnode->argv[0]);
      ODB_fprintf(cfp,"%s ",ODB_keymap(what));
      if (what == ODB_AND || what == ODB_OR) {
	RB(1); LB(3);
      }
      where_cond(cfp, pnode->argv[1]);
      RP;
      break;

    case ODB_UNARY_PLUS:
    case ODB_UNARY_MINUS:
    case ODB_NOT:
      LP;
      ODB_fprintf(cfp,"%s",ODB_keymap(what));
      where_cond(cfp, pnode->argv[0]);
      RP;
      break;

    case ODB_NUMBER:
      {
	double dval = pnode->dval;
	ODB_fprintf(cfp,"%.14g ", dval);
      }
      break;

    case ODB_WHERE_SYMBOL:
      {
	char *tag = pnode->argv[2];
	char *s = strchr(tag,':');
	char *name = s ? STRDUP(s+1) : STRDUP(tag);
	if (s) CHECK_LENOFS(name);
	ODB_fprintf(cfp,"%s ", name);
	FREE(name);
      }
      break;

    case ODB_NAME:
    case ODB_HASHNAME:
    case ODB_USDNAME:
    case ODB_BSNUM:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *name = psym->name;
	ODB_fprintf(cfp,"%s ", name);
      }
      break;

    case ODB_STRING:
    case ODB_WC_STRING:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *name = psym->dname;
	ODB_fprintf(cfp,"'%s' ", name);
      }
      break;

    default:
      break;
    } /* switch (what) */
  }
}


#define PROCESS_SPLIT(x,append) \
char *pvar=NULL, *pmember=NULL, *ptable=NULL; \
char *a = append?append:", "; \
ODB_split(v->tag[x],NULL,&pvar,&pmember,&ptable,NULL); \
if (pmember) {\
  ODB_fprintf(cfp,"%s.%s@%s%s",pvar,pmember,ptable,a); \
} \
else         { \
  CHECK_LENOFS(pvar); \
  ODB_fprintf(cfp,"%s@%s%s",pvar,ptable,a); \
} \
FREE(pvar); FREE(pmember); FREE(ptable);

PRIVATE void
write_sql_old(FILE *cfp, ODB_View *v, int start_bit)
{
  /* May become obsolete (used only if the new version fails) */
  const int step = 2;
  int j;

  if (start_bit) {
    NL(1);
    ODB_fprintf(cfp,"static const char *Sql[] = {\n");
  }

  {
    ODB_Symbol *psym;
    for (psym = ODB_start_symbol(); psym != NULL; psym = psym->next) {
      if (psym->kind == ODB_USDNAME) {
	uint done = (DONE_SYM(psym->flag, 4) || DONE_SYM(psym->flag, 5));
	if (done) {  /* write only if found in WHERE-statement or SELECT-expression */
	  char *s = psym->name;
	  if (!IS_POOLNO(s) && !IS_USDHASH(s)) {
	    /* Prevent echo'ing '$#' i.e. poolno */
	    LB(0);
	    if (psym->dname) {
	      ODB_fprintf(cfp,"SET %s = '%s';",s,psym->dname);
	    }
	    else {
	      ODB_fprintf(cfp,"SET %s = %.14g;",s,psym->dval);
	    }
	    RB(1);
	  }
	}
      } /* if (psym->kind == ODB_USDNAME) */
    }
  }

  LB(0); RB(1);

  LB(0); ODB_fprintf(cfp,"CREATE VIEW %s AS",v->view->name); RB(1);

  LB(0); 
  ODB_fprintf(cfp,"SELECT%s",v->select_distinct ? " UNIQUE" : "");
  RB(1);

  for (j=0; j<v->nselect; j++) {
    if (j==0 || j%step == 0) LB(2);
    if (v->tag[j]) {
      PROCESS_SPLIT(j,NULL);
    }
    else {
      WRCOMMA(v->select[j]->name);
    }
    if (j==v->nselect-1 || (j+1)%step == 0) RB(1);
  }

  if (v->nuniqueby > 0 && !v->select_distinct) {
    LB(0); ODB_fprintf(cfp,"UNIQUEBY"); RB(1);
    LB(2);
    for (j=0; j<v->nuniqueby; j++) {
      int k = (v->nselect_all + v->nwhere - v->nuniqueby) + j;
      if (v->tag[k]) {
	PROCESS_SPLIT(k,NULL);
      }
      else {
	WRCOMMA(v->uniqueby[j]->name);
      }
    }
    RB(1);
  }

  LB(0); ODB_fprintf(cfp,"FROM"); RB(1);
  LB(2);
  for (j=0; j<v->nfrom; j++) {
    WRCOMMA(v->from[j]->table->name);
  }
  RB(1);

  if (v->cond) {
    ODB_Tree *pcond = v->cond;
    int what = pcond->what;
    LB(0); ODB_fprintf(cfp,"WHERE"); RB(1);
    if (what == ODB_AND) {
      /* Strip off the compiler generated call to the function Unique() */
      ODB_Tree *left = pcond->argv[0];
      ODB_Tree *right = pcond->argv[1];
      if (right->what == ODB_FUNC) {
	ODB_Symbol *psym = right->argv[0];
	char *name = psym->name;
	if (strequ(name,"Unique")) pcond = left;
      }
    }
    LB(3); where_cond(cfp,pcond); RB(1);
  }

  if (v->norderby > 0) {
    LB(0); ODB_fprintf(cfp,"ORDERBY"); RB(1);
    LB(2);
    for (j=0; j<v->norderby; j++) {
      int i = v->mkeys ? ABS(v->mkeys[j]) : 0;
      if (i >= 1 && i <= v->nselect) {
	char *sign = (v->mkeys[j] > 0) ? NULL : " DESC, ";
	i--;
	if (v->tag[i]) {
	  PROCESS_SPLIT(i,sign);
	}
	else {
	  WRCOMMA(v->select[i]->name);
	}
      }
      else {
	WRCOMMA(v->orderby[j]->name);
      }
    }
    RB(1);
  }

  LB(0); ODB_fprintf(cfp,";"); RB(2);
  TAB(1); ODB_fprintf(cfp,"NULL\n};\n");
  NL(1);
}

PRIVATE void
write_sql(FILE *cfp, ODB_View *v, int start_bit) 
{
  /* New version; takes original SQL-file from disk (if possible) */
  extern char *odb_source;

  if (start_bit) {
    NL(1);
    ODB_fprintf(cfp,"static const char *Sql[] = {\n");
  }

  if (ODB_copyfile(cfp, odb_source, "  \"", "\",", 1, 1) <= 0) {
    write_sql_old(cfp, v, 0);
  }
  else {
    LBDQ(0); ODB_fprintf(cfp,";"); RBDQ(2);
    TAB(1); ODB_fprintf(cfp,"NULL\n};\n");
    NL(1);   
  }
}

PRIVATE void
write_sql_for_info() 
{
  if (fpinf) {
    extern char *odb_source;
    FPINF_KEY(sql_query);
    (void) ODB_copyfile(fpinf, odb_source, "  ", "", 0, 1);
    FPINF_KEYEND(sql_query);
  }
}



PRIVATE void
write_macros(FILE *cfp) 
{ /* Write macros (#defines) */
  ODB_View *pview;
  
  init_list(NULL);
  
  for (pview = ODB_start_view(); pview != NULL; pview = pview->next) {
    int nselect = pview->nselect;
    int nselect_all = pview->nselect_all;
    int nwhere = pview->nwhere;
    int nall = nselect_all + nwhere;
    int norderby = pview->norderby;
    int nselsym = pview->nselsym;
    int j, k;

    for (j=nselect_all; j<nall; j++) {
      char *p;
      int plen;

      if (IS_HASH(pview->where[j-(nselect_all)]->name)) continue;
      if (IS_DOLLAR(pview->where[j-(nselect_all)]->name)) continue;
      if (IS_BSNUM(pview->where[j-(nselect_all)]->name)) continue;
      
      plen = strlen(pview->def_put[j]) + strlen(pview->alias_put[j]) + 80;
      ALLOC(p, plen);
      snprintf(p, plen, "#define %s(i) %s", pview->def_put[j], pview->alias_put[j]);
      if (!in_list(p)) {
	add_list(p);
	ODB_fprintf(cfp,"%s /* '%s' */\n", p, pview->tag[j]);
      }
      FREE(p);
    }
    
    k = 0;
    for (j=nselect_all+nwhere+norderby; j<nselect_all+nwhere+norderby+nselsym; j++, k++) {
      char *p;
      int plen;

      if (IS_HASH(pview->selsym[k]->name)) continue;
      if (IS_DOLLAR(pview->selsym[k]->name)) continue;
      if (IS_BSNUM(pview->selsym[k]->name)) continue;
      
      plen = strlen(pview->def_put[j]) + strlen(pview->alias_put[j]) + 80;
      ALLOC(p, plen);
      sprintf(p, "#define %s(i) %s", pview->def_put[j], pview->alias_put[j]);
      if (!in_list(p)) {
	add_list(p);
	ODB_fprintf(cfp,"%s /* '%s' */\n", p, pview->tag[j]);
      }
      FREE(p);
    }
  }
  
  destroy_list();
}


PRIVATE void
write_hollerith(FILE *cfp)
{
  /* Hollerith symbols (if any) */
  extern int ODB_hollerith_strings;
  int nhs = ODB_hollerith_strings;

  FPINF_KEYNUM(strings,nhs);
  if (nhs > 0) {
    ODB_Symbol *psym;
    int j = 0;
    NL(1);
    for (psym = ODB_start_symbol(); psym != NULL; psym = psym->next) {
      if (psym->kind == ODB_STRING || psym->kind == ODB_WC_STRING) {
	ODB_fprintf(cfp, "#define %s \"%s\" /* [%d] */\n",psym->name,psym->sorig,j);
	FPINF_KEYSTR(j,psym->sorig);
	j++;
      } /* if (psym->kind == ODB_STRING || psym->kind == ODB_WC_STRING) */
    }
    NL(1);
  } /* if (nhs > 0) */
  FPINF_KEYEND(strings);
}


PRIVATE int
write_setsymbols(FILE *cfp, const char *prefix, Boolean is_a_view)
{
  int count = 0;
  /* Write SET-symbols */
  ODB_Symbol *psym;

  for (psym = ODB_start_symbol(); psym != NULL; psym = psym->next) {
    if (psym->kind == ODB_USDNAME) {
      char *s = psym->name;
      uint do_write = is_a_view ? /* write only if found in WHERE-statement or SELECT-expression */
	(DONE_SYM(psym->flag, 4) || DONE_SYM(psym->flag, 5)) : 
	1;                      /* Otherwise write always */
      if (s && do_write && !IS_POOLNO(s) && !IS_USDHASH(s)) {
	char *tmp;
	const char *theprefix;
	ALLOC(tmp, strlen(s+1) + 5);
	sprintf(tmp, "USD_%s", s+1);
	
	has_USD_symbols = 1;
	count++;
	
	if (is_a_view && psym->only_view) {
	  theprefix = "PRIVATE";
	}
	else {
	  theprefix = prefix;
	}
	
	if (strequ(theprefix, "extern")) {
	  ODB_fprintf(cfp,"%s double %s_%s; /* %s */\n",
		      theprefix, tmp, odb_label, s);
	}
	else {
	  ODB_fprintf(cfp,"%s double %s_%s = %.14g; /* %s */\n",
		      theprefix, tmp, odb_label, psym->dval, s);
	}

	FREE(tmp);
      } /* if (do_write) */
    }
  }
  if (has_USD_symbols) NL(1);
  return count;
}

PUBLIC Boolean
ODB_IsSimpleExpr(const ODB_Tree *pnode, Boolean funcs_are_simple)
{
  Boolean is_simple = 0;
  if (pnode) {
    int what = pnode->what;
    switch (what) {
    case ODB_NUMBER:
    case ODB_NAME:
    case ODB_HASHNAME:
    case ODB_USDNAME:
      is_simple = 1;
      break;
    case ODB_FUNC:
    case ODB_FUNCAGGR:
      is_simple = funcs_are_simple;
      break;
    default:
      is_simple = 0;
      break;
    }
  }
  return is_simple;
}

PUBLIC char *
dump_s(char *in, ODB_Tree *pnode, int flag, Boolean *subexpr_is_aggrfunc)
{
  char *s = NULL;
  int len = STRLEN(in);
  Boolean Master = subexpr_is_aggrfunc ? 1 : 0;
  static int aggrfunc_count = 0;

  if (Master) aggrfunc_count = 0;

  if (pnode) {
    int what = pnode->what;

    switch (what) {

    case ODB_MATCH:
      {
	int j, nummatch = pnode->argc; /* Ought to be 1 */
	int slen = len + 1;
	ALLOC(s,slen);
	*s = '\0';
	if (in) strcat(s,in);
	for (j=0; j<nummatch; j++) {
	  ODB_Match_t *match = pnode->argv[j];
	  ODB_Tree *expr = match->expr;
	  char *formula = dump_s(NULL,expr,flag,NULL);
	  slen += STRLEN(formula) + 20;
	  REALLOC(s,slen);
	  if (j>0) strcat(s,",");
	  strcat(s,"'");
	  strcat(s,formula);
	  strcat(s,"'");
	  FREE(formula);
	} /* for (j=0; j<nummatch; j++) */
      }
      break;

    case ODB_COND:
      { /* left ? middle : right */
	ODB_Tree *left   = pnode->argv[0]; char *lhs = dump_s(NULL,left,flag,NULL);
	ODB_Tree *middle = pnode->argv[1]; char *mid = dump_s(NULL,middle,flag,NULL);
	ODB_Tree *right  = pnode->argv[2]; char *rhs = dump_s(NULL,right,flag,NULL);
	int slen = len + strlen(lhs) + strlen(mid) + strlen(rhs) + 30;
	ALLOC(s,slen);
	snprintf(s,slen,"%s(%s?%s:%s)",in?in:"",lhs,mid,rhs);
	FREE(lhs);
	FREE(mid);
	FREE(rhs);
      }
      break;

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
      {
	Boolean lhs_simple = ODB_IsSimpleExpr(pnode->argv[0],1);
	char *lhs = dump_s(NULL,pnode->argv[0],flag,NULL);
	int lenlhs = STRLEN(lhs);
	Boolean rhs_simple = ODB_IsSimpleExpr(pnode->argv[1],1);
	char *rhs = dump_s(NULL,pnode->argv[1],flag,NULL);
	char *op = ODB_keymap(what);
	char *blank = "";
	char *newline = blank;
	int slen = len + strlen(lhs) + strlen(op) + strlen(rhs) + 20;
	if (flag == 1 && lenlhs > linelen_threshold) {
	  /* if no newlines yet, add a newline */
	  /* if since last newline > linelen_threshold, add a newline */
	  char *pnl = strrchr(lhs,'\n');
	  if (!pnl || lhs + lenlhs - pnl > linelen_threshold) newline = "\n";
	}
	ALLOC(s,slen);
	snprintf(s,slen,"%s%s%s%s%s%s%s%s%s%s",
		         in?in:"",
		           lhs_simple ? "" : "(",
		             lhs,
		               lhs_simple ? "" : ")",
		                 blank,
		                   op,
		                     newline,
		                       rhs_simple ? "" : "(",
		                         rhs,
		                           rhs_simple ? "" : ")"
		 );
	FREE(lhs);
	FREE(rhs);
      }
      break;

    case ODB_UNARY_PLUS:
    case ODB_UNARY_MINUS:
    case ODB_NOT:
      {
	Boolean lhs_simple = ODB_IsSimpleExpr(pnode->argv[0],1);
	char *lhs = dump_s(NULL,pnode->argv[0],flag,NULL);
	char *op = ODB_keymap(what);
	int slen = len + strlen(op) + strlen(lhs) + 10;
	ALLOC(s,slen);
	snprintf(s,slen,"%s%s%s%s%s",in?in:"",op,
		 lhs_simple ? "" : "(",
		 lhs,
		 lhs_simple ? "" : ")"
		 );
	FREE(lhs);
      }
      break;

    case ODB_NUMBER:
      {
	int slen = len + 40 + 1;
	ALLOC(s,slen);
	if (flag == 1) {
	  snprintf(s,slen,"%s((double)%.14g)",in?in:"",pnode->dval);
	}
	else {
	  snprintf(s,slen,"%s%.14g",in?in:"",pnode->dval);
	}
      }
      break;

    case ODB_NAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	int slen = len + strlen(psym->name) + 20;
	char *expanded = (flag == 1) ? ODB_expand_sym(psym,"F(i)",NULL,NULL) : NULL;
	slen += STRLEN(expanded);
	ALLOC(s,slen);
	snprintf(s,slen,"%s%s",in?in:"",expanded?expanded:psym->name);
	FREE(expanded);
      }
      break;
 
    case ODB_STRING:
    case ODB_WC_STRING:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *name = dblquotes(psym->name);
	int slen = len + strlen(name) + 20;
	ALLOC(s,slen);
	snprintf(s,slen,"%s '%s'",in?in:"",name);
	FREE(name);
      }
      break;
 
    case ODB_BSNUM:
      {
	ODB_Symbol *psym = pnode->argv[0];
	int slen = len + strlen(psym->name) + 20;
	ALLOC(s,slen);
	snprintf(s,slen,"%s%s",in?in:"",psym->name);
      }
      break;

    case ODB_HASHNAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	int slen = len + strlen(psym->name+1) + ((flag == 1) ? strlen("_ROWIDX") : 0) + 2;
	ALLOC(s,slen);
	if (flag == 1) {
	  snprintf(s,slen,"%s%s_ROWIDX",in?in:"",psym->name+1);
	}
	else {
	  snprintf(s,slen,"%s%s",in?in:"",psym->name);
	}
      }
      break;

    case ODB_USDNAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *name = psym->name;
	int slen = len + 
	  (((flag == 1) && IS_POOLNO(name)) ? strlen(POOLNO) : strlen(name+1)) + 
	  ((flag == 1) ? strlen("lc_USDHASH_") : 0) + 2;
	ALLOC(s,slen);
	if (flag == 1) {
	  if (IS_USDDOTHASH(name)) {
	    char *sh = STRDUP(name);
	    int sh_len = STRLEN(sh);
	    char *dot = strchr(sh,'.');
	    if (dot) *dot = 'D'; /* IS_USDDOTHASH(s) */
	    sh[sh_len-1] = '\0'; /* replace '#' with '\0' */
	    snprintf(s,slen,"%slc_USDHASH_%s",in?in:"",sh+1);
	    FREE(sh);
	  }
	  else {
	    snprintf(s,slen,"%slc_USD_%s",in?in:"",IS_POOLNO(name) ? POOLNO : name+1);
	  }
	}
	else {
	  snprintf(s,slen,"%s%s",in?in:"",name);
	}
	has_USD_symbols = 1;
      }
      break;

    case ODB_FUNCAGGR:
      if (!Master) aggrfunc_count++;
      /* no "break;" i.e. fall through purposely */

    case ODB_FUNC:
      {
	ODB_Symbol *pfunc = pnode->argv[0];
	int j, numargs = pnode->argc - 1;
	struct { char *s; } *args = NULL;
	int jstart;
	int slen = len + ((flag == 1) ? strlen("Func_") : 0) + 1 + strlen(pfunc->name) + 3;
	switch (flag) {
	case 2:
	  jstart = (pnode->joffset > 0) ? 2 : 1;
	  break;
	case 3:
	  jstart = 1 + pnode->joffset;
	  break;
	default:
	  jstart = 1;
	  break;
	}
	if (numargs > 0) {
	  ALLOC(args,numargs);
	  for (j=jstart; j<=numargs; j++) {
	    ODB_Tree *arg = pnode->argv[j];
	    char *x = dump_s(NULL,arg,flag,NULL);
	    slen += STRLEN(x);
	    if (j<numargs) slen += 1;
	    args[j-1].s = x;
	  }
	}
	ALLOC(s,slen);
	if (flag == 1) {
	  snprintf(s,slen,"%sFunc_%s%s(",
		   in?in:"",
		   (what == ODB_FUNCAGGR) ? "_" : "", /* prepend underscore with aggregate funcs */
		   pfunc->name);
	}
	else {
	  snprintf(s,slen,"%s%s%s(",
		   in?in:"",
		   (what == ODB_FUNCAGGR) ? "_" : "", /* prepend underscore with aggregate funcs */
		   pfunc->name);
	}
	if (numargs > 0) {
	  for (j=jstart; j<=numargs; j++) {
	    char *x = args[j-1].s;
	    strcat(s,x);
	    if (j<numargs) strcat(s,",");
	    FREE(x);
	  }
	  FREE(args);
	}
	strcat(s,")");
      }
      break;

    default:
      if (in) {
	SETMSG3("Unsupported expression after '%s' : Key '%s' (%d) not implemented in this context\n",
		in, ODB_keymap(what), what);
      }
      else {
	SETMSG2("Unsupported expression : Key '%s' (%d) not implemented in this context\n",
		ODB_keymap(what), what);
      }
      YYerror(msg);
      break;
    }
  }
  FREE(in);

  if (Master && subexpr_is_aggrfunc) {
    *subexpr_is_aggrfunc = (aggrfunc_count > 0) ? 1 : 0;
    aggrfunc_count = 0;
  }

  return s;
}

PUBLIC void 
dump_c(FILE *cfp, int lineno, ODB_Tree *pnode)
{
  ODB_lineno = lineno;

  if (pnode) {
    int what = pnode->what;

    switch (what) {

    case ODB_MATCH:
      {
	int j, nummatch = pnode->argc; /* Ought to be 1 */
	for (j=0; j<nummatch; j++) {
	  ODB_Match_t *match = pnode->argv[j];
#if 0
	  char *formula = match->formula; /* We don't need this, actually */
	  ODB_Symbol *psym = match->symexpr->argv[0]; /* This is formula's S2D_<number>-massagee!! */
	  if (j>0) ODB_fprintf(cfp, ", ");
	  ODB_fprintf(cfp, "EvalMe(%s)", psym->name);
#else
	  ODB_Tree *expr = match->expr;
	  if (j>0) ODB_fprintf(cfp, ", ");
	  ODB_fprintf(cfp, "EvalMe(");
	  dump_c(cfp,lineno,expr);
	  ODB_fprintf(cfp, ")");
#endif
	} /* for (j=0; j<nummatch; j++) */
      }
      break;

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
      ODB_fprintf(cfp,"(");
      dump_c(cfp,lineno,pnode->argv[0]);
      ODB_fprintf(cfp," %s ",ODB_keymap(what));
      if (info_dump < 2 && (what == ODB_AND || what == ODB_OR)) {
	int tabs = (!info_dump) ? 6 : 1;
	NL(1); TAB(tabs);
      }
      dump_c(cfp,lineno,pnode->argv[1]);
      ODB_fprintf(cfp,")");
      break;

    case ODB_UNARY_PLUS:
    case ODB_UNARY_MINUS:
    case ODB_NOT:
      ODB_fprintf(cfp,"(");
      ODB_fprintf(cfp,"%s",ODB_keymap(what));
      dump_c(cfp,lineno,pnode->argv[0]);
      ODB_fprintf(cfp,")");
      break;

    case ODB_NUMBER:
      if (!info_dump) {
	ODB_fprintf(cfp,"((double)%.14g)",pnode->dval);
      }
      else {
	ODB_fprintf(cfp,"%.14g",pnode->dval);
      }
      break;

    case ODB_STRING:
    case ODB_WC_STRING:
    case ODB_NAME:
    case ODB_BSNUM:
      {
	ODB_Symbol *psym = pnode->argv[0];
	ODB_fprintf(cfp,"%s",psym->name);
      }
      break;

    case ODB_HASHNAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	if (!info_dump) {
	  ODB_fprintf(cfp,"%s_ROW",psym->name+1);
	}
	else {
	  ODB_fprintf(cfp,"%s",psym->name);
	}
      }
      break;

    case ODB_FUNC:
    case ODB_FUNCAGGR:
      {
	ODB_Symbol *pfunc = pnode->argv[0];
	int j, numargs = pnode->argc - 1;
	int jstart = 1;
	ODB_fprintf(cfp,"%s%s%s(",
		    info_dump ? "" : "Func_",
		    (what == ODB_FUNCAGGR) ? "_" : "",
		    pfunc->name);
	if (ddl_piped && pnode->joffset > 0) jstart = 2;
	for (j=jstart; j<=numargs; j++) {
	  ODB_Tree *arg = pnode->argv[j];
	  dump_c(cfp,lineno,arg);
	  if (j<numargs) ODB_fprintf(cfp,", ");
	}
	ODB_fprintf(cfp,")");
      }
      break;

    case ODB_FILE:
      {
	ODB_Symbol *pfunc = pnode->argv[0];
	char *filename = dblquotes(pnode->argv[1]);
	ODB_Tree *arg = pnode->argv[2];
	ODB_fprintf(cfp,"%s%s(\"%s\",",info_dump ? "" : "Func_",pfunc->name,filename);
	dump_c(cfp,lineno,arg);
	ODB_fprintf(cfp,")");
	FREE(filename);
      }
      break;

    case ODB_USDNAME:
      {
	ODB_Symbol *psym = pnode->argv[0];
	char *s = psym->name;
	if (!info_dump) {
	  if (IS_USDDOTHASH(s)) {
	    char *sh = STRDUP(s);
	    int sh_len = STRLEN(sh);
	    char *dot = strchr(sh,'.');
	    if (dot) *dot = 'D'; /* IS_USDDOTHASH(s) */
	    sh[sh_len-1] = '\0'; /* replace '#' with '\0' */
	    ODB_fprintf(cfp,"lc_USDHASH_%s",sh+1);
	    FREE(sh);
	  }
	  else {
	    ODB_fprintf(cfp,"lc_USD_%s",IS_POOLNO(s) ? POOLNO : s+1);
	  }
	}
	else {
	  ODB_fprintf(cfp,"%s", s);
	}
	has_USD_symbols = 1;
      }
      break;

    case ODB_WHERE_SYMBOL:
      {
	if (!info_dump) {
	  char *s = pnode->argv[0];
	  char *pvar = NULL;
	  int isym;
	  (void) ODB_split(s, NULL, &pvar, NULL, NULL, NULL);
	  isym = atoi(pvar);
	  ODB_fprintf(cfp,"tmp%d",isym);
	  FREE(pvar);
	}
	else {
	  char *tag = pnode->argv[2];
	  char *s = strchr(tag,':');
	  char *name = s ? STRDUP(s+1) : STRDUP(tag);
	  /* if (s) CHECK_LENOFS(name); */
	  ODB_fprintf(cfp,"%s", name);
	  FREE(name);
	}
      }
      break;

    case ODB_SET:
      break;

    case ODB_TYPE:
      {
	ODB_Type *ptype = pnode->argv[0];

	if (ptype) {
	  /* typedef for TYPE */
	  int nsym = ptype->nsym;
	  Boolean processed = ptype->processed;

	  if (nsym > 0 && !processed) {
	    char *name = ptype->type->name;

	    ODB_fprintf(cfp,"/* *************** TYPE \"%s\" *************** */\n",name);
	    if (nsym == 1 && ptype->no_members) {
	      int i = 0;
	      ODB_fprintf(cfp,
		      "\n/* typedef %s %s; */\n",
		      ptype->sym[i]->name, ptype->member[i]->name);
	      ODB_fprintf(cfp,
		      "#define %s %s\n",
		      ptype->member[i]->name, ptype->sym[i]->name);
	    }
	    else {
	      int i;
	      ODB_fprintf(cfp,"\n/* typedef struct {\n");
	      for (i=0; i<nsym; i++) {
		char *s = 
		  get_typedef(ptype->sym[i]->name, 
			      ptype->member[i]->name, NULL);
		TAB(1); ODB_fprintf(cfp,"%s;\n",s);
		FREE(s);
	      }
	      ODB_fprintf(cfp,"} %s; */\n",name);
	    }
	    NL(1);

	    ptype->processed = 1;
	    ODB_fprintf(cfp,"/* *************** End of TYPE \"%s\" *************** */\n",name);
	  }
	}
      }
      break;

    case ODB_TABLE:
      {
	ODB_Table *ptable = pnode->argv[0];

	ODB_pushFILE(fptable);

	if (ptable) {
	  char *typetag = NULL;
	  char *nametag = NULL;
	  char *exttypetag = NULL;
	  char *extnametag = NULL;
	  int typetag_len = 0;
	  int nametag_len = 0;
	  int exttypetag_len = 0;
	  int extnametag_len = 0;
	  int nsym = ptable->nsym;
	  int nmem = 0;

	  if (nsym > 0) {
	    char *name = ptable->table->name;
	    Boolean dummy_table = is_dummy_table(name);
	    int i, j;
	    int ntypes_table = ntypes;
	    Boolean typemask[NTYPES];

	    typemask[0] = 1;

	    if (verbose)
	      fprintf(stderr,"genc: Processing TABLE='%s' (dummy_table status %d) ...\n",
		      name, (int)dummy_table);

	    if (fptable != fpdevnull) {
	      char *p, *dofunc;
	      int len;
	      
	      len = strlen(odb_label) + strlen(name) + 4;
	      ALLOC(dofunc, len);
	      sprintf(dofunc,"%s_T_%s", odb_label, name);

	      len = strlen(dofunc) + 3;
	      ALLOC(p, len);
	      sprintf(p, "%s.c", dofunc);

	      if (ABS(filtered_info) < 2) {
		fptable = FOPEN(p, "w");
	      }
	      else {
		fptable = NULL;
	      }
	      FREE(p);

	      ALLOC(flist, 1);
	      flist->filename = STRDUP(dofunc);
	      flist->create_so = 0;
	      flist->next = NULL;
	      if (!flist_start) {
		flist_start = flist;
	      }
	      else {
		flist_last->next = flist;
	      }
	      flist_last = flist;

	      FREE(dofunc);
	    }

	    cfp = fptable;

	    ODB_fprintf(cfp,"#undef ODB_MAINCODE\n");
	    ODB_fprintf(cfp,"#undef IS_a_VIEW\n");
	    ODB_fprintf(cfp,"#define IS_a_TABLE_%s 1\n",name);
	    /* set_optlevel(cfp, 2); */
	    set_optlevel(cfp, 0);
	    ODB_fprintf(cfp,"#include \"%s.h\"\n",odb_label);
	    NL(1);

	    ODB_pushFILE(cfp);
	    cfp = fphdr;

	    /* typedef for TABLE */

	    ODB_fprintf(cfp,"#if defined(IS_a_TABLE_%s) || defined(ODB_MAINCODE) || defined(IS_a_VIEW)\n",name);
	    NL(1);
	    ODB_fprintf(cfp,
			"/* *************** TABLE \"%s\" : "
			"appearance order#%d, hierarchy rank# %d, weight = %.6f *************** */\n",
			name, ptable->tableno, ptable->rank, ptable->wt);
	    ODB_fprintf(cfp,"\ntypedef struct {\n");
	    TAB(1); ODB_fprintf(cfp,"int Handle;\n");
	    TAB(1); ODB_fprintf(cfp,"int PoolNo;\n");
	    TAB(1); ODB_fprintf(cfp,"ODB_Funcs *Funcs;\n");
	    TAB(1); ODB_fprintf(cfp,"boolean Is_loaded;\n");
	    TAB(1); ODB_fprintf(cfp,"boolean Is_new;\n");
	    TAB(1); ODB_fprintf(cfp,"boolean Swapped_out;\n");
	    TAB(1); ODB_fprintf(cfp,"boolean Byteswap;\n");
	    TAB(1); ODB_fprintf(cfp,"int IO_method;\n");
	    TAB(1); ODB_fprintf(cfp,"int Created[2];\n");
	    TAB(1); ODB_fprintf(cfp,"int LastUpdated[2];\n");
	    TAB(1); ODB_fprintf(cfp,"int Ncols;\n");
	    TAB(1); ODB_fprintf(cfp,"int Nrows;\n");
	    TAB(1); ODB_fprintf(cfp,"int Nalloc;\n");
	    TAB(1); ODB_fprintf(cfp,"int Numreqs;\n");

	    if (!dummy_table) {
	      for (i=0; i<nsym; i++) {
		ODB_Type *ptype = ptable->type[i];
		char *tyname = get_typename(ptype);
		char *symname = ptable->sym[i]->name;
		char *s = get_typedef(tyname, symname, NULL);
		
		if (ptype->bitstream) {
		  char *p = strchr(s,' ');
		  if (p) *p = ',';
		  TAB(1);
		  ODB_fprintf(cfp,
			      "DeclareDS(%s); /* Alias bit stream -typedef \"%s\" */\n",
			      s,tyname);
		}
		else {
		  char *p = strchr(s,' ');
		  if (p) *p = ',';
		  TAB(1);
		  ODB_fprintf(cfp, "DeclareDS(%s);\n", s);
		}

		FREE(s);
		
	      } /* for (i=0; i<nsym; i++) */
	    } /* if (!dummy_table) */

	    ODB_fprintf(cfp,"} TABLE_%s;\n",name);
	    NL(1);
	    ODB_fprintf(cfp,
		    "#endif /* defined(IS_a_TABLE_%s) || defined(ODB_MAINCODE)  || defined(IS_a_VIEW) */\n",
		    name);
	    NL(1);

	    /* Tags for TABLE */

	    nametag = STRDUP(ODB_tag_delim);
	    nametag_len = 2;
	    typetag = STRDUP(ODB_tag_delim);
	    typetag_len = 2;
	    extnametag = STRDUP(ODB_tag_delim);
	    extnametag_len = 2;
	    exttypetag = STRDUP(ODB_tag_delim);
	    exttypetag_len = 2;

	    ODB_fprintf(cfp,"#if !defined(ODB_MAINCODE) && defined(IS_a_TABLE_%s)\n",name);
	    ODB_fprintf(cfp,"extern const ODB_Tags *%s_Set_T_%s_TAG(int *ntag_out, int *nmem_out);\n",odb_label,name);
	    ODB_fprintf(cfp,"extern const ODB_PrepTags *%s_Set_T_%s_PREPTAG(int *npreptag_out);\n",odb_label,name);
	    ODB_fprintf(cfp,"#elif defined(ODB_MAINCODE)\n");
	    ODB_fprintf(cfp,"PRIVATE const ODB_Tags *%s_T_%s_TAG = NULL;\n",odb_label,name);
	    ODB_fprintf(cfp,"PRIVATE const ODB_PrepTags *%s_T_%s_PREPTAG = NULL;\n",odb_label,name);
	    ODB_fprintf(cfp,"PRIVATE int %s_nT_%s_TAG = 0;\n",odb_label,name);
	    ODB_fprintf(cfp,"PRIVATE int %s_nT_%s_PREPTAG = 0;\n",odb_label,name);
	    ODB_fprintf(cfp,"PRIVATE int %s_nT_%s_MEM = 0;\n",odb_label,name);

	    ODB_fprintf(cfp,"PUBLIC const ODB_Tags *\n%s_Set_T_%s_TAG(int *ntag_out, int *nmem_out)\n",odb_label,name);
	    ODB_fprintf(cfp,"{\n");

	    if (!dummy_table) {
	      nmem = 0;
	      TAB(1); ODB_fprintf(cfp,"if (!%s_T_%s_TAG) {\n", odb_label,name);
	      TAB(2); ODB_fprintf(cfp,"int ntag = %d;\n", nsym);
	      TAB(2); ODB_fprintf(cfp,"ODB_Tags *T = NULL;\n");
	      TAB(2); ODB_fprintf(cfp,"CALLOC(T, ntag);\n");
	      
	      for (i=0; i<nsym; i++) {
		ODB_Type *ptype = ptable->type[i];
		char *tyname = get_typename(ptype);
		char *symname = ptable->sym[i]->name;
		int ksym = ptype->nsym;
		ODB_Symbol **member = ptype->member;
		
		TAB(2);
		ODB_fprintf(cfp,
			    "{ static char s[] = \"%s:%s@%s\"; T[%d].name = s; }\n",
			    tyname,symname,name,i);
	      
		nametag_len += strlen(symname) + 1 + strlen(name) + 1;
		REALLOC(nametag, nametag_len);
		strcat(nametag,symname);
		strcat(nametag,"@");
		strcat(nametag,name);
		strcat(nametag,ODB_tag_delim);
		
		typetag_len += strlen(tyname) + 1;
		REALLOC(typetag, typetag_len);
		strcat(typetag,tyname);
		strcat(typetag,ODB_tag_delim);

		if (ksym > 1) {
		  int ii;
		  TAB(2);
		  ODB_fprintf(cfp,
			      "T[%d].nmem = %d;\n",
			      i,ksym);

		  ODB_fprintf(cfp,
			      "ALLOC(T[%d].memb, %d);\n",i,ksym);
		  nmem += ksym;
		  for (ii=0; ii<ksym; ii++) {
		    char *member_name = member[ii]->name;
		    int nbits = ptype->len[ii];
		    char cbits[3];
		    TAB(3);
		    ODB_fprintf(cfp,
				"{ static char s[] = \"%s %d\"; T[%d].memb[%d] = s; }\n",
				member_name, nbits, i, ii);
		    
		    extnametag_len += strlen(symname) + 1 + strlen(member_name) + 1 + strlen(name) + 1;
		    REALLOC(extnametag, extnametag_len);
		    strcat(extnametag,symname);
		    strcat(extnametag,".");
		    strcat(extnametag,member_name);
		    strcat(extnametag,"@");
		    strcat(extnametag,name);
		    strcat(extnametag,ODB_tag_delim);
		    
		    exttypetag_len += strlen("bit") + 2 + 1; /* The "2" since MAXBITS is always < 100 */
		    REALLOC(exttypetag, exttypetag_len);
		    strcat(exttypetag,"bit");
		    snprintf(cbits,sizeof(cbits),"%d",nbits);
		    strcat(exttypetag,cbits);
		    strcat(exttypetag,ODB_tag_delim);
		  } /* for (ii=0; ii<ksym; ii++) */
		}
	      } /* for (i=0; i<nsym; i++) */
	      TAB(2); ODB_fprintf(cfp,"%s_T_%s_TAG = T;\n",odb_label,name);
	      TAB(2); ODB_fprintf(cfp,"%s_nT_%s_TAG = ntag;\n",odb_label,name);
	      TAB(2); ODB_fprintf(cfp,"%s_nT_%s_MEM = %d;\n",odb_label,name,nmem);
	  
	      TAB(1); ODB_fprintf(cfp,"}\n");
	    }

	    TAB(1); ODB_fprintf(cfp,"if (ntag_out) *ntag_out = %s_nT_%s_TAG;\n",odb_label,name);
	    TAB(1); ODB_fprintf(cfp,"if (nmem_out) *nmem_out = %s_nT_%s_MEM;\n",odb_label,name);
	    TAB(1); ODB_fprintf(cfp,"return %s_T_%s_TAG;\n}\n",odb_label,name);
	    
	    if (nmem > 0) {
	      char *p = NULL;
	      extnametag_len += nametag_len - 1;
	      p = STRDUP(extnametag+1); /* All, but the first ODB_tag_delim */
	      FREE(extnametag);
	      ALLOC(extnametag,extnametag_len);
	      strcpy(extnametag,nametag);
	      strcat(extnametag,p);
	      FREE(p);
	      exttypetag_len += typetag_len - 1;
	      p = STRDUP(exttypetag+1); /* All, but the first ODB_tag_delim */
	      FREE(exttypetag);
	      ALLOC(exttypetag,exttypetag_len);
	      strcpy(exttypetag,typetag);
	      strcat(exttypetag,p);
	      FREE(p);
	    }
	    
	    ODB_fprintf(cfp,"PUBLIC const ODB_PrepTags *\n%s_Set_T_%s_PREPTAG(int *npreptag_out)\n",odb_label,name);
	    ODB_fprintf(cfp,"{\n");
	    if (!dummy_table) {
	      TAB(1); ODB_fprintf(cfp,"if (!%s_T_%s_PREPTAG) {\n", odb_label,name);
	      TAB(2); ODB_fprintf(cfp,"int npreptag = %d;\n", (nmem > 0) ? 4 : 2);
	      TAB(2); ODB_fprintf(cfp,"ODB_PrepTags *T = NULL;\n");
	      TAB(2); ODB_fprintf(cfp,"ALLOC(T, npreptag);\n");
	      if (nmem > 0) {
		TAB(2); ODB_fprintf(cfp,"T[0].tagtype = preptag_name;\n");
		TAB(2); ODB_fprintf(cfp,"T[0].longname_len = %d;\n",strlen(nametag));
		TAB(2); ODB_fprintf(cfp,"{ static char s[] =");
		TagStrip(cfp,3,nametag,";\n");
		TAB(3); ODB_fprintf(cfp,"T[0].longname = s; }\n");

		TAB(2); ODB_fprintf(cfp,"T[1].tagtype = preptag_type;\n");
		TAB(2); ODB_fprintf(cfp,"T[1].longname_len = %d;\n",strlen(typetag));
		TAB(2); ODB_fprintf(cfp,"{ static char s[] =");
		TagStrip(cfp,3,typetag,";\n");
		TAB(3); ODB_fprintf(cfp,"T[1].longname = s; }\n");

		TAB(2); ODB_fprintf(cfp,"T[2].tagtype = preptag_extname;\n");
		TAB(2); ODB_fprintf(cfp,"T[2].longname_len = %d;\n",strlen(extnametag));
		TAB(2); ODB_fprintf(cfp,"{ static char s[] =");
		TagStrip(cfp,3,extnametag,";\n");
		TAB(3); ODB_fprintf(cfp,"T[2].longname = s; }\n");

		TAB(2); ODB_fprintf(cfp,"T[3].tagtype = preptag_exttype;\n");
		TAB(2); ODB_fprintf(cfp,"T[3].longname_len = %d;\n",strlen(exttypetag));
		TAB(2); ODB_fprintf(cfp,"{ static char s[] =");
		TagStrip(cfp,3,exttypetag,";\n");
		TAB(3); ODB_fprintf(cfp,"T[3].longname = s; }\n");
	      }
	      else {
		TAB(2); ODB_fprintf(cfp,"T[0].tagtype = (preptag_name | preptag_extname);\n");
		TAB(2); ODB_fprintf(cfp,"T[0].longname_len = %d;\n",strlen(nametag));
		TAB(2); ODB_fprintf(cfp,"{ static char s[] =");
		TagStrip(cfp,3,nametag,";\n");
		TAB(3); ODB_fprintf(cfp,"T[0].longname = s; }\n");

		TAB(2); ODB_fprintf(cfp,"T[1].tagtype = (preptag_type | preptag_exttype);\n");
		TAB(2); ODB_fprintf(cfp,"T[1].longname_len = %d;\n",strlen(typetag));
		TAB(2); ODB_fprintf(cfp,"{ static char s[] =");
		TagStrip(cfp,3,typetag,";\n");
		TAB(3); ODB_fprintf(cfp,"T[1].longname = s; }\n");
	      }
	      TAB(2); ODB_fprintf(cfp,"%s_T_%s_PREPTAG = T;\n",odb_label,name);
	      TAB(2); ODB_fprintf(cfp,"%s_nT_%s_PREPTAG = npreptag;\n",odb_label,name);
	      TAB(1); ODB_fprintf(cfp,"}\n");
	    }
	    TAB(1); ODB_fprintf(cfp,"if (npreptag_out) *npreptag_out = %s_nT_%s_PREPTAG;\n",odb_label,name);
	    TAB(1); ODB_fprintf(cfp,"return %s_T_%s_PREPTAG;\n}\n",odb_label,name);

	    ODB_fprintf(cfp,"#endif\n");
	    NL(1);
	    
	    FREE(nametag);
	    FREE(typetag);
	    FREE(extnametag);
	    FREE(exttypetag);

	    ODB_fprintf(cfp,"#if defined(ODB_MAINCODE)\n");
	    NL(1);

	    cfp = ODB_popFILE();

	    (void) write_setsymbols(cfp, "extern", 0);
	    NL(1);

	    /* CANCEL function for TABLE : removed */
	
	    /* 
	    ODB_fprintf(fphdr,"extern void %s_Ccl_T_%s(void *T);\n",odb_label,name);
	    ODB_fprintf(cfp,"PUBLIC void\n%s_Ccl_T_%s(void *T)\n{ }\n",odb_label,name);
	    NL(1);
	    */


	    /* PACK function for TABLE */
	    
	    ODB_fprintf(fphdr,"extern int %s_Pack_T_%s(void *T);\n",odb_label,name);
	    ODB_fprintf(cfp,"PUBLIC int\n%s_Pack_T_%s(void *T)\n{\n",odb_label,name);
	    if (!dummy_table) {
	      TAB(1); ODB_fprintf(cfp,"int Nbytes = 0;\n");
	      TAB(1); ODB_fprintf(cfp,"TABLE_%s *P = T;\n",name);
	      TAB(1); ODB_fprintf(cfp,"Packed_DS *PDS;\n");
	      TAB(1); ODB_fprintf(cfp,"if (P->Is_loaded) {\n");
	      for (i=0; i<nsym; i++) {
		ODB_Type *ptype = ptable->type[i];
		char *tyname = get_typename(ptype);
		char *symname = ptable->sym[i]->name;
		TAB(2); 
		ODB_fprintf(cfp,"PDS = PackDS(P, %s, %s, %s); ",odb_label,tyname,symname);
		ODB_fprintf(cfp,"CHECK_PDS_ERROR(%d);\n",i+1);
	      }
	      TAB(1); ODB_fprintf(cfp,"}\n");
	      TAB(1); ODB_fprintf(cfp,"return Nbytes;\n");
	    }
	    else {
	      TAB(1); ODB_fprintf(cfp,"return 0;\n");
	    }
	    ODB_fprintf(cfp,"}\n");
	    NL(1);

	    /* UNPACK function for TABLE */
	    
	    ODB_fprintf(fphdr,"extern int %s_Unpack_T_%s(void *T);\n",odb_label,name);
	    ODB_fprintf(cfp,"PUBLIC int\n%s_Unpack_T_%s(void *T)\n{\n",odb_label,name);
	    if (!dummy_table) {
	      TAB(1); ODB_fprintf(cfp,"int Nbytes = 0;\n");
	      TAB(1); ODB_fprintf(cfp,"TABLE_%s *P = T;\n",name);
	      TAB(1); ODB_fprintf(cfp,"if (P->Is_loaded) {\n");
	      for (i=0; i<nsym; i++) {
		ODB_Type *ptype = ptable->type[i];
		char *tyname = get_typename(ptype);
		char *symname = ptable->sym[i]->name;
		TAB(2); ODB_fprintf(cfp,"UseDS(P, %s, %s, %s); Nbytes += BYTESIZE(P->%s.d);\n",
				    odb_label,tyname,symname,symname);
	      }
	      TAB(1); ODB_fprintf(cfp,"}\n");
	      TAB(1); ODB_fprintf(cfp,"return Nbytes;\n");
	    }
	    else {
	      TAB(1); ODB_fprintf(cfp,"return 0;\n");
	    }
	    ODB_fprintf(cfp,"}\n");
	    NL(1);

	    /* SELECT function for TABLE */
	    
	    ODB_fprintf(fphdr,"extern int %s_Sel_T_%s(void *T, ODB_PE_Info *PEinfo, int phase, void *feedback);\n",
		    odb_label,name);
	    ODB_fprintf(cfp,"PUBLIC int\n%s_Sel_T_%s(void *T, ODB_PE_Info *PEinfo, int phase, void *feedback)\n{\n",
		    odb_label,name);
	    if (!dummy_table) {
	      TAB(1); ODB_fprintf(cfp,"TABLE_%s *P = T;\n",name);
	      TAB(1); ODB_fprintf(cfp,"ODBMAC_TABLE_DELAYED_LOAD(%s);\n",name);
	      TAB(1); ODB_fprintf(cfp,"return P->Nrows;\n");
	    }
	    else {
	      TAB(1); ODB_fprintf(cfp,"return 0;\n");
	    }
	    ODB_fprintf(cfp,"}\n");
	    NL(1);

	    for (j=0; j<ntypes_table; j++) {
	      char *pKey = Key[j];
	      char *pExtType = ExtType[j];
	      
	      if (!typemask[j]) continue;

	      /* GET function for TABLE */

	      init_list(NULL);
	      
	      for (i=0; i<nsym; i++) {
		char *p;
		ODB_Type *ptype = ptable->type[i];
		char *type = get_typename(ptype);
		
		ALLOC(p, strlen(type) + 3);
		sprintf(p, "/%s/", type);
		
		if (!in_list(p)) {
		  add_list(p);
		  vector_loop(cfp, 0);
		}
		
		FREE(p);
	      } /* for (i=0; i<nsym; i++) */
	      NL(1);
	      
	      ODB_fprintf(fphdr,"PreGetTable(%s, %s, %s, %s);\n", odb_label, pKey, pExtType, name);
	      ODB_fprintf(cfp,"PreGetTable(%s, %s, %s, %s)\n", odb_label, pKey, pExtType, name);
	      
	      if (!dummy_table) {
		for (i=0; i<nsym; i++) {
		  char *s = ptable->sym[i]->name;
		  ODB_Type *ptype = ptable->type[i];
		  char *type = get_typename(ptype);
		  char *datatype = NULL;
		  (void)get_PKmethod(type,NULL,&datatype,NULL);
		  
		  TAB(1);
		  ODB_fprintf(cfp,
			      "Call_CopyGet_TABLE(%s, %s, %d, %s, %s, D, %s, Count, DATATYPE_%s);\n",
			      odb_label, pKey, i+1, name, type, s, datatype);
		  FREE(datatype);
		}
	      }

	      ODB_fprintf(cfp,"PostGetTable(%s, %s, %s)\n", pKey, pExtType, name);
	      NL(1);
	      
	      /* PUT function for TABLE */
	      
	      init_list(NULL);
	      
	      for (i=0; i<nsym; i++) {
		char *p;
		/* char *s = ptable->sym[i]->name; */
		ODB_Type *ptype = ptable->type[i];
		char *type = get_typename(ptype);
		
		ALLOC(p, strlen(type) + 3);
		sprintf(p, "/%s/", type);
		
		if (!in_list(p)) {
		  add_list(p);
		  vector_loop(cfp, 0);
		}
		
		FREE(p);
	      }
	      NL(1);

	      ODB_fprintf(fphdr,"PrePutTable(%s, %s, %s, %s);\n", odb_label, pKey, pExtType, name);
	      ODB_fprintf(cfp,"PrePutTable(%s, %s, %s, %s)\n", odb_label, pKey, pExtType, name);
	      
	      if (!dummy_table) {
		for (i=0; i<nsym; i++) {
		  char *s = ptable->sym[i]->name;
		  ODB_Type *ptype = ptable->type[i];
		  char *type = get_typename(ptype);
		  char *datatype = NULL;
		  (void)get_PKmethod(type,NULL,&datatype,NULL);
		  
		  TAB(1);
		  ODB_fprintf(cfp,
			      "Call_CopyPut_TABLE(%s, %s, %d, %s, %s, %s, D, Count, DATATYPE_%s);\n",
			      odb_label, pKey, i+1, name, type, s, datatype);
		  FREE(datatype);
		}
	      }

	      ODB_fprintf(cfp,"PostPutTable(%s, %s, %s)\n", pKey, pExtType, name);
	      NL(1);
	    } /* for (j=0; j<ntypes_table; j++) */
	    
	    /* Load function for TABLE */

	    ODB_fprintf(fphdr,"PreLoadTable(%s, %s);\n",odb_label,name);
	    ODB_fprintf(cfp,"PreLoadTable(%s, %s);\n",odb_label,name);

	    if (!dummy_table) {
	      for (i=0; i<nsym; i++) {
		char *s = ptable->sym[i]->name;
		ODB_Type *ptype = ptable->type[i];
		char *type = get_typename(ptype);
		char *datatype = NULL;
		(void)get_PKmethod(type,NULL,&datatype,NULL);
		TAB(1); 
		ODB_fprintf(cfp,
			    "Call_Read_DS(%s, fp_idx, filename, Nbytes, %s, DATATYPE_%s, %s);\n",
			    odb_label, type, datatype, s);
		FREE(datatype);
	      }
	    }

	    ODB_fprintf(cfp,"PostLoadTable(%s)\n",name);
	    NL(1);

	    /* Store function for TABLE */

	    ODB_fprintf(fphdr,"PreStoreTable(%s, %s);\n",odb_label,name);
	    ODB_fprintf(cfp,"PreStoreTable(%s, %s);\n",odb_label,name);

	    if (!dummy_table) {
	      for (i=0; i<nsym; i++) {
		char *s = ptable->sym[i]->name;
		ODB_Type *ptype = ptable->type[i];
		char *type = get_typename(ptype);
		char *datatype = NULL;
		(void)get_PKmethod(type,NULL,&datatype,NULL);
		TAB(1); 
		ODB_fprintf(cfp,
			    "Call_Write_DS(%s, fp_idx, filename, Nbytes, %s, DATATYPE_%s, %s);\n",
			    odb_label, type, datatype, s);
		FREE(datatype);
	      }
	    }

	    ODB_fprintf(cfp,"PostStoreTable(%s)\n",name);
	    NL(1);

	    /* Dimension function for TABLE */

	    ODB_fprintf(cfp,"DefineLookupTable(%s)\n",name);
	    NL(1);

	    ODB_fprintf(fphdr,
		    "extern void %s_Dim_T_%s(void *T, int *Nrows, int *Ncols, ",
		    odb_label,name);
	    ODB_fprintf(cfp,
		    "PUBLIC void\n%s_Dim_T_%s(void *T, int *Nrows, int *Ncols,\n",
		    odb_label,name);
	    TAB(1);
	    ODB_fprintf(fphdr,"int *Nrowoffset, int ProcID);\n");
	    ODB_fprintf(cfp,"int *Nrowoffset, int ProcID)\n{\n");

	    TAB(1); ODB_fprintf(cfp,"TABLE_%s *P = T;\n",name);
	    TAB(1); ODB_fprintf(cfp,"Call_LookupTable(%s, P, Nrows, Ncols);\n",name);
	    TAB(1); ODB_fprintf(cfp,"if (Nrowoffset) *Nrowoffset = 0;\n");
	    ODB_fprintf(cfp,"}\n");
	    NL(1);

	    /* Removing from memory / swapping out a TABLE */

	    ODB_fprintf(fphdr,
		    "extern void %s_Swapout_T_%s(void *T);\n",
		    odb_label,name);
	    ODB_fprintf(cfp,
		    "PUBLIC void\n%s_Swapout_T_%s(void *T)\n{\n",
		    odb_label,name);
	    TAB(1); ODB_fprintf(cfp,"TABLE_%s *P = T;\n",name);
	    TAB(1); ODB_fprintf(cfp,"int Nbytes = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"int Count = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"int PoolNo = P->PoolNo;\n");
	    TAB(1); ODB_fprintf(cfp,"FILE *do_trace = NULL;\n");

	    TAB(1); ODB_fprintf(cfp,"if (P->Swapped_out || !P->Is_loaded) return;\n");
	    TAB(1); ODB_fprintf(cfp,"do_trace = ODB_trace_fp();\n");

	    if (!dummy_table) {
	      for (i=0; i<nsym; i++) {
		char *s = ptable->sym[i]->name;
		TAB(1); ODB_fprintf(cfp,"FreeDS(P, %s, Nbytes, Count);\n",s);
	      } /* for (i=0; i<nsym; i++) */
	    }

	    TAB(1); ODB_fprintf(cfp,"P->Nrows = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"P->Nalloc = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"P->Is_loaded = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"P->Swapped_out = P->Is_new ? 0 : 1;\n");

	    TAB(1); ODB_fprintf(cfp,"ODBMAC_TRACE_SWAPOUT(%s,%d);\n",name,nsym);

	    ODB_fprintf(cfp,"}\n");
	    NL(1);

	    /* Remove function for TABLE */

	    ODB_fprintf(cfp,"DefineRemoveTable(%s, %s)\n", odb_label, name);
	    NL(1);

	    /* TABLE SQL : A dummy */

	    ODB_fprintf(fphdr, "extern int %s_Sql_T_%s(",odb_label,name);
	    ODB_fprintf(fphdr, "FILE *fp, int mode, ");
	    ODB_fprintf(fphdr, "const char *prefix, const char *postfix, char **sqlout);\n");

	    ODB_fprintf(cfp, "PUBLIC int\n%s_Sql_T_%s(",odb_label,name);
	    ODB_fprintf(cfp, "FILE *fp, int mode, const char *prefix, const char *postfix, char **sqlout) ");
	    ODB_fprintf(cfp, "{ ODBMAC_TABLESQL(); }\n");
	    NL(1);

	    /* Sorting keys for TABLE : removed */

	    /*
	    ODB_fprintf(fphdr,
		    "extern int *%s_SortKeys_T_%s(void *V, int *NSortKeys);\n",
		    odb_label,name);
	    ODB_fprintf(cfp,
		    "PUBLIC int *\n%s_SortKeys_T_%s(void *V, int *NSortKeys)\n{\n",
		    odb_label,name);
	    TAB(1); ODB_fprintf(cfp,"TABLE_%s *P = V;\n",name);
	    TAB(1); ODB_fprintf(cfp,"if (NSortKeys) *NSortKeys = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"return NULL;\n");
	    ODB_fprintf(cfp,"}\n");
	    NL(1);
	    */

	    /* Initialization function for TABLE */

	    ODB_fprintf(fphdr,
		    "extern void *%s_Init_T_%s(void *T, ODB_Pool *Pool, int Is_new, int IO_method, int it, int dummy);\n",
		    odb_label,name);
	    ODB_fprintf(cfp,
		    "PUBLIC void *\n%s_Init_T_%s(void *T, ODB_Pool *Pool, int Is_new, int IO_method, int it, int dummy)\n{\n",
		    odb_label,name);
	    TAB(1); ODB_fprintf(cfp,"TABLE_%s *P = T;\n",name);
	    TAB(1); ODB_fprintf(cfp,"int PoolNo = Pool->poolno;\n");
	    TAB(1); ODB_fprintf(cfp,"ODB_Funcs *pf;\n");
	    TAB(1); ODB_fprintf(cfp,"static ODB_CommonFuncs *pfcom = NULL; /* Shared between pools & threads */\n");
	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_START(%s_Init_T_%s);\n",odb_label,name);
	    }
	    TAB(1); ODB_fprintf(cfp,"if (!P) ALLOC(P, 1);\n");
	    TAB(1); ODB_fprintf(cfp,"PreInitTable(P, %d);\n",nsym);

	    if (!dummy_table) {
	      for (i=0; i<nsym; i++) {
		char *s = ptable->sym[i]->name;
		ODB_Type *ptype = ptable->type[i];
		char *type = get_typename(ptype);
		char *datatype = NULL;
		int pmethod = get_PKmethod(type,NULL,&datatype,NULL);
		
		TAB(1); 
		ODB_fprintf(cfp,"InitDS(%s, DATATYPE_%s, %s, %s, %d);\n",type, datatype, s, name, pmethod);
		FREE(datatype);
	      } /* for (i=0; i<nsym; i++) */
	    }

	    TAB(1); ODB_fprintf(cfp,"if (!pfcom) { /* Initialize once only */\n");
	    TAB(2); ODB_fprintf(cfp,"CALLOC(pfcom,1);\n");

	    TAB(2); ODB_fprintf(cfp,"{ static char s[] = \"@%s\"; pfcom->name = s; }\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->is_table = 1;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->is_considered = 0;\n");

	    TAB(2); ODB_fprintf(cfp,"pfcom->ntables = 0;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->ncols = %d;\n",nsym);

	    TAB(2); ODB_fprintf(cfp,"pfcom->tableno = %d;\n",ptable->tableno);
	    TAB(2); ODB_fprintf(cfp,"pfcom->rank = %d;\n",ptable->rank);
	    TAB(2); ODB_fprintf(cfp,"pfcom->wt = %.6f;\n",ptable->wt);

	    TAB(2); ODB_fprintf(cfp,"pfcom->tags = %s_Set_T_%s_TAG(&pfcom->ntag, &pfcom->nmem);\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->preptags = %s_Set_T_%s_PREPTAG(&pfcom->npreptag);\n",odb_label,name);
	    
	    TAB(2); ODB_fprintf(cfp,"pfcom->Info = NULL;\n");

	    TAB(2); ODB_fprintf(cfp,"pfcom->create_index = 0;\n");

	    TAB(2); ODB_fprintf(cfp,"pfcom->init = %s_Init_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->swapout = %s_Swapout_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->dim = %s_Dim_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->sortkeys = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->update_info = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->aggr_info = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->getindex = NULL; /* N/A */\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->putindex = NULL; /* N/A */\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->select = %s_Sel_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->remove = %s_Remove_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->peinfo = NULL; /* N/A */\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->cancel = NULL;\n");

	    for (j=0; j<ntypes; j++) {
	      char *pKey = Key[j];
	      char *pKeyInfo = KeyInfo[j];

	      if (!dummy_table && typemask[j]) {
		TAB(2); ODB_fprintf(cfp,"pfcom->%sget = %s_%sGet_T_%s; /* %s dbmgr */\n", 
				pKey, odb_label, pKey, name, pKeyInfo);
		TAB(2); ODB_fprintf(cfp,"pfcom->%sput = %s_%sPut_T_%s; /* %s dbmgr */\n", 
				pKey, odb_label, pKey, name, pKeyInfo);
	      }
	      else {
		TAB(2); ODB_fprintf(cfp,"pfcom->%sget = NULL; /* no %s dbmgr allowed */\n", 
				pKey, pKeyInfo);
		TAB(2); ODB_fprintf(cfp,"pfcom->%sput = NULL; /* no %s dbmgr allowed */\n", 
				pKey, pKeyInfo);
	      }
	    }
	    
	    TAB(2); ODB_fprintf(cfp,"pfcom->load = %s_Load_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->store = %s_Store_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->pack = %s_Pack_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->unpack = %s_Unpack_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->sql = %s_Sql_T_%s;\n",odb_label,name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->ncols_aux = 0;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->colaux = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->has_select_distinct = 0;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->has_usddothash = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"} /* if (!pfcom) */\n");

	    TAB(1); ODB_fprintf(cfp,"ALLOC(pf, 1);\n");
	    TAB(1); ODB_fprintf(cfp,"pf->it = it;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->data = P;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->Res = NULL;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->tmp = NULL;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->pool = Pool;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->common = pfcom;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->next = NULL;\n");

	    TAB(1); ODB_fprintf(cfp,"P->Funcs = pf;\n");
	    TAB(1); ODB_fprintf(cfp,"P->Handle = P->Funcs->pool->handle;\n");

	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_END(0);\n");
	    }
	    TAB(1); ODB_fprintf(cfp,"return P;\n");
	    ODB_fprintf(cfp,"}\n");
	    NL(1);

	    ODB_fprintf(fphdr,"\n");
	    ODB_fprintf(fphdr,"#endif /* defined(ODB_MAINCODE) */\n");
	    ODB_fprintf(fphdr,"\n");

	    ODB_fprintf(cfp,"/* *************** End of TABLE \"%s\" *************** */\n",name);

	    if (cfp != fpdevnull) FCLOSE(cfp);
	  } /* if (nsym > 0) */
	} /* if (ptable) */

	fptable = ODB_popFILE();
      }
      break;

    case ODB_VIEW:
      {
	ODB_View *pview = pnode->argv[0];

	ODB_pushFILE(cfp);

	if (pview) {
	  int nfrom_start = pview->nfrom_start; /* deferred implementation */
	  int nfrom = pview->nfrom;

	  if (nfrom >= 0) {
	    int i, j;
	    int ntypes_view = ntypes;
	    int typemask[NTYPES];
	    char **consider_tables = NULL;
	    char *name = pview->view->name;
	    ODB_Tree *pcond = pview->cond;
	    int nselect = pview->nselect;
	    int nselect_all = pview->nselect_all;
	    int nwhere = pview->nwhere;
	    int norderby = pview->norderby;
	    int nuniqueby = pview->nuniqueby;
	    int nall = nselect_all + nwhere;
	    int ncols_aux = 0;
	    int nselsym = pview->nselsym;
	    int maxfrom = pview->maxfrom;
	    int oneif = 0;
	    int has_link = 0;
	    int linkless = 0;
	    Boolean *done = NULL;
	    Boolean *orphan = NULL;
	    FILE *fpview = NULL;
	    char *dofunc = NULL;
	    int pcond_USD_symbols = 0;
	    int selsym_USD_symbols = 0;
	    int count_USD_symbols = 0;
	    ODB_AndList *andlist = pview->andlist;
	    int andlen = pview->andlen;
	    struct {
	      int len;
	      int i;
	      int protected;
	      int level_plus_one;
	    } *subloop = NULL;
	    int *close_bracket = NULL;
	    char **inlined_len = NULL;
	    int numsg = 0;
	    char *typetag = NULL;
	    char *nametag = NULL;
	    int typetag_len = 0;
	    int nametag_len = 0;
	    int numands = 0;
	    int nwhere_dupl = 0;
	    ODB_Symbol *sym_maxcount = ODB_lookup(ODB_USDNAME, usd_maxcount, NULL);

	    typemask[0] = 1;

	    if (verbose) 
	      fprintf(stderr,"genc: Processing VIEW='%s' ...\n",name);

	    if (!pcond) {
	      extern ODB_Tree *YACC_dummy_cond; /* from yacc.y */
	      pcond = pview->cond = YACC_dummy_cond;
	    }

	    if (!pcond) {
	      SETMSG1("*** Internal error: Unexpected empty WHERE-condition in view '%s'",
		      name);
	      YYerror(msg);
	    }

	    if (!sym_maxcount) {
	      /* ODBTk's $__maxcount__ has not been assigned -> assign it with mdi = ABS(RMDI) */
	      sym_maxcount = ODB_new_symbol(ODB_USDNAME, usd_maxcount);
	      sym_maxcount->dval = mdi;
	    }

	    pcond_USD_symbols = assign_USD_symbols(NULL, pcond, 0, 4, NULL, name);

	    CALLOC(tmpsym, nall);

	    {
	      char *p;
	      int len;
	      
	      len = strlen(name) + strlen(odb_label) + 2;
	      ALLOC(dofunc, len);
	      snprintf(dofunc, len, "%s_%s", odb_label, name);

	      len = strlen(dofunc) + 3;
	      ALLOC(p, len);
	      snprintf(p, len, "%s.c", dofunc);

	      if (ABS(filtered_info) < 2) {
		fpview = FOPEN(p, "w");
	      }
	      else {
		fpview = NULL;
	      }
	      FREE(p);

	      if (ABS(filtered_info) > 0) {
		if (filtered_info < 0) {
		  len = strlen(dofunc) + 6;
		  ALLOC(p, len);
		  snprintf(p, len, "%s.info", dofunc);
		}
		else {
		  p = STRDUP("stdout");
		}
		fpinf = FOPEN(p, "w");
		FPINF_MAGIC();
		FPINF_KEYLINE(database, pview->db->dbname);
		FPINF_KEYLINE(srcpath, pview->db->srcpath);
		FPINF_KEYLINE(datapath, pview->db->datapath);
		FPINF_KEYLINE(idxpath, pview->db->idxpath);
		FPINF_KEYLINE(poolmask, pview->db->poolmask);
		FPINF_KEYLINE(viewname, name);
		FPINF_KEYLINE(has_select_distinct, pview->select_distinct ? "1" : "0");
		FPINF_KEYLINE(has_count_star, pview->has_count_star ? "1" : "0");
		FPINF_KEYLINE(has_aggrfuncs, pview->select_aggr_flag ? "1" : "0");
		FPINF_KEYLINE(has_thin, pview->has_thin ? "1" : "0");
		if (filtered_info < 0) ODB_fprintf(stdout,"%s\n",name);
		FREE(p);
	      }
	      else {
		fpinf = NULL;
	      }

	      ALLOC(flist, 1);
	      flist->filename = STRDUP(dofunc);
	      flist->create_so = 1;
	      flist->next = NULL;
	      if (!flist_start) {
		flist_start = flist;
	      }
	      else {
		flist_last->next = flist;
	      }
	      flist_last = flist;
	    }

	    cfp = fpview;

	    ODB_fprintf(cfp,"#define IS_a_VIEW 1\n");

	    ODB_fprintf(cfp,"/* Compilation options used :\n%s",ARGH);
	    process_one_tables(cfp, "\n\t ","");
	    ODB_fprintf(cfp,"\n\n*/\n\n");

	    /* set_optlevel(cfp, 2); */
	    set_optlevel(cfp, 0);
	    ODB_fprintf(cfp,"#include \"%s.h\"\n",odb_label);
	    NL(1);

	    write_sql(cfp, pview, 1);
	    NL(1);
	
	    write_sql_for_info();

	    /* Echo ODB_CONSIDER_TABLES */
	    ODB_fprintf(cfp,"\n#define ODB_CONSIDER_TABLES \"/");
	    CALLOC(consider_tables, nfrom);
	    for (i=0; i<nfrom; i++) {
	      const char *s = pview->from[i]->table->name;
	      ODB_fprintf(cfp,"%s/",s);
	      consider_tables[i] = STRDUP(s);
	    }
	    ODB_fprintf(cfp,"%s\"\n", (nfrom > 0) ? "" : "/");
	    NL(1);
	    
	    FPINF_KEYNUM(from, nfrom);
	    for (i=0; i<nfrom; i++) {
	      const char *s = pview->from[i]->table->name;
	      double wt = pview->from[i]->wt;
	      FPINF_NUMITEM_WT(i,s,wt);
	    }
	    FPINF_KEYEND(from);

	    write_macros(cfp);
	    NL(1);

	    write_hollerith(cfp);
	    NL(1);

	    /* Count no. of (distinct) dollar-variables in SELECT-expression(s) */
      
	    selsym_USD_symbols = 0;
	    if (pview->sel) {
	      for (i=0; i<nselect; i++) {
		if (pview->is_formula[i]) {
		  ODB_Tree *expr = pview->sel[i]->expr;
		  selsym_USD_symbols += assign_USD_symbols(NULL, expr, 0, 5, NULL, name);
		}
	      } /* for (i=0; i<nselect; i++) */
	    }

	    count_USD_symbols = write_setsymbols(cfp, "extern", 1);
	    NL(1);

	    /* Calculate the total number of auxiliary columns (ncols_aux) needed */

	    ncols_aux = 0;
	    if (pview->sel) {
	      for (i=0; i<nselect; i++) {
		if (pview->is_formula[i]) {
		  uint flag = pview->sel[i]->aggr_flag;
		  if (flag != ODB_AGGR_NONE) ncols_aux += pview->sel[i]->ncols_aux;
		}
	      } /* for (i=0; i<nselect; i++) */
	    }
	    pview->ncols_aux = ncols_aux;

	    /* The special HASH-symbols to enable row number (#) access */
	    ODB_fprintf(cfp,"#if !defined(K0_lo_var) && !defined(K0_lo_const)\n");
	    ODB_fprintf(cfp,"#define K0_lo 0\n");
	    ODB_fprintf(cfp,"#endif\n");
	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      /* The meaning of the little advertised '#tblname' changed on 19/01/2004 by SS
		 to denote absolute rowid of table '@tblname', foreach at pool;
		 It used to denote relative rowid i.e. absolute - minus link_offset;
		 If you had used '#tblname' before, you may now have to change it and
		 calculate relativeness yourself by '#tblname - tblname.offset@parent + 1' */
	      /* ODB_fprintf(cfp,"#define %s_ROW ((double)((K%d - K%d_lo) + 1))\n",s,i,i); */
	      ODB_fprintf(cfp,"#define %s_ROW ((double)(K%d + 1))\n",s,i);
	      if (pview->has_formulas) {
		ODB_fprintf(cfp,"#define %s_ROWIDX ((double)(P->Index_%s[i] + 1))\n",s,s);
	      }
	    }
	    NL(1);

	    ALLOC(done, nall);
	    ZEROIT(done, 0, nall);

	    /* typedef for VIEW */

	    ODB_fprintf(cfp,"/* *************** VIEW \"%s\" *************** */\n",name);
	    ODB_fprintf(cfp,"\ntypedef struct {\n");
	    TAB(1); ODB_fprintf(cfp,"int Handle;\n");
	    TAB(1); ODB_fprintf(cfp,"int PoolNo;\n");
	    TAB(1); ODB_fprintf(cfp,"ODB_Funcs *Funcs;\n");
	    TAB(1); ODB_fprintf(cfp,"int Ncols;\n");
	    TAB(1); ODB_fprintf(cfp,"int Nrows;\n");
	    TAB(1); ODB_fprintf(cfp,"int  USD_symbols;\n");
	    TAB(1); ODB_fprintf(cfp,"int  Replicate_PE;\n");
	    TAB(1); ODB_fprintf(cfp,"int  Npes;\n");
	    TAB(1); ODB_fprintf(cfp,"int *NrowVec;\n");
	    TAB(1); ODB_fprintf(cfp,"int *NrowOffset;\n");
	    TAB(1); ODB_fprintf(cfp,"int  NSortKeys;\n");
	    if (pview->norderby > 0) {
	      TAB(1); ODB_fprintf(cfp,"int  SortKeys[%d];\n",pview->norderby);
	    }
	    else {
	      TAB(1); ODB_fprintf(cfp,"int *SortKeys;\n");
	    }
	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      TAB(1); 
	      if ((pview->active[i] & 0x1) == 0x1) {
		ODB_fprintf(cfp,"ODBMAC_VIEW_TABLEDECL_WITH_INDEX(%s);\n",s);
	      }
	      else {
		ODB_fprintf(cfp,"ODBMAC_VIEW_TABLEDECL_NO_INDEX(%s);\n",s);
	      }
	    }
	    TAB(1); ODB_fprintf(cfp,"uint can_UPDATE[RNDUP_DIV(%d,MAXBITS)];\n",nselect+ncols_aux);
	    if (pview->select_aggr_flag) {
	      TAB(1); ODB_fprintf(cfp,"int aggr_func_flag[%d];\n",nselect+ncols_aux);
	      TAB(1); ODB_fprintf(cfp,"int numaggr;\n");
	      TAB(1); ODB_fprintf(cfp,"Boolean all_aggr;\n");
	    }

	    ODB_fprintf(cfp,"} VIEW_%s;\n",name);
	    NL(1);

	    /* Setup master-slave relationship of ALIGNed items -- if merge_table_indices */

	    if (merge_table_indices) {
	      for (i=0; i<nfrom; i++) {
		/* Loop over "slaves" */
		if ((pview->active[i] & 0x1) == 0x1) {
		  char *slave = pview->from[i]->table->name;
		  for (j=0; j<nfrom; j++) {
		    /* Loop over ALL the "master" candidates : pick the first one */
		    if ((pview->active[j] & 0x1) == 0x1) {
		      char *master = pview->from[j]->table->name;
		      if (is_master(master,slave)) {
			pview->merged_with[i] = STRDUP(master); /* Yesh, index IS indeed 'i' !! */
			break; /* First found; quit loop "for (j=0; j<i; j++)" */
		      }
		    } /* if ((pview->active[j] & 0x1) == 0x1) */
		  } /* for (j=0; j<i; j++) */
		} /* if ((pview->active[i] & 0x1) == 0x1) */
	      } /* for (i=0; i<nfrom; i++) */
	    } /* if (merge_table_indices) */

	    /* Tags for VIEW */
	    
	    {
	      /* Check # of duplicates in WHERE first */
	      nwhere_dupl = 0;
	      init_list(NULL);
	      for (i=nselect_all; i<nselect_all+nwhere; i++) {
		const char *p =  pview->tag[i] ? pview->tag[i] : pview->where[i-(nselect_all)]->name;
		if (!in_list(p)) add_list(p); else nwhere_dupl++;
	      }
	      destroy_list();
	    }

	    ODB_fprintf(cfp,"PRIVATE int nV_%s_TAG = %d;\n",
		    name,nselect+ncols_aux+nselsym+nwhere-nwhere_dupl+norderby+nuniqueby);
	    ODB_fprintf(cfp,"PRIVATE int nV_%s_MEM = 0;\n",name);
	    NL(1);

	    nametag = STRDUP(ODB_tag_delim);
	    nametag_len = 2;
	    typetag = STRDUP(ODB_tag_delim);
	    typetag_len = 2;

	    ODB_fprintf(cfp,"PRIVATE const ODB_Tags V_%s_TAG[%d] = {\n",
		    name,nselect+ncols_aux+nselsym+nwhere-nwhere_dupl+norderby+nuniqueby);

	    /* Record the current ODB_maxcols() for use by ORDERBY ABS-sorting */
	    if (fpinf) ODB_fprintf(fpinf, "\n/maxcols=%d\n", ODB_maxcols());

	    TAB(1); ODB_fprintf(cfp,"/* === SELECT-symbols (count = %d) === */\n", nselect);
	    FPINF_KEY(select); /* Don't know the total length */
	    for (i=0; i<nselect; i++) {
	      int flag = 0;
	      const char *ptag = pview->tag[i];
	      const char *poslen = pview->poslen[i];
	      char *ptype = NULL;
	      char *pvar = NULL;
	      char *pmem = NULL;
	      char *ptable = NULL;
	      char *poffset = NULL;
	      const ODB_Symbol *nicksym = (pview->sel && pview->sel[i]) ? pview->sel[i]->nicksym : NULL;
	      char *nickname = HAS_NICKNAME(nicksym) ? nicksym->name : NULL;
	      Boolean nickname_allocated = 0;
	      Boolean is_usddothash = 0;

	      if (pview->sel && pview->sel[i] && pview->sel[i]->formula) {
		const char *f1 = pview->sel[i]->formula+1;
		is_usddothash = IS_USDDOTHASH(f1);
	      }

	      if (nickname && !pview->is_formula[i]) {
		/* Make sure we are not dealing with 
		   linklen_t:LINKLEN(child_table)@parent_table or
		   linkoffset_t:LINKOFFSET(child_table)@parent_table */
		char *atag = STRDUP(ptag);
		char *acolon = strchr(atag,':');
		if (acolon) {
		  char *alink = acolon + 1;
		  char *aat = strchr(alink,'@');
		  if (aat) {
		    char *atype = atag;
		    *acolon = '\0';
		    if ((strequ(atype,LINKLENTYPE) && strnequ(alink,"LINKLEN(",8)) ||
			(strequ(atype,LINKOFFSETTYPE) && strnequ(alink,"LINKOFFSET(",11))) {
		      nickname = NULL;
		    }
		  }
		}
		FREE(atag);
	      }

	      if (nickname) {
		char *pnick = STRDUP(nickname);
		char *pat = strrchr(pnick,'@');
		char *pcolon = strchr(pnick,':');
		if ((pcolon && !pat) || (pcolon && pat && pcolon < pat)) {
		  char *datatype = NULL;
		  char *test_type = pnick;
		  *pcolon++ = '\0';
		  (void)get_PKmethod(test_type, NULL, &datatype, NULL);
		  if (!strequ(datatype,"UNDEF")) {
		    /* This is a known (pre-defined) datatype */
		    ptype = STRDUP(test_type);
		    nickname = STRDUP(pcolon);
		    nickname_allocated = 1;
		  }
		  FREE(datatype);
		}
		FREE(pnick);
	      }

	      if (pview->is_formula[i]) {
		if (!ptype) ptype = STRDUP("Formula");
		ptable = STRDUP("Formula");
	      }
	      else {
		(void) ODB_split(ptag, ptype ? NULL : &ptype, &pvar, &pmem, &ptable, &poffset);
	      }

	      if (nickname) {
		int pnlen;
		char *pnick = nickname_allocated ? nickname : STRDUP(nickname);
		char *pat = strrchr(pnick,'@');
		Boolean valid_chars = 1;
		if (pat) {
		  /* Check that all the following chars are valid chars for a table */
		  const char *c = pat;
		  valid_chars = (STRLEN(c) > 1) ? 1 : 0;
		  while ( valid_chars && *++c ) {
		    if (!IS_VARCHAR(c)) valid_chars = 0;
		  }
		}
		if (pat && valid_chars) {
		  *pat++ = '\0';
		  pnlen = strlen(pnick) + 1 + strlen(pat) + 1;
		}
		else {
		  pnlen = strlen(pnick) + 1 + strlen(ptable) + 1;
		}
		ALLOC(pvar,pnlen);
		snprintf(pvar,pnlen,"%s@%s",pnick,(pat && valid_chars) ? pat : ptable);
		FREE(pnick);
	      }
	      else if (pview->is_formula[i]) {
		const char *pcolon = strchr(ptag,':');
		if (pcolon) { ++pcolon; pvar = STRDUP(pcolon); }
		else { pvar = STRDUP("<unknown>"); }
	      }

	      if (nickname || pview->is_formula[i]) {
		nametag_len += strlen(pvar) + 1;
		REALLOC(nametag, nametag_len);
		strcat(nametag, pvar);
		strcat(nametag,ODB_tag_delim);
		if (pview->is_formula[i]) {
		  flag = pview->sel[i]->aggr_flag ? 4 : 2;
		  if (!pview->sel[i]->formula_out) {
		    /* This seems to happen, where the query is table-less */
		    ODB_Tree *expr = pview->sel[i]->expr;
		    pview->sel[i]->formula_out = dump_s(NULL,expr,ddl_piped ? 2 : 0,NULL);
		  }
		  FPINF_NUMITEM(flag, pview->sel[i]->formula_out, NULL, NULL);
		}
		else {
		  flag = 1;
		}
	      }
	      else if (pmem) {
		nametag_len += strlen(pvar) + 1 + strlen(pmem) + 1 + strlen(ptable) + 1;
		REALLOC(nametag, nametag_len);
		strcat(nametag,pvar);
		strcat(nametag,".");
		strcat(nametag,pmem);
		strcat(nametag,"@");
		strcat(nametag,ptable);
		strcat(nametag,ODB_tag_delim);
		flag = 1;
	      }
	      else {
		nametag_len += strlen(pvar) + 1 + strlen(ptable) + 1;
		REALLOC(nametag, nametag_len);
		strcat(nametag,pvar);
		strcat(nametag,"@");
		strcat(nametag,ptable);
		strcat(nametag,ODB_tag_delim);
		flag = 1;
	      }
	      
	      typetag_len += strlen(ptype) + 1;
	      REALLOC(typetag, typetag_len);
	      strcat(typetag, ptype);
	      strcat(typetag,ODB_tag_delim);

	      TAB(1); ODB_fprintf(cfp,"{ \"%s\", %d, 0, NULL } ,\n", ptag, 
				  is_usddothash ? 1 : 0);
	      if (flag == 1) FPINF_NUMITEM(flag, ptag, poslen, poffset);
	      FREE(pvar);
	      FREE(pmem);
	      FREE(ptable);
	      FREE(ptype);
	      FREE(poffset);
	    } /* for (i=0; i<nselect; i++) */

	    {
	      const int flag = 8; /* Auxiliary aggregate column */
	      TAB(1); ODB_fprintf(cfp,"/* === Symbols for auxiliary columns (count = %d) === */\n",ncols_aux);
	      for (i=0; i<ncols_aux; i++) {
		TAB(1);
		ODB_fprintf(cfp,"{ \"Formula:ColAux_%d@Formula\", 0, 0, NULL },\n",i+1);
		FPINF_FMT2("%d %d\t0 0 0\n", flag, i+1); /* Output an auxiliary column# >= 1 */
	      }
	    }

	    { 
	      const int flag = 0; /* Possibly not a pure column, but part of a formula or formulas */
	      int k = 0;
	      TAB(1); ODB_fprintf(cfp,"/* === Symbols in SELECT-expressions (count = %d) === */\n",nselsym);
	      for (i=nselect_all+nwhere+norderby; i<nselect_all+nwhere+norderby+nselsym; i++, k++) {
		TAB(1); ODB_fprintf(cfp,"{ \"%s\", 0, 0, NULL },\n",
				    pview->tag[i] ? pview->tag[i] : pview->selsym[k]->name);
		FPINF_NUMITEM(flag, 
			      pview->tag[i] ? pview->tag[i] : pview->selsym[k]->name, 
			      pview->poslen[i], pview->offset[i]);
	      }
	    }

	    FPINF_KEYEND(select);

	    if (fpinf) {
	      FPINF_KEY(nickname); /* We don't know how many symbols have nickname; maybe none */
	      for (i=0; i<nselect; i++) {
		const ODB_Symbol *nicksym = (pview->sel && pview->sel[i]) ? pview->sel[i]->nicksym : NULL;
		const char *nickname = HAS_NICKNAME(nicksym) ? nicksym->name : NULL;
		if (nickname) {
		  FPINF_NICKNAME(i, nickname);
		}
	      }
	      FPINF_KEYEND(nickname);
	    } /* if (fpinf) */

	    {
	      TAB(1); ODB_fprintf(cfp,"/* === WHERE-symbols (count = %d) === */\n",nwhere-nwhere_dupl);
	      init_list(NULL);
	      FPINF_KEYNUM(wheresym,nwhere-nwhere_dupl);
	      for (i=nselect_all; i<nselect_all+nwhere; i++) {
		const char *p =  pview->tag[i] ? pview->tag[i] : pview->where[i-(nselect_all)]->name;
		if (!in_list(p)) {
		  add_list(p);
		  TAB(1); ODB_fprintf(cfp,"{ \"%s\", 0, 0, NULL },\n", p);
		  if (pview->tag[i]) {
		    FPINF_NUMITEM(1, pview->tag[i], pview->poslen[i], pview->offset[i]);
		  }
		  else {
		    FPINF_NUMITEM(0, pview->where[i-(nselect_all)]->name, NULL, NULL); 
		  }
		} /* if (!in_list(p)) */
	      }
	      destroy_list();
	      FPINF_KEYEND(wheresym);
	    }

	    TAB(1); ODB_fprintf(cfp,"/* === ORDERBY-symbols (count = %d) === */\n",norderby);
	    FPINF_KEYNUM(orderby,norderby);
	    for (i=nselect_all+nwhere; i<nselect_all+nwhere+norderby; i++) {
	      TAB(1); ODB_fprintf(cfp,"{ \"%s\", 0, %d, NULL },\n",
				  pview->tag[i],
				  pview->mkeys[i-(nselect_all+nwhere)]);
	      FPINF_NUMITEM(pview->mkeys[i-(nselect_all+nwhere)], 
			    pview->tag[i], pview->poslen[i], pview->offset[i]);
	    }
	    FPINF_KEYEND(orderby);

	    TAB(1); ODB_fprintf(cfp,"/* === UNIQUEBY-symbols (count = %d) === */\n",nuniqueby);
	    FPINF_KEYNUM(uniqueby,nuniqueby);
	    for (i=0; i<nuniqueby; i++) {
	      int k = (nselect_all + nwhere - nuniqueby) + i;
	      const int flag = 1;
	      char *ptag = pview->tag[k];
	      char *poslen = pview->poslen[k];
	      char *ptype = NULL;
	      char *pvar = NULL;
	      char *pmem = NULL;
	      char *ptable = NULL;

	      (void) ODB_split(ptag, &ptype, &pvar, &pmem, &ptable, NULL);

	      TAB(1); 
	      if (pmem) {
		ODB_fprintf(cfp,"{ \"%s:%s.%s@%s\", 0, 0, NULL } ,\n",
			    ptype, pvar, pmem, ptable);
		FPINF_NUMITEM_TYPE_VAR_MEM_TABLE(flag, ptype, pvar, pmem, ptable, poslen);
	      }
	      else {
		ODB_fprintf(cfp,"{ \"%s:%s@%s\", 0, 0, NULL } ,\n",
			    ptype, pvar, ptable);
		FPINF_NUMITEM_TYPE_VAR_MEM_TABLE(flag, ptype, pvar, NULL, ptable, poslen);
	      }
	      FREE(ptype);
	      FREE(pvar);
	      FREE(pmem);
	      FREE(ptable);
	    }
	    FPINF_KEYEND(uniqueby);
	    ODB_fprintf(cfp,"};\n");
	    NL(1);

	    if (fpinf && pcond) { /* WHERE-cond to info-file */
	      int lineno = ODB_lineno;
	      FILE *save_cfp = cfp;

	      cfp = fpinf;
	      info_dump = 1;

	      FPINF_KEYNUM(wherecond,1);
	      TAB(1);
	      dump_c(cfp, lineno, pcond);
	      NL(1);
	      FPINF_KEYEND(wherecond);

	      if (andlist && nfrom > 0) {
		/* Don't generate this for table-less queries */
		int ja;
		info_dump = 2;
		for (ja=0; ja<andlen; ja++) {
		  FPINF_KEYNUM(wherecond_and,andlist[ja].maxfrom);
		  TAB(1);
		  dump_c(cfp, lineno, andlist[ja].expr);
		  NL(1);
		  FPINF_KEYEND(wherecond_and);
		}
	      }

	      info_dump = 0;
	      cfp = save_cfp;
	      ODB_lineno = lineno;
	    } /* if (fpinf && pcond) */

	    ODB_fprintf(cfp,"PRIVATE int nV_%s_PREPTAG = 3;\n",name);
	    ODB_fprintf(cfp,"PRIVATE const ODB_PrepTags V_%s_PREPTAG[3] = {\n",name);
	    TAB(1); ODB_fprintf(cfp,"/* Prepared tags for faster codb_getnames() */\n");
	    TAB(1); ODB_fprintf(cfp,"{ (preptag_name | preptag_extname), %d,",strlen(nametag));
	    TagStrip(cfp,2,nametag," },\n");
	    TAB(1); ODB_fprintf(cfp,"{ (preptag_type | preptag_exttype), %d,",strlen(typetag));
	    TagStrip(cfp,2,typetag," },\n");
	    { /* A more robust list of tables that belong to the view */
	      char *tbltag = NULL;
	      int len = 2;
	      tbltag = STRDUP(ODB_tag_delim);
	      for (i=0; i<nfrom; i++) {
		len += strlen(consider_tables[i]) + 2;
		REALLOC(tbltag, len);
		strcat(tbltag,"@");
		strcat(tbltag,consider_tables[i]);
		strcat(tbltag,ODB_tag_delim);
		FREE(consider_tables[i]);
	      }
	      FREE(consider_tables);
	      TAB(1); ODB_fprintf(cfp,"{  preptag_tblname, %d,", strlen(tbltag));
	      TagStrip(cfp,2,tbltag," },\n");
	      FREE(tbltag);
	    }
	    ODB_fprintf(cfp,"};\n");
	    NL(1);

	    FREE(nametag);
	    FREE(typetag);

	    /* CANCEL function for VIEW */
	    
	    ODB_fprintf(cfp,"PRIVATE void\nCcl_V_%s(void *T)\n{\n",name);
	    TAB(1); ODB_fprintf(cfp,"ODBMAC_CCL_V_PRE(%s);\n",name);

	    for (i=0; i<nfrom; i++) {
	      if ((pview->active[i] & 0x1) == 0x1) { 
		char *s = pview->from[i]->table->name;
		TAB(1); ODB_fprintf(cfp,"FREEINDEX(%s);\n",s); 
	      }
	    }

	    ODB_fprintf(cfp,"}\n");
	    NL(1);

	    /* SELECT function */

	    /* SELECT (Pre) : Gather the size of the set */

	    ODB_fprintf(cfp,"PRIVATE int\nPrS_V_%s(FILE *do_trace,\n", name);
	    TAB(1); ODB_fprintf(cfp,"VIEW_%s *P, int it, ODB_PE_Info *PEinfo",name);

	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      ODB_fprintf(cfp,"\n");
	      TAB(1);
	      ODB_fprintf(cfp,"/* TABLE '%s' */ , int N%d, unsigned int BmapIdx%d[]",
			  s,i,i);
	    }

	    init_list(NULL);
	    ZEROIT(done, 0, nall);

	    for (i=nselect_all; i<nall; i++) {
	      char *call_arg = pview->call_arg[i];
	      char *pvar;
	      char *ptype;
	      char *p;
	      int plen;

	      if (!call_arg) continue;

	      (void) ODB_split(call_arg, &ptype, &pvar, NULL, NULL, NULL);
	      
	      plen = strlen(ptype) + strlen(pvar) + 10;
	      ALLOC(p, plen);
	      snprintf(p, plen, "%s %s[]", ptype, pvar);

	      if (!in_list(p)) {
		add_list(p);
		ODB_fprintf(cfp,",\n");
		TAB(1); ODB_fprintf(cfp,"const %s",p);
		done[i] = 1;
	      }

	      FREE(p);
	      FREE(pvar);
	      FREE(ptype);
	    }

	    destroy_list();

	    ODB_fprintf(cfp,")\n{\n",name);

	    (void) assign_USD_symbols(NULL, pcond, 0, 0, NULL, name); /* Reset flags */
	    if (pcond_USD_symbols > 0) {
	      (void) assign_USD_symbols(cfp, pcond, 1, 1, NULL, name);
	    }

	    TAB(1); 
	    ODB_fprintf(cfp,
			"double *Addr = (PEinfo && P->USD_symbols > 0) ? PEinfo->addr : NULL;\n");
	    TAB(1); ODB_fprintf(cfp,"int PE, PEstart = 1;\n");
	    TAB(1); ODB_fprintf(cfp,"int PEend = PEinfo ? PEinfo->npes : PEstart;\n");
	    TAB(1); ODB_fprintf(cfp,"int NPEs = PEend - PEstart + 1;\n");

	    if (nfrom > 0) {
	      TAB(1); ODB_fprintf(cfp,"int K0;\n");
	      ODB_fprintf(cfp,"#if defined(K0_lo_var)\n");
	      TAB(1); ODB_fprintf(cfp,"int K0_lo =  0;\n");
	      ODB_fprintf(cfp,"#elif defined(K0_lo_const)\n");
	      TAB(1); ODB_fprintf(cfp,"const int K0_lo =  0;\n");
	      ODB_fprintf(cfp,"#endif\n");
	      TAB(1); ODB_fprintf(cfp,"int K0_hi = N0;\n");
	    }

	    TAB(1); ODB_fprintf(cfp,"int Count = 0;\n");

	    FPINF_KEY(prefetch);
	    PREFETCH_CODE(1,PRE,fpinf); /* Prefetch possible OFFSETs & LENGTHs */
	    FPINF_KEYEND(prefetch);
	    
	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_START(PrS_V_%s);\n", name);
	    }

	    TAB(1); ODB_fprintf(cfp,"ODBMAC_PEINFO_SETUP();\n");

	    if (pcond_USD_symbols > 0) {
	      (void) assign_USD_symbols(cfp, pcond, 1, 2, NULL, name);
	    }

#ifdef REMOVE_COMPLAINTS
	    TAB(1); ODB_fprintf(cfp,"NPEs=NPEs;   /* Removes some compiler complaints */\n");
#endif
	    TAB(1); ODB_fprintf(cfp,"for (PE=PEstart; PE<=PEend; PE++) {\n");
	    TAB(2); ODB_fprintf(cfp,"int tmpcount = 0;\n");

	    TAB(2); ODB_fprintf(cfp,"if (Addr) {\n");
	    TAB(3); ODB_fprintf(cfp,"boolean Addr_trigger = 0;\n");
	    TAB(3); ODB_fprintf(cfp,"*Addr = PE;\n");
	    (void) assign_USD_symbols(NULL, pcond, 0, 0, NULL, name); /* Reset flags */
	    if (pcond_USD_symbols > 0) {
	      (void) assign_USD_symbols(cfp, pcond, 3, 2, "PEinfo->varname", name);
	    }
	    TAB(3); ODB_fprintf(cfp,"if (!Addr_trigger) Addr = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"}\n");

	    TAB(2); ODB_fprintf(cfp,"ODBMAC_PEINFO_SKIP();\n");

	    ZEROIT(done, 0, nall);

	    CALLOC(subloop, nfrom);
	    CALLOC(close_bracket, nfrom);
	    CALLOC(inlined_len, nfrom);
	    CALLOC(orphan, nfrom);
	    
	    oneif = 0;
	    has_link = 0;
	    linkless = 0;

	    FPINF_KEYNUM(links,nfrom);
	    for (i=0; i<nfrom; i++) {
	      numands = 0;
	      
	      if (i < nfrom - 1 || (i == nfrom - 1 && maxfrom == i)) {
		TAB(1+i+1+oneif); 
		if (subloop[i].len == 1) { /* The ONE-looper via '-1' */
		  int im1 = subloop[i].i;
		  ODB_fprintf(cfp,
			      "K%d = K%d_lo; { "
			      "/* TABLE '%s' (due to ONELOOPER with '%s') : weight = %.6f */\n",
			      i,i,
			      pview->from[i]->table->name,
			      pview->from[im1]->table->name,
			      pview->from[i]->wt);
		  FPINF_LINK(i,pview->from[i]->table->name,im1,
			     pview->from[im1]->table->name,"1",
			     im1, pview->from[im1]->table->name);
		}
		else if (subloop[i].len == 2) { /* ALIGN-table via '-A' */
		  int im1 = subloop[i].i;
		  if (orphan[i]) {
		    ODB_fprintf(cfp,
				"K%d = K%d; { "
				"/* Orphan TABLE '%s' (due to ALIGN with '%s') : weight = %.6f */\n",
				i,im1,
				pview->from[i]->table->name,
				pview->from[im1]->table->name,
				pview->from[i]->wt);
		    FPINF_LINK(i,pview->from[i]->table->name,im1,
			       pview->from[im1]->table->name,"=",
			       -1, "0");
		  }
		  else {
		    int offset_parent_id = subloop[i].level_plus_one - 1;
		    const char *offset_parent_name = (offset_parent_id >= 0) ?
		      pview->from[offset_parent_id]->table->name : "0";
		    ODB_fprintf(cfp,
				"K%d = K%d_lo + (K%d - K%d_lo); { "
				"/* TABLE '%s' (due to ALIGN with '%s') : weight = %.6f */\n",
				i,i,im1,im1,
				pview->from[i]->table->name,
				pview->from[im1]->table->name,
				pview->from[i]->wt);
		    FPINF_LINK(i,pview->from[i]->table->name,im1,
			       pview->from[im1]->table->name,"A",
			       offset_parent_id, offset_parent_name);
		  }
		}
		else {
		  int offset_parent_id = subloop[i].level_plus_one - 1;
		  const char *offset_parent_name = (offset_parent_id >= 0) ?
		    pview->from[offset_parent_id]->table->name : "0";
		  int im1 = (i == 0) ? -1 : subloop[i].i;
		  ODB_fprintf(cfp,
			      "for (K%d=K%d_lo; K%d<K%d_hi; K%d++) { "
			      "/* TABLE '%s' : weight = %.6f */\n",
			      i,i,i,i,i,pview->from[i]->table->name,pview->from[i]->wt);
		  if (im1 >= 0) {
		    const char *ckind = NULL;
		    if (subloop[i].len == 1) ckind = "1";
		    else if (subloop[i].len == 2) ckind = orphan[i] ? "=" : "A";
		    else ckind = "N";
		    FPINF_LINK(i,pview->from[i]->table->name,im1,
			       pview->from[im1]->table->name,ckind,
			       offset_parent_id, offset_parent_name);
		  }
		  else {
		    FPINF_LINK(i,pview->from[i]->table->name,im1,
			       "0","N",
			       offset_parent_id, offset_parent_name);
		  }
		}
	      }
	      else if (i == nfrom - 1 && nfrom > 1 && !has_link) {
		TAB(1+i+1+oneif); 
		ODB_fprintf(cfp,
			    "for (K%d=K%d_lo; K%d<K%d_hi; K%d++) { "
			    "/* Linkless TABLE '%s' : weight = %.6f */\n",
			    i,i,i,i,i,pview->from[i]->table->name,pview->from[i]->wt);
		FPINF_LINK(i,pview->from[i]->table->name,-1,
			   "0","N",
			   -1, "0");
		linkless = 1;
	      }
	      else {
		int offset_parent_id = subloop[i].level_plus_one - 1;
		const char *offset_parent_name = (offset_parent_id >= 0) ?
		  pview->from[offset_parent_id]->table->name : "0";
		int im1 = (i == 0) ? -1 : subloop[i].i;
		if (im1 >= 0) {
		  const char *ckind = NULL;
		  if (subloop[i].len == 1) ckind = "1";
		  else if (subloop[i].len == 2) ckind = orphan[i] ? "=" : "A";
		  else ckind = "N";
		  FPINF_LINK(i,pview->from[i]->table->name,im1,
			     pview->from[im1]->table->name,ckind,
			     offset_parent_id, offset_parent_name);
		}
		else {
		  FPINF_LINK(i,pview->from[i]->table->name,im1,
			     "0","N",
			     offset_parent_id, offset_parent_name);
		}
	      }
	      subloop[i].protected = 1;
	      
	      if (i < nfrom - 1) {
		Boolean link_found = 0;
		char *snext = pview->from[i+1]->table->name;
		char *sthis = NULL;
		int level = -1;
		
		for (j=i; j>=0; j--) {
		  ODB_Table *t = pview->from[j];
		  if (t->link) {
		    int l;
		    for (l=0; l<t->nlink; l++) {
		      char *s = t->link[l]->table->name;
		      link_found = strequ(s, snext);
		      if (link_found) {
			sthis = pview->from[j]->table->name;
			level = j;
			goto PRE_break;
		      }
		    } /* for (l=0; l<t->nlink; l++) */
		  } /* if (t->link) */
		} /* for (j=i; j>=0; j--) */
		
	      PRE_break:
		has_link = 0;
		
		if (!link_found) {
		  int ii = i+1;

		  /* Search for ALIGNs only */
		  sthis = pview->from[i]->table->name;
		  for (j=i+1; j<nfrom; j++) { /* Forward */
		    if (!subloop[j].protected) {
		      char *ssnext = pview->from[j]->table->name;
		      int align = onegrep(sthis, ssnext,__LINE__);
		      if (align == 2) {
			subloop[j].len = align;
			subloop[j].i = i;
			has_link = 1;
			snext = ssnext;
			ii = j;
			break;
		      }
		    } /* if (!subloop[j].protected) */
		  } /* for (j=i+1; j<nfrom; j++) */

		  if (!has_link) {
		    /* Do "earlier" tables contain any ALIGN-ments ? */
		    for (j=i-1; j>=0; j--) { /* Backward */
		      if (!subloop[j].protected) {
			char *ssnext = pview->from[j]->table->name;
			int align = onegrep(sthis, ssnext,__LINE__);
			if (align == 2) {
			  subloop[j].len = align;
			  subloop[j].i = i;
			  has_link = 1;
			  snext = ssnext;
			  ii = j;
			  break;
			}
		      } /* if (!subloop[j].protected) */
		    } /* for (j=i-1; j>=0; j--) */
		  }

		  ii = i + 1; /* Reset back to i + 1 */
		  orphan[ii] = 1;
		  TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d_lo = 0;\n",ii);
		  /* TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d_len = 1;\n",ii); */
		  inlined_len[ii] = STRDUP("1");
		  TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d_hi = N%d;\n",ii,ii);
		  TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d;\n",ii);
		}
		else {
		  int ii = i+1;
		  int test = onegrep(sthis, snext,__LINE__);
		  has_link = 1; /* some sort of LINK found anyway */
		  if (test == 0) {
		    /* Search for ALIGNs only */
		    sthis = pview->from[i]->table->name;
		    for (j=i+1; j<nfrom; j++) { /* Forward */
		      if (!subloop[j].protected) {
		        char *ssnext = pview->from[j]->table->name;
			int align = onegrep(sthis, ssnext,__LINE__);
			if (align == 2) {
			  subloop[j].len = align;
			  subloop[j].i = i;
			  /* snext = ssnext; */
			  ii = j;
			  break;
			}
		      } /* if (!subloop[j].protected) */
		    } /* for (j=i+1; j<nfrom; j++) */
		  }
		  else if (!subloop[ii].protected) {
		    subloop[ii].len = test;
		    subloop[ii].i = level;
		  } /* if (test == 0) else ... */
		  ii = i + 1; /* Reset back to i + 1 */
		  
		  /*
		    TAB(1+i+2+oneif); 
		    ODB_fprintf(cfp,"int K%d_len = P%s_len[K%d];\n",ii,snext,level);
		  */
		  
		  ALLOC(inlined_len[ii],strlen(snext) + 30);
		  sprintf(inlined_len[ii],"P%s_len[K%d]",snext,level);
		  
		  if (pview->safeGuard) {
		    TAB(1+i+2+oneif);
		    ODB_fprintf(cfp,"if (%s > 0) { /* safeGuard#%d */\n",inlined_len[ii],++numsg);
		    close_bracket[i] = 1+i+2+oneif;
		  }
		  
		  TAB(1+i+2+oneif);
		  ODB_fprintf(cfp,"int K%d_lo = P%s_off[K%d];\n",ii,snext,level);
		  subloop[ii].level_plus_one = level + 1;
		  
		  if (subloop[ii].len == 0) {
		    if (!subloop[ii].protected) {
		      subloop[ii].i = level;
		      subloop[ii].protected = 1;
		    }
		    TAB(1+i+2+oneif); 
		    ODB_fprintf(cfp,"int K%d_hi = K%d_lo + %s;\n",
				ii,ii,inlined_len[ii]);
		  }
		  TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d;\n",ii);
		}
	      } /* if (i < nfrom - 1) */
	      
	      CONDCODE(cfp,numands);
	    } /*  for (i=0; i<nfrom; i++) */

	    if (nfrom > 0) {
	      i = nfrom-1;
	      if (numands == 0 && subloop[i].len == 0) {
		TAB(1+nfrom+oneif); 
		ODB_fprintf(cfp,"tmpcount += %s; /* TABLE '%s' : weight = %.6f */\n",
			    inlined_len[i],
			    pview->from[i]->table->name,pview->from[i]->wt);
	      }
	      else {
		TAB(1+nfrom+2+oneif); 
		ODB_fprintf(cfp,"tmpcount++; /* TABLE '%s' : weight = %.6f */\n",
			    pview->from[i]->table->name,pview->from[i]->wt);
	      }
	      if (linkless && nfrom > 1 && !has_link) { /* i is now nfrom-1 */
		TAB(1+i+1+oneif); 
		ODB_fprintf(cfp,"} /* Linkless TABLE '%s' */\n",pview->from[i]->table->name);
		linkless = 0;
	      }
	    }
	    else {
	      i = 0;
	      TAB(1+i+1); ODB_fprintf(cfp,"{ /* FROM-less WHERE statement (if any) */\n");
	      CONDCODE(cfp,numands);
	      TAB(1+i+3); ODB_fprintf(cfp,"tmpcount++;\n");
	      TAB(1+i+2); ODB_fprintf(cfp,"} /* if-block end */\n"); \
	      TAB(1+i+1); ODB_fprintf(cfp,"}\n");
	    }
	    
	    for (i=nfrom-1; i>=0; i--) {
	      if (pcond && (i == maxfrom)) {
		oneif--;
		TAB(1+i+2);
		ODB_fprintf(cfp,"} /* if-block end */\n");
	      }
	      
	      if (pview->safeGuard && close_bracket[i]) {
		int ii = i + 1; /* Reset back to i + 1 */
		int tabs = close_bracket[i];
		TAB(tabs); 
		ODB_fprintf(cfp,"} /* end safeGuard#%d : if (%s > 0) ... */\n",numsg--,inlined_len[ii]);
		close_bracket[i] = 0;
	      }
	      
	      if (i < nfrom - 1 || (numands > 0 || numands == -1)) {
		TAB(1+i+1+oneif);
		ODB_fprintf(cfp,"} /* TABLE '%s' */\n", pview->from[i]->table->name);
	      }
	    } /* for (i=nfrom-1; i>=0; i--) */
	  
	    TAB(2); ODB_fprintf(cfp,"ODBMAC_PEINFO_UPDATE_COUNTS();\n");
	    TAB(2); ODB_fprintf(cfp,"Count += tmpcount;\n");
	    TAB(1); ODB_fprintf(cfp,"} /* for (PE=PEstart; PE<=PEend; PE++) */\n");

	    TAB(1); ODB_fprintf(cfp,"ODBMAC_PEINFO_COPY();\n");

	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_END(Count);\n");
	    }

	    TAB(1); ODB_fprintf(cfp,"return Count;\n}\n");
	    NL(1);

	    /* SELECT (post) : Fill indices */

	    free_tmpsym(0);
	    
	    ODB_fprintf(cfp,"PRIVATE int\nPoS_V_%s(FILE *do_trace,\n", name);
	    TAB(1); ODB_fprintf(cfp,"const VIEW_%s *P, int it, ODB_PE_Info *PEinfo",name);

	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      ODB_fprintf(cfp,"\n");
	      TAB(1); 
	      ODB_fprintf(cfp,"/* TABLE '%s' */ , int N%d, const unsigned int BmapIdx%d[]",
			  s,i,i);
	      if ((pview->active[i] & 0x1) == 0x1) {
		ODB_fprintf(cfp,", int Index_%s[]", s);
	      }
	    }

	    init_list(NULL);
	    ZEROIT(done, 0, nall);

	    for (i=nselect_all; i<nall; i++) {
	      char *call_arg = pview->call_arg[i];
	      char *pvar;
	      char *ptype;
	      char *p;
	      int plen;

	      if (!call_arg) continue;

	      (void) ODB_split(call_arg, &ptype, &pvar, NULL, NULL, NULL);
	      
	      plen = strlen(ptype) + strlen(pvar) + 10;
	      ALLOC(p, plen);
	      sprintf(p, "%s %s[]", ptype, pvar);

	      if (!in_list(p)) {
		add_list(p);
		ODB_fprintf(cfp,",\n");
		TAB(1); ODB_fprintf(cfp,"const %s",p);
		done[i] = 1;
	      }

	      FREE(p);
	      FREE(pvar);
	      FREE(ptype);
	    }

	    destroy_list();

	    ODB_fprintf(cfp,")\n{\n",name);

	    (void) assign_USD_symbols(NULL, pcond, 0, 0, NULL, name); /* Reset flags */
	    if (pcond_USD_symbols > 0) {
	      (void) assign_USD_symbols(cfp, pcond, 1, 1, NULL, name);
	    }
	    TAB(1); 
	    ODB_fprintf(cfp,
		    "double *Addr = (PEinfo && P->USD_symbols > 0) ? PEinfo->addr : NULL;\n");
	    TAB(1); ODB_fprintf(cfp,"int PE, PEstart = 1;\n");
	    TAB(1); ODB_fprintf(cfp,"int PEend = PEinfo ? PEinfo->npes : PEstart;\n");
	    TAB(1); ODB_fprintf(cfp,"int NPEs = PEend - PEstart + 1;\n");

	    if (nfrom > 0) {
	      TAB(1); ODB_fprintf(cfp,"int K0;\n");
	      ODB_fprintf(cfp,"#if defined(K0_lo_var)\n");
	      TAB(1); ODB_fprintf(cfp,"int K0_lo = 0;\n");
	      ODB_fprintf(cfp,"#elif defined(K0_lo_const)\n");
	      TAB(1); ODB_fprintf(cfp,"const int K0_lo = 0;\n");
	      ODB_fprintf(cfp,"#endif\n");
	      TAB(1); ODB_fprintf(cfp,"int K0_hi = N0;\n");
	    }

	    TAB(1); ODB_fprintf(cfp,"int Count = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"int tmpcount = 0;\n");
	    PREFETCH_CODE(1,POST,NULL); /* Prefetch possible OFFSETs & LENGTHs */

	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_START(PoS_V_%s);\n", name);
	    }

	    if (pcond_USD_symbols > 0) {
	      (void) assign_USD_symbols(cfp, pcond, 1, 2, NULL, name);
	    }

#ifdef REMOVE_COMPLAINTS
	    TAB(1); ODB_fprintf(cfp,"Count=Count; /* Removes some compiler complaints */\n");
	    TAB(1); ODB_fprintf(cfp,"NPEs=NPEs;   /* Removes some compiler complaints */\n");
#endif
	    TAB(1); ODB_fprintf(cfp,"for (PE=PEstart; PE<=PEend; PE++) {\n");
	    TAB(2); ODB_fprintf(cfp,"if (Addr) {\n");
	    TAB(3); ODB_fprintf(cfp,"boolean Addr_trigger = 0;\n");
	    TAB(3); ODB_fprintf(cfp,"*Addr = PE;\n");
	    (void) assign_USD_symbols(NULL, pcond, 0, 0, NULL, name); /* Reset flags */
	    if (pcond_USD_symbols > 0) {
	      (void) assign_USD_symbols(cfp, pcond, 3, 2, "PEinfo->varname", name);
	    }
	    TAB(3); ODB_fprintf(cfp,"if (!Addr_trigger) Addr = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"}\n");
	    TAB(2); ODB_fprintf(cfp,"ODBMAC_PEINFO_BREAKLOOP();\n");

	    ZEROIT(done, 0, nall);

	    oneif = 0;

	    for (i=0; i<nfrom; i++) {
	      numands = 0;

	      /* if (i < nfrom - 1 || (i == nfrom - 1 && maxfrom == i)) */ {
		TAB(1+i+1+oneif); 
		if (i > 0 && subloop[i].len == 1) { /* The ONE-looper via '-1' */
		  int im1 = subloop[i].i;
		  ODB_fprintf(cfp,
			      "K%d = K%d_lo; { "
			      "/* TABLE '%s' (due to ONELOOPER with '%s') : weight = %.6f */\n",
			      i,i,
			      pview->from[i]->table->name,
			      pview->from[im1]->table->name,
			      pview->from[i]->wt);
		}
		else if (i > 0 && subloop[i].len == 2) { /* ALIGN-table via '-A' */
		  int im1 = subloop[i].i;
		  if (orphan[i]) {
		    ODB_fprintf(cfp,
				"K%d = K%d; { "
				"/* Orphan TABLE '%s' (due to ALIGN with '%s') : weight = %.6f */\n",
				i,im1,
				pview->from[i]->table->name,
				pview->from[im1]->table->name,
				pview->from[i]->wt);
		  }
		  else {
		    ODB_fprintf(cfp,
				"K%d = K%d_lo + (K%d - K%d_lo); { "
				"/* TABLE '%s' (due to ALIGN with '%s') : weight = %.6f */\n",
				i,i,im1,im1,
				pview->from[i]->table->name,
				pview->from[im1]->table->name,
				pview->from[i]->wt);
		  }
		}
		else {
		  ODB_fprintf(cfp,
			      "for (K%d=K%d_lo; K%d<K%d_hi; K%d++) { "
			      "/* TABLE '%s' : weight = %.6f */\n",
			      i,i,i,i,i,pview->from[i]->table->name,pview->from[i]->wt);
		}
	      }

	      if (i < nfrom - 1) {
		Boolean link_found = 0;
		char *snext = pview->from[i+1]->table->name;
		/* char *sthis = NULL; */
		int level = -1;
		
		for (j=i; j>=0; j--) {
		  ODB_Table *t = pview->from[j];
		  if (t->link) {
		    int l;
		    for (l=0; l<t->nlink; l++) {
		      char *s = t->link[l]->table->name;
		      link_found = strequ(s, snext);
		      if (link_found) {
			/* sthis = pview->from[j]->table->name; */
			level = j;
			goto POST_break;
		      }
		    } /* for (l=0; l<t->nlink; l++) */
		  } /* if (t->link) */
		} /* for (j=i; j>=0; j--) */

	      POST_break:
		
		if (!link_found) {
		  int ii = i + 1; /* Reset back to i + 1 */
		  orphan[ii] = 1;
		  TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d_lo = 0;\n",ii);
		  /* TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d_len = 1;\n",ii); */
		  inlined_len[ii] = STRDUP("1");
		  TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d_hi = N%d;\n",ii,ii);
		  TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d = K%d_lo;\n",ii,ii);
		}
		else {
		  int ii = i + 1; /* Reset back to i + 1 */
		  /*
		  TAB(1+i+2+oneif); 
		  ODB_fprintf(cfp,"int K%d_len = P%s_len[K%d];\n",ii,snext,level);
		  */

		  ALLOC(inlined_len[ii],strlen(snext) + 30);
		  sprintf(inlined_len[ii],"P%s_len[K%d]",snext,level);

		  if (pview->safeGuard) {
		    TAB(1+i+2+oneif);
		    ODB_fprintf(cfp,"if (%s > 0) { /* safeGuard#%d */\n",inlined_len[ii],++numsg);
		    close_bracket[i] = 1+i+2+oneif;
		  }

		  TAB(1+i+2+oneif);
		  ODB_fprintf(cfp,"int K%d_lo = P%s_off[K%d];\n",ii,snext,level);

		  if (subloop[ii].len == 0) {
		    if (!subloop[ii].protected) {
		      subloop[ii].i = level;
		      subloop[ii].protected = 1;
		    }
		    TAB(1+i+2+oneif); 
		    ODB_fprintf(cfp,"int K%d_hi = K%d_lo + %s;\n",
				ii,ii,inlined_len[ii]);
		  }
		  TAB(1+i+2+oneif); ODB_fprintf(cfp,"int K%d = K%d_lo;\n",ii,ii);
		}
	      } /* if (i < nfrom - 1) */
	      
	      CONDCODE(cfp,numands);
	    } /* for (i=0; i<nfrom; i++) */
	    FPINF_KEYEND(links);
	    
	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      if ((pview->active[i] & 0x1) == 0x1) {
		TAB(1+nfrom+2); 
		ODB_fprintf(cfp,"Index_%s[tmpcount] = K%d;\n", s, i);
	      }
	    } /* for (i=0; i<nfrom; i++) */

	    if (nfrom > 0) {
	      TAB(1+nfrom+2);
	      ODB_fprintf(cfp,"tmpcount++;\n");
	    }
	    else {
	      i = 0;
	      TAB(1+i+1); ODB_fprintf(cfp,"{ /* FROM-less WHERE statement (if any) */\n");
	      CONDCODE(cfp,numands);
	      TAB(1+i+3); ODB_fprintf(cfp,"tmpcount++;\n");
	      TAB(1+i+2); ODB_fprintf(cfp,"} /* if-block end */\n"); \
	      TAB(1+i+1); ODB_fprintf(cfp,"}\n");
	    }
	    
	    for (i=nfrom-1; i>=0; i--) {
	      if (pcond && (i == maxfrom)) {
		oneif--;
		TAB(1+i+2);
		ODB_fprintf(cfp,"} /* if-block end */\n");
	      }

	      if (pview->safeGuard && close_bracket[i]) {
		int ii = i + 1; /* Reset back to i + 1 */
		int tabs = 1+i+2;
		TAB(tabs); ODB_fprintf(cfp,"} /* end safeGuard#%d : if (%s > 0) ... */\n",numsg--,inlined_len[ii]);
		close_bracket[i] = 0;
	      }

	      /* if (i < nfrom - 1 || (numands > 0 || numands == -1)) */ {
		TAB(1+i+1+oneif); 
		ODB_fprintf(cfp,"} /* TABLE '%s' */\n", pview->from[i]->table->name);
	      }
	    } /* for (i=nfrom-1; i>=0; i--) */

	    TAB(1); ODB_fprintf(cfp,"} /* for (PE=PEstart; PE<=PEend; PE++) */\n");

	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_END(tmpcount);\n");
	    }

	    TAB(1); ODB_fprintf(cfp,"return tmpcount;\n}\n");
	    NL(1);

	    free_tmpsym(1);
	    
	    FREE(subloop);
	    FREE(close_bracket);
	    for (i=0; i<nfrom; i++) {
	      if (inlined_len[i]) FREE(inlined_len[i]);
	    }
	    FREE(inlined_len);
	    FREE(orphan);

	    /* SELECT : The parent SELECT-function for a view */

	    ODB_fprintf(cfp,
		    "PRIVATE int\nSel_V_%s(void *V, ODB_PE_Info *PEinfo, int phase, void *feedback)\n{\n",
		    name);
	    TAB(1); ODB_fprintf(cfp,"VIEW_%s *P = V;\n",name);
	    TAB(1); ODB_fprintf(cfp,"int CountPrS = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"int CountPoS = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"int Nbytes = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"ODBMAC_TRACE_SELVIEW_SETUP(%s, \"",name);
	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      ODB_fprintf(cfp,"%s%s",s,(i==nfrom-1) ? "" : ",");
	    }
	    ODB_fprintf(cfp,"\");\n");
	    TAB(1); ODB_fprintf(cfp,"int it = get_thread_id_();\n");

	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_START(Sel_V_%s);\n", name);
	    }

	    TAB(1); ODB_fprintf(cfp,"ODBMAC_PEINFO_SELVIEW_SETUP();\n");

	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      if ((pview->active[i] & 0x1) == 0x1) { TAB(1); ODB_fprintf(cfp,"FREEINDEX(%s);\n", s); }
	    }

	    TAB(1); ODB_fprintf(cfp,"ODBMAC_TRACE_SELVIEW_PRE();\n");

	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      TAB(1); ODB_fprintf(cfp,"ODBMAC_VIEW_DELAYED_LOAD(%s);\n",s);
	    }

	    TAB(1); ODB_fprintf(cfp,"ODBMAC_TRACE_SELVIEW_0();\n");

	    /*
	    TAB(1); ODB_fprintf(cfp,"P->MaxRows = ");
	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      if (i>0 && nfrom > 2) { NL(1); TAB(2); }
	      if (i>0)              { ODB_fprintf(cfp,"* "); }
	      ODB_fprintf(cfp,"P->T_%s->Nrows%s",s,(i<nfrom-1) ? "" : ";\n");
	    }
	    NL(1);
	    */

	    if (nuniqueby > 0) {
	      TAB(1); ODB_fprintf(cfp,"codb_hash_set_lock_();\n");
	      TAB(1); ODB_fprintf(cfp,"codb_hash_init_();\n");
	    }

	    TAB(1); ODB_fprintf(cfp,"ODBMAC_TRACE_SELVIEW_1();\n");

	    {
	      const int tabinc = 1;
	      int nvars = 0;

	      TAB(tabinc); ODB_fprintf(cfp,"{ /* Start of pre- & post-select block */\n");

	      init_list(NULL);
	      ZEROIT(done, 0, nall);
	      
	      for (i=nselect_all; i<nall; i++) {
		char *tag = pview->tag[i];
		char *ptype;
		char *pvar;
		char *ptable;
		char *p;
		int plen;
		
		if (!tag) continue;
		
		(void) ODB_split(tag, &ptype, &pvar, NULL, &ptable, NULL);
		
		plen = strlen(pvar) + strlen(ptable) + 10;
		ALLOC(p, plen);
		snprintf(p, plen, "P->T_%s->%s", ptable, pvar);
		
		if (!in_list(p)) {
		  add_list(p);
		  TAB(tabinc+1); 
		  ODB_fprintf(cfp,"const %s *TmpVec_%d = ",ptype,i);
		  ODB_fprintf(cfp,"UseDSlong(P->T_%s, %s, %s, %s);\n", ptable, odb_label, ptype, p);
		  done[i] = 1;
		  nvars++;
		}

		FREE(p);
		FREE(ptype);
		FREE(pvar);
		FREE(ptable);
	      }

	      destroy_list();
	      
	      for (i=0; i<nfrom; i++) {
		char *s = pview->from[i]->table->name;
		TAB(tabinc+1); ODB_fprintf(cfp,"ALLOCBITMAPINDEX(%s);\n",s);
	      }

	      if (pview->has_thin) {
		TAB(tabinc+1); ODB_fprintf(cfp,"codb_thin_reset_(&it);\n");
	      }

	      TAB(tabinc+1); ODB_fprintf(cfp,"CountPrS = PrS_V_%s(do_trace, P, it, PEinfo",name);
	      
	      for (i=0; i<nfrom; i++) {
		char *s = pview->from[i]->table->name;
		ODB_fprintf(cfp,"\n",s);
		TAB(tabinc+2); ODB_fprintf(cfp,", P->T_%s->Nrows, P->BitmapIndex_%s",s,s);
	      }

	      if (nvars > 0) {
		for (i=nselect_all; i<nall; i++) {
		  if (done[i]) {
		    ODB_fprintf(cfp,",\n");
		    TAB(tabinc+2); ODB_fprintf(cfp,"TmpVec_%d",i);
		  }
		}
	      }
	      
	      ODB_fprintf(cfp,");\n");
	      
	      TAB(tabinc+1); ODB_fprintf(cfp,"ODBMAC_TRACE_SELVIEW_POST();\n");
	      
	      if (!merge_table_indices) {
		for (i=0; i<nfrom; i++) {
		  char *s = pview->from[i]->table->name;
		  if ((pview->active[i] & 0x1) == 0x1) { 
		    TAB(tabinc+1); 
		    ODB_fprintf(cfp, "ALLOCINDEX(%s, CountPrS);\n", s); 
		  }
		} /* for (i=0; i<nfrom; i++) */
	      }
	      else {
		/* In index allocation take into account the merging of 
		   certain ALIGNed (or -A) table indices */
		
		TAB(tabinc+1); ODB_fprintf(cfp,"/* Index merge requested via '-O3 -m' options */\n");
		
		/* Masters first */
		for (i=0; i<nfrom; i++) {
		  char *master = pview->from[i]->table->name;
		  if ((pview->active[i] & 0x1) == 0x1 && !pview->merged_with[i]) { 
		    TAB(tabinc+1); 
		    ODB_fprintf(cfp, "ALLOC(P->Index_%s, CountPrS); P->Allocated_%s = 1;\n", 
				master, master); 
		  }
		} /* for (i=0; i<nfrom; i++) */
		
		/* Slaves last */
		for (i=0; i<nfrom; i++) {
		  char *master = pview->from[i]->table->name;
		  if ((pview->active[i] & 0x1) == 0x1 && pview->merged_with[i]) { 
		    char *slave = pview->merged_with[i];
		    TAB(tabinc+1); 
		    ODB_fprintf(cfp, "P->Index_%s = P->Index_%s; P->Allocated_%s = 0;\n", 
				master, slave, master); 
		  }
		} /* for (i=0; i<nfrom; i++) */
	      }

	      if (nuniqueby > 0) {
		TAB(tabinc+1); ODB_fprintf(cfp,"codb_hash_reset_();\n");
	      }

	      TAB(tabinc+1); ODB_fprintf(cfp,"ODBMAC_TRACE_SELVIEW_2();\n");
	      
	      TAB(tabinc+1); ODB_fprintf(cfp,"if (CountPrS > 0) {\n");
	      if (pview->has_thin) {
		TAB(tabinc+2); ODB_fprintf(cfp,"codb_thin_reset_(&it);\n");
	      }
	      TAB(tabinc+2); ODB_fprintf(cfp,"CountPoS = PoS_V_%s(do_trace, P, it, PEinfo",name);
	      
	      for (i=0; i<nfrom; i++) {
		char *s = pview->from[i]->table->name;
		ODB_fprintf(cfp,"\n");
		TAB(tabinc+3); ODB_fprintf(cfp,", P->T_%s->Nrows, P->BitmapIndex_%s",s,s);
		if ((pview->active[i] & 0x1) == 0x1) {
		  ODB_fprintf(cfp,", P->Index_%s",s);
		}
	      }
	      
	      if (nvars > 0) {
		for (i=nselect_all; i<nall; i++) {
		  if (done[i]) {
		    ODB_fprintf(cfp,",\n");
		    TAB(tabinc+3); ODB_fprintf(cfp,"TmpVec_%d",i);
		  }
		}
	      }
	      
	      ODB_fprintf(cfp,"); }\n");
	      TAB(tabinc+1); ODB_fprintf(cfp,"else { CountPoS = CountPrS; }\n");

	      for (i=0; i<nfrom; i++) {
		char *s = pview->from[i]->table->name;
		TAB(tabinc+1); ODB_fprintf(cfp,"FREEBITMAPINDEX(%s);\n",s);
	      }

	      TAB(tabinc); ODB_fprintf(cfp,"} /* End of pre-& post-select block */\n");
	    }

	    if (nuniqueby > 0) {
	      TAB(1); ODB_fprintf(cfp,"codb_hash_init_();\n");
	      TAB(1); ODB_fprintf(cfp,"codb_hash_unset_lock_();\n");
	    }
	    
	    TAB(1); ODB_fprintf(cfp,"ODBMAC_TRACE_SELVIEW_LAST();\n");
	    TAB(1); ODB_fprintf(cfp,"ODBMAC_ERRMSG_SELVIEW(%s);\n",name);
	    
	    {
	      int cnt, ntbl = 0;
	      for (i=0; i<nfrom; i++) {
		if ((pview->active[i] & 0x1) == 0x1) ntbl++;
	      }
	      TAB(1); 
	      ODB_fprintf(cfp,"ODB_debug_print_index(stdout, \"%s\", P->PoolNo, CountPrS, %d",
			  name, ntbl);
	      cnt = 0;
	      for (i=0; i<nfrom; i++) {
		char *s = pview->from[i]->table->name;
		if ((pview->active[i] & 0x1) == 0x1) {
		  ODB_fprintf(cfp,"\n");
		  TAB(4); 
		  ODB_fprintf(cfp,", \"%s\", P->Index_%s, P->T_%s, P->T_%s->Nrows",
			      s, s, s, s);
		  ++cnt;
		}
	      }
	      ODB_fprintf(cfp,");\n");
	    }
	    TAB(1); ODB_fprintf(cfp,"P->Nrows = CountPrS;\n");

	    if (pview->has_thin) {
	      TAB(1); ODB_fprintf(cfp,"codb_thin_reset_(&it);\n");
	    }

	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_END(CountPrS);\n");
	    }

	    TAB(1); ODB_fprintf(cfp,"return CountPrS;\n");
	    ODB_fprintf(cfp,"}\n");
	    NL(1);

	    for (j=0; j<ntypes_view; j++) {
	      char *pKey = Key[j];
	      char *pExtType = ExtType[j];

	      if (!typemask[j]) continue;

	      ODB_fprintf(cfp,"PRIVATE int\n%sGet_V_%s(void *V, %s D[],\n",
		      pKey, name, pExtType);
	      TAB(1); ODB_fprintf(cfp,"int LdimD, int Nrows, int Ncols,\n");
	      TAB(1); ODB_fprintf(cfp,"int ProcID, const int Flag[], int row_offset)\n{\n");

	      TAB(1); ODB_fprintf(cfp,"VIEW_%s *P = V;\n",name);
	      TAB(1); ODB_fprintf(cfp,"int Count = MIN(Nrows, P->Nrows);\n");
	      TAB(1); ODB_fprintf(cfp,"int K1 = 0, K2 = Count;\n");
	      TAB(1); ODB_fprintf(cfp,"int Npes = P->Npes;\n");
	      if (pview->has_formulas) {
		TAB(1); ODB_fprintf(cfp,"int ii, it = get_thread_id_();\n");
		TAB(1); ODB_fprintf(cfp,"Formula *tmp = NULL;\n");
	      }
	      TAB(1); ODB_fprintf(cfp,"FILE *do_trace = ODB_trace_fp();\n");

	      if (insert_drhook) {
	        TAB(1); ODB_fprintf(cfp,"DRHOOK_START(%sGet_V_%s);\n", pKey, name);
	      }

	      TAB(1); ODB_fprintf(cfp,"ODBMAC_PEINFO_OFFSET();\n");

	      if (pview->sel) {
		int kaux = pview->nselect;
		int k = 0;
		int ioffset = nselect_all+nwhere+norderby;
		for (i=0; i<nselect; i++) {
		  if (pview->is_formula[i]) {
		    int ksave = k;
		    int naux, ns = 0, ne = pview->sel[i]->ncols_aux;
		    for (naux=ns; naux <= ne; naux++) {
		      Boolean need_i = 0;
		      int ii = (naux == ns) ? i : kaux++;
		      TAB(1); ODB_fprintf(cfp,"if (FLAG_FETCH(Flag[%d])) { /* column#%d */\n",ii,ii+1);
		      TAB(2); ODB_fprintf(cfp,"/* Formula: '%s' */\n",pview->sel[i]->formula+1);
		      if (pview->sel[i]->ncols_aux > 0) {
			TAB(2); ODB_fprintf(cfp,"const int ArgNo = %d;\n",1+naux);
		      }
		      { /* Start of dollar & column variables */
			int jj;
			k = ksave;
			init_list(NULL);
			for (jj=0; jj<pview->sel[i]->nsym; jj++, k++) {
			  char *s = pview->sel[i]->sym[jj]->name;
			  char *call_arg = pview->call_arg[ioffset+k];
			  if (IS_HASH(s)) { need_i = 1; continue; }
			  if (in_list(s)) continue;
			  if (IS_DOLLAR(s)) {
			    if (IS_POOLNO(s)) {
			      TAB(2); 
			      ODB_fprintf(cfp,
					  "double lc_USD_%s = P->PoolNo; "
					  "/* A fixed value */\n", POOLNO);
			    }
			    else if (!IS_USDHASH(s)) {
			      TAB(2); 
			      ODB_fprintf(cfp,
					  "double ODBMAC_LC_GETVAL(%s, %s, %c%s%c); /* '%s' */\n",
					  odb_label,s+1,'"',name,'"',s);
			    }
			    else if (IS_USDHASH(s)) {
			      /* These are NOT all correct ; 
				 they need to be dynamic/loop variable depended etc. */
			      char *eq = NULL;
			      if (IS_(ROWNUM,s)) eq = "0";
			      if (IS_(UNIQNUM,s)) eq = "ODB_put_one_control_word(0, P->PoolNo)";
			      else if (IS_(NROWS,s)) eq = "P->Nrows";
			      else if (IS_(NCOLS,s)) eq = "P->Ncols";
			      else if (IS_(COLNUM,s)) eq = "0"; /* Don't know the column ? */
			      else if (IS_USDDOTHASH(s)) eq = "0"; 
			      if (eq) {
				char *sh = STRDUP(s);
				int sh_len = STRLEN(sh);
				char *dot = strchr(sh,'.');
				if (dot) *dot = 'D'; /* IS_USDDOTHASH(s) */
				sh[sh_len-1] = '\0'; /* replace '#' with '\0' */
				TAB(2);
				ODB_fprintf(cfp,
					    "double lc_USDHASH_%s = %s; /* Not correct */\n",
					    sh+1,eq);
				FREE(sh);
			      }
			    }
			    add_list(s);
			  }
			  else if (STRLEN(call_arg) > 0) {
			    char *pcolon = strchr(call_arg,':');
			    char *atag = pview->tag[ioffset+k];
			    if (pcolon && in_list(pcolon)) continue;
			    if (pcolon && atag) {
			      char *p = NULL;
			      int plen;
			      char *ptype = NULL;
			      char *pvar = NULL;
			      char *ptable = NULL;
			      (void) ODB_split(atag, &ptype, &pvar, NULL, &ptable, NULL);
			      TAB(2); ODB_fprintf(cfp,"const %s *%s = ",ptype,pcolon+1);
			      plen = strlen(pvar) + strlen(ptable) + 10;
			      ALLOC(p, plen);
			      snprintf(p, plen, "P->T_%s->%s", ptable, pvar);
			      ODB_fprintf(cfp,"UseDSlong(P->T_%s, %s, %s, %s);\n", 
					  ptable, odb_label, ptype, p);
			      FREE(p);
			      FREE(ptype);
			      FREE(pvar);
			      FREE(ptable);
			      need_i = 1;
			      add_list(pcolon);
			    }
			  }
			}
			destroy_list();
		      } /* End of dollar & column variables */
		      TAB(2); ODB_fprintf(cfp,"tmp = &D[%d*LdimD];\n",ii);
		      TAB(2); ODB_fprintf(cfp,"for (ii=K1; ii<K2; ii++) {\n");
		      if (need_i) {
			TAB(3); ODB_fprintf(cfp,"int i = ii + row_offset;\n");
		      }
		      TAB(3); ODB_fprintf(cfp,"tmp[ii] =");
		      {
			char *f1 =  dump_s(NULL,pview->sel[i]->expr,1,NULL);
			int f1len = STRLEN(f1);
			if (f1len < linelen_threshold) {
			  ODB_fprintf(cfp," %s;\n",f1);
			}
			else {
			  char *p1 = f1;
			  ODB_fprintf(cfp,"\n");
			  TAB(4);
			  while ( *p1 ) {
			    ODB_fprintf(cfp,"%c",*p1);
			    if (*p1 == '\n') TAB(4);
			    p1++;
			  }
			  ODB_fprintf(cfp,";\n");
			}
			FREE(f1);
		      }
		      TAB(2); ODB_fprintf(cfp,"} /* for (ii=K1; ii<K2; ii++) */\n");
		      TAB(1); ODB_fprintf(cfp,"} /* if (FLAG_FETCH(Flag[%d])) -- column#%d */\n",ii,ii+1);
		    } /* for (naux=ns; naux <= ne; naux++) */
		  }
		  else {
		    int ifrom = pview->table_index[i];
		    ODB_Table *from = pview->from[ifrom];
		    char *tname = from->table->name;
		    char *tag = pview->tag[i];
		    /* char *define = pview->def_get[i]; */
		    char *alias_get = pview->alias_get[i];
		    char *ptype = NULL;
		    char *pvar = NULL;
		    char *datatype = NULL;
		    int pos = 0, len = 0;
		    char *p = NULL;
		    
		    (void) ODB_split(tag, &ptype, &pvar, NULL, NULL, NULL);
		    (void) get_PKmethod(ptype,NULL,&datatype,NULL);
		    
		    p = strstr(alias_get, "_BITS(");
		    if (p && strequ(ptype,BITFIELD)) {
		      /* Read pos & len from ", %d, %d)" */
		      char *psave = STRDUP(alias_get);
		      char *comma = strrchr(psave, ','); /* The last comma */
		      if (comma) {
			len = atoi(comma+1);
			*comma = '\0';
			comma = strrchr(psave, ','); /* 2nd last comma */
			if (comma) pos = atoi(comma+1);
		      }
		      FREE(psave);
		    }
		
		    TAB(1); 
		    ODB_fprintf(cfp,
				"Call_GatherGet_VIEW(%s, %s, %d, %s, %s, K1, K2, %s, D, %s, DATATYPE_%s, %d, %d);\n",
				odb_label, pKey, i+1, name, ptype, tname, pvar,
				datatype, pos, len);
		    
		    FREE(pvar);
		    FREE(ptype);
		    FREE(datatype);
		  } /* if (pview->is_formula[i]) ...  else ... */
		} /* for (i=0; i<nselect; i++) */
	      }

	      if (insert_drhook) {
		TAB(1); ODB_fprintf(cfp,"DRHOOK_END(K2-K1);\n");
	      }

	      TAB(1); ODB_fprintf(cfp,"return K2-K1;\n");
	      ODB_fprintf(cfp,"}\n");
	      NL(1);	    

	      if (!pview->all_readonly) {
		ODB_fprintf(cfp,"PRIVATE int\n%sPut_V_%s(void *V, const %s D[],\n",
			pKey, name, pExtType);
		TAB(1); ODB_fprintf(cfp,"int LdimD, int Nrows, int Ncols,\n");
		TAB(1); ODB_fprintf(cfp,"int ProcID, const int Flag[])\n{\n");
		
		TAB(1); ODB_fprintf(cfp,"VIEW_%s *P = V;\n",name);
		TAB(1); ODB_fprintf(cfp,"int Count = MIN(Nrows, P->Nrows);\n");
		TAB(1); ODB_fprintf(cfp,"int K1 = 0, K2 = Count;\n");
		TAB(1); ODB_fprintf(cfp,"int Npes = P->Npes;\n");
		TAB(1); ODB_fprintf(cfp,"FILE *do_trace = ODB_trace_fp();\n");

		if (insert_drhook) {
	          TAB(1); ODB_fprintf(cfp,"DRHOOK_START(%sPut_V_%s);\n", pKey, name);
		}

		TAB(1); ODB_fprintf(cfp,"ODBMAC_PEINFO_OFFSET();\n");
		
		for (i=0; i<nselect; i++) {
		  if (!pview->readonly[i]) {
		    int ifrom = pview->table_index[i];
		    ODB_Table *from = pview->from[ifrom];
		    char *tname = from->table->name;
		    char *tag = pview->tag[i];
		    /* char *define = pview->def_put[i]; */
		    char *alias_put = pview->alias_put[i];
		    char *ptype = NULL;
		    char *pvar = NULL;
		    char *datatype = NULL;
		    int pos = 0, len = 0;
		    char *p = NULL;

		    (void) ODB_split(tag, &ptype, &pvar, NULL, NULL,NULL);
		    (void) get_PKmethod(ptype,NULL,&datatype,NULL);

		    p = strstr(alias_put, "_BITS(");
		    if (p && strequ(ptype,BITFIELD)) {
		      /* Read pos & len from ", %d, %d)" */
		      char *psave = STRDUP(alias_put);
		      char *comma = strrchr(psave, ','); /* The last comma */
		      if (comma) {
			len = atoi(comma+1);
			*comma = '\0';
			comma = strrchr(psave, ','); /* 2nd last comma */
			if (comma) pos = atoi(comma+1);
		      }
		      FREE(psave);
		    }

		    TAB(1); 
		    ODB_fprintf(cfp,
		      "Call_ScatterPut_VIEW(%s, %s, %d, %s, %s, K1, K2, %s, %s, D, DATATYPE_%s, %d, %d);\n",
				odb_label, pKey, i+1, name, ptype, tname, pvar,
				datatype, pos, len);
		    
		    FREE(pvar);
		    FREE(ptype);
		  }
		} /* for (i=0; i<nselect; i++) */
		
		if (insert_drhook) {
		  TAB(1); ODB_fprintf(cfp,"DRHOOK_END(K2-K1);\n");
		}

		TAB(1); ODB_fprintf(cfp,"return K2-K1;\n}\n");
		NL(1);
	      } /* if (!pview->all_readonly) */
	    }/* for (j=0; j<ntypes_view; j++) */

	    /* Dimension function for VIEW */

	    ODB_fprintf(cfp,"PRIVATE void\nDim_V_%s(",name);
	    ODB_fprintf(cfp,"void *V, int *Nrows, int *Ncols, int *Nrowoffset, int ProcID) ");
	    ODB_fprintf(cfp,"{ ODBMAC_DIM(%s); }\n",name);
	    NL(1);

	    /* Removing and deallocating VIEW (the same as cancel (Ccl_V_) for views) */

	    ODB_fprintf(cfp,"#define Swapout_V_%s Ccl_V_%s\n",name, name);
	    NL(1);

	    /* VIEW SQL */

	    ODB_fprintf(cfp, "PRIVATE int\nSql_V_%s(", name);
	    ODB_fprintf(cfp, "FILE *fp, int mode, const char *prefix, const char *postfix, char **sqlout) ");
	    ODB_fprintf(cfp, "{ ODBMAC_VIEWSQL(); }\n");
	    NL(1);

	    /* VIEW colaux */

	    {
	      int kaux = pview->nselect;
	      int Ncols_tot = kaux + ncols_aux;
	      FPINF_KEYNUM(colaux, Ncols_tot);
	      if (ncols_aux >= 0) {
		int *colaux = NULL;
		CALLOC(colaux, Ncols_tot);
		ODB_fprintf(cfp,"PRIVATE int\nColAux_V_%s(",name);
		ODB_fprintf(cfp,"void *V, int colaux[], int colaux_len)\n{\n");
		for (i=0; i<nselect; i++) {
		  if (pview->is_formula[i]) {
		    uint flag = pview->sel[i]->aggr_flag;
		    if (pview->sel[i]->ncols_aux > 0) {
		      int naux, ns = 0, ne = pview->sel[i]->ncols_aux;
		      int ii = i;
		      for (naux=ns; naux < ne; naux++) {
			colaux[ii] = kaux+1; /* Fortran-index */
			ii = kaux++;
		      }
		    }
		    else {
		      colaux[i] = i+1; /* Fortran-index */
		    }
		  }
		  else {
		    colaux[i] = i+1; /* Fortran-index */
		  }
		}
		TAB(1); ODB_fprintf(cfp,"int filled = 0;\n");
		TAB(1); ODB_fprintf(cfp,"static const int ColAux_len = %d;\n",Ncols_tot);
		TAB(1); ODB_fprintf(cfp,"static const int ColAux[%d] = ",Ncols_tot);
		for (i=0; i<Ncols_tot; i++) {
		  ODB_fprintf(cfp,"%s%d",(i>0) ? "," : "{",colaux[i]);
		  FPINF_NUM(i, colaux[i]);
		}
		ODB_fprintf(cfp,"};\n");
		TAB(1); ODB_fprintf(cfp,"ODBMAC_COPY_COLAUX(%s);\n",name);
		TAB(1); ODB_fprintf(cfp,"return filled;\n");
		ODB_fprintf(cfp,"}\n");
		NL(1);
		FREE(colaux);
	      }
	      FPINF_KEYEND(colaux);
	    }

	    /* Sorting keys */

	    ODB_fprintf(cfp,
			"PRIVATE int *\nSortKeys_V_%s(void *V, int *NSortKeys) { ODBMAC_SORTKEYS(%s); }\n",
			name, name);
	    NL(1);

	    /* Inquire update status per column of SQL */

	    ODB_fprintf(cfp,
		    "PRIVATE int \nUpdateInfo_V_%s(void *V, const int ncols, int can_UPDATE[]) ",
		    name);
	    ODB_fprintf(cfp,"{ ODBMAC_UPDATEINFO(%s); }\n",name);
	    NL(1);

	    /* Aggregate funcs in use ? */

	    if (pview->select_aggr_flag) {
	      ODB_fprintf(cfp,
			  "PRIVATE int \nAggrInfo_V_%s(void *V, const int ncols, int aggr_func_flag[]) ",
			  name);
	      ODB_fprintf(cfp,"{ ODBMAC_AGGRINFO(%s); }\n",name);
	      NL(1);
	    }

	    /* Get indices */

	    ODB_fprintf(cfp,
		    "PRIVATE int *\nGetIndex_V_%s(void *V, const char *Table, int *Nidx)\n{\n",
		    name);
	    TAB(1); ODB_fprintf(cfp,"VIEW_%s *P = V;\n",name);
	    TAB(1); ODB_fprintf(cfp,"int Dummy = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"int *Nlen = Nidx ? Nidx : &Dummy;\n");
	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      if ((pview->active[i] & 0x1) == 0x1) { 
		TAB(1); ODB_fprintf(cfp,"ODBMAC_GETINDEX(%s);\n",s);
	      }
	    }
	    TAB(1); ODB_fprintf(cfp,"return NULL;\n}\n");
	    NL(1);

	    ODB_fprintf(cfp,
		    "PRIVATE int\nPutIndex_V_%s(void *V, const char *Table, int Nidx, int idx[], int by_address)\n{\n",
		    name);
	    TAB(1); ODB_fprintf(cfp,"/* *** Warning: This is a VERY DANGEROUS routine -- if misused !!! */\n");
	    TAB(1); ODB_fprintf(cfp,"VIEW_%s *P = V;\n",name);
	    TAB(1); ODB_fprintf(cfp,"int rc = 0;\n");
	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      if ((pview->active[i] & 0x1) == 0x1) { 
		TAB(1); ODB_fprintf(cfp,"ODBMAC_PUTINDEX(%s);\n",s);
	      }
	    }
	    TAB(1); ODB_fprintf(cfp,"return rc;\n}\n");
	    NL(1);

	    /* PE-info */

	    ODB_fprintf(cfp,"PRIVATE void\nPEinfo_V_%s(void *V, ODB_PE_Info *PEinfo) ",name);
	    ODB_fprintf(cfp,"{ ODBMAC_PEINFO_INIT(%s); }\n",name);
	    NL(1);

	    /* Initialization function for VIEW (table pointers are resolved at Create_Pool) */

	    ODB_fprintf(cfp,
		    "PRIVATE void *\nInit_V_%s(void *V, ODB_Pool *Pool, int Dummy1, int Dummy2, int it, int add_vars)\n{\n",
		    name);
	    TAB(1); ODB_fprintf(cfp,"VIEW_%s *P = V;\n",name);
	    TAB(1); ODB_fprintf(cfp,"int PoolNo = Pool->poolno;\n");
	    TAB(1); ODB_fprintf(cfp,"ODB_Funcs *pf;\n");
	    TAB(1); ODB_fprintf(cfp,"static ODB_CommonFuncs *pfcom = NULL; /* Shared between pools & threads */\n");

	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_START(Init_V_%s);\n",name);
	    }
	    TAB(1); ODB_fprintf(cfp,"if (!P) ALLOC(P, 1);\n");

	    TAB(1); ODB_fprintf(cfp,"P->PoolNo = PoolNo;\n");
	    TAB(1); ODB_fprintf(cfp,"P->Ncols = %d;\n",nselect+ncols_aux);
	    TAB(1); ODB_fprintf(cfp,"P->Nrows = 0;\n");
	    /* TAB(1); ODB_fprintf(cfp,"P->MaxRows = 0;\n"); */

	    TAB(1); ODB_fprintf(cfp,"P->USD_symbols = %d; /* In SELECT = %d ; In WHERE = %d */\n",
				count_USD_symbols,selsym_USD_symbols,pcond_USD_symbols);

	    TAB(1); ODB_fprintf(cfp,"P->Replicate_PE = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"P->Npes = 0;\n");
	    TAB(1); ODB_fprintf(cfp,"P->NrowVec = NULL;\n");
	    TAB(1); ODB_fprintf(cfp,"P->NrowOffset = NULL;\n");

	    TAB(1); ODB_fprintf(cfp,"P->NSortKeys = %d;\n",pview->norderby);
	    if (pview->norderby > 0) {
	      for (i=0; i<pview->norderby; i++) {
		TAB(1); 
		ODB_fprintf(cfp,"P->SortKeys[%d] = %d;\n",
			i,pview->mkeys[i]);
	      }
	    }
	    else {
	      TAB(1); ODB_fprintf(cfp,"P->SortKeys = NULL;\n");
	    }

	    TAB(1);  
	    ODB_fprintf(cfp,
			"/* Initially all bits set to 0, which corresponds to read-only -mode for all */\n");

	    TAB(1);  
	    ODB_fprintf(cfp,
			"memset(P->can_UPDATE, 0, sizeof(P->can_UPDATE));\n");

	    for (i=0; i<nselect; i++) {
	      TAB(1);
	      if (pview->readonly[i]) {
		ODB_fprintf(cfp,"ODBIT_unset(P->can_UPDATE, P->Ncols, MAXBITS, %d, %d); /* %s */\n", 
			    i, i, pview->tag[i]);
	      }
	      else {
		ODB_fprintf(cfp,"ODBIT_set(P->can_UPDATE, P->Ncols, MAXBITS, %d, %d);   /* %s */\n", 
			    i, i, pview->tag[i]);
	      }
	    }

	    if (pview->select_aggr_flag) {
	      int numaggr = 0;
	      TAB(1); ODB_fprintf(cfp,"/* Aggregate function flags */\n");
	      TAB(1); ODB_fprintf(cfp,"memset(P->aggr_func_flag, 0, sizeof(P->aggr_func_flag));\n");
	      for (i=0; i<nselect; i++) {
		if (pview->is_formula[i]) {
		  uint flag = pview->sel[i]->aggr_flag;
		  switch (flag) {
		  case ODB_AGGR_COUNT :          PrtAggrFlag(ODB_AGGR_COUNT); break;
		  case ODB_AGGR_COUNT_DISTINCT:  PrtAggrFlag(ODB_AGGR_COUNT_DISTINCT); break;
		  case ODB_AGGR_BCOUNT :         PrtAggrFlag(ODB_AGGR_BCOUNT); break;
		  case ODB_AGGR_BCOUNT_DISTINCT: PrtAggrFlag(ODB_AGGR_BCOUNT_DISTINCT); break;
		  case ODB_AGGR_MIN :            PrtAggrFlag(ODB_AGGR_MIN); break;
		  case ODB_AGGR_MAX :            PrtAggrFlag(ODB_AGGR_MAX); break;
		  case ODB_AGGR_SUM :            PrtAggrFlag(ODB_AGGR_SUM); break;
		  case ODB_AGGR_SUM_DISTINCT:    PrtAggrFlag(ODB_AGGR_SUM_DISTINCT); break;
		  case ODB_AGGR_AVG :            PrtAggrFlag(ODB_AGGR_AVG); break;
		  case ODB_AGGR_AVG_DISTINCT:    PrtAggrFlag(ODB_AGGR_AVG_DISTINCT); break;
		  case ODB_AGGR_MEDIAN :         PrtAggrFlag(ODB_AGGR_MEDIAN); break;
		  case ODB_AGGR_MEDIAN_DISTINCT: PrtAggrFlag(ODB_AGGR_MEDIAN_DISTINCT); break;
		  case ODB_AGGR_STDEV :          PrtAggrFlag(ODB_AGGR_STDEV); break;
		  case ODB_AGGR_STDEV_DISTINCT:  PrtAggrFlag(ODB_AGGR_STDEV_DISTINCT); break;
		  case ODB_AGGR_VAR :            PrtAggrFlag(ODB_AGGR_VAR); break;
		  case ODB_AGGR_VAR_DISTINCT:    PrtAggrFlag(ODB_AGGR_VAR_DISTINCT); break;
		  case ODB_AGGR_RMS :            PrtAggrFlag(ODB_AGGR_RMS); break;
		  case ODB_AGGR_RMS_DISTINCT:    PrtAggrFlag(ODB_AGGR_RMS_DISTINCT); break;
		  case ODB_AGGR_DOTP :           PrtAggrFlag(ODB_AGGR_DOTP); break;
		  case ODB_AGGR_DOTP_DISTINCT:   PrtAggrFlag(ODB_AGGR_DOTP_DISTINCT); break;
		  case ODB_AGGR_NORM :           PrtAggrFlag(ODB_AGGR_NORM); break;
		  case ODB_AGGR_NORM_DISTINCT:   PrtAggrFlag(ODB_AGGR_NORM_DISTINCT); break;
		  case ODB_AGGR_COVAR :          PrtAggrFlag(ODB_AGGR_COVAR); break;
		  case ODB_AGGR_CORR :           PrtAggrFlag(ODB_AGGR_CORR); break;
		  case ODB_AGGR_LINREGR_A :      PrtAggrFlag(ODB_AGGR_LINREGR_A); break;
		  case ODB_AGGR_LINREGR_B :      PrtAggrFlag(ODB_AGGR_LINREGR_B); break;
		  case ODB_AGGR_MINLOC :         PrtAggrFlag(ODB_AGGR_MINLOC); break;
		  case ODB_AGGR_MAXLOC :         PrtAggrFlag(ODB_AGGR_MAXLOC); break;
		  case ODB_AGGR_DENSITY :        PrtAggrFlag(ODB_AGGR_DENSITY); break;
		  default: break;
		  }
		}
	      } /* for (i=0; i<nselect; i++) */

	      /* For now: Auxiliary columns -- despite aggregate-func related -- are ignored */

	      TAB(1); ODB_fprintf(cfp,"P->numaggr = %d;\n", numaggr);
	      TAB(1); ODB_fprintf(cfp,"P->all_aggr = %d;\n", 
				  (numaggr == nselect) ? 1 : 0); /* 1 if all columns aggregate funcs */
	    } /* if (pview->select_aggr_flag) */

	    TAB(1); ODB_fprintf(cfp,"{\n");
	    TAB(2); ODB_fprintf(cfp,"ODB_Pool *p = Pool;\n");
	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      TAB(2); ODB_fprintf(cfp,"ODBMAC_ASSIGN_TABLEDATA(%s);\n",s);
	    }
	    TAB(1); ODB_fprintf(cfp,"}\n");
	    
	    for (i=0; i<nfrom; i++) {
	      char *s = pview->from[i]->table->name;
	      if ((pview->active[i] & 0x1) == 0x1) { 
		TAB(1); ODB_fprintf(cfp,"NULLIFY_INDEX(%s);\n",s);
	      }
	    }

	    TAB(1); ODB_fprintf(cfp,"if (!pfcom) { /* Initialize once only */\n");
	    TAB(2); ODB_fprintf(cfp,"CALLOC(pfcom,1);\n");

	    TAB(2); ODB_fprintf(cfp,"{ static char s[] = \"%s\"; pfcom->name = s; }\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->is_table = 0;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->is_considered = 0;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->ntables = %d;\n",nfrom);
	    TAB(2); ODB_fprintf(cfp,"pfcom->ncols = P->Ncols;\n");

	    TAB(2); ODB_fprintf(cfp,"pfcom->tableno = 0;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->rank = 0;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->wt = 0;\n");

	    TAB(2); ODB_fprintf(cfp,"pfcom->tags = V_%s_TAG;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->preptags = V_%s_PREPTAG;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->ntag = nV_%s_TAG;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->npreptag = nV_%s_PREPTAG;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->nmem = nV_%s_MEM;\n",name);
	    
	    TAB(2); ODB_fprintf(cfp,"pfcom->Info = NULL;\n");

	    TAB(2); ODB_fprintf(cfp,"pfcom->create_index = %d;\n", pview->create_index);

	    if (fpinf) {
	      ODB_fprintf(fpinf, "\n/create_index=%d\n", pview->create_index);
	      ODB_fprintf(fpinf, "\n/binary_index=%d\n", pview->binary_index ? 1 : 0);
	      if (pview->create_index <= 0) {
		ODB_fprintf(fpinf, "\n/use_indices=%d\n",pview->use_indices ? 1 : 0);
		ODB_fprintf(fpinf, "\n/use_index_name=%s\n",pview->use_index_name);
	      }
	    }

	    TAB(2); ODB_fprintf(cfp,"pfcom->init = Init_V_%s;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->swapout = Swapout_V_%s;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->dim = Dim_V_%s;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->sortkeys = SortKeys_V_%s;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->update_info = UpdateInfo_V_%s;\n",name);
	    TAB(2);
	    if (pview->select_aggr_flag) {
	      ODB_fprintf(cfp,"pfcom->aggr_info = AggrInfo_V_%s;\n",name);
	    }
	    else {
	      ODB_fprintf(cfp,"pfcom->aggr_info = NULL;\n");
	    }
	    TAB(2); ODB_fprintf(cfp,"pfcom->getindex = GetIndex_V_%s;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->putindex = PutIndex_V_%s;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->peinfo = PEinfo_V_%s;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->select = Sel_V_%s;\n",name);
	    TAB(2); ODB_fprintf(cfp,"pfcom->remove = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->cancel = Ccl_V_%s;\n",name);

	    for (j=0; j<ntypes; j++) {
	      char *pKey = Key[j];
	      if (typemask[j]) {
		TAB(2); ODB_fprintf(cfp,"pfcom->%sget = %sGet_V_%s;\n", 
				    pKey, pKey, name);
		if (pview->all_readonly) {
		  TAB(2); ODB_fprintf(cfp,"pfcom->%sput = NULL; /* All view entries read-only */\n", 
				      pKey);
		}
		else {
		  TAB(2); ODB_fprintf(cfp,"pfcom->%sput = %sPut_V_%s;\n", 
				      pKey, pKey, name);
		}
	      }
	      else {
		TAB(2); ODB_fprintf(cfp,"pfcom->%sget = NULL; /* no dbmgr allowed for this type */\n", 
				    pKey);
		TAB(2); ODB_fprintf(cfp,"pfcom->%sput = NULL; /* no dbmgr allowed for this type */\n", 
				    pKey);
	      }
	    }
	    
	    TAB(2); ODB_fprintf(cfp,"pfcom->load = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->store = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->pack = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->unpack = NULL;\n");
	    TAB(2); ODB_fprintf(cfp,"pfcom->sql = Sql_V_%s;\n",name);

	    TAB(2); ODB_fprintf(cfp,"pfcom->ncols_aux = %d;\n",ncols_aux);
	    if (ncols_aux >= 0) {
	      TAB(2); ODB_fprintf(cfp,"pfcom->colaux = ColAux_V_%s;\n",name);
	    }
	    else {
	      TAB(2); ODB_fprintf(cfp,"pfcom->colaux = NULL;\n");
	    }

	    TAB(2);
	    if (pview->select_distinct && nuniqueby > 0) {
	      ODB_fprintf(cfp,"pfcom->has_select_distinct = %d; /* SELECT DISTINCT ... */\n",
			  -nuniqueby);
	    }
	    else if (nuniqueby > 0) {
	      ODB_fprintf(cfp,"pfcom->has_select_distinct = %d; /* SELECT ... UNIQUEBY ... */\n",
			  nuniqueby);
	    }
	    else if (nfrom == 0) {
	      ODB_fprintf(cfp,"pfcom->has_select_distinct = 1; /* No active tables */\n");
	    }
	    else {
	      ODB_fprintf(cfp,"pfcom->has_select_distinct = 0;\n");
	    }
	    TAB(2); ODB_fprintf(cfp,"pfcom->has_usddothash = %d;\n",pview->usddothash ? 1 : 0);
	    TAB(1); ODB_fprintf(cfp,"} /* if (!pfcom) */\n");

	    TAB(1); ODB_fprintf(cfp,"ALLOC(pf, 1);\n");
	    TAB(1); ODB_fprintf(cfp,"pf->it = it;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->data = P;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->Res = NULL;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->tmp = NULL;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->pool = Pool;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->common = pfcom;\n");
	    TAB(1); ODB_fprintf(cfp,"pf->next = NULL;\n");

	    TAB(1); ODB_fprintf(cfp,"P->Funcs = pf;\n");
	    TAB(1); ODB_fprintf(cfp,"P->Handle = P->Funcs->pool->handle;\n");

	    TAB(1); ODB_fprintf(cfp,"if (add_vars) {\n");
	    
	    (void) assign_USD_symbols(NULL, pcond, 0, 0, NULL, name); /* Reset flags */

	    if (has_USD_symbols > 0) {
	      /* Set-symbols */
	      ODB_Symbol *psym;

	      init_list(NULL);
	      FPINF_KEY(set);

	      if (pcond_USD_symbols > 0) {
		/* Give priority to WHERE-symbols over the other symbols */
		(void) assign_USD_symbols(cfp, pcond, 2, 3, name, name);
	      } /* if (pcond_USD_symbols > 0) */
	      
	      if (selsym_USD_symbols > 0) {
		/* Also give priority to SELECT-expression -symbols over the other symbols */
		for (i=0; i<nselect; i++) {
		  if (pview->sel[i] && pview->sel[i]->expr) {
		    ODB_Tree *expr = pview->sel[i]->expr;
		    (void) assign_USD_symbols(cfp, expr, 2, 3, name, name);
		  }
		} /* for (i=0; i<nselect; i++) */
	      } /* if (selsym_USD_symbols > 0) */

	      for (psym = ODB_start_symbol(); psym != NULL; psym = psym->next) {
		if (psym->kind == ODB_USDNAME && psym->only_view) {
		  if (DONE_SYM(psym->flag, 4) || DONE_SYM(psym->flag, 5)) {
		    char *s = psym->name;
		    double value = psym->dval;
		    if (!in_list(s)) {
		      if (!IS_POOLNO(s) && !IS_USDHASH(s)) {
			TAB(2);
			ODB_fprintf(cfp,"Pool->add_var(\"%s\", \"%s\", \"%s\", it, USD_%s_%s);\n",
				    odb_label, s, name ? name : "???", s+1, odb_label);
		      }
		      add_list(s);
		      FPINF_ITEMFLP(s, value);
		    }
		  } /* if (DONE_SYM(psym->flag, 4) || DONE_SYM(psym->flag, 5)) */
		}
		if (psym->kind == ODB_USDNAME) {
		  RESET_SYM(psym->flag, 4); /* Reset for subsequent uses */
		  RESET_SYM(psym->flag, 5); /* Reset for subsequent uses */
		}
	      }

	      destroy_list();
	      /* Make sure ODBTk's $__maxcount__ gets there, too */
	      FPINF_ITEMFLP(sym_maxcount->name, sym_maxcount->dval);
	      FPINF_KEYEND(set);
	    } /* if (has_USD_symbols > 0) */
	      
	    TAB(1);  ODB_fprintf(cfp,"} /* if (add_vars) */\n");
	    
	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_END(0);\n");
	    }
	    TAB(1); ODB_fprintf(cfp,"return P;\n}\n");
	    NL(1);

	    ODB_fprintf(cfp,"/* *************** End of VIEW \"%s\" *************** */\n",name);
	    NL(1);

	    FREE(done);

	    ODB_fprintf(cfp,
		    "PUBLIC ODB_Funcs *\nAnchor2%s(void *V, ODB_Pool *pool, int *nviews, int it, int add_vars)\n{\n",
		    dofunc);
	    TAB(1); ODB_fprintf(cfp,"VIEW_%s *P = V;\n",name);
	    TAB(1); ODB_fprintf(cfp,"ODB_Funcs *pf;\n");
	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_START(Anchor2%s);\n",dofunc);
	    }
	    TAB(1); ODB_fprintf(cfp,"if (!P) P = Init_V_%s(NULL, pool, -1, -1, it, add_vars);\n",name);
	    TAB(1); ODB_fprintf(cfp,"if (nviews) *nviews = 1;\n");
	    TAB(1); ODB_fprintf(cfp,"pf = P->Funcs;\n");
	    if (insert_drhook) {
	      TAB(1); ODB_fprintf(cfp,"DRHOOK_END(0);\n");
	    }
	    TAB(1); ODB_fprintf(cfp,"return pf;\n");
	    ODB_fprintf(cfp,"}\n");

	    FREE(dofunc);

	    FCLOSE(fpview);
	    FCLOSE(fpinf);
	  } /* if (nfrom >= 0) */
	} /* if (pview) */ 

	cfp = ODB_popFILE();
      }
      break;

    default:
      SETMSG3("DDL-source line#%d: Key '%s' (%d) not implemented\n",
	      lineno, ODB_keymap(what), what);
      YYerror(msg);
      break;
    } /* switch (what) */
  }
}



PUBLIC ODB_Filelist *
genc(Boolean views_only)
{
  FILE *cfp = NULL;

  {
    char *p;
    int len;

    len = strlen(odb_label);

    if (views_only) {
      fphdr = fpdevnull;
      fptable = fpdevnull;
    }
    else {
      ALLOC(p, len + 5);
      sprintf(p, "%s.h", odb_label);
      if (ABS(filtered_info) < 2) {
	fphdr = FOPEN(p, "w");
      }
      else {
	fphdr = NULL;
      }
      FREE(p);

      ALLOC(p, len + 3);
      sprintf(p, "%s.c", odb_label);
      if (ABS(filtered_info) < 2) {
	fptable = FOPEN(p, "w");
      }
      else {
	fptable = NULL;
      }
      FREE(p);
    }
  }
  
  cfp = fphdr;

  if (!views_only) { /* Part#1 */
    char *odb_h = strrchr(ODB_H,'/');
    if (odb_h) odb_h++; else odb_h = ODB_H;

    ODB_fprintf(cfp,"#ifndef ODB_GENCODE\n");
    ODB_fprintf(cfp,"#define ODB_GENCODE 1\n");
    ODB_fprintf(cfp,"#endif\n");
    NL(1);

    {
      int numeric = 0;
      const char *vstr = codb_versions_(NULL, NULL, &numeric, NULL);
      ODB_fprintf(cfp,"\n/* Software revision : %s (%d) */\n", vstr, numeric);
    }
    NL(1);

#ifdef INLINE_ODB_H
    ODB_fprintf(cfp,"#ifdef ODB_H\n");
    ODB_fprintf(cfp,"#include \"%s\"\n",odb_h);
    ODB_fprintf(cfp,"#else\n");
    NL(1);
    ODB_fprintf(cfp,"/* Start of include '%s' */\n",ODB_H);
    NL(1);
    {
      if (ODB_copyfile(cfp, ODB_H, NULL, NULL, 0, 1) <= 0) {
	SETMSG1("Problems with master include file '%s'",ODB_H);
	YYerror(msg);
      }
    }
    NL(1);
    ODB_fprintf(cfp,"/* End of include '%s' */\n",ODB_H);
    NL(1);
    ODB_fprintf(cfp,"#endif /* ifdef ODB_H */\n");
#else
    ODB_fprintf(cfp,"#include \"%s\"\n",odb_h);
#endif
    {
      const char odb_macros_h[] = "odb_macros.h";
      ODB_fprintf(cfp,"#include \"%s\"\n",odb_macros_h);
    }
    if (insert_drhook) {
      const char cdrhook_h[] = "cdrhook.h";
      ODB_fprintf(cfp,"#include \"%s\"\n",cdrhook_h);
    }
    NL(1);

    ODB_fprintf(cfp,"#define ODB_LABEL    \"%s\"\n",odb_label);
    NL(1);

    ODB_fprintf(cfp,"\n/* Compilation options used :\n%s",ARGH);
    process_one_tables(cfp, "\n\t ","");
    ODB_fprintf(cfp,"\n\n*/\n");
    NL(1);
    
    /* Table hierarchy */
    if (table_hier && ODB_ntables > 0) {
      int j;
      int len = 10;
      ODB_fprintf(cfp,
		  "/* ----- Table hierarchy (= the default scanning order) : # of tables = %d\n",
		  ODB_ntables);
      NL(1);
      for (j=0; j<ODB_ntables; j++) {
	ODB_Table *ptable = table_hier[j];
	int len_t = strlen(ptable->table->name);
	len = MAX(len, len_t);
      }
      TAB(1);
      ODB_fprintf(cfp,"%10s    %*s : %10s   %s\n",
		  "Rank#", len, "Table", "Order#", "Weight");
      TAB(1);
      ODB_fprintf(cfp,"%10s    %*s : %10s   %s\n",
		  "-----", len, "-----", "------", "------");
      for (j=0; j<ODB_ntables; j++) {
	ODB_Table *ptable = table_hier[j];
	TAB(1);
	ODB_fprintf(cfp,"%10d    %*s : %10d   %.6f\n",
		    ptable->rank, len, ptable->table->name,
		    ptable->tableno, ptable->wt);
      }
      NL(1);
      ODB_fprintf(cfp,"   ----- End of table hierarchy ----- */\n"); 
      NL(1);
    } /* if (table_hier && ODB_ntables > 0) */

    /* One common function to print flags-file */

    ODB_fprintf(cfp,"PUBLIC void %s_print_flags_file(void);\n",odb_label);
    NL(1);

    ODB_fprintf(cfp,"#if defined(ODB_MAINCODE)\n");
    NL(1);
    (void) write_setsymbols(cfp, "PUBLIC", 0);
    ODB_fprintf(cfp,"#endif /* defined(ODB_MAINCODE) */\n");
    NL(1);

    /* Just in case there are no views, which would have invoked ... */
    ODB_link_massage();

    /* Write alias types only */
    {
      ODB_Cmd *pcmd;
      for (pcmd = ODB_start_cmd(); pcmd != NULL; pcmd = pcmd->next) {
	ODB_Tree *pnode = pcmd->node;
	if (pnode) {
	  if (pnode->what == ODB_TYPE) {
	    dump_c(cfp, pcmd->lineno, pcmd->node);
	    NL(1);
	  }
	} /* if (pnode) */
      } /* for (pcmd = ODB_start_cmd(); pcmd != NULL; pcmd = pcmd->next) */
    }
    
    /* Create data structures */
    {
      ODB_Table *ptable;
      Boolean done_INT = 0;
      Boolean done_UINT = 0;
      Boolean done_DBL = 0;
      
      init_list(NULL);
      
      for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	int j, nsym = ptable->nsym;
	
	for (j=0; j<nsym; j++) {
	  char *p;
	  ODB_Type *ptype = ptable->type[j];
	  /* ODB_Symbol *psym = ptable->sym[j]; */
	  char *type = get_typename(ptype);
	  
	  ALLOC(p, strlen(type) + 3);
	  sprintf(p, "/%s/", type);
	  
	  if (!in_list(p)) {
	    char *ftype = NULL;
	    int pmethod = get_PKmethod(type, &ftype, NULL,NULL);
	    
	    add_list(p);
	    ODB_fprintf(cfp,"DefineDS(%s);\n",type);
	    
	    if (pmethod == 0) {
	      ODB_fprintf(cfp,"DS_DummyPacks(%s, %s, %s)\n",odb_label,ftype,type);
	    }
	    else {
	      if      (!done_INT  && strequ(ftype,"INT")) {
		ODB_fprintf(cfp,"#define %s_pack_%s ODB_pack_%s\n",odb_label,ftype,ftype);
		ODB_fprintf(cfp,"#define %s_unpack_%s ODB_unpack_%s\n",odb_label,ftype,ftype);
		done_INT = 1;
	      }
	      else if (!done_UINT && strequ(ftype,"UINT")) {
		ODB_fprintf(cfp,"#define %s_pack_%s ODB_pack_%s\n",odb_label,ftype,ftype);
		ODB_fprintf(cfp,"#define %s_unpack_%s ODB_unpack_%s\n",odb_label,ftype,ftype);
		done_UINT = 1;
	      }
	      else if (!done_DBL  && strequ(ftype,"DBL")) {
		ODB_fprintf(cfp,"#define %s_pack_%s ODB_pack_%s\n",odb_label,ftype,ftype);
		ODB_fprintf(cfp,"#define %s_unpack_%s ODB_unpack_%s\n",odb_label,ftype,ftype);
		done_DBL = 1;
	      }
	    }
	    ODB_fprintf(cfp,"DS_Unpacking(%s, %s, %s)\n", odb_label, ftype, type); 
	    ODB_fprintf(cfp,"DS_Packing(%s, %s, %s)\n", odb_label, ftype, type); 
	    NL(1);
	    
	    FREE(ftype);
	  }
	  
	  FREE(p);
	} /* for (j=0; j<nsym; j++) */
      }
      
      destroy_list();
    }
    NL(1);
  }  /* if (!views_only) { -- Part#1 */

  /* Write statements (type, table, view etc. defs) */
  
  cfp = fptable;
    
  ODB_fprintf(cfp,"#define ODB_MAINCODE 1\n");
  NL(1);
  set_optlevel(cfp, 0);
  ODB_fprintf(cfp,"#include \"%s.h\"\n",odb_label);
  NL(1);

  {
    ODB_Cmd *pcmd;
    for (pcmd = ODB_start_cmd(); pcmd != NULL; pcmd = pcmd->next) {
      dump_c(cfp, pcmd->lineno, pcmd->node);
    }
  }

  if (!views_only) { /* Part#2 */
    /* Write the "odb_label" specific information */

    /* Printing of flags-file */

    ODB_fprintf(cfp,
	    "PUBLIC void \n%s_print_flags_file(void)\n{\n", odb_label);
    TAB(1); ODB_fprintf(cfp,"int rc = 0, io = -1;\n");
    TAB(1); ODB_fprintf(cfp,"FILE *fp = NULL;\n");
    TAB(1); 
    ODB_fprintf(cfp,
	    "cma_open_(&io, \"%s.flags\", \"w\", &rc, strlen(\"%s.flags\"), strlen(\"w\"));\n",
	    odb_label, odb_label);
    TAB(1); ODB_fprintf(cfp, "if (rc != 1) return; /* multi-bin file ==> forget flags-file */\n");
    TAB(1); ODB_fprintf(cfp, "fp = CMA_get_fp(&io);\n");
    TAB(1); ODB_fprintf(cfp, "if (!fp) return; /* pointer NULL ==> forget the flags-file ;-( */\n");
    if (!reset_align_issued) {
      /* grep for lines starting with "-A" */
      if (ODB_grepfile(cfp, "^-A",
		       "$ODB_COMPILER_FLAGS", 
		       "  fprintf(fp,\"%s\\n\",\"", "\");", 1, 0) < 0) { 
	/* < 0, not <= 0 to allow file '/dev/null' in $ODB_COMPILER_FLAGS */
	char *env = getenv("ODB_COMPILER_FLAGS");
	SETMSG1("Problems accessing $ODB_COMPILER_FLAGS-file '%s'", env ? env : "");
	YYerror(msg);
      }
    }
    if (!reset_onelooper_issued) {
      /* grep for lines starting with "-1" */
      if (ODB_grepfile(cfp, "^-1",
		       "$ODB_COMPILER_FLAGS", 
		       "  fprintf(fp,\"%s\\n\",\"", "\");", 1, 0) < 0) { 
	/* < 0, not <= 0 to allow file '/dev/null' in $ODB_COMPILER_FLAGS */
	char *env = getenv("ODB_COMPILER_FLAGS");
	SETMSG1("Problems accessing $ODB_COMPILER_FLAGS-file '%s'", env ? env : "");
	YYerror(msg);
      }
    }
    if (one_tables) {
      process_one_tables(cfp,"  fprintf(fp,\"","\\n\");\n");
    }
    TAB(1); ODB_fprintf(cfp, "cma_close_(&io, &rc);\n}\n");
    NL(1);

    /* Add funcs */
    
    ODB_fprintf(cfp,
		"PRIVATE int \nCreate_Funcs(ODB_Pool *pool, int is_new, int io_method, int it)\n{\n");
    TAB(1); ODB_fprintf(cfp,"int nfuncs = 0;\n");
    TAB(1); ODB_fprintf(cfp,"static int first_time = 1;\n");
    {
      ODB_Table *ptable;
      int num_tables = 0;
      int id = 0;
      
      /* Create caching info about TableIsConsidered */

      id = 0;
      for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	++id;
      }
      num_tables = id;
      TAB(1); ODB_fprintf(cfp,"static int IsConsidered[%d];\n",num_tables);

      if (insert_drhook) {
	TAB(1); ODB_fprintf(cfp,"DRHOOK_START(Create_Funcs);\n");
      }

      TAB(1); ODB_fprintf(cfp,"if (first_time) {\n");
      id = 0;
      for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	char *name = ptable->table->name;
	TAB(2); ODB_fprintf(cfp,"ODBMAC_INIT_IsConsidered(%s, %d);\n",name,id++);
      }
      TAB(2); ODB_fprintf(cfp,"first_time = 0;\n");
      TAB(1); ODB_fprintf(cfp,"} /* if (first_time) */\n");

      /* Declare tables and setup, alloc & add table funcs (all in one go) */

      id = 0;
      for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	char *name = ptable->table->name;
	Boolean dummy_table = is_dummy_table(name);
	TAB(1); ODB_fprintf(cfp,"ODBMAC_CREATE_TABLE(%s, %s, %d, %d);\n",
			    odb_label,name,id++,
			    dummy_table ? 0 : 1);
      } /* for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) */
    
      if (insert_drhook) {
	TAB(1); ODB_fprintf(cfp,"DRHOOK_END(0);\n");
      }
    }

    TAB(1); ODB_fprintf(cfp,"return nfuncs;\n}\n");
    NL(1);

    /* Establish the data "load" and "store" -functions for the pool */
    
    /* Load */
    
    {
      ODB_Table *ptable;
     
      ODB_fprintf(cfp,"PRIVATE int\nLoad_Pool(ODB_Pool *P, int io_method)\n{\n");
      TAB(1); ODB_fprintf(cfp,"int rc = 0;\n");
      TAB(1); ODB_fprintf(cfp,"int Nbytes = 0;\n");
      TAB(1); ODB_fprintf(cfp,"ODB_Funcs *pf = P->funcs;\n");
      TAB(1); ODB_fprintf(cfp,"if (io_method != 5) {\n");
      
      /* Read tables */
      
      for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	char *s = ptable->table->name;
	TAB(2); ODB_fprintf(cfp, "Call_TABLE_Load(%s, pf, 1);\n", s);
      }

      TAB(1); ODB_fprintf(cfp,"} /* if (io_method != 5) */\n");
      TAB(1); ODB_fprintf(cfp,"return (rc < 0) ? rc : Nbytes;\n}\n");
      NL(1);
    }
  
    /* Store */
    
    {
      ODB_Table *ptable;
      
      ODB_fprintf(cfp,"PRIVATE int\nStore_Pool(const ODB_Pool *P, int io_method)\n{\n");
      if (readonly_mode) {
	TAB(1); ODB_fprintf(cfp,"/* Data layout was compiled under Read/Only mode (-r option or ODB_READONLY=1) */\n");
	TAB(1); ODB_fprintf(cfp,"return 0;\n}\n");
      }
      else {
	TAB(1); ODB_fprintf(cfp,"int rc = 0;\n");
	TAB(1); ODB_fprintf(cfp,"int Nbytes = 0;\n");
	TAB(1); ODB_fprintf(cfp,"ODB_Funcs *pf = P->funcs;\n");
	TAB(1); ODB_fprintf(cfp,"if (io_method != 5) {\n");
	
	/* Write tables */
	
	for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
	  char *s = ptable->table->name;
	  TAB(2); ODB_fprintf(cfp, "Call_TABLE_Store(%s, pf, 1);\n",s);
	}

	TAB(1); ODB_fprintf(cfp,"} /* if (io_method != 5) */\n");
	TAB(1); ODB_fprintf(cfp,"return (rc < 0) ? rc : Nbytes;\n}\n");
      }

      NL(1);
    }
  }  /* if (!views_only) { -- Part#2 */

  if (fphdr != fpdevnull) FCLOSE(fphdr);

  if (views_only) {
    if (fptable != fpdevnull) FCLOSE(fptable);
    goto finish;
  }

  /* Anchor-function to bind the database dynamically with
     its create, load and store functions */

  cfp = fptable;

  ODB_fprintf(cfp,
	  "PUBLIC ODB_Funcs *\nAnchor2%s(void *V, ODB_Pool *pool, int *ntables, int it, int add_vars)\n{\n",
	  odb_label);
  TAB(1); ODB_fprintf(cfp,"ODB_Anchor_Funcs *func = V;\n");
  TAB(1); ODB_fprintf(cfp,"ODB_Pool *p = pool;\n");
  if (insert_drhook) {
    TAB(1); ODB_fprintf(cfp,"DRHOOK_START(Anchor2%s);\n",odb_label);
  }
  TAB(1); 
  ODB_fprintf(cfp,
	      "/* A special case : ntables not a NULL => return no. of tables */\n");
  TAB(1); ODB_fprintf(cfp,"if (ntables) {\n");
  /* Fixed no. of TABLEs for this definition */
  {
    int ntables = 0;
    ODB_Table *ptable;
    
    for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
      ntables++;
    }
    TAB(2); ODB_fprintf(cfp,"*ntables = %d;\n",ntables);
  }

  TAB(2); ODB_fprintf(cfp,"goto finish;\n");
  TAB(1); ODB_fprintf(cfp,"}\n");

  TAB(1); ODB_fprintf(cfp,"func->create_funcs = Create_Funcs;\n");
  TAB(1); ODB_fprintf(cfp,"func->load         = Load_Pool;\n");
  TAB(1); ODB_fprintf(cfp,"func->store        = Store_Pool;\n");

  /* Set-symbols */
  TAB(1); ODB_fprintf(cfp,"if (add_vars) {\n");
  {
    ODB_Symbol *psym;
    for (psym = ODB_start_symbol(); psym != NULL; psym = psym->next) {
      if (psym->kind == ODB_USDNAME) {
	char *s = psym->name;
	if (!IS_POOLNO(s) && !IS_USDHASH(s)) {
	  char *tmp;
	  ALLOC(tmp, strlen(s+1) + 5);
	  sprintf(tmp, "USD_%s", s+1);
	  TAB(2); ODB_fprintf(cfp,
			      "p->add_var(p->dbname, \"%s\", NULL, it, %s_%s);\n",
			      s, tmp, odb_label);
	  FREE(tmp);
	}
      }
    }
  }
  TAB(1);  ODB_fprintf(cfp,"} /* if (add_vars) */\n");
      
  TAB(1); ODB_fprintf(cfp,"finish:\n");
  if (insert_drhook) {
    TAB(1); ODB_fprintf(cfp,"DRHOOK_END(0);\n");
  }
  TAB(1); ODB_fprintf(cfp,"return NULL;\n}\n");

  if (fptable != fpdevnull) FCLOSE(fptable);


 finish:

  if (!views_only && (filtered_info == 0 || filtered_info == -1)) {

    /* Create setup file for subsequent uses */

    extern char *IOresolve_env(const char *str); /* from libioassign.a */
    ODB_Table *ptable;
    char *setup_file = getenv("ODB_SETUP_FILE");
    char *params, *maxproc;
    char *odb_ld, *odb_libs;
    char *shell;
    Boolean opened = 0;
    FILE *fp = stderr;
    
    if (setup_file) {
      char *file = IOresolve_env(setup_file);
      fp = strequ(file,"/dev/null") ? NULL : FOPEN(file, "w");
      if (!fp) {
	if (!strequ(file,"/dev/null")) PERROR(file);
      }
      else {
	fprintf(stderr,"Creating ODB setup-file '%s' ...\n",file);
	opened = 1;
      }
      FREE(file);
    }

    odb_ld = getenv("ODB_LD");
    odb_ld = odb_ld ? STRDUP(odb_ld) : STRDUP("");

    odb_libs = getenv("ODB_LIBS");
    odb_libs = odb_libs ? STRDUP(odb_libs) : STRDUP("");

    shell = getenv("ODB_SETUP_SHELL");
    if (!shell) shell = getenv("SHELL");
    shell = shell ? STRDUP(shell) : STRDUP("/bin/ksh");
   
    ODB_fprintf(fp,"#!%s\n",shell);
    if (strstr(shell,"/ksh") || strstr(shell,"/bash")) {
      ODB_fprintf(fp,"%sexport ODB_LD=\"%s\"\n",
	      strlen(odb_ld) > 0 ? "" : "# ",
	      odb_ld);
      ODB_fprintf(fp,"%sexport ODB_LIBS=\"%s\"\n",
	      strlen(odb_libs) > 0 ? "" : "# ",
	      odb_libs);
    }
    else if (strstr(shell,"/sh")) {
      ODB_fprintf(fp,"%sODB_LD=\"%s\"; export ODB_LD\n",
	      strlen(odb_ld) > 0 ? "" : "# ",
	      odb_ld);
      ODB_fprintf(fp,"%sODB_LIBS=\"%s\"; export ODB_LIBS\n",
	      strlen(odb_libs) > 0 ? "" : "# ",
	      odb_libs);
    }
    else {
      ODB_fprintf(fp,"%ssetenv ODB_LD \"%s\"\n",
	      strlen(odb_ld) > 0 ? "" : "# ",
	      odb_ld);
      ODB_fprintf(fp,"%ssetenv ODB_LIBS \"%s\"\n",
	      strlen(odb_libs) > 0 ? "" : "# ",
	      odb_libs);
    }

    params = getenv("ODB_IOASSIGN_PARAMS");
    params = params ? STRDUP(params) : STRDUP("");

    /* Value for the following variable doesn't matter anymore (since 30R2 onwards) */
    maxproc = getenv("ODB_IOASSIGN_MAXPROC");
    maxproc = maxproc ? STRDUP(maxproc) : STRDUP("64");

    ODB_fprintf(fp,
	    "ioassign -s -a \\$ODB_SRCPATH_%s/%s.dd %s\n",
	    odb_label,odb_label,odb_label);

    ODB_fprintf(fp,
	    "ioassign -s -a \\$ODB_SRCPATH_%s/%s.sch %s.sch\n",
	    odb_label,odb_label,odb_label);

    ODB_fprintf(fp,
	    "ioassign -s %s -a \\$ODB_SRCPATH_%s/%s.iomap %s.iomap\n",
	    params, odb_label,odb_label,odb_label);

    ODB_fprintf(fp,
	    "ioassign -s -a \\$ODB_SRCPATH_%s/%s.flags %s.flags\n",
	    odb_label,odb_label,odb_label);

    for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
      char *s = ptable->table->name;

      ODB_fprintf(fp,
	      "ioassign -s %s -a \\$ODB_SRCPATH_%s/dca/%s.dca %s.%s.dca\n",
	      params,
	      odb_label, s,
	      odb_label, s);
    }

    for (ptable = ODB_start_table(); ptable != NULL; ptable = ptable->next) {
      char *s = ptable->table->name;

      ODB_fprintf(fp,
	      "ioassign -s -n %s %s -a \\$ODB_DATAPATH_%s/%%d/%s %s.%s.%%d\n",
	      maxproc, params,
	      odb_label, s,
	      odb_label, s);
    }

    if (opened) FCLOSE(fp);

    ALLOC(flist, 1);
    flist->filename = STRDUP(odb_label);
    flist->create_so = 1;
    flist->next = NULL;
    if (!flist_start) {
      flist_start = flist;
    }
    else {
      flist_last->next = flist;
    }
    flist_last = flist;

    FREE(odb_ld);
    FREE(odb_libs);
    FREE(shell);
    FREE(params);
    FREE(maxproc);
  }

  if (genstatic == 1) {
    ODB_View *pview;
    char *pstatic, *p;
    FILE *cfp;
    int viewlen = 0;
    extern int Ccompile_Sstatic_stubb;
    
    int len = strlen(odb_label);

    ALLOC(p, len + 9);
    sprintf(p,"%s_Sstatic", odb_label);

    if (Ccompile_Sstatic_stubb) {
      ALLOC(flist, 1);
      flist->filename = STRDUP(p);
      flist->create_so = 0;
      flist->next = NULL;
      if (!flist_start) {
	flist_start = flist;
      }
      else {
	flist_last->next = flist;
      }
      flist_last = flist;
    }

    ALLOC(pstatic, strlen(p) + 3);
    sprintf(pstatic,"%s.c", p);

    if (incremental) {
      if (is_regular_file_(pstatic)) {
	char *newname = NULL;
	ALLOC(newname, strlen(pstatic) + 5);
	sprintf(newname,"%s.old",pstatic);
	if (rename_file_(pstatic,newname) != 0) {
	  incremental = 0; /* Rename failed, but we don't want to abort */
	}
	FREE(newname);
      }
      else {
	incremental = 0;
      }
    }

    if (ABS(filtered_info) < 2) {
      cfp = FOPEN(pstatic, "w");
    }
    else {
      cfp = NULL;
    }
    FREE(p);

    ODB_fprintf(cfp,"#define ODB_GENCODE 0\n");
    
    set_optlevel(cfp, 0);
    ODB_fprintf(cfp,"#include \"%s.h\"\n",odb_label);

    NL(1);

    ODB_fprintf(cfp,"PUBLIC void %s_static_init() {\n",odb_label);
    ODB_fprintf(cfp,"ODB_ANCHOR(%s);\n",odb_label);

    viewlen = 0;
    for (pview = ODB_start_view(); pview != NULL; pview = pview->next) {
      char *name = pview->view->name;
      ODB_fprintf(cfp,"ODB_ANCHOR_VIEW(%s, %s );\n",odb_label,name);
      viewlen += strlen(name) + 1;
    }

    if (incremental && viewlen > 0 && ABS(filtered_info) < 2) {
      /* Strip away those views that were just recompiled */
      char *cmd = NULL;
      char *delim = "(";
      FCLOSE(cfp);
      viewlen += 2 * strlen(pstatic) + 256;
      ALLOC(cmd, viewlen);

      if (verbose) {
	sprintf(cmd,"ls -l %s.old",pstatic);
        fprintf(stderr,"## Executing: %s\n",cmd);
	(void)system(cmd);
      }

      if (verbose) {
	sprintf(cmd,"wc -l %s.old",pstatic);
	fprintf(stderr,"## Executing: %s\n",cmd);
	(void)system(cmd);
      }

      sprintf(cmd,"egrep ODB_ANCHOR_VIEW %s.old | egrep -v ' ",pstatic);
      for (pview = ODB_start_view(); pview != NULL; pview = pview->next) {
	char *name = pview->view->name;
	strcat(cmd,delim);
	strcat(cmd,name);
	delim = "|";
      }
      delim = ") ' >> ";
      strcat(cmd,delim);
      strcat(cmd,pstatic);
      if (verbose) fprintf(stderr,"## Executing\n\t%s\n",cmd);
      (void)system(cmd);
      FREE(cmd);
      {
	extern char *IOresolve_env(const char *str); /* from libioassign.a */
	char *pfile = IOresolve_env(pstatic);
	cfp = FOPEN(pfile,"a");
	ODB_fprintf(cfp,"}\n");
	FCLOSE(cfp);
	FREE(pfile);
      }
    }
    else {
      ODB_fprintf(cfp,"}\n");
      FCLOSE(cfp);
    }

    FREE(pstatic);
  }

  return flist_start;
}
