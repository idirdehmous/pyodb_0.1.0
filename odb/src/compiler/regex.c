#include "defs.h"

/* See also lib/wildcard.c */

#if defined(LINUX) || defined(SUN4) || defined(RS6K)
/* Watch for this; This GNU-thing may now be okay for other systems, too */
#include <regex.h>
#define REGEX_DEF(p)      regex_t p
#define REGCMP(p,pattern) status = regcomp(&p,pattern,REG_EXTENDED | REG_NOSUB)
#define REGEX(p,s)        status = regexec(&p,s,0,NULL,0)
#define REGFREE(p)        regfree(&p)
#else
#include <libgen.h>
#define REGEX_DEF(p)      char *p
#define REGCMP(p,pattern) p = regcmp(pattern,NULL), status = (p ? 0 : 1)
#define REGEX(p,s)        status = (regex(p,s) ? 0 : 1)
#define REGFREE(p)
#endif

PUBLIC int 
ODB_regex(const char *s, ODB_Table **from, const int *from_attr, int nfrom)
{
  int count = 0;
  int len;
  char *rex = NULL;
  int status;
  REGEX_DEF(p);
  
  Boolean neg = 0;
  Boolean ignore_case = 0;

  if (*s == '/') {
    rex = STRDUP(s+1);
  }
  else if (strnequ(s,"!/",2) || strnequ(s,"~/",2)) {
    rex = STRDUP(s+2);
    neg = 1;
  }
  else {
    return 0;
  }

  len = STRLEN(rex);
  ignore_case = (len > 3 && strncaseequ(&rex[len-2],"/i",2)); 

  {
    char *ptmp = strrchr(rex,'/');
    if (ptmp) *ptmp = '\0';
  }

  if (ignore_case) {
    char *c = ODB_lowercase(rex);
    FREE(rex);
    rex = c;
  }

  REGCMP(p, rex);

  if (status == 0) {
    int j;
    /* fprintf(stderr,"$$$ regex-analyzing '%s'; nfrom=%d\n",s,nfrom); */
    for (j=0; j<nfrom; j++) {
      ODB_Table *pfrom = from[j];
      int flag = from_attr[j];
      /* Ignore auto-inserted tables, since these were not tables that user intended to have */
      if ((flag & ODB_FROM_ATTR_INSERT) == ODB_FROM_ATTR_INSERT) continue;
      if (pfrom && pfrom->expname) {
	int k;
	for (k=0; k<pfrom->nsym; k++) {
	  char *c = ignore_case ? 
	    ODB_lowercase(pfrom->expname[k]) : STRDUP(pfrom->expname[k]);
	  REGEX(p, c);
	  FREE(c);
	  if ((!neg && status==0) || (neg && status!=0)) {
	    char *pvar = strchr(pfrom->expname[k],':');
	    if (pvar) {
	      char *var = STRDUP(pvar + 1);
	      ODB_pushstr(var);
	      count++;
	      /*
	      fprintf(stderr,"\t$$$ table='%s': match#%d for '%s'\n",pfrom->table->name,count,var);
	      {
		int kk;
		for (kk=0; kk<pfrom->nsym; kk++) {
		  fprintf(stderr,"\t  >>> '%s': col#%d = '%s'\n",pfrom->table->name,kk+1,pfrom->expname[kk]);
		}
	      }
	      */
	    }
	  } /* if ((!neg && status==0) || (neg && status!=0)) */
	} /* for (k=0; k<pfrom->nsym; k++) */
      } /* if (pfrom && pfrom->expname) */
    } /* for (j=0; j<nfrom; j++) */
  }
  
  REGFREE(p);

  FREE(rex);

  return count;
}
