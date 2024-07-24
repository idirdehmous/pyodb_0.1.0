#include "odb.h"
#include "evaluate.h"
#include "cdrhook.h"

#if defined(LINUX) || defined(SUN4) || defined(RS6K) || defined(SV2)
/* Watch for this; This GNU-thing may now be okay for other systems, too */
#include <regex.h>
#define REGEX_DEF(p)      regex_t p
#define REGCMP(p,pattern) status = regcomp(&p,pattern,REG_EXTENDED | REG_NOSUB | REG_ICASE)
#define REGEX(p,s)        status = regexec(&p,s,0,NULL,0)
#define REGFREE(p)        regfree(&p)
#else
#ifdef ALPHA
#define _XOPEN_SOURCE_EXTENDED 1
#endif
#include <libgen.h>
#define REGEX_DEF(p)      char *p
#define REGCMP(p,pattern) p = regcmp(pattern,NULL), status = (p ? 0 : 1)
#define REGEX(p,s)        status = (regex(p,s) ? 0 : 1)
#define REGFREE(p)
#endif


PUBLIC double
ODBoffset(double colvar, double offset)
{
  return colvar;
}


PUBLIC int
ODB_Common_StrEqual(const char *str,
		    const char *cmpstr,
		    int n, const double d[], 
		    Boolean is_wildcard)
{
  int rc = 0; /* by default not-equal */
  DRHOOK_START(ODB_Common_StrEqual);
  if (cmpstr) {
    n = 0;
    if (str) {
      /* A simple match ? */
      if (strcaseequ(str, cmpstr) || 
	  StrCaseStr(str, cmpstr, true)) /* from odb/lib/evaluate.c */
	rc = 1;
    }
  }
  if (rc == 0 && str && ((n > 0 && d) || cmpstr)) {
    int status = 0;
    REGEX_DEF(p);
    char *ref_str = STRDUP(str);
    char *pref = ref_str;
    int ref_len = STRLEN(ref_str);
    /* Skip over the initial blanks, unless the user supplied reference string
       has explicit got blanks or we have wildcard comparison in concern */
    Boolean skip_blanks_at_start = 
      (is_wildcard || (ref_len > 0 && *ref_str == ' ')) ? false : true;
    /* Ignore the trailing blanks, unless we are in wildcard comparison */
    Boolean skip_blanks_at_end = 
      is_wildcard ? false : true;
    char *cmp_str = cmpstr ? STRDUP(cmpstr) : NULL;
    
    if (skip_blanks_at_start) {
      while (*pref == ' ') ++pref;
      ref_len = STRLEN(pref);
    }
    
    if (skip_blanks_at_end) {
      char *last = (ref_len > 0) ? pref + ref_len - 1 : NULL;
      if (last && *last == ' ') {
	do {
	  *last-- = '\0';
	} while (last >= pref && *last == ' ');
      }
    }

    if (is_wildcard) {
      char *rex = STRDUP(pref);
      REGCMP(p,rex);
      FREE(rex);
      if (status != 0) {
	rc = 0;
	goto quick_exit;
      }
    }

    if (!cmpstr) {
      int j, len_cmp_str = n * sizeof(double) + 1;
      ALLOC(cmp_str, len_cmp_str);
      *cmp_str = '\0'; /* to allow strcat() */
    
      for (j=0; j<n; j++) {
	S2D_Union u;
	char *s = u.str;
	u.dval = d[j];
	u.str[sizeof(double)] = '\0';
	if (j == 0 && !is_wildcard) {
	  if (skip_blanks_at_start) {
	    while (*s == ' ') ++s;
	  }
	  if (!strncaseequ(pref,s,1)) {
	    /* Quick exit : when the first char doesn't match */
	    rc = 0;
	    goto quick_exit;
	  }
	}
	strcat(cmp_str, s);
      } /* for (j=0; j<n; j++) */
    }

    if (skip_blanks_at_end) {
      int len = STRLEN(cmp_str);
      char *last = (len > 0) ? cmp_str + len - 1 : NULL;
      if (last && *last == ' ') {
	do {
	  *last-- = '\0';
	} while (last >= cmp_str && *last == ' ');
      }
    }

    /* The return value */
    if (is_wildcard) {
      REGEX(p, cmp_str);
      rc = (status == 0) ? 1 : 0;
    }
    else {
      rc = strcaseequ(pref, cmp_str);
    }

  quick_exit:
    if (is_wildcard) REGFREE(p);
    FREE(cmp_str);
    FREE(ref_str);
  }
  DRHOOK_END(0);
  return rc;
}


PUBLIC int
ODB_StrEqual(int n, const char *str, ...)
{
  int rc = 0; /* by default not-equal */
  if (--n > 0 && str) {
    int j;
    double *d = NULL;
    va_list ap;
    va_start(ap, str);
    ALLOC(d,n);
    for (j=0; j<n; j++) {
      d[j] = va_arg(ap, double);
    }
    va_end(ap);
    rc = ODB_Common_StrEqual(str, NULL, n, d, false);
    FREE(d);
  }
  return rc;
}


PUBLIC int
ODB_WildCard(int n, const char *str, ...)
{
  int rc = 0; /* by default not-equal */
  if (--n > 0 && str) {
    int j;
    double *d = NULL;
    va_list ap;
    va_start(ap, str);
    ALLOC(d,n);
    for (j=0; j<n; j++) {
      d[j] = va_arg(ap, double);
    }
    va_end(ap);
    rc = ODB_Common_StrEqual(str, NULL, n, d, true);
    FREE(d);
  }
  return rc;
}


PUBLIC double
ODBstrequal(const int Nd, const double d[])
{
  double rc = 0; /* by default not-equal */
  int n = Nd;
  if (n-- > 1 && d) {
    /* Note: d[0] (i.e. *d) is interpreted as an address to a character string (saddr) */
    char *str = NULL;
    S2D_Union u;
    u.dval = *d++;
    str = u.saddr;
    rc = ODB_Common_StrEqual(str, NULL, n, d, false);
  }
  return rc;
}


PUBLIC double
ODBwildcard(const int Nd, const double d[])
{
  double rc = 0; /* by default not-equal */
  int n = Nd;
  if (n-- > 1 && d) {
    /* Note: d[0] (i.e. *d) is interpreted as an address to a character string (saddr) */
    char *str = NULL;
    S2D_Union u;
    u.dval = *d++;
    str = u.saddr;
    rc = ODB_Common_StrEqual(str, NULL, n, d, true);
  }
  return rc;
}
