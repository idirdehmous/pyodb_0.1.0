#include "defs.h"

extern Boolean verbose;

typedef struct _Defines {
  char *s;
  double value;
  struct _Defines *next;
} Defines_t;

static Defines_t *first = NULL;
static Defines_t *last  = NULL;

void
ODB_put_define(const char *s, double default_value)
{
  if (s) {
    char *x;
    Defines_t *p = NULL;
    
    ALLOC(p, 1);
    if (first) last->next = p;
    else       first = p;
    last = p;
    
    while (isspace(*s)) s++;
    p->s = STRDUP(s);

    x = strchr(p->s,'=');
    if (x && default_value == 0) {
      /* for use by -Ukey or #undef key */
      *x = '\0';
      x = NULL;
    }
    if (x) {
      *x = '\0';
      x++;
      p->value = atof(x);
    }
    else {
      x = p->s;
      while (isspace(*x)) x++;
      while (*x) {
	if (isspace(*x)) {
	  *x = '\0';
	  break;
	}
	x++;
      }
      p->value = default_value;
    }
    p->next = NULL;
    /* if (verbose) fprintf(stderr,"\t-D\"%s=%g\"\n",p->s,p->value); */
  }
}

double
ODB_get_define(const char *s)
{
  if (s) {
    char *ws;
    char *x = STRDUP(s);
    char *tmp = x;
    ws = strchr(x,'\n');
    if (ws) *ws = '\0';
    /* if (verbose) fprintf(stderr,"ODB_get_define(\"%s\");\n",x); */

    while (isspace(*x)) x++;
    if (strnequ(x,"#ifdef",6)) x += 6;
    else if (strnequ(x,"#ifndef",7)) x += 7;
    while (isspace(*x)) x++;

    ws = x;
    while ( *ws ) {
      if (isspace(*ws)) {
	*ws = '\0';
	break;
      }
      ws++;
    }
    /* if (verbose) fprintf(stderr,"\tComparing '%s' ...\n",x); */
    {
      Defines_t *p;
      for (p = first; p; p = p->next) {
	if (strequ(p->s,x)) {
	  /* if (verbose) fprintf(stderr,"\t==> value for '%s' = %g\n",p->s,p->value); */
          FREE(tmp);
	  return p->value;
	}
      }
    }
    FREE(tmp);
  }
  return 0;
}

Boolean
ODB_has_define(const char *s)
{
  if (s) {
    Defines_t *p;
    for (p = first; p; p = p->next) {
      if (strequ(p->s,s)) {
        return 1;
      }
    }
  }
  return 0;
}
