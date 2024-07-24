#include "defs.h"

static const char DELIM = '|';

PRIVATE char *list = NULL;

PUBLIC char *
get_list()
{
  return list;
}

PUBLIC char
get_list_delim()
{
  return DELIM;
}

PUBLIC int
get_list_elemcount()
{
  int cnt = -1; /* "-1" since there is one less elements in the list than delimiters */
  const char *p = list;
  if (p) {
    while (*p) {
      if (*p == DELIM) cnt++;
      p++;
    }
  }
  return (cnt >= 0) ? cnt : 0;
}

PUBLIC void
destroy_list()
{
  FREE(list);
}


PUBLIC char *
init_list(const char *p)
{
  destroy_list();
  if (p) {
    ALLOC(list, strlen(p) + 3);
    sprintf(list,"%c%s%c",DELIM,p,DELIM);
  }
  else {
    ALLOC(list,2);
    list[0] = DELIM;
    list[1] = '\0';
  }
  return list;
}


PUBLIC Boolean 
in_list(const char *p)
{
  Boolean found = 0;
  if (list && p) {
    char *token;
    ALLOC(token, strlen(p) + 3);
    sprintf(token,"%c%s%c",DELIM,p,DELIM);
    found = (strstr(list, token) != NULL);
    FREE(token);
  }
  return found;
}


PUBLIC Boolean 
in_mylist(const char *p, const char *mylist, char delim)
{
  Boolean found = 0;
  if (mylist && p && delim) {
    char *token;
    ALLOC(token, strlen(p) + 3);
    sprintf(token,"%c%s%c",delim,p,delim);
    found = (strstr(mylist, token) != NULL);
    FREE(token);
  }
  return found;
}


PUBLIC Boolean 
in_extlist(const char *p, const char *extlist)
{
  Boolean found = 0;
  if (extlist && p) {
    char *token;
    ALLOC(token, strlen(p) + 3);
    sprintf(token,"%c%s%c",DELIM,p,DELIM);
    found = (strstr(extlist, token) != NULL);
    FREE(token);
  }
  return found;
}


PUBLIC char *
in_extlist1(const char *p, const char *extlist)
{
  char *found = NULL;
  if (extlist && p) {
    char *token;
    char *first;
    ALLOC(token, strlen(p) + 2);
    sprintf(token,"%s%c",p,DELIM);
    first = token;
    found = strstr(extlist, token);
    if (found) {
      char *a;
      char *f_start = found;
      char *f_end;
      while (*--f_start != DELIM);
      f_start++;
      a = f_end = STRDUP(f_start);
      while (*f_end != *first) f_end++;
      *f_end = '\0';
      found = STRDUP(a); /* master name directly */
      FREE(a);
    }
    FREE(token);
  }
  return found;
}


PUBLIC char *
add_list(const char *p)
{
  if (!list) {
    list = init_list(p);
  }
  else {
    int lenp = STRLEN(p);
    if (p && lenp > 0) {
      char tmp[2];
      int lenlist = strlen(list);
      REALLOC(list, lenlist + lenp + 2);
      strcat(list, p);
      tmp[0] = DELIM; tmp[1] = '\0';
      strcat(list, tmp);
    }
  }
  return list;
}


PUBLIC ODB_linklist *
manage_linklist(int function,
		const char *lhs, const char *rhs, int type)
{
  static ODB_linklist *linklist = NULL;
  ODB_linklist *retlist = NULL;

  switch (function) {
  case FUNC_LINKLIST_ADD:
    if (lhs && rhs) {
      ODB_linklist *plinklist;
      if (!linklist) CALLOC(linklist,1); /* Start constructing it now */
      plinklist = linklist;
      while (plinklist) {
	if (plinklist->n_rhs == 0 || 
	    (plinklist->type == type && strequ(plinklist->lhs,lhs))) {
	  if (plinklist->n_rhs == 0) { /* An empty slot */
	    plinklist->type = type;
	    plinklist->lhs = STRDUP(lhs);
	    ALLOC(plinklist->rhs,1);
	    plinklist->last_rhs = plinklist->rhs;
	  }
	  else { /* plinklist->type == type && strequ(plinklist->lhs,lhs) */
	    ALLOC(plinklist->last_rhs->next,1);
	    plinklist->last_rhs = plinklist->last_rhs->next;
	  }
	  plinklist->last_rhs->name = STRDUP(rhs);
	  plinklist->last_rhs->next = NULL;
	  plinklist->n_rhs++;
	  retlist = plinklist;
	  break;
	}
	if (!plinklist->next) CALLOC(plinklist->next,1);
	plinklist = plinklist->next;
      } /* while (plinklist) */
    } /* if (lhs && rhs) */
    break;

  case FUNC_LINKLIST_START:
    retlist = linklist;
    break;

  case FUNC_LINKLIST_QUERY:
    if (lhs && rhs) {
      ODB_linklist *plinklist = linklist;
      while (plinklist) {
	if (plinklist->n_rhs > 0 && 
	    plinklist->type == type && strequ(plinklist->lhs,lhs)) {
	  /* Loop over all rhs-(table)names */
	  struct _rhs_t *first_rhs = plinklist->rhs;
	  while (first_rhs) {
	    if (strequ(first_rhs->name,rhs)) {
	      retlist = plinklist;
	      break;
	    }
	    first_rhs = first_rhs->next;
	  } /* while (first_rhs) */
	  break;
	}
	plinklist = plinklist->next;
      } /* while (plinklist) */
    } /* if (lhs && rhs) */
    break;

  } /* switch (function) */

  return retlist;
}
