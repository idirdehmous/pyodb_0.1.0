#include "odb.h"

#define DELIM "|"

PRIVATE char *list = NULL;

PUBLIC void
destroy_alist()
{
  FREE(list);
}


PUBLIC char *
init_alist(const char *p)
{
  destroy_alist();
  if (p) {
    ALLOC(list, strlen(p) + 3);
    sprintf(list,"%s%s%s",DELIM,p,DELIM);
  }
  else {
    ALLOC(list,2);
    strcpy(list,DELIM);
  }
  return list;
}


PUBLIC boolean 
in_alist(const char *p)
{
  boolean found = 0;
  if (list && p) {
    char *token;
    ALLOC(token, strlen(p) + 3);
    sprintf(token,"%s%s%s",DELIM,p,DELIM);
    found = (strstr(list, token) != NULL);
    FREE(token);
  }
  return found;
}

PUBLIC char *
add_alist(const char *p)
{
  if (!list) {
    list = init_alist(p);
  }
  else {
    int lenp = p ? strlen(p) : 0;
    if (p && lenp > 0) {
      int lenlist = strlen(list);
      REALLOC(list, lenlist + lenp + 2);
      strcat(list, p);
      strcat(list, DELIM);
    }
  }
  return list;
}
