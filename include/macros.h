#define SET_PATH(x,X,subdir) \
{ \
  char *add_subdir_if_not_defined = subdir; \
  char *x = getenv("ODB_" #X); \
  int len; \
  char *xdb = NULL; \
  len = strlen("ODB_" #X) + 1 + strlen(p_dbname) + 1; \
  ALLOC(xdb, len); \
  snprintf(xdb,len,"ODB_" #X "_%s",p_dbname); \
  env = getenv(xdb); \
  FREE(xdb); \
  if (!env) env = x; \
  if (env) { \
    xdb = STRDUP(env); \
    add_subdir_if_not_defined = NULL; \
  } \
  else if (!x##_dbname) { \
    char curpath[4096]; \
    xdb = STRDUP(getcwd(curpath, sizeof(curpath))); \
  } \
  if (xdb) { FREE(x##_dbname); x##_dbname = STRDUP(xdb); } \
  FREE(xdb); \
  len = strlen("ODB_" #X) + 1 + strlen(p_dbname) + 1 + strlen(x##_dbname) + 1; \
  if (add_subdir_if_not_defined) len += strlen(add_subdir_if_not_defined); \
  if (add_subdir_if_not_defined) len += strlen(add_subdir_if_not_defined); \
  ALLOC(env, len); /* Remains allocated ; cannot be free'd */ \
  snprintf(env,len,"ODB_" #X "_%s=%s%s",p_dbname,x##_dbname,\
           add_subdir_if_not_defined?add_subdir_if_not_defined:""); \
  putenv(env); \
  { \
    char *penv = STRDUP(env); \
    char *eq = strchr(penv,'='); \
    if (eq) { \
      *eq = '\0'; \
      env = getenv(penv); \
      fprintf(stderr,"%s=%s\n",penv,env?env:NIL); \
      if (env) { FREE(x##_dbname); x##_dbname = STRDUP(env); } \
    } \
    FREE(penv); \
  } \
}

