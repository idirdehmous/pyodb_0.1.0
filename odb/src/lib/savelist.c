#include "odb.h"

/* savelist.c */

/* === Note: Not at all optimized ; to be done */

/* 
   Checks whether particular column is in the savelist
   (i.e. ODB_SAVELIST or ODB_SAVELIST_<dbname>)
   and will make sure ODB_close() updates-to-disk *ONLY*
   such columns which are in the list.
   Other columns remain unchanged, even if their values
   have been changed during the course of program execution.

   Examples: 

   1) save physically to disk only "an_depar@body" for any database:

   export ODB_SAVELIST="an_depar@body"

   2) save physically to disk only "an_depar@body,fg_depar@body" for database CCMA:

   export ODB_SAVELIST_CCMA="an_depar@body,fg_depar@body"

*/


static int
SearchEnv(const char *env, const char *name)
{
  int rc = 0; /* Since we are indeed calling this utility, env-variable has a value */
  int name_len = strlen(name);
  const char *pos = env;
  const char *last = env + strlen(env);
  for (;;) {
    const char *loc = strstr(pos,name);
    if (loc) {
      const char delims[] = " ,/|\t";
      char lo = (loc == pos) ? 0 : loc[-1];
      char hi = (loc + name_len < last) ? loc[name_len] : 0;
      if (
	  (lo == 0 || strchr(delims,lo)) &&
	  ((lo == hi) || (hi == 0 || strchr(delims,hi)))
	 ) {
	rc = 1; /* found !! */
	break;
      }
      pos = loc + name_len;
      if (pos >= last) break; /* cannot be found */
    }
    else {
      break; /* no chance to find */
    } /* if (loc) else ... */
  } /* for (;;) */
  return rc;
}


int 
ODB_savelist(const char *dbname, /* NULL or f.ex. "ECMA" */
	     const char *name)   /* Like "colname@tablename" */
{
  /* 
     Returns 0, if :
     (ODB_SAVELIST_<dbname> OR ODB_SAVELIST) is defined AND
     "colname@tablename" is *NOT* found in the environment variable;
     consequently forces the use of saved_data[]

     Returns 1, if :
     (ODB_SAVELIST_<dbname> OR ODB_SAVELIST) is defined AND 
     "colname@tablename" *IS* defined within the environment variable;
     forces the use of updated data (d[] or pd[])

     Returns -1, if : (the default)
     (ODB_SAVELIST_<dbname> OR ODB_SAVELIST) is NOT defined;
     forces the use of updated data (d[] or pd[])
  */

  int rc = -1;
  int done = 0;

  if (!name) goto finish;

  if (dbname) {
    int envname_len = strlen("ODB_SAVELIST") + 1 + strlen(dbname) + 1;
    char *envname;
    ALLOC(envname,envname_len);
    snprintf(envname,envname_len,"ODB_SAVELIST_%s",dbname);
    {
      char *env = getenv(envname);
      if (env) {
	rc = SearchEnv(env,name);
	done = 1;
      }
    }
    FREE(envname);
  } /* if (dbname) */

  if (!done) {
    static int first_time = 1; /* not thread safe */
    static char *env = NULL;
    if (first_time) {
      env = getenv("ODB_SAVELIST");
      first_time = 0;
    }
    if (env) {
      rc = SearchEnv(env,name);
      done = 2;
    }
  } /* if (!done) */

 finish:
  /* fprintf(stderr,"ODB_savelist(%s, %s): done=%d, rc=%d\n",dbname,name,done,rc); */
  return rc;
}
