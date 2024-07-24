/* copyfile.c */

/* copies file into a file-stream pointed by fpout */

#include "defs.h"
#undef MAXLINE
#include "iostuff.h"

PRIVATE void
shared_code(int *rc,
	    FILE *fpout, FILE *fp,
	    const char *start_mark, const char *end_mark,
	    Boolean backslash_dblquote,
	    Boolean keep_backslash)
{
  int c;
  int newline = 1;
  int start_mark_printed = 0;
  int end_mark_printed = 0;
  
  while ((c = fgetc(fp)) != EOF) {
    if (keep_backslash || c != '\\') {
      (*rc)++;
      if (newline && start_mark) {
	fprintf(fpout,"%s", start_mark);
	start_mark_printed = 1;
	newline = 0;
	end_mark_printed = 0;
      }
      newline = (c == '\n');
      if (newline && end_mark) {
	fprintf(fpout,"%s", end_mark);
	end_mark_printed = 1;
      }
      if (c == '"' &&  backslash_dblquote) fprintf(fpout,"\\");
      fprintf(fpout,"%c", c);
    } /* if (c != '\\') */
  } /* while ((c = fgetc(fp)) != EOF) */
  
  if (start_mark_printed && !end_mark_printed && end_mark) {
    /* In case file does not end with newline */
    fprintf(fpout,"%s", end_mark);
  }
}

int 
ODB_copyfile(FILE *fpout, const char *file,
	     const char *start_mark, const char *end_mark,
	     Boolean backslash_dblquote,
	     Boolean keep_backslash)
{
  int rc = 0;
  if (fpout && file) {
    FILE *fp = FOPEN(file, "r");
    if (fp) {
      shared_code(&rc,
		  fpout, fp,
		  start_mark, end_mark,
		  backslash_dblquote,
		  keep_backslash);
      FCLOSE(fp);
    }
    else {
      PERROR(file);
      rc = -1;
    }
  }
  return rc;
}

#define EGREP "egrep"

int 
ODB_grepfile(FILE *fpout, const char *pattern, const char *file,
	     const char *start_mark, const char *end_mark,
	     Boolean backslash_dblquote,
	     Boolean keep_backslash)
{
  int rc = 0;
  char *truefile = (fpout && file && pattern) ? IOtruename(file,NULL) : NULL;
  if (truefile) {
    FILE *fp = NULL;
    char *cmd = NULL;
    char *env = getenv("ODB_EGREP"); /* Just in case 'egrep' was not found from the PATH */
    char *egrep = env ? STRDUP(env) : STRDUP(EGREP);
    ALLOC(cmd, strlen(egrep) + strlen(pattern) + strlen(truefile) + 10);
    sprintf(cmd,"%s '%s' %s", egrep, pattern, truefile);
    fp = popen(cmd, "r");
    if (fp) {
      shared_code(&rc,
		  fpout, fp,
		  start_mark, end_mark,
		  backslash_dblquote,
		  keep_backslash);
      pclose(fp);
    }
    else {
      PERROR(cmd);
      rc = -1;
    }
    FREE(cmd);
    FREE(egrep);
    FREE(truefile);
  }
  return rc;
}
