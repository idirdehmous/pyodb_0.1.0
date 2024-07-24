
#include "alloc.h"

#include <sys/types.h>
#include <sys/stat.h>

extern char *IOtruename(const char *name, const int *len_str);

int
file_exist_(const char *s)
{
  int exist = 0;
  if (s) {
    struct stat buf;
    int len = strlen(s);
    char *file = IOtruename(s,&len);
    exist = (stat(file, &buf) == -1) ? 0 : 1;
    FREE(file);
  } /* if (s) */
  return exist;
}

int
is_regular_file_(const char *s)
{
  int exist = 0;
  if (s) {
    struct stat buf;
    int len = strlen(s);
    char *file = IOtruename(s,&len);
    exist = (stat(file, &buf) == -1) ? 0 : 1;
    if (exist) exist = S_ISREG(buf.st_mode);
    FREE(file);
  } /* if (s) */
  return exist;
}

int
is_directory_(const char *s)
{
  int exist = 0;
  if (s) {
    struct stat buf;
    int len = strlen(s);
    char *file = IOtruename(s,&len);
    exist = (stat(file, &buf) == -1) ? 0 : 1;
    if (exist) exist = S_ISDIR(buf.st_mode);
    FREE(file);
  } /* if (s) */
  return exist;
}

int
rename_file_(const char *in, const char *out)
{
  int rc = 0;
  if (in && out) {
    int exist_infile = file_exist_(in);
    if (exist_infile) {
      int exist_outfile = file_exist_(out);
      int lenin = strlen(in);
      char *in_resolved = IOtruename(in,&lenin);
      int stalin = strlen(out);
      char *out_resolved = IOtruename(out,&stalin);
      if (exist_outfile) {
	if (remove(out_resolved) != 0) {
	  /* PERROR(out_resolved); */
	  rc = 2;
	}
      } /* if (exist_outfile) */
      if (rc == 0 && rename(in_resolved, out_resolved) != 0) {
	/*
	PERROR("rename_file: rename()");
	fprintf(stderr,"***Warning: Unable to rename '%s' to '%s'\n",
		in_resolved, out_resolved);
	*/
	rc = 3;
      } /* if (rename(in_resolved, out_resolved) != 0) */
      FREE(in_resolved);
      FREE(out_resolved);
    }
    else {
      /* fprintf(stderr,"***Warning: Unable to locate (in)file '%s'\n",in); */
      rc = 1;
    } /* if (exist_infile) */
  }
  return rc;
}

int
remove_file_(const char *filename)
{
  int rc = 0;
  if (filename) {
    int len = strlen(filename);
    char *p = IOtruename(filename,&len); 
    rc = remove(p);
    FREE(p);
  }
  return rc;
}
