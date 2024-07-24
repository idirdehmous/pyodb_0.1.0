
/* infile.c */

/* A very slow implementation for the moment */

#include "odb.h"

#define GET_NEXT(d) (fscanf(fp, "%lf", &d) == 1)

PUBLIC double
ODB_InFile(const char *filename, double target)
{
  /* Search from file/pipe and stop immediately when matched */
  Boolean found = 0;
  Boolean is_pipe = (filename && *filename == '|');
  FILE *fp = is_pipe ? popen(++filename,"r") : fopen(filename,"r");
  if (fp) {
    double d;
    while (GET_NEXT(d)) {
      if (Func_uintequal(d, target)) {
	found = 1;
	break;
      }
    }
    is_pipe ? pclose(fp) : fclose(fp);
  }
  return (double)found;
}

PUBLIC double
ODBinfile(double filename, double target)
{
  /* Note: filename is interpreted as an address to a character string (saddr) */
  S2D_Union u;
  u.dval = filename;
  return ODB_InFile(u.saddr, target);
}


PUBLIC double
ODB_NotInFile(const char *filename, double target)
{
  /* Search from file/pipe and stop immediately when matched */
  Boolean notfound = 1;
  Boolean is_pipe = (filename && *filename == '|');
  FILE *fp = is_pipe ? popen(++filename,"r") : fopen(filename,"r");
  if (fp) {
    double d;
    while (GET_NEXT(d)) {
      if (Func_uintequal(d, target)) {
	notfound = 0;
	break;
      }
    }
    is_pipe ? pclose(fp) : fclose(fp);
  }
  return (double)notfound;
}


PUBLIC double
ODBnotinfile(double filename, double target)
{
  /* Note: filename is interpreted as an address to a character string (saddr) */
  S2D_Union u;
  u.dval = filename;
  return ODB_NotInFile(u.saddr, target);
}
