/* odbfiletime.c */

/* 
   Usage: echo "0.0" | odbfiletime.x files_or_directories
                       odbfiletime.x files_or_directories < /dev/null

   Prints a decimal number "<ull>.<count>" to the stdout,
   where <ull> is an unsigned long long int (64-bit) of the sum of the valid files' modications times
         <count> is the count of valid files

   In particular prints "0.0", if no valid files are found.

   Author: Sami Saarinen, ECMWF, 28-Dec-2007
*/

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

int
main(int argc, char *argv[])
{
  unsigned long long int u = 0;
  int cnt = 0;
  int j;
  { /* Initial values, if any */
    int nelem = fscanf(stdin,"%llu.%d",&u,&cnt);
    if (nelem != 2) { u = 0; cnt = 0; }
  }
  for (j=1; j<argc; ++j) {
    char *file = argv[j];
    struct stat buf;
    if (stat(file,&buf) == 0) {
      time_t mtime = buf.st_mtime;
#if 0
      char s[80];
      strftime(s, sizeof(s), " %d-%b-%Y  %H:%M:%S ", localtime(&mtime));
      fprintf(stderr, "%s --> %lld i.e. %s\n",file,(long long int)mtime,s);
#endif
      u += mtime;
      ++cnt;
    }
    else {
#if 0
      fprintf(stderr,"%s --> ???\n",file);
#endif
    }
  }
  printf("%llu.%d\n",u,cnt);
  return 0;
}
