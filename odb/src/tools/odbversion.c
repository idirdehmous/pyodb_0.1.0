
/*
  odbversion         --> returns "CY30R2.076" --> should become the $ODB_VERSION_ID
  odbversion -minor  --> "2.076"
  odbversion -major  --> "30"
  otherwise prints error message to stderr and exit-code == 1
*/

#include "odb.h"
#include "magicwords.h"

int main(int argc, char *argv[])
{
  int rc = 0;
  char *a_out = argv[0];
  double major, minor;
  int numeric;
  const char *str = codb_versions_(&major, &minor, &numeric, NULL);
  --argc;
  if (argc == 0) printf("%s\n",str);
  else if (argc == 1 && strequ(argv[1],"-major")) {
    printf("%.0f\n",major);
  }
  else if (argc == 1 && strequ(argv[1],"-minor")) {
    printf("%.3f\n",minor);
  }
  else if (argc == 1 && strequ(argv[1],"-numeric")) {
    printf("%d\n",numeric);
  }
  else {
    fprintf(stderr,"Usage: %s\n",a_out);
    fprintf(stderr,"       %s -major\n",a_out);
    fprintf(stderr,"       %s -minor\n",a_out);
    fprintf(stderr,"       %s -numeric\n",a_out);
    rc++;
  }
  return(rc);
}

