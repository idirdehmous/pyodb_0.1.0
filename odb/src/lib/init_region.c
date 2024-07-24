
/* Initializes region with a predefined 4-byte string */

#ifdef RS6K
#pragma options noextchk
#endif

#include "odb.h"

static boolean Init_Region = 0;

void 
init_region_(int array[], 
	     const int *nbytes, 
	     const int *the_magic_4bytes)
{
  static boolean first_time = 1; 

  if (first_time) {
    char *p = getenv("ODB_INIT_REGION");
    Init_Region = p ? atoi(p) : 0;
    Init_Region = (Init_Region > 0) ? 1 : 0;
    if (Init_Region) {
      int Myproc;
      codb_procdata_(&Myproc, NULL, NULL, NULL, NULL);
      if (Myproc == 1) 
	fprintf(stderr,
	"***init_region_(): Return matrices will be initialized with ODB_UNDEF/UNDEFDB in ODB_GET()\n");
    }
    first_time = 0;
  }

  if (Init_Region) {
    int Nwords = (*nbytes)/4; /* truncated down to the nearest 4-byte bdry */
    int Magics = *the_magic_4bytes;
    int j;
    for (j=0; j<Nwords; j++) array[j] = Magics;
  }
}

