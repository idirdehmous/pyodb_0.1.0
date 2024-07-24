#include "odb.h"

/* 
   A F90 TRANSFER-function substitution for SUN4, since
   character <=> integer-array transfer didn't work.

   Called from codb_distrubute (... _str())

*/

void 
ctransfer_(const int *mode,
	   const int *mult,
	   char s[],
	   int x[],
	   /* Hidden arguments */
	   const int s_len)
{
  if (*mode == 1) { /* Character to integer */
    memcpy(x, s, (*mult) * s_len * sizeof(*s));
  }
  else { /* Integer to character */
    memcpy(s, x, (*mult) * s_len * sizeof(*s));
  }
}
