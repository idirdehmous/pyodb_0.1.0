#include "cmaio.h"

FORTRAN_CALL void
cma_readc_(const integer4 *unit,
	   byte1           s[],
	   integer4       *retcode
	   /* Hidden arguments */
	   , int slen)
{
  memset(s, ' ', slen);
  cma_readb_(unit, s, &slen, retcode);
}
