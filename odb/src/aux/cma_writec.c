#include "cmaio.h"

FORTRAN_CALL void
cma_writec_(const integer4 *unit,
	    const byte1     s[],
	    integer4       *retcode
	    /* Hidden arguments */
	    , int slen)
{
  cma_writeb_(unit, s, &slen, retcode);
}
