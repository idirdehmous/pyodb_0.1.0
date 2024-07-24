#include "cmaio.h"

FORTRAN_CALL void 
cma_debug_(const integer4 *toggle, 
	         integer4 *old_value)
{
  int cache = *toggle;
  *old_value = iostuff_debug;
  iostuff_debug = (cache > 0) ? true : false;
}
