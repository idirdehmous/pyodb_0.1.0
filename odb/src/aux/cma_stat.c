#include "cmaio.h"

FORTRAN_CALL void 
cma_stat_(const integer4 *ftn_unit,
	  const integer4 *toggle, 
	        integer4 *old_value)
{
  int cache = *toggle;
  *old_value = iostuff_stat;
  iostuff_stat = (cache > 0) ? true : false;
  iostuff_stat_ftn_unit = *ftn_unit;
}
