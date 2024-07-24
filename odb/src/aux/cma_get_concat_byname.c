#include "cmaio.h"

void 
cma_get_concat_byname_(const char *filename,
		       integer4 *retcode,
		       /* Hidden arguments to Fortran-program */
		       int len_filename)
{
  *retcode = IOconcat_byname(filename, len_filename);
}

		    
