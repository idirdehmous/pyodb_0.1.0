#ifndef _ECSTDLIB_H_
#define _ECSTDLIB_H_

#include <stdlib.h>

#ifdef CRAYXT
/* Cray XT3/XT4 with catamount microkernel */
#define system(cmd) (-1)
#endif

#endif /* _ECSTDLIB_H_ */
