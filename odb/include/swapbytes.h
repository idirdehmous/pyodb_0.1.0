/* swapbytes.h */

#ifndef _SWAPBYTES_H_
#define _SWAPBYTES_H_

#include "privpub.h"

/* Swap bytes : Thanks to Linux-file "/usr/include/bits/byteswap.h" for cleaness ;-) */

/* Swap bytes in 8 bit value == does nothing; for convenience  */
#define bswap8bits(x) (x)

/* Swap bytes in 16 bit value.  */
#define bswap16bits(x) \
     (  (((x) >> 8) & 0xff) \
      | (((x) & 0xff) << 8) )

/* Swap bytes in 32 bit value.  */
#define bswap32bits(x) \
     (  (((x) & 0xff000000) >> 24) \
      | (((x) & 0x00ff0000) >>  8) \
      | (((x) & 0x0000ff00) <<  8) \
      | (((x) & 0x000000ff) << 24) )

/* Swap bytes in 64 bit value.  */
#define bswap64bits(x) \
     (  (((x) & 0xff00000000000000ull) >> 56) \
      | (((x) & 0x00ff000000000000ull) >> 40) \
      | (((x) & 0x0000ff0000000000ull) >> 24) \
      | (((x) & 0x000000ff00000000ull) >>  8) \
      | (((x) & 0x00000000ff000000ull) <<  8) \
      | (((x) & 0x0000000000ff0000ull) << 24) \
      | (((x) & 0x000000000000ff00ull) << 40) \
      | (((x) & 0x00000000000000ffull) << 56) )

extern void swap1bytes_
            (void *v, const int *vlen); /* 8-bit/1-byte word swap : does nothing; for convenience */

extern void swap2bytes_
            (void *v, const int *vlen); /* 16-bit/2-byte word swap */

extern void swap4bytes_
            (void *v, const int *vlen); /* 32-bit/4-byte word swap */

extern void swap8bytes_
            (void *v, const int *vlen); /* 64-bit/8-byte word swap */

extern void
swapANYbytes(void *v, int elsize, int n, unsigned int dtype, 
	     const char *dtype_name, const char *varname);

#endif /* _SWAPBYTES_H_ */
