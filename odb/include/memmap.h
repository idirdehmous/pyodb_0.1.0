#ifndef _MEMMAP_H_
#define _MEMMAP_H_

/* memmap.h */

/* Memory mapped I/O */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifndef HAS_MMAP

#if (defined(LINUX) && !defined(CRAYXT)) || defined(RS6K) || defined(SUN4) || defined(ALPHA) || defined(HPPA) || defined(SGI)
#define HAS_MMAP
#endif

#endif

#ifdef HAS_MMAP

#if defined(SUN4) && defined(PRIVATE)
/* Sun/Solaris #define's PRIVATE, which we also do in privpub.h ;-( */
#define PRIVATE_RYAN PRIVATE
#undef PRIVATE
#endif

#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#endif

typedef struct {
  caddr_t buf;
  size_t len;
  off_t offset;
} memmap_t;

extern memmap_t *memmap_open_read(int fd, const off_t *Offset, const size_t *Len);
extern int memmap_close(memmap_t *m);
extern memmap_t *memmap_alloc(size_t Len, int Shared);

#if defined(SUN4) && defined(PRIVATE)
/* Get rid of PRIVATE #define'd in <sys/mman.h> */
#undef PRIVATE
#endif

#if defined(SUN4) && defined(PRIVATE_RYAN)
/* Restore our original PRIVATE (if it was already defined) */
#define PRIVATE PRIVATE_RYAN
#undef PRIVATE_RYAN
#endif

#endif
