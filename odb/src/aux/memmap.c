
/* memmap.c */

/* mmap()'ed I/O : Activate by compiling with -DHAS_MMAP (unless LINUX or RS6K) */

#include "memmap.h"
#include "alloc.h"

memmap_t *
memmap_open_read(int fd, const off_t *Offset, const size_t *Len)
{
  memmap_t *m = NULL;
#ifdef HAS_MMAP
  if (fd >= 0) { /* ok */
    struct stat st;
    int ok = fstat(fd, &st);
    if (ok == 0 && (   S_ISREG(st.st_mode) /* Accept regular files */
#if defined(S_ISLNK)
		    || S_ISLNK(st.st_mode) /* Accept symbolic links (Not in POSIX.1-1996.) */
#endif
	)) { 
      caddr_t buf;
      off_t offset = (Offset && *Offset >= 0) ? *Offset : 0;
      size_t len = st.st_size - offset;
      if (Len && *Len >= 0) len = MIN(len, *Len);
      buf = mmap(NULL, len, 
		 PROT_READ, MAP_SHARED,
		 fd, offset);
      if (buf && buf != MAP_FAILED) { /* ok */
	ALLOC(m, 1);
	m->buf = buf;
	m->len = len;
	m->offset = offset;
      }
    } /* if (ok == 0) */
  }
#endif
  return m;
}

int
memmap_close(memmap_t *m)
{
  int rc = 0;
#ifdef HAS_MMAP
  if (m && m->buf) {
    rc = munmap(m->buf, m->len);
    FREE(m);
  }
#endif
  return rc;
}

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef HAS_MMAP
#if defined(MAP_ANONYMOUS) && !defined(MAP_ANON)
#define MAP_ANON MAP_ANONYMOUS
#endif
#endif

memmap_t *
memmap_alloc(size_t Len, int Shared)
{
  memmap_t *m = NULL;
#ifdef HAS_MMAP
#if defined(MAP_ANON)
  int fd = -1;
  int flags = Shared ? (MAP_ANON | MAP_SHARED) : (MAP_ANON | MAP_PRIVATE);
#else
  int fd = open("/dev/zero", O_RDWR);
  int flags = Shared ? MAP_SHARED : MAP_PRIVATE;
#endif
  off_t offset = 0;
  size_t len = (Len > 0) ? Len : 0;
  if (len > 0) {
    caddr_t buf = mmap(NULL, len,
		       PROT_READ | PROT_WRITE,
		       flags,
		       fd, offset);
    if (buf && buf != MAP_FAILED) { /* ok */
      ALLOC(m, 1);
      m->buf = buf;
      m->len = len;
      m->offset = offset;
    }
  }
#if !defined(MAP_ANON)
  close(fd); /* Is this *really* ok ? */
#endif
#endif
  return m;
}
