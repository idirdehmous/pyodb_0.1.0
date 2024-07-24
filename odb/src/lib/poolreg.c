/* poolreg.c */

/* Provides cached/hashed access to (handle,poolno)-pair */

/* Note : Similar to forfunc.c, but independent of "it" -- thread id */

#include "odb.h"

#ifndef POOLREG_HASHSIZE
#define POOLREG_HASHSIZE 1021U
#endif

PRIVATE uint POOLREG_hashsize = 0;

typedef struct _POOLREG_cache {
  int handle;
  int poolno;
  ODB_Pool *pool;
  struct _POOLREG_cache *collision;
} POOLREG_cache;

PRIVATE POOLREG_cache *POOLREG_ht = NULL;

PRIVATE uint
Hash(int handle, int poolno)
{
  uint hashval = 100 * poolno + handle; /* handle typically << 100 */
  hashval %= POOLREG_hashsize;
  return hashval;
}

PRIVATE void
HashInit()
{
  if (!POOLREG_ht) {
    POOLREG_cache *tmp_POOLREG_ht = NULL;
    char *p = getenv("ODB_POOLREG_HASHSIZE");
    if (p) {
      POOLREG_hashsize = atoi(p);
      if (POOLREG_hashsize <= 0) POOLREG_hashsize = POOLREG_HASHSIZE;
    }
    else
      POOLREG_hashsize = POOLREG_HASHSIZE;
    CALLOC(tmp_POOLREG_ht, POOLREG_hashsize);
    POOLREG_ht = tmp_POOLREG_ht;
  }
}

PUBLIC void
init_POOLREG_lock()
{
  HashInit();
}

PUBLIC ODB_Pool *
get_poolreg(int handle, int poolno)
{
  ODB_Pool *pool = NULL;
  if (handle >= 1 && poolno >= 1) {
    uint index = Hash(handle, poolno);
    POOLREG_cache *p = &POOLREG_ht[index];
    while (p) {
      if (p->handle == handle && p->poolno == poolno && p->pool) {
	pool = p->pool;
	break;
      }
      p = p->collision;
    }
  }
  return pool;
}


PRIVATE POOLREG_cache *
NewEntry(int handle, int poolno, ODB_Pool *pool)
{
  POOLREG_cache *p;
  ALLOC(p, 1);
  p->handle    = handle;
  p->poolno    = poolno;
  p->pool      = pool;
  p->collision = NULL;
  return p;
}

PUBLIC void
put_poolreg(int handle, int poolno, ODB_Pool *pool)
{
  if (handle >= 1 && poolno >= 1) {
    uint index = Hash(handle, poolno);
    POOLREG_cache *p = &POOLREG_ht[index];
    if (p->handle == 0) { /* Allocated (upon HashInit), but not in use yet */
      p->handle = handle;
      p->poolno = poolno;
      p->pool   = pool;
    }
    else {
      POOLREG_cache *lastp = NULL;
      do { /* Re-use an existing position ? */
	lastp = p;
	if (p->handle == handle && p->poolno == poolno) {
	  p->pool = pool;
	  break;
	}
	p = p->collision;
      } while (p);
      if (!p) {
	p = NewEntry(handle, poolno, pool);
	lastp->collision = p; /* chain */
      }
    }
  }
}
