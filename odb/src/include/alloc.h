#ifndef _ALLOC_H_
#define _ALLOC_H_

#if (defined(LINUX) && !defined(CYGWIN)) || defined(SUN4) || defined(RS6K) || defined(NECSX)
/* Above systems: Enforce to use <alloca.h> i.e. ALLOCX becomes ALLOCA, unless explicitly set -DUSE_ALLOCA_H=0 */
#if !defined(USE_ALLOCA_H) || (defined(USE_ALLOCA_H) && USE_ALLOCA_H != 0)
#define USE_ALLOCA_H 1
#endif
#endif

#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#ifdef USE_MALLOC_H
#include <malloc.h>
#endif
#if defined(USE_ALLOCA_H) && USE_ALLOCA_H != 0
#include <alloca.h>
#endif
#include "ecstdlib.h"
#include <signal.h>

#include "privpub.h"

#if defined(STD_MEM_ALLOC) && STD_MEM_ALLOC == 1 && defined(INTERCEPT_ALLOC)
#undef INTERCEPT_ALLOC
#endif

#if defined(INTERCEPT_ALLOC)
#if defined(RS6K) && defined(__64BIT__)

#define EC_free     __free
#define EC_malloc   __malloc
#define EC_calloc   __calloc
#define EC_realloc  __realloc
#define EC_strdup   __strdup

#elif defined(NECSX)
/* Do nothing */
#else
/* Illegal to have -DINTERCEPT_ALLOC */
#undef INTERCEPT_ALLOC
#endif

#endif

#if defined(INTERCEPT_ALLOC)

/* For reference, see also ifsaux/utilities/getcurheap.c */

#define THEmalloc  EC_malloc
extern void *EC_malloc(ll_t size);
#define THEcalloc  EC_calloc
extern void *EC_calloc(ll_t nelem, ll_t elsize);
#define THErealloc EC_realloc
extern void *EC_realloc(void *p, ll_t size);
#define THEstrdup  EC_strdup
extern char *EC_strdup(const char *s);
#define THEfree    EC_free
extern void EC_free(void *p);

#else /* i.e. !defined(INTERCEPT_ALLOC) */

#define THEmalloc  malloc
#define THEcalloc  calloc
#define THErealloc realloc
#define THEstrdup  strdup
#define THEfree    free

#endif

extern int ODB_std_mem_alloc(int onoff);
extern void *ODB_reserve_zeromem(int size_elem, int num_elem, const char *var, const char *file, int linenum);
extern void *ODB_reserve_mem(int size_elem, int num_elem, const char *var, const char *file, int linenum);
extern void *ODB_re_alloc(void *p, int size_elem, int num_elem, const char *var, const char *file, int linenum);
extern void  ODB_release_mem(void *x, const char *var, const char *file, int linenum);
extern char *ODB_strdup_mem(const char *s, const char *var, const char *file, int linenum);
extern int ODB_freeptr_trace(FILE *fp, const char *routine, const char *file, int line, int onoff);

extern void
codb_strblank_(char *c,
	       /* Hidden arguments */
	       int len_c);
extern void
codb_zerofill_(void *c, const size_t *nbytes);

/* Some common file I/O utilities */

int file_exist_(const char *s);
int is_regular_file_(const char *s);
int is_directory_(const char *s);
int rename_file_(const char *in, const char *out);
int remove_file_(const char *filename);

#define ALLOC_ABORT(x, bytes, via) { \
  fprintf(stderr,"***Error: Unable to allocate %lld bytes via '%s' for %s at %s:%d\n", \
                  (ll_t)(bytes), via, #x, __FILE__, __LINE__); \
  RAISE(SIGABRT); \
}

/* Since ALLOCA uses alloca(), don't encapsulate the ALLOCA around {}'s */
#define ALLOCA(x,size) \
  x = alloca((ll_t)MAX(1,(size)) * sizeof(*(x))); \
  if (!x) ALLOC_ABORT(x, MAX(1,(size)) * sizeof(*(x)), "alloca()"); \
  { extern ll_t getstk_(); /* From IFSAUX */ \
         (void) getstk_(); /* Keep track of stacksizes */ }

#if defined(USE_ALLOCA_H) && USE_ALLOCA_H != 0
#define ALLOCX ALLOCA
#define FREEX(x) (x) = NULL
#else
#define ALLOCX ALLOC
#define FREEX  FREE
#endif

#if defined(STD_MEM_ALLOC) && STD_MEM_ALLOC == 1

/* Memory allocation via standard route 
   (not necessarely the default; pls. check config/Makefiles) */

#define CALLOC(x,size) { \
  ll_t _nelem_x = MAX(1,(size)); \
  x = THEcalloc(_nelem_x, sizeof(*(x))); \
  if (!x) ALLOC_ABORT(x, _nelem_x * sizeof(*(x)), "calloc()"); }

#define CALLOC3(x,size,elemsize) { \
  ll_t _nelem_x = MAX(1,(size)); \
  x = THEcalloc(_nelem_x, elemsize); \
  if (!x) ALLOC_ABORT(x, _nelem_x * elemsize), "calloc()@3"); }

#define ALLOC(x,size) { \
  ll_t _nelem_x = MAX(1,(size)); \
  x = THEmalloc(_nelem_x * sizeof(*(x))); \
  if (!x) ALLOC_ABORT(x, _nelem_x * sizeof(*(x)), "malloc()"); }

#define ALLOC3(x,size,elemsize) { \
  ll_t _nelem_x = MAX(1,(size)); \
  x = THEmalloc(_nelem_x * elemsize); \
  if (!x) ALLOC_ABORT(x, _nelem_x * elemsize, "malloc()@3"); }

#define REALLOC(x,size) { \
  ll_t _nelem_x = MAX(1,(size)); \
  x = THErealloc(x, _nelem_x * sizeof(*(x))); \
  if (!x) ALLOC_ABORT(x, _nelem_x * sizeof(*(x)), "realloc()"); }

#define REALLOC3(x,size,elemsize) { \
  ll_t _nelem_x = MAX(1,(size)); \
  x = THErealloc(x, _nelem_x * elemsize); \
  if (!x) ALLOC_ABORT(x, _nelem_x * elemsize), "realloc()@3"); }

#define FREE(x) ((x) ? (THEfree(x), (x) = NULL) : NULL)

#define FREEPTR_TRACE(fp, routine, onoff) 0

#define STRDUP(s) THEstrdup(s ? s : "")

#else

/* Use memory allocation via wrapper library for better control of memory leaks when needed */

#define CALLOC(x,size) \
  x = ODB_reserve_zeromem(sizeof(*(x)), (size), #x, __FILE__, __LINE__)

#define CALLOC3(x,size,elemsize) \
  x = ODB_reserve_zeromem(elemsize, (size), #x, __FILE__, __LINE__)

#define  ALLOC(x,size) \
  x = ODB_reserve_mem(sizeof(*(x)), (size), #x, __FILE__, __LINE__)

#define  ALLOC3(x,size,elemsize) \
  x = ODB_reserve_mem(elemsize, (size), #x, __FILE__, __LINE__)

#define REALLOC(x,size) \
  x = ODB_re_alloc(x, sizeof(*(x)), (size), #x, __FILE__, __LINE__)

#define REALLOC3(x,size,elemsize) \
  x = ODB_re_alloc(x, elemsize, (size), #x, __FILE__, __LINE__)

#define FREE(x) \
  if (x) { ODB_release_mem(x, #x, __FILE__, __LINE__); (x) = NULL; }

#define FREEPTR_TRACE(fp, routine, onoff) \
  ODB_freeptr_trace(fp, #routine, __FILE__, __LINE__, onoff)

#define STRDUP(s) ODB_strdup_mem(s,  #s, __FILE__, __LINE__)

#endif

/* string equalities (moved here from odb.h) */

#define strequ(s1,s2)     ((const void *)(s1) && (const void *)(s2) && *(s1) == *(s2) && strcmp(s1,s2) == 0)
#define strnequ(s1,s2,n)  ((const void *)(s1) && (const void *)(s2) && *(s1) == *(s2) && strncmp(s1,s2,n) == 0)

#ifndef odbA2aDIFF
#define odbA2aDIFF 32
#endif

#define strcaseequ(s1,s2)     ((const void *)(s1) && (const void *)(s2) && \
(*(s1) == *(s2) || *(s1) == (*(s2)) + odbA2aDIFF || *(s2) == (*(s1)) + odbA2aDIFF) && \
 strcasecmp(s1,s2) == 0)
#define strncaseequ(s1,s2,n)  ((const void *)(s1) && (const void *)(s2) && \
(*(s1) == *(s2) || *(s1) == (*(s2)) + odbA2aDIFF || *(s2) == (*(s1)) + odbA2aDIFF) && \
 strncasecmp(s1,s2,n) == 0)

/* For convenience & consistency */

#define CALLOC2  CALLOC
#define ALLOC2   ALLOC
#define REALLOC2 REALLOC

/* Fortran to C character string passing */

#define DECL_FTN_CHAR(x)  char *p_##x = NULL

/* Keep ALLOCX's outside {}'s since may be an alloca() */

#define ALLOC_FTN_CHAR(x) \
  ALLOCX(p_##x, x##_len + 1); \
  { register int _plen = x##_len; /* try not to alter the value of x##_len */ \
  /* skip leading blanks, if any */ \
  while ((_plen > 0) && (*x == ' ')) { ++x; --_plen; } \
  /* copy string */ \
  memcpy(p_##x, x, _plen); \
  p_##x[_plen] = '\0'; \
  /* strip trailing blanks, if any */ \
  while (_plen-- > 0) { if (p_##x[_plen] != ' ' && p_##x[_plen] != '\0') break; } \
  p_##x[++_plen] = '\0'; }

#define ALLOC_OUTPUT_FTN_CHAR(x) ALLOCX(p_##x, x##_len + 1)

#define COPY_2_FTN_CHAR(x) if ((p_##x) && (x)) { \
  int _plen = strlen(p_##x); \
  _plen = MIN(x##_len, _plen); \
  memcpy(x, p_##x, _plen); \
  if (x##_len > _plen) memset(&x[_plen], ' ', x##_len - _plen); }

#define COPY_2_FTN_CHARv2(x,len) if ((p_##x) && (x)) { \
  int _plen = MIN(x##_len, len); \
  memcpy(x, p_##x, _plen); \
  if (x##_len > _plen) memset(&x[_plen], ' ', x##_len - _plen); }

#define FREE_FTN_CHAR(x)  FREEX(p_##x)

#ifdef USE_CTRIM
/* Faster macros ; assume FTN input strings are char(0) terminated 
   Assume that -DUSE_CTRIM is used both in C and Fortran90 compilations */

#ifdef SV2 
  /* CGG 03/09 */ 
#define ALLOC_FASTFTN_CHAR(x) { \
  int _plen = x##_len;  /* try not to alter the value of x##_len */ \
  p_##x = (char *)x; \
  if (_plen > 0 && p_##x[_plen-1] == '\0') --_plen; \
}
#define FREE_FASTFTN_CHAR(x) FREE_FTN_CHAR(x)
#else
  /* The following is used */
#define ALLOC_FASTFTN_CHAR(x) /* do nothing */
#define FREE_FASTFTN_CHAR(x)  /* do nothing */
#endif


#else

#define ALLOC_FASTFTN_CHAR(x) ALLOC_FTN_CHAR(x)
#define FREE_FASTFTN_CHAR(x)  FREE_FTN_CHAR(x)

#endif

#endif



