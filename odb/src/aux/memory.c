#include <stdio.h>
#include <string.h>
#include <sys/types.h>
/* #include <malloc.h> */
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>

#include "alloc.h"
#include "magicwords.h"

#define WORDLEN   ((int)sizeof(ll_t))

#define SETSIZE(size_in_bytes) { \
  if (size_in_bytes < WORDLEN) size_in_bytes = WORDLEN; \
  size_in_bytes = size_in_bytes + cushion * WORDLEN; \
  size_in_bytes = RNDUP(size_in_bytes,WORDLEN); \
}

#define NCTRL_BYTES ((3 + cushion) * WORDLEN)

#define PAD  ((u_ll_t)0xdeadbeef)

#define CUSH ((u_ll_t)0xf000c0de)

#define CUSHION_DEFAULT 0

#if !defined(STD_MEM_ALLOC)
#define STD_MEM_ALLOC 1
#endif

PRIVATE int memory_set_up = 0;
PRIVATE int cushion = CUSHION_DEFAULT;
PRIVATE int std_mem_alloc = STD_MEM_ALLOC;
PRIVATE int memory_trace = 0;
PRIVATE int memory_trace_with_traceback = 0;
PRIVATE int init_memory_area = 0;
PRIVATE int freeptr_trace = 0;
PRIVATE FILE *fp_freeptr_trace = NULL;

PRIVATE const void *pmon = NULL;

PUBLIC void 
ODB_monitor_memory_addr(const void *p, const char *pwhat, 
			const char *file, int linenum)
{
  if (!memory_trace) return;
  if (!pmon) {
    pmon = p;
    fprintf(stderr,"--> ODB_monitor_memory_addr(p=%p, pwhat=%s) activated in %s:%d\n",
	    p,pwhat,file,linenum);
  }
}

PRIVATE void 
memory_exit(const char *routine, int size_in_bytes, void *addr,
	    const char *extra_msg, unsigned int do_abort,
	    const char *var, const char *file, int linenum)
{
  fprintf(stderr,
	  "memory_exit(bytes=%d,addr=%p,cushion=%d): Memory problem encountered in '%s'\n",
	  size_in_bytes,addr,cushion,routine);
  fprintf(stderr,"\tCalled for '%s', from file='%s', line=%d\n", var, file, linenum);
  if (extra_msg) fprintf(stderr,"\t%s\n",extra_msg);
  if (do_abort) {
    char *c = getenv("ODB_MEMORY_TRACE_CMD");
    if (c) {
      char cmd[256];
      if (strchr(c,'%')) 
        sprintf(cmd,"%s %d",c,getpid());
      else
        sprintf(cmd,"%s",c);
      fprintf(stderr,"\tRunning command '%s' ...",cmd);
      (void)system(cmd);
    }
    fprintf(stderr,"\tAborting ...\n");
    RAISE(SIGABRT);
  }
}


PRIVATE void 
set_up_memory(const char *var, const char *file, int linenum)
{
  if (!memory_set_up) {
    char *s = getenv("ODB_MEMORY_ALLOC_STANDARD");
    char *m = getenv("ODB_MEMORY_CUSHION");
    char *t = getenv("ODB_MEMORY_TRACE");
    char *tbk = getenv("ODB_MEMORY_TRACE_WITH_TRACEBACK");
    char *x = getenv("ODB_MEMORY_INIT");
    char *z = getenv("ODB_MEMORY_FREEPTR_TRACE");
    memory_trace = t ? atoi(t) : 0;
    memory_trace_with_traceback = tbk ? atoi(tbk) : 0;
    cushion = m ? atoi(m) : CUSHION_DEFAULT;
    if (cushion < 0) cushion = 0;
    std_mem_alloc = s ? atoi(s) : STD_MEM_ALLOC;
    if (std_mem_alloc <= 0) std_mem_alloc = 0;
    init_memory_area = x ? atoi(x) : 0;
    if (init_memory_area < 0) init_memory_area = 0;
    freeptr_trace = z ? atoi(z) : 0;
    if (t) {
      fprintf(stderr,
      "set_up_memory(): Memory overflow cushion set to %d (%d-byte) words. Allocation %s. Allocated memory %s. Freepointer trace %s.\n",
	      cushion,WORDLEN,
	      std_mem_alloc ? "standard" : "non-standard",
	      init_memory_area ? "will be initialized" : "will not be initialized",
	      freeptr_trace ? "active" : "not active");
      
    }
    if (WORDLEN != 8) {
      memory_exit("set_up_memory",0,NULL,
		  "Implementation error: WORDLEN not equal to 8",1,
		  var,file,linenum);
    }
    memory_set_up = 1;
  }
}

/* Routines available with non-standard memory allocation method only */

/* Set magic word to "MMRY" */

#ifdef LITTLE
#define MAGIC_WORD YRMM
#else
#define MAGIC_WORD MMRY
#endif

typedef struct {
  u_ll_t magics;
    ll_t lenbytes;
  u_ll_t *user_area;
  u_ll_t *cushion;
  u_ll_t pad;
} Mem_t;


PRIVATE u_ll_t *
current_alloc_start(u_ll_t *addr, int *len,
		    const char *var, const char *file, int linenum)
{
  u_ll_t *p = addr ? addr - 2 : NULL;
  unsigned int magics = p[0];
  *len = (int)p[1];
  if (magics != MAGIC_WORD) {
    char msg[256];
    sprintf(msg,
	    "Error: %s Word#1 = '%u' not matching the magic = '%u'; p[0]=%llu, p[1]=%llu",
	    "A memory corruption?", magics, MAGIC_WORD, p[0], p[1]);
    memory_exit("current_alloc_start", *len, addr, 
		msg, 1,
		var,file,linenum);
  }
  return p;
}


PRIVATE void
free_mem(void *addr,
	 const char *var, const char *file, int linenum,
	 int do_free)
{
  u_ll_t *p;
  int size_in_bytes;
  u_ll_t pad;
  char msg[256];
  unsigned int do_abort = 0;

  /* Check that the initial padding is reasonable (not necessarely okay) */
  
  p = current_alloc_start(addr, &size_in_bytes,var,file,linenum);

  if (do_free && memory_trace > 0) {
    fprintf(stderr,"free_mem(%d): Size=%d, True address of '%s' at %p at %s:%d\n",
	    do_free,size_in_bytes,var,p,file,linenum);
  }

  if (size_in_bytes <= 0) {
    sprintf(msg,
	    "Error: Attempted to free corrupted memory: size_in_bytes=%d", size_in_bytes);
    memory_exit("free_mem", size_in_bytes, addr, 
		msg, do_free, /* No abort unless *really* free'ing call */
		var,file,linenum);
    do_abort++;
  }

  /* Check the cushion area */

  if (cushion > 0) {
    int j, count = 0;
    for (j=0; j<cushion; j++) {
      u_ll_t cush = p[2 + size_in_bytes/WORDLEN + j];
      if (cush != CUSH) count++;
    }
    if (count > 0) {
      sprintf(msg,
      "Error: Overwrote into the cushion area? Start of expected pattern=0x%llx, but found=0x%llx",
	      CUSH, p[2 + size_in_bytes/WORDLEN]);
      memory_exit("free_mem", size_in_bytes, addr, 
		  msg, 0, /* No abort ... yet */
		  var,file,linenum);
      do_abort++;
    }
  }

  /* Check end padding */
  /* Assume that the "size_in_bytes" is still correct */

  pad = p[2 + size_in_bytes/WORDLEN + cushion];

  if (pad != PAD) {
    sprintf(msg,
	    "Error: Memory overwrite problem: pad=0x%llx (expected 0x%llx)",
	    pad, PAD);
    memory_exit("free_mem", size_in_bytes, addr, 
		msg, 0, /* No abort ... yet */
		var,file,linenum);
    do_abort++;
  }

  if (do_abort) RAISE(SIGABRT);

  if (do_free) {
    if (pmon && pmon == addr) {
      pmon = NULL;
      fprintf(stderr,"--> ODB_monitor_memory_addr(addr=%p) disabled\n",addr);
    }
    free(p);
  }
}

PUBLIC void 
ODB_check_memory_overwrite(const char *file, int linenum)
{
  if (pmon && memory_trace > 0) {
    free_mem((void *)pmon,
	     "<ODB_monitor_memory_address>",
	     file, linenum, 0);
  }
}


/* Memory allocation wrapper routines */


int
ODB_std_mem_alloc(int onoff)
{
  /* Use this only if you knwo what you are doing;
     Meant for imposing the permanent standard or 
     adjusted memory allocation method;
     Once set at the beginning, do not re-adjust !! */
  
  int rc; /* previous value */
  if (!memory_set_up) set_up_memory("ODB_std_mem_alloc()", __FILE__, __LINE__);
  rc = std_mem_alloc;
  std_mem_alloc = (onoff > 0) ? 1 : 0;
  if (memory_trace > 0) 
    fprintf(stderr,"***Warning: Allocation method is now %s\n",
	    std_mem_alloc ? "standard" : "adjusted");
  return rc;
}


void *
ODB_reserve_mem(int size_elem, int num_elem,
		const char *var, const char *file, int linenum) 
{ 
  double *dp;
  u_ll_t *p;
  size_t total_len;
  size_t size_in_bytes = (size_t)size_elem * (size_t)num_elem;

  if (!memory_set_up) set_up_memory(var,file,linenum);

  SETSIZE(size_in_bytes);

  if (std_mem_alloc) {
    total_len = size_in_bytes;

    if (memory_trace > 0 && total_len >= memory_trace) {
      fprintf(stderr,
	      "ODB_reserve_mem[std]: malloc(%d >= %d x %d) for '%s' at %s:%d\n",
	      total_len, size_elem, num_elem, var, file, linenum);
#ifdef RS6K
      if (memory_trace_with_traceback) { xl__trbk_(); }
#endif
    }
    
    dp = (double *)malloc(total_len);
    p = (u_ll_t *)dp;
    if (memory_trace > 0 && total_len >= memory_trace) {
      fprintf(stderr,"ODB_reserve_mem(std): Size=%d, True address of '%s' at %p at %s:%d\n",
	      size_in_bytes, var, p, file, linenum);
    }
    if (!p) memory_exit("ODB_reserve_mem[std]",size_in_bytes,p,
			NULL,1,
			var,file,linenum);
    if (init_memory_area && p) codb_zerofill_(p,&total_len);
    return p;
  }
  else {
    total_len = RNDUP(size_in_bytes + NCTRL_BYTES, WORDLEN);
    
    if (memory_trace > 0 && total_len >= memory_trace) {
      fprintf(stderr,
	      "ODB_reserve_mem[non-std]: malloc(%d >= %d x %d) for '%s' at %s:%d\n",
	      total_len, size_elem, num_elem, var, file, linenum);
#ifdef RS6K
      if (memory_trace_with_traceback) {  xl__trbk_(); }
#endif
    }
    
    dp = (double *)malloc(total_len);
    p = (u_ll_t *)dp;
    if (memory_trace > 0 && total_len >= memory_trace) {
      fprintf(stderr,"ODB_reserve_mem(non-std): Size=%d, True address of '%s' at %p at %s:%d\n",
	      size_in_bytes, var, p, file, linenum);
    }
    if (!p) memory_exit("ODB_reserve_mem[non-std]",total_len,p,
			NULL,1,
			var,file,linenum);
    if (init_memory_area && p) codb_zerofill_(p,&total_len);
    p[0] = MAGIC_WORD;
    p[1] = size_in_bytes;
    /* User area is from p[2] to p[size_in_bytes/WORDLEN - 1] */

    /* Possible cushion initialization */
    if (cushion > 0) {
      int j;
      for (j=0; j<cushion; j++) {
	p[2 + size_in_bytes/WORDLEN + j] = CUSH;
      }
    }

    /* End padding */
    p[2 + size_in_bytes/WORDLEN + cushion] = PAD;

    ODB_check_memory_overwrite(file,linenum);

    return p + 2;
  }
}


void *
ODB_reserve_zeromem(int size_elem, int num_elem,
		    const char *var, const char *file, int linenum) 
{ 
  size_t  size_in_bytes = (size_t)size_elem * (size_t)num_elem;
  void *p = ODB_reserve_mem(size_elem,num_elem,var,file,linenum);
  if (p && size_in_bytes > 0) codb_zerofill_(p,&size_in_bytes);
  return p;
}

#if defined(RS6K) && defined(__64BIT__)
PRIVATE int
check_freeptr(const void *p,
	      const char *var, const char *file, int linenum)
{
  FILE *fp = fp_freeptr_trace;
  int okay = 1;
  const u_ll_t bss_lo = (u_ll_t)0x000000100000000;
  const u_ll_t bss_hi = (u_ll_t)0x6FFFFFFF0000000;
  u_ll_t address = (u_ll_t)p;
  if (address < bss_lo || address > bss_hi) {
    if (fp) {
      fprintf(fp,
	      " check_freeptr(): Address 0x%llx not allocated from heap (var=%s, file=%s:%d)\n",
	      address, var, file, linenum);
      fflush(fp);
    }
    okay = 0; /* Out of valid range */
  }
  else { /* Just print it */
    if (fp) {
      fprintf(fp," check_freeptr(): Valid heap address 0x%llx (var=%s, file=%s:%d)\n",
	      address, var, file, linenum);
      fflush(fp);
    }
  }
  return okay;
}
#endif

void 
ODB_release_mem(void *p,
		const char *var, const char *file, int linenum) 
{ 
  if (!memory_set_up) set_up_memory(var,file,linenum);
  if (std_mem_alloc) {
    int okay = 1;
#if defined(RS6K) && defined(__64BIT__)
    if (freeptr_trace) {
      okay = check_freeptr(p, var, file, linenum);
    }
#endif
    /* if (okay && p) free(p); -- not quite yet */
    if (p) free(p); 
  }
  else {
    if (p) {
      if (pmon && p != pmon) ODB_check_memory_overwrite(file,linenum);
      free_mem(p,var,file,linenum,1); 
    }
  }
}


void *
ODB_re_alloc(void *p, int size_elem, int num_elem,
	     const char *var, const char *file, int linenum)
{
  int size_in_bytes = size_elem * num_elem;
  if (!memory_set_up) set_up_memory(var,file,linenum);
  if (!p) 
    p = ODB_reserve_mem(size_elem,num_elem,var,file,linenum); /* Revert to reserve_mem */
  else {
    SETSIZE(size_in_bytes);
    if (std_mem_alloc) {
      double *dp = (double *)realloc(p, size_in_bytes);
      p = dp;
      if (!p) memory_exit("ODB_re_alloc",size_in_bytes,p,
			  NULL,1,
			  var,file,linenum);
    }
    else {
      int size_old;
      (void) current_alloc_start(p, &size_old,var,file,linenum);
      if (size_old < size_in_bytes) {
	void *d = ODB_reserve_mem(size_elem,num_elem,var,file,linenum);
	if (d) memcpy(d,p,size_old);
	ODB_release_mem(p,var,file,linenum);
	p = d;
      }
    }
  }
  return p;
}


char *
ODB_strdup_mem(const char *s,
	       const char *var, const char *file, int linenum)
{
  char *p = NULL;
  if (!memory_set_up) set_up_memory(var,file,linenum);
  if (std_mem_alloc) {
    p = strdup(s ? s : "");
  }
  else {
    int size_in_bytes = s ? strlen(s) : 0;
    if (memory_trace > 0 && size_in_bytes >= memory_trace && s) {
      fprintf(stderr,"ODB_strdup_mem(%s) : len=%d\n",
	      s, size_in_bytes);
#ifdef RS6K
      if (memory_trace_with_traceback) { xl__trbk_(); }
#endif
    }
    p = ODB_reserve_mem(sizeof(*p),size_in_bytes+1,var,file,linenum);
    if (p && s) memcpy(p,s,size_in_bytes); 
    if (p) p[size_in_bytes] = '\0';
  }
  return p;
}


int
ODB_freeptr_trace(FILE *fp,
		  const char *routine,
		  const char *file,
		  int linenum,
		  int onoff)
{
  int rc = 0;
  if (fp && freeptr_trace) {
    fprintf(fp, "*** %s ODB_freeptr_trace() in routine=%s(), file=%s:%d\n",
	    onoff ? "Enabling" : "Disabling",
	    routine, file, linenum);
    fflush(fp);
    fp_freeptr_trace = onoff ? fp : NULL;
    if (fp_freeptr_trace) rc = 1;
  }
  return rc;
}

/* Fast(er) memory initialization routines for vector machines */

/* The "scalar" version should be ok for NEC SX, since its C-compiler 
   can replace memset & memcpy with vectorized versions easily */

#ifdef VPP
#pragma global noalias
#pragma global novrec

PRIVATE const int min_veclen = 5;  /* minimum vector length, where faster method is applied to */
PRIVATE const long long align = 8; /* address alignment in double-word boundary */
#endif

void
codb_strblank_(char *c,
	       /* Hidden arguments */
	       int len_c)
{
  /* Initializes F90 character string into blanks in a vector loop */
#ifdef VPP
  int *d;
  if (len_c >= min_veclen*sizeof(*d)) {
    long long int addr;
    int j, n, rem;
    void *v;

    do {
      addr = (long long)c;
      if (addr%align == 0) break;
      *c++ = ' ';
      len_c--;
    } while (len_c > 0);

    n = len_c/sizeof(*d);
    v = c; d = v; /* ... in order to remove complaints ;-) */
    for (j=0; j<n; j++) d[j] = 0x20202020; /* Fill in 4 spaces in one go, hex=20 into each */
    rem = len_c%sizeof(*d);
    if (rem > 0) memset(&c[n*sizeof(*d)],' ',rem);
  }
  else {
    memset(c,' ',len_c);
  }
#else
  memset(c,' ',len_c);
#endif
}


void
codb_zerofill_(void *x, const size_t *nbytes)
{
  /* Initializes array of *nbytes to zero */
  size_t len_c = *nbytes;
  char *c = x;
#ifdef VPP
  int *d;
  if (len_c >= min_veclen*sizeof(*d)) {
    long long int addr;
    int j, rem;
    size_t n;
    void *v;

    do {
      addr = (long long)c;
      if (addr%align == 0) break;
      *c++ = 0;
      len_c--;
    } while (len_c > 0);

    n = len_c/sizeof(*d);
    v = c; d = v; /* ... in order to remove complaints from compiler ;-) */
    for (j=0; j<n; j++) d[j] = 0; /* zerofill */
    rem = len_c%sizeof(*d);
    if (rem > 0) memset(&c[n*sizeof(*d)],0,rem);
  }
  else {
    memset(c,0,len_c);
  }
#else
  memset(c,0,len_c);
#endif
}


void
codb_str2dbl_(const char str[], const int *nstr, 
	      double dbl[], const int *ndbl)
{
  if (str && nstr && *nstr > 0 &&
      dbl && ndbl && *ndbl > 0) {
    int nbytes = (*ndbl) * sizeof(*dbl);
    if (*nstr < nbytes) nbytes = *nstr;
    memcpy(dbl,str,nbytes);
  }
}
