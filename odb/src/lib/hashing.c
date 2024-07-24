
/* hashing.c */

#ifdef RS6K
#pragma options noextchk
#endif

#include "odb.h"

/* Hash function : Specific for UNIQUE BY -keyword processing */

#ifndef HASHSIZE
#define HASHSIZE 3019U
#endif

PRIVATE uint Hashsize = HASHSIZE;

#ifndef HASHHITS
#define HASHHITS 5
#endif

PRIVATE int  Hashhits = HASHHITS;

typedef struct _ODB_Hash_Table {
  int tag;
  int nhashval;
  double *d;
  int hits;
  struct _ODB_Hash_Table *next;
} ODB_Hash_Table;

PRIVATE ODB_Hash_Table **ODB_hashptr = NULL;

PUBLIC int  ODB_hashmin = INT_MAX;
PUBLIC int  ODB_hashmax = 0;
PRIVATE int  ODB_hashdebug = 0;
PRIVATE int  ODB_hashcalls = 0;
PRIVATE int *ODB_hashhits = NULL;


PRIVATE uint
Hashfunc(int n, const uint u[]) 
{ 
  int j; 
  uint h = 0;
#ifdef NECSX
  /* for now ... */
#pragma cdir novector
#endif
  for (j=0; j<n; j++) {
    h = HASHFUNC(h, u[j], Hashsize);
  } 
  h %= Hashsize;
  ODB_hashmin = MIN(ODB_hashmin, h);
  ODB_hashmax = MAX(ODB_hashmax, h);
  return h; 
}

PUBLIC uint
ODB_Hashsize()
{
  static boolean first_time = 1;

  if (first_time) {
    char *p = getenv("ODB_HASHDEBUG");
    if (p) ODB_hashdebug = atoi(p);
    p = getenv("ODB_HASHSIZE");
    if (p) {
      Hashsize = atoi(p);
      if (Hashsize <= 0) Hashsize = HASHSIZE;
    }
    else
      Hashsize = HASHSIZE;

    CALLOC(ODB_hashptr, Hashsize);
    CALLOC(ODB_hashhits, Hashsize);

    Hashhits = HASHHITS;
    p = getenv("ODB_HASHHITS");
    if (p) {
      int value = atoi(p);
      if (value > 0) Hashhits = value;
    }

    first_time = 0;
  }

  return Hashsize;
}


PUBLIC void
codb_hash_init_()
{
  int j;

  (void) ODB_Hashsize();
  if (ODB_hashdebug) ODB_Hash_print(NULL);

  for (j=ODB_hashmin; j<=ODB_hashmax; j++) {
    ODB_Hash_Table *p = ODB_hashptr[j];
    if (p) {
      while (p) {
	ODB_Hash_Table *next = p->next;
	FREE(p->d);
	FREE(p);
	p = next;
      }
      ODB_hashptr[j] = NULL; /* Since the previous FREE(p) freed already */
    }
  }
  if (ODB_hashdebug) {
    ODB_hashcalls = 0;
    for (j=ODB_hashmin; j<=ODB_hashmax; j++) ODB_hashhits[j] = 0;
  }
  ODB_hashmin = INT_MAX;
  ODB_hashmax = 0;
}


PUBLIC void
codb_hash_reset_()
{
  int j;

  (void) ODB_Hashsize();
  if (ODB_hashdebug) ODB_Hash_print(NULL);

  for (j=ODB_hashmin; j<=ODB_hashmax; j++) {
    ODB_Hash_Table *p = ODB_hashptr[j];
    while (p) {
      p->hits = 0;
      p = p->next;
    }
  }

  if (ODB_hashdebug) ODB_hashcalls = 0;
}


PUBLIC void
codb_d_unique_(const int *nval,
	       const double d[],
	       const uint *hash,
	       int *is_unique,
	       int *tag,
	       uint *hash_out)
{
  int n = *nval;
  *is_unique = 1;
  if (hash_out) *hash_out = -1;

  (void) ODB_Hashsize();
  if (ODB_hashdebug) ODB_hashcalls++;

  if (n > 0) {
    ODB_Hash_Table *p;
    int nuint = n * (sizeof(*d)/sizeof(uint));
    /* A hash might already be calculated in the vectorizable routine */
    uint h = hash ? *hash : Hashfunc(nuint, (const uint *)d);

    if (hash_out) *hash_out = h;

    p = ODB_hashptr[h];
    if (!p) {
      ALLOC(p,1);
      p->tag = tag ? *tag : 0;
      p->nhashval = n;
      ALLOC(p->d, n);
      ODB_dbl2dbl(p->d, d, n);
      p->hits = 1;
      p->next = NULL;
      ODB_hashptr[h] = p;
      if (ODB_hashdebug) ODB_hashhits[h]++;
    }
    else {
      for (;;) {
	if ((p->nhashval == n) && 
	    ODB_uint_equal((const uint *)p->d, (const uint *)d, nuint)) {
	  if (++p->hits > 1) {
	    *is_unique = 0;
	    if (tag) *tag = p->tag;
	  }
	  break;
	}
	else {
	  if (p->next) {
	    p = p->next;
	  }
	  else {
	    ALLOC(p->next,1);
	    p = p->next;
	    p->tag = tag ? *tag : 0;
	    p->nhashval = n;
	    ALLOC(p->d, n);
	    ODB_dbl2dbl(p->d, d, n);
	    p->hits = 1;
	    p->next = NULL;
	    if (ODB_hashdebug) ODB_hashhits[h]++;
	    break;
	  }
	}
      } /* for (;;) */
    }
  }

  if (tag && *is_unique) *tag = 0;

  if (ODB_hashdebug < 0) {
    int arg = -ODB_hashdebug;
    if (ODB_hashcalls%arg == 0) ODB_Hash_print(NULL);
  }
}


PUBLIC void
codb_ui_unique_(const int *nval,
		const uint ui[],
		const uint *hash,
		int *is_unique,
		int *tag,
		uint *hash_out)
{
  int n = *nval;
  *is_unique = 1;

  if (n > 0) {
    double *d = NULL;
    ALLOC(d, n);
    ODB_uint2dbl(d, ui, n);
    codb_d_unique_(nval, d, hash, is_unique, tag, hash_out);
    FREE(d);
  }
}


PUBLIC void
codb_r_unique_(const int *nval,
	       const float r[],
	       const uint *hash,
	       int *is_unique,
	       int *tag,
	       uint *hash_out)
{
  int n = *nval;
  *is_unique = 1;

  if (n > 0) {
    double *d = NULL;
    ALLOC(d, n);
    ODB_float2dbl(d, r, n);
    codb_d_unique_(nval, d, hash, is_unique, tag, hash_out);
    FREE(d);
  }
}


PUBLIC int
ODB_Unique(const int n, ...)
{
  int is_unique = 1;
  if (n > 0) {
    double *d = NULL;
    ALLOC(d, n);
    {
      int j;
      va_list ap;
      va_start(ap, n);
      for (j=0; j<n; j++) {
	d[j] = va_arg(ap, double);
      }
      va_end(ap);
    }
    codb_d_unique_(&n, d, NULL, &is_unique, NULL, NULL);
    FREE(d);
  }
  return is_unique;
}

PUBLIC double
ODBunique(const int n, const double args[])
{
  int is_unique = 1;
  if (n > 0) {
    codb_d_unique_(&n, args, NULL, &is_unique, NULL, NULL);
  }
  return is_unique;
}

PRIVATE char *
d2str(double d)
{
  int j;
  const int ns = sizeof(double);
  static union {
    double dval;
    char   s[sizeof(double)+1];
  } u;
  u.dval = d;
  for (j=0; j<ns; j++) {
    int c = u.s[j];
    if (!isprint(c)) u.s[j] = '?';
  }
  u.s[ns] = '\0';
  return u.s;
}


PUBLIC void
ODB_Hash_print(FILE *fp)
{
  (void) ODB_Hashsize();

  if (!fp) fp = stderr;

  if (ODB_hashdebug) {
    int j, nunique = 0;
    int *count;
    CALLOC(count, Hashhits);
    for (j=ODB_hashmin; j<=ODB_hashmax; j++) {
      if (ODB_hashhits[j] < Hashhits) count[ODB_hashhits[j]]++;
      nunique += ODB_hashhits[j];
    }
    fprintf(fp,
	    " *** Hashing statistics : Hashsize = %d, Hashhits = %d\n",
	    Hashsize, Hashhits);
    fprintf(fp,"     (MIN,MAX)-hash = (%d, %d)\n",
	    ODB_hashmin, ODB_hashmax);
    fprintf(fp,"\tNo. of calls = %d\n",ODB_hashcalls);
    fprintf(fp,"\tNo. of unique keys found = %d\n",nunique);
    for (j=0; j<Hashhits; j++) {
      fprintf(fp,"\tCount = %d : hit #%d times\n",j,count[j]);
    }
    FREE(count);
    if (ODB_hashdebug > 1 && ODB_hashcalls > 0) {
      /* More extensive output */
      for (j=ODB_hashmin; j<=ODB_hashmax; j++) {
	ODB_Hash_Table *p = ODB_hashptr[j];
	if (p) fprintf(fp,"\tHash#%d :\n",j);
	while (p) {
	  int k;
	  fprintf(fp,"\tHits=%d, # of values=%d :",p->hits,p->nhashval);
	  for (k=0; k<p->nhashval; k++) {
	    fprintf(fp," %.14g ('%s') ",p->d[k], d2str(p->d[k]));
	  }
	  fprintf(fp,"\n");
	  p = p->next;
	}
      } /* for (j=ODB_hashmin; j<=ODB_hashmax; j++) */
    } /* if (ODB_hashdebug > 1) */
  } /* if (ODB_hashdebug) */
}

PUBLIC boolean
odb_debug_print_(const double arg)
{
  fprintf(stderr,"debug_print: value=%.14g ('%s')\n",arg,d2str(arg));
  return 1;
}
