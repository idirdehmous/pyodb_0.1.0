
/* orlist.c */

#include "odb.h"
#include "odbcrc.h"
#include "pcma_extern.h"

PUBLIC double
ODB_maxval(const int n, ...)
{
  const double mdi = ABS(RMDI);
  double dmax = mdi;
  va_list ap;
  va_start(ap, n);
  if (n > 0) { 
    double d = mdi;
    int jj=0;
    while (jj<n) {
      d = va_arg(ap, double);
      if (ABS(d) != mdi) break;
      jj++;
    }
    if (jj<n) {
      int j;
      dmax = d;
      for (j=jj+1; j<n; j++) {
	d = va_arg(ap, double);
	if (ABS(d) != mdi && dmax < d) dmax = d;
      }
    }
  } /* if (n > 0) */
  va_end(ap);
  return dmax;
}

PUBLIC double
ODB_minval(const int n, ...)
{
  const double mdi = ABS(RMDI);
  double dmin = mdi;
  va_list ap;
  va_start(ap, n);
  if (n > 0) { 
    double d = mdi;
    int jj=0;
    while (jj<n) {
      d = va_arg(ap, double);
      if (ABS(d) != mdi) break;
      jj++;
    }
    if (jj<n) {
      int j;
      dmin = d;
      for (j=jj+1; j<n; j++) {
	d = va_arg(ap, double);
	if (ABS(d) != mdi && dmin > d) dmin = d;
      }
    }
  } /* if (n > 0) */
  va_end(ap);
  return dmin;
}

PUBLIC double
ODB_Cksum32(const int n, ...)
{
  unsigned int crc32 = 0;
  int keylen = n * sizeof(double);
  if (n > 0) {
    int j;
    va_list ap;
    va_start(ap, n);
    for (j=0; j<n; j++) {
      double d = va_arg(ap, double);
      crc32 = ODB_cksum32((const char *)&d, sizeof(double), crc32);
    }
    va_end(ap);
  } /* if (n > 0) */
  crc32 = ODB_pp_cksum32(keylen, crc32);
  return (double)crc32;
}

typedef struct _thin_info_t {
  int cnt;
  int last_taken;
  double key;
  double first_value;
  double second_value;
  unsigned int datakey;
  struct _thin_info_t *next;
} ThinInfo_t;


static ThinInfo_t **thinfo = NULL;
static int inumt = 0;

PUBLIC void
codb_thin_init_()
{
  if (!thinfo) {
    inumt = get_max_threads_();
    CALLOC(thinfo, inumt);
  }
}

PUBLIC void
codb_thin_reset_(const int *It)
{
  if (!thinfo) codb_thin_init_(); /* Not thread-safe, but normally not called */
  if (It) {
    int it = *It;
    if (it >= 1 && it <= inumt) {
      ThinInfo_t *p = thinfo[--it];
      while (p) {
	p->cnt = 0;
	p = p->next;
      }
    }
  }
}

PRIVATE ThinInfo_t *
find_thinfo(const double key, int it)
{
  ThinInfo_t *p, *plast;
  if (!thinfo) codb_thin_init_(); /* Not thread-safe, but normally not called */
  p = plast = thinfo[--it];
  while (p) {
    plast = p;
    if (p->cnt > 0 && p->key == key) break;
    if (p->cnt == 0) break;
    p = p->next;
  }
  if (!p) {
    CALLOC(p, 1);
    if (!thinfo[it]) thinfo[it] = p;
    if (plast) plast->next = p; /* chain */
  }
  return p;
}

PUBLIC double
ODB_vthin(const int N, const double d[])
{
  boolean take_this;
  int every_nth;
  double key = 0;
  double Every_nth = 0;
  int n = N;
  if (n >= 2) {
    key = d[0];
    Every_nth = d[1];
  }
  n -= 2;
  every_nth = (n >= 0 && Every_nth >= 0 && Every_nth <= INT_MAX) ? (int)Every_nth : 0;
  take_this = (n >= 0 && every_nth >= 1) ? 1 : 0;
  if (n >= 0 && every_nth > 1) {
    DEF_IT;
    ThinInfo_t *th = find_thinfo(key, it);
    unsigned int datakey = n;
    double first_value = 0;
    double second_value = 0;

    if (n > 0) {
      int j, keylen = 0;
      if (n == 1) {
	first_value = d[2];
      }
      else if (n == 2) {
	first_value = d[3];
	second_value = d[4];
      }
      else { /* n > 2 */
	for (j=0; j<n; j++) {
	  double dd = d[2+j];
	  if (j==0) first_value = dd;
	  else if (j==1) second_value = dd;
	  datakey = ODB_cksum32((const char *)&dd, sizeof(dd), datakey);
	  keylen += sizeof(dd);
	}
	datakey = ODB_pp_cksum32(keylen, datakey);
      }
    }

    if (th->cnt == 0) { /* Start "recording" */
      (th->cnt)++;
      th->last_taken = 1;
      th->key = key;
      th->first_value = first_value;
      th->second_value = second_value;
      th->datakey = datakey;
      take_this = 1;
    }
    else if (th->last_taken && 
	     datakey == th->datakey && 
	     first_value == th->first_value &&
	     second_value == th->second_value) {
      /* Take only values that are consecutively the same ; do not increment counter */
      take_this = 1;
    }
    else {
      (th->cnt)++;
      if ((th->cnt)%every_nth == 1) {
	th->last_taken = 1;
	th->first_value = first_value;
	th->second_value = second_value;
	th->datakey = datakey;
	take_this = 1;
      }
      else {
	th->last_taken = 0; /* Breaks "consecutiveness" */
	take_this = 0;
      }
    }
  }
  return (double)take_this;
}

PUBLIC double
ODB_thin(const int n, /* double key, double Every_nth, */...)
{
  double take_this = 0;
  va_list ap;
  va_start(ap, n);
  if (n > 0) {
    int j;
    double *d = NULL;
    ALLOCX(d,n);
    for (j=0; j<n; j++) {
      d[j] = va_arg(ap, double);
    }
    take_this = ODB_vthin(n, d);
    FREEX(d);
  }
  va_end(ap);
  return take_this;
}


typedef struct _In_Vector_t {
  double *d;
  int *i;
  int nd;
} In_Vector_t;


typedef union _Union_Vector_t {
  double addr;
  In_Vector_t *iv;
} Union_Vector_t;


void fodb_register_in_vector_(void *d, const int *nd, const int *is_int, double *addr)
{
  if (d && nd && *nd > 0 && is_int && addr) {
    boolean IsInt = (*is_int != 0) ? 1 : 0;
    Union_Vector_t uv;
    In_Vector_t *iv = NULL;
    CALLOC(iv, 1);
    iv->nd = *nd;
    if (IsInt) {
      iv->d = NULL;
      iv->i = d;
    }
    else {
      iv->d = d;
      iv->i = NULL;
    }
    uv.iv = iv;
    *addr = uv.addr;
  }
  else if (addr) {
    *addr = 0;
  }
}

void fodb_unregister_in_vector_(double *addr)
{
  if (addr) {
    Union_Vector_t uv;
    uv.addr = *addr;
    if ( uv.iv != (In_Vector_t *) NULL ) {
      In_Vector_t *iv = NULL;
      uv.addr = *addr;
      iv = uv.iv;
      FREE(iv);
      *addr = 0;
    }
  }
}

double ODB_in_vector(double var, double addr)
{
  double rc = 0; /* false */
  Union_Vector_t uv;
  uv.addr = addr;
  if (uv.iv != (In_Vector_t *) NULL) {
    In_Vector_t *iv = NULL;
    iv = uv.iv;
    if (iv && (iv->d || iv->i) && iv->nd > 0) {
      int j, nd = iv->nd;
      double *d = iv->d;
      int *i = iv->i;
      if (d) {
	for (j=0; j<nd; j++) {
	  if (d[j] == var) {
	    rc = 1; /* true */
	    break;
	  }
	}
      }
      else {
	int ivar = var;
	for (j=0; j<nd; j++) {
	  if (i[j] == ivar) {
	    rc = 1; /* true */
	    break;
	  }
	}
      }
    } /* if (iv && (iv->d || iv->i) && iv->nd > 0) */
  }
  return rc;
}
