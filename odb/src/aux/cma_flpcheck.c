#ifdef RS6K
#pragma options noextchk
#endif

#include "odb.h"
#include "cmaio.h"
#include <math.h>

/* 
   cma_flpcheck.c: Checks 64-bit IEEE flp validity.

   Mainly for CMA-data checking

   Author: Sami Saarinen, ECMWF, 03-Nov-1998
             -"-           -"-   13-Mar-2001 : little-endian version squeezed in
             -"-           -"-   28-May-2002 : ODB-flpcheck wrapper around cma_flpcheck added
             -"-           -"-   30-May-2002 : Suspected integer overflow & underflow checker on doubles
             -"-           -"-   12-Jul-2006 : includes also "odb.h" => uses trunc()-fix from "privpub.h"


 */

/* Both big and little endian machines supported now */

#define SIGN(x)  ( ((x) & 0x80000000) >> 31 )
#define EXPO(x)  ( ((x) & 0x7ff00000) >> 20 )
#define M1(x)    ( ((x) & 0x000fffff) )
#define M2(x)    (x)

#ifdef LITTLE
#define WORD_1_OFFSET 1
#define WORD_2_OFFSET 0 
#else
#define WORD_1_OFFSET 0
#define WORD_2_OFFSET 1
#endif

typedef unsigned int Uint;

typedef union _Flp_t {
  double d;
  Uint   u[2];
#ifdef LITTLE
  struct {
    Uint m2   : 32;
    Uint m1   : 20;
    Uint exp  : 11;
    Uint sign :  1;
  } s;
#else
  struct {
    Uint sign :  1;
    Uint exp  : 11;
    Uint m1   : 20;
    Uint m2   : 32;
  } s;
#endif
} Flp_t;

PRIVATE void
any_endian_version(const Uint  u[],
		          int  flag[],
		          int  n,
		          int *rinf,
		          int *rtiny,
		          int *rnan)
{
  int i, j;
  Uint expo, m1, m2;
  int R1 = 0;
  int R2 = 0;
  int R3 = 0;
  static int first_time = 1;
  static int cmaflp_check_Inf = 1;
#ifdef VPP5000
  static int cmaflp_check_Tiny = 0; /* Off for VPP5000 */
#else
  static int cmaflp_check_Tiny = 1;
#endif
  static int cmaflp_check_NaN = 1;

  if (first_time) {
    int ienv;
    char *env;
    env = getenv("CMAFLP_CHECK_INF");
    if (env) {
      ienv = atoi(env);
      cmaflp_check_Inf = MAX(0,ienv);
    }
    env = getenv("CMAFLP_CHECK_TINY");
    if (env) {
      ienv = atoi(env);
      cmaflp_check_Tiny = MAX(0,ienv);
    }
    env = getenv("CMAFLP_CHECK_NAN");
    if (env) {
      ienv = atoi(env);
      cmaflp_check_NaN = MAX(0,ienv);
    }
    first_time = 0;
  }

  if (flag) {
#ifdef VPP
#pragma loop noalias
#elif defined(NECSX)
#pragma cdir nodep
#endif
    for (j=0; j<n; j++) {
      i = 2 * j;
      expo = EXPO(u[i+WORD_1_OFFSET]);
      m1   = M1(u[i+WORD_1_OFFSET]);
      m2   = M2(u[i+WORD_2_OFFSET]);
	
      if (cmaflp_check_Inf && expo == 0x7ff && m1 == 0 && m2 == 0)  {
	/* Infinite */
	R1++;
	flag[j] = 1;
      }
      else if (cmaflp_check_Tiny && expo == 0 && (m1 > 0 || m2 > 0)) {
	/* Too tiny */
	R2++; 
	flag[j] = 2;
      }
      else if (cmaflp_check_NaN && expo == 0x7ff) {
	/* NaN */
	R3++; 
	flag[j] = 3;
      }
      else
	flag[j] = 0;
    }
  }
  else {
#ifdef VPP
#pragma loop noalias
#elif defined(NECSX)
#pragma cdir nodep
#endif
    for (j=0; j<n; j++) {
      i = 2 * j;
      expo = EXPO(u[i+WORD_1_OFFSET]);
      m1   = M1(u[i+WORD_1_OFFSET]);
      m2   = M2(u[i+WORD_2_OFFSET]);
	
      if (cmaflp_check_Inf && expo == 0x7ff && m1 == 0 && m2 == 0)  {
	/* Infinite */
	R1++;
      }
      else if (cmaflp_check_Tiny && expo == 0 && (m1 > 0 || m2 > 0)) {
	/* Too tiny */
	R2++; 
      }
      else if (cmaflp_check_NaN && expo == 0x7ff) {
	/* NaN */
	R3++; 
      }
    }
  }

  /* Spit out counts of invalid discoveries */

  if (rinf)  *rinf  = cmaflp_check_Inf  ? R1 : 0;
  if (rtiny) *rtiny = cmaflp_check_Tiny ? R2 : 0;
  if (rnan)  *rnan  = cmaflp_check_NaN  ? R3 : 0;
}

void
cma_flpcheck_(const double d[],
	      const   int *nd,
	              int  flag[],
	      const   int *nf,
	              int *rinf,
	              int *rtiny,
	              int *rnan)
{
  int ND = *nd;
  int NF = *nf;
  any_endian_version((const Uint *)d,
		     (flag && (NF >= ND)) ? flag : NULL,
		     ND,
		     rinf, rtiny, rnan);
}

void
odb_flpcheck_(const double d[],
	      const   int *nd,
	              int  flag[],
	      const   int *nf,
	              int *rinf,
	              int *rtiny,
	              int *rnan)
{
  static int first_time = 1;
  static int do_test = 0;

  if (rinf)  *rinf  = 0;
  if (rtiny) *rtiny = 0;
  if (rnan)  *rnan  = 0;

  if (first_time) {
    char *env = getenv("ODB_FLPCHECK");
    if (env) {
      int myproc = 0;
      int value = atoi(env);
      codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
      do_test = (value == -1 || value == myproc) ? 1 : 0;
      if (do_test) {
	fprintf(stderr,
		"***Warning from odb_flpcheck_(): Floating point checks will be performed on PE#%d\n",
		myproc);
      }
    }
    first_time = 0;
  }

  if (do_test) {
    cma_flpcheck_(d, nd, flag, nf, rinf, rtiny, rnan);
  }
}

void
odb_intcheck_(const double d[],
	      const   int *nd,
	      const   int *test_unsigned_only,
	              int  flag[],
	      const   int *nf,
	              int *nlo,
	              int *nhi)
{
  static int first_time = 1;
  static int do_test = 0;
  int NLO = 0;
  int NHI = 0;

  if (first_time) {
    char *env = getenv("ODB_INTCHECK");
    if (env) {
      int myproc = 0;
      int value = atoi(env);
      codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
      do_test = (value == -1 || value == myproc) ? 1 : 0;
      if (do_test) {
	fprintf(stderr,
		"***Warning from odb_intcheck_(): Integer checks will be performed on PE#%d\n",
		myproc);
      }
    }
    first_time = 0;
  }

  if (do_test) {
    double minimum, maximum;
    int j;
    int ND = *nd;
    int NF = *nf;
    int UNSIGN = *test_unsigned_only;

    if (UNSIGN) { /* test in unsigned sense only */
      minimum =           0U;
      maximum =  4294967295U;
    }
    else {
      minimum = -2147483647;
      maximum =  2147483647;
    }

    if (flag && NF >= ND) {
      for (j=0; j<ND; j++) {
	double trc = trunc(d[j]);
	int is_integer = (d[j] == trc);
	int flg = 0;
	if (is_integer && trc < minimum) { NLO++; flg = -1; }
	else if (is_integer && trc > maximum) { NHI++; flg = +1; }
	else flg = 0;
	flag[j] = flg;
      } /* for (j=0; j<ND; j++) */
    }
    else { /* no flag vector present or it is too short */
      for (j=0; j<ND; j++) {
	double trc = trunc(d[j]);
	int is_integer = (d[j] == trc);
	if (is_integer && trc < minimum) NLO++;
	else if (is_integer && trc > maximum) NHI++;
      } /* for (j=0; j<ND; j++) */
    } /* if (flag && NF >= ND) ... else */
  }

  if (nlo) *nlo = NLO;
  if (nhi) *nhi = NHI;
}

#ifndef NFLAGLEN
#define NFLAGLEN 2048
#endif

PRIVATE int cmaflp_check = -1;
PRIVATE int cmaflp_iounit = 0;
PRIVATE int cmaflp_ignore_error = 0;
PRIVATE int cmaflp_dump_max = -1;
PRIVATE int cmaflp_dump_only_invalid = 0;

void
CMA_flpchecker(const char *name,
	       int mode, /* 1=read, 2=write */
	       const char *filename,
	       const double pbuf[],
	       int filepos,
	       int *retcode,
	       Boolean *check_it)
{
  int rc = *retcode;

  if (cmaflp_check == -1) {
    int ienv;
    char *env = getenv("CMAFLP_CHECK");
    if (env) {
      ienv = atoi(env);
      cmaflp_check = MAX(0,ienv);
    }
    else
      cmaflp_check = 0;
    
    if (cmaflp_check > 0) {
      env = getenv("CMAFLP_IOUNIT");
      if (env) {
	ienv = atoi(env);
	cmaflp_iounit = MAX(0,ienv);
      }
      
      env = getenv("CMAFLP_IGNORE_ERROR");
      if (env) {
	ienv = atoi(env);
	cmaflp_ignore_error = (ienv != 0);
      }
      
      env = getenv("CMAFLP_DUMP_MAX");
      if (env) {
	ienv = atoi(env);
	cmaflp_dump_max = MAX(-1, ienv);
      }

      env = getenv("CMAFLP_DUMP_ONLY_INVALID");
      if (env) {
	ienv = atoi(env);
	cmaflp_dump_only_invalid = MAX(0, ienv);
      }
    }

    *check_it = (cmaflp_check & mode) > 0 ? true : false;
  }

  if (rc > 0 && (cmaflp_check & mode) > 0) {
    int flag[NFLAGLEN];
    int nd = rc;
    int nf = (NFLAGLEN >= nd) ? nd : 0;
    int rinf, rtiny, rnan;
    int NULOUT  = cmaflp_iounit;
    int ONLYINV = cmaflp_dump_only_invalid;
    int MAXDUMP = MIN(cmaflp_dump_max, nd);
    if (MAXDUMP == -1) MAXDUMP = nd;
    
    cma_flpcheck_(pbuf, &nd, 
		  flag, &nf,
		  &rinf, &rtiny, &rnan);
    
    if (rnan > 0 || rinf > 0 || rtiny > 0) {
      extern void cma_flperr_(
	  const int *io, const int *onlyinv, const int *maxdump, const int *mode,
	  const char *name, const char *filename,
	  const double d[], const int *nd,
	  const int flag[], const int *nf,
	  const int *woff, 
	  const int *inf, const int *itiny, const int *inan
	  /* Hidden arguments */
	  , int name_len, int filename_len);

      int word_offset = 
	(filepos - nd * sizeof(*pbuf)) / sizeof(*pbuf);

      cma_flperr_(&NULOUT, &ONLYINV, &MAXDUMP, &mode,
		  name, filename,
		  pbuf, &nd,
		  flag, &nf,
		  &word_offset,
		  &rinf, &rtiny, &rnan,
		  /* Hidden arguments to Fortran-program */
		  strlen(name), strlen(filename));

      if (!cmaflp_ignore_error) {
	/* Error : Invalid floating point value(s) were detected */
	rc = -7;
      }
    }
  }

  *retcode = rc;
}
