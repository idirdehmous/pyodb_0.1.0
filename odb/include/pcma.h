#ifndef _PCMA_H_
#define _PCMA_H_

#include <stdio.h>
#include <string.h>
#ifdef USE_MALLOC_H
#include <malloc.h>
#endif
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <math.h>

#include "alloc.h"
#include "swapbytes.h"
#include "pcma_extern.h"
#include "cdrhook.h"

#define PCMA_NEW_VERSION(h) ((*(h+1))%MAXSHIFT == 0) ? (PCMA_HDRLEN-HDRLEN) : 0

/* Change the following every time you add a new method to the look_up[]
   Also keep track which is/has the max at any given moment by moving around
   the comment "The current MAXPACKFUNC minus 1" */

#define MAXPACKFUNC 10

#if defined(PCMA_C) || defined(UPCMA_C)
#define BAD_IDX 0x8B8B8B8B
static const int look_up[1+MAXPACKINGMETHOD /* packing_methods [1..255] ; including zero i.e. no packing */] = 
{
  BAD_IDX, /* method#0 : undefined (since means no packing) */

  0, /* method#1 */
  1, /* method#2 */
  2, /* method#3 */

  7, /* method#4 */

  3, /* method#5 */

  BAD_IDX, /* method#6 : undefined */
  BAD_IDX, /* method#7 : undefined */
  BAD_IDX, /* method#8 : undefined */

  4, /* method#9 */

  BAD_IDX, /* method#10 : undefined */

  /* methods #11..19 map to the same method routine ; significant digits => 10 + sign_digits */

  5, 5, 5, 5, 5, 5, 5, 5, 5,

  /* methods #20..100 */

  BAD_IDX, /* method#20, undefined */

  /* methods #21..29 map to the same method routine ; significant digits => 20 + sign_digits ;
     10^exponent is saved separately; a merger of method#1 & method#11..19 for unnormalized
     reals */

           6, 6, 6, 6, 6, 6, 6, 6, 6,

  /* method #30 : undefined */

  BAD_IDX,

  /* methods #31..39 : Similar to #21..29, but retain full accuracy of 4-byte integers */

           9, 9, 9, 9, 9, 9, 9, 9, 9,    /* The current MAXPACKFUNC minus 1 */

  /* methods #40..93 */

  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, 

  /* method #94 (pseudo) */

                             8,

  /* methods #95..100 : undefined  */

                                      BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,

  /* methods #101..200  : undefined */

  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,

  /* methods #201..254 : undefined */

  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX,
  BAD_IDX, BAD_IDX, BAD_IDX, BAD_IDX, 

  /* method#255  : undefined */

  BAD_IDX
};
#undef BAD_IDX
#endif

#if defined(VPP) || defined(NECSX)

static const unsigned int pwr2[32+1] = 
{ 
           1U,            2U,            4U,            8U,
          16U,           32U,           64U,          128U,
         256U,          512U,         1024U,         2048U,
        4096U,         8192U,        16384U,        32768U,
       65536U,       131072U,       262144U,       524288U,
     1048576U,      2097152U,      4194304U,      8388608U,
    16777216U,     33554432U,     67108864U,    134217728U,
   268435456U,    536870912U,   1073741824U,   2147483648U,
           0U,
};

#define PWR2(shift) (pwr2[shift])
#else
#define PWR2(shift) (1<<(shift))
#endif

#define N32BITS 32

#define b1   1
#define b2   2
#define b4   4
#define b8   8
#define b10 10
#define b16 16
#define b32 32

#define MINBLOCK     6144U
#define MAXBLOCK (MAXSHIFT-1U)

#define MINLEN ((int)((HDRLEN+1)*sizeof(unsigned int)))

#define ZERO  0

#define SWAP_DATA_BACK(swp,can_swap_data,cma,lencma) \
if (swp && !can_swp_data && lencma > 1) { /* swap data back */ \
  int len = lencma - 1; \
  swap8bytes_(&cma[fill_zeroth_cma], &len); \
}

#define SWAP_DATA_BACKv2(swp,can_swap_data,cma,stacma,endcma) \
if (swp && !can_swp_data && endcma-stacma > 0) { /* swap data back, vers#2 */ \
  int len = endcma-stacma; \
  swap8bytes_(&cma[fill_zeroth_cma+stacma], &len); \
}

#define STAEND_FIX() \
    if (idx && idxlen > 0) { \
      int j; \
      int minidx = chunk+1; \
      int maxidx = 0; \
      for (j=0; j<idxlen; j++) { \
	if (idx[j] < minidx) minidx = idx[j]; \
	if (idx[j] > maxidx) maxidx = idx[j]; \
      } \
      stacma = minidx; \
      stacma = MAX(0,stacma); \
      endcma = maxidx+1; \
      endcma = MIN(chunk,endcma); \
    }

#define END_FIX() \
    if (idx && idxlen > 0) { \
      int j; \
      int maxidx = 0; \
      for (j=0; j<idxlen; j++) { \
	if (idx[j] > maxidx) maxidx = idx[j]; \
      } \
      endcma = maxidx+1; \
      endcma = MIN(chunk,endcma); \
    }

#define FCLOSE(fp)        if (fp) { fclose(fp); fp = NULL; }

extern int
pcma_1_driver(          int method,
	              FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	            double  nmdi,
	            double  rmdi,
	           Packbuf *pbuf);

extern int
pcma_2_driver(          int method,
	              FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	            double  nmdi,
	            double  rmdi,
	           Packbuf *pbuf);

extern int
pcma_3_driver(          int method,
	              FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	            double  nmdi,
	            double  rmdi,
	           Packbuf *pbuf);

extern int
pcma_4_driver(          int method,
	              FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	            double  nmdi,
	            double  rmdi,
	           Packbuf *pbuf);

extern int
pcma_5_driver(          int method,
	              FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	            double  nmdi,
	            double  rmdi,
	           Packbuf *pbuf);

extern int
pcma_9_driver(          int method,
	              FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	            double  nmdi,
	            double  rmdi,
	           Packbuf *pbuf);

extern int
pcma_11to19_driver(      int method,
	              FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	            double  nmdi,
	            double  rmdi,
	           Packbuf *pbuf);

extern int
pcma_21to29_driver(     int method,
	              FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	            double  nmdi,
	            double  rmdi,
	           Packbuf *pbuf);

extern int
pcma_31to39_driver(     int method,
	              FILE *fp_out,
	      const double  cma[],
                       int  lencma,
	            double  nmdi,
	            double  rmdi,
	           Packbuf *pbuf);

extern int
upcma_1_driver(int method,
	       int swp, int can_swp_data,
	       int new_version,
	       const unsigned int  packed_data[],
	                      int  len_packed_data,	
	                      int  msgbytes,
	                   double  nmdi,
	                   double  rmdi,
                	     FILE *fp_out, 
	                const int  idx[], int  idxlen,
              	              int  fill_zeroth_cma,
	                   double  cma[], int lencma);

extern int
upcma_2_driver(int method,
	       int swp, int can_swp_data,
	       int new_version,
	       const unsigned int  packed_data[],
	                      int  len_packed_data,	
	                      int  msgbytes,
	                   double  nmdi,
	                   double  rmdi,
                	     FILE *fp_out, 
	                const int  idx[], int  idxlen,
              	              int  fill_zeroth_cma,
	                   double  cma[], int lencma);

extern int
upcma_3_driver(int method,
	       int swp, int can_swp_data,
	       int new_version,
	       const unsigned int  packed_data[],
   	                      int  len_packed_data,	
	                      int  msgbytes,
	                   double  nmdi,
	                   double  rmdi,
	                     FILE *fp_out, 
	                const int  idx[], int  idxlen,
              	              int  fill_zeroth_cma,
	                   double  cma[], int lencma);

extern int
upcma_4_driver(int method,
	       int swp, int can_swp_data,
	       int new_version,
	       const unsigned int  packed_data[],
   	                      int  len_packed_data,	
	                      int  msgbytes,
	                   double  nmdi,
	                   double  rmdi,
	                     FILE *fp_out, 
	                const int  idx[], int  idxlen,
              	              int  fill_zeroth_cma,
	                   double  cma[], int lencma);

extern int
upcma_5_driver(int method,
	       int swp, int can_swp_data,
	       int new_version,
	       const unsigned int  packed_data[],
	                      int  len_packed_data,	
	                      int  msgbytes,
	                   double  nmdi,
	                   double  rmdi,
                	     FILE *fp_out, 
	                const int  idx[], int  idxlen,
              	              int  fill_zeroth_cma,
	                   double  cma[], int lencma);

extern int
upcma_9_driver(int method,
	       int swp, int can_swp_data,
	       int new_version,
	       const unsigned int  packed_data[],
	                      int  len_packed_data,	
	                      int  msgbytes,
	                   double  nmdi,
	                   double  rmdi,
                	     FILE *fp_out, 
	                const int  idx[], int  idxlen,
              	              int  fill_zeroth_cma,
	                   double  cma[], int lencma);

extern int
upcma_11to19_driver(int method,
		    int swp, int can_swp_data,
		    int new_version,
		    const unsigned int  packed_data[],
		                   int  len_packed_data,	
		                   int  msgbytes,
		                double  nmdi,
                                double  rmdi,
		                  FILE *fp_out, 
		             const int  idx[], int  idxlen,
		                   int  fill_zeroth_cma,
		                double  cma[], int lencma);

extern int
upcma_21to29_driver(int method,
		    int swp, int can_swp_data,
		    int new_version,
		    const unsigned int  packed_data[],
		                   int  len_packed_data,	
		                   int  msgbytes,
		                double  nmdi,
		                double  rmdi,
		                  FILE *fp_out, 
		             const int  idx[], int  idxlen,
		                   int  fill_zeroth_cma,
		                double  cma[], int lencma);

extern int
upcma_31to39_driver(int method,
		    int swp, int can_swp_data,
		    int new_version,
		    const unsigned int  packed_data[],
		                   int  len_packed_data,	
		                   int  msgbytes,
		                double  nmdi,
		                double  rmdi,
		                  FILE *fp_out, 
		             const int  idx[], int  idxlen,
		                   int  fill_zeroth_cma,
		                double  cma[], int lencma);

/* Utilities */

extern void 
pcma_zero_uint(unsigned int u[], int n);

extern void 
pcma_copy_uint(unsigned int to[], const unsigned int from[], int n);

extern void 
pcma_copy_dbl(double to[], const double from[], int n);

extern unsigned int *
pcma_alloc(Packbuf *pbuf, int count);

extern int 
pcma_filesize(const char *path);

extern FILE *
pcma_prealloc(const char *filename,
	      int         prealloc,
	      int         extent,
	      int        *blksize,
	      int        *retcode);

#endif
