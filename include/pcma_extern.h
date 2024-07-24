
#ifndef _PCMA_EXTERN_H_
#define _PCMA_EXTERN_H_

/* pcma_extern.h */

#define RMDI   -2147483647
#define NMDI    2147483647

#define MAXSHIFT 16777216U
#define HDRLEN 3

#define MAXPACKINGMETHOD 255

/* hdrlen for the new version, since 14/12/2000 */
#define PCMA_HDRLEN (HDRLEN + 1 + 2*(sizeof(double)/sizeof(int)))

#include "magicwords.h"

extern int pcma_blocksize;
extern int pcma_verbose;
extern int pcma_restore_packed;

extern double pcma_nmdi; /* the default : an integer NMDI =  2147483647   */
extern double pcma_rmdi; /* the default : a real     RMDI = -2147483647.0 */

typedef struct {
  int counter;
  int maxalloc;
  int len;
  unsigned int *p;
  unsigned char allocatable;
} Packbuf;

typedef struct {
  int magic; /* PCMA */
#ifdef LITTLE
  int old_replen : 24;
  int method : 8;
#else
  int method : 8; /* Packing method [0..255] */
  int old_replen : 24; /* Old style report length; [1..16777215] words
			  if zero, use new_replen + nmdi & rmdi present, too */
#endif
  int msgbytes; /* total length of packed message (excluding header), in bytes */
  /* The following are only present if the old_replen is zero */
  unsigned int new_replen; /* New style report length that can now span between [1..2147483647] */
  double nmdi; /* NMDI used (the default = pcma_nmdi) */
  double rmdi; /* RMDI used (the default = pcma_rmdi) */
} Packhdr;

/* Error exit */

extern int
pcma_error(const char *where,
           int how_much,
           const char *what,
           const char *srcfile,
           int srcline,
           int perform_abort);

/* CMA-packing */

extern int
pcma(        FILE *fp_in,
             FILE *fp_out,
              int  method,
     const double  cma[],
              int  lencma,
          Packbuf *pbuf,
              int *bytes_in,
              int *bytes_out);


extern void *
PackDoubles(const double v[],
	    const int nv,
	    int *nbytes);

/* CMA-unpacking */

extern int
upcma(         int  can_swp_data,
              FILE *fp_in,
              FILE *fp_out,
	 const int  idx[], int  idxlen,
	       int  fill_zeroth_cma,
            double  cma[],
               int  lencma,
      unsigned int *hdr_in,
           Packbuf *pbuf,
               int *bytes_in,
               int *bytes_out);

extern int
upcma_hdr(FILE *fp_in, int *swp,
          unsigned int *hdr_in, int read_hdr,
          int *method, int *replen, int *msgbytes,
	  double *nmdi, double *rmdi,
	  int *new_version);

extern int 
upcma_data(FILE *fp_in, const unsigned int *hdr,
	   int new_version,
           unsigned int *packed_data, int count);

void
upcmaTOcma(const          int *can_swp_data,
	   const unsigned int  packed_hdr[],
           const unsigned int  packed_stream[],
	            const int  idx[], int  idxlen,
	                  int  fill_zeroth_cma,
                       double  cma[],
                    const int *lencma,
                          int *retcode);

extern double *
UnPackDoubles(const void *pk,
	      const int nbytes,
	      int *nv);


/* Fortran callable routines */

#ifndef FORTRAN_CALL
#define FORTRAN_CALL extern
#endif

FORTRAN_CALL void
pcma_set_mdis_(const double *nmdi,
	       const double *rmdi);

FORTRAN_CALL void
pcma_get_mdis_(double *nmdi,
	       double *rmdi);

FORTRAN_CALL void 
upcma_info_(const unsigned int  packed_hdr[],
                           int  info[],
                     const int *infolen,
                           int *retcode);

FORTRAN_CALL void
pcma2cma_(const          int *can_swp_data,
	  const unsigned int  packed_data[],
                   const int *packed_len,
	           const int  idx[], const int *idxlen,
	           const int *fill_zeroth_cma,
                      double  cma[],
                   const int *lencma,
                         int *packed_count,
                         int *retcode);

FORTRAN_CALL void
cma2pcma_(const    int *method,
          const double  cma[],
	  const    int *lencma,
	  unsigned int  packed_stream[],
	  const    int *lenpacked_stream,
                   int *bytes_in,
                   int *bytes_out,
	           int *retcode);

FORTRAN_CALL void
vpack_bits_(const int *N_bits,
	    const unsigned int    unpacked[],
	             const int *N_unpacked,
	          unsigned int    packed[],
	             const int *N_packed,
	          int *retcode);

FORTRAN_CALL void
vunpack_bits_(const int *N_bits,
	      const unsigned int    packed[],
	               const int *N_packed,
	            unsigned int    unpacked[],
	               const int *N_unpacked,
	            int *retcode);

FORTRAN_CALL void
lzw_unpack_(const unsigned char ibuf[],
	    const int *Ilen,
	    unsigned char obuf[], 
	    const int *Olen,
	    int *retcode);

FORTRAN_CALL void
lzw_pack_(const unsigned char ibuf[], 
	  const int *Ilen,
	  unsigned char obuf[],
	  const int *Olen,
	  int *retcode);

#endif
