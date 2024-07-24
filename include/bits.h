#ifndef _BITS_H_
#define _BITS_H_

/* bits.h */

/* Fortran callables */

extern void
codbit_get_count_(const void *data,
		  const int *nbits,
		  const int *nbw,
		  int *rc);

extern void 
codbit_getidx_(const void *data, 
	       const int *nbits,
	       const int *nbw,
	       unsigned int idx[], /* bit numbers [0 .. *nbits - 1] that are set to 1 */
	       const int *nidx, /* length of idx[], potentially >= *nbits */
	       int *rc); /* no. of bits set to 1 */

extern void
codbit_get_(const void *data, 
	    const int *nbits,
	    const int *nbw,
	    const int *bitno, /* C-indexing : between [0 .. *nbits - 1] */
	    int *rc);

extern void
codbit_set_(void *data, 
	    const int *nbits,
	    const int *nbw,
	    const int *bitno1,
	    const int *bitno2); /* C-indexing : between [0 .. *nbits - 1] */

extern void 
codbit_setidx_(void *data, 
	       const int *nbits,
	       const int *nbw,
	       const unsigned int idx[], /* bit numbers [0 .. *nbits - 1] that will be set to 1 */
	       const int *nidx); /* length of idx[], potentially >= *nbits */

extern void
codbit_unset_(void *data, 
	      const int *nbits,
	      const int *nbw,
	      const int *bitno1,
	      const int *bitno2); /* C-indexing : between [0 .. *nbits - 1] */

extern void 
codbit_unsetidx_(void *data, 
		 const int *nbits,
		 const int *nbw,
		 const unsigned int idx[], /* bit numbers [0 .. *nbits - 1] that will be set to 0 */
		 const int *nidx); /* length of idx[], potentially >= *nbits */

extern void 
codbit_and_(void *out_data, 
	    const void *in_data,
	    const int *nbits,
	    const int *nbw);

extern void 
codbit_or_(void *out_data, 
	   const void *in_data,
	   const int *nbits,
	   const int *nbw);

/* Non-Fortran callables */

extern int
ODBIT_get_count(const void *data, int nbits, int nbw);

extern unsigned int *
ODBIT_getidx(const void *data, int nbits, int nbw, int *idxlen);

extern void
ODBIT_free(void *v);

extern char *
ODBIT_getmap(const void *data, int nbits, int nbw, char *s);

extern int
ODBIT_get(const void *data, int nbits, int nbw, int bitno);

extern int
ODBIT_test(const void *data, int nbits, int nbw, int bitno1, int bitno2);

extern void
ODBIT_set(void *data, int nbits, int nbw, int bitno1, int bitno2);

extern void
ODBIT_unset(void *data, int nbits, int nbw, int bitno1, int bitno2);

#endif
