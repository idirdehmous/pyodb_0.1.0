#include <stdio.h>
#include <signal.h>
#include <math.h>

#include "swapbytes.h"

PUBLIC void
swap1bytes_(void *v, const int *vlen)
{
  /* does nothing; for convenience */
}

PUBLIC void
swap2bytes_(void *v, const int *vlen)
{
  unsigned short int *u = v;
  int j,ulen = vlen ? *vlen : 0;
  for (j=0; j<ulen; j++) {
    u[j] = bswap16bits(u[j]);
  }
}

PUBLIC void
swap2bytes_via_copy_(void *v, const void *vin, const int *vlen)
{
  unsigned short int *u = v;
  const unsigned short int *uin = vin;
  int j,ulen = vlen ? *vlen : 0;
  for (j=0; j<ulen; j++) {
    u[j] = bswap16bits(uin[j]);
  }
}

PUBLIC void
swap4bytes_(void *v, const int *vlen)
{
  unsigned int *u = v;
  int j,ulen = vlen ? *vlen : 0;
  for (j=0; j<ulen; j++) {
    u[j] = bswap32bits(u[j]);
  }
}

PUBLIC void
swap4bytes_via_copy_(void *v, const void *vin, const int *vlen)
{
  unsigned int *u = v;
  const unsigned int *uin = vin;
  int j,ulen = vlen ? *vlen : 0;
  for (j=0; j<ulen; j++) {
    u[j] = bswap32bits(uin[j]);
  }
}

PUBLIC void
swap8bytes_(void *v, const int *vlen)
{
  u_ll_t *u = v;
  int j,ulen = vlen ? *vlen : 0;
  for (j=0; j<ulen; j++) {
    u[j] = bswap64bits(u[j]);
  }
}

PUBLIC void
swap8bytes_via_copy_(void *v, const void *vin, const int *vlen)
{
  u_ll_t *u = v;
  const u_ll_t *uin = vin;
  int j,ulen = vlen ? *vlen : 0;
  for (j=0; j<ulen; j++) {
    u[j] = bswap64bits(uin[j]);
  }
}

PUBLIC void
swapANYbytes(void *v, int elsize, int n, unsigned int dtype, 
	     const char *dtype_name, const char *varname)
{
  union {
    odb_types_t t;
    unsigned int ui;
  } u;
  u.ui = (dtype << 8); /* shift over the pmethod-member */
#ifdef DEBUG
  fprintf(stderr,
	  "swapANYbytes(%s:%s): elsize=%d, n=%d, dtype=%u (0x%x)\n",
	  dtype_name, varname, elsize, n, dtype, dtype);
  fprintf(stderr,
	  "\tpmethod=%u,sign=%u,bswaple=%u,prec=%u,base=%u,other=%u (0x%x)\n",
	  u.t.pmethod,u.t.signbit,u.t.byte_swappable,u.t.precision_bits,
	  u.t.base_type,u.t.other_type);
#endif
  if (u.t.byte_swappable) {
    double bits = (1 << u.t.precision_bits); /* pow(2,u.t.precision_bits); */
#ifdef DEBUG
    fprintf(stderr,"\tbits=%.20g := 2^%u\n",bits,u.t.precision_bits);
#endif
    if (bits == 16 && elsize == 2) {
      swap2bytes_(v, &n);
    }
    else if (bits == 32 && elsize == 4) {
      swap4bytes_(v, &n);
    }
    else if (bits == 64 && elsize == 8) {
      swap8bytes_(v, &n);
    }
    else {
      fprintf(stderr,
	      " ***Error in swapANYbytes() [elsize=%d]: Can't swap bytes for dtype=0x%x (type=%s, var='%s')\n",
	      elsize, dtype, dtype_name, varname);
      fprintf(stderr,
	      "signbit=%u, byte_swappable=%u, precision_bits=%u, base_type=%u, other_type=0x%x (%u)\n",
	      u.t.signbit, u.t.byte_swappable, u.t.precision_bits,
	      u.t.base_type, u.t.other_type, u.t.other_type);
      RAISE(SIGABRT);
    }
  }
}

