#include "odb.h"
#include "cmaio.h"

/* Fortran-interface to convert logical (CMAIO) name
   into physical filename, with any possible environment 
   variables resolved */

void 
codb_truename_(const char *in,
	             char *out,
		     int  *retcode,
	       /* Hidden arguments */
	             int  in_len,
	       const int  out_len)
{
  int rc = 0;

  {
    char *s;
    DECL_FTN_CHAR(out);

    ALLOC_OUTPUT_FTN_CHAR(out);

    s = IOtruename(in, &in_len);
    rc = strlen(s);
    rc = MIN(rc, out_len);
    strncpy(p_out, s, rc);
    p_out[rc] = '\0'; /* Don't worry: p_out always +1 char longer than out */
    FREE(s);

    COPY_2_FTN_CHAR(out);
    FREE_FTN_CHAR(out);
  }

  *retcode = rc;
}
