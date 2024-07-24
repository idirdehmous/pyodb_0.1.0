#include "cmaio.h"

FORTRAN_CALL void
cma_get_report_(const integer4 *unit,
		real8           pbuf[],
		const integer4 *kbufsize,
		integer4       *retcode)
{
  int rc = 0;
  /* int index = IOINDEX(*unit); */
  integer4 retc;
  integer4 len;
  real8 report_len;
  integer4 bufsize = *kbufsize;

  if (iostuff_debug) fprintf(stderr,"cma_get_report(%d)\n",*unit);

  len = 1;
  if (len > bufsize) {
    /* Error : Buffer size provided is too short */
    rc = -5;
    goto finish;
  }

  cma_read_(unit, &report_len, &len, &retc);

  if (retc != len) {
    /* Error */
    rc = retc;
    goto finish;
  }

  if (iostuff_debug)
    fprintf(stderr," : replen=%.2f, bufsize=%d\n",
	    report_len, bufsize);

  len = report_len - 1;
  if (len > bufsize - 1) {
    /* Error : Buffer size provided is too short */
    rc = -len;
    goto finish;
  }

  pbuf[0] = report_len;
  cma_read_(unit, &pbuf[1], &len, &retc);
  if (retc != len) {
    /* Error */
    rc = retc;
    goto finish;
  }

  rc = len + 1;

 finish:
  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
}
