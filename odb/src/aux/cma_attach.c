#include "cmaio.h"

FORTRAN_CALL void 
cma_attach_(integer4       *unit, 
	    const char     *filename,
	    const char     *mode,
	    integer4       *retcode,
	    /* Hidden arguments to Fortran-program */
	    int             len_filename,
	    int             len_mode)
{
  int rc = 0;
  Boolean found = false;
  Boolean re_open = false;
  Boolean read_only = false;
  int maxcmaio = CMA_get_MAXCMAIO();
  int i, index = -1;
  char *sname;    
  char Mode;
  int tid = get_thread_id_();

  sname = IOstrdup(filename, &len_filename); /* sname now STRDUP'ed */

  if (iostuff_debug) {
    fprintf(stderr,"cma_attach('%s') :\n",sname);
  }

  if (!mode) {
    /* Error : Invalid open mode */
    rc = -3;
    goto finish;
  }

  Mode = isupper(*mode) ? tolower(*mode) : *mode;

  if (Mode != 'r' && Mode != 'w') {
    /* Error : Invalid open mode */
    rc = -3;
    goto finish;
  }

  read_only = (Mode == 'r') ? true : false;

  for (i=0; i<maxcmaio; i++) {
    IOcma *pcmaio = &cmaio[tid][i];
    if (pcmaio->io.is_inuse && 
	pcmaio->io.is_detached && 
	/* The following test prevents from (accidental) attaching to 
	   a file with conflicting open mode */
	(pcmaio->io.read_only == read_only)) {
      if (strequ(pcmaio->io.logical_name, sname)) {
	found = true;
	index = i;
	break;
      }
    }
  }

  FREE(sname);

  if (!found) {
    re_open = true;
  }
  else {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    pcmaio->io.is_detached = false;

    if (iostuff_debug) {
      fprintf(stderr," : unit=%d; file(s)=('%s' => '%s%s') : numbins=%d",
	      index, 
	      pcmaio->io.logical_name, 
	      pcmaio->io.leading_part,
	      (pcmaio->io.numbins == 1) ? "\0" : ",*",
	      pcmaio->io.numbins);
    }
    
    *unit = index;
    rc = pcmaio->io.numbins;
  }

  if (re_open) {
    cma_open_(unit, filename, mode, retcode, len_filename, len_mode);
    index = *unit;
    rc = *retcode;
  }

 finish:

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
}
