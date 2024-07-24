#include "cmaio.h"
#include "cdrhook.h"

FORTRAN_CALL void 
cma_close_(const integer4 *unit, integer4 *retcode)
{
  int rc = 0;
  int index = IOINDEX(*unit);
  /* int bin = IOBIN(*unit); */ /* Ignore, since closing ALL bins */
  DRHOOK_START(cma_close_);

  if (iostuff_debug) fprintf(stderr,"cma_close(%d)\n",index);

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.is_inuse) {
      int j, numbins = pcmaio->io.numbins;
      int (*close_func)(FILE *stream) = 
	((pcmaio->io.scheme & external) == external) ? pclose : fclose;

      for (j=0; j<numbins; j++) {
	IObin *pbin = &pcmaio->io.bin[j];

	if (iostuff_stat) IOtimes(NULL);
	close_func(pbin->fp);
	if (iostuff_stat) {
	  IOtimes(&pbin->stat.t);
	  pbin->stat.num_trans++;
	}

	if (iostuff_stat && iostuff_stat_ftn_unit >= 0) {
	  char *p;
	  char *str_open_time, *str_close_time;
	  char *logical_name, *true_name;
	  char *pipecmd, *cmd;
	  integer4 binno = j;
	  integer4 read_only = pcmaio->io.read_only ? 1 : 0;
	  real8 xfer_speed = (pbin->stat.t.walltime > 0) ?
	    (pbin->stat.bytes/pbin->stat.t.walltime)/MEGABYTE : 0;
	  integer4 readbuf_is_alloc  = pbin->readbuf.p  ? 1 : 0;
	  integer4 writebuf_is_alloc = pbin->writebuf.p ? 1 : 0;
	  integer4 mrfs_flag = (pcmaio->io.on_mrfs == true) ? 1 : 0;

	  time(&pbin->stat.close_time);
	  
	  if (!read_only && pbin->true_name) {
	    (void) IOgetsize(pbin->true_name, &pbin->filesize, &pbin->blksize);
	  }
	  
	  str_open_time  = STRDUP(ctime(&pbin->stat.open_time));
	  p = strchr(str_open_time,'\n'); if (p) *p = '\0';
	  str_close_time = STRDUP(ctime(&pbin->stat.close_time));
	  p = strchr(str_close_time,'\n'); if (p) *p = '\0';
	  logical_name   = STRDUP(TRIM(pcmaio->io.logical_name));
	  true_name      = STRDUP(TRIM(pbin->true_name));
	  pipecmd        = STRDUP(TRIM(pbin->pipecmd));
	  cmd            = STRDUP(TRIM(pbin->cmd));
	  
	  cma_prt_stat_(&iostuff_stat_ftn_unit, &index,
			&binno, &pcmaio->io.numbins,
			&pbin->fileno,
			str_open_time, str_close_time,
			logical_name, true_name,
			pipecmd, cmd,
			&read_only, &pcmaio->io.packmethod, &pcmaio->io.blocksize,
			&pcmaio->numddrs, &pcmaio->lenddrs,
			&pcmaio->numobs,  &pcmaio->maxreplen, &pcmaio->cmalen,
			&pbin->filesize, &pbin->filepos, &pbin->blksize,
			&pbin->stat.bytes, &pbin->stat.num_trans,
			&pbin->readbuf.len, &readbuf_is_alloc,
			&pbin->writebuf.len, &writebuf_is_alloc,
			&pbin->prealloc, &pbin->extent,
			&mrfs_flag,
			&pbin->stat.t.walltime, &xfer_speed,
			&pbin->stat.t.usercpu, &pbin->stat.t.syscpu,
			/* Hidden arguments to Fortran-program */
			strlen(str_open_time), strlen(str_close_time),
			strlen(logical_name), strlen(true_name),
			strlen(pipecmd), strlen(cmd)
			);
	  
	  FREE(str_open_time);
	  FREE(str_close_time);
	  FREE(logical_name);
	  FREE(true_name);
	  FREE(pipecmd);
	  FREE(cmd);
	}

	IOstuff_bin_reset(pbin);

      } /* for (j=0; j<numbins; j++) */

      IOstuff_reset(&pcmaio->io);

      FREE(pcmaio->zinfo);
      pcmaio->zinfolen = 0;
      FREE(pcmaio->ddrs);
      pcmaio->numddrs   = 0;
      pcmaio->lenddrs   = 0;
      pcmaio->numobs    = 0;
      pcmaio->maxreplen = 0;
      pcmaio->cmalen    = 0;

      pcmaio->ddr_read_counter = 0;
  
      pcmaio->chunk     = 0;

      if (cmaio_1stfree[tid] > index) cmaio_1stfree[tid] = index;
    }
    else {
      /* Error: File not in use i.e. reference not found */
      rc = -1;
    }
  }
  else {
    /* Error: Invalid internal file unit */
    rc = -2;
  }

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  *retcode = rc;
  DRHOOK_END(0);
}
