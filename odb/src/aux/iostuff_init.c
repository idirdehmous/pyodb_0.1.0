#include "iostuff.h"

Boolean  iostuff_debug = false;
Boolean  iostuff_stat  = false;
integer4 iostuff_stat_ftn_unit = -1;

void 
IOstuff_init(IOstuff *p)
{
  p->logical_name = NULL;
  p->leading_part = NULL;

  p->is_inuse     = false;
  p->read_only    = true;
  p->is_detached  = false;
  p->on_mrfs      = false;
  p->is_mrfs2disk = false;
  p->req_byteswap = false;

  p->scheme       = none;

  p->packmethod   = 0;
  p->blocksize    = 0;

  p->concat       = 0;

  p->numbins      = 0;
  p->bin          = NULL;
}


void 
IOstuff_bin_init(IObin *pbin)
{
  pbin->true_name    = NULL;

  pbin->fp           = NULL;
  pbin->fileno       = -1;
  pbin->begin_filepos = 0;
  pbin->filepos      = 0;
  pbin->filesize     = 0;

  pbin->blksize      = 0;

  pbin->readbuf.p    = NULL;
  pbin->readbuf.len  = 0;
  
  pbin->writebuf.p   = NULL;
  pbin->writebuf.len = 0;

  pbin->prealloc     = 0;
  pbin->extent       = 0;

  pbin->pipecmd      = NULL;
  pbin->cmd          = NULL;

  pbin->stat.open_time  = 0;
  pbin->stat.close_time = 0;
  pbin->stat.bytes      = 0;
  pbin->stat.num_trans  = 0;
  pbin->stat.t.walltime = 0;
  pbin->stat.t.usercpu  = 0;
  pbin->stat.t.syscpu   = 0;

  pbin->mr2d_infolen = 0;
  pbin->mr2d_info    = NULL;
}

Boolean
IOstuff_byteswap(IOstuff *p, Boolean value)
{
  Boolean oldvalue = p->req_byteswap;
  p->req_byteswap = value;
  return oldvalue;
}

void 
IOstuff_reset(IOstuff *p)
{
  p->is_inuse     = false;
  /* p->read_only  = true; */
  p->is_detached  = false;
  p->on_mrfs      = false;
  p->is_mrfs2disk = false;
  /* p->req_byteswap = false; */

  p->scheme = none;
	
  p->packmethod = 0;
  p->blocksize = 0;
	
  p->concat = 0;

  FREE(p->bin);
  p->numbins = 0;
  
  FREE(p->logical_name);
  FREE(p->leading_part);
}


void 
IOstuff_bin_reset(IObin *pbin)
{
  FREE(pbin->true_name);
	
  pbin->fp = NULL;
  pbin->fileno = -1;
  pbin->begin_filepos = 0;
  pbin->filepos = 0;
  pbin->filesize = 0;
	
  pbin->blksize = 0;

  FREEIOBUF(pbin->readbuf);

  FREEIOBUF(pbin->writebuf);
	
  pbin->prealloc = 0;
  pbin->extent   = 0;
	
  FREE(pbin->pipecmd);
  FREE(pbin->cmd);
	
  pbin->stat.open_time  = 0;
  pbin->stat.close_time = 0;
  pbin->stat.bytes      = 0;
  pbin->stat.num_trans  = 0;
  pbin->stat.t.walltime = 0;
  pbin->stat.t.usercpu  = 0;
  pbin->stat.t.syscpu   = 0;

  pbin->mr2d_infolen = 0;
  FREE(pbin->mr2d_info);
}
