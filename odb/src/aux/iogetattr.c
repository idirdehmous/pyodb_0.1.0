#include "iostuff.h"
#include "ioassign.h"

PRIVATE Ioassign *ioassign = NULL;
PRIVATE Ioassign *ioassign_start = NULL;
PRIVATE int pbcount = 0;
PRIVATE char *mrfs = NULL;

int 
IOgetattr(IOstuff *iostuff)
{
  int rc = 0;

  if (iostuff_debug) fprintf(stderr,"iogetattr");

  if (pbcount < 0) {
    /* Error : IOASSIGN-file didn't exist or is incorrect; do not retry */
    rc = -1;
  }
  else if (pbcount == 0) {
    /* One-off */
    {
      /* Get MRFSDIR, if defined */
      char *MRFSDIR = getenv("MRFSDIR");
      if (MRFSDIR) {
	mrfs = STRDUP(MRFSDIR);
      }
    }

    {
      /* Read IOASSIGN data */

      /* Check whether IOASSIGN-file is already stored incore ? */
      char *env = getenv("IOASSIGN_INCORE");
      int incore_value = env ? atoi(env) : 0;
      char *IOASSIGN = getenv("IOASSIGN");
      
      if (!IOASSIGN) IOASSIGN = "IOASSIGN";
      if (iostuff_debug) fprintf(stderr," : IOASSIGN='%s'\n",IOASSIGN);

      if (iostuff_debug) fprintf(stderr," : IOASSIGN_INCORE=%d\n",incore_value);
      
      rc = -9999;
      if (incore_value > 0) {
	rc = IOassign_read_incore(IOASSIGN, &pbcount, &ioassign_start, &ioassign);
      }
      if (rc < 0) { /* The original scheme */
	rc = IOassign_read(IOASSIGN, &pbcount, &ioassign_start, &ioassign);
      }
    }
  }

  if (!iostuff) goto finish;

  if (rc == 0) {
    rc = -4;

    if (ioassign_start) {
      int i = 0;
      Ioassign *p = ioassign_start;
      char *env = NULL;

      /* Scan first with respect to internal hash */
      p = IOassign_lookup(iostuff->logical_name, p);
      if (!p) p = ioassign_start;
      
      while ( p ) {
	int nproc = 0;
   char table_name[100];
   table_name[0]=0;
   
	env = IOresolve_env(p->filename);

	if (IOstrequ(env,iostuff->logical_name,&nproc,table_name,p->fromproc,p->toproc)) {

	  int j;
     int table_name_set = (strlen(table_name)!=0) ? 1 : 0;
	  FREE(env);
     if (nproc > 0) {
      if (table_name_set) {
        env = IOstrdup_fmt2(p->aliasname, nproc, table_name);
      }
      else {
        env = IOstrdup_fmt(p->aliasname, nproc);
      }
     } // nproc > 0
     else { // nproc not set
      if (table_name_set) {
        env = IOstrdup_fmt3(p->aliasname, table_name);
      }
      else {
        env = STRDUP(p->aliasname);
      } 
     }
	  iostuff->leading_part = IOresolve_env(env);
	  FREE(env);
	  
	  if (mrfs) {
	    char *lp = iostuff->leading_part;
	    int mrfs_len = strlen(mrfs);

	    iostuff->on_mrfs = 
	      (strncmp(lp,mrfs,mrfs_len) == 0) ?  true : false;
	  }
	  else {
	    iostuff->on_mrfs = false;
	  }
	  
	  iostuff->packmethod = MAX(0,p->compression);
	  iostuff->blocksize = MAX(0,p->blocksize);

	  if (iostuff_debug) {
	    fprintf(stderr,
		    " : assign '%s' to '%s%s' : attr=(%d,%d,%d,%d,%d,%d,%d,%d,%d)\n : on MRFSDIR[%s]=%s",
		    p->filename, p->aliasname,
		    (p->numbins == 1) ? "\0" : ",*",
		    p->numbins,
		    p->readbufsize, p->writebufsize,
		    p->prealloc, p->extent,
		    p->compression, p->blocksize,
		    p->concat, p->maxproc,
		    mrfs ? mrfs : "<not defined>",
		    iostuff->on_mrfs ? "YES" : "NO");
	    if (p->pipecmd) fprintf(stderr,"\n : pipe='%s'",p->pipecmd);
	  }

	  iostuff->numbins = MAX(1,p->numbins);
	  ALLOC(iostuff->bin, iostuff->numbins);

	  iostuff->concat = p->concat;

	  for (j=0; j<iostuff->numbins; j++) {
	    IObin *pbin = &iostuff->bin[j];

	    IOstuff_bin_init(pbin);

	    if (iostuff->numbins == 1) {
	      pbin->true_name = STRDUP(iostuff->leading_part);
	    }
	    else {
	      pbin->true_name = IOstrdup_int(iostuff->leading_part, j);
	    }
	    pbin->readbuf.len  = p->readbufsize;
	    pbin->writebuf.len = p->writebufsize;
	    pbin->prealloc = p->prealloc;
	    pbin->extent   = p->extent;
	    /* pbin->pipecmd = p->pipecmd ? STRDUP(p->pipecmd) : NULL; */
	    pbin->pipecmd = p->pipecmd ? IOresolve_env(p->pipecmd) : NULL;
	    /* A safety measure to disallow simultaneous internal & external packing */
	    if (pbin->pipecmd) iostuff->packmethod = 0; 
	  }

	  rc = i;
	  break;
	} /* if (IOstrequ(p->filename,iostuff->logical_name,&nproc) */

	FREE(env);

	p = p->next;
	i++;
      } /* while ( p ) */

      FREE(env);
    } /* if (ioassign_start) */

  } /* if (rc == 0) */

  if (rc < 0) {
    /* Error : Nothing found ; initialize with defaults */      
    int numbins = 1;
    IObin *pbin = NULL;
    
    ALLOC(iostuff->bin, numbins);
    iostuff->numbins = numbins;
    iostuff->leading_part = IOresolve_env(iostuff->logical_name);
    
    pbin = iostuff->bin;
    IOstuff_bin_init(pbin);
    pbin->true_name = STRDUP(iostuff->leading_part);
  }

 finish:
  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  return rc;
}


char *
IOtruename(const char *name, const int *len_str)
{
  char *truename = NULL;

  if (name) {
    IOstuff ios;
    
    IOstuff_init(&ios);
    ios.logical_name = IOstrdup(name,len_str);
    
    (void) IOgetattr(&ios);
    
    if (ios.leading_part) {
      truename = STRDUP(ios.leading_part);
    }
    else {
      truename = STRDUP(name);
    }

    { /* Clean up code */
      int j, numbins = ios.numbins;
      for (j=0; j<numbins; j++) {
	IOstuff_bin_reset(&ios.bin[j]);
      }
      IOstuff_reset(&ios);
    }
  }

  return truename;
}

