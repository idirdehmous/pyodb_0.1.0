#include "cmaio.h"
#include "swapbytes.h"
#include "magicwords.h"
#include "cdrhook.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/*
 * P. Marguinaud : 10-10-2013 : Rename files before overwriting (by using env. ODB_OVERWRITE_METHOD)
 */

static o_lock_t CMAIO_mylock[MAX_CMAIO_LOCKS] = { 0 } ; /* A specific OMP-lock; initialized only once in
							   odb/lib/codb.c, routine codb_init_omp_locks_() */

#define MAXCMAIO_DEFAULT 1000

IOcma **cmaio = NULL;
int *cmaio_1stfree = NULL;

/* flags for thread-safe perror()'ing ; used to be handled via cma_perror() */
int *cma_perror_flag = NULL; 

void cma_set_lock(int lockno)
{
  if (lockno >= 0 && lockno < MAX_CMAIO_LOCKS) coml_set_lockid_(&CMAIO_mylock[lockno]);
}

void cma_unset_lock(int lockno)
{
  if (lockno >= 0 && lockno < MAX_CMAIO_LOCKS) coml_unset_lockid_(&CMAIO_mylock[lockno]);
}


int CMA_get_MAXCMAIO()
{
  static int maxcmaio = MAXCMAIO_DEFAULT;
  if (!cmaio || !cmaio_1stfree || !cma_perror_flag) { /* 1st */
    cma_set_lock(0);
    /* We ought to call 1st OpenMP FLUSH here, but we're in C ... */
    /* 
       Can't we do something like 
       int size=sizeof(var);
       coml_flush_(&var,&size);
       ??
    */
    if (!cmaio || !cmaio_1stfree || !cma_perror_flag) { /* 2nd */
      int j, inumt = get_max_threads_();
      if (!cmaio) {
	char *env = getenv("MAXCMAIO");
	int value = 0;
	if (env) value = atoi(env);
	if (value <= 0) value = maxcmaio;
	maxcmaio = value;
	ALLOC(cmaio, 1+inumt); /* "1+" for convenience i.e. no need to do "--tid" */
	for (j=0; j<=inumt; j++) {
	  CALLOC(cmaio[j], maxcmaio);
	}
      }

      if (!cmaio_1stfree) {
	CALLOC(cmaio_1stfree, 1+inumt); /* "1+" for convenience; see above */
      }

      if (!cma_perror_flag) {
	ALLOC(cma_perror_flag, 1+inumt); /* "1+" for convenience; see above */
	for (j=0; j<=inumt; j++) cma_perror_flag[j] = 1; /* by default call perror() */
      }

      /* We ought to call 2nd OpenMP FLUSH here, but we're in C ... */
    } /* if (!cmaio || !cmaio_1stfree || !cma_perror_flag) 2nd */
    cma_unset_lock(0);
  } /* if (!cmaio || !cmaio_1stfree || !cma_perror_flag) 1st */
  return maxcmaio;
}

void
init_CMAIO_lock()
{
  int j;
  for (j=0; j<MAX_CMAIO_LOCKS; j++) {
    char lockname[80];
    snprintf(lockname,80,"cma_open.c:CMAIO_mylock[%d]",j);
    INIT_LOCKID_WITH_NAME(&CMAIO_mylock[j],lockname);
  }
  (void) CMA_get_MAXCMAIO(); /* to avoid possible lack-of-OpenMP FLUSH-conditions/problems later */
  (void) IOgetattr(NULL); /* Read in IOASSIGN unless already done */
}

FILE *
CMA_get_fp(const integer4 *unit)
{
  /* int rc = 0; */
  int index = IOINDEX(*unit);
  int bin = IOBIN(*unit);
  FILE *fp = NULL;

  if (index >= 0 && index < CMA_get_MAXCMAIO()) {
    int tid = get_thread_id_();
    IOcma *pcmaio = &cmaio[tid][index];

    if (pcmaio->io.is_inuse) {
      int j, numbins = pcmaio->io.numbins;

      for (j=0; j<numbins; j++) {
	IObin *pbin = &pcmaio->io.bin[j];
	if (j == bin) {
	  fp = pbin->fp;
	  break;
	}
      } /* for (j=0; j<numbins; j++) */
    } /* if (pcmaio->io.is_inuse) */
  } /* if (index >= 0 && index < CMA_get_MAXCMAIO()) */

  return fp;
}


static void 
init_cmaio(IOcma *pcmaio)
{
  if (pcmaio) {
    IOstuff_init(&pcmaio->io);
    
    pcmaio->zinfolen  = 0;
    pcmaio->zinfo     = NULL;
    pcmaio->numddrs   = 0;
    pcmaio->lenddrs   = 0;
    pcmaio->numobs    = 0;
    pcmaio->maxreplen = 0;
    pcmaio->cmalen    = 0;
    pcmaio->ddrs      = NULL;
    
    pcmaio->ddr_read_counter = 0;
    
    pcmaio->chunk     = 0;
  }
}


FORTRAN_CALL void 
cma_open_(integer4       *unit, 
	  const char     *filename,
	  const char     *mode,
	  integer4       *retcode,
	  /* Hidden arguments to Fortran-program */
	  int             len_filename,
	  int             len_mode)
{
  int rc = 0;
  Boolean found = false;
  int maxcmaio = CMA_get_MAXCMAIO();
  int i, index = -1;
  int tid = get_thread_id_();
  DRHOOK_START(cma_open_);

  *unit = -1;

  i = cmaio_1stfree[tid];
  if (i < maxcmaio) {
    IOcma *pcmaio = &cmaio[tid][i];
    if (pcmaio->io.is_inuse == false) {
      index = i;
      found = true;
    }
  }

  if (!found) { /* Fallback*/
    for (i=0; i<maxcmaio; i++) {
      IOcma *pcmaio = &cmaio[tid][i];
      if (pcmaio->io.is_inuse == false) {
	found = true;
	index = i;
	break;
      }
    }
  }

  if (iostuff_debug) fprintf(stderr,"cma_open");

  if (found) {
    IOcma *pcmaio = &cmaio[tid][index];
    char Mode;
    int j, numbins;
    Boolean append_mode = false;
    int scheme_saved = -1;

    init_cmaio(pcmaio);

    if (!mode) {
      /* Error : Invalid open mode */
      rc = -3;
      goto finish;
    }

    Mode = isupper(*mode) ? tolower(*mode) : *mode;

    if (Mode != 'r' && Mode != 'w' && Mode != 'a' ) {
      /* Error : Invalid open mode */
      rc = -3;
      goto finish;
    }
    
    pcmaio->io.read_only = (Mode == 'r') ? true : false;
    append_mode = (Mode == 'a') ? true : false;
    
    pcmaio->io.logical_name = IOstrdup(filename, &len_filename);

    if (iostuff_debug) fprintf(stderr,"('%s') :",pcmaio->io.logical_name);
    
    (void) IOgetattr(&pcmaio->io);

    numbins = pcmaio->io.numbins;

    if (iostuff_debug) fprintf(stderr," numbins=%d\n",numbins);

    for (j=0; j<numbins; j++) {
      IObin *pbin = &pcmaio->io.bin[j];
      FILE *fp = NULL;

      if (scheme_saved >= 0) pcmaio->io.scheme = scheme_saved;

    redo: /* Jump back here (from bottom), if you detected the file needs an external tool */
      
      if (j == 0) {
	if (pbin->pipecmd) {
	  pcmaio->io.scheme = external;
	}
	else {
	  /* The following is quite useless test for READ-ONLY files,
	     since an automatic sensing will take place later on */
	  pcmaio->io.scheme = (pcmaio->io.packmethod == 0) ? none : internal;
	}
	if (scheme_saved < 0) scheme_saved = pcmaio->io.scheme;
      }
      
      if (j == 0 && !pcmaio->io.read_only) (void) IOmkdir(pbin->true_name);

      if (!pcmaio->io.read_only && !append_mode && 
	  ((pcmaio->io.scheme & none) == none)) {
	/* For write/only ("w", but not "a") files, check the file suffix,
	   and if it something recognizable, like ".gz", ".Z", ".zip", then use
	   corresponding external packing hack */
	const char *scheme_change = Compression_Suffix_Check(pbin->true_name);
	if (scheme_change) {
	  if (iostuff_debug) {
	    fprintf(stderr,
		    "cma_open(%s,'w'): scheme_change triggered. Now '%s'\n",
		    pbin->true_name, scheme_change);
	  }
	  FREE(pbin->pipecmd);
	  pbin->pipecmd = STRDUP(scheme_change);
	  pcmaio->io.scheme = external;
	}
      }

      if (((pcmaio->io.scheme & none) == none) || 
	  ((pcmaio->io.scheme & internal) == internal)) {
	/* Regular file opening; valid for both none or internal compression */
	
	cma_set_lock(0);
	time(&pbin->stat.open_time);	
	if (iostuff_stat) IOtimes(NULL);
#ifdef VPP
	if (!pcmaio->io.read_only && !pcmaio->io.on_mrfs 
	    && pbin->prealloc > 0 && !append_mode) {
	  fp = IOprealloc(pbin->true_name,
			  pbin->prealloc,
			  pbin->extent,
			  NULL,
			  &pbin->fileno);
	}
	else {
	  char *m = pcmaio->io.read_only ? "r" : "w";
	  if (append_mode) m = "a";
	  fp = fopen(pbin->true_name, m);
	}
#else
	{
	  char *m = pcmaio->io.read_only ? "r" : "w";
	  if (append_mode) m = "a";
          if (*m == 'w')
            {   
              static int odb_w = -1; 
              if (odb_w < 0)
                {   
                  char * codb_w = getenv ("ODB_OVERWRITE_METHOD");
                  if (codb_w)
                    sscanf (codb_w, "%d", &odb_w);
                  else
                    odb_w = 0;
                }   
              if (odb_w == 1)
                {   
                  unlink (pbin->true_name);
                }   
              else if (odb_w == 2)
                {   
                  char tmp[1024];
                  char * ext;
                  int i;
                  strcpy (tmp, pbin->true_name);
                  ext = tmp + strlen (pbin->true_name);
                  for (i = 0; ; i++)
                    {   
                      struct stat st; 
                      sprintf (ext, ".%6.6d", i); 
                      if (stat (tmp, &st) == -1) 
                        break;
                    }   
                  rename (pbin->true_name, tmp);
                }   
            }   
	  fp = fopen(pbin->true_name, m);
	}
#endif
	if (!fp && cma_perror_flag && cma_perror_flag[tid]) PERROR(pbin->true_name);
	if (iostuff_stat) IOtimes(&pbin->stat.t);
	cma_unset_lock(0);
      }
      else if ((pcmaio->io.scheme & external) == external) {
	/* Data to be accessed via Unix-pipe i.e. popen() */
	int go_ahead = 0;

	if (pcmaio->io.read_only) { 
	  /* When R/O check existence of the true file */
	  struct stat buf;
	  if (stat(pbin->true_name,&buf) == 0) go_ahead = 1;
	}
	else {
	  go_ahead = 1;
	}

	if (go_ahead) {
	  char *m = pcmaio->io.read_only ? "r" : "w";
	  const IObuf *iob = pcmaio->io.read_only ? &pbin->readbuf : &pbin->writebuf;
	  char *cmd = NULL;
	  Boolean has_incore = false;
	  char *pipecmd = IOknowncmd(pbin->pipecmd, pcmaio->io.read_only, &has_incore, NULL, iob);
	  int len = strlen(pipecmd) + strlen(pbin->true_name) + 20;

	  ALLOC(cmd, len);
	  sprintf(cmd, pipecmd, pbin->true_name);

	  if (iostuff_debug) {
	    fprintf(stderr,"cma_open(truename='%s',mode='%s'): pipecmd='%s' --> cmd='%s'\n",
		    pbin->true_name, m, pipecmd, cmd);
	  }
	  
	  FREE(pipecmd);
	  
	  time(&pbin->stat.open_time);	
	  if (iostuff_stat) IOtimes(NULL);
	  if (append_mode) m = "a";
	  fp = popen(cmd, m);
	  if (!fp && cma_perror_flag && cma_perror_flag[tid]) PERROR(pbin->true_name);
	  if (iostuff_stat) IOtimes(&pbin->stat.t);
	  
	  if (fp) pbin->cmd = STRDUP(cmd);
	  FREE(cmd);
	}
	else
	  fp = NULL;
      }

      if (fp) {
	if (iostuff_stat) {
	  pbin->stat.num_trans++;
	}
	
	pbin->fp = fp;
	pbin->fileno = fileno(fp);
	
	/* Set I/O-buffering, if applicable */

	(void) IOsetbuf(fp, pbin->true_name, pcmaio->io.read_only,
			pcmaio->io.read_only ? &pbin->readbuf : &pbin->writebuf,
			pcmaio->io.read_only ? &pbin->filesize : NULL,
			pcmaio->io.read_only ? &pbin->blksize : NULL);

	if ((pcmaio->io.scheme & none) == none || (pcmaio->io.scheme & internal) == internal) {

	  if (pcmaio->io.read_only && j == 0 && pbin->filesize > 0) {
	    Boolean is_mr2d = false;
	    int offset = 0;
	    unsigned int first_word = 0;
	    
	    int num;
	    int swp = 0;
	    const char *scheme_change = NULL;

	    if (iostuff_stat) IOtimes(NULL);
	    scheme_change = Compression_Magic_Check(fp, &first_word, 0);
	    if (iostuff_stat) IOtimes(&pbin->stat.t);

	    if (scheme_change) {
	      if (iostuff_debug) {
		fprintf(stderr,
			"cma_open(%s,'r'): scheme_change triggered. Now '%s'\n",
			pbin->true_name, scheme_change);
	      }
	      FREE(pbin->pipecmd);
	      pbin->pipecmd = STRDUP(scheme_change);
	      pcmaio->io.scheme = external;
	      fclose(fp);
	      FREEIOBUF(pbin->readbuf);
	      goto redo;
	    }

	    num = 1;
	    pbin->stat.bytes += num * sizeof(unsigned int);
	    pbin->stat.num_trans++;

	    /* Check for concatenated file */

	    if (first_word == MR2D || first_word == D2RM) {
	      is_mr2d = true;
	      swp = (first_word == D2RM);
	    }
	    else {
	      is_mr2d = false;
	      swp = 0;
	    }
	    pcmaio->io.is_mrfs2disk = is_mr2d;
	    pcmaio->io.req_byteswap = swp;

	    if (is_mr2d) {
	      unsigned int nchunks = 0;

	      if (iostuff_stat) IOtimes(NULL);
	      fread(&nchunks, sizeof(unsigned int), 1, fp);
	      if (swp) {
		int len = 1;
		swap4bytes_(&nchunks, &len);
	      }
	      if (iostuff_stat) IOtimes(&pbin->stat.t);

	      pcmaio->io.concat = nchunks; /* In a normal case remains constant across the bins */

	      pbin->mr2d_infolen = 1 + 2 * nchunks;
	      ALLOC(pbin->mr2d_info, pbin->mr2d_infolen);

	      pbin->mr2d_info[0] = nchunks;

	      if (iostuff_stat) IOtimes(NULL);
	      fread(&pbin->mr2d_info[1], sizeof(unsigned int), pbin->mr2d_infolen - 1, fp);
	      if (swp) {
		int len = pbin->mr2d_infolen - 1;
		swap4bytes_(&pbin->mr2d_info[1], &len);
	      }
	      if (iostuff_stat) IOtimes(&pbin->stat.t);
	      
	      num = 1 + 2 * nchunks;

	      pbin->stat.bytes += num * sizeof(unsigned int);
	      pbin->stat.num_trans += 2;

	      offset = sizeof(unsigned int) * (1 + num);

	      if (iostuff_stat) IOtimes(NULL);
	      fread(&first_word, sizeof(unsigned int), 1, fp);
	      if (iostuff_stat) IOtimes(&pbin->stat.t);

	      num = 1;
	      pbin->stat.bytes += num * sizeof(unsigned int);
	      pbin->stat.num_trans++;
	    }

	    /* Check for internal compression */
	    if (first_word == PCMA || first_word == AMCP) {
	      pcmaio->io.scheme = internal;
	      if (!is_mr2d) swp = (first_word == AMCP);
	    }
	    else {
	      pcmaio->io.scheme = none;
	      if (!is_mr2d) {
		swp = (first_word == _BDO || first_word == _23CH);
	      }
	    }

	    pcmaio->io.req_byteswap = swp;

	    fseek(fp, offset, SEEK_SET);

	    pbin->filepos = offset;
	    pbin->begin_filepos = offset;
	  }
	}

	if (j == 0 && (pcmaio->io.scheme & internal) == internal) {
	  if (pcmaio->io.read_only) {
	    /* Due to an automatic sensing the R/O files do not need this */
	    pcmaio->io.packmethod = -1; 
	  }
	  /* ... and for W/O-files the pcmaio->io.packmethod was
	     already gathered upon IOgetattr() -phase */

	  pcma_blocksize = MAX(pcma_blocksize, pcmaio->io.blocksize);
	  pcmaio->io.blocksize = pcma_blocksize;
	}

	if (iostuff_debug) {
	  fprintf(stderr," bin=%d : (packing_flag=0x%x; method#%d,blk=%d) %s='%s' ; %s",
		  j,
		  pcmaio->io.scheme,
		  ((pcmaio->io.scheme & none) == none)     ? 0 : pcmaio->io.packmethod,
		  ((pcmaio->io.scheme & none) == none)     ? 0 : pcmaio->io.blocksize,
		  ((pcmaio->io.scheme & external) == external) ? "pipe" : "file",
		  ((pcmaio->io.scheme & external) == external) ? pbin->cmd : pbin->true_name,
		  pcmaio->io.read_only ? "READ-ONLY" : "WRITE-ONLY");
	}
	
      }
      else {
	/* Error : Couldn't open the file */
	rc = -1;
	goto finish;
      }

      if (iostuff_debug) fprintf(stderr,"\n");
    } /* for (j=0; j<numbins; j++) */
    
    /* Everything ok */
	
    pcmaio->io.is_inuse = true;
    pcmaio->io.is_detached = false;
    
    *unit = index;
    if (cmaio_1stfree[tid] <= index) cmaio_1stfree[tid] = index+1;
    
    rc = pcmaio->io.numbins;
  }
  else {
    /* Error : Invalid internal file unit */
    rc = -2;
  }

 finish:

  if (iostuff_debug) fprintf(stderr," : rc=%d\n",rc);

  /* Upon successful completion now returns no. of bins allocated */
  *retcode = rc;
  DRHOOK_END(0);
}

void
cma_set_perror_(const integer4 *onoff, integer4 *retcode)
{
  int rc = -1;
  int tid = get_thread_id_();

  if (cma_perror_flag && onoff) {
    rc = cma_perror_flag[tid]; /* old value */
    cma_perror_flag[tid] = (*onoff) ? 1 : 0;
  }

  *retcode = rc;
}


void
cma_get_perror_(integer4 *retcode)
{
  int tid = get_thread_id_();
  *retcode = cma_perror_flag ? cma_perror_flag[tid] : 1; /* old value */
}


void
cma_is_externally_compressed_(const char *filename,
			      const char *mode,
			      integer4 *retcode,
			      /* Hidden argument to Fortran-program */
			      int len_filename,
			      int len_mode)
{ /* Called at least from lib/msgpass_loaddata.F90 and lib/msgpass_storedata.F90 */
  int rc = 0;
  char *sname = IOtruename(filename, &len_filename);

  if (sname) {
    Boolean readonly = (mode && *mode == 'r') ? 1 : 0;
    if (readonly) {
      cma_set_lock(0);
      {
	FILE *fp = fopen(sname, "r");
	if (fp) {
	  unsigned int first_word;
	  const char *scheme_change = Compression_Magic_Check(fp, &first_word, 1); /* magicwords.c */
	  rc = (scheme_change != NULL);
	}
      }
      cma_unset_lock(0);
    }
    else { /* write only files */
      const char *scheme_change = Compression_Suffix_Check(sname);
    }
    FREE(sname);
  }

  *retcode = rc;
}
