/* ioassign.c (main program) */

/* Written by Sami Saarinen 1997-98, 2000-01, 2003 (ECMWF) */

#include "iostuff.h"
#include "ioassign.h"

#define ioassign main

static char vers[]     = "3.1";
static char date_str[] = __DATE__;
static char time_str[] = __TIME__;

#if defined(CRAY) && !defined(T3D) && !defined(T3E)
#define SYSTEM_NAME "(CRAY PVP)"
#endif

#ifdef T3D
#define SYSTEM_NAME "(CRAY T3D)"
#endif

#ifdef T3E
#define SYSTEM_NAME "(CRAY T3E)"
#endif

#ifdef SGI
#define SYSTEM_NAME "(Silicon Graphics)"
#endif

#ifdef RS6K
#define SYSTEM_NAME "(IBM RS/6000)"
#endif

#ifdef VPP
#define SYSTEM_NAME "(Fujitsu VPP)"
#endif

#ifdef NECSX
#define SYSTEM_NAME "(NEC SX)"
#endif

#ifndef SYSTEM_NAME
#define SYSTEM_NAME "(Unknown system)"
#endif

static char system_name[] = SYSTEM_NAME;

#define USAGE  \
  "Usage: ioassign [-Version] [-verbose] [-help] [-debug]\n" \
  "                [-m internal_packing_method] [-B blocksize]\n" \
  "                [-n nproc] [-b numbins] [-s]\n" \
  "                [-Incore] [-r readbufsize] [-w writebufsize]\n" \
  "                [-p prealloc] [-e extent]\n" \
  "                [-linkonly] [-a alias_name] [-c 'unix_pipe_filter']\n" \
  "                [-x (to expand file names according to nproc)]\n" \
  "                [-k [-]concatenate_processor_number]\n" \
  "                [-S starting_number_for_logical_filename]\n" \
  "                [-f] logical_file_name\n" \
  "For example:\n" \
  "(1) Print contents of the current $IOASSIGN (default=IOASSIGN) file:\n" \
  "    ioassign -v\n" \
  "(2) Generate PE-related symbolic links with verbose output:\n" \
  "    setenv NPES 4\n" \
  "    ioassign -v -link -a /path/target_file.%d  symbolic_link.%d\n" \
  "  or\n" \
  "    ioassign -v -n 4 -link -a /path/target_file.%d  symbolic_link.%d\n" \
  "(3) Assign a file with 4MB read and write buffers:\n" \
  "    ioassign -r 4m -w 4m -a /path/target_file.%d filename_in_open_stmt.%d\n" \
  "(4) Assign a file with internal packing of method#2:\n" \
  "    ioassign -w 4m -m2 -a /path/target_file.%d filename_in_open_stmt.%d\n" \
  "(5) Use pipes: \n" \
  "    ioassign -r 2m -w 2m -c '#gzip' filename_in_open_stmt.%d\n" \
  "(6) Explicit read-pipe from gunzip decoder; input file read to in-memory buffer:\n" \
  "    ioassign -Incore -c 'gunzip -c < %s' filename_in_open_stmt.%d\n" \
  "(7) Request for concatenation of 16 files upon write: \n" \
  "    ioassign -k 16 -a /path/target_file.%d filename_in_open_stmt.%d\n" \
  "NOTE: The two first are general purpose and do NOT need any special OPEN/READ/WRITE/CLOSE."

#define APPLIED  \
  "+ ioassign %s%s-n %d -b %d -r %d -w %d -p %d -e %d -m %d -B %d -k %d %s%s%s%s%s%s%s%s%s\n"

#define FLAGS "a:b:B:c:de:f:hI:k:l:m:n:p:r:sS:vVw:x"

static char *
KiloMegaGiga(char *s)
{
  const double maxint = 2147483647.0;
  char *p = s;
  int   len = strlen(s);
  char *last = &s[len-1];
  double num;

  if (*last == 'k' || *last == 'K') {
    *last = '\0';
    num = atoi(s) * 1024.0; /* multiplier: 1kB */
    num = MIN(num, maxint);
    num = MAX(0, num);
    ALLOC(p, 20);
    sprintf(p,"%0.f",num);
  }
  else if (*last == 'm' || *last == 'M') {
    *last = '\0';
    num = atoi(s) * 1048576.0; /* multiplier: 1MB */
    num = MIN(num, maxint);
    num = MAX(0, num);
    ALLOC(p, 20);
    sprintf(p,"%.0f",num);
  }
  else if (*last == 'g' || *last == 'G') {
    *last = '\0';
    num = atoi(s) * 1073741824.0; /* multiplier: 1GB */
    num = MIN(num, maxint);
    num = MAX(0, num);
    ALLOC(p, 20);
    sprintf(p,"%.0f",num);
  }

  return p;
}


static int numbins       = 1;
static int readbufsize   = 0;
static int writebufsize  = 0;
static int pre_alloc      = 0;
static int extent        = 0;
static int compression   = 0;
static int blocksize     = 0;
static int concat        = 0;
static char *pipecmd     = NULL;
static int pbcount       = 0;


static Ioassign *
IOassign_scan(Ioassign *p,
         const char *file,
         const char *alias,
         int nproc,
         int fromproc,
         int toproc)
{
  Ioassign *empty_slot = NULL;
  Boolean found = false;

  while ( p ) {
    Ioassign *next = p->next;

    if (!p->filename) {
      empty_slot = p;
    }
    else if (strequ(file, p->filename)) {
      p->aliasname    = STRDUP(alias);
      p->numbins      = numbins;
      p->readbufsize  = readbufsize;
      p->writebufsize = writebufsize;
      p->prealloc     = pre_alloc;
      p->extent       = extent;
      p->compression  = compression;
      p->blocksize    = blocksize;
      p->concat       = concat;
      p->pipecmd      = pipecmd ? STRDUP(pipecmd) : NULL;
      p->maxproc      = nproc;
      p->fromproc     = fromproc;
      p->toproc       = toproc;

      found = true;
      break;
    }

    if (next)  
      p = next;
    else {
      found = false;
      break;
    }
  }
  
  if (!found) {
    Ioassign *tmp;

    if (empty_slot)
      tmp = empty_slot;
    else {
      ALLOC(tmp, 1);
      p->next = tmp;
    }

    p = tmp;

    p->filename     = STRDUP(file);
    p->aliasname    = STRDUP(alias);
    p->numbins      = numbins;
    p->readbufsize  = readbufsize;
    p->writebufsize = writebufsize;
    p->prealloc     = pre_alloc;
    p->extent       = extent;
    p->compression  = compression;
    p->blocksize    = blocksize;
    p->concat       = concat;
    p->pipecmd      = pipecmd ? STRDUP(pipecmd) : NULL;
    p->maxproc      = nproc;
    p->fromproc     = fromproc;
    p->toproc       = toproc;

    if (!empty_slot) {
      p->next = NULL;
      pbcount++;
    }

    found = true;
  }

  return p;
}

FILE *
ODB_trace_fp(void)
{
  /* A dummy routine to return NULL fp of non-existent tracing file */
  return NULL;
}

#define ioassign main

int ioassign(int argc, char *argv[])
{
  int nproc         = 1;
  char *aliasname   = NULL;
  char *filename    = NULL;
  int verbose       = 0;
  Boolean debug     = false;
  Boolean start_no_is_set = false;
  int print_version = 0;
  int incore        = 0;
  int linkonly      = 0;
  int expand        = 0;
  int start_no      = 1;
  
  Ioassign *ioassign_start = NULL;
  
  int c;
  int errflg = 0;
  extern char *optarg;
  extern int optind;
  /* int (*func)(void) = NULL; */
  char *NPES = getenv("NPES");
  char *IOASSIGN = getenv("IOASSIGN");
  char *program = strrchr(argv[0],'/');
    
  program = program ? (program+1) : argv[0];

  if (NPES) {
    nproc = atoi(NPES);
    nproc = MAX(1,nproc);
  }

  if (!IOASSIGN) IOASSIGN = "IOASSIGN";

  while ((c = getopt(argc, argv, FLAGS)) != -1) {
    switch (c) {
    case 'a':
      aliasname = STRDUP(optarg);
      break;
    case 'b':
      numbins = atoi(optarg);
      numbins = MAX(0,numbins);
      break;
    case 'B':
      blocksize = atoi(KiloMegaGiga(optarg));
      blocksize = MAX(0, blocksize);
      break;
    case 'c':
      pipecmd = STRDUP(optarg);
      break;
    case 'd':
      debug = true;
      break;
    case 'e':
      extent = atoi(KiloMegaGiga(optarg));
      extent = MAX(0,extent);
      break;
    case 'f':
      filename = STRDUP(optarg);
      break;
    case 'h':
      errflg++;
      break;
    case 'I':
      incore = 1;
      break;
    case 'k':
      concat = atoi(optarg);
      break;
    case 'l':
      linkonly = 1;
      expand   = 1;
      break;
    case 'm':
      compression = atoi(optarg);
      compression = MAX(0, compression);
      break;
    case 'n':
      nproc = atoi(optarg);
      nproc = MAX(1,nproc);
      break;
    case 'p':
      pre_alloc = atoi(KiloMegaGiga(optarg));
      pre_alloc = MAX(0,pre_alloc);
      break;
    case 'r':
      readbufsize = atoi(KiloMegaGiga(optarg));
      readbufsize = MAX(-1,readbufsize);
      break;
    case 's':
      /* Do not check the existing IOASSIGN and pass data to stream directly */
      IOASSIGN = NULL;
      break;
    case 'S':
      start_no = atoi(optarg);
      if (start_no < 1) start_no = 1;
      start_no_is_set = true;
      break;
    case 'v':
      verbose = 1;
      break;
    case 'V':
      print_version = 1;
      break;
    case 'w':
      writebufsize = atoi(KiloMegaGiga(optarg));
      writebufsize = MAX(0,writebufsize);
      break;
    case 'x':
      expand = 1;
      break;
    default:
    case '?':
      errflg++;
      break;
    }
  }

  if (print_version) {
    fprintf(stderr,"%s: Version %s, on %s %s   %s\n", 
	    program, vers, time_str, date_str, system_name);
    fprintf(stderr,"Copyright (c) 1997-98, 2000 ECMWF. All Rights Reserved.\n");
  }

  if (!errflg) {
    iostuff_debug = debug;

    if (readbufsize == -1) incore = 1;
    if (incore)            readbufsize = -1;

    if (argc - 1 == optind) {
      filename = STRDUP(argv[argc-1]);
    }
    else if (argc > optind) {
      errflg++;
    }
  }

  if (!errflg && !filename && aliasname) {
    filename = STRDUP(aliasname);
  }

  if (!errflg && !filename) {
    if (verbose) {
      Ioassign *p;
      int rc = IOassign_read(IOASSIGN, &pbcount, &ioassign_start, &p);

      if (pbcount > 0) {
	fprintf(stderr,
		"== Contents of the current '%s' -file ==\n",
		IOASSIGN);
	IOassign_write(NULL, pbcount, ioassign_start);
      }

      return(0);
    }
    else
      errflg++;
  }

  if (errflg) {
    fprintf(stderr,"%s\n",USAGE);
    return(errflg);
  }

  if (!errflg && !aliasname && filename) {
    aliasname = STRDUP(filename);
  }

  if (!errflg && extent == 0) {
    extent = pre_alloc;
  }

  if (!errflg && verbose) {
    fprintf(stderr,APPLIED,
	    verbose ? "-v " : "\0",
	    debug   ? "-d " : "\0",
	    nproc, numbins, readbufsize, writebufsize, 
	    pre_alloc, extent,
	    compression, blocksize,
	    concat,
	    expand ? "-x " : "\0",
	    linkonly ? "-link " : "\0",
	    aliasname ? "-a "     : "\0",
	    aliasname ? aliasname : "\0",
	    aliasname ? " "       : "\0",
	    pipecmd ? "-c '"   : "\0",
	    pipecmd ? pipecmd : "\0",
	    pipecmd ? "' "     : "\0",
	    filename ? filename : "\0");
  }

  if (!errflg) {    
    int j;

    if (linkonly) {
      char cmd[1024];

      if (has_no_percent_sign(filename)) {
	nproc = 1;
      }
      
      for (j=1; j<=nproc; j++) {
	if (!strequ(filename,aliasname)) {
	  char file[255], alias[255];
	  sprintf(file,filename,j+start_no-1);
	  sprintf(alias,aliasname,j);
	  sprintf(cmd,"\\ln -s %s %s\n",alias,file);
	  if (verbose) fprintf(stderr,"+ %s",cmd);
	  system(cmd);
	}
      }
    }
    else {
      Ioassign *p;
      if (debug) fprintf(stderr," : IOASSIGN='%s'\n",IOASSIGN);
      
      errflg = IOassign_read(IOASSIGN, &pbcount, &ioassign_start, &p);
      
      if (!errflg || errflg == -2) {
	
	if (pbcount == 0 || !ioassign_start) {
	  /* Make sure that there is at least one entry */
	  Ioassign *p;
	  ALLOC(p, 1);
	  
	  ioassign_start = p;
	  p->next = NULL;
	  
	  p->filename  = NULL;
	  p->aliasname = NULL;
	  p->numbins  = 0;
	  p->readbufsize  = 0;
	  p->writebufsize = 0;
	  p->prealloc  = 0;
	  p->extent    = 0;
	  p->compression  = 0;
	  p->blocksize    = 0;
	  p->concat    = 0;
     p->maxproc     = 0;
     p->fromproc  = 0;
     p->toproc    = 0;
	  p->pipecmd   = NULL;
	  
	  pbcount = 1;
	}
	

	{ /* Update IOASSIGN-file */
	  Ioassign *p;

	  if (has_no_percent_sign(filename)) {
	    nproc = 1;
	  }

	  if (expand) {
	    for (j=1; j<=nproc; j++) {
	      char *file  = IOstrdup_fmt(filename, j + start_no - 1);
	      char *alias = IOstrdup_fmt(aliasname, j);
	      
	      p = IOassign_scan(ioassign_start, file, alias, 0, 0, 0);
	      
	      FREE(file);
	      FREE(alias);
	      
	      if (verbose && p) IOassign_write(NULL, 1, p);
	    }
	  }
	  else {
	    if (has_no_percent_sign(filename) &&
		has_no_percent_sign(aliasname)) nproc = 0;
       if (start_no_is_set){
          p = IOassign_scan(ioassign_start, filename, aliasname, nproc, start_no, start_no + nproc - 1 );
       }else
          p = IOassign_scan(ioassign_start, filename, aliasname, nproc, 0, 0 );
	    if (verbose && p) IOassign_write(NULL, 1, p);
	  }
	}
	
	errflg = IOassign_write(IOASSIGN, pbcount, ioassign_start);
      }
    }
  }
    
  return(errflg);
}
