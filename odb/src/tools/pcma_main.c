#include "pcma.h"

static char vers[]     = "4.8";
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

#ifdef LINUX
#define SYSTEM_NAME "(Linux)"
#endif

#ifdef HPPA
#define SYSTEM_NAME "(HP Unix)"
#endif

#ifndef SYSTEM_NAME
#define SYSTEM_NAME "(Unknown system)"
#endif

static char system_name[] = SYSTEM_NAME;

#define USAGE_PCMA  \
  "Usage: pcma [-Version] [-verbose] [-help] [-m packing_method]\n" \
  "            [-Incore] [-r readbufsize] [-w writebufsize]\n" \
  "            [-p prealloc] [-e extent]\n" \
  "            [-B blocksize] [-i input] [-o output]"

#define USAGE_UPCMA \
  "Usage: upcma [-Version] [-verbose] [-help]\n" \
  "             [-Incore] [-r readbufsize] [-w writebufsize]\n" \
  "             [-p prealloc] [-e extent] [-s can_swp_data]\n" \
  "             [-B blocksize] [-i input] [-o output]"

#define FLAGS_PCMA  "B:e:hI:i:m:o:p:r:vVw:"
#define FLAGS_UPCMA "B:e:hI:i:o:p:r:s:vVw:"


static int readbufsize   = 0;
static int writebufsize  = 0;
static int pre_alloc  = 0;
static int extent    = 0;
static int packingmethod = 2;
static int blocksize     = MINBLOCK;
static int verbose       = 0;
static int can_swp_data  = 1;

static char *program = NULL;

static FILE *fp_in  = NULL;
static char *input  = NULL;
static char *inbuf  = NULL;

static FILE *fp_out = NULL;
static char *output = NULL;
static char *outbuf = NULL;

static int
pack()
{
  int rc = 0;
  int bytes_in  = 0;
  int bytes_out = 0;

  rc = pcma(fp_in,  /* CMA-input channel */
	    fp_out, /* Packed output channel */
	    packingmethod, /* Packing method */
	    NULL, 0, /* No in-core CMA available */
	    NULL,    /* No output packed buffers */
	    &bytes_in, &bytes_out); /* Bytes in/out count */

  if (rc == 0) {
    if (verbose && bytes_in > 0) {
      double ratio = (1 - ((double) bytes_out/bytes_in)) * 100.0;
      
      fprintf(stderr,"%s: bytes in=%d, bytes out=%d : space saving=%.2f%%\n",
	      program, bytes_in, bytes_out, ratio);
    }
  }
  else {
    fprintf(stderr,"%s: Error(s) in packing : rc=%d\n",
	    program, rc);
    if (errno != 0) perror(program);
  }

  return rc;
}



static int
unpack()
{
  int rc = 0;
  int bytes_in  = 0;
  int bytes_out = 0;

  rc = upcma(can_swp_data,
	     fp_in,  /* CMA-input channel */
	     fp_out, /* Packed output channel */
	     NULL, 0, 1, /* No index, idxlen=0, fill_zeroth_cma=1 */
	     NULL, 0, /* No in-core CMA available */
	     NULL,
	     NULL,
	     &bytes_in, &bytes_out); /* Bytes in/out count */

  if (rc == 0) {
    if (verbose && bytes_out > 0) {
      double ratio = (1 - ((double) bytes_in/bytes_out)) * 100.0;
      
      fprintf(stderr,"%s: bytes in=%d, bytes out=%d : space saving was=%.2f%%\n",
	      program, bytes_in, bytes_out, ratio);
    }
  }
  else {
    fprintf(stderr,"%s: rc=%d\n",program, rc);
  }

  return rc;
}


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



int main(int argc, char *argv[])
{
  int c;
  int errflg = 0;
  extern char *optarg;
  extern int optind;
  int incore        = 0;
  int print_version = 0;
  int packing = 1; /* Unpacking = 0 */
  int (*func)(void) = NULL;

  program = strrchr(argv[0],'/');
  program = program ? (program+1) : argv[0];
  packing = strequ(program,"pcma");
  func    = packing ? pack : unpack;

  while ((c = getopt(argc, argv, packing ? FLAGS_PCMA : FLAGS_UPCMA)) != -1) {
    switch (c) {
    case 'B':
      blocksize = atoi(KiloMegaGiga(optarg));
      blocksize = MAX(MINBLOCK,blocksize);
      blocksize = MIN(blocksize, MAXBLOCK);
      break;
    case 'e':
      extent = atoi(KiloMegaGiga(optarg));
      extent = MAX(0,extent);
      break;
    case 'h':
      errflg++;
      break;
    case 'I':
      incore = 1;
      break;
    case 'i':
      FREE(input);
      input = strdup(optarg);
      break;
    case 'm':
      packingmethod = atoi(optarg);
      break;
    case 'o':
      FREE(output);
      output = strdup(optarg);
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
      can_swp_data = atoi(optarg);
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
    default:
    case '?':
      errflg++;
      break;
    }
  }

  if (print_version) {
    fprintf(stderr,"%s: Version %s, on %s %s   %s\n", 
	    program, vers, time_str, date_str, system_name);
    fprintf(stderr,"Copyright (c) 1997,2000-2001 ECMWF. All Rights Reserved. \n");
  }

  if (argc > optind) errflg++;

  if (!errflg) {
    if (readbufsize == -1) incore = 1;
    if (incore)            readbufsize = -1;

    fp_in = input ? fopen(input,"r") : stdin;
    if (!fp_in) {
      perror(input);
      fprintf(stderr,
	      "%s: Unable to open input file %s\n",
	      program,
	      input ? input : "\0");
      errflg++;
    }

    if (fp_in && fp_in != stdin && readbufsize == -1) {
      readbufsize = pcma_filesize(input);
    }

    if (fp_in && readbufsize > 0) {
      ALLOC(inbuf, readbufsize + 8);
      if (inbuf) {
	setvbuf(fp_in, inbuf, _IOFBF, readbufsize);
      }
      else {      
	perror(input);
	fprintf(stderr,
		"%s: Unable to allocate space (%d bytes) for read I/O-buffer\n",
		program,
		readbufsize);
	errflg++;
      }
    }
  }

  if (!errflg) {
#ifdef VPP
    if (output && pre_alloc > 0) {
      fp_out = pcma_prealloc(output,
			     pre_alloc, 
			     extent,
			     NULL,
			     NULL);
    }
    else {
      fp_out = output ? fopen(output,"w") : stdout;
    }
#else
    fp_out = output ? fopen(output,"w") : stdout;
#endif
    if (!fp_out) {
      perror(output);
      fprintf(stderr,
	      "%s: Unable to open output file %s\n",
	      program,
	      output ? output : "\0");
      errflg++;
    }

    if (fp_out && writebufsize > 0) {
      ALLOC(outbuf, writebufsize);
      if (outbuf) {
	setvbuf(fp_out, outbuf, _IOFBF, writebufsize);
      }
      else {
	perror(output);
	fprintf(stderr,
		"%s: Unable to allocate space (%d bytes) for write I/O-buffer\n",
		program,
		writebufsize);
	errflg++;
      }
    }
  }

  if (errflg) {
    fprintf(stderr,"%s\n",packing ? USAGE_PCMA : USAGE_UPCMA);
    return(errflg);
  }

  if (verbose) {
    fprintf(stderr,"%s: Following options are now active:\n",program);
    if (packing) {
      fprintf(stderr,"\tpacking method=%d\n",packingmethod);
    }
    else {
      fprintf(stderr,"\tpacking method=<detected on-the-fly>\n");
    }
    fprintf(stderr,"\tblocksize=%d\n",blocksize);
    fprintf(stderr,"\tinputfile=%s\n",input ? input : "<standard input>");
    fprintf(stderr,"\toutputfile=%s\n",output ? output : "<standard output>");
    fprintf(stderr,"\treadbufsize=%d\n",readbufsize);
    fprintf(stderr,"\twritebufsize=%d\n",writebufsize);
    fprintf(stderr,"\tprealloc=%d\n",pre_alloc);
    fprintf(stderr,"\textent=%d\n",extent);
  }

  pcma_blocksize = blocksize;
  pcma_verbose   = verbose;

  errflg = func();

  if (!errflg) {
    FCLOSE(fp_in);
    FCLOSE(fp_out);
  }

  return(errflg);
}
