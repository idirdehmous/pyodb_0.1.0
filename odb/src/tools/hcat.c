#include "odb.h"
#include "cmaio.h"
#include "swapbytes.h"
#include "magicwords.h"

/* Routine to manipulate HC32 (Horizontally Concatenated 32-bit/integer) files */

/* Author: Sami Saarinen, ECMWF, 30-Jan-2003
   = with original release: table of contents (toc) output
   = file split added on 30-Jun-2003 by SS
   = -S to -L : 17-Mar-2006 by SS
   = byte swapping issues : 18-Mar-2006 by SS
   = USAGE (format) fixes : 12-Feb-2012 by SS
 */

static char vers[] = "v1.12Feb2012";
static char date_str[] = __DATE__;
static char time_str[] = __TIME__;

#define FLAGS "1:4:FG:hi:l:L:n:o:t:vVzZ"

#define USAGE \
   "\nUsage(s): %s\n" \
   "  [-v]  for more verbose output\n" \
   "  [-V]  for version number\n" \
   "  [-n npools]  for number of (input) pools to scan\n" \
   "               the default npools taken from $ODB_SRCPATH_<dbname>/<dbname>.dd\n" \
   "  [-Z]  write zero-length files\n" \
   "  [-z]  do *not* write zero-length files (the default)\n" \
   "  [-F]  enforce overwriting existing file(s) [dangerous!]\n" \
   "   -l dbname  database label/name (MUST be given)\n" \
   "   -t table   table to be processed (MUST be given)\n" \
   "\n" \
   "# Split (I/O-method#4 to #1 -conversion):\n" \
   "  -41 [-i input_filename] -t table [-o output_file_template]\n\n" \
   "# Concatenate (I/O-method#1 to #4 -conversion):\n" \
   "  -14 [-i input_filename] -t table [-o output_file_template] [-L chunk_size_MB] [-G group_size]\n\n" \
   "# The default values: -z -L %d -G %d\n" \
   "# The default input file(s) (if not given via -i) are  : <dbname>.<table>.%%d\n" \
   "# The default output file(s) (if not given via -o) are : _/%%d/<table>\n" \
   "\n" \
   "Please note that options [-14|-41] are mutually exclusive\n" \
   "\n" \

#define SIZE_MB 32
#define GRPSIZE 1
#define HDRSIZE 6

static int errflg = 0;

static int concat  = 0;
static int overwrite = 0;
static int grpsize = GRPSIZE;
static int help = 0;
static char *input = NULL;
static int npools = -1;
static char *output = NULL;
static int split   = 0;
static int size_MB = SIZE_MB;
static int verbose = 0;
static int version = 0;
static int zerolen = 0;
static char *dbname = NULL;
static char *table = NULL;
static int ncols_tbl = 0;

/* substitute procid, if '%' was found in input string */

static char *
unmassage_name(const char *in, const int procid)
{
  char *p = NULL;
  char *out = NULL;
  int len = strlen(in) + 50;
  ALLOCX(p, len);
  if (strchr(in,'%')) {
    sprintf(p,in,procid);
  }
  else {
    strcpy(p,in);
  }
  len = strlen(p);
  out = IOtruename(p,&len); /* FREE()'able */
  FREEX(p);
  return out;
}

/* Read .dd-file (borrowed from dcagen.c & modified) */

static int
read_ddfile(const char *ddfile, const char *tablename, int *npools_out)
{
  int ncols = 0;
  int unit, iret;
  FILE *fp = NULL;
  cma_open_(&unit, ddfile, "r", &iret, strlen(ddfile), strlen("r"));
  fp = CMA_get_fp(&unit);
  if (fp) {
    char line[256];
    char tbl[4096];
    int lineno = 0;
    while (!feof(fp)) {
      int n, nel = 0;
      lineno++;
      if (!fgets(line, sizeof(line), fp)) break;
      if (npools_out && lineno == 5) { /* get number of pools */
        nel = sscanf(line,"%d",npools_out);
        continue;
      }
      nel = sscanf(line,"%s %d",tbl,&n);
      if (nel != 2) continue;
      if (*tbl == '@' && strequ(tbl+1,tablename)) {
	/* Match ! */
        ncols = n;
	break; /* while (!feof(fp)) */
      }
    } /* while (!feof(fp)) */
    cma_close_(&unit, &iret);
  }
  return ncols;
}


/*********/
/* split */
/*********/

static void
split_file()
{ /* Split HC32-files and create one ODB1-file per pool */

  int jp, jpsave = 0;
  if (!strchr(input,'%')) npools = 1;

  for (jp=1; jp<=npools; jp++) {
    int filesize = 0;
    int In = -1;
    int rc = 0;
    char *fin = unmassage_name(input, jp);
    IOgetsize(fin, &filesize, NULL);

    if (verbose) 
      fprintf(stderr,
	      "Opening input file '%s' for pool=%d (length=%d, logical='%s')\n", 
	      fin, jp, filesize, input);

    if (filesize <= 0) {
      if (verbose) fprintf(stderr,"File '%s' not found\n", fin);
      goto next_jp; /* skip to the next "jp" */
    }

    cma_open_(&In, fin, "r", &rc, strlen(fin), 1);

    if (rc == 1) {
      int swp = 0;
      int icat = 0;
      int len;
      
      cma_get_byteswap_(&In, &swp);

      if (swp && verbose) 
	fprintf(stderr,"Byteswapping is needed and will be performed automatically\n");

      len = 1;
      cma_readi_(&In, &icat, &len, &rc);
      
      if (rc == len && icat == HC32) {
	int hdr[HDRSIZE];
	int blk = 0;

	for (;;) {
	  byte1 *p = NULL;
	  int nw, ndata;
	  int ipoolno, igrpsize, nbytes, nrows, ncols, npad;

	  len = HDRSIZE;
	  cma_readi_(&In, hdr, &len, &rc);
	  if (rc == -1) {
	    if (verbose) fprintf(stderr,
				 "Successful EOF reached in input file '%s', after block#%d\n", 
				 fin, blk);
	    break; /* for (;;) */
	  }
	  blk++;

	  if (rc != len) {
	    fprintf(stderr,
		    "***Error: Unable to read hdr-info from file '%s', block#%d\n", fin, blk);
	    errflg++;
	    goto finish; /* Error */
	  }

	  ipoolno = hdr[0];
	  ipoolno = ++jpsave; /* An override */
	  igrpsize = hdr[1];
	  nbytes = hdr[2];
	  nrows = hdr[3];
	  ncols = hdr[4];
	  npad = hdr[5];

	  if (verbose) {
	    fprintf(stderr,
		    "\tBlock#%d: pool#%d, grpsize=%d, nbytes=%d, nrows=%d, ncols=%d, npad=%d\n",
		    blk, ipoolno, igrpsize, nbytes, nrows, ncols, npad);
	  }

	  if ((npad & 0x1) == 1) {
	    nw = (nbytes + sizeof(int) - 1)/sizeof(int);
	    ndata = nw * sizeof(int);
	  }
	  else {
	    ndata = nbytes;
	  }

	  ALLOC(p, ndata); /* One-day: introduce data chunking, since "ndata" may be large */
	  cma_readb_(&In, p, &ndata, &rc);

	  if (rc == ndata && (nbytes > 0 || (zerolen && (nbytes == 0 || nrows == 0)))) {
	    int Out = -1;
	    char *fout = unmassage_name(output, ipoolno);
	    IOgetsize(fout, &filesize, NULL);
	    if ((filesize > 0 && overwrite) || filesize <= 0) {
	      if (verbose) fprintf(stderr,
				   "Opening output file '%s' for pool=%d (logical='%s')\n", 
				   fout, ipoolno, output);
	      cma_open_(&Out, fout, "w", &rc, strlen(fout), 1);
	      cma_writeb_(&Out, p, &nbytes, &rc);
	      if (rc != nbytes) {
		fprintf(stderr,
			"***Error: Unable to write data to file '%s' : nbytes=%d, rc=%d\n", 
			fout, nbytes, rc);
		errflg++;
		FREE(fout);
		FREE(p);
		goto finish; /* Error */
	      }
	      cma_close_(&Out, &rc);
	      Out = -1;
	    }
	    else {
	      fprintf(stderr,
		      "***Error: Unable to overwrite existing file '%s'\n", fout);
	      fprintf(stderr,
		      "          Please remove it first or"
		      " use option \"-F\" to enforce overwriting (dangerous)\n");
	      errflg++;
	      FREE(fout);
	      FREE(p);
	      goto finish; /* Error */
	    }

	    FREE(fout);
	    rc = ndata;
	  } /* if (rc == ndata && (nbytes > 0 ... */
	  FREE(p);
	  
	  if (rc != ndata) {
	    fprintf(stderr,
		    "***Error: Unable to read data from file '%s', block#%d : ndata=%d, rc=%d\n", 
		    fin, blk, ndata, rc);
	    errflg++;
	    goto finish; /* Error */
	  }
	} /* for (;;) */
      }
      else {
	fprintf(stderr,
		"***Error: Input file format for splitting of file '%s' (logical='%s') was not recognized\n",
		fin,input);
	errflg++;
	goto finish; /* Error */
      } /* if (rc == len && icat == HC32) */
    } /* if (rc == 1) */
    cma_close_(&In, &rc);

  next_jp:
    FREE(fin);
  } /* for (jp=1; jp<=npools; jp++) */
 finish:
  return;
}

/**********/
/* concat */
/**********/

static void
concat_file()
{ /* Concatenate a number of of ODB1-files into one HC32 */

  int Out = -1;
  char *fout = NULL;
  int jp;
  int offset;
  int filenum = 0;
  int first_time = 1;
  if (!strchr(input,'%')) npools = 1;

  offset=0;
  for (jp=1; jp<=npools; jp++) {
    int filesize = 0;
    int In = -1;
    int rc = 0;
    char *fin = unmassage_name(input, jp);
    IOgetsize(fin, &filesize, NULL);

    if (verbose) 
      fprintf(stderr,
	      "Opening input file '%s' for pool=%d (length=%d, logical='%s')\n", 
	      fin, jp, filesize, input);

    if (filesize <= 0) {
      if (verbose) fprintf(stderr,"File '%s' not found\n", fin);
      goto next_jp; /* skip to the next "jp" */
    }

    cma_open_(&In, fin, "r", &rc, strlen(fin), 1);

    if (rc == 1) {
      int swp = 0;
      int icat = 0;
      int len;
      int re_open;
      
      cma_get_byteswap_(&In, &swp);

      if (swp && verbose) 
	fprintf(stderr,"Byteswapping is needed and will be performed automatically\n");

      len = 1;
      cma_readi_(&In, &icat, &len, &rc);
      
      if (rc == len && icat == ODB_) {
	int hc32word = HC32;
	int hc32[HDRSIZE];
	int hdr[INFOLEN]; /* INFOLEN from odb/include/odb.h */
	byte1 *p = NULL;
	int nw, ndata;
	int ipoolno, igrpsize, nbytes, nrows, ncols, npad;

	hdr[0] = ODB_;
	len = INFOLEN - 1;
	cma_readi_(&In, hdr+1, &len, &rc);
	if (rc == -1) {
	  fprintf(stderr,
		  "***Error: Unexpected EOF encountered in input file '%s'\n", 
		  fin);
	  errflg++;
	  goto finish;
	}

	if (rc != len) {
	  fprintf(stderr,
		  "***Error: Unable to read info-data from file '%s'\n", fin);
	  errflg++;
	  goto finish; /* Error */
	}

	ipoolno = jp; /* An override */
	igrpsize = grpsize;
	nbytes = filesize;
	nrows = hdr[1];
	ncols = hdr[2];
	npad = 0;

	hc32[0] = ipoolno;
	hc32[1] = igrpsize;
	hc32[2] = nbytes;
	hc32[3] = nrows;
	hc32[4] = ncols;
	hc32[5] = npad;

	if (verbose) {
	  fprintf(stderr,
		  "\tpool#%d, grpsize=%d, nbytes=%d, nrows=%d, ncols=%d, npad=%d\n",
		    ipoolno, igrpsize, nbytes, nrows, ncols, npad);
	}

	/* Close previously opened file, if offset has exceeded size_MB (times 1024x1024)
	   or modulo(ipoolno-1,grpsize) == 0 */

	if (Out != -1) {
	  if (offset >= 1024*1024*size_MB || ((ipoolno-1)%igrpsize == 0)) {
	    cma_close_(&Out, &rc);
	    Out = -1;
	    FREE(fout);
	    offset = 0;
	  }
	}

	re_open = (!fout || Out == -1);

	if (!fout) fout = unmassage_name(output, ipoolno);

	ndata = nbytes - sizeof(hdr);

	ALLOC(p, ndata); /* One-day: introduce data chunking, since "ndata" may be large */
	cma_readb_(&In, p, &ndata, &rc);

	if (rc != ndata) {
	  fprintf(stderr,
		  "***Error: Unable to read data from file '%s' : ndata=%d, rc=%d\n", 
		  fin, ndata, rc);
	  errflg++;
	  goto finish; /* Error */
	}

	if (rc == ndata && (nbytes > 0 || (zerolen && (nbytes == 0 || nrows == 0)))) {
	  if (swp) {
	    int n = 1;
	    swap4bytes_(&hc32word, &n);
	    n = HDRSIZE;
	    swap4bytes_(hc32, &n);
	    n = INFOLEN;
	    swap4bytes_(hdr, &n);
	  }

	  if (re_open) {
	    IOgetsize(fout, &filesize, NULL);
	    if ((filesize > 0 && overwrite) || filesize <= 0) {
	      if (verbose) fprintf(stderr,
				   "Opening output file '%s' for pool=%d (logical='%s')\n", 
				   fout, ipoolno, output);
	      filenum = ipoolno;
	      cma_open_(&Out, fout, "w", &rc, strlen(fout), 1);
	      len = 1;
	      cma_writei_(&Out, &hc32word, &len, &rc);
	      if (rc != len) {
		fprintf(stderr,
			"***Error: Unable to write hc32word to file '%s' : len=%d, rc=%d\n",
			fout, len, rc);
		errflg++;
		FREE(p);
		goto finish; /* Error */
	      }
	      offset += sizeof(hc32word);

	      if (first_time) {
		/* For I/O-map (goes to stdout) */
		printf("%d\n",ncols_tbl);
		first_time = 0;
	      }

	    }
	    else {
	      fprintf(stderr,
		      "***Error: Unable to overwrite existing file '%s'\n", fout);
	      fprintf(stderr,
		      "          Please remove it first or"
		      " use option \"-F\" to enforce overwriting (dangerous)\n");
	      errflg++;
	      FREE(p);
	      goto finish; /* Error */
	    }
	  } /* if (re_open) */

	  /* For I/O-map (goes to stdout) */
	  printf("%d %d %lld %lld %d\n",ipoolno,filenum,(long long int)offset,(long long int)(sizeof(hc32)+nbytes),nrows);

	  len = HDRSIZE;
	  cma_writei_(&Out, hc32, &len, &rc);
	  if (rc != len) {
	    fprintf(stderr,
		    "***Error: Unable to write HC32-header data to file '%s' : len=%d, rc=%d\n", 
		    fout, len, rc);
	    errflg++;
	    FREE(p);
	    goto finish; /* Error */
	  }
	  offset += sizeof(hc32);

	  len = INFOLEN;
	  cma_writei_(&Out, hdr, &len, &rc);
	  if (rc != len) {
	    fprintf(stderr,
		    "***Error: Unable to write ODB1-header data to file '%s' : len=%d, rc=%d\n", 
		    fout, len, rc);
	    errflg++;
	    FREE(p);
	    goto finish; /* Error */
	  }
	  offset += sizeof(hdr);

	  cma_writeb_(&Out, p, &ndata, &rc);
	  if (rc != ndata) {
	    fprintf(stderr,
		    "***Error: Unable to write data to file '%s' : ndata=%d, rc=%d\n", 
		    fout, ndata, rc);
	    errflg++;
	    FREE(p);
	    goto finish; /* Error */
	  }
	  offset += ndata;

	} /* if (rc == ndata && (nbytes > 0 ... */
	FREE(p);
	
      }
      else {
	fprintf(stderr,
		"***Error: Input file format for concatenation of file '%s' (logical='%s') was not recognized\n",
		fin,input);
	errflg++;
	goto finish; /* Error */
      } /* if (rc == 1 && icat == ODB_) else ... */
    }

    cma_close_(&In, &rc);
  next_jp:
    FREE(fin);
  } /* for (jp=1; jp<=npools; jp++) */

 finish:
  if (!errflg && Out != -1) {
    int rc;
    cma_close_(&Out, &rc);
    FREE(fout);
  }
  return;
}

/********/
/* main */
/********/

int main(int argc, char *argv[])
{
  int npools_in = -1;
  int c;

  errflg = 0;

  { /* get default values for grpsize from ODB_IO_GRPSIZE */
    char *env = getenv("ODB_IO_GRPSIZE");
    if (env) grpsize = atoi(env);
    grpsize = MAX(GRPSIZE,grpsize);
  }

  { /* get default values for grpsize from ODB_IO_FILESIZE */
    char *env = getenv("ODB_IO_FILESIZE");
    if (env) size_MB = atoi(env);
    size_MB = MAX(1,size_MB);
    size_MB = MIN(1000,size_MB);
  }

  while ((c = getopt(argc, argv, FLAGS)) != -1) {
    switch (c) {
    case '1': /* concatenate (#1 --> #4) */
      if (strequ(optarg,"4")) concat = 1;
      else {
	fprintf(stderr,
		"***Error: Argument -1 must follow '4'"
		" i.e. use -14. Found '%s'\n", optarg);
	errflg++;
      }
      break;
    case '4': /* split (#4 --> #1) */
      if (strequ(optarg,"1")) split = 1;
      else {
	fprintf(stderr,
		"***Error: Argument -4 must follow '1'"
		" i.e. use -41. Found '%s'\n", optarg);
	errflg++;
      }
      break;
    case 'F': /* enForce to overwrite -mode */
      overwrite = 1;
      break;
    case 'G': /* group size */
      grpsize = atoi(optarg);
      grpsize = MAX(1,grpsize);
      break;
    case '?':
    case 'h': /* print help */
      help = 1;
      break;
    case 'i': /* input file name(s) */
      if (input) FREE(input);
      input = STRDUP(optarg);
      break;
    case 'l': /* database name */
      if (dbname) FREE(dbname);
      dbname = STRDUP(optarg);
      break;
    case 'L': /* preferred file size in MB */
      size_MB = atoi(optarg);
      size_MB = MAX(1,size_MB);    /* 1MB lower limit */
      size_MB = MIN(1000,size_MB); /* 1GB upper limit; see ../lib/msgpass_loaddata.F90 and ../lib/msgpass_storedata.F90*/
      break; 
    case 'n': /* npools_in */
      npools_in = atoi(optarg);
      npools_in = MAX(1,npools_in);
      break;
    case 'o': /* output file name(s) */
      if (output) FREE(output);
      output = STRDUP(optarg);
      break;
    case 't': /* table name */
      if (table) FREE(table);
      table = STRDUP(optarg);
      break;
    case 'v': /* verbose */
      verbose = 1;
      break;
    case 'V': /* print version */
      version = 1;
      break;
    case 'Z': /* write zero length files */
      zerolen = 1;
      break;
    case 'z': /* do *not* write zero length files */
      zerolen = 0;
      break;
    default:
      errflg++;
      break;
    }
  }

  if (version) {
    fprintf(stderr,"hcat -- ODB's Horizontal conCATenate files -tool -- Revision %s, %s, %s\n",
	    vers, date_str, time_str);
    fprintf(stderr,"Copyright (c) 1998-2003, 2006 ECMWF. All Rights Reserved. ");
    fprintf(stderr,"Author: Sami Saarinen.\n");
  }

  if (help) {
    fprintf(stderr,USAGE,argv[0],SIZE_MB,GRPSIZE);
    return(errflg);
  }

  if (argc - 1 == optind) { /* wrong ? */
    errflg++;
  }

  if (concat + split != 1) { 
    fprintf(stderr,"***Error: Options [-14|-41] are mutually exclusive\n");
    errflg++;
  }

  if (!dbname) {
    fprintf(stderr,"***Error: Database label/name (-l dbname) must be given\n");
    errflg++;
  }

  if (!table) {
    fprintf(stderr,"***Error: Table name (-t table) must be given\n");
    errflg++;
  }

  if (!errflg) {
    int envlen = strlen("ODB_XXXXPATH_") + strlen(dbname) + 3; /* 3 : '=' + '.' + '\0' */
    char *env = NULL;
    char *p = NULL;
    ALLOCX(env, envlen);
    /* ODB_DATAPATH_<dbname> */
    sprintf(env,"ODB_DATAPATH_%s",dbname);
    p = getenv(env);
    if (!p) { /* i.e. not set; lets set it to current dir '.' */
      char *envcopy;
      sprintf(env,"ODB_DATAPATH_%s=.",dbname);
      envcopy = STRDUP(env); /* Remains allocated i.e. cannot be free()'d */
      putenv(envcopy);
      if (verbose) {
	p = strchr(env,'=');
	if (p) *p = '\0';
	p = getenv(env);
	fprintf(stderr,">Variable '%s' now points to '%s'\n",env,p ? p : NIL);
      }
    }
    /* ODB_SRCPATH_<dbname> */
    sprintf(env,"ODB_SRCPATH_%s",dbname);
    p = getenv(env);
    if (!p) { /* i.e. not set; lets set it to current dir '.' */
      char *envcopy;
      sprintf(env,"ODB_SRCPATH_%s=.",dbname);
      envcopy = STRDUP(env); /* Remains allocated i.e. cannot be free()'d */
      putenv(envcopy);
      if (verbose) {
	p = strchr(env,'=');
	if (p) *p = '\0';
	p = getenv(env);
	fprintf(stderr,">Variable '%s' now points to '%s'\n",env,p ? p : NIL);
      }
    }
    FREEX(env);
  }

  if (!errflg) { /* Get npools & ncols etc. */
    ncols_tbl = read_ddfile(dbname, table, &npools);
    if (verbose) fprintf(stderr,
			 "> npools=%d, npools_in=%d, ncols_tbl=%d\n",
			 npools, npools_in, ncols_tbl);
    if (npools_in >= 1) npools = MIN(npools_in, npools);
    grpsize = MIN(npools,grpsize);
  }

  if (!errflg) {
    if (!input) {
      int len = strlen(dbname) + strlen(table) + 10;
      ALLOC(input, len);
      sprintf(input,"%s.%s.%%d",dbname,table);
    }
    if (!output) {
      int len = strlen(table) + 10;
      ALLOC(output,len);
      sprintf(output,"_/%%d/%s",table);
    }
  }

  if (!errflg) {
    if (split)  split_file();
    else if (concat) concat_file();
  }

  if (errflg) {
    fprintf(stderr,USAGE,argv[0],SIZE_MB,GRPSIZE);
    return(errflg);
  }

  return(errflg);
}
