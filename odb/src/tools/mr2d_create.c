#include "mr2d_hdr.h"

static char program[]="mr2d_create";

/*

   mr2d_create: Creates a MR2D-concatenated file from given input file(s)

   Notes: Works only in a single processor mode i.e.
          no parallel $MRFSDIR access is available on VPP

*/	       

#define USAGE \
  "mr2d_create -i input_file[.%d]\n" \
  "            -o output_file\n" \
  "            -n number_of_input_files_in_connection_with_%d_format\n" \
  "            -r read_buf_size\n" \
  "            -w write_buf_size\n" \
  "            -p preallocation_size (VPP only)\n" \
  "            -e extent_size (VPP only)\n" \
  "            -v [verbose output]\n" \
  "            -A [input files are concatenated in ascending order]\n" \
  "            -D [input files are concatenated in descending order; the default]\n" \
  "            -V [print version number of the software]\n" \
  "            -help"

#define FLAGS "ADe:hi:n:o:p:r:vVw:"

#define CHECK_ERROR(f,n) if (rc != (n)) { perror(f); return(1); }


#define mr2d_create main

int mr2d_create(int argc, char *argv[])
{
  int rc, fs;
  FILE *fp_out;

  int verbose = 0;
  int print_version = 0;
  int extent = 0;
  int prealloc = 0;
  int readbufsize = 0;
  int writebufsize = 0;
  int descending = 1;
  int nfiles = 0;
  char *infile = NULL;
  char *outfile = NULL;

  IObuf in, out;

  unsigned int mr2dword = MR2D;
  unsigned int numchunks = 0;
  unsigned int *hdr = NULL;
  int hdrsize;
  unsigned int *index = NULL;
  unsigned int *chunk = NULL;
  int j, k;

  int c;
  int errflg = 0;
  extern char *optarg;
  extern int optind;

  /* Get command line options */

  while ((c = getopt(argc, argv, FLAGS)) != -1) {
    switch (c) {
    case 'A':
      descending = 0;
      break;
    case 'D':
      descending = 1;
      break;
    case 'e':
      extent = atoi(KiloMegaGiga(optarg));
      extent = MAX(0,extent);
      break;
    case 'i':
      infile = STRDUP(optarg);
      break;
    case 'n':
      nfiles = atoi(optarg);
      nfiles = MAX(0,nfiles);
      break;
    case 'o':
      outfile = STRDUP(optarg);
      break;
    case 'p':
      prealloc = atoi(KiloMegaGiga(optarg));
      prealloc = MAX(0,prealloc);
      break;
    case 'r':
      readbufsize = atoi(KiloMegaGiga(optarg));
      readbufsize = MAX(-1,readbufsize);
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
    case 'h':
      errflg++;
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
    fprintf(stderr,"Copyright (c) 1998 ECMWF. All Rights Reserved.\n");
  }

  if (!infile) errflg++;
  if (!outfile) errflg++;
  if (argc != optind) errflg++;
  if (nfiles <= 0) errflg++;

  if (errflg) {
    fprintf(stderr,"%s\n",USAGE);
    return(errflg);
  }

  numchunks = nfiles;
  hdrsize = 2 + 2 * numchunks;

  /* Open the output file */

  fp_out = fopen_prealloc(outfile,"w",prealloc,extent);

  if (!fp_out) {
    perror(outfile);
    return(1);
  }

  /* Obtain input file sizes */

  if (has_no_percent_sign(infile)) {
    char *p;
    
    ALLOC(p, strlen(infile) + 20);
    strcpy(p,infile);
    strcat(p,".%d");
    
    FREE(infile);
    infile = p;
  }

  /* Calculate chunk sizes */

  ALLOC(hdr, hdrsize);
  ALLOC(index, numchunks);
  ALLOC(chunk, numchunks);

  if (descending) {
    k = 0;
    for (j=numchunks; j>=1; j--) {
      index[k] = j;
      k++;
    }
  }
  else {
    for (j=1; j<=numchunks; j++) {
      index[j-1] = j;
    }
  }

  for (j=0; j<numchunks; j++) {
    char *file;
    ALLOC(file, strlen(infile) + 20);
    sprintf(file,infile,index[j]);
    chunk[j] = filesize(file);
    FREE(file);
  }

  
  hdr[0] = mr2dword;
  hdr[1] = numchunks;
  k = 2;
  for (j=0; j<numchunks; j++) {
    hdr[k + 0] = index[j];
    hdr[k + 1] = chunk[j];
    k += 2;
  }

  fs = hdrsize * sizeof(unsigned int);
  for (j=0; j<numchunks; j++) {
    fs += chunk[j];
  }

  if (writebufsize > 0 || writebufsize == -1) {
    out.len = (writebufsize == -1) ? fs : writebufsize;
    out.len = MIN(out.len, fs);
    SETBUF(fp_out, out);
  }
  else {
    out.len = 0;
    out.p = NULL;
  }

  rc = fwrite(hdr, sizeof(unsigned int), hdrsize, fp_out);
  CHECK_ERROR(outfile,hdrsize);

  FREE(hdr);

  /* Read in file-by-file and write to the file */

  if (verbose) {
    printf("%s: Concatenating input files to the '%s'\n",program,outfile);
    printf("Writing order %s\n",descending ? "descending" : "ascending");
    for (j=0; j<numchunks; j++) {
      printf("Chunk#%d for PE#%d : size = %d bytes\n",
	     j+1,index[j],chunk[j]);
    }
  }
  
  for (j=0; j<numchunks; j++) {
    unsigned char *data;
    int datasize;
    char *file;
    FILE *fp_in;
    
    ALLOC(file, strlen(infile) + 20);
    sprintf(file,infile,index[j]);
    
    datasize = chunk[j];
    if (verbose) {
      printf("Reading %d bytes from the file '%s' ...\n",
	     datasize,file);
    }

    fp_in = fopen(file,"r");
    if (!fp_in) {
      perror(file);
      return(1);
    }

    if (readbufsize == -1) readbufsize = datasize;
    if (readbufsize > 0) {
      in.len = MIN(readbufsize,datasize);
      SETBUF(fp_in, in);
    }
    else {
      in.len = 0;
      in.p = NULL;
    }

    ALLOC(data, datasize);
    rc = fread(data, sizeof(unsigned char), datasize, fp_in);
    CHECK_ERROR(infile,datasize);

    rc = fwrite(data, sizeof(unsigned char), datasize, fp_out);
    CHECK_ERROR(file,datasize);

    fclose(fp_in);

    FREEIOBUF(in);
    
    FREE(data);
    FREE(file);
  }

  fclose(fp_out);

  FREEIOBUF(out);

  FREE(index);
  FREE(chunk);

  return(0);
}

