#include "mr2d_hdr.h"

static char program[]="mr2d_split";

/*

   mr2d_split: From a given concatenated file creates
               individual files as they were before concatenation.
	       Number of files to be created is defined from the 
	       input file's M2RD-header.

   Notes: Works only in a single processor mode i.e.
          no parallel $MRFSDIR access is available on VPP


*/	       

#define USAGE \
  "mr2d_split  -i input_file\n" \
  "            -o output_file[.%d]\n" \
  "            -r read_buf_size\n" \
  "            -w write_buf_size\n" \
  "            -p preallocation_size (VPP only)\n" \
  "            -e extent_size (VPP only)\n" \
  "            -v [verbose output]\n" \
  "            -V [print version number of the software]\n" \
  "            -help"

#define FLAGS "e:hi:o:p:r:vVw:"


#define CHECK_ERROR(f,n) if (rc != (n)) { perror(f); return(1); }


#define mr2d_split main

int mr2d_split(int argc, char *argv[])
{
  int rc, fs;
  FILE *fp_in;

  int verbose = 0;
  int print_version = 0;
  int extent = 0;
  int prealloc = 0;
  int readbufsize = 0;
  int writebufsize = 0;
  char *infile = NULL;
  char *outfile = NULL;

  IObuf in, out;

  unsigned int mr2dword = 0;
  unsigned int numchunks = 0;

  int c;
  int errflg = 0;
  extern char *optarg;
  extern int optind;

  /* Get command line options */

  while ((c = getopt(argc, argv, FLAGS)) != -1) {
    switch (c) {
    case 'e':
      extent = atoi(KiloMegaGiga(optarg));
      extent = MAX(0,extent);
      break;
    case 'i':
      infile = STRDUP(optarg);
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

  if (errflg) {
    fprintf(stderr,"%s\n",USAGE);
    return(errflg);
  }

  /* Process the input file */

  fp_in = fopen(infile,"r");

  if (!fp_in) {
    perror(infile);
    return(1);
  }

  fs = filesize_by_fp(fp_in, infile);
  if (fs < 8) {
    fprintf(stderr,"%s: Invalid file length %d bytes\n",infile,fs);
    return(1);
  }

  if (readbufsize == -1) readbufsize = fs;
  if (readbufsize > 0) {
    in.len = MIN(readbufsize,fs);
    SETBUF(fp_in, in);
  }
  else {
    in.len = 0;
    in.p = NULL;
  }

  rc = fread(&mr2dword, sizeof(unsigned int), 1, fp_in);
  CHECK_ERROR(infile,1);

  if (mr2dword != MR2D) {
    fprintf(stderr,"%s: File is not an MR2D-file\n",infile);
    return(1);
  }

  rc = fread(&numchunks, sizeof(unsigned int), 1, fp_in);
  CHECK_ERROR(infile,1);

  if (verbose) {
    printf("%s: Splitting file '%s' into %d chunks ...\n",
	   program,infile,numchunks);
  }


  /* Read in chunk-by-chunk and write out to the corresponding output files */

  if (numchunks > 0) {
    int j, k;
    unsigned int sum;
    unsigned int *hdr = NULL;
    int hdrsize = 2 * numchunks;
    unsigned int *index = NULL;
    unsigned int *chunk = NULL;
    
    ALLOC(hdr, hdrsize);
    ALLOC(index, numchunks);
    ALLOC(chunk, numchunks);

    rc = fread(hdr, sizeof(unsigned int), hdrsize, fp_in);
    CHECK_ERROR(infile,hdrsize);

    k = 0;
    for (j=0; j<numchunks; j++) {
      index[j] = hdr[k + 0];
      chunk[j] = hdr[k + 1];
      k += 2;
    }

    if (verbose) {
      for (j=0; j<numchunks; j++) {
	printf("Chunk#%d for PE#%d : size = %d bytes\n",
	       j+1,index[j],chunk[j]);
      }
    }

    /* Cross-check that the sum of hdrlen and fsizes equals to filesize */

    sum = (1 + 1 + hdrsize) * sizeof(unsigned int);
    for (j=0; j<numchunks; j++) {
      sum += chunk[j];
    }

    if (sum != fs) {
      fprintf(stderr,"%s: Chunksize(s) mismatch with filesize (=%d)\n",
	      infile,fs);
      return(1);
    }

    if (has_no_percent_sign(outfile)) {
      char *p;

      ALLOC(p, strlen(outfile) + 20);
      strcpy(p,outfile);
      strcat(p,".%d");

      FREE(outfile);
      outfile = p;
    }

    for (j=0; j<numchunks; j++) {
      unsigned char *data;
      int datasize;
      char *file;
      FILE *fp_out;

      ALLOC(file, strlen(outfile) + 20);
      sprintf(file,outfile,index[j]);

      datasize = chunk[j];
      if (verbose) {
	printf("Writing %d bytes to the file '%s' ...\n",
	       datasize,file);
      }

      fp_out = fopen_prealloc(file,"w",prealloc,extent);
      if (!fp_out) {
	perror(file);
	return(1);
      }

      if (writebufsize > 0 || writebufsize == -1) {
	out.len = (writebufsize == -1) ? datasize : writebufsize;
	out.len = MIN(out.len, datasize);
	SETBUF(fp_out, out);
      }
      else {
	out.len = 0;
	out.p = NULL;
      }

      ALLOC(data, datasize);
      rc = fread(data, sizeof(unsigned char), datasize, fp_in);
      CHECK_ERROR(infile,datasize);

      rc = fwrite(data, sizeof(unsigned char), datasize, fp_out);
      CHECK_ERROR(file,datasize);

      fclose(fp_out);

      FREEIOBUF(out);

      FREE(data);
      FREE(file);
    }

    fclose(fp_in);

    FREE(hdr);
    FREE(index);
    FREE(chunk);
  }

  FREEIOBUF(in);

  return(0);
}

