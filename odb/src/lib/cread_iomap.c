
/* cread_iomap.c */

#include "cmaio.h"

#define MAXLEN 512

typedef const int cint_t;
extern void create_iomap_( /* A Fortran routine in "odb/lib/create_iomap.F90" */
    cint_t *khandle, cint_t *kfirst_call, cint_t *kmaxpools,
    cint_t *kpoff, cint_t *kfblk, const char *cdfile, cint_t *kgrpsize,
    cint_t *ktblno, const char *cdtbl,
    cint_t *kpoolno, cint_t *kfileno, cint_t *koffset, cint_t *klength, cint_t *krows, cint_t *kcols,
    int *kret
    /* Hidden arguments */
    , long cdfile_len, long cdtbl_len);

PUBLIC void
cread_iomap_(const int *handle,
	     const char *file,
	     int *fileblockno,
	     int *pool_offset,
	     int *retcode
	     /* Hidden arguments */
	     , int file_len)
{
  int Handle = *handle;
  int Fblk = *fileblockno;
  int Poff = *pool_offset;
  int io, rc, fmt=0;
  int nelem = 0;
  static int myproc = 0;
  char s[MAXLEN+1];
  FILE *fp = NULL;
  DECL_FTN_CHAR(file);
  extern void codb_procdata_(int *myproc, int *nproc, int *pid,
			     int *it, int *inumt);

  if (myproc == 0) codb_procdata_(&myproc, NULL, NULL, NULL, NULL);

  ALLOC_FTN_CHAR(file);
  io = -1;
  if (myproc == 1) fprintf(stderr, "Opening IOMAP-file='%s' for reading\n",p_file);
  cma_open_(&io, p_file, "r", &rc, file_len, strlen("r"));
  if (rc != 1) goto finish;

  fp = CMA_get_fp(&io);
  if (!fp) goto finish;

  nelem = fscanf(fp, "%d\n", &fmt);
  if (myproc == 1) fprintf(stderr, "\tFormat=%d\n",fmt);

  if (fmt == 1) { /* Automatically created */
    int maxtables=0, maxpools=0, grpsize=0, pad=0, io_method=0, length_multiplier=0;
    nelem = fscanf(fp, "%s\n%d %d %d\n%d %d %d\n", 
		   s, &maxtables, &maxpools, &grpsize,
		   &pad, &io_method, &length_multiplier);
    if (nelem == 7) {
      int first_call = 1;
      Fblk++;
      if (myproc == 1) {
	fprintf(stderr, 
	"\tFile block#%d: tables=%d, maxpools=%d, grpsize=%d, pad=%d, io_method=%d, lenmult=%d\n",
	   Fblk, maxtables, maxpools, grpsize, pad, io_method, length_multiplier);
      }
      for (;;) { /* TABLE_LOOP */
	char *tblname = s;
	int tblno=0, ncols=0;
	nelem = fscanf(fp, "%d %d %s\n", &tblno, &ncols, tblname);
	/*
	if (myproc == 1) {
	  fprintf(stderr,
		  "\t tbl#%d, ncols=%d, tblname='%s', nelem=%d\n", 
		  tblno, ncols, tblname, nelem);
	}
	*/
	if (nelem != 3 || tblno < 1 || tblno > maxtables) break; /* TABLE_LOOP */
	for (;;) { /* POOL_LOOP */
	  int poolno=0, fileno=0, offset=0, length=0, nrows=0;
	  nelem = fscanf(fp, "%d %d %d %d %d\n",  &poolno, &fileno, &offset, &length, &nrows);
/*AFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
 *AF 24/08/09 bug found - if several "empty" databases are created and merged together
 *AF it does not create the iomap structure (%nfileblocks is then 0) and cannot write it back
 *AF when we close the database (because if a database exists but it empty we have -1 for poolno in
 *AF the iomap file
 *AF as a workaround, do create a table ("desc") which is then overwritten. */
	  if (nelem != 5 || poolno < 1 || poolno > maxpools) break; /* POOL_LOOP */
	  /* Pass data to Fortran-layer (after scaling offset & length to be byte-units) */
	  if (length_multiplier > 1) {
	    offset *= length_multiplier;
	    length *= length_multiplier;
	  }
	  /* if (first_call && myproc == 1) { fprintf(stderr,"\t first_call; Fblk#%d\n", Fblk); } */
	  create_iomap_(&Handle, &first_call, &maxpools,
			&Poff, &Fblk, p_file, &grpsize,
			&tblno, tblname,
			&poolno, &fileno, &offset, &length, &nrows, &ncols,
			&rc,
			strlen(p_file), strlen(tblname));
	  first_call = 0;
	} /* POOL_LOOP */
      } /* TABLE_LOOP */
      Poff += maxpools;
    } /* if (nelem == 6) */
  }
  else if (fmt == 2) { /* Created normally by a script */
    char *next_iomap_file = s;
    for (;;) { /* FILEBLK_LOOP */
      if (feof(fp)) break; /* FILEBLK_LOOP */
      /* get another iomap-file */
      nelem = fscanf(fp, "%s\n", next_iomap_file);
      if (nelem != 1) break; /* FILEBLK_LOOP */
      cread_iomap_(&Handle, next_iomap_file, 
		   &Fblk, &Poff, 
		   &rc, strlen(next_iomap_file));
    } /* FILEBLK_LOOP */
  }
  
  cma_close_(&io, &rc);
  *pool_offset = Poff;
  *fileblockno = Fblk;

 finish: 
  FREE_FTN_CHAR(file);

  *retcode = rc;
}
