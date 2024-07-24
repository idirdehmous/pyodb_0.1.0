/* 
   A program to re-create "ddl_"-file for a given database
   using "dd"-file as input 

   Usage: dd2ddl < dd_file > ddl_file

   Following yet to be implemented :

   Usage: dd2ddl [options]

   options include: 

   -i input dd-file     (like: -i ECMA.dd)
   -o output ddl_-file  (like: -o ECMA.ddl_ ; the default prefix from the input dd-file)
   -l database_name     (like: -l ECMA ; the default from input dd-file)
   -s set_parameter     (like: -s '$abc = 1')

   If -i is not given, but -l is, then the input file name is
   "guessed" from -l's argument.

   Author: Sami Saarinen, ECMWF, 29-Aug-2000

 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
/* #include <malloc.h> */

typedef struct _Mem_t {
  char *name;
  int nbits;
} Mem_t;

typedef struct _Col_t {
  int id;
  char *name;
  char *type;
  int nmem;
  Mem_t *mem;
} Col_t;

typedef struct _Table_t {
  int id;
  char *name;
  int ncol;
  Col_t *col;
} Table_t;


typedef struct _Set_t {
  char *name;
  double value;
} Set_t;

static int 
IsNumber(const char *p) 
{
  int count = 0;
  if (p) {
    while (*p != '\0') {
      if (!isdigit(*p)) {
	count = 0;
	break;
      }
      count++;
      p++;
    }
  }
  return (count > 0) ? 1 : 0;
}

int main(int argc, char *argv[])
{
  int debug = 0;
  FILE *fpin = stdin;
  FILE *fpout = stdout;
  double Version_Major, Version_Minor;
  char CrDate[9], CrTime[7];
  char UDate[9] ,  UTime[7];
  char AnDate[9], AnTime[7];
  int Npools  = 0;
  int Ntables = 0;
  int t, p, j;
  int nel;
  int nsetvar;
  int colref = 0;
  Table_t *table = NULL;
  Set_t *set = NULL;
  int tmp1, tmp2;
  char tmpstr[256];
  char tmptype[256], tmpcol[256];
  int io_method = 1;
  char line[256];

  if (fgets(line, sizeof(line), fpin)) {
    nel = sscanf(line,"%lf %lf %d",&Version_Major, &Version_Minor, &io_method);
    if (nel == 2) { /* pre-25r4 */
      io_method = 1;
    }
  }
  
  fscanf(fpin,"%8s %6s\n", CrDate, CrTime); /* Date created */
  fscanf(fpin,"%8s %6s\n", UDate, UTime);   /* Date last updated */
  fscanf(fpin,"%8s %6s\n", AnDate, AnTime); /* Analysis date */
  fscanf(fpin,"%d\n", &Npools);
  fscanf(fpin,"%d\n", &Ntables);

  fprintf(fpout,"// Automatically generated DDL-file\n");
  fprintf(fpout,"// Contains %d tables\n", Ntables);

  table = (Table_t *)malloc(Ntables * sizeof(Table_t));

  for (t=0; t<Ntables; t++) {
    nel = fscanf(fpin,"%d @%s %d", &tmp1, tmpstr, &tmp2);
    if (debug) { 
      fprintf(stderr,"<debug(#%d/3)table:> %d @%s %d\n", nel, tmp1, tmpstr, tmp2); 
    }
    table[t].id = tmp1;
    table[t].name = strdup(tmpstr);
    table[t].ncol = 0;
    table[t].col = NULL;
    if (tmp2 != Npools && tmp2 != -1) {
      fprintf(stderr,
	      "***Error[1]: No. of pools found (=%d) for table '%s' not the expected one (=%d)\n",
	      tmp2, tmpstr, Npools);
      return(1);
    }
    /* Read dummy (for this application) */
    for (p=0; p<tmp2; p++) { 
      nel = fscanf(fpin,"%d",&tmp1);
      if (debug) { 
	fprintf(stderr," %d", tmp1); 
      }
    } /* for (p=0; p<tmp2; p++) */
    fscanf(fpin,"\n");
    if (debug) { fprintf(stderr,"\n"); }
  } /* for (t=0; t<Ntables; t++) */

  for (t=0; t<Ntables; t++) {
    nel = fscanf(fpin,"@%s %d\n", tmpstr, &tmp1);
    if (debug) { 
      fprintf(stderr,"<debug(#%d/2)TABLE:> @%s %d\n", nel, tmpstr, tmp1); 
    }
    if (strcmp(tmpstr, table[t].name) == 0) {
      int ncol = tmp1;
      int c;
      table[t].ncol = ncol;
      table[t].col = (Col_t *)malloc(ncol * sizeof(Col_t));
      for (c=0; c<ncol; c++) {
	nel = fscanf(fpin, "%s %d\n", tmptype, &tmp1);
	{
	  char *copy = strdup(tmptype);
	  char *x = tmptype;
	  while (*x != ':') {
	    if (*x == '\0') {
	      fprintf(stderr,
		      "***Error[4]: End of string occurred while processing type info at '%s'\n",
		      copy);
	      return(4);
	    }
	    x++;
	  }
	  *x++ = '\0';
	  strcpy(tmpcol, x);
	  x = tmpcol;
	  while (*x != '@') {
	    if (*x == '\0') {
	      fprintf(stderr,
		      "***Error[5]: End of string occurred while processing column info at '%s'\n",
		      copy);
	      return(5);
	    }
	    x++;
	  }
	  *x++ = '\0';
	  strcpy(tmpstr, x);
	  nel += 2;
	  free(copy);
	}

	if (debug) { 
	  fprintf(stderr,"<debug(#%d/4)col:> '%s':'%s'@'%s' '%d'\n", 
		  nel, tmptype, tmpcol, tmpstr, tmp1); 
	}
	if (strcmp(tmpstr, table[t].name) != 0) {
	  fprintf(stderr,
		  "***Error[3]: Unexpected table name '%s' found while extracting column '%s'. Expected '%s'\n",
		  tmpstr, tmpcol, table[t].name);
	  return(3);
	}
	table[t].col[c].id = ++colref;
	table[t].col[c].name = strdup(tmpcol);
	table[t].col[c].type = strdup(tmptype);
	table[t].col[c].nmem = tmp1;
	if (tmp1 > 0) {
	  int m;
	  int nmem = tmp1;
	  table[t].col[c].mem = (Mem_t *)malloc(nmem * sizeof(Mem_t));
	  for (m=0; m<nmem; m++) {
	    nel = fscanf(fpin, "%s %d\n", tmpstr, &tmp1);
	    if (debug) {
	      fprintf(stderr,"<debug(#%d/2)mem:> %s %d\n", 
		      nel, tmpstr, tmp1); 
	    }
	    table[t].col[c].mem[m].name = strdup(tmpstr);
	    table[t].col[c].mem[m].nbits = tmp1;
	  } /* for (m=0; m<nmem; m++) */
	}
	else {
	  table[t].col[c].mem = NULL;
	}
      } /* for (c=0; c<ncol; c++) */
    }
    else {
      fprintf(stderr,
	      "***Error[2]: Unexpected table name '%s' found. Expected '%s'\n",
	      tmpstr, table[t].name);
      return(2);
    } /* if (strcmp(tmpstr, table[t].name) == 0) */
  } /* for (t=0; t<Ntables; t++) */

  /* === SET-variables (if any) === */

  nsetvar = 0;
  nel = fscanf(fpin, "%d\n", &nsetvar);
  if (nel == 1 && nsetvar > 0) {
    char name[4096];
    double value;
    set = malloc(sizeof(*set) * nsetvar);
    for (j=0; j<nsetvar; j++) {
      set[j].name = NULL;
      set[j].value = 0;
    }
    for (j=0; j<nsetvar; j++) {
      nel = fscanf(fpin, "$%s %lf\n", name, &value);
      if (nel == 2) {
	set[j].name = strdup(name);
	set[j].value = value;
      }
      else {
	nsetvar = j-1;
	break;
      }
    }
  }


  /* === OUTPUT === */

  fprintf(fpout,"\n//-- SET-variables --\n\n");
  
  for (j=0; j<nsetvar; j++) {
    fprintf(fpout,"SET $%s = %.14g;\n", set[j].name, set[j].value);
  }

  fprintf(fpout,"\n//-- Typedefs --\n\n");

  for (t=0; t<Ntables; t++) {
    /* typedefs */
    int c;
    for (c=0; c<table[t].ncol; c++) {
      if (table[t].col[c].nmem > 0) {
	int m;
	int minc = 1;
	fprintf(fpout,"CREATE TYPE %s_%s_%d_t AS ( // Generated type name\n", 
		table[t].name, table[t].col[c].type, table[t].col[c].id);
	for (m=0; m<table[t].col[c].nmem; m+=minc) {
	  if (strchr(table[t].col[c].mem[m].name,'_')) {
	    int mm;
	    int nset = 0;
	    int n1 = 0, n2;
	    int refbits = table[t].col[c].mem[m].nbits;
	    char *refname = strdup(table[t].col[c].mem[m].name);
	    char *pos = strrchr(refname,'_');
	    if (pos) *pos = '\0';
	    for (mm=m; mm<table[t].col[c].nmem; mm++) {
	      int bits = table[t].col[c].mem[mm].nbits;
	      char *name = strdup(table[t].col[c].mem[mm].name);
	      pos = strrchr(name,'_');
	      if (pos) *pos = '\0';
	      if (pos && strcmp(name, refname) == 0 && bits == refbits) {
		char *z = &pos[1];
		int is_digit = IsNumber(z);
		nel = sscanf(z, "%d", &tmp1);
		if (nel != 1 || !is_digit) {
		  free(name);
		  break;
		}
		nset++;
		if (mm == m) n1 = tmp1;
	      }
	      free(name);
	    } /* for (mm=m; mm<table[t].col[c].nmem; mm++) */
	    n2 = n1 + nset - 1;
	    if (nset == 0) {
	      fprintf(fpout,"  %s bit%d,\n", table[t].col[c].mem[m].name, table[t].col[c].mem[m].nbits);
	    }
	    else {
	      fprintf(fpout,"  %s[%d:%d] bit%d,\n", refname, n1, n2, table[t].col[c].mem[m].nbits);
	      minc = nset;
	    }
	    free(refname);
	  }
	  else {
	    fprintf(fpout,"  %s bit%d,\n", 
		    table[t].col[c].mem[m].name, table[t].col[c].mem[m].nbits);
	  }
	} /* for (m=0; m<table[t].col[c].nmem; m++) */
	fprintf(fpout,");\n\n");
      } /* if (table[t].col[c].nmem > 0) */
    } /* for (c=0; c<table[t].ncol; c++) */
  }

  fprintf(fpout,"\n//-- Tables --\n\n");

  for (t=0; t<Ntables; t++) {
    /* tables */
    int c;
    int cinc = 1;
    fprintf(fpout,"CREATE TABLE %s AS (\n", table[t].name);
    for (c=0; c<table[t].ncol; c+=cinc) {
      cinc = 1;
      if (table[t].col[c].nmem > 0) {
	fprintf(fpout,"  %s %s_%s_%d_t,\n", 
		table[t].col[c].name, table[t].name, table[t].col[c].type, table[t].col[c].id);
      }
      else {
	if (strncmp(table[t].col[c].name, "LINKOFFSET(", 11) == 0) {
	  char *copy = strdup(table[t].col[c].name);
	  char *x = &table[t].col[c].name[11];
	  while (*x != ')') {
	    if (*x == '\0') {
	      fprintf(stderr,
		      "***Error[6]: End of string occurred while processing LINK info at '%s'\n",
		      copy);
	      return(6);
	    }
	    x++;
	  }
	  *x = '\0';
	  fprintf(fpout,"  %s @LINK,\n", &table[t].col[c].name[11]);
	  free(copy);
	}
	else if (strncmp(table[t].col[c].name, "LINKLEN(", 8) == 0) {
	  continue;
	}
	else if (strchr(table[t].col[c].name,'_')) {
	  int cc;
	  int nset = 0;
	  int n1 = 0, n2;
	  char *refname = strdup(table[t].col[c].name);
	  char *reftype = strdup(table[t].col[c].type);
	  char *pos = strrchr(refname,'_');
	  if (pos) *pos = '\0';
	  for (cc=c; cc<table[t].ncol; cc++) {
	    char *name = strdup(table[t].col[cc].name);
	    char *type = strdup(table[t].col[cc].type);
	    pos = strrchr(name,'_');
	    if (pos) *pos = '\0';
	    if (pos && strcmp(name, refname) == 0 && strcmp(type, reftype) == 0) {
	      char *z = &pos[1];
	      int is_digit = IsNumber(z);
	      nel = sscanf(z, "%d", &tmp1);
	      if (nel != 1 || !is_digit) {
		free(name);
		break;
	      }
	      nset++;
	      if (cc == c) n1 = tmp1;
	    }
	    free(name);
	  } /* for (cc=c; cc<table[t].ncol; cc++) */
	  n2 = n1 + nset - 1;
	  if (nset == 0) {
	    fprintf(fpout,"  %s %s,\n", table[t].col[c].name, table[t].col[c].type);
	  }
	  else {
	    fprintf(fpout,"  %s[%d:%d] %s,\n", refname, n1, n2, table[t].col[c].type);
	    cinc = nset;
	  }
	  free(refname);
	}
	else {
	  fprintf(fpout,"  %s %s,\n", table[t].col[c].name, table[t].col[c].type);
	}
      }
    } /* for (c=0; c<table[t].ncol; c+=cinc) */
    fprintf(fpout,");\n\n");
  } /* for (t=0; t<Ntables; t++) */

}

