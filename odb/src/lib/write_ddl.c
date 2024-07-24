#include "odb.h"

/* write_ddl.c */

PUBLIC void
write_ddl_(const int  *handle,
	   const int  *iounit,
	   int *retcode)
{
  int rc = 0;
  int Handle = *handle;
  FILE *fp = CMA_get_fp(iounit);
  ODB_Pool *p;

  if (!fp) { rc = -1; goto finish; }

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      ODB_Funcs *pf;
      char *dbname = p->dbname;
      int id;
      const char *dtbuf = odb_datetime_(NULL, NULL); /* ptr to a static variable; do NOT free */

      fprintf(fp,"\n");
      fprintf(fp,"// Schema for database '%s'\n",dbname);
      fprintf(fp,"// File created on %s\n",dtbuf);
      fprintf(fp,"\n");

      /* SET-variables */

      {
	ODB_Setvar *setvar = NULL;
	int nsetvar = ODB_get_vars(dbname, NULL, 0, 0, NULL);
	if (nsetvar > 0) {
	  ALLOC(setvar, nsetvar);
	  rc = ODB_get_vars(dbname, NULL, 0, nsetvar, setvar);
	  if (rc == nsetvar) {
	    int j;
	    for (j=0; j<nsetvar; j++) {
	      fprintf(fp,"SET %s = %.14g;\n",setvar[j].symbol,setvar[j].value);
	      FREE(setvar[j].symbol);
	    }
	  }
	  else {
	    rc = -2; 
	  }
	  FREE(setvar);
	  if (rc < 0) goto finish; 
	}
      }

      /* TYPE-definitions */

      id = 0;
      FORFUNC {
	if (PFCOM->is_table) {
	  int k;
	  int ntag = PFCOM->ntag;
	  char *table_name = PFCOM->name;
	  const ODB_Tags *tags = PFCOM->tags;
	  table_name++;
	  for (k=0; k<ntag; k++) {
	    int nmem = tags[k].nmem;
	    if (nmem > 0) {
	      char *m = tags[k].memb[nmem-1];
	      if (strequ(m,UNUSED)) nmem--;
	    }
	    if (nmem > 0) {
	      int i;
	      int nbits = 0;
	      char *memb_name = NULL;
	      char *type_name = STRDUP(tags[k].name);
	      char *c = strchr(type_name,':');
	      if (c) *c = '\0';

	      id++;
	      fprintf(fp,
		      "\nCREATE TYPE %s_%s_%d_t AS ( // ... with %d members\n",
		      table_name, type_name, id, nmem); 
	      FREE(type_name);

	      for (i=0; i<nmem; i++) {
		memb_name = STRDUP(tags[k].memb[i]);
		c = strchr(memb_name,' ');
		if (c) { /* Extract number of bits reserved for this member */
		  *c = '\0';
		  c++;
		  nbits = atoi(c);
		}
		fprintf(fp,"  %s bit%d,\n", memb_name, nbits);
		FREE(memb_name);
	      }

	      fprintf(fp,");\n");
	    } /* if (nmem > 0) */
	  } /* for (k=0; k<ntag; k++) */
	}
      } /* FORFUNC for TYPE-definitions */

      /* TABLE-definitions */

      id = 0;
      FORFUNC {
	if (PFCOM->is_table) {
	  int k;
	  int ntag = PFCOM->ntag;
	  char *table_name = PFCOM->name;
	  const ODB_Tags *tags = PFCOM->tags;
	  table_name++;
	  fprintf(fp,
		  "\nCREATE TABLE %s AS (\n",
		  table_name);
	  for (k=0; k<ntag; k++) {
	    int nmem = tags[k].nmem;
	    char *name = STRDUP(tags[k].name);
	    char *colon = strchr(name,':');
	    char *atsign = strchr(name,'@');
	    char *var_name, *type_name;

	    if (nmem > 0) {
	      char *m = tags[k].memb[nmem-1];
	      if (strequ(m,UNUSED)) nmem--;
	    }

	    if (atsign) *atsign = '\0';
	    if (colon)  *colon = '\0';

	    type_name = name;
	    var_name = colon ? colon + 1 : NIL;

	    if (nmem > 0) {
	      id++;
	      fprintf(fp,"  %s %s_%s_%d_t,\n", var_name, table_name, type_name, id);
	    }
	    else if (strnequ(var_name, "LINKOFFSET(", 11)) {
	      char *table_ref_name = &var_name[11];
	      char *x = table_ref_name;
	      while (*x != ')') {
		if (*x == '\0') break;
		x++;
	      }
	      *x = '\0';
	      fprintf(fp,"  %s @LINK,\n", table_ref_name);
	    }
	    else if (strnequ(var_name, "LINKLEN(", 8)) {
	      /* do nothing */
	    }
	    else {
	      fprintf(fp,"  %s %s,\n",var_name, type_name);
	    }

	    FREE(name);
	  } /* for (k=0; k<ntag; k++) */
	  fprintf(fp,");\n");
	}
      } /* FORFUNC for TABLE-definitions */

      break; /* The first occurence would do */
    }
  }

 finish:
  *retcode = rc;
}
