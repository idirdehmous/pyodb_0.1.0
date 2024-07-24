
/* cmdbkeys.c */

#include "odb.h"

/* Fortran-callable functions to associate Fortran MDBxxx -keys
   with table/view columns at run-time */

/*--- Defines functions:

  Initialization (a scalar):
  CALL cmdb_reg('lat@hdr', 'MDBLAT', MDBLAT, it, rc)

  Initialization (a vector):
  CALL cmdb_vecreg('initial@update', 'MDBIOM0', MDBIOM0, 1, JPMXUP, it, rc)
  CALL cmdb_vecreg('LINK(body)@hdr', 'MLNK_hdr2body', MLNK_hdr2body, 1, 2, it, rc)

  Set MDB column indecides directly by using a vector of MDB-pointer addresses
  CALL cmdb_vecset(zaddr, n_zaddr, rc)

  Set MDBLAT to actual_column_index:
  CALL cmdb_set('lat@hdr', actual_column_index, it, rc)

  Reset all MDBxxx -keys to MDIDB:
  CALL cmdb_reset('*', reset_value, it, rc)

  Resets particular columns MDBxxx value to MDIDB:
  CALL cmdb_reset('lat@hdr', reset_value, it, rc)

  Inquire current value of MDBxxx -key:
  CALL cmdb_get('lat@hdr', get_value, it, rc)

  Inquire address of MDBxxx -key:
  CALL cmdb_addr('lat@hdr', zaddr, it, rc)

  Debug printout to STDERR:
  CALL cmdb_debug(enforce=[0|1],message)

  Returns string 'MDBxxx' for a given column :
  character(len=*) cMDBxxx
  CALL cmdb_name('lat@hdr', cMDBxxx, rc)

  Print 'MDBxxx' and their values and references to STDERR (unit=0; default) or STDOUT (unit=6)
  CALL cmdb_print(unit, message, only_active, it, rc)

 */


#ifndef MDB_HASHSIZE
#define MDB_HASHSIZE 1031U
#endif

PRIVATE uint CMDBKEYS_hashsize = 0;

typedef struct _MDB_Hash_Table {
  char *colname;
  char *mdbname;
  int *mdbkey; /* Note: Fortran INTEGER address */
  int active_cnt;
  struct _MDB_Hash_Table *collision;
} MDB_Hash_Table;

typedef  union _MDB_Union {
  int *mdbptr;
  double d;
} MDB_Union;

PRIVATE MDB_Hash_Table **CMDBKEYS_hashtable = NULL;

PRIVATE uint
Hash(const char *s, int slen, int *lenout)
{ 
  uint hashval = 0;
  const char *ss = s;
  for (; slen-- > 0 && *s && *s != ' ' ; s++) {
    hashval = (*s) + 31U * hashval;
  }
  hashval = hashval % CMDBKEYS_hashsize;
  if (lenout) *lenout = s - ss;
  return hashval;
}


PRIVATE void
HashInit()
{
  if (!CMDBKEYS_hashtable) {
    MDB_Hash_Table **tmp_CMDBKEYS_hashtable = NULL;
    int inumt = get_max_threads_();
    int it;
    char *p = getenv("ODB_MDB_HASHSIZE");
    if (p) {
      CMDBKEYS_hashsize = atoi(p);
      if (CMDBKEYS_hashsize <= 0) CMDBKEYS_hashsize = MDB_HASHSIZE;
    }
    else
      CMDBKEYS_hashsize = MDB_HASHSIZE;
    ALLOC(tmp_CMDBKEYS_hashtable, inumt + 1); /* entry 0 is unused, 1..inumt for threads */
    for (it = 0; it <= inumt ; it++) {
      CALLOC(tmp_CMDBKEYS_hashtable[it], CMDBKEYS_hashsize);
    }
    CMDBKEYS_hashtable = tmp_CMDBKEYS_hashtable;
  }
}

PRIVATE MDB_Hash_Table *
NewEntry(void)
{
  MDB_Hash_Table *p;
  ALLOC(p, 1);
  p->colname = NULL;
  p->mdbname = NULL;
  p->mdbkey  = NULL;
  p->active_cnt = 0;
  p->collision = NULL;
  return p;
}

PUBLIC void
init_CMDBKEYS_lock()
{
  HashInit();
}

/*
  Returns string 'MDBxxx' for a given column :
  character(len=*) cMDBxxx
  CALL cmdb_name('lat@hdr', cMDBxxx, rc)
 */

PUBLIC void 
cmdb_name_(const char *colname,
           char mdbname[],
           int *retcode,
           /* Hidden arguments */
                 int colname_len,
           const int mdbname_len)
{
  Bool is_formula = false;
  int rc = 0;

  if (mdbname_len > 0) {
    int it = 1; /* In this case getting the "it" from the master thread#1 is ok for any thread */
    uint index;
    MDB_Hash_Table *p;
    DECL_FTN_CHAR(colname);
    DECL_FTN_CHAR(mdbname);

    ALLOC_FTN_CHAR(colname);
    ALLOC_OUTPUT_FTN_CHAR(mdbname);

    rc = index = Hash(p_colname, colname_len, NULL);
    p = &CMDBKEYS_hashtable[it][index];

    if (p && p->colname) {
      while (p && p->colname) {
        if (strequ(p->colname, p_colname)) {
          strncpy(p_mdbname, p->mdbname, mdbname_len);
          break; /* The first occurence will do */
        }
        if (p->collision) 
          p = p->collision;
        else {
	  int myproc = 0;
	  codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
	  is_formula = strstr(p_colname,"@Formula") ? true : false; /* bailout if a formula */
          rc = -2;
/*
	  if (!is_formula || (is_formula && myproc == 1)) {
	    fprintf(stderr,
		    "cmdb_name_:  Unrecognized column name '%s' for it#%d [rc=-2]\n",
		    p_colname, it);
	  }
*/
          goto finish; /* Unrecognized column name */
        }
      } /* while (p && p->colname) */  
    }
    else {
      int myproc = 0;
      codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
      is_formula = strstr(p_colname,"@Formula") ? true : false; /* bailout if a formula */
      rc = -1;
      if (!is_formula || (is_formula && myproc == 1)) {
	fprintf(stderr,
		"cmdb_name_:  Column '%s' is not registered for it#%d [rc=-1]\n",
		p_colname, it);
      }
      goto finish; /* Column not registered */
    } /* if (p && p->colname) ... else ... */ 

  finish:
    COPY_2_FTN_CHAR(mdbname);
    FREE_FTN_CHAR(colname);

    if (rc < 0 && !is_formula) RAISE(SIGABRT);
  } /* if (mdbname_len > 0) */

  *retcode = rc;
}

/*
 */

/*---
  Initialization (a scalar):
  CALL cmdb_reg('lat@hdr', 'MDBLAT', MDBLAT, it, rc)
  ---*/

PUBLIC void
cmdb_reg_(const char *colname,
          const char *mdbname,
          int *mdbkey,
          const int *It,
          int *retcode,
          /* Hidden arguments */
          int colname_len,
          int mdbname_len)
{
  int rc = 0;
  int it = *It;
  uint index;
  MDB_Hash_Table *p;
  DECL_FTN_CHAR(colname);
  DECL_FTN_CHAR(mdbname);

  ALLOC_FTN_CHAR(colname);
  ALLOC_FTN_CHAR(mdbname);

  rc = index = Hash(p_colname, colname_len, NULL);
  p = &CMDBKEYS_hashtable[it][index];

  while (p && p->colname) {
    if (strequ(p->colname, p_colname)) {
      if (p->mdbkey != mdbkey) {
	fprintf(stderr,
		"cmdb_reg_(it#%d): Cannot re-register colname='%s' to a "
		"different (mdbname,addr)=('%s',%p)"
		" since already registered to ('%s',%p, it#%d)\n",
		it, p_colname, p_mdbname, mdbkey, p->mdbname, p->mdbkey, it);
	rc = -1;
	RAISE(SIGABRT);
      }
      goto finish; /* Already in list */
    }
    if (!p->collision) p->collision = NewEntry();
    p = p->collision;
  } /* while (p && p->colname) */

  p->colname = STRDUP(p_colname);
  p->mdbname = STRDUP(p_mdbname);
  p->mdbkey  = mdbkey;
  p->active_cnt = 0;

 finish:
  FREE_FTN_CHAR(mdbname);
  FREE_FTN_CHAR(colname);

  *retcode = rc;
}

/*---
  Initialization (a vector):
  CALL cmdb_vecreg('initial@update', 'MDBIOM0', MDBIOM0, 1, JPMXUP, it, rc)
  CALL cmdb_vecreg('LINK(body)@hdr', 'MLNK_hdr2body', MLNK_hdr2body, 1, 2, it, rc)
  ---*/

PUBLIC void
cmdb_vecreg_(const char *colname,
             const char *mdbname,
             int vecmdbkey[],
             const int *low,
             const int *high,
             const int *It,
             int *retcode,
             /* Hidden arguments */
             int colname_len,
             int mdbname_len)
{
  int rc = 0;
  int it = *It;
  int k1 = *low;
  int k2 = *high;
  DECL_FTN_CHAR(colname);
  DECL_FTN_CHAR(mdbname);

  ALLOC_FTN_CHAR(colname);
  ALLOC_FTN_CHAR(mdbname);

  if (k1 == 1 && k2 == 2 && strnequ(p_colname,"LINK(",5)) {
    /* We have to set up for LINKOFFSET (=1) and LINKLEN (=2) */
    char *col, *mdb;
    int *addr;
    int lencol, lenmdb;
    char *s = strchr(p_colname,'(');
    
    lencol = strlen("LINKOFFSET") + strlen(s) + 1;
    ALLOC(col, lencol);
    sprintf(col,"LINKOFFSET%s",s);
    lenmdb = strlen(p_mdbname) + 4;
    ALLOC(mdb, lenmdb);
    sprintf(mdb,"%s(1)",p_mdbname);
    addr = &vecmdbkey[k1-1];
    cmdb_reg_(col, mdb, addr, It, &rc, lencol, lenmdb);
    FREE(col);
    FREE(mdb);
    if (rc < 0) goto finish;

    lencol = strlen("LINKLEN") + strlen(s) + 1;
    ALLOC(col, lencol);
    sprintf(col,"LINKLEN%s",s);
    lenmdb = strlen(p_mdbname) + 4;
    ALLOC(mdb, lenmdb);
    sprintf(mdb,"%s(2)",p_mdbname);
    addr = &vecmdbkey[k2-1];
    cmdb_reg_(col, mdb, addr, It, &rc, lencol, lenmdb);
    FREE(col);
    FREE(mdb);
    if (rc < 0) goto finish;
  }
  else if (k1 <= k2) {
    char *col, *mdb;
    int *addr;
    int lencol, lenmdb;
    int k;
    char *s = strchr(p_colname,'@');
    char *str;
    char *p;
    double d;
    int numdigits;

    if (!s) {
      fprintf(stderr,
              "cmdb_vecreg_: colname='%s' for mdbname='%s' has no '@'-sign\n",
              p_colname, p_mdbname);
      rc = -1;
      RAISE(SIGABRT);
      goto finish;
    }

    str = STRDUP(p_colname);
    p = strchr(str,'@');
    *p = '\0';

    d = ABS(MAX(ABS(k1), ABS(k2)));
    d = MAX(d,1);
    numdigits = log10(d) + 1;

    lencol = strlen(str) + strlen(p+1) + numdigits + 4;
    ALLOC(col, lencol);

    lenmdb = strlen(p_mdbname) + numdigits + 3;
    ALLOC(mdb, lenmdb);

    if (k1 < 0) {
      int kend = MIN(k2,-1);
      for (k=k1; k<=kend; k++) {
        sprintf(col,"%s__%d@%s",str,ABS(k),p+1);
        sprintf(mdb,"%s(%d)",p_mdbname,ABS(k));
        addr = &vecmdbkey[k-1];
        cmdb_reg_(col, mdb, addr, It, &rc, strlen(col), strlen(mdb));
        if (rc < 0) break;
      }
      k1 = 0;
    }

    if (rc == 0 && k1 >= 0) {
      for (k=k1; k<=k2; k++) {
        sprintf(col,"%s_%d@%s",str,k,p+1);
        sprintf(mdb,"%s(%d)",p_mdbname,k);
        addr = &vecmdbkey[k-1];
        cmdb_reg_(col, mdb, addr, It, &rc, strlen(col), strlen(mdb));
        if (rc < 0) break;
      }
    }

    FREE(mdb);
    FREE(col);
    FREE(str);

    if (rc < 0) goto finish;
  }
  else {
    fprintf(stderr,
            "cmdb_vecreg_: start index=%d greater than end index=%d in colname='%s', mdbname='%s'\n",
            k1, k2,
            p_colname, p_mdbname);
    rc = -2;
    RAISE(SIGABRT);
    goto finish;
  }

 finish:
  FREE_FTN_CHAR(mdbname);
  FREE_FTN_CHAR(colname);

  *retcode = rc;
}

/*---
  Set MDB column indecides directly by using a vector of MDB-pointer addresses
  CALL cmdb_vecset(zaddr, n_zaddr, rc)
  ---*/

PUBLIC void
cmdb_vecset_(const double zaddr[],
	     const int *n_zaddr,
	     int *retcode)
{
  int j, ncols = *n_zaddr;
  MDB_Union u;
  for (j=0; j<ncols; j++) {
    u.d = zaddr[j];
    if (u.mdbptr) *u.mdbptr = j+1; /* "+1" due to Fortran indexing */
  }
  *retcode = ncols;
}

/*---
  Set MDBLAT to actual_column_index:
  CALL cmdb_set('lat@hdr', actual_column_index, it, rc)
  ---*/


PUBLIC void
cmdb_set_(const char *colname,
          const int *actual_column_index,
          const int *It,
          int *retcode,
          /* Hidden arguments */
          int colname_len)
{
  /* Note: This routine is optimized i.e. DECL_FTN, ALLOC_FTN -stuff removed 
           and actual colname_len obtained while evaluating Hash() */
  int rc = 0;
  int count = 0;
  int it = *It;
  uint index;
  MDB_Hash_Table *p;

  rc = index = Hash(colname, colname_len, &colname_len);
  p = &CMDBKEYS_hashtable[it][index];

  while (p && p->colname) {
    if (strnequ(p->colname, colname, colname_len)) {
      if (p->mdbkey) *p->mdbkey = *actual_column_index;
      p->active_cnt++;
      count++;
    }
    p = p->collision;
  } /* while (p && p->colname) */

  if (count == 0) {
    fprintf(stderr,
            "cmdb_set_: Column '%*s' not registered for it#%d\n",
            colname_len, colname, it);
    rc = -2;
    RAISE(SIGABRT);
    goto finish; /* Column not found */
  }

 finish:

  *retcode = rc;
}



/*---
  Reset all MDBxxx -keys to MDIDB:
  CALL cmdb_reset('*', reset_value, it, rc)

  Resets particular columns MDBxxx value to MDIDB:
  CALL cmdb_reset('lat@hdr', reset_value, it, rc)
  ---*/


PUBLIC void
cmdb_reset_(const char *colname,
            int *reset_value,
            const int *It,
            int *retcode,
            /* Hidden arguments */
            int colname_len)
{
  int rc = 0;
  int it = *It;

  if (strnequ(colname,"*",colname_len)) {
    uint j;
    for (j=0; j<CMDBKEYS_hashsize; j++) {
      MDB_Hash_Table *p = &CMDBKEYS_hashtable[it][j];
      while (p && p->colname) {
        if (p->mdbkey) {
          *p->mdbkey = *reset_value;
          p->active_cnt = 0;
        }
        p = p->collision;
      } /* while (p && p->colname) */
    } /* for (j=0; j<CMDBKEYS_hashsize; j++) */
  }
  else {
    cmdb_set_(colname, reset_value, It, &rc, colname_len);
  }

  /* finish: */

  *retcode = rc;
}


/*---
  Inquire current value of MDBxxx -key:
  CALL cmdb_get('lat@hdr', get_value, it, rc)
  ---*/


PUBLIC void
cmdb_get_(const char *colname,
          int *get_value,
          const int *It,
          int *retcode,
          /* Hidden arguments */
          int colname_len)
{
  int rc = 0;
  int it = *It;
  int found = 0;
  uint index;
  MDB_Hash_Table *p;
  DECL_FTN_CHAR(colname);

  ALLOC_FTN_CHAR(colname);

  rc = index = Hash(p_colname, colname_len, NULL);
  p = &CMDBKEYS_hashtable[it][index];

  while (p && p->colname) {
    if (strequ(p->colname, p_colname)) {
      found = 1;
      break; /* The first occurence will do */
    }
    if (p->collision) 
      p = p->collision;
    else 
      break; /* not found */
  } /* while (p && p->colname) */

  if (!found) {
    fprintf(stderr,
            "cmdb_set_: Column '%s' not registered for it#%d\n",
            p_colname, it);
    rc = -2;
    RAISE(SIGABRT);
    goto finish; /* Unrecognized column name */
  }
  
  *get_value = p->mdbkey ? *p->mdbkey : 0;

 finish:
  FREE_FTN_CHAR(colname);

  *retcode = rc;
}

/*---
  Inquire address of MDBxxx -key:
  CALL cmdb_addr('lat@hdr', zaddr, it, rc)
  ---*/

PUBLIC void
cmdb_addr_(const char *colname,
	   double *zaddr,
	   const int *It,
	   int *retcode,
	   /* Hidden arguments */
	   int colname_len)
{
  int rc = 0;
  int it = *It;
  int found = 0;
  uint index;
  MDB_Hash_Table *p;
  MDB_Union u;
  DECL_FTN_CHAR(colname);

  u.d = 0;

  ALLOC_FTN_CHAR(colname);

  rc = index = Hash(p_colname, colname_len, NULL);
  p = &CMDBKEYS_hashtable[it][index];

  while (p && p->colname) {
    if (strequ(p->colname, p_colname)) {
      found = 1;
      break; /* The first occurence will do */
    }
    if (p->collision) 
      p = p->collision;
    else 
      break; /* not found */
  } /* while (p && p->colname) */

  if (!found) {
    fprintf(stderr,
            "cmdb_set_: Column '%s' not registered for it#%d\n",
            p_colname, it);
    rc = -2;
    RAISE(SIGABRT);
    goto finish; /* Unrecognized column name */
  }
  
  u.mdbptr = p->mdbkey;

 finish:
  FREE_FTN_CHAR(colname);

  *zaddr = u.d;
  *retcode = rc;
}

/*---
  Debug printout to STDERR:
  CALL cmdb_debug(enforce=[0|1],message)
  ---*/

PUBLIC void
cmdb_debug_(const int *enforce,
            const char *msg,
            /* Hidden arguments */
            int msg_len)
{
  DECL_FTN_CHAR(msg);
  int print_it = *enforce;
  int j;
  int entries=0;
  int hits=0;
  char *env = getenv("ODB_MDB_DEBUG");
  
  if (env) {
    int ienv = atoi(env);
    ienv = MAX(0,MIN(1,ienv));
    print_it |= ienv;
  }

  if (!print_it) return;

  ALLOC_FTN_CHAR(msg);

  if (msg_len > 0) fprintf(stderr,"cmdb_debug_: %s\n",p_msg);
  fprintf(stderr,
          "cmdb_debug_: hashsize=%u, hashtable at %p\n",
          CMDBKEYS_hashsize, CMDBKEYS_hashtable);

  if (CMDBKEYS_hashtable) {
    int inumt = get_max_threads_();
    int it;
    for (it = 1; it <= inumt ; it++) {
      for (j=0; j<CMDBKEYS_hashsize; j++) {
        MDB_Hash_Table *p = &CMDBKEYS_hashtable[it][j];
        int k=1;
        if (p->colname) entries++;
        while (p && p->colname) {
          fprintf(stderr,
		  "hash#%d [hit#%d, thread#%d] : '%s' alias '%s' = %d (0x%x) at "
		  "addr=%p (active_cnt=%d)\n",
                  j,k,it,
                  p->colname ? p->colname : NIL, 
                  p->mdbname ? p->mdbname : NIL,
                  p->mdbkey ? *p->mdbkey : 0, p->mdbkey ? *p->mdbkey : 0,
                  p->mdbkey, p->active_cnt);
          p = p->collision;
          k++;
          hits++;
        } /* while (p && p->colname) */
      } /* for (j=0; j<CMDBKEYS_hashsize; j++) */
    } /* for (it = 1; it <= inumt ; it++) */
  } /* if (CMDBKEYS_hashtable) */

  fprintf(stderr,"cmdb_debug_: Total %d hits in %d hash-entries\n",hits,entries);

  FREE_FTN_CHAR(msg);
}

/*---
  Print 'MDBxxx' and their values and references to STDERR (unit=0; default) or STDOUT (unit=6)
  CALL cmdb_print(unit, message, only_active, it, rc)
  ---*/

PUBLIC void
cmdb_print_(const int *unit,
            const char *msg,
            const int *only_active,
            const int *It,
            int *retcode
            /* Hidden arguments */
            ,int msg_len)
{
  int it = MAX(1,*It);
  int Unitno = *unit;
  FILE *fp = (Unitno == 6) ? stdout : stderr;
  int Only_Active = (*only_active > 0) ? 1 : 0;
  int j;
  int nlp = 0;
  DECL_FTN_CHAR(msg);

  ALLOC_FTN_CHAR(msg);

  if (!fp) goto finish;

  fprintf(fp,
          "cmdb_print_(%s, only_active=%d, it#%d): hash: (size,table-addr)=(%u,%p)\n",
          p_msg, Only_Active, it,
          CMDBKEYS_hashsize, CMDBKEYS_hashtable);
  fflush(fp);

  if (CMDBKEYS_hashtable) {
    for (j=0; j<CMDBKEYS_hashsize; j++) {
      MDB_Hash_Table *p = &CMDBKEYS_hashtable[it][j];
      while (p && p->colname) {
        int print_it = (Only_Active == 0);
        print_it |= (Only_Active == 1 && (p->active_cnt > 0));
        if (print_it) {
          fprintf(fp,
                  "\t[%s;#%d] : '%s' => '%s' = %d (0x%x) at addr=%p (active#%d)\n",
                  p_msg, it,
                  p->colname ? p->colname : NIL, 
                  p->mdbname ? p->mdbname : NIL,
                  p->mdbkey? *p->mdbkey : 0, p->mdbkey ? *p->mdbkey : 0,
                  p->mdbkey, p->active_cnt);
          fflush(fp);
          nlp++;
        } /* if (print_it) */
        p = p->collision;
      } /* while (p && p->colname) */
    } /* for (j=0; j<CMDBKEYS_hashsize; j++) */
  } /* if (CMDBKEYS_hashtable) */

  fprintf(fp,"End of cmdb_print_(%s): Total lines printed = %d. (it#%d)\n",
          p_msg, nlp, it);
  fflush(fp);

 finish:
  FREE_FTN_CHAR(msg);

  *retcode = nlp;
}
