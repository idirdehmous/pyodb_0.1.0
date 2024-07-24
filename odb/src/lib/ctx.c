
/* ctx.c */

#include "odb.h"

/* Fortran-callable functions to associate
   database & retrieval into a context */

/*--- Defines functions:

  character(len=*) dbname, retr
  integer version, ctxid, retcode, ifailmsg
  CALL ctxreg(dbname, retr, version, ctxid, retcode)
  ifailmsg = 0 ! Dont want a printout when retcode becomes == -2
  ifailmsg = 1 ! Will have a printout when retcode becomes == -2
  CALL ctxid(dbname, retr, version, ctxid, ifailmsg, retcode)

  character(len=*) message
  integer enforce
  CALL ctxdebug(enforce=[0|1],message)

  ---*/

#ifndef CTX_HASHSIZE
#define CTX_HASHSIZE 223U
#endif

PRIVATE uint CTX_hashsize = 0;

typedef struct _CTX_Hash_Table {
  uint  version;
  char *retr;
  char *dbname;
  int   ctxid;
  struct _CTX_Hash_Table *collision;
} CTX_Hash_Table;

PRIVATE CTX_Hash_Table **CTX_hashtable = NULL;

PRIVATE uint CTX_hashmin = INT_MAX;
PRIVATE uint CTX_hashmax = 0;

PRIVATE uint
Hash(const uint u, const char *s1, const char *s2)
{ 
  uint hashval = u;
  for (; *s1 ; s1++) {
    hashval = (*s1) + 31U * hashval;
  }
  for (; *s2 ; s2++) {
    hashval = (*s2) + 31U * hashval;
  }
  hashval = hashval % CTX_hashsize;
  return hashval;
}


PRIVATE void
HashInit()
{
  if (!CTX_hashtable) {
    CTX_Hash_Table **tmp_CTX_hashtable = NULL;
    char *p = getenv("ODB_CTX_HASHSIZE");
    if (p) {
      CTX_hashsize = atoi(p);
      if (CTX_hashsize <= 0) CTX_hashsize = CTX_HASHSIZE;
    }
    else
      CTX_hashsize = CTX_HASHSIZE;
    CALLOC(tmp_CTX_hashtable, CTX_hashsize);
    CTX_hashtable = tmp_CTX_hashtable;
  }
}


PRIVATE CTX_Hash_Table *
NewEntry(void)
{
  CTX_Hash_Table *p;
  ALLOC(p, 1);
  p->version = 0;
  p->retr = NULL;
  p->dbname = NULL;
  p->ctxid = 0;
  p->collision = NULL;
  return p;
}

PUBLIC void
init_CTX_lock()
{
  HashInit();
}

/*---
  CALL ctxreg(dbname, retr, version, ctxid, retcode)
  ---*/

PUBLIC void
ctxreg_(const char *dbname,
	const char *retr,
	const int  *version,
	const int  *ctxid,
	int *retcode,
	/* Hidden arguments */
	int dbname_len,
	int retr_len)
{
  int rc = 0;
  uint Version = *version;
  int Ctxid = *ctxid;
  uint index;
  CTX_Hash_Table *p;
  DECL_FTN_CHAR(dbname);
  DECL_FTN_CHAR(retr);

  ALLOC_FTN_CHAR(dbname);
  ALLOC_FTN_CHAR(retr);

  rc = index = Hash(Version, p_retr, p_dbname);

  CTX_hashmin = MIN(CTX_hashmin, index);
  CTX_hashmax = MAX(CTX_hashmax, index);

  p = CTX_hashtable[index];

  if (!p) {
    p = NewEntry();
    CTX_hashtable[index] = p;
  }
  
  while (p->dbname && p->retr) {
    if (Version == p->version &&
	strequ(p->dbname, p_dbname) &&
	strequ(p->retr, p_retr)) {
      if (Ctxid != p->ctxid) {
        fprintf(stderr,
                "ctxreg_[ctxid=%d]: (version,db,retr)=(%u,'%s','%s') already registered with a different ctxid=%d\n",
                Ctxid, Version, p_dbname, p_retr, p->ctxid);
        rc = -1;
      }
      goto finish; /* Already in list (whether bad or good, doesn't matter) */
    }
    if (!p->collision) p->collision = NewEntry();
    p = p->collision;
  } /* while (p->dbname && p->retr) */

  p->version  = Version;
  p->retr   = STRDUP(p_retr);
  p->dbname = STRDUP(p_dbname);
  p->ctxid  = Ctxid;

 finish:
  FREE_FTN_CHAR(dbname);
  FREE_FTN_CHAR(retr);

  *retcode = rc;
}


/*---
  CALL ctxid(dbname, retr, version, ctxid, retcode)
  ---*/


PUBLIC void
ctxid_(const char *dbname,
       const char *retr,
       const int *version,
       int *ctxid,
       const int *ifailmsg,
       int *retcode,
       /* Hidden arguments */
       int dbname_len,
       int retr_len)
{
  int rc = 0;
  uint Version = *version;
  int Ctxid = 0; /* Correct range starts from 1 */
  uint index;
  CTX_Hash_Table *p;
  DECL_FTN_CHAR(dbname);
  DECL_FTN_CHAR(retr);

  ALLOC_FTN_CHAR(dbname);
  ALLOC_FTN_CHAR(retr);

  rc = index = Hash(Version, p_retr, p_dbname);
  p = CTX_hashtable[index];

  if (!p) {
    /*
    fprintf(stderr,
	    "ctxid_: Unregistered (version,db,retr)=(%u,'%s','%s')\n",
	    Version, p_dbname, p_retr);
	    */
    rc = -1;
    goto finish; /* Unrecognized triplet */
  }
  
  while (p->dbname && p->retr) {
    if (p->version == Version &&
	strequ(p->dbname, p_dbname) &&
	strequ(p->retr, p_retr)) break; /* Found */
    if (p->collision) 
      p = p->collision;
    else {
      if (*ifailmsg) {
	fprintf(stderr,
		"ctxid_: (version,db,retr)=(%u,'%s','%s') not registered\n",
		Version, p_dbname, p_retr);
      }
      rc = -2;
      goto finish; /* Unrecognized triplet */
    }
  } /* while (p->dbname && p->retr) */
  
  Ctxid = p->ctxid;

 finish:
  FREE_FTN_CHAR(dbname);
  FREE_FTN_CHAR(retr);

  *ctxid = Ctxid;
  *retcode = rc;
}

/*---
  Debug printout to STDERR:
  CALL ctxdebug(enforce=[0|1],message)
  ---*/

PUBLIC void
ctxdebug_(const int *enforce,
	  const char *msg,
	  /* Hidden arguments */
	  int msg_len)
{
  DECL_FTN_CHAR(msg);
  int print_it = *enforce;
  int j;
  int entries=0;
  int hits=0;
  char *env = getenv("ODB_CTX_DEBUG");
  char blank = ' ';
  
  if (env) {
    int myproc = 0;
    int ienv = atoi(env);
    codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
    if (ienv == -1 || ienv == myproc) ienv = 1;
    else ienv = 0;
    print_it |= ienv;
  }

  if (!print_it) return;

  {
    extern void ctxprint_(const int *print_it, const char *msg, const char *dbname
			  /* Hidden arguments */
			  , int msg_len, int dbname_len);
    ctxprint_(&print_it, msg, &blank, msg_len, 1); /* A genuine Fortran-routine */
  }

  ALLOC_FTN_CHAR(msg);

  if (msg_len > 0) fprintf(stderr,"ctxdebug_: %s\n",p_msg);
  fprintf(stderr,
	  "ctxdebug_: hashsize=%u, hashmin=%u, hashmax=%u, hashtable at %p\n",
	  CTX_hashsize, CTX_hashmin, CTX_hashmax, CTX_hashtable);

  if (CTX_hashtable) {
    for (j=CTX_hashmin; j<=CTX_hashmax; j++) {
      CTX_Hash_Table *p = CTX_hashtable[j];
      int k=1;
      if (p) entries++;
      while (p) {
	fprintf(stderr,
		"hash#%d [hit#%d] : version=%u, retr='%s', db='%s', ctxid=%d\n",
		j,k,
		p->version,
		p->retr ? p->retr : NIL,
		p->dbname ? p->dbname : NIL, 
		p->ctxid);
	p = p->collision;
	k++;
	hits++;
      } /* while (p) */
    } /* for (j=CTX_hashmin; j<=CTX_hashmax; j++) */
  } /* if (CTX_hashtable) */

  fprintf(stderr,"ctxdebug_: Total %d hits in %d hash-entries\n",hits,entries);

  FREE_FTN_CHAR(msg);
}
