#include "odb.h"

typedef struct List {
  int selected;
  int vhandle;
  int replicate_PE;
  int npes;
  struct List *collision;
} List_t;

#ifndef PEINFO_HASHSIZE
#define PEINFO_HASHSIZE 223U
#endif

PRIVATE uint PEINFO_hashsize = 0;
PRIVATE List_t **PEINFO_hashtable = NULL;


PRIVATE List_t *
NewEntry() 
{
  List_t *p;
  CALLOC(p,1);
  return p;
}


#define HASH(x) (((uint)(x)) % PEINFO_hashsize)


PRIVATE void 
HashInit()
{
  if (!PEINFO_hashtable) {
    List_t **tmp_PEINFO_hashtable = NULL;
    int inumt = get_max_threads_();
    int it;
    char *p = getenv("ODB_PEINFO_HASHSIZE");
    if (p) {
      PEINFO_hashsize = atoi(p);
      if (PEINFO_hashsize <= 0) PEINFO_hashsize = PEINFO_HASHSIZE;
    }
    else {
      PEINFO_hashsize = PEINFO_HASHSIZE;
    }
    ALLOC(tmp_PEINFO_hashtable, inumt);
    for (it=0; it<inumt; it++) {
      /* CALLOC below implies that tmp_PEINFO_hashtable[it]->selected is zero */
      CALLOC(tmp_PEINFO_hashtable[it],PEINFO_hashsize); 
    }
    PEINFO_hashtable = tmp_PEINFO_hashtable;
  }
}


PUBLIC void
init_PEINFO_lock()
{
  HashInit();
}

PUBLIC void 
codb_save_peinfo_(const int *vhandle,
		  const int *replicate_PE, 
		  const int *npes)
{
  int it = get_thread_id_();
  FILE *do_trace = ODB_trace_fp();
  int found = 0;
  List_t *p;
  uint index;

  index = HASH(*vhandle);
  p = &PEINFO_hashtable[--it][index];

  while (p) {
    if (!p->selected) {
      p->selected = 1;
      p->vhandle = *vhandle;
      p->replicate_PE = *replicate_PE;
      p->npes = *npes;
      found = 1;
      break; /* found */
    }
    if (!p->collision) p->collision = NewEntry();
    p = p->collision;
  } /* while (p) */

  if (do_trace) {
    ODB_Trace TracE;
    TracE.handle = -1;
    TracE.msglen = 256;
    TracE.msg = NULL;
    ALLOC(TracE.msg, TracE.msglen);
    TracE.numargs = 0; 
    TracE.mode = -1;
    sprintf(TracE.msg,
	    "save_peinfo: vhandle=%d, found=%d, repl_PE=%d, npes=%d, it=%d",
	    *vhandle, found, *replicate_PE, *npes, it+1);
    codb_trace_(&TracE.handle, &TracE.mode,
		TracE.msg, TracE.args, 
		&TracE.numargs, TracE.msglen);
    FREE(TracE.msg);
  } /* if (do_trace) */
}


PUBLIC void 
codb_restore_peinfo_(const int *vhandle,
		     int *replicate_PE, 
		     int *npes)
{
  int it = get_thread_id_();
  FILE *do_trace = ODB_trace_fp();
  int found = 0;
  List_t *p;
  uint index;

  index = HASH(*vhandle);
  p = &PEINFO_hashtable[--it][index];

  *replicate_PE = 0;
  *npes = 0;

  while (p) {
    if (p->selected && p->vhandle == *vhandle) {
      *replicate_PE = p->replicate_PE;
      *npes = p->npes;
      p->selected = 0;
      found = 1;
      break; /* found */
    }
    p = p->collision;
  } /* while (p) */

  if (do_trace) {
    ODB_Trace TracE;
    TracE.handle = -1;
    TracE.msglen = 256;
    TracE.msg = NULL;
    ALLOC(TracE.msg, TracE.msglen);
    TracE.numargs = 0; 
    TracE.mode = -1;
    sprintf(TracE.msg,
	    "restore_peinfo: vhandle=%d, found=%d, repl_PE=%d, npes=%d, it=%d",
	    *vhandle, found, *replicate_PE, *npes, it+1);
    codb_trace_(&TracE.handle, &TracE.mode,
		TracE.msg, TracE.args, 
		&TracE.numargs, TracE.msglen);
    FREE(TracE.msg);
  } /* if (do_trace) */
}
