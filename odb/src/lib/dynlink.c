#include "odb.h"
#include "cdrhook.h"

extern char *IOresolve_env(const char *str); /* from libioassign.a */

#include <sys/types.h>
#include <sys/stat.h>

extern int ODBstatic_mode;

PRIVATE const char spath[] = "ODB_SRCPATH";
PRIVATE const char outpath[] = "ODB_OUTPATH";

PRIVATE void
fixenv(const char *name, const char *value)
{
  char *env;
  int len = strlen(name) + strlen(value) + 2;
  ALLOC(env, len);
  sprintf(env,"%s=%s",name,value);
  /* fprintf(stderr,"fixenv: %s\n",env); */
  putenv(env);
}

typedef struct _ODB_StatFunc {
  char *dbname;
  char *viewname;
  ODB_Funcs *(*anchor)(void *V, ODB_Pool *pool, int *ntables, int it, int add_vars);
  struct _ODB_StatFunc *next;
} ODB_StatFunc;

PRIVATE ODB_StatFunc *first_st = NULL;
PRIVATE ODB_StatFunc *last_st = NULL;

typedef struct _Dyn_Handles {
  void *handle;
  char *dbname;
  char *viewname;
  char *symname;
  char *so_file;
  union {
    void *entry;
    ODB_Funcs *(*func)(void *V, ODB_Pool *pool, int *ntables, int it, int add_vars);
  } u;
  ODB_Funcs *(*anchor)(void *V, ODB_Pool *pool, int *ntables, int it, int add_vars);
  struct _Dyn_Handles *next;
} Dyn_Handles;

PRIVATE Dyn_Handles *dynlink_start = NULL;
PRIVATE Dyn_Handles *dynlink_last  = NULL;


#ifdef STATIC_LINKING

/* Dummies for dlopen(), dlsym(), dlclose() and dlerror() */

PRIVATE char dl_errmsg[] = "An attempt to perform dynamic linking when it was turned off";

PRIVATE void *dlopen(const char *pathname, int mode) { return NULL; }
PRIVATE int dlclose(void *handle) { return 1; /* Indicates error */ } 
PRIVATE void *dlsym(void *handle, const char *name) { return NULL; }
PRIVATE char *dlerror(void) { return dl_errmsg; }

#define DYNAMIC_ERROR(where,error_text)         fprintf(stderr,"%s : %s\n",where,error_text); perror(dlerror())
#define DYNAMIC_OPEN(handle, path, ormode)      handle = dlopen(path, 0 | ormode); DYNAMIC_ERROR("dlopen", path);
#define DYNAMIC_CLOSE(handle, rc)               rc = dlclose(handle); DYNAMIC_ERROR("dlclose", "");
#define DYNAMIC_SYMBOL(handle, name, entry, rc) entry = dlsym(handle, name); rc = -1; DYNAMIC_ERROR("dlsym", name);

#else

/* Genuinely a dynamic version (relies on -ldl) */

#ifdef HPPA
/* This can be set to 0, when HP-UX 11.xx is in place 
   (it has got dlopen() etc. proper stuff) */
#define HP_SHL_LOAD 1
#else
#define HP_SHL_LOAD 0
#endif

#if HP_SHL_LOAD == 1
#include <dl.h>
#define DYNAMIC_OPEN(handle, path, ormode)           handle = shl_load(path, \
								  runtime_load_now ? BIND_IMMEDIATE | ormode : \
						 		       BIND_DEFERRED | BIND_VERBOSE | ormode \
								  , 0L) ; \
                                                { if (handle && Myproc == 1) { fprintf(stderr,"shl_load: Successfully loaded '%s'\n", path); } } \
                                                { if (!handle && Myproc == 1) { DYNAMIC_ERROR("shl_load", path); } }
#define DYNAMIC_CLOSE(handle, rc)               rc = shl_unload(handle)
#define DYNAMIC_SYMBOL(handle, name, entry, rc) rc = shl_findsym((shl_t *)&handle, name, TYPE_UNDEFINED, &entry)
#define DYNAMIC_ERROR(where, error_text)        fprintf(stderr,"%s : %s\n",where,error_text); perror(error_text)
#else
#include <dlfcn.h>
#define DYNAMIC_OPEN(handle, path, ormode)      handle = dlopen(path, runtime_load_now ? RTLD_NOW | ormode : RTLD_LAZY | ormode) ; \
                                                { if (handle && Myproc == 1) { fprintf(stderr,"dlopen: Successfully loaded '%s'\n", path); } } \
                                                { if (!handle && Myproc == 1) { DYNAMIC_ERROR("dlopen", path); } }
#define DYNAMIC_CLOSE(handle, rc)               rc = dlclose(handle)
#define DYNAMIC_SYMBOL(handle, name, entry, rc) entry = dlsym(handle, name); rc = (!entry) ? -1 : 0
#define DYNAMIC_ERROR(where,error_text)         fprintf(stderr,"***Problems; %s : %s\n",where,error_text) ; perror(dlerror())
#endif

#endif



PUBLIC void
ODB_addstatfunc(const char *dbname,
		const char *viewname,
		ODB_Funcs *(*anchor)(void *V, ODB_Pool *pool, int *ntables, int it, int add_vars))
{
  DRHOOK_START(ODB_addstatfunc);
  if (dbname && anchor) {
    ODB_StatFunc *pst;
    boolean found = 0;
    
    if (viewname) {
      for (pst = first_st; pst != NULL; pst = pst->next) {
	if (pst->viewname &&
	    strequ(pst->viewname, viewname) &&
	    strequ(pst->dbname, dbname)) {
	  found = 1;
	  break;
	}
      }
    }
    else {
      for (pst = first_st; pst != NULL; pst = pst->next) {
	if (!pst->viewname &&
	    strequ(pst->dbname, dbname)) {
	  found = 1;
	  break;
	}
      }
    }
    
    if (!found) {
      ALLOC(pst, 1);
      if (first_st) last_st->next = pst;
      else          first_st = pst;
      last_st = pst;
      
      pst->dbname = STRDUP(dbname);
      pst->viewname = viewname ? STRDUP(viewname) : NULL;
      pst->anchor = anchor;
      pst->next = NULL;
    }
  }
  DRHOOK_END(0);
}


PRIVATE void
ODB_linkview_static(const int *handle, 
		    const char *viewname,
		    int *retcode,
		    /* Hidden arguments */
		    int viewname_len)
{
  int rc = 0;
  int Handle = *handle;
  ODB_StatFunc *st = NULL;
  ODB_Pool *p;
  DECL_FTN_CHAR(viewname);
  DRHOOK_START(ODB_linkview_static);

  ALLOC_FTN_CHAR(viewname);

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      boolean found = 0;
      ODB_StatFunc *pst = NULL;
      char *dbname  = p->dbname;

      for (pst = first_st; pst != NULL; pst = pst->next) {
	if (pst->viewname && 
	    strequ(pst->viewname, p_viewname) &&
	    strequ(pst->dbname, dbname)) {
	  st = pst;
	  found = 1;
	  break;
	}
      }
      if (!found) rc = -1; /* Not statically declared */
      break;
    }
  }

  if (st) {
    /* Anchor the static view and register it to the pools it belongs to */
    int inumt = get_max_threads_();
    int add_vars = 1;
    FORPOOL {
      if (p->inuse && p->handle == Handle) {
	int it;
	for (it = 1; it <= inumt ; it++) {
	  ODB_Funcs *pf = st->anchor(NULL, p, NULL, it, add_vars);
	  ODB_add_funcs(p, pf, it);
	} /* for (it = 1; it <= inumt ; it++) */
	add_vars = 0;
      }
    } /* FORPOOL */
  }

  /* finish: */
  FREE_FTN_CHAR(viewname);
  
  *retcode = rc;
  DRHOOK_END(0);
}


PRIVATE void
ODB_linkdb_static(const char *dbname,
		  int *retcode,
		  /* Hidden arguments */
		  int dbname_len)
{
  int rc = 0;
  boolean found = 0;
  ODB_StatFunc *st = NULL;
  ODB_StatFunc *pst;
  DECL_FTN_CHAR(dbname);
  DRHOOK_START(ODB_linkdb_static);

  ALLOC_FTN_CHAR(dbname);

  for (pst = first_st; pst != NULL; pst = pst->next) {
    if (!pst->viewname &&
	strequ(pst->dbname, p_dbname)) {
      st = pst;
      found = 1;
      break;
    }
  }
  if (!found) rc = -1; /* Not statically declared */

  if (st) {
    /* Get no. of TABLEs (indepedent of pools) */
    st->anchor(NULL, NULL, &rc, 0, 0);
  }

  /* finish: */
  FREE_FTN_CHAR(dbname);
  *retcode = rc;
  DRHOOK_END(0);
}


PRIVATE ODB_Pool *
ODB_create_pool_static(int handle, 
		       const char *dbname, 
		       int poolno, 
		       int is_new,
		       int io_method,
		       int add_vars)
{
  ODB_Pool *p = NULL;
  DRHOOK_START(ODB_create_pool_static);

  if (dbname) {
    ODB_Funcs *(*anchor)(void *V, ODB_Pool *pool, int *ntables, int it, int add_vars) = NULL;
    ODB_StatFunc *pst;

    for (pst = first_st; pst != NULL; pst = pst->next) {
      if (!pst->viewname &&
	  strequ(pst->dbname, dbname)) {
	anchor = pst->anchor;
	break;
      }
    }

    if (anchor) {
      ODB_Anchor_Funcs func = { NULL, NULL, NULL };

      ALLOC(p, 1);
      if (first_pool) last_pool->next = p;
      else            first_pool = p;
      last_pool = p;

      p->handle = handle;
      p->dbname = STRDUP(dbname);
      p->srcpath = STRDUP("");
      p->poolno = poolno;
      
      p->pm = ODB_get_poolmask_by_handle(handle);

      p->add_var = ODB_add_var;
      anchor(&func, p, NULL, 0, add_vars);
      
      p->load = func.load;
      p->store = func.store;
      
      p->funcs  = NULL;
      p->nfuncs = 0;
      p->nfuncs = func.create_funcs(p, is_new, io_method, 0);
      
      p->next = NULL;
      p->inuse = 1;

      put_poolreg(handle, poolno, p);
    }
  }

  DRHOOK_END(0);
  return p;
}


PUBLIC void
codb_makeview_(const int  *handle,
               const char *viewname,
	       const char *viewfile,
               const char *select,
               const char *uniqueby,
               const char *from,
               const char *where,
               const char *orderby,
	       const char *set,
	       const char *query,
               int *retcode,
	       /* Hidden arguments */
               int viewname_len,
               int viewfile_len,
	       int select_len,
	       int uniqueby_len,
	       int from_len,
	       int where_len,
	       int orderby_len,
	       int set_len,
	       int query_len)
{
#ifdef DEBUG
  const char Pwd[] = "pwd;";
#else
  const char Pwd[] = "";
#endif
  static char *odb_compiler = NULL;
  int rc = 0;
  int Handle = *handle;
  ODB_Pool *p;
  DECL_FTN_CHAR(viewname);
  DECL_FTN_CHAR(viewfile);
  DECL_FTN_CHAR(select);
  DECL_FTN_CHAR(uniqueby);
  DECL_FTN_CHAR(from);
  DECL_FTN_CHAR(where);
  DECL_FTN_CHAR(orderby);
  DECL_FTN_CHAR(set);
  DECL_FTN_CHAR(query);
  DRHOOK_START(codb_makeview_);

  if (ODBstatic_mode) {
    rc = -1; /* N/A in static mode */
    goto finish;
  }

  ALLOC_FTN_CHAR(viewname);
  ALLOC_FTN_CHAR(viewfile);
  ALLOC_FTN_CHAR(select);
  ALLOC_FTN_CHAR(uniqueby);
  ALLOC_FTN_CHAR(from);
  ALLOC_FTN_CHAR(where);
  ALLOC_FTN_CHAR(orderby);
  ALLOC_FTN_CHAR(set);
  ALLOC_FTN_CHAR(query);

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      char *dbname  = p->dbname;
      char *srcpath = p->srcpath;
      FILE *fp;
      char *filename, *so_file;
      char *cmd;
      int len;
      
      if (!odb_compiler) {
	char *s = getenv("ODB_COMPILER");
	odb_compiler = s ? STRDUP(s) : STRDUP(ODB_COMPILER_DEFAULT);
      }

      len = strlen(srcpath) + strlen(p_viewname) + strlen(dbname);
      ALLOC(so_file, len + 8);
      sprintf(so_file,"%s/%s_%s.so", srcpath, dbname, p_viewname);

      if (viewfile_len > 0) {
	filename = STRDUP(p_viewfile);
      }
      else {
	/* Generate automatically */
	int unit;

	len = strlen(srcpath) + strlen(dbname) + strlen(p_viewname);
	ALLOC(filename, len + 10);
	sprintf(filename, "%s/%s_%s.sql", srcpath, dbname, p_viewname);
	
	cma_open_(&unit, filename, "w", &rc, strlen(filename), 1);
	if (rc != 1) {
	  perror(filename);
	  rc = -1;
	  goto finish;
	}

	fp = CMA_get_fp(&unit);
	fprintf(fp,"// Automatically created for view '%s' :\n",p_viewname);
	if (set_len > 0) fprintf(fp,"%s",p_set);
	fprintf(fp,"\nCREATE VIEW %s AS",p_viewname);
	if (query_len > 0) {
	  fprintf(fp,"%s",p_query);
	}
	else {
	  fprintf(fp,"\n\t  SELECT %s",p_select);
	  if (uniqueby_len > 0) fprintf(fp,"\n\tUNIQUEBY %s",p_uniqueby);
	  fprintf(fp,"\n\t    FROM %s",p_from);
	  if (where_len > 0)    fprintf(fp,"\n\t   WHERE %s",p_where);
	  if (orderby_len > 0)  fprintf(fp,"\n\t ORDERBY %s",p_orderby);
	}
	fprintf(fp,"\n;\n");
	fclose(fp);
      }

      len = 
	strlen(filename) + 2 * strlen(so_file) + strlen(odb_compiler) + 
	  2 * strlen(dbname) + strlen(srcpath) + strlen(filename);
      ALLOC(cmd, len + 100);
#ifdef HPPA
      sprintf(cmd,
	      "\n\t%s\n\tcat %s;\n\t/bin/mv -f %s __hppa.so;\n\t%s -w -l %s -D%s -o %s %s;\n\tls -l %s",
	      Pwd, filename, so_file,
	      odb_compiler,dbname,dbname,srcpath,filename,so_file);
#else
      sprintf(cmd,
	      "\n\t%s\n\tcat %s;\n\t/bin/rm -f %s;\n\t%s -w -l %s -D%s -o %s %s;\n\tls -l %s",
	      Pwd, filename, so_file,
	      odb_compiler,dbname,dbname,srcpath,filename,so_file);
#endif
      fprintf(stderr,">> Compiling: %s\n",cmd);

      {
	char *pp = getenv("ODB_LIBS_KEEP"); /* if == 1, keep ODB_LIBS unaltered for views */
	int keep = pp ? atoi(pp) : 0; /* The default for compatibility is to *ALTER* $ODB_LIBS */
	char *p = getenv("ODB_LIBS");
	char *odb_libs = p ? STRDUP(p) : NULL;

	if (!keep && odb_libs) { /* Make ODB_LIBS empty */
	  fixenv("ODB_LIBS", "");
	} /* if (!keep && odb_libs) */
	 
	system(cmd);

	if (!keep && odb_libs) { /* Reset ODB_LIBS back */
	  fixenv("ODB_LIBS", odb_libs);
	} /* if (!keep && odb_libs) */

	FREE(odb_libs);
      }

      FREE(cmd);
      FREE(filename);

      /* if (viewfile_len <= 0) */
      {
	char *tmp = IOresolve_env(so_file);
	struct stat buf;
	if (stat(tmp, &buf) == -1) {
	  fprintf(stderr,"Shareable object file '%s' (alias '%s') not found\n",so_file,tmp);
	  perror(so_file);
	  rc = -1;
	}
	FREE(tmp);
      }

      FREE(so_file);

      break;
    }
  }

 finish:
  FREE_FTN_CHAR(viewname);
  FREE_FTN_CHAR(viewfile);
  FREE_FTN_CHAR(select);
  FREE_FTN_CHAR(uniqueby);
  FREE_FTN_CHAR(from);
  FREE_FTN_CHAR(where);
  FREE_FTN_CHAR(orderby);
  FREE_FTN_CHAR(set);
  FREE_FTN_CHAR(query);

  *retcode = rc;
  DRHOOK_END(0);
}


PRIVATE void *
link_dynamically(const char *dbname, 
		 const char *viewname, 
		 const char *symname, 
		 const char *so_file,
		 int *retcode)
{
  int rc = 0;
  int dlretcode = 0;
  Dyn_Handles *dynlink = NULL;
  Dyn_Handles *d = dynlink_start;
  FILE *do_trace = ODB_trace_fp(); 
  ODB_Trace TracE;
  const char trace_text[] = "link_dynamically :";
  static int Myproc = 0;
  char *rldpath = getenv("ODB_RLDPATH");
  char *rldpath_only = getenv("ODB_RLDPATH_ONLY");
  char *rld_now = getenv("ODB_RLD_NOW");  
  char *rld_ormode = getenv("ODB_RLD_ORMODE");
  int value_rldpath_only = 0;
  int runtime_load_now = 0;
  int runtime_load_ormode = 0;

  if (rldpath && rldpath_only) value_rldpath_only = atoi(rldpath_only);
  if (rld_now) runtime_load_now = atoi(rld_now);

  if (Myproc == 0) codb_procdata_(&Myproc, NULL, NULL, NULL, NULL);

  if (do_trace) {
    TracE.handle = -1;
    TracE.msglen = 
      (dbname ? strlen(dbname) : strlen(NIL)) + 
	(viewname ? strlen(viewname) : strlen(NIL)) +
	  (symname ? strlen(symname) : strlen(NIL)) +
	      strlen(trace_text) + 10;
    ALLOC(TracE.msg,TracE.msglen);
    sprintf(TracE.msg,"%s (%s,%s,%s)",
	    trace_text, 
	    dbname ? dbname : NIL, 
	    viewname ? viewname : NIL, 
	    symname ? symname : NIL); 
    TracE.numargs = 0;
    TracE.mode = 1;
    codb_trace_(&TracE.handle, &TracE.mode,
		TracE.msg, TracE.args, &TracE.numargs, TracE.msglen);
  }

  while (d) {
    if (strequ(d->dbname, dbname) && 
	strequ(d->symname, symname)) {
      dynlink = d;
      break;
    }
    d = d->next;
  }

  if (!dynlink) {
    ALLOC(d, 1);
    d->handle = NULL;
    d->dbname = STRDUP(dbname);
    d->viewname = viewname ? STRDUP(viewname) : NULL;
    d->symname = STRDUP(symname);
    d->so_file = IOresolve_env(so_file);
    d->u.entry = NULL;
    d->anchor = NULL;
    d->next = NULL;
    dynlink = d;
    
    if (!dynlink_start) {
      dynlink_start = dynlink;
    }
    else {
      dynlink_last->next = dynlink;
    }
    dynlink_last = dynlink;
  }
  
  if (dynlink->handle) {
    int inumt = get_max_threads_();
    int it;
    for (it = 1; it <= inumt; it++) {
      ODB_del_vars(dbname, dynlink->viewname, it);
    }
    DYNAMIC_CLOSE(dynlink->handle, dlretcode);
    if (dlretcode != 0) { /* Failed : Cannot close */
      DYNAMIC_ERROR("dlclose",dynlink->dbname);
      rc = -1;
      goto finish;
    }      
  }
  
  dynlink->handle = NULL;
  FREE(dynlink->so_file);
  dynlink->u.entry = NULL;
  dynlink->anchor  = NULL;
      
  /* Perform the actual dynamic linking */
  
  dynlink->so_file = IOresolve_env(so_file);

#ifdef DEBUG
  fprintf(stderr,
	  ">> Dynamic linking of symbol '%s' from DSO-file '%s'\n",
	  symname, so_file);
#endif

  if (value_rldpath_only <= 0) {
    DYNAMIC_OPEN(dynlink->handle, dynlink->so_file, runtime_load_ormode);
  }
  if (!dynlink->handle || value_rldpath_only > 0) { /* Failed : Cannot get handle */
    /* Giving a second try ? rely on ODB_RLDPATH */

    char *bname = strrchr(dynlink->so_file,'/');

    if (rldpath && bname) {
      char *saved = STRDUP(rldpath);
      char *token = strtok(saved,":");
      char *basename;
      int len, baselen;
      bname++;
      basename = STRDUP(bname);
      baselen = strlen(basename);
      /* fprintf(stderr,"ODB_RLDPATH='%s'\n",rldpath); */
      while (!dynlink->handle && token) {
	/* fprintf(stderr,"token='%s' : basename='%s'\n",token,basename); */
	len = strlen(token) + baselen;
	FREE(dynlink->so_file);
	ALLOC(dynlink->so_file, len + 2);
	sprintf(dynlink->so_file, "%s/%s", token, basename);
	DYNAMIC_OPEN(dynlink->handle, dynlink->so_file, runtime_load_ormode);
	token = strtok(NULL,":");
      }
      FREE(saved);
      FREE(basename);
    }

    if (!dynlink->handle) { /* Failed : Cannot get handle */
      DYNAMIC_ERROR("dlopen",dynlink->so_file);
      rc = -2;
      goto finish;
    }
  }
  
  DYNAMIC_SYMBOL(dynlink->handle, symname, dynlink->u.entry, dlretcode);
  if (dlretcode != 0) { /* Failed : No such entry */
    DYNAMIC_ERROR("dlsym",symname);
    rc = -3;
    goto finish;
  }
  
  dynlink->anchor = dynlink->u.func; /* For convenience */

  if (!viewname) {
    /* Get no. of TABLEs (indepedent of pools) */
    dynlink->anchor(NULL, NULL, &rc, 0, 0);
  }

 finish:

  if (do_trace) {
    TracE.args[0] = rc;
    TracE.numargs = 1;
    TracE.mode = 0;
    codb_trace_(&TracE.handle, &TracE.mode,
		TracE.msg, TracE.args, &TracE.numargs, TracE.msglen);
    FREE(TracE.msg);
  }

  if (retcode) *retcode = rc;

  return (rc == 0 && dynlink) ? dynlink : NULL;
}


PUBLIC int
ODB_undo_dynlink(const char *dbname, boolean views_only)
{
  int rc = 0;
  int dlretcode = 0;
  Dyn_Handles *dynlink = dynlink_start;
  int count = 0;
  FILE *do_trace = ODB_trace_fp(); 
  ODB_Trace TracE;
  const char trace_text[] = "undo_dynlink for ";

  if (ODBstatic_mode) 
    return 0; /* N/A in static mode => return code = 0 is okay */

  if (do_trace) {
    TracE.handle = -1;
    TracE.msglen = strlen(dbname) + strlen(trace_text) + 1;
    ALLOC(TracE.msg,TracE.msglen);
    sprintf(TracE.msg,"%s%s",trace_text,dbname);
    TracE.args[0] = views_only;
    TracE.numargs = 1;
    TracE.mode = 1;
    codb_trace_(&TracE.handle, &TracE.mode,
		TracE.msg, TracE.args, &TracE.numargs, TracE.msglen);
  }

#ifdef DEBUG
  fprintf(stderr,
	  "Running ODB_undo_dynlink: dbname=%s, views_only=%d\n",
	  dynlink->dbname,
	  (int)views_only);
#endif

  while (dynlink) {
    if (dynlink->handle &&
	strequ(dynlink->dbname, dbname)) {
      
      if ((views_only && dynlink->viewname) || !views_only) {
	char *viewname = views_only ? dynlink->viewname : NULL;
	int it = 0;
	if (viewname) {
	  int inumt = get_max_threads_();
	  for (it = 1; it <= inumt ; it++) {
	    ODB_del_vars(dbname, viewname, it);
	  }
	}
	else {
	  ODB_del_vars(dbname, viewname, it);
	}
	
	count++;
#ifdef DEBUG
	fprintf(stderr,
		"#%d: dbname=%s, viewname=%s, handle=%x\n",
		count,
		dynlink->dbname,
		dynlink->viewname ? dynlink->viewname : NIL,
		dynlink->handle);
#endif

	DYNAMIC_CLOSE(dynlink->handle, dlretcode);
	if (dlretcode != 0) {
	  DYNAMIC_ERROR("dlclose", dynlink->dbname);
	  rc = -1;
	  goto finish;
	}
	
	dynlink->handle = NULL;
	if (!views_only) goto finish; /* Only one anchor per DBase */
      }
    }

    dynlink = dynlink->next;
  } /* while (dynlink) */

 finish:

  if (do_trace) {
    TracE.args[0] = views_only;
    TracE.args[1] = count;
    TracE.args[2] = rc;
    TracE.numargs = 3;
    TracE.mode = 0;
    codb_trace_(&TracE.handle, &TracE.mode,
		TracE.msg, TracE.args, &TracE.numargs, TracE.msglen);
    FREE(TracE.msg);
  }

  return rc;
}


PUBLIC void
codb_linkview_(const int *handle, 
	       const char *viewname,
	       int *retcode,
	       /* Hidden arguments */
	       int viewname_len)
{
  int rc = 0;
  int Handle = *handle;
  Dyn_Handles *dynlink = NULL;
  ODB_Pool *p;
  DECL_FTN_CHAR(viewname);
  DRHOOK_START(codb_linkview_);

  if (ODBstatic_mode) {
    ODB_linkview_static(handle, viewname, &rc, viewname_len);
    goto finish;
  }

  ALLOC_FTN_CHAR(viewname);

  FORPOOL {
    if (p->inuse && p->handle == Handle) {
      char *dbname  = p->dbname;
      char *srcpath = p->srcpath;
      char *symname;
      char *so_file;
      int len;

      len = strlen("Anchor2") + strlen(p_viewname) + strlen(dbname);
      ALLOC(symname, len + 2);
      sprintf(symname,"Anchor2%s_%s",dbname,p_viewname);

      len = strlen(srcpath) + strlen(p_viewname) + strlen(dbname);

      ALLOC(so_file, len + 6);
      sprintf(so_file, "%s/%s_%s.so", srcpath, dbname, p_viewname);

      dynlink = link_dynamically(dbname, p_viewname, symname, so_file, &rc);

      FREE(symname);
      FREE(so_file);

      if (rc < 0) goto finish;

      break;
    }
  }

  if (dynlink) {
    /* Anchor the dynamic view and register it to the pools it concerns */
    int inumt = get_max_threads_();
    int add_vars = 0;
    FORPOOL {
      if (p->inuse && p->handle == Handle) {
	int it;
	for (it = 1; it <= inumt ; it++) {
	  ODB_Funcs *pf = dynlink->anchor(NULL, p, NULL, it, add_vars);
	  ODB_add_funcs(p, pf, it);
	} /* for (it = 1; it <= inumt ; it++) */
	add_vars = 0;
      }
    } /* FORPOOL */
  }

 finish:
  FREE_FTN_CHAR(viewname);
  
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC void
codb_linkdb_(const char *dbname,
	     int *retcode,
	     /* Hidden arguments */
	     int dbname_len)
{
  int rc = 0;
  Dyn_Handles *dynlink = NULL;
  DECL_FTN_CHAR(dbname);
  DRHOOK_START(codb_linkdb_);

  if (ODBstatic_mode) {
    ODB_linkdb_static(dbname, &rc, dbname_len);
    goto finish;
  }

  ALLOC_FTN_CHAR(dbname);

  {
    char *symname;
    char *so_file;
    char *dollar_path = NULL;
    char *srcpath = NULL;
    int len;

    len = strlen(spath) + strlen(p_dbname) + 2;
    ALLOC(dollar_path, len);
    sprintf(dollar_path, "%s_%s", spath, p_dbname);

    srcpath = getenv(dollar_path);

    if (!srcpath) {
      srcpath = getenv(spath);
      if (srcpath) {
	fixenv(dollar_path, srcpath);
	FREE(dollar_path);
	dollar_path = STRDUP(spath);
      }
      else {
	fprintf(stderr,
		"*** Error in codb_linkdb_(): Either %s or %s must be defined\n",
		dollar_path, spath);
	rc = -1;
	goto finish;
      }
    }
    
    FREE(dollar_path);

    len = strlen("Anchor2") + strlen(p_dbname);
    ALLOC(symname, len + 1);
    sprintf(symname,"Anchor2%s",p_dbname);
    
    len = strlen(srcpath) + strlen(p_dbname);

    ALLOC(so_file, len + 5);
    sprintf(so_file, "%s/%s.so", srcpath, p_dbname);

    dynlink = link_dynamically(p_dbname, NULL, symname, so_file, &rc);
    
    FREE(symname);
    FREE(so_file);
    
    if (rc < 0 || !dynlink) goto finish;
  }

 finish:
  FREE_FTN_CHAR(dbname);
  
  *retcode = rc;
  DRHOOK_END(0);
}


PUBLIC ODB_Pool *
ODB_create_pool(int handle, 
		const char *dbname, 
		int poolno, 
		int is_new,
		int io_method,
		int add_vars)
{
  ODB_Pool *p = NULL;
  DRHOOK_START(ODB_create_pool);

  if (ODBstatic_mode) {
    p = ODB_create_pool_static(handle, dbname, poolno, is_new, io_method, add_vars);
    goto finish;
  }

  if (dbname) {
    ODB_Anchor_Funcs func = { NULL, NULL, NULL };
    Dyn_Handles *dynlink = NULL;

    char *srcpath = NULL;
    char *dollar_path = NULL;

    {
      int len;
      
      len = strlen(spath) + strlen(dbname) + 2;
      ALLOC(dollar_path, len);
      sprintf(dollar_path, "%s_%s", spath, dbname);

      srcpath = getenv(outpath);

      if (!srcpath) {
	srcpath = getenv(dollar_path);
      }
      else {
	fixenv(dollar_path, srcpath);
	FREE(dollar_path);
	dollar_path = STRDUP(outpath);
      }

      if (!srcpath) {
	srcpath = getenv(spath);
	if (srcpath) {
	  fixenv(dollar_path, srcpath);
	  FREE(dollar_path);
	  dollar_path = STRDUP(spath);
	}
	else {
	  fprintf(stderr,
		  "*** Error in ODB_create_pool(): Either %s or %s must be defined\n",
		  dollar_path, spath);
	  goto finish;
	}
      }
    }  

    {
      Dyn_Handles *d = dynlink_start;
      
      while (d) {
	if (!d->viewname && strequ(d->dbname, dbname)) {
	  dynlink = d;
	  break;
	}
	d = d->next;
      }

      if (!dynlink) {
	fprintf(stderr,
		"*** Error: Database %s is not linked\n",
		dbname);
	goto finish;
      }
    }


    ALLOC(p, 1);
    if (first_pool) last_pool->next = p;
    else            first_pool = p;
    last_pool = p;

    p->handle = handle;
    p->dbname = STRDUP(dbname);
    ALLOC(p->srcpath, strlen(dollar_path) + 2);
    sprintf(p->srcpath,"$%s",dollar_path);
    p->poolno = poolno;

    p->pm = ODB_get_poolmask_by_handle(handle);

    p->add_var = ODB_add_var;
    (void) dynlink->anchor(&func, p, NULL, 0, add_vars);

    p->load = func.load;
    p->store = func.store;

    p->funcs  = NULL;
    p->nfuncs = 0;
    p->nfuncs = func.create_funcs(p, is_new, io_method, 0);

    p->next = NULL;
    p->inuse = 1;

    put_poolreg(handle, poolno, p);

    FREE(dollar_path);
  }

 finish:

  DRHOOK_END(0);
  return p;
}
