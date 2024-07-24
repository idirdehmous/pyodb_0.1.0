#if REAL_VERSION == 8

#define GEN_TYPE       double
#define GEN_TYPE_CAST  GEN_TYPE
#define GEN_GET        codb_dget_
#define GEN_PUT        codb_dput_
#define GEN_GETFUNC    dget
#define GEN_PUTFUNC    dput
#define GEN_GETNAME    "codb_dget_"
#define GEN_PUTNAME    "codb_dput_"
#define GEN_BYTES      8

#else

  ERROR in programming : No datatype given (should never have ended up here)

#endif

#if defined(REAL_VERSION)

PUBLIC void
GEN_GET(const int *handle,
	const int *poolno,
	const char *dataname,
	GEN_TYPE d[],
	const int *doffset,
	const int *ldimd,
	const int *nrows,
	const int *ncols,
	const int  flag[],
	const int *procid,
	const int *istart,
	const int *ilimit,
	const int *inform_progress,
	const int *using_it,
	int *retcode,
	/* Hidden arguments */
	int dataname_len)
{
  int rc = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  int Pbar = *inform_progress; /* whether to invoke codb_progress_bar_() or not */
  int ProcID = *procid;
  boolean AnyPool = (Poolno == -1);
  int LdimD = *ldimd;
  int Doffset = *doffset;
  int Nrows = *nrows;
  int Ncols = *ncols;
  int Neff_rows = 0;
  int Neff_cols = 0;
  int Nrows_total = 0;
  int Nrows_polled = 0;
  int k1 = Doffset;
  int Start = *istart;
  int HighWaterMark = *ilimit;
  int Limit = *ilimit;
#if 0
  int globprt = 0;
#endif
  POOLREG_DEF;
  int it = USING_IT;
  static int myproc = 0;
  DECL_FTN_CHAR(dataname);
  static char *env = NULL;
  static int first_time = 1;
  static int do_debug = 0;
  int windowing = (ProcID <= 0);
  DRHOOK_START_BY_STRING(GEN_GETNAME);

  if (first_time) {
    env = getenv("ODB_DEBUG_CODB_H");
    if (env) {
      int value = atoi(env);
      codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
      if (value == -1 || value == myproc) do_debug = 1;
    }
    first_time = 0;
  }

  ALLOC_FASTFTN_CHAR(dataname);

  if (do_debug) {
    printf("\n>>>%s(view=%s, myproc=%d, Poolno=%d,"
	   " ProcID=%d, LdimD=%d, Doffset=%d, Nrows=%d, Ncols=%d, Start=%d, Limit=%d)\n",
	   GEN_GETNAME, p_dataname, myproc, Poolno,
	   ProcID, LdimD, Doffset, Nrows, Ncols,
	   Start, Limit);
    fflush(stdout);
  }

  if (LdimD - Doffset < Nrows) { rc = -3; goto finish; }

  POOLREG_FOR {
    boolean pf_found = 0;

    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);

      if (pf) {
	int GetInterMed = pf->tmp ? 1 : 0;
	void *data = pf->data;
	int nr, nc, noffset;
	int pool = p->poolno;
	int row_offset = 0;
	int nr_orig;
	extern double util_walltime_();
	double zwall[2];
	
	if (Pbar) zwall[0] = util_walltime_();

	pf_found = 1;

	if (!GetInterMed) {
	  PFCOM->dim(data, &nr, &nc, &noffset, ProcID);
	}
	else {
	  nr = pf->tmp->nr;
	  nc = pf->tmp->nc;
	  noffset = 0;
	}
	nr_orig = nr;

	if (do_debug) {
	  printf("<0>%s(myproc=%d): pf=%p, data=%p, d=%p, pool=%d :"
		 " k1=%d, noffset=%d, Nrows=%d, nc=%d,"
		 " nr_orig=%d, Start=%d, Limit=%d, Nrows_polled=%d, GetInterMed=%d\n",
		 p_dataname, myproc, pf, data, d, pool, 
		 k1, noffset, Nrows, nc,
		 nr_orig, Start, Limit, Nrows_polled, GetInterMed);
	  fflush(stdout);
	}

	if (windowing && !GetInterMed) {
	  /* (start,limit)-windowing allowed only for non-parallel (i.e. non-shuffle/non-replicated) views */
	  if (Start >= Nrows_polled && Start < Nrows_polled + nr_orig) {
	    row_offset = Start - Nrows_polled;
	    nr = MIN(nr - row_offset, Limit);
	    Start += nr;
	    Limit -= nr;
	    Nrows_polled += nr_orig;
	  }
	  else {
	    Nrows_polled += nr_orig;
	    goto next_pool;
	  }
	}

	if (Ncols < nc) nc = Ncols;

	Nrows -= nr;

	if (do_debug) {
	  printf("<1>%s(myproc=%d): pf=%p, data=%p, d=%p, pool=%d :"
		 " k1=%d, noffset=%d, Nrows=%d, nc=%d,"
		 " nr=%d, Start=%d, Limit=%d, Nrows_polled=%d, row_offset=%d\n",
		 p_dataname, myproc, pf, data, d, pool,
		 k1, noffset, Nrows, nc,
		 nr, Start, Limit, Nrows_polled, row_offset);
	  fflush(stdout);
	}

	if (Nrows < 0) { rc = -2; goto finish; }

	if (nr > LdimD) { rc = -5; goto finish; }
	
	if (nr > 0) {
	  int k2 = k1 + nr;

	  if (do_debug) {
	    printf("<2>%s(myproc=%d): k1=%d, k2=%d, nr=%d, nc=%d ; LdimD=%d, LdimD + k1=%d ; getfunc()=%p\n",
		   p_dataname, myproc, k1, k2, nr, nc, LdimD, LdimD + k1, PFCOM->GEN_GETFUNC);
	    fflush(stdout);
	  }

	  if (GetInterMed) { 
	    int jc;
	    for (jc=0; jc<nc; jc++) {
	      int jr;
	      const double *din = pf->tmp->d[jc];
	      GEN_TYPE *dout = (GEN_TYPE_CAST *)&d[LdimD + jc*LdimD + k1];
	      for (jr=0; jr<nr; jr++) dout[jr] = din[jr];
	    }
	    rc = nr;
	  }
	  else {
	    rc = PFCOM->GEN_GETFUNC(data, (GEN_TYPE_CAST *)&d[LdimD + k1], 
				    LdimD, nr, nc, 
				    ProcID, flag, row_offset);

	    if (rc == nr && PFCOM->has_usddothash && PFCOM->tags) {
	      /* Resolve "$<parent_table>.<child_table>" -variables */
	      int jc;
	      for (jc=0; jc<nc; jc++) {
		if (FLAG_FETCH(flag[jc])) {
		  const ODB_Tags *tag = &PFCOM->tags[jc];
		  if (tag && tag->is_usddothash && 
		      tag->name && *tag->name == 'F') {
		    char *s = STRDUP(tag->name);
		    char *name = s;
		    char *p = strchr(s,'$');
		    if (p) name = p;
		    p = strchr(name,'#');
		    if (p) p[1] = '\0';
		    (void) ODB_dynfill(name,
				       data,
				       PFCOM->getindex,
				       &d[LdimD + jc*LdimD], LdimD,
				       k1, k1+nr, 0);
		    FREE(s);
		  } /* if (tag && tag->is_usddothash) */
		} /* if (FLAG_FETCH(flag[jc])) */
	      } /* for (jc=0; jc<nc; jc++) */
	    }
	  }

	  if (rc != nr) {
	    rc = -1;
	    goto finish;
	  }
	  else {
	    const int offset = 0;
	    int kk1=0, kk2=nr;

	    noffset += row_offset; /* For (start,limit) "ODB_gets" gives the correct dbidx */

	    codb_put_control_word_(&d[k1], 
				   &kk1, 
				   &kk2, 
				   &offset, 
				   &pool, 
				   &noffset);

#if 0
	    if (do_debug && ProcID > 0) {
	      int j, jc;
	      globprt = 1;
	      printf("<sub>: j = [k1=%d .. k2=%d) : (pool#%d)\n",k1,k2,pool);
	      for (j=k1; j<k2; j++) {
		const int x[4] = { 0, 1, 0, 0 };
		int poolno = 0, rownum = 0;
		double dd = d[LdimD * 0 + j];
		codb_get_poolnos_(&dd, &x[0], &x[1], &x[2], &poolno);
		codb_get_rownum_(&dd, &x[0], &x[1], &x[2],&x[3], &rownum);
		printf("<sub>: %d) [%5d:%12d]",j, poolno, rownum);
		for (jc=1; jc<=nc; jc++) {
		  printf(" %25.14g", d[LdimD * jc + j]);
		}
		printf("\n");
	      }
	      fflush(stdout);
	    }
#endif

	  }
	  
	  Nrows_total += rc;
	  k1 += rc;

	  if (do_debug) {
	    printf("<3>%s(myproc=%d): Nrows_total=%d, Limit=%d, k1=%d, rc=%d, nr=%d\n",
		   p_dataname, myproc, Nrows_total, Limit, k1, rc, nr);
	    fflush(stdout);
	  }

	  if (Pbar) {
	    double wtime;
	    zwall[1] = util_walltime_();
	    wtime = zwall[1] - zwall[0];
	    codb_progress_bar_(NULL,
			       p_dataname,
			       &p->poolno,
			       NULL,
			       &rc,
			       NULL,
			       &wtime,
			       NULL,
			       strlen(p_dataname));
	  }
	  
	} /* if (nr > 0) */
      } /* if (pf) */
    } /* if (MATCHING) */

  next_pool:
    if (pf_found && do_debug) {
      int pool = p->poolno;
      printf("<4>%s(myproc=%d): pool=%d : Nrows_total=%d, Limit=%d, HighWaterMark=%d\n",
	     p_dataname, myproc, pool, Nrows_total, Limit, HighWaterMark);
      fflush(stdout);
    }
    if (Nrows_total >= HighWaterMark) break;
    POOLREG_BREAK;
  } /* POOLREG_FOR */

  rc = Nrows_total;

 finish:
  if (do_debug) {
    printf("<<<%s(view=%s, myproc=%d, Poolno=%d, ProcID=%d, LdimD=%d, Doffset=%d, Nrows=%d, Ncols=%d, rc=%d)\n",
	   GEN_GETNAME, p_dataname, myproc, Poolno, ProcID, LdimD, Doffset, Nrows, Ncols, rc);
    fflush(stdout);
  }

  FREE_FASTFTN_CHAR(dataname);

  *retcode = rc;

#if 0
  if (globprt && rc > 0) {
    int j, jc;
    int nc = Ncols;
    int k2 = Nrows_total;
    k1 = Doffset;
    printf("<all>: j = [k1=Doffset=%d .. k2=Nrows_total=%d) :\n",k1,k2);
    for (j=k1; j<k2; j++) {
      const int x[4] = { 0, 1, 0, 0 };
      int poolno = 0, rownum = 0;
      double dd = d[LdimD * 0 + j];
      codb_get_poolnos_(&dd, &x[0], &x[1], &x[2], &poolno);
      codb_get_rownum_(&dd, &x[0], &x[1], &x[2],&x[3], &rownum);
      printf("<all>: %d) [%5d:%12d]",j, poolno, rownum);
      for (jc=1; jc<=nc; jc++) {
	printf(" %25.14g", d[LdimD * jc + j]);
      }
      printf("\n");
    }
    fflush(stdout);
  }
#endif

  if (drhook_lhook && rc >= 0) {
    int jf;
    Neff_rows = rc;
    for (jf=0; jf<Ncols; jf++) {
      if (flag[jf] & 0x1) Neff_cols++;
    }
  }
  DRHOOK_END(Neff_rows * Neff_cols * GEN_BYTES);
}


PUBLIC void
GEN_PUT(const int *handle,
	const int  *poolno,
	const char *dataname,
	const GEN_TYPE d[],
	const int *doffset,
	const int *ldimd,
	const int *nrows,
	const int *ncols,
	const int  flag[],
	const int *procid,
	const int *fill_intermed,
	const int *using_it,
	int *retcode,
	/* Hidden arguments */
	int dataname_len)
{
  int rc = 0;
  int Handle = *handle;
  int Poolno = *poolno;
  int ProcID = *procid;
  boolean AnyPool = (Poolno == -1);
  int LdimD = *ldimd;
  int Doffset = *doffset;
  int Nrows_in = *nrows;
  int Nrows = *nrows;
  int Ncols_in = *ncols;
  int Ncols = *ncols;
  int Neff_rows = 0;
  int Neff_cols = 0;
  int Nrows_total = 0;
  int k1 = Doffset;
  POOLREG_DEF;
  int it = USING_IT;
  static int myproc = 0;
  int PutInterMed = *fill_intermed;
  DECL_FTN_CHAR(dataname);
  static char *env = NULL;
  static int first_time = 1;
  static int do_debug = 0;
  DRHOOK_START_BY_STRING(GEN_PUTNAME);

  if (first_time) {
    env = getenv("ODB_DEBUG_CODB_H");
    if (env) {
      int value = atoi(env);
      codb_procdata_(&myproc, NULL, NULL, NULL, NULL);
      if (value == -1 || value == myproc) do_debug = 1;
    }
    first_time = 0;
  }

  ALLOC_FASTFTN_CHAR(dataname);

  if (do_debug) {
    printf("\n>>>%s(view=%s, myproc=%d, Poolno=%d, ProcID=%d, LdimD=%d, Doffset=%d, Nrows=%d, Ncols=%d)\n",
	   GEN_PUTNAME, p_dataname, myproc, Poolno, ProcID, LdimD, Doffset, Nrows, Ncols);
    fflush(stdout);
  }
  
  if (LdimD - Doffset < Nrows) { rc = -3; goto finish; }

  POOLREG_FOR {
    if (MATCHING) {
      ODB_Funcs *pf = get_forfunc(p->handle, p->dbname, p->poolno, it, p_dataname, 1);

      if (pf) {
	void *data = pf->data;
	boolean is_table = PFCOM->is_table;
	int nr, nc;
	int pool = p->poolno;
	int Npools = 1;
	int offset = 0;

	if (do_debug) {
	  printf("<1>%s(myproc=%d): pf=%p, data=%p, d=%p, pool=%d, is_table=%d : k1=%d, PutInterMed=%d\n",
		 p_dataname, myproc, pf, data, d, pool, (int)is_table, k1, PutInterMed);
	  fflush(stdout);
	}

	if (!PutInterMed) {
	  PFCOM->dim(data, &nr, &nc, NULL, ProcID);
	}
	else {
	  nr = Nrows_in;
	  nc = Ncols_in;
	}

	if (is_table) {
	  nr = Nrows_in; /* by definition we do this pool only for TABLEs; Poolno cannot be -1 for TABLEs */
	}

	if (PutInterMed) {
	  int jc;
	  DELETE_INTERMED(pf->tmp);
	  ALLOC(pf->tmp,1);
	  ALLOC(pf->tmp->d, nc);
	  pf->tmp->nc = nc;
	  for (jc=0; jc<nc; jc++) ALLOC(pf->tmp->d[jc], nr);
	  pf->tmp->nr = 0; /* To be changed after data has been filled */
	}

	if (do_debug) {
	  printf("<2>%s(myproc=%d): nr=%d, nc=%d\n", p_dataname, myproc, nr, nc);
	  fflush(stdout);
	}

	if (nr > LdimD) { rc = -5; goto finish; }

	if (nr > 0) {
	  if (Ncols < nc) nc = Ncols;
	  
	  if (!is_table) {
	    int k2 = k1 + nr;
	    int kk1=0, kk2=nr;
	    int count = 0;

	    codb_get_pool_count_(&d[k1], 
				 &kk1, 
				 &kk2,
				 &offset,
				 &Npools, 
				 &pool, 
				 &count);

	    if (do_debug) {
	      printf("<3>%s(myproc=%d): k1=%d, k2=%d, nr=%d, nc=%d, count=%d ; LdimD=%d, LdimD + k1=%d ; putfunc()=%p\n",
		     p_dataname, myproc, k1, k2, nr, nc, count, LdimD, LdimD + k1, PFCOM->GEN_PUTFUNC);
	      fflush(stdout);
	    }

	    if (count != nr) { rc = -2; goto finish; }
	  }

	  Nrows -= nr;

	  if (PutInterMed) {
	    int jc;
	    for (jc=0; jc<nc; jc++) {
	      int jr;
	      double *dout = pf->tmp->d[jc];
	      const GEN_TYPE *din = (GEN_TYPE_CAST *)&d[LdimD + jc*LdimD + k1];
	      for (jr=0; jr<nr; jr++) dout[jr] = din[jr];
	    }
	    rc = pf->tmp->nr = nr;
	  }
	  else {
	    if (PFCOM->GEN_PUTFUNC) {
	      rc = PFCOM->GEN_PUTFUNC(data, (const GEN_TYPE_CAST *)&d[LdimD + k1],
				      LdimD, nr, nc, 
				      ProcID, flag);
	    }
	    else
	      rc = nr;
	  }

	  if (is_table) {
	    /* ... since "rc" denotes total no. of rows in TABLE so far */
	    if (rc < 0) { rc = -4; goto finish; }
	  }
	  else {
	    if (rc != nr) { rc = -1; goto finish; }
	  }
	  if (PFCOM->GEN_PUTFUNC) Neff_rows += nr;
	    
	  Nrows_total += rc;
	  k1 += rc; /* Meaningful for VIEWs only */
	} /* if (nr > 0) */
      } /* if (pf) */
    } /* if (MATCHING) */
    POOLREG_BREAK;
  } /* POOLREG_FOR */

  rc = Nrows_total;

 finish:
  if (do_debug) {
    printf("<<<%s(view=%s, myproc=%d, Poolno=%d, ProcID=%d, LdimD=%d, Doffset=%d, Nrows=%d, Ncols=%d, rc=%d)\n",
	   GEN_PUTNAME, p_dataname, myproc, Poolno, ProcID, LdimD, Doffset, Nrows, Ncols, rc);
    fflush(stdout);
  }

  FREE_FASTFTN_CHAR(dataname);

  *retcode = rc;

  if (drhook_lhook && rc >= 0) {
    int jf;
    for (jf=0; jf<Ncols; jf++) {
      if (flag[jf] & 0x1) Neff_cols++;
    }
  }
  DRHOOK_END(Neff_rows * Neff_cols * GEN_BYTES);
}

#undef GEN_TYPE
#undef GEN_TYPE_CAST
#undef GEN_GET
#undef GEN_PUT
#undef GEN_GETFUNC
#undef GEN_PUTFUNC
#undef GEN_GETNAME
#undef GEN_PUTNAME
#undef GEN_BYTES

#endif

