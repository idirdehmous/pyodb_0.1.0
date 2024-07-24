
/* aggr.c */

#include "odb.h"
#include "idx.h"
#include "cdrhook.h"
#include "pcma_extern.h"

static const double mdi = ABS(RMDI);

#define NAME_FLAG(x) #x, x

typedef struct {
  const char *name;
  uint aggr_func_flag;
} name_flag_t;

PRIVATE name_flag_t 
name_flag[] = {
  NAME_FLAG(ODB_AGGR_NONE           ),
  NAME_FLAG(ODB_AGGR_COUNT          ),
  NAME_FLAG(ODB_AGGR_COUNT_DISTINCT ),
  NAME_FLAG(ODB_AGGR_BCOUNT         ),
  NAME_FLAG(ODB_AGGR_BCOUNT_DISTINCT),
  NAME_FLAG(ODB_AGGR_MIN            ),
  NAME_FLAG(ODB_AGGR_MAX            ),
  NAME_FLAG(ODB_AGGR_SUM            ),
  NAME_FLAG(ODB_AGGR_SUM_DISTINCT   ),
  NAME_FLAG(ODB_AGGR_AVG            ),
  NAME_FLAG(ODB_AGGR_AVG_DISTINCT   ),
  NAME_FLAG(ODB_AGGR_MEDIAN         ),
  NAME_FLAG(ODB_AGGR_MEDIAN_DISTINCT),
  NAME_FLAG(ODB_AGGR_STDEV          ),
  NAME_FLAG(ODB_AGGR_STDEV_DISTINCT ),
  NAME_FLAG(ODB_AGGR_RMS            ),
  NAME_FLAG(ODB_AGGR_RMS_DISTINCT   ),
  NAME_FLAG(ODB_AGGR_DOTP           ),
  NAME_FLAG(ODB_AGGR_DOTP_DISTINCT  ),
  NAME_FLAG(ODB_AGGR_NORM           ),
  NAME_FLAG(ODB_AGGR_NORM_DISTINCT  ),
  NAME_FLAG(ODB_AGGR_VAR            ),
  NAME_FLAG(ODB_AGGR_VAR_DISTINCT   ),
  NAME_FLAG(ODB_AGGR_COVAR          ),
  NAME_FLAG(ODB_AGGR_CORR           ),
  NAME_FLAG(ODB_AGGR_LINREGR_A      ),
  NAME_FLAG(ODB_AGGR_LINREGR_B      ),
  NAME_FLAG(ODB_AGGR_MINLOC         ),
  NAME_FLAG(ODB_AGGR_MAXLOC         ),
  NAME_FLAG(ODB_AGGR_DENSITY        ),
  NULL,
};

PRIVATE const char *get_name_flag(uint aggr_func_flag)
{
  static const char *not_found = "<Unrecognized aggregate function>";
  name_flag_t *p = name_flag;
  while (p && p->name) {
    if (p->aggr_func_flag == aggr_func_flag) return p->name;
    p++;
  }
  return not_found;
}

#define N_AUX 5

typedef struct {
  /* Auxiliary info for more economic aggregate funcs implementation */
  uint aggr_func_flag;
  int num; /* No. of d[N_AUX] entries that will be filled : 0 or 1 or 2 or ... up to N_AUX */
  int nd; /* Number of values accumulated in d[]'s */
  int ind; /* Number of values accumulated in d[]'s that satisfy : ABS(value) != mdi */
  double d[N_AUX]; /* partial sums and/or sum-squares etc. so far */
  double *uniq; /* A copy of "nd" unique values so far (for xxx_DISTINCT) */
} Aggr_aux;


typedef union {
  double daddr; /* 8 bytes */
  Aggr_aux *addr; /* 4 or 8 bytes address */
} Aggr_address_fudge;


PRIVATE int cmp(const void *A, const void *B) {
  const double *a = A;
  const double *b = B;
  if      ( *a < *b ) return -1;
  else if ( *a > *b ) return  1;
  else		      return  0;
}

#define MEDIAN(x,n) ( ((n)%2 == 0) ? (((x)[(n)/2] + (x)[(n)/2 + 1])/2) : (x)[(n)/2] )

PRIVATE void
CalcAggr0(const int    *aggr_func_flag,
	  const double  d[],
	  const double  daux[],
	  const int    *nd, 
	  double        rc[])
{
  DRHOOK_START(CalcAggr0);
  if (aggr_func_flag && d && nd && rc) {
    int Nd = *nd;
    uint flag = (uint) *aggr_func_flag;
    double value = RMDI; /* Initialized to missing data indicator */
    if (Nd >= 0 && flag != ODB_AGGR_NONE) {
      Aggr_aux *aux = NULL;
      int j;
      CALLOC(aux,1);
      aux->aggr_func_flag = flag;
      if ((flag & ODB_AGGRMASK_DISTINCT) == ODB_AGGRMASK_DISTINCT) {
	/* {COUNT|SUM|AVG|MEDIAN|STDEV|RMS|DOTP|NORM|VAR}(DISTINCT arg) */
	int ncard = 0;
	int *unique_idx = NULL;

	if (Nd > 0) {
	  int bailout = -1; /* bailing out turned off; can be slow */
	  const int ncols = 1;
	  int lda, nrows;
	  lda = nrows = Nd;
	  ALLOC(unique_idx, nrows);
	  codb_cardinality_(&ncols, &nrows, &lda,
			    d, &ncard,
			    unique_idx,
			    &nrows,
			    &bailout);
	}
	else
	  ncard = 0;

	aux->num = 0;
	if (ncard > 0) {
	  int iNd = 0;
	  aux->nd = ncard;
	  ALLOC(aux->uniq, ncard);
	  for (j=0; j<ncard; j++) {
	    double dd = d[unique_idx[j]-1];
	    aux->uniq[j] = dd;
	    if (ABS(dd) != mdi) ++iNd;
	  }
	  aux->ind = iNd;
	}

	FREE(unique_idx);
      }
      else {
	/* Just {COUNT|SUM|AVG|MEDIAN|STDEV|RMS|MIN|MAX|DOTP|NORM|VAR}(arg) */
	switch (flag) {
	case ODB_AGGR_COUNT:
	  aux->num = 0;
	  aux->nd = aux->ind = Nd;
	  value = Nd;
	  break;
	case ODB_AGGR_BCOUNT:
	  {
	    int ntrue = 0;
	    for (j=0; j<Nd; j++) if (d[j] != 0) ntrue++;
	    aux->num = 0;
	    aux->nd = aux->ind = ntrue;
	    value = ntrue;
	  }
	  break;
	case ODB_AGGR_RMS:
	  {
	    int iNd = 0;
	    value = 0;
	    for (j=0; j<Nd; j++) {
	      if (ABS(d[j]) != mdi) {
		value += d[j] * d[j];
		++iNd;
	      }
	    }
	    aux->num = 1;
	    aux->nd = Nd;
	    aux->ind = iNd;
	    aux->d[0] = (iNd > 0) ? value : RMDI;
	  }
	  break;
	case ODB_AGGR_MEDIAN:
	  aux->num = 0;
	  aux->nd = aux->ind = Nd;
	  ALLOC(aux->uniq, Nd);
	  for (j=0; j<Nd; j++) aux->uniq[j] = d[j];
	  break;
	case ODB_AGGR_AVG:
	case ODB_AGGR_SUM:
	case ODB_AGGR_DENSITY:
	case ODB_AGGR_DOTP:
	case ODB_AGGR_NORM:
	  {
	    int iNd = 0;
	    value = 0;
	    if (flag == ODB_AGGR_DENSITY) {
	      double min_resol = ODB_eq_min_resol();
	      for (j=0; j<Nd; j++) {
		if (ABS(d[j]) != mdi) {
		  value += MAX(d[j],min_resol);
		  ++iNd;
		}
	      }
	    }
	    else {
	      for (j=0; j<Nd; j++) {
		if (ABS(d[j]) != mdi) {
		  value += d[j];
		  ++iNd;
		}
	      }
	    }
	    aux->num = 1;
	    aux->nd = Nd;
	    aux->ind = iNd;
	    aux->d[0] = (iNd > 0) ? value : RMDI;
	  }
	  break;
	case ODB_AGGR_STDEV:
	case ODB_AGGR_VAR:
	  {
	    int iNd = 0;
	    double sum = 0;
	    value = 0;
	    for (j=0; j<Nd; j++) {
	      if (ABS(d[j]) != mdi) {
		sum += d[j];
		value += d[j] * d[j];
		++iNd;
	      }
	    }
	    aux->num = 2;
	    aux->nd = Nd;
	    aux->ind = iNd;
	    aux->d[0] = (iNd > 0) ? sum : RMDI;
	    aux->d[1] = (iNd > 0) ? value : RMDI;
	  }
	  break;
	case ODB_AGGR_MINLOC:
	case ODB_AGGR_MAXLOC:
	  {
	    int iNd = 0;
	    double loc;
	    if (daux && Nd > 0) {
	      int jj = 0;
	      /* In case the first one is a missing data indicator */
	      while (jj<Nd && ABS(d[jj]) == mdi) jj++;
	      if (jj<Nd) {
		j = jj;
		loc = daux[j];
		value = d[j];
		++iNd;
		if (flag == ODB_AGGR_MINLOC) {
		  for (j=jj+1; j<Nd; j++) {
		    if (ABS(d[j]) != mdi) {
		      if (value > d[j]) {
			value = d[j];
			loc = daux[j];
		      }
		      ++iNd;
		    }
		  } /* for (j=jj+1; j<Nd; j++) */
		}
		else { /* ODB_AGGR_MAXLOC */
		  for (j=jj+1; j<Nd; j++) {
		    if (ABS(d[j]) != mdi) {
		      if (value < d[j]) {
			value = d[j];
			loc = daux[j];
		      }
		      ++iNd;
		    }
		  } /* for (j=jj+1; j<Nd; j++) */
		}
	      } /* if (jj<Nd) */
	    } /* if (daux && Nd > 0) */

	    aux->num = 2;
	    aux->nd = Nd;
	    aux->ind = iNd;
	    aux->d[0] = (iNd > 0) ? value : RMDI;
	    aux->d[1] = (iNd > 0) ? loc : RMDI;
	  }
	  break;
	case ODB_AGGR_MIN:
	case ODB_AGGR_MAX:
	  {
	    int iNd = 0;
	    if (Nd > 0) {
	      int jj = 0;
	      /* In case the first one is a missing data indicator */
	      while (jj<Nd && ABS(d[jj]) == mdi) jj++;
	      if (jj<Nd) {
		j = jj;
		value = d[j];
		++iNd;
		if (flag == ODB_AGGR_MIN) {
		  for (j=jj+1; j<Nd; j++) {
		    if (ABS(d[j]) != mdi) {
		      if (value > d[j]) value = d[j];
		      ++iNd;
		    }
		  } /* for (j=jj+1; j<Nd; j++) */
		}
		else { /* ODB_AGGR_MAX */
		  for (j=jj+1; j<Nd; j++) {
		    if (ABS(d[j]) != mdi) {
		      if (value < d[j]) value = d[j];
		      ++iNd;
		    }
		  } /* for (j=jj+1; j<Nd; j++) */
		}
	      }
	    } /* if (Nd > 0) */
	    aux->num = 1;
	    aux->nd = Nd;
	    aux->ind = iNd;
	    aux->d[0] = (iNd > 0) ? value : RMDI;
	  }
	  break;
	case ODB_AGGR_LINREGR_A:
	case ODB_AGGR_LINREGR_B:
	case ODB_AGGR_CORR:
	case ODB_AGGR_COVAR:
	  {
	    /* y = A + B * x */
	    int iNd = 0;
	    value = RMDI;
	    aux->num = 5;
	    if (daux && Nd > 0) {
	      for (j=0; j<aux->num; j++) aux->d[j] = 0;
	      for (j=0; j<Nd; j++) {
		if (ABS(d[j]) != mdi && ABS(daux[j]) != mdi) {
		  aux->d[0] += d[j]; /* x := d[] , sum(x) */
		  aux->d[1] += daux[j]; /* y := daux[] , sum(y)*/
		  aux->d[2] += d[j] * d[j]; /* sum(x^2) */
		  aux->d[3] += daux[j] * daux[j]; /* sum(y^2) */
		  aux->d[4] += d[j] * daux[j]; /* sum(x * y) */
		  ++iNd;
		}
	      } /* for (j=0; j<Nd; j++) */
	    }
	    aux->nd = Nd;
	    aux->ind = iNd;
	  }
	  break;
	}
      }
      if (aux) {
	Aggr_address_fudge afu;
	afu.daddr = 0;
	afu.addr = aux;
	value = afu.daddr;
      }
    }
    else if (Nd == 1 && flag == ODB_AGGR_NONE) {
      value = d[0];
    }
    *rc = value;
  }
  DRHOOK_END(0);
}


PRIVATE void
CalcAggr1(const int    *aggr_func_flag,
	  const double  daddr[],
	  const int    *ndaddr, 
	  double       *rc)
{
  DRHOOK_START(CalcAggr1);
  if (aggr_func_flag && daddr && ndaddr && rc) {
    int Ndaddr = *ndaddr;
    uint flag = (uint) *aggr_func_flag;
    double value = RMDI; /* Initialized to missing data indicator */
    if (Ndaddr >= 0 && flag != ODB_AGGR_NONE) {
      int j, jf;

      if ((flag & ODB_AGGRMASK_DISTINCT) == ODB_AGGRMASK_DISTINCT) {
	/* {COUNT|SUM|AVG|STDEV|RMS|DOTP|NORM|VAR}(DISTINCT arg) */
	int ncard = 0;
	int *unique_idx = NULL;
	double *d = NULL;
	int Nd = 0;
	int iNd = 0;
	Bool is_count_dist = (flag == ODB_AGGR_COUNT_DISTINCT) ? true : false;
	int Nfill;

	/* Re-construct d[] */
	{
	  for (jf=0; jf<Ndaddr; jf++) {
	    Aggr_aux *aux;
	    Aggr_address_fudge afu;
	    afu.daddr = daddr[jf];
	    aux = afu.addr;
	    if (aux && aux->uniq && aux->nd > 0) {
	      Nd += aux->nd;
	      iNd += aux->ind;
	    } /* if (aux) */
	  } /* for (jf=0; jf<Ndaddr; jf++) */

	  if (Nd > 0) {
	    int jd = 0;
	    ALLOC(d, Nd);
	    for (jf=0; jf<Ndaddr; jf++) {
	      Aggr_aux *aux;
	      Aggr_address_fudge afu;
	      afu.daddr = daddr[jf];
	      aux = afu.addr;
	      if (aux && aux->uniq && aux->nd > 0) {
		int nnd = aux->nd; /* Upper limit ; due to mdi's, this may never get filled that much */
		for (j=0; j<nnd; j++) {
		  double dd = aux->uniq[j];
		  if (is_count_dist || ABS(dd) != mdi) d[jd++] = dd; /* Don't fill mdi's */
		}
	      } /* if (aux) */
	    } /* for (jf=0; jf<Ndaddr; jf++) */
	  }

	  /* Release aux-space */
	  for (jf=0; jf<Ndaddr; jf++) {
	    Aggr_aux *aux;
	    Aggr_address_fudge afu;
	    afu.daddr = daddr[jf];
	    aux = afu.addr;
	    if (aux) {
	      FREE(aux->uniq);
	      FREE(aux);
	    } /* if (aux) */
	  } /* for (jf=0; jf<Ndaddr; jf++) */
	}

	Nfill = is_count_dist ? Nd : iNd;

	if (Nfill > 0) {
	  int bailout = -1; /* bailing out turned off; can be slow */
	  const int ncols = 1;
	  int lda, nrows;
	  lda = nrows = Nfill;
	  if (!is_count_dist) ALLOC(unique_idx, nrows);
	  codb_cardinality_(&ncols, &nrows, &lda,
			    d, &ncard,
			    unique_idx,
			    &nrows,
			    &bailout);
	}
	else
	  ncard = 0;

	switch (flag) {
	case ODB_AGGR_COUNT_DISTINCT:
	  value = ncard; /* Note : we INCLUDE missing datas in count() */
	  break;
	case ODB_AGGR_BCOUNT_DISTINCT:
	  {
	    int ntrue = 0;
	    for (j=0; j<ncard; j++) {
	      double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	      if (dd != 0) ntrue++;
	    }
	    value = ntrue;
	  }
	  break;
	case ODB_AGGR_RMS_DISTINCT:
	  value = 0;
	  for (j=0; j<ncard; j++) {
	    double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	    value += dd * dd;
	  }
	  if (ncard > 0) 
	    value = (value > 0) ? sqrt(value/ncard) : (double)0;
	  else 
	    value = RMDI;
	  break;
	case ODB_AGGR_MEDIAN_DISTINCT:
	  value = RMDI;
	  if (ncard > 0) {
	    double *dtmp = NULL;
	    ALLOC(dtmp, ncard);
	    for (j=0; j<ncard; j++) dtmp[j] = d[unique_idx[j]-1];
	    qsort(dtmp, ncard, sizeof(*dtmp), cmp);
	    value = MEDIAN(dtmp,ncard); /* median */
	    FREE(dtmp);
	  }
	  break;
	case ODB_AGGR_AVG_DISTINCT:
	case ODB_AGGR_SUM_DISTINCT:
	case ODB_AGGR_NORM_DISTINCT:
	case ODB_AGGR_DOTP_DISTINCT:
	  value = 0;
	  for (j=0; j<ncard; j++) {
	    double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	    value += dd;
	  }
	  if (ncard > 0) {
	    if (flag == ODB_AGGR_AVG_DISTINCT) value /= ncard;
	    else if (flag == ODB_AGGR_NORM_DISTINCT) value = (value > 0) ? sqrt(value) : (double)0;
	  }
	  else
	    value = RMDI;
	  break;
	case ODB_AGGR_STDEV_DISTINCT:
	case ODB_AGGR_VAR_DISTINCT:
	  {
	    double avg = 0;
	    for (j=0; j<ncard; j++) {
	      double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	      avg += dd;
	    }
	    if (ncard > 0) {
	      avg /= ncard;
	      value = 0;
	      for (j=0; j<ncard; j++) {
		double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
		dd -= avg;
		value += dd * dd;
	      }
	    }
	    if (ncard > 1) {
	      value = value/(ncard-1);
	      if (flag == ODB_AGGR_STDEV_DISTINCT) value = sqrt(value);
	    }
	    else if (ncard == 1)
	      value = 0;
	    else
	      value = RMDI;
	  }
	  break;
	}
	FREE(unique_idx);
	FREE(d);
      }
      else {
	/* Just {COUNT|SUM|AVG|MEDIAN|STDEV|RMS|MIN|MAX|DOTP|NORM|VAR}(arg) */
	int Nd = 0;
	int iNd = 0;
	int N[N_AUX];
	double **d;
	double *tmp;

	ALLOCX(d, N_AUX);
	for (j=0; j<N_AUX; j++) {
	  N[j] = 0;
	  d[j] = NULL;
	}

	for (jf=0; jf<Ndaddr; jf++) {
	  Aggr_aux *aux;
	  Aggr_address_fudge afu;
	  afu.daddr = daddr[jf];
	  aux = afu.addr;
	  if (aux) {
	    Nd += aux->nd;
	    iNd += aux->ind;
	    for (j=0; j<aux->num; j++) N[j]++;
	  } /* if (aux) */
	} /* for (jf=0; jf<Ndaddr; jf++) */

	if (flag != ODB_AGGR_MEDIAN) {
	  for (j=0; j<N_AUX; j++) {
	    if (N[j] > 0) {
	      int jd = 0;
	      ALLOC(d[j], N[j]);
	      tmp = d[j];
	      for (jf=0; jf<Ndaddr; jf++) {
	        Aggr_aux *aux;
		Aggr_address_fudge afu;
		afu.daddr = daddr[jf];
		aux = afu.addr;
		if (aux && aux->num > 0) {
		  double dd = aux->d[j];
		  tmp[jd++] = dd; /* Missing datas checked later */
		}
	      }
	    } /* if (N[j] > 0) */
	  } /* for (j=0; j<N_AUX; j++) */
	}
	else { /* flag == ODB_AGGR_MEDIAN */
	  int jd = 0;
	  ALLOC(tmp, Nd); /* Upper limit ; due to mdi's, this may never get filled that much */
	  for (jf=0; jf<Ndaddr; jf++) {
	    Aggr_aux *aux;
	    Aggr_address_fudge afu;
	    afu.daddr = daddr[jf];
	    aux = afu.addr;
	    if (aux && aux->uniq && aux->nd > 0 && aux->ind > 0) {
	      int nnd = aux->nd;
	      for (j=0; j<nnd; j++) {
		double dd = aux->uniq[j];
		if (ABS(dd) != mdi) tmp[jd++] = dd;
	      }
	    } /* if (aux && aux->uniq && aux->num > 0) */
	  } /* for (jf=0; jf<Ndaddr; jf++) */ 
	}

	/* Release aux-space */
	for (jf=0; jf<Ndaddr; jf++) {
	  Aggr_aux *aux;
	  Aggr_address_fudge afu;
	  afu.daddr = daddr[jf];
	  aux = afu.addr;
	  if (aux) {
	    FREE(aux->uniq);
	    FREE(aux);
	  } /* if (aux) */
	} /* for (jf=0; jf<Ndaddr; jf++) */

	switch (flag) {
	case ODB_AGGR_COUNT:
	case ODB_AGGR_BCOUNT:
	  value = Nd; /* Note : we INCLUDE missing datas in count() */
	  break;
	case ODB_AGGR_MEDIAN:
	  value = RMDI;
	  if (iNd > 0) {
	    qsort(tmp, iNd, sizeof(*tmp), cmp);
	    value = MEDIAN(tmp,iNd); /* median */
	  }
	  FREE(tmp);
	  break;
	case ODB_AGGR_RMS:
	case ODB_AGGR_AVG:
	case ODB_AGGR_SUM:
	case ODB_AGGR_DENSITY:
	case ODB_AGGR_DOTP:
	case ODB_AGGR_NORM:
	  value = 0;
	  tmp = d[0];
	  if (iNd > 0) {
	    for (j=0; j<N[0]; j++) {
	      double dd = tmp[j];
	      if (ABS(dd) != mdi) value += dd;
	    }
	    if (flag == ODB_AGGR_RMS) value = (value > 0) ? sqrt(value/iNd) : (double)0;
	    else if (flag == ODB_AGGR_AVG) value /= iNd;
	    else if (flag == ODB_AGGR_NORM) value = (value > 0) ? sqrt(value) : (double)0;
	    else if (flag == ODB_AGGR_DENSITY) {
	      double resol = value/iNd; /* Average resolution, in degrees */
	      /* Approximate area of a small spherical eq_region [in degrees^2] */
	      double area  = resol * resol;
	      value = iNd / area; /* Density = # of points divided by the approx. area  */
	    }
	  }
	  else
	    value = RMDI;
	  break;
	case ODB_AGGR_STDEV:
	case ODB_AGGR_VAR:
	  if (iNd > 1) {
	    /* Now STDEV = sqrt((sumsqr - sum*sum/iNd)/(iNd-1)) */
	    double sum = 0;    /* i.e. aux->d[0] */
	    double sumsqr = 0; /* i.e. aux->d[1] */
	    tmp = d[0];
	    for (j=0; j<N[0]; j++) {
	      double dd = tmp[j];
	      if (ABS(dd) != mdi) sum += dd;
	    }
	    tmp = d[1];
	    for (j=0; j<N[1]; j++) {
	      double dd = tmp[j];
	      if (ABS(dd) != mdi) sumsqr += dd;
	    }
	    if (iNd > 1) {
	      value = sumsqr - (sum * (sum/iNd));
	      value = (value > 0) ? (value/(iNd-1)) : (double)0;
	      if (flag == ODB_AGGR_STDEV) value = sqrt(value);
	    }
	    else if (iNd == 1)
	      value = 0;
	    else
	      value = RMDI;
	  }
	  else if (iNd == 1)
	    value = 0;
	  else
	    value = RMDI;
	  break;
	case ODB_AGGR_MINLOC:
	case ODB_AGGR_MAXLOC:
	  value = RMDI;
	  if (iNd > 0) {
	    int jj = 0;
	    double loc = RMDI;
	    tmp = d[0];
	    /* In case the first one is a missing data indicator */
	    while (jj<N[0] && ABS(tmp[jj]) == mdi) jj++;
	    if (jj<N[0]) {
	      value = tmp[jj];
	      loc = d[1][jj];
	      if (flag == ODB_AGGR_MINLOC) {
		for (j=jj+1; j<N[0]; j++) {
		  double dd = tmp[j];
		  if (ABS(dd) != mdi && value > dd) {
		    value = dd;
		    loc = d[1][j];
		  }
		} /* for (j=1; j<N[0]; j++) */
	      }
	      else { /* ODB_AGGR_MAXLOC */
		for (j=jj+1; j<N[0]; j++) {
		  double dd = tmp[j];
		  if (ABS(dd) != mdi && value < dd) {
		    value = dd;
		    loc = d[1][j];
		  }
		} /* for (j=1; j<N[0]; j++) */
	      }
	    }
	    value = (ABS(loc) != mdi) ? loc : RMDI;
	  }
	  break;
	case ODB_AGGR_MIN:
	case ODB_AGGR_MAX:
	  value = RMDI;
	  if (iNd > 0) {
	    int jj = 0;
	    tmp = d[0];
	    /* In case the first one is a missing data indicator */
	    while (jj<N[0] && ABS(tmp[jj]) == mdi) jj++;
	    if (jj<N[0]) {
	      value = tmp[jj];
	      if (flag == ODB_AGGR_MIN) {
		for (j=jj+1; j<N[0]; j++) {
		  double dd = tmp[j];
		  if (ABS(dd) != mdi && value > dd) value = dd;
		}
	      }
	      else { /* ODB_AGGR_MAX */
		for (j=jj+1; j<N[0]; j++) {
		  double dd = tmp[j];
		  if (ABS(dd) != mdi && value < dd) value = dd;
		}
	      }
	    } /* if (jj<N[0]) */
	  }
	  break;
	case ODB_AGGR_LINREGR_A:
	case ODB_AGGR_LINREGR_B:
	case ODB_AGGR_CORR:
	case ODB_AGGR_COVAR:
	  /* y = A + B * x */
	  value = RMDI;
	  if (iNd > 0) {
	    double sxy, sxx, syy;
	    double sumx = 0, sumy = 0, sumx2 = 0, sumy2 = 0, sumxy = 0;
	    double xavg, yavg;
	    tmp = d[0];
	    for (j=0; j<N[0]; j++) {
	      double dd = tmp[j];
	      if (ABS(dd) != mdi) sumx += dd;
	    }
	    xavg = sumx/iNd;
	    tmp = d[1];
	    for (j=0; j<N[1]; j++) {
	      double dd = tmp[j];
	      if (ABS(dd) != mdi) sumy += dd;
	    }
	    yavg = sumy/iNd;
	    tmp = d[2];
	    for (j=0; j<N[2]; j++) {
	      double dd = tmp[j];
	      if (ABS(dd) != mdi) sumx2 += dd;
	    }
	    tmp = d[3];
	    for (j=0; j<N[3]; j++) {
	      double dd = tmp[j];
	      if (ABS(dd) != mdi) sumy2 += dd;
	    }
	    tmp = d[4];
	    for (j=0; j<N[4]; j++) {
	      double dd = tmp[j];
	      if (ABS(dd) != mdi) sumxy += dd;
	    }
	    sxy = sumxy - iNd * xavg * yavg; 
	    sxx = sumx2 - iNd * xavg * xavg;
	    syy = sumy2 - iNd * yavg * yavg;
	    if (flag == ODB_AGGR_CORR) {
	      value = (sxx != 0 && syy != 0) ? (sxy * sxy)/(sxx * syy) : (double)0;
	      value = (value > 0) ? sqrt(value) : (double)0;
	    }
	    else if (flag == ODB_AGGR_COVAR) {
	      value = sxy/iNd;
	    }
	    else { /* ODB_AGGR_LINREGR_A or ODB_AGGR_LINREGR_B */
	      double b = (sxx != 0) ? sxy/sxx : 1e-30;
	      double a = yavg - b * xavg;
	      value = (flag == ODB_AGGR_LINREGR_B) ? b : a;
	    }
	  }
	  break;
	}

	for (j=0; j<N_AUX; j++) {
	  FREE(d[j]);
	}
	FREEX(d);
      }
    }
    else if (Ndaddr == 1 && flag == ODB_AGGR_NONE) {
      value = daddr[0];
    }
    *rc = value;
  }
  DRHOOK_END(0);
}


PRIVATE void
CalcAggrAll(const int    *aggr_func_flag,
	    const double  d[],
	    const double  daux[],
	    const int    *nd, 
	    double        rc[])
{
  DRHOOK_START(CalcAggrAll);
  if (aggr_func_flag && d && nd && rc) {
    int Nd = *nd;
    uint flag = (uint) *aggr_func_flag;
    double value = RMDI; /* Initialized to missing data indicator */
    double value_aux =  RMDI;
    if (Nd >= 0 && flag != ODB_AGGR_NONE) {
      int j;
      if ((flag & ODB_AGGRMASK_DISTINCT) == ODB_AGGRMASK_DISTINCT) {
	/* {COUNT|SUM|AVG|MEDIAN|STDEV|RMS|DOTP|NORM|VAR}(DISTINCT arg) */
	int ncard = 0;
	int *unique_idx = NULL;

	if (Nd > 0) {
	  int bailout = -1; /* bailing out turned off; can be slow */
	  const int ncols = 1;
	  int lda, nrows;
	  lda = nrows = Nd;
	  if (flag != ODB_AGGR_COUNT_DISTINCT) ALLOC(unique_idx, nrows);
	  codb_cardinality_(&ncols, &nrows, &lda,
			    d, &ncard,
			    unique_idx,
			    &nrows,
			    &bailout);
	}
	else
	  ncard = 0;

	switch (flag) {
	case ODB_AGGR_COUNT_DISTINCT:
	  value = ncard;
	  break;
	case ODB_AGGR_BCOUNT_DISTINCT:
	  {
	    int ntrue = 0;
	    for (j=0; j<ncard; j++) {
	      double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	      if (dd != 0) ntrue++;
	    }
	    value = ntrue;
	  }
	  break;
	case ODB_AGGR_RMS_DISTINCT:
	  {
	    int iNd = 0;
	    value = 0;
	    for (j=0; j<ncard; j++) {
	      double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	      if (ABS(dd) != mdi) {
		value += dd * dd;
		++iNd;
	      }
	    }
	    if (iNd > 0) 
	      value = (value > 0) ? sqrt(value/iNd) : (double)0;
	    else 
	      value = RMDI;
	  }
	  break;
	case ODB_AGGR_MEDIAN_DISTINCT:
	  value = RMDI;
	  if (ncard > 0) {
	    double *dtmp = NULL;
	    ALLOC(dtmp, ncard);
	    for (j=0; j<ncard; j++) 
	      dtmp[j] = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	    qsort(dtmp, ncard, sizeof(*dtmp), cmp);
	    value = MEDIAN(dtmp, ncard); /* median */
	    FREE(dtmp);
	  }
	  break;
	case ODB_AGGR_AVG_DISTINCT:
	case ODB_AGGR_SUM_DISTINCT:
	case ODB_AGGR_NORM_DISTINCT:
	case ODB_AGGR_DOTP_DISTINCT:
	  {
	    int iNd = 0;
	    value = 0;
	    for (j=0; j<ncard; j++) {
	      double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	      if (ABS(dd) != mdi) {
		value += dd;
		++iNd;
	      }
	    }
	    if (iNd > 0) {
	      if (flag == ODB_AGGR_AVG_DISTINCT) value /= iNd;
	      else if (flag == ODB_AGGR_NORM_DISTINCT) value = (value > 0) ? sqrt(value) : (double)0;
	    }
	    else
	      value = RMDI;
	  }
	  break;
	case ODB_AGGR_STDEV_DISTINCT:
	case ODB_AGGR_VAR_DISTINCT:
	  {
	    int iNd = 0;
	    double avg = 0;
	    for (j=0; j<ncard; j++) {
	      double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	      if (ABS(dd) != mdi) {
		avg += dd;
		++iNd;
	      }
	    }
	    if (iNd > 0) avg /= iNd;
	    value = 0;
	    for (j=0; j<ncard; j++) {
	      double dd = d[unique_idx[j]-1]; /* -1 since unique_idx[] is Fortran-index >= 1 */
	      if (ABS(dd) != mdi) {
		dd -= avg;
		value += dd * dd;
	      }
	    }
	    if (iNd > 1) {
	      value = value/(iNd-1);
	      if (flag == ODB_AGGR_STDEV_DISTINCT) value = sqrt(value);
	    }
	    else if (iNd == 1)
	      value = 0;
	    else
	      value = RMDI;
	  }
	  break;
	}
	FREE(unique_idx);
      }
      else {
	/* Just {COUNT|SUM|AVG|STDEV|RMS|MIN|MAX|DOTP|NORM|VAR}(arg) */
	switch (flag) {
	case ODB_AGGR_COUNT:
	  value = Nd;
	  break;
	case ODB_AGGR_BCOUNT:
	  {
	    int ntrue = 0;
	    for (j=0; j<Nd; j++) {
	      if (d[j] != 0) ntrue++;
	    }
	    value = ntrue;
	  }
	  break;
	case ODB_AGGR_RMS:
	  {
	    int iNd = 0;
	    value = 0;
	    for (j=0; j<Nd; j++) {
	      if (ABS(d[j]) != mdi) {
		value += d[j] * d[j];
		++iNd;
	      }
	    }
	    if (iNd > 0) 
	      value = (value > 0) ? sqrt(value/iNd) : (double)0;
	    else
	      value = RMDI;
	  }
	  break;
	case ODB_AGGR_MEDIAN:
	  value = RMDI;
	  if (Nd > 0) {
	    int iNd = 0;
	    double *dtmp = NULL;
	    ALLOC(dtmp, Nd);
	    for (j=0; j<Nd; j++) {
	      if (ABS(d[j]) != mdi) {
		dtmp[iNd++] = d[j];
	      }
	    }
	    if (iNd > 0) {
	      qsort(dtmp, iNd, sizeof(*dtmp), cmp);
	      value = MEDIAN(dtmp, iNd); /* median */
	    }
	    FREE(dtmp);
	  }
	  break;
	case ODB_AGGR_AVG:
	case ODB_AGGR_SUM:
	case ODB_AGGR_DENSITY:
	case ODB_AGGR_DOTP:
	case ODB_AGGR_NORM:
	  {
	    int iNd = 0;
	    value = 0;
	    for (j=0; j<Nd; j++) {
	      if (ABS(d[j]) != mdi) {
		value += d[j];
		++iNd;
	      }
	    }
	    if (iNd > 0) {
	      if (flag == ODB_AGGR_AVG) value /= iNd;
	      else if (flag == ODB_AGGR_NORM) value = (value > 0) ? sqrt(value) : (double)0;
	      else if (flag == ODB_AGGR_DENSITY) {
		double resol = value/iNd; /* Average resolution, in degrees */
		/* Approximate area of a small spherical eq_region [in degrees^2] */
		double area  = resol * resol;
		value = iNd / area; /* Density = # of points divided by the approx. area  */
	      }
	    }
	    else
	      value = RMDI;
	  }
	  break;
	case ODB_AGGR_STDEV:
	case ODB_AGGR_VAR:
	  {
	    int iNd = 0;
	    double avg = 0;
	    for (j=0; j<Nd; j++) {
	      if (ABS(d[j]) != mdi) {
		avg += d[j];
		++iNd;
	      }
	    }
	    if (iNd > 0) avg /= iNd;
	    value = 0;
	    for (j=0; j<Nd; j++) {
	      double dd = d[j];
	      if (ABS(d[j]) != mdi) {
		dd -= avg;
		value += dd * dd;
	      }
	    }
	    if (iNd > 1) {
	      value = value/(iNd-1);
	      if (flag == ODB_AGGR_STDEV) value = sqrt(value);
	    }
	    else if (iNd == 0)
	      value = 0;
	    else
	      value = RMDI;
	  }
	  break;
	case ODB_AGGR_MINLOC:
	case ODB_AGGR_MAXLOC:
	  if (daux && Nd > 0) {
	    int iNd = 0;
	    int jj = 0;
	    /* In case the first one is a missing data indicator */
	    while (jj<Nd && ABS(d[jj]) == mdi) jj++;
	    if (jj<Nd) {
	      j = jj;
	      value_aux = d[j];
	      value = daux[j];
	      ++iNd;
	      if (flag == ODB_AGGR_MAXLOC) {
		for (j=jj+1; j<Nd; j++) {
		  if (ABS(d[j]) != mdi) {
		    if (value_aux > d[j]) {
		      value_aux = d[j];
		      value = daux[j];
		    }
		    ++iNd;
		  }
		}
	      }
	      else { /* ODB_AGGR_MINLOC */
		for (j=jj+1; j<Nd; j++) {
		  if (ABS(d[j]) != mdi) {
		    if (value_aux < d[j]) {
		      value_aux = d[j];
		      value = daux[j];
		    }
		    ++iNd;
		  }
		}
	      }
	    } /* if (jj<Nd) */
	    if (iNd == 0) {
	      value_aux = RMDI;
	      value = RMDI;
	    }
	  }
	  break;
	case ODB_AGGR_MIN:
	case ODB_AGGR_MAX:
	  {
	    value = RMDI;
	    if (Nd > 0) {
	      int jj = 0;
	      /* In case the first one is a missing data indicator */
	      while (jj<Nd && ABS(d[jj]) == mdi) jj++;
	      if (jj<Nd) {
		j = jj;
		value = d[j];
		if (flag == ODB_AGGR_MIN) {
		  for (j=jj+1; j<Nd; j++) {
		    if (ABS(d[j]) != mdi && value > d[j]) value = d[j];
		  }
		}
		else { /* ODB_AGGR_MAX */
		  for (j=jj+1; j<Nd; j++) {
		    if (ABS(d[j]) != mdi && value < d[j]) value = d[j];
		  }
		}
	      } /* if (jj<Nd) */
	    }
	  }
	  break;
	case ODB_AGGR_LINREGR_A:
	case ODB_AGGR_LINREGR_B:
	case ODB_AGGR_CORR:
	case ODB_AGGR_COVAR:
	  /* y = A + B * x */
	  value = RMDI;
	  if (daux && Nd > 0) {
	    int iNd = 0;
	    double xavg = 0, yavg = 0; /* x := d[], y := daux[] */
	    double top = 0, bot = 0;
	    for (j=0; j<Nd; j++) {
	      if (ABS(d[j]) != mdi && ABS(daux[j]) != mdi) {
		xavg += d[j];
		yavg += daux[j];
		++iNd;
	      }
	    }
	    if (iNd > 0) {
	      xavg /= iNd;
	      yavg /= iNd;
	      for (j=0; j<Nd; j++) {
		if (ABS(d[j]) != mdi && ABS(daux[j]) != mdi) {
		  double dd = d[j] - xavg;
		  top += (dd - xavg) * (daux[j] - yavg);
		  bot += dd * dd;
		}
	      }
	      if (bot == 0) bot = 1e-30;
	      switch (flag) {
	      case ODB_AGGR_LINREGR_A:
		value_aux = top/bot; /* B */
		value = yavg - value_aux * xavg; /* A */
		break;
	      case ODB_AGGR_LINREGR_B:
		value = top/bot; /* B */
		value_aux = yavg - value * xavg; /* A */
		break;
	      case ODB_AGGR_CORR:
		{
		  double sumx2 = 0, sumy2 = 0;
		  double sumxy = 0;
		  double sxy, sxx, syy;
		  for (j=0; j<Nd; j++) {
		    if (ABS(d[j]) != mdi && ABS(daux[j]) != mdi) {
		      sumxy += d[j] * daux[j];
		      sumx2 += d[j] * d[j];
		      sumy2 += daux[j] * daux[j];
		    }
		  }
		  sxy = sumxy - iNd * xavg * yavg;
		  sxx = sumx2 - iNd * xavg * xavg;
		  syy = sumy2 - iNd * yavg * yavg;
		  value = (sxx != 0 && syy != 0) ? (sxy * sxy)/(sxx * syy) : (double)0;
		  value_aux = value;
		  value = (value > 0) ? sqrt(value) : (double)0;
		}
		break;
	      case ODB_AGGR_COVAR:
		{
		  double sumxy = 0;
		  double sxy;
		  for (j=0; j<Nd; j++) {
		    if (ABS(d[j]) != mdi && ABS(daux[j]) != mdi) {
		      sumxy += d[j] * daux[j];
		    }
		  }
		  sxy = sumxy - iNd * xavg * yavg;
		  value_aux = sxy;
		  value = sxy / iNd;
		}
		break;
	      }
	    }
	    else /* if (iNd > 0) ... */
	      value = RMDI;
	  }
	  break;
	}
      }
    }
    else if (Nd == 1 && flag == ODB_AGGR_NONE) {
      value = d[0];
    }
    rc[0] = value;
    if (daux) rc[1] = value_aux;
  }
  DRHOOK_END(0);
}


PUBLIC void
codb_calc_aggr_(const int    *phase_id,
		const int    *aggr_func_flag,
		int          *nrc,
		double        rc[],
		const int    *nd, 
		const double  d[],
		const double  daux[])
{
  DRHOOK_START(codb_calc_aggr_);
  if (phase_id && aggr_func_flag && d && nd && nrc && rc) {
    int Phase_id = *phase_id;
    int Nrc = *nrc;
    /* Note: Aggregate functions 
       "covar", "corr", "linregr_a", "linregr_b",
       are expected to have Nrc == 2 && daux[] specified */

    if (Phase_id == 0) {
      /* This means: pf->tmp has NOT yet been created i.e. using values from database */
      CalcAggr0(aggr_func_flag, d, (Nrc==2 && daux) ? daux : NULL, nd, rc);
    }
    else if (Phase_id == 1) {
      /* This means: pf->tmp is available; accumulate results */
      /* daux[] is irrelevant */
      CalcAggr1(aggr_func_flag, d, nd, rc);
    }
    else { /* Process all in one go: Supposed to be called at least, when ODBMP_nproc > 1 */
      CalcAggrAll(aggr_func_flag, d, (Nrc==2 && daux) ? daux : NULL, nd, rc);
    }
  }
  DRHOOK_END(0);
}
