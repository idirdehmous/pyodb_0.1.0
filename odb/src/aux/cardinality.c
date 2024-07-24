/* cardinality.c */

#include "odb.h"
#include "idx.h"

typedef struct _card_t {
  /* Binary tree */
  double *key; /* keys to compare */
  struct _card_t *left;  /* memcmp() > 0 */
  struct _card_t *right; /* memcmp() < 0 */
  Bool alloc_here;
} card_t;


static card_t **begin_card = NULL;


PUBLIC void
InitCard()
{
  if (!begin_card) {
    DEF_INUMT;
    CALLOC(begin_card, inumt);
  }
}


PRIVATE card_t *
GetCard(const double key[], int keylen)
{
  DEF_IT;
  card_t *p = NULL;
  if (key && keylen > 0) {
    keylen *= sizeof(*key); /* Now in bytes */
    p = begin_card[IT];
    while (p) {
      int cmp = memcmp(p->key,key,keylen);
      if (cmp == 0) { /* A match */
	break;
      }
      p = (cmp < 0) ? p->left : p->right;
    }
  }
  return p;
}


PRIVATE card_t *
PutCard(const double key[], int keylen, double *mem, 
	Bool alloc_here, Bool *added)
{
  DEF_IT;
  card_t *p = GetCard(key, keylen);
  if (added) *added = false;
  if (keylen > 0 && !p) {
    int j;
    CALLOC(p,1);
    if (!mem) {
      ALLOC(p->key, keylen);
      p->alloc_here = true;
    }
    else {
      p->key = mem; /* to avoid memory fragmentation */
      p->alloc_here = alloc_here;
    }
    for (j=0; j<keylen; j++) p->key[j] = key[j];
    if (begin_card[IT]) {
      card_t *top = begin_card[IT];
      keylen *= sizeof(*key); /* Now in bytes */
      while (top) {
	int cmp = memcmp(top->key,p->key,keylen);
        card_t *next = NULL;
        if (cmp < 0) {
          if (!top->left) { top->left = p; } else { next = top->left; }
        }
        else {
          if (!top->right) { top->right = p; } else { next = top->right; }
        }
        top = next; /* If next == NULL => new position in tree found => get out */
      }
    }
    else {
      begin_card[IT] = p;
    }
    if (added) *added = true;
  }
  return p;
}


PRIVATE int
DelCard(card_t *p)
{
  int rc = 0;
  if (p) {
    if (p->left) rc += DelCard(p->left);
    if (p->right) rc += DelCard(p->right);
    if (p->alloc_here) FREE(p->key);
    FREE(p);
    rc++;
  }
  return rc;
}


PRIVATE int
DelAllCard()
{
  int rc = 0;
  if (begin_card) {
    DEF_IT;
    card_t *p = begin_card[IT];
    rc += DelCard(p);
    begin_card[IT] = NULL;
  }
  return rc;
}


PUBLIC void
codb_cardinality_(const int *ncols, const int *nrows, const int *lda,
		  const double a[], /* A Fortran matrix A(LDA,1:NCOLS), LDA >= NROWS */
		  int *rc,          /* cardinality i.e. number of distinct values <= NROWS */
		  int unique_idx[], /* if not NULL, then Fortran-indices to unique rows of A upon output */
		  const int *nunique_idx, /* Number of elements allocated for unique_idx[] */
		  const int *bailout /* Bailout from cardinality study after this many distinct entries found */
		  )
{
  /* 
     Calculates no. of distinct values on a column or set of columns;
     Essentially gives the number of rows for 'SELECT DISTINCT col1,col2,..,colN' 
     Also returns (Fortran) indices to the unique row positions
  */

  int nunique = nunique_idx ? *nunique_idx : 0;
  int sign = 1;
  int klen, kldim, kvals, kvals8;
  int i, j, keff;
  double *pd;
  double *mem = NULL, *pmem;
  int nmem;
  int Bailout = bailout ? *bailout : 0;
  Bool Bailout_reached = false;
  Bool onekey; /* true if a very common case of one (key) column only */
  Bool alloc_here = false;
  card_t *plast = NULL;

  klen = nrows ? *nrows : 0;
  kldim = lda ? *lda : klen;
  kvals = ncols ? *ncols : 1;
  kvals8 = kvals * sizeof(*pd);
  
  onekey = (kvals == 1) ? true : false;

  if (Bailout <= 0) Bailout = klen + 1;

  if (!onekey) {
    ALLOC(pd, kvals);
  }
  else {
    pd = (double *)a; /* a[] is not overwritten, though */
  }

  if (!begin_card) InitCard(); /* Normally never called here */

  nmem = ((klen+9)/10)*kvals; /* Assume initially that 1:10th of total rows maybe distinct keys */
  if (nmem > 0) {
    ALLOC(mem, nmem);
    alloc_here = true;
  }
  pmem = mem;

  keff = 0;
  for (i=0; i<klen; i++) {
    Bool is_unique = false;
    if (!onekey) { /* More than one key */
      for (j=0; j<kvals; j++) pd[j] = a[kldim*j + i];
    }

    if (plast && memcmp(plast->key,pd,kvals8) == 0) {
      /* 
	 The entries "pd" are the same as (one of) the previously tried entries 
	 i.e. cannot be unique
      */
      is_unique = false;
    }
    else {
      if (pmem) {
	if (pmem >= mem + nmem) {
	  /* Need more memory for keys in a less fragmented manner.
	     Please note that previously allocated "mem" will be freed either in
	     DelAllCard() or via FREE(mem) after the for-loop "i" */
	  nmem = ((klen-i+3)/4)*kvals; /* Assume that 1:4th of total rows still available maybe distinct keys */
	  ALLOC(mem, nmem);
	  pmem = mem;
	  alloc_here = true;
	}
	else {
	  alloc_here = false;
	}
      } /* if (pmem) */
      /* 
	 The "is_unique" will be set to "true" if a new entry will be added to the cardinality binary tree.
	 The "plast" remembers the latest "found" position. 
	 The use of "plast" may give some speedup in the next round of "i".
      */
      plast = PutCard(pd, kvals, pmem, alloc_here, &is_unique);
      if (is_unique && pmem) pmem += kvals;
    }

    if (is_unique) {
      if (nunique > 0 && unique_idx && keff < nunique) unique_idx[keff] = i+1; /* "+1" due to Fortran-indexing */
      if (sign == 1 && nunique > 0 && unique_idx && keff >= nunique) sign = -1; /* Not enough space in unique_idx */
      keff++;
      if (keff >= Bailout) {
	/* Too many distinct values ... bailing out */
	Bailout_reached = true;
	keff = 0; /* bailed out from (failed) cardinality study */
	break;
      }
    } /* if (is_unique) */

    if (onekey) pd++;
  } /* for (i=0; i<klen; i++) */
  if (rc) *rc = sign * keff; /* ABS(*rc) is the space required for unique_idx[] */

  DelAllCard(); 
  /* Please note that DelAllCard() took care of deallocation of "mem"'s, 
     unless the condition below is true */
  if (pmem && pmem == mem) FREE(mem);

  if (!onekey) FREE(pd);
}

#ifdef STANDALONE_TEST
int main()
{
  int rc = 0;
  double *d = NULL;
  FILE *fp = stdin;
  int n = 0;
  if (fscanf(fp,"%d", &n) == 1) {
    int j;
    ALLOC(d,n);
    for (j=0; j<n; j++) {
      if (fscanf(fp,"%lf", &d[j]) != 1) {
	rc = -(j+1);
	break;
      }
    }
  }
  else {
    rc = 1;
  }
  if (rc == 0 && n > 0) {
    int ncard = 0;
    int ncols = 1;
    int nrows = n;
    int lda = n;
    int *idx = NULL;
    CALLOC(idx, n);
    codb_cardinality_(&ncols, &nrows, &lda,
		      d, &ncard, idx, &n, NULL);
    printf("n = %d, ncard = %d\n",n, ncard);
    FREE(idx);
  }
  FREE(d);
  return rc;
}
#endif
