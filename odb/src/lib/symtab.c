
/* symtab.c */

#include "alloc.h"
#include "symtab.h"
#include "evaluate.h"
#include "info.h"

static symtab_t **begin_sym = NULL;

PUBLIC void
initsym()
{
  if (!begin_sym) {
    DEF_INUMT;
    CALLOC(begin_sym, inumt);
  }
}


PUBLIC double *
putsymvec(const char *s, double value, double *vec, int nvec)
{
  double *addr = NULL;
  if (s) {
    DEF_IT;
    symtab_t *p = begin_sym[IT];
    while (p) {
      int cmp = strcmp(p->name,s);
      if (cmp == 0) break; /* A match */
      p = (cmp < 0) ? p->left : p->right;
    }
    if (!p) {
      CALLOC(p,1);
      p->name = STRDUP(s);
      if (begin_sym[IT]) {
	symtab_t *top = begin_sym[IT];
	while (top) {
	  int cmp = strcmp(top->name,p->name);
	  symtab_t *next = NULL;
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
	begin_sym[IT] = p;
      }
    }
    p->value = value;
    p->active = true;
    p->vec = vec;
    p->nvec = nvec;
    addr = &p->value;
  }
  return addr;
}

PUBLIC double *
putsym(const char *s, double value)
{
  return putsymvec(s, value, NULL, 0);
}

PRIVATE void
print1sym(void *fp, const symtab_t *p, int it)
{
  if (fp && p) {
    const thsafe_parse_t *thsp = GetTHSP();
    const char *s = p->name;
    double value = p->value;
    FprintF(fp,"(it#%d)@%p>> %s = %.12g (active ? %s) : left=%p, right=%p\n",
	    it,p,
	    s,value,p->active ? "yes" : "no",
	    p->left, p->right);
    print1sym(fp,p->left,it);
    print1sym(fp,p->right,it);
  }
}

PUBLIC void
printsym(void *fp, int it)
{
  if (fp) {
    const thsafe_parse_t *thsp = GetTHSP();
    const symtab_t *p = begin_sym[IT];
    FprintF(fp,"---------------------\n");
    print1sym(fp, p, it);
  }
}

PUBLIC double
getsym(const char *s, Bool *on_error)
{
  double value = mdi;
  if (s) {
    DEF_IT;
    symtab_t *p = begin_sym[IT];
    while (p) {
      int cmp = strcmp(p->name,s);
      if (cmp == 0) { /* A match */
	value = p->value;
	if (!p->active) p = NULL;
	break;
      }
      p = (cmp < 0) ? p->left : p->right;
    }
    if (on_error) {
      if (!p) {
	/* "Variable not initialized/defined/active" */
	*on_error = true;
      }
      else {
	*on_error = false;
      }
    }
  }
  else {
    if (on_error) *on_error = true;
  }
  return value;
}

PUBLIC double *
getsymaddr(const char *s)
{
  double *addr = NULL;
  if (s) {
    DEF_IT;
    symtab_t *p = begin_sym[IT];
    while (p) {
      int cmp = strcmp(p->name,s);
      if (cmp == 0) { /* A match */
	if (!p->active) p = NULL;
	break;
      }
      p = (cmp < 0) ? p->left : p->right;
    }
    addr = p ? &p->value : NULL;
  }
  return addr;
}

PUBLIC double *
getsymvec(const char *s, int *nvec)
{
  if (s) {
    DEF_IT;
    symtab_t *p = begin_sym[IT];
    while (p) {
      int cmp = strcmp(p->name,s);
      if (cmp == 0) { /* A match */
	if (!p->active) p = NULL;
	break;
      }
      p = (cmp < 0) ? p->left : p->right;
    }
    if (nvec && p) *nvec = p->nvec;
    else if (nvec) *nvec = 0;
    return p ? p->vec : NULL;
  }
  else {
    if (nvec) *nvec = 0;
    return NULL;
  }
}

PUBLIC double
delsym(const char *s, int it)
{
  double value = mdi;
  if (s) {
    symtab_t *p = begin_sym[IT];
    while (p) {
      int cmp = strcmp(p->name,s);
      if (cmp == 0) { /* A match */
	if (*s != '$') {
	  /* De-activate all but $-variables */
	  /* Please note that only delentry() [called by delallsym()]
	     de-activates ALL symbols regardless of their kind */
	  p->active = false;
	}
	value = p->value; /* Its latest value */
	break;
      }
      p = (cmp < 0) ? p->left : p->right;
    }
  }
  return value;
}

PRIVATE void
delentry(symtab_t *p)
{
  while (p) {
    p->active = false;
    delentry(p->left);
    p = p->right;
  }
}


PUBLIC void
delallsym(const int *specific_it)
{
  if (begin_sym) {
    DEF_INUMT;
    int it;
    int it_start = 1, it_end = inumt;
    if (specific_it) {
      int sit = *specific_it;
      if (sit < 1 || sit > inumt) return;
      it_start = it_end = sit;
    }
    for (it=it_start; it<=it_end; it++) {
      symtab_t *p = begin_sym[IT];
      delentry(p);
    }
  } /* if (begin_sym) */
}


PUBLIC symtab_t *
getsymtab(const char *s)
{
  symtab_t *stab = NULL;
  if (s) {
    DEF_IT;
    symtab_t *p = begin_sym[IT];
    while (p) {
      int cmp = strcmp(p->name,s);
      if (cmp == 0) { /* A match */
	if (!p->active) p = NULL;
	break;
      }
      p = (cmp < 0) ? p->left : p->right;
    }
    stab = p;
  }
  return stab;
}
