#ifndef _SYMTAB_H_
#define _SYMTAB_H_

/* symtab.h */

typedef struct _symtab_t {
  /* Binary tree */
  char *name;
  Bool active;
  double value;
  double *vec;
  int nvec;
  struct _symtab_t *left;  /* strcmp() > 0 */
  struct _symtab_t *right; /* strcmp() < 0 */
} symtab_t;

extern void initsym();
extern double *putsym(const char *s, double value);
extern double *putsymvec(const char *s, double value, double *vec, int nvec);
extern double  getsym(const char *s, Bool *on_error);
extern double *getsymaddr(const char *s);
extern double *getsymvec(const char *s, int *nvec);
extern void printsym(void *fp, int it);
extern double delsym(const char *s, int it);
extern void delallsym(const int *specific_it);
extern symtab_t *getsymtab(const char *s);

#endif /* _SYMTAB_H_ */
