#ifndef _NODE_H_
#define _NODE_H_

/* node.h */

#include "evaluate.h"

typedef struct _Node_t {
  Kind_t kind;
  char *name;
  double value;
  int numargs;
  const void *funcptr; /* = const funcs_t * from funcs.c */
  double *symval;      /* = symtab_t's ->value from symtab.c */
  struct _Node_t **args;
  struct _Node_t *next;
} Node_t;

#endif
