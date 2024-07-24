/* random_odb.c */

#include "odb.h"

#define ODB_RAND_MAX (32767.0)

static unsigned int *seed = NULL;

PRIVATE double
myrand(unsigned int *next) {
    /* Returns a random number between [0 .. 1] double floating point */
    (*next) = (*next) * 1103515245 + 12345;
    return ( ((*next)/65536U) % 32768U )/ODB_RAND_MAX;
}


PUBLIC void
init_RANDOM()
{
  if (!seed) {
    DEF_INUMT;
    int j;
    ALLOC(seed, inumt);
    for (j=0; j<inumt; j++) seed[j] = 1;
  }
}


PUBLIC double
ODB_seed(double myseed)
{
  double oldseed;
  int is_parallel_region = 0;
  coml_in_parallel_(&is_parallel_region);
  if (is_parallel_region) {
    DEF_IT;
    oldseed = seed[--it];
    seed[it] = myseed;
  }
  else {
    DEF_INUMT;
    int j;
    if (!seed) init_RANDOM(); /* Normally not called */
    oldseed = seed[0];
    for (j=0; j<inumt; j++) seed[j] = myseed;
  }
  return oldseed;
}


PUBLIC double
ODB_random()
{
  DEF_IT;
  if (!seed) init_RANDOM(); /* Normally not called */
  return myrand(&seed[--it]);
}
