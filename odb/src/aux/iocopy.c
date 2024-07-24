#include "iostuff.h"

#ifdef VPP
#pragma global noalias
#pragma global novrec
#elif defined(NECSX)
#pragma cdir options -pvctl,nodep
#endif

void
IOrcopy(real8 out[], const real8 in[], int len)
{
  int j;
  /* Vectorizable */
  for (j=0; j<len; j++) out[j] = in[j];
}

void
IOircopy(integer4 out[], const real8 in[], int len)
{
  int j;
  /* Vectorizable */
  for (j=0; j<len; j++) out[j] = in[j];
}

void
IOricopy(real8 out[], const integer4 in[], int len)
{
  int j;
  /* Vectorizable */
  for (j=0; j<len; j++) out[j] = in[j];
}

void
IOrucopy(real8 out[], const unsigned int in[], int len)
{
  int j;
  /* Vectorizable */
  for (j=0; j<len; j++) out[j] = in[j];
}

void
IOicopy(integer4 out[], const integer4 in[], int len)
{
  int j;
  /* Vectorizable */
  for (j=0; j<len; j++) out[j] = in[j];
}
