
/* 
   Alter PCMA's missing data indicators (a new version feature):
   Say: NMDI -> 2147483647 and RMDI -> 1.7e+38 (instead of -2147483647)
   Fortran callable.
   Author: Sami Saarinen, ECMWF, 14/12/2000
 */

#include "pcma.h"

double pcma_nmdi = NMDI;
double pcma_rmdi = RMDI;

void
pcma_set_mdis_(const double *nmdi, const double *rmdi)
{
  if (nmdi) pcma_nmdi = *nmdi;
  if (rmdi) pcma_rmdi = *rmdi;
}

void
pcma_get_mdis_(double *nmdi, double *rmdi)
{
  if (nmdi) *nmdi = pcma_nmdi;
  if (rmdi) *rmdi = pcma_rmdi;
}
