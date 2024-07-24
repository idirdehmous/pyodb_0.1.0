
/* === CMA-packing method#255 === */

/* This method does not exist */

#include "pcma.h"

#include <signal.h>

int
pcma_255_driver(int method, /* ignored */
		FILE *fp_out,
		const double  cma[],
		int  lencma,
		double nmdi,
		double rmdi,
		Packbuf *pbuf)
{
  DRHOOK_START(pcma_255_driver);
  fprintf(stderr,
	  "pcma_255_driver: This packing method#%d (%d) has not been implemented\n", 
	  255, method);
  RAISE(SIGABRT);
  DRHOOK_END(0);
  return 0;
}


int
upcma_255_driver(int method,
		 int swp, int can_swp_data,
		 int new_version,
		 const unsigned int packed_data[],
		 int len_packed_data,
		 int msgbytes,
		 double nmdi,
		 double rmdi,
		 FILE *fp_out, 
		 double cma[], int lencma)
{
  DRHOOK_START(upcma_255_driver);
  fprintf(stderr,
	  "upcma_255_driver: This unpacking method#%d (%d) has not been implemented\n", 
	  255, method);
  RAISE(SIGABRT);
  DRHOOK_END(0);
  return 0;
}
