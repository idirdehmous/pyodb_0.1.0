#include "odbcs.h"

#ifdef ODBCS

/* 
   Returns IP-address (ipv4) for the given host_name.
   If not found, then exit code (rc) is non-zero.

   Usage: odbi_host.x host_name 

   Author: Sami Saarinen, ECMWF, 20-Dec-2007
*/

int
main(int argc, char *argv[])
{
  int rc = 2; /* by default all NOT ok */
  if (--argc >= 1) {
    char *hostout = NULL;
    struct hostent *h = gethostbyname(argv[1]);
    if (h && h->h_addrtype == AF_INET) {
      char **pptr;
      char str[ODBCS_INET_ADDRSTRLEN];
      const char *p = NULL;
      pptr = h->h_addr_list;
      for ( ; *pptr != NULL ; pptr++) {
	p = Inet_ntop(h->h_addrtype, *pptr, str, sizeof(str));
      }
      if (p) { printf("%s\n",p); rc = 0; } /* All ok */
    }
  }
  else
    rc = 1; /* arg#1 is missing */
  return rc;
}

#else

#include <stdio.h>

int
main()
{
  fprintf(stderr,"***Error: ODB client/server not implemented on this platform\n");
  return 3;
}

#endif
