/*

  Calculate or Check MD5 Signature of File or Command Line Argument

			    by John Walker
		       http://www.fourmilab.ch/

		This program is in the public domain.

		Adapted for ODB-environment by Sami Saarinen, ECMWF

*/

#include "odbmd5.h"

/*  Main program  */

int main(int argc, char *argv[])
{
  char *env = getenv("ODB_MD5SUM_DEBUG");
  if (env) {
    int what = atoi(env);
    if (what == 1) {
      unsigned char sign[16];
      int rc = MD5_signature(argc, argv, sign);
      if (rc == 0) {
	char *s = MD5_sign2hex(sign, 0);
	printf("%s\n",s);
	FREE(s);
      }
      return rc;
    }
    else if (what == 2 && argc == 2) {
      unsigned char sign[16];
      int rc = MD5_str2sign(argv[1], sign);
      if (rc == 0) {
	char *s = MD5_sign2hex(sign, 0);
	printf("%s\n",s);
	FREE(s);
      }
      return rc;
    }
  }
  return MD5_signature(argc, argv, NULL);
}
