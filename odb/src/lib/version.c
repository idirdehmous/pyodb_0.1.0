
/* version.c */

/* Keeps track of ODB-version */

#include "alloc.h"
#include "magicwords.h"
#include <ctype.h>

#define VERSION_MAJOR 43     /* A positive integer; Number(s) after possible decimal point are ignored */
#define VERSION_MINOR 0.000  /* Four digits precision/three decimal points (e.q. 2.068) here, please */
			     /* Please make also sure the last digit is between 1 and 9 i.e. not 0 */

const char *
codb_versions_(double *major, double *minor, int *numeric, const int *check_IFS_CYCLE_env_first)
{
  const double Major = (int)VERSION_MAJOR;
  const double Minor = VERSION_MINOR;
  const int Numeric = ((int)VERSION_MAJOR) * 10000 + (int)(VERSION_MINOR * 1000);

  static double ifs_cycle_Major = 0;
  static double ifs_cycle_Minor = 0;
  static int ifs_cycle_num = 0;

  static char *ifs_cycle_str = NULL;
  static char *cycle_str = NULL;

  if (!cycle_str) { /* first time */
    ALLOC(cycle_str, 20);
    snprintf(cycle_str, 20, "CY%.0fR%.3f", Major, Minor);

    ifs_cycle_str = getenv("IFS_CYCLE");
    if (ifs_cycle_str) {
      char *s = ifs_cycle_str;
      int slen = STRLEN(s);
      char *last = s + slen;
      int numdigits = 0;

      while (s < last) { /* pick up all the digits */
	if (isdigit(*s)) {
	  ++numdigits;
	  ifs_cycle_num = 10*ifs_cycle_num + (*s - '0');
	}
	++s;
      } /* while (s < last) */

      switch (numdigits) {
      case 2:
	ifs_cycle_num = 10000 * ifs_cycle_num + 1; /* e.g. CY34 --> 340001 */
	break;
      case 3:
	ifs_cycle_num = 1000 * ifs_cycle_num + 1; /* e.g. CY33R1 --> 331001 */
	break;
      case 4:
	ifs_cycle_num *= 100; /* e.g. CY33R1.4 --> 331400 */
	break;
      case 5:
	ifs_cycle_num *= 10; /* e.g. CY33R1.45 --> 331450 */
	break;
      case 6:
	ifs_cycle_num *= 1; /* e.g. CY33R1.456 --> 331456 */
	break;
      default: /* Anything else --> use the value of the Numeric */
	ifs_cycle_num = Numeric;
	break;
      } /* switch (numdigits) */
    }
    else {
      ifs_cycle_num = Numeric;
    }

    if (ifs_cycle_num != Numeric) {
      ifs_cycle_Major = (int)(ifs_cycle_num/10000);
      ifs_cycle_Minor = (ifs_cycle_num%10000)/(double)1000;
    }
    else {
      ifs_cycle_Major = Major;
      ifs_cycle_Minor = Minor;
    }
  } /* if (!cycle_str) */

  if (check_IFS_CYCLE_env_first && *check_IFS_CYCLE_env_first != 0) {
    if (major) *major = ifs_cycle_Major;
    if (minor) *minor = ifs_cycle_Minor;
    if (numeric) *numeric = ifs_cycle_num;
  }
  else {
    if (major) *major = Major;
    if (minor) *minor = Minor;
    if (numeric) *numeric = Numeric;
  }

  return (const char *)cycle_str;
}
