#include "iostuff.h"

int
IOconcat_byname(const char *filename,
		int len_filename)
{
  int rc = 0;
  IOstuff ios;

  IOstuff_init(&ios);
  ios.logical_name = IOstrdup(filename, &len_filename);

  rc = IOgetattr(&ios);

  if (rc >= 0) {
    rc = ios.concat;
  }
  else {
    rc = 0;
  }

  { /* Clean up code */
    int j, numbins = ios.numbins;
    for (j=0; j<numbins; j++) {
      IOstuff_bin_reset(&ios.bin[j]);
    }
    IOstuff_reset(&ios);
  }

  return rc;
}
