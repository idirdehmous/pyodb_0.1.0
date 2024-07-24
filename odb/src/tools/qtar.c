
/* qtar (pronounced like Guitar) */

/* An archive & direct access utility similar to "ar" to access file(s) within a qtar-file */

/* Author: Sami Saarinen, ECMWF, 25-Nov-2002 */

#include "qtar.h"


/* 
   For example: (partially obsolete --> these need to be refreshed)

   1) Create a new qtar-file, but do not place in any members in the file:

   qtar -c file.qtar

   2) Create a new qtar-file and place two member files in the file:

   qtar -c file.qtar subdir/file_a ./file_b

   or

   qtar -c -m subdir/file_a -m ./file_b file.qtar
   

   3) Un-qtar all members with verbose

   qtar -xv -f file.qtar

   4) Print table of contents

   qtar -tv file.qtar

   5) Update existing member in a qtar-file

   qtar -u file.qtar ./file_b

   or just

   qtar file.qtar ./file_b

   6) Update/create members into the qtar-file from s;
   ** Note: byte length of each file MUST be given after colon

   echo "subdir/file_a:1200578 ./file_b:45007" | qtar -s file.qtar

   or

   qtar -s file.qtar subdir/file_a:1200578 ./file_b:45007

   7) Extract via pipe (one member at a time, though):

   echo "./file_b" | qtar -x -s file.qtar

   or

   qtar -x -s file.qtar ./file_b

   8) Using find command update qtar-file (10 files at a time)

   find . -name '*.c' -print | xargs -n10 qtar file.qtar

*/

int
main(int argc, char *argv[])
{
  return QTAR_main(argc, argv);
}
