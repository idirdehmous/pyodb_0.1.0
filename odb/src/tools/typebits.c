/* This program can be used to generate DATATYPE_xxx #define's
   used in ../include/privpub.h 

   Run on RS6K, big-endian machine:

   cc -DRS6K -I../include typebits.c -lm && a.out > out

   or LITTLE-endian (say Linux/Pentium):

   cc -DLINUX -DLITTLE -I../include typebits.c -lm && a.out > out

   and you should get identical results.

   Insert file "out" at the end of privpub.h, if you have added new types.

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "privpub.h"

typedef union {
  odb_types_t t;
  unsigned int ui;
} ut;

void tb(const char *name, const char *defname, 
	int signbit, int byte_swappable,
	int precision_bits, int base_type,
	int other_type)
{
  double bits = pow(2,precision_bits);
  long long int bytes = bits/8;
  ut u;
  u.ui = 0;
  u.t.signbit = signbit;
  u.t.byte_swappable = byte_swappable;
  u.t.precision_bits = precision_bits;
  u.t.base_type = base_type;
  u.t.other_type = other_type;
  {
    printf("\n/* signbit=%u, byte_swappable=%u, precision_bits=%u, base_type=%u, other_type=0x%x (%u) */\n",
	   u.t.signbit, u.t.byte_swappable, u.t.precision_bits,
	   u.t.base_type, u.t.other_type, u.t.other_type);
  }
  u.ui >>= 8; /* shift over pmethod */
  if (u.ui == 0) {
    printf("#define DATATYPE_%-20s 0x%-8x   /* (%u dec) %s */\n",
	   defname,u.ui,u.ui,name);
  }
  else if (bits > 256) {
    printf("#define DATATYPE_%-20s 0x%-8x   /* (%u dec) %s : %lld bytes */\n",
	   defname,u.ui,u.ui,name,bytes);
  }
  else {
    printf("#define DATATYPE_%-20s 0x%-8x   /* (%u dec) %s : %.10g bits, %lld bytes */\n",
	   defname,u.ui,u.ui,name,bits,bytes);
  }
}

int main()
{
  tb("undef","UNDEF",0,0,0,0x0,0x0);

  tb("bit"       ,"BIT"  ,0,0,0,0x1,0x0);

  tb("char"      ,"INT1" ,1,1,3,0x1,0x0);
  tb("short"     ,"INT2" ,1,1,4,0x1,0x0);
  tb("int"       ,"INT4" ,1,1,5,0x1,0x0);
  tb("long long" ,"INT8" ,1,1,6,0x1,0x0);

  tb("uchar"     ,"UINT1",0,1,3,0x1,0x0);
  tb("ushort"    ,"UINT2",0,1,4,0x1,0x0);
  tb("uint"      ,"UINT4",0,1,5,0x1,0x0);
  tb("ulonglong" ,"UINT8",0,1,6,0x1,0x0);

  tb("float"      ,"REAL4" ,0,1,5,0x2,0x0);
  tb("double"     ,"REAL8" ,0,1,6,0x2,0x0);
  tb("long double","REAL16",0,1,7,0x2,0x0);

  tb("complex4"   ,"CMPLX4" ,0,1,6,0x3,0x0);
  tb("complex8"   ,"CMPLX8" ,0,1,7,0x3,0x0);
  tb("complex16"  ,"CMPLX16",0,1,8,0x3,0x0);

  tb("Bitfield"   ,"BITFIELD",0,1,5,0x0,0x1);
  tb("string"     ,"STRING"  ,0,0,6,0x0,0x2);
  tb("yyyymmdd"   ,"YYYYMMDD",0,1,5,0x0,0x4);
  tb("hhmmss"     ,"HHMMSS"  ,0,1,5,0x0,0x8);

  tb("link_offset","LINKOFFSET",1,1,5,0x0,0x10);
  tb("link_length","LINKLEN"   ,1,1,5,0x0,0x20);

  tb("bufr"       ,"BUFR"    ,0,1,5,0x0,0x40);
  tb("grib"       ,"GRIB"    ,0,1,5,0x0,0x80);

  tb("blob64kB"   ,"BLOB"    ,0,0,19,0x0,0x100);
  tb("blob2GB"    ,"LONGBLOB",0,0,34,0x0,0x200);

  tb("char(1:255)"   ,"CHAR"    ,0,0,11,0x0,0x400);
  tb("varchar(1:255)","VARCHAR" ,0,0,11,0x0,0x800);

  /*
  tb("","" ,0,,,0x0,0x);
  */

  return 0;
}
