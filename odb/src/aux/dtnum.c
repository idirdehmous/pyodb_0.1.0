#include "alloc.h"

//#include "codb_netcdf.h"

//#ifdef HAS_NETCDF /* NETCDF is available */
//#include "netcdf.h"
//#endif

/*void
dtnum_to_netcdf_type_(const uint *Dtnum, int *retcode)
{
  int rc = -1;
#ifdef HAS_NETCDF 
  if (Dtnum && retcode) {
    uint dtnum = *Dtnum;
    if      (dtnum == DATATYPE_REAL8)      rc = NC_DOUBLE;
    else if (dtnum == DATATYPE_INT4)       rc = NC_INT;
    else if (dtnum == DATATYPE_STRING)     rc = NC_CHAR;
    else if (dtnum == DATATYPE_YYYYMMDD)   rc = NC_INT;
    else if (dtnum == DATATYPE_HHMMSS)     rc = NC_INT;
    else if (dtnum == DATATYPE_BITFIELD)   rc = NC_INT;
    else if (dtnum == DATATYPE_LINKOFFSET) rc = NC_INT;
    else if (dtnum == DATATYPE_LINKLEN)    rc = NC_INT;
    else if (dtnum == DATATYPE_BUFR)       rc = NC_INT;
    else if (dtnum == DATATYPE_GRIB)       rc = NC_INT;
    else if (dtnum == DATATYPE_INT1)       rc = NC_BYTE;
    else if (dtnum == DATATYPE_INT2)       rc = NC_SHORT;
    else if (dtnum == DATATYPE_REAL4)      rc = NC_FLOAT;
    else if (dtnum == DATATYPE_UINT1)      rc = NC_BYTE;
    else if (dtnum == DATATYPE_UINT2)      rc = NC_SHORT;
    else if (dtnum == DATATYPE_UINT4)      rc = NC_INT;
    else rc = NC_EBADTYPE;
  }
#endif
  if (retcode) *retcode = rc;
}*/

uint
get_dtnum(const char *dt)
{
  if (dt) {
    int len = strlen(dt);
    if (len >= 6 && strnequ(dt,"pk",2) && strstr(dt,"int")) return DATATYPE_INT4;
    else if (len >= 7 && strnequ(dt,"pk",2) && strstr(dt,"real")) return DATATYPE_REAL8;
    else if (strequ(dt,"Formula")) return DATATYPE_REAL8;
    else if (strequ(dt,"yyyymmdd")) return DATATYPE_YYYYMMDD;
    else if (strequ(dt,"hhmmss")) return DATATYPE_HHMMSS;
    else if (strequ(dt,"Bitfield")) return DATATYPE_BITFIELD;
    else if (strequ(dt,"linkoffset_t")) return DATATYPE_LINKOFFSET;
    else if (strequ(dt,"linklen_t")) return DATATYPE_LINKLEN;
    else if (strequ(dt,"string")) return DATATYPE_STRING;
    else if (strequ(dt,"int")) return DATATYPE_INT4;
    else if (strequ(dt,"float")) return DATATYPE_REAL4;
    else if (strequ(dt,"real4")) return DATATYPE_REAL4;
    else if (strequ(dt,"double")) return DATATYPE_REAL8;
    else if (strequ(dt,"real")) return DATATYPE_REAL8;
    else if (strequ(dt,"real8")) return DATATYPE_REAL8;
    else if (strequ(dt,"bufr")) return DATATYPE_BUFR;
    else if (strequ(dt,"grib")) return DATATYPE_GRIB;
    else if (strequ(dt,"uint")) return DATATYPE_UINT4;
    else if (strequ(dt,"char")) return DATATYPE_INT1;
    else if (strequ(dt,"uchar")) return DATATYPE_UINT1;
    else if (strequ(dt,"short")) return DATATYPE_INT2;
    else if (strequ(dt,"ushort")) return DATATYPE_UINT2;
    /* else if (strequ(dt,"")) return DATATYPE_; */
  } /* if (dt) */
  return DATATYPE_UNDEF;
}

int
get_dtsize(const char *dt)
{ /* Returns data type specific length in bytes */
  if (dt) {
    int len = strlen(dt);
    if (len >= 6 && strnequ(dt,"pk",2) && strstr(dt,"int")) return sizeof(int);
    else if (len >= 7 && strnequ(dt,"pk",2) && strstr(dt,"real")) return sizeof(double);
    else if (strequ(dt,"Formula")) return sizeof(Formula);
    else if (strequ(dt,"yyyymmdd")) return sizeof(yyyymmdd);
    else if (strequ(dt,"hhmmss")) return sizeof(hhmmss);
    else if (strequ(dt,"Bitfield")) return sizeof(Bitfield);
    else if (strequ(dt,"linkoffset_t")) return sizeof(linkoffset_t);
    else if (strequ(dt,"linklen_t")) return sizeof(linklen_t);
    else if (strequ(dt,"string")) return sizeof(string);
    else if (strequ(dt,"int")) return sizeof(int);
    else if (strequ(dt,"float")) return sizeof(float);
    else if (strequ(dt,"real4")) return sizeof(real4);
    else if (strequ(dt,"double")) return sizeof(double);
    else if (strequ(dt,"real")) return sizeof(real);
    else if (strequ(dt,"real8")) return sizeof(real8);
    else if (strequ(dt,"bufr")) return sizeof(bufr);
    else if (strequ(dt,"grib")) return sizeof(grib);
    else if (strequ(dt,"uint")) return sizeof(uint);
    else if (strequ(dt,"char")) return sizeof(char);
    else if (strequ(dt,"uchar")) return sizeof(uchar);
    else if (strequ(dt,"short")) return sizeof(short);
    else if (strequ(dt,"ushort")) return sizeof(ushort);
    /* else if (strequ(dt,"")) return sizeof(...); */
  } /* if (dt) */
  return 0;
}
