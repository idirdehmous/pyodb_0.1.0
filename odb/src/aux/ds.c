
/* ds.c */

#include "odb.h"
#include "odb_macros.h"
#include "cdrhook.h"

DefineDS(void);

PUBLIC int
Read_DS(void *Pvar,
	int Handle, int PoolNo, int Nrows, int Byteswap, int IO_method,
	const char *Dbase, int fp_idx, const char *filename,
	uint datatype)
{
  int Nbytes = 0;
  uint N[2];
  DeclareDS(void, *Var);
  int packing_method;
  int savelist;
  DRHOOK_START(Read_DS);

  Var = Pvar;
  packing_method = Var->pmethod_from_datatype; /* Remember the preset value */
  Var->pmethod = (ODB_packing(NULL) == 0) ? 0 : ODB_packing(&packing_method);
  savelist = Var->savelist; /* Remember the preset value */
  FREE_alldata(*Var);
  Var->savelist = savelist; /* Restore the preset value */

  if (Nrows > 0) { /* Read nothing unless Nrows > 0 */
    uint dt;
    const int two = 2;
    Nbytes += newio_Read32(&fp_idx, filename, "No of bytes & Pmethod",
                           Var->name, Handle, PoolNo,
			   N, sizeof(*N), two);
    if (Byteswap) swap4bytes_(N, &two);
    dt = EXTRACT_DATATYPE(N[1]);
    /* if (dt > 0) datatype = dt; */
    N[1] = EXTRACT_PMETHOD(N[1]);
  }
  else { /* No data */
    N[0] = 0; 
    N[1] = Var->pmethod_from_datatype;
  }

  Var->datatype = datatype;

  if (Var->savelist == 0) {
    Var->saved_data_N[0] = N[0];
    Var->saved_data_N[1] = N[1];
  }

  if (N[0] > 0 && N[1] > 0) { /* Packed data, that is greater than zero-length */
    int nw = N[0]/sizeof(*Var->pd);
    ALLOC(Var->pd, nw);
    Var->pdlen = nw;
    Nbytes += newio_Read32(&fp_idx, filename, "Packed data",
                           Var->name, Handle, PoolNo,
			   Var->pd, sizeof(*Var->pd), Var->pdlen);
    if (Var->savelist == 0) {
      /* Create a backup for ODB_SAVELIST-purposes (keep packed; retain byte swapping in packed data) */
      char *bck;
      int bcklen = sizeof(*Var->pd) * Var->pdlen;
      ALLOC(bck,bcklen);
      memcpy(bck, Var->pd, bcklen);
      if (Var->saved_data) FREE(Var->saved_data);
      Var->saved_data = bck;
      Var->saved_data_nbytes = bcklen;
    }
    Var->is_packed = 1;
  }
  else { /* Data is in unpacked form */
    int nw = N[0]/Var->elemsize;
    ALLOC3(Var->d, nw, Var->elemsize);
    Var->nalloc = Var->dlen = nw;
    Nbytes += newio_Read32(&fp_idx, filename, "Unpacked data",
                           Var->name, Handle, PoolNo,
			   Var->d, Var->elemsize, Var->dlen);
    if (Byteswap) {
      swapANYbytes(Var->d, Var->elemsize, Var->dlen,
                   Var->datatype, Var->type, Var->name);
    }
    if (Var->savelist == 0) {
      /* Create a backup for ODB_SAVELIST-purposes (keep unpacked & *with* byte swapping)*/
      char *bck;
      int bcklen = Var->elemsize * Var->dlen;
      ALLOC(bck,bcklen);
      memcpy(bck, Var->d, bcklen); /* now byte swapped (if applicable) */
      if (Var->saved_data) FREE(Var->saved_data);
      Var->saved_data = bck;
      Var->saved_data_nbytes = bcklen;
    }
    Var->is_packed = 0;
  }

  DRHOOK_END(Nbytes);
  return Nbytes;
}

PUBLIC int
Write_DS(void *Pvar,
	 int Handle, int PoolNo, int Nrows, int Byteswap, int IO_method,
	 const char *Dbase, int fp_idx, const char *filename,
	 uint datatype,
	 Packed_DS *(*DoPackDS)(void *))
{
  int Nbytes = 0;
  DRHOOK_START(Write_DS);

  if (Nrows > 0) { /* Write nothing unless Nrows > 0 */
    Packed_DS *Packed = NULL;
    int pmethod = 0;
    uint N[2];
    DeclareDS(void, *Var);
    int packing_method;
    
    Var = Pvar;
    packing_method = Var->pmethod_from_datatype; /* Remember the preset value */
    
    Var->pmethod = (ODB_packing(NULL) == 0) ? 0 : ODB_packing(&packing_method);
    /* Packed = PackDS(P, Dbase, Type, Var); */
    Packed = ( (IO_method == 5 && !Var->d && !Var->pd) ?
	       Var->d = DCA_fetch(Handle, Dbase, Var->table, Var->name, PoolNo, 1,
				  NULL, 0, &Var->dlen, NULL, NULL, NULL) : NULL,
	       DoPackDS(Var) );
    FREE(Packed);

    if (Var->savelist == 0) {
      N[0] = Var->saved_data_N[0];
      N[1] = Var->saved_data_N[1];
    }
    else {
      N[0] = (Var->is_packed) ? BYTESIZE(Var->pd) : BYTESIZE2(Var->d,Var->elemsize);
      N[1] = (Var->is_packed) ? Var->pmethod : 0;
      pmethod = N[1];
      Var->datatype = datatype;
      N[1] |= (datatype << 8);
    }

    /*
    fprintf(stderr,
	    "Write_DS(%s@%s): PoolNo=%d, savelist=%d, N[0:1]=(%d:%d)\n",
	    Var->name, Var->table, PoolNo, Var->savelist, N[0], N[1]);
    */
    
    Nbytes += newio_Write32(&fp_idx, filename, "No of bytes & Pmethod",
			    Var->name, Handle, PoolNo,
			    N, sizeof(*N), 2);

    if (Var->savelist == 0) {
      Nbytes += newio_Write32(&fp_idx, filename, "Data from savelist-backup",
			      Var->name, Handle, PoolNo,
			      Var->saved_data, 1, Var->saved_data_nbytes);
    }
    else {
      if (pmethod > 0) {
	Nbytes += newio_Write32(&fp_idx, filename, "Packed data",
				Var->name, Handle, PoolNo,
				Var->pd, sizeof(*Var->pd), Var->pdlen);
      }
      else {
	Nbytes += newio_Write32(&fp_idx, filename, "Unpacked data",
				Var->name, Handle, PoolNo,
				Var->d, Var->elemsize, Var->dlen);
      }
    }
  }

  DRHOOK_END(Nbytes);
  return Nbytes;
}


PUBLIC int
ODBSQL_PrintGet(const char *Sql[],
		FILE *fp, int mode, const char *prefix, const char *postfix, char **sqlout)
{
  int rc = 0;
  if (Sql && fp) {
    int j=0;
    const char *s;
    while ((s = Sql[j]) != NULL) {
      fprintf(fp,"%s%s%s",
          prefix ? prefix : "", s,
          postfix ? postfix : "\n"); j++;
    } /* while ... */
    rc = j;
  }
  if (sqlout) *sqlout = NULL; /* Not implemented */
  return rc;
}

#define THEGETLOOP(Type) \
  { const Type *pVar = Var; for (k=0; k<Count; k++) D[k] = pVar[k+row_offset]; rc = Count; } break

PUBLIC int
ODBCopyGetTable(int Flag, int Count, double D[], const void *Var, uint datatype, int row_offset)
{ 
  int rc = 0;
  if (FLAG_FETCH(Flag)) {
    int k;
    DRHOOK_START(ODBCopyGetTable);
    switch (datatype) {
      /* Most common */
    case DATATYPE_REAL8 : THEGETLOOP(double);
    case DATATYPE_INT4 : THEGETLOOP(int);
    case DATATYPE_BITFIELD : THEGETLOOP(Bitfield);
    case DATATYPE_STRING : THEGETLOOP(string);
    case DATATYPE_YYYYMMDD : THEGETLOOP(yyyymmdd);
    case DATATYPE_HHMMSS : THEGETLOOP(hhmmss);
    case DATATYPE_LINKOFFSET : THEGETLOOP(linkoffset_t);
    case DATATYPE_LINKLEN : THEGETLOOP(linklen_t);
    case DATATYPE_BUFR : THEGETLOOP(bufr);
      /* Less common */
    case DATATYPE_INT1 : THEGETLOOP(char);
    case DATATYPE_INT2 : THEGETLOOP(short);
    case DATATYPE_INT8 : THEGETLOOP(longlong);
    case DATATYPE_UINT1 : THEGETLOOP(uchar);
    case DATATYPE_UINT2 : THEGETLOOP(ushort);
    case DATATYPE_UINT4 : THEGETLOOP(uint);
    case DATATYPE_UINT8 : THEGETLOOP(ulonglong);
    case DATATYPE_REAL4 : THEGETLOOP(float);
    case DATATYPE_GRIB : THEGETLOOP(grib);
      /* Unsupported */
    default:
      fprintf(stderr,"***Error in CopyGetTable(): Unsupported datatype"
	      " 0x%x (see odb/include/privpub.h)\n",datatype);
      RAISE(SIGABRT);
      rc = -1;
      break;
    } /* switch (datatype) */
    DRHOOK_END(Count);
  }
  return rc;
}

#define THEPUTLOOP(Type) \
  { Type *pVar = Var; for (k=0; k<Count; k++) pVar[k] = D[k]; rc = Count; } break

PUBLIC int
ODBCopyPutTable(int Flag, int Count, void *Var, const double D[], uint datatype)
{ 
  int rc = 0;
  if (FLAG_UPDATE(Flag)) {
    int k;
    DRHOOK_START(ODBCopyPutTable);
    switch (datatype) {
      /* Most common */
    case DATATYPE_REAL8 : THEPUTLOOP(double);
    case DATATYPE_INT4 : THEPUTLOOP(int);
    case DATATYPE_BITFIELD : THEPUTLOOP(Bitfield);
    case DATATYPE_STRING : THEPUTLOOP(string);
    case DATATYPE_YYYYMMDD : THEPUTLOOP(yyyymmdd);
    case DATATYPE_HHMMSS : THEPUTLOOP(hhmmss);
    case DATATYPE_LINKOFFSET : THEPUTLOOP(linkoffset_t);
    case DATATYPE_LINKLEN : THEPUTLOOP(linklen_t);
    case DATATYPE_BUFR : THEPUTLOOP(bufr);
      /* Less common */
    case DATATYPE_INT1 : THEPUTLOOP(char);
    case DATATYPE_INT2 : THEPUTLOOP(short);
    case DATATYPE_INT8 : THEPUTLOOP(longlong);
    case DATATYPE_UINT1 : THEPUTLOOP(uchar);
    case DATATYPE_UINT2 : THEPUTLOOP(ushort);
    case DATATYPE_UINT4 : THEPUTLOOP(uint);
    case DATATYPE_UINT8 : THEPUTLOOP(ulonglong);
    case DATATYPE_REAL4 : THEPUTLOOP(float);
    case DATATYPE_GRIB : THEPUTLOOP(grib);
      /* Unsupported */
    default:
      fprintf(stderr,
	      "***Error in CopyPutTable(): Unsupported datatype"
	      " 0x%x (see odb/include/privpub.h)\n",datatype);
      RAISE(SIGABRT);
      rc = -1;
      break;
    } /* switch (datatype) */
    DRHOOK_END(Count);
  }
  return rc;
}


/* GatherGet */

#ifdef MASK
#undef MASK
#endif

static const unsigned int 
mask[1+32] = {
          0U,  /*                                 0 */
          1U,  /*                                 1 */
          3U,  /*                                11 */
          7U,  /*                               111 */
         15U,  /*                              1111 */
         31U,  /*                             11111 */
         63U,  /*                            111111 */
        127U,  /*                           1111111 */
        255U,  /*                          11111111 */
        511U,  /*                         111111111 */
       1023U,  /*                        1111111111 */
       2047U,  /*                       11111111111 */
       4095U,  /*                      111111111111 */
       8191U,  /*                     1111111111111 */
      16383U,  /*                    11111111111111 */
      32767U,  /*                   111111111111111 */
      65535U,  /*                  1111111111111111 */
     131071U,  /*                 11111111111111111 */
     262143U,  /*                111111111111111111 */
     524287U,  /*               1111111111111111111 */
    1048575U,  /*              11111111111111111111 */
    2097151U,  /*             111111111111111111111 */
    4194303U,  /*            1111111111111111111111 */
    8388607U,  /*           11111111111111111111111 */
   16777215U,  /*          111111111111111111111111 */
   33554431U,  /*         1111111111111111111111111 */
   67108863U,  /*        11111111111111111111111111 */
  134217727U,  /*       111111111111111111111111111 */
  268435455U,  /*      1111111111111111111111111111 */
  536870911U,  /*     11111111111111111111111111111 */
 1073741823U,  /*    111111111111111111111111111111 */
 2147483647U,  /*   1111111111111111111111111111111 */
 4294967295U   /*  11111111111111111111111111111111 */
};

#define MASK(n) mask[n]

#define THEGATHERGETLOOP(Type) \
{ \
  const Type *pVar = Var; \
  if (Index) { for (k=k1; k<k2; k++) d[k-k1] = pVar[Index[k+row_offset]]; } \
  else       { for (k=k1; k<k2; k++) d[k-k1] = pVar[k-k1+row_offset]; } \
} break

PRIVATE void /* Note: a PRIVATE-routine; called only via ODBGatherGetBits() */
GatherGet(int k1, int k2, const int *Index, double d[], const void *Var, uint datatype, 
	  int row_offset)
{
  int k;
  DRHOOK_START(GatherGet);
  switch (datatype) {
    /* Most common */
  case DATATYPE_REAL8 : THEGATHERGETLOOP(double);
  case DATATYPE_INT4 : THEGATHERGETLOOP(int);
  case DATATYPE_BITFIELD : THEGATHERGETLOOP(Bitfield);
  case DATATYPE_STRING : THEGATHERGETLOOP(string);
  case DATATYPE_YYYYMMDD : THEGATHERGETLOOP(yyyymmdd);
  case DATATYPE_HHMMSS : THEGATHERGETLOOP(hhmmss);
  case DATATYPE_LINKOFFSET : THEGATHERGETLOOP(linkoffset_t);
  case DATATYPE_LINKLEN : THEGATHERGETLOOP(linklen_t);
  case DATATYPE_BUFR : THEGATHERGETLOOP(bufr);
    /* Less common */
  case DATATYPE_INT1 : THEGATHERGETLOOP(char);
  case DATATYPE_INT2 : THEGATHERGETLOOP(short);
  case DATATYPE_INT8 : THEGATHERGETLOOP(longlong);
  case DATATYPE_UINT1 : THEGATHERGETLOOP(uchar);
  case DATATYPE_UINT2 : THEGATHERGETLOOP(ushort);
  case DATATYPE_UINT4 : THEGATHERGETLOOP(uint);
  case DATATYPE_UINT8 : THEGATHERGETLOOP(ulonglong);
  case DATATYPE_REAL4 : THEGATHERGETLOOP(float);
  case DATATYPE_GRIB : THEGATHERGETLOOP(grib);
    /* Unsupported */
  default:
    fprintf(stderr,
	    "***Error in GatherGet(): Unsupported datatype"
	    " 0x%x (see odb/include/privpub.h)\n",datatype);
    RAISE(SIGABRT);
    break;
  } /* switch (datatype) */
  DRHOOK_END(k2-k1);
}


#define THEGATHERGETBITSLOOP(Type) \
{ \
  const Type *pVar = Var; \
  if (Index) { for (k=k1; k<k2; k++) d[k-k1] = GET_BITS(pVar[Index[k+row_offset]], pos, len); } \
  else       { for (k=k1; k<k2; k++) d[k-k1] = GET_BITS(pVar[k-k1+row_offset], pos, len); } \
} break

PUBLIC void
ODBGatherGetBits(int k1, int k2, const int *Index, double d[], const void *Var, uint datatype,
		 int pos, int len, int row_offset)
{
  int k;
  if (len <= 0) {
    GatherGet(k1, k2, Index, d, Var, datatype, row_offset);
    return;
  }
  else {
    DRHOOK_START(ODBGatherGetBits);
    switch (datatype) {
      /* The only applicable */
    case DATATYPE_BITFIELD : THEGATHERGETBITSLOOP(Bitfield);
      /* Unsupported */
    default:
      fprintf(stderr,
	      "***Error in ODBGatherGetBits(): Unsupported datatype 0x%x"
	      " (see odb/include/privpub.h)\n",datatype);
      RAISE(SIGABRT);
      break;
    } /* switch (datatype) */
    DRHOOK_END(k2-k1);
  }
}


/* ScatterPut */


#define THESCATTERPUTLOOP(Type) \
{ \
  Type *pVar = Var; \
  for (k=k1; k<k2; k++) pVar[Index[k]] = d[k-k1]; \
} break

PRIVATE void /* Note: a PRIVATE-routine; called only via ODBScatterPutBits() */
ScatterPut(int k1, int k2, const int *Index, void *Var, const double d[], uint datatype)
{
  int k;
  DRHOOK_START(ScatterPut);
  switch (datatype) {
    /* Most common */
  case DATATYPE_REAL8 : THESCATTERPUTLOOP(double);
  case DATATYPE_INT4 : THESCATTERPUTLOOP(int);
  case DATATYPE_BITFIELD : THESCATTERPUTLOOP(Bitfield);
  case DATATYPE_STRING : THESCATTERPUTLOOP(string);
  case DATATYPE_YYYYMMDD : THESCATTERPUTLOOP(yyyymmdd);
  case DATATYPE_HHMMSS : THESCATTERPUTLOOP(hhmmss);
  case DATATYPE_LINKOFFSET : THESCATTERPUTLOOP(linkoffset_t);
  case DATATYPE_LINKLEN : THESCATTERPUTLOOP(linklen_t);
  case DATATYPE_BUFR : THESCATTERPUTLOOP(bufr);
    /* Less common */
  case DATATYPE_INT1 : THESCATTERPUTLOOP(char);
  case DATATYPE_INT2 : THESCATTERPUTLOOP(short);
  case DATATYPE_INT8 : THESCATTERPUTLOOP(longlong);
  case DATATYPE_UINT1 : THESCATTERPUTLOOP(uchar);
  case DATATYPE_UINT2 : THESCATTERPUTLOOP(ushort);
  case DATATYPE_UINT4 : THESCATTERPUTLOOP(uint);
  case DATATYPE_UINT8 : THESCATTERPUTLOOP(ulonglong);
  case DATATYPE_REAL4 : THESCATTERPUTLOOP(float);
  case DATATYPE_GRIB : THESCATTERPUTLOOP(grib);
    /* Unsupported */
  default:
    fprintf(stderr,
	    "***Error in ScatterPut(): Unsupported datatype"
	    " 0x%x (see odb/include/privpub.h)\n",datatype);
    RAISE(SIGABRT);
    break;
  } /* switch (datatype) */
  DRHOOK_END(k2-k1);
}


#define THESCATTERPUTBITSLOOP(Type) \
{ \
  Type *pVar = Var; \
  for (k=k1; k<k2; k++) PUT_BITS(pVar[Index[k]], d[k-k1], pos, len); \
} break

PUBLIC void
ODBScatterPutBits(int k1, int k2, const int *Index, void *Var, const double d[], uint datatype,
		 int pos, int len)
{
  int k;
  if (!Index) {
    /* Not applicable */
    return;
  }
  else if (len <= 0) {
    ScatterPut(k1, k2, Index, Var, d, datatype);
    return;
  }
  else {
    DRHOOK_START(ODBScatterPutBits);
    switch (datatype) {
      /* The only applicable */
    case DATATYPE_BITFIELD : THESCATTERPUTBITSLOOP(Bitfield);
      /* Unsupported */
    default:
      fprintf(stderr,
	      "***Error in ODBScatterPutBits(): Unsupported datatype"
	      " 0x%x (see odb/include/privpub.h)\n",datatype);
      RAISE(SIGABRT);
      break;
    } /* switch (datatype) */
    DRHOOK_END(k2-k1);
  }
}
