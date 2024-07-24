#ifndef _ODB_MACROS_H_
#define _ODB_MACROS_H_

extern int
Read_DS(void *Pvar,
	int Handle, int PoolNo, int Nrows, int Byteswap, int IO_method,
	const char *Dbase, int fp_idx, const char *filename,
	uint datatype);

extern int
Write_DS(void *Pvar,
	 int Handle, int PoolNo, int Nrows, int Byteswap, int IO_method,
	 const char *Dbase, int fp_idx, const char *filename,
	 uint datatype,
	 Packed_DS *(*DoPackDS)(void *));

extern int
ODBSQL_PrintGet(const char *Sql[],
		FILE *fp, int mode, const char *prefix, const char *postfix, char **sqlout);

extern int
ODBCopyGetTable(int Flag, int Count, double D[], const void *Var, uint datatype, int row_offset);

extern int
ODBCopyPutTable(int Flag, int Count, void *Var, const double D[], uint datatype);

extern void
ODBGatherGetBits(int k1, int k2, const int *Index, double d[], const void *Var, uint datatype,
		 int pos, int len, int row_offset);

extern void
ODBScatterPutBits(int k1, int k2, const int *Index, void *Var, const double d[], uint datatype,
		  int pos, int len);

#ifdef ODB_GENCODE
/* For generated codes only */

/* Macros used by generated C-code to shorten the code itself */

#define ALLOCINDEX(name, cnt) ALLOC(P->Index_##name, cnt); P->Allocated_##name = 1
#define FREEINDEX(name)  if (P->Allocated_##name) FREE(P->Index_##name)

/* Use of bitmap-index not implemented yet */
#ifdef USE_BITMAP_INDEX
#define ALLOCBITMAPINDEX(name) { int n_ = RNDUP_DIV(P->T_##name->Nrows, MAXBITS); ALLOC(P->BitmapIndex_##name, n_); }
#else
#define ALLOCBITMAPINDEX(name) P->BitmapIndex_##name = NULL
#endif

#define FREEBITMAPINDEX(name)  FREE(P->BitmapIndex_##name)

#define NULLIFY_INDEX(name)  \
  P->BitmapIndex_##name = NULL; P->Index_##name = NULL; P->Allocated_##name = 0

#define OFFSET_Macro(macro,i,offset) \
(((i+offset)>=i##_lo && (i+offset)<i##_hi) ? macro((int)(i+offset)) : 0)

#define FreeDS(P, Var, Nbytes, Count) { \
  if (P->Var.is_packed) { Nbytes += P->Var.pdlen * sizeof(*P->Var.pd); } \
                   else { Nbytes += P->Var.dlen * P->Var.elemsize; } \
  if (P->Var.savelist == 0) Nbytes += P->Var.saved_data_nbytes; \
  Count++; FREE_alldata(P->Var); \
}

/* Copy for Get/Put Table */

#define Call_CopyGet_TABLE(Dbase, Key, Num, Table, Type, D, Var, Count, DataType) \
{ \
  Type *Ptr = UseDS(P, Dbase, Type, Var); \
  if (P->Var.d != Ptr || !Ptr || P->Var.dlen < Count) { \
    fprintf(stderr,\
    "***Error in CopyGet_TABLE(%s, col#%d, @%s, d=%p, LdimD=%d, var=%s, Ptr=%p," \
    " Var.d=%p, dlen=%d, Count=%d, type=(%d,%s))\n", \
    #Dbase, Num, #Table, D, LdimD, P->Var.name, Ptr, P->Var.d, P->Var.dlen, \
    Count, DataType, #Type); RAISE(SIGABRT); \
  } \
  (void) ODBCopyGetTable(Flag[(Num-1)], Count, &D[(Num-1)*LdimD], Ptr, DataType, row_offset); \
  if (FLAG_FREE(Flag[(Num-1)])) { int Dummy=0; FreeDS(P, Var, Dummy, Dummy); } \
  else if (FLAG_PACK(Flag[(Num-1)])) { Packed_DS *Packed = PackDS(P, Dbase, Type, Var); FREE(Packed); } \
}

#define Call_CopyPut_TABLE(Dbase, Key, Num, Table, Type, Var, D, Count, DataType) \
{ \
  Type *Ptr = UseDSalloc(P, Dbase, Type, Var, Offset, \
                         (DoAlloc || P->Var.nalloc < P->Nalloc) ? P->Nalloc : 0); \
  int Dlen = P->Var.dlen; \
  if (P->Var.d + Offset != Ptr || !Ptr || P->Var.nalloc < Count) { \
    fprintf(stderr,\
    "***Error in CopyPut_TABLE(%s, col#%d, @%s, d=%p, LdimD=%d, var=%s, Ptr=%p," \
    " Var.d+%d=%p, nalloc=%d, Count=%d, type=(%d,%s))\n", \
    #Dbase, Num, #Table, D, LdimD, P->Var.name, Ptr, Offset, P->Var.d + Offset, P->Var.nalloc, \
    Count, DataType, #Type); RAISE(SIGABRT); \
  } \
  (void) ODBCopyPutTable(Flag[(Num-1)], Count, Ptr, &D[(Num-1)*LdimD], DataType); \
  P->Var.dlen = Dlen + Count; \
  if (FLAG_PACK(Flag[(Num-1)])) { Packed_DS *Packed = PackDS(P, Dbase, Type, Var); FREE(Packed); } \
}

/* Gather/Scatter for Get/Put View */

#define ODBMAC_TRACE_GATHERGET(Dbase, View, Table, Var, K1, K2, Num) \
    if (do_trace) { \
      ODB_Trace TracE; \
      TracE.handle = P->Handle; \
      TracE.msg = "Fetching '" #Var "@" #Table "' in view '" #View "'"; \
      TracE.msglen = strlen(TracE.msg); \
      TracE.args[0] = P->Funcs->pool->poolno; \
      TracE.args[1] = Num; \
      TracE.args[2] = K1; \
      TracE.args[3] = K2; \
      TracE.args[4] = LdimD; \
      TracE.args[5] = ncount; \
      TracE.args[6] = out_of_range; \
      TracE.args[7] = use_index; \
      TracE.args[8] = row_offset; \
      TracE.numargs = 9; \
      TracE.mode = -1; \
      codb_trace_(&TracE.handle, &TracE.mode, \
                  TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
    } \
    if (P->T_##Table->Var.d != Ptr || !Ptr || out_of_range > 0) { \
      fprintf(stderr,\
      "***Error in GatherGet_VIEW(%s, col#%d, %s, d=%p, LdimD=%d, var=%s@%s, Ptr=%p, " \
      "Var.d=%p, Var.dlen=%d, ncount=%d, low=%d, high=%d, out_of_range=%d, use_index=%d, row_offset=%d)\n", \
      #Dbase, Num, #View, D, LdimD, #Var, #Table, Ptr, \
      P->T_##Table->Var.d, P->T_##Table->Var.dlen, ncount, \
      low, high, out_of_range, use_index, row_offset); \
      RAISE(SIGABRT); \
    }

#define USE_INDEX(P,Var) (row_offset == 0 && P && P->IO_method == 5 && !Var.d && !Var.pd) ? 1 : 0

#define Call_GatherGet_VIEW(Dbase, Key, Num, View, Type, K1, K2, Table, D, Var, DataType, pos, len) { \
 if (Num <= Ncols && FLAG_FETCH(Flag[(Num-1)])) { \
    boolean use_index = USE_INDEX(P->T_##Table,P->T_##Table->Var); \
    /* boolean use_index = 0; activate this line if the line above is shocking! */ \
    int *idxaddr = &P->Index_##Table[K1]; \
    int ncount = K2 - K1; \
    Type *Ptr = UseDSindex(P->T_##Table, Dbase, Type, Var, \
                           use_index ? idxaddr : NULL, ncount); \
    int low = 0; \
    int high = P->T_##Table->Var.dlen - 1; \
    int out_of_range = 0; \
    if (!use_index) codb_test_index_range_(idxaddr, &ncount, &low, &high, &out_of_range); \
    ODBMAC_TRACE_GATHERGET(Dbase, View, Table, Var, K1, K2, Num); \
    if (!use_index) { \
      ODBGatherGetBits(K1, K2, P->Index_##Table, &D[(Num-1)*LdimD], Ptr, DataType, pos, len, row_offset); \
    } \
    else { /* Data already gathered (via previously used index --> use_index == 1) into consecutive locations */ \
      ODBGatherGetBits(K1, K2, NULL, &D[(Num-1)*LdimD], Ptr, DataType, pos, len, row_offset); \
      { int Dummy=0; FreeDS(P->T_##Table, Var, Dummy, Dummy); } \
    } \
 } \
 if (FLAG_FREE(Flag[(Num-1)])) { int Dummy=0; FreeDS(P->T_##Table, Var, Dummy, Dummy); } \
 else if (FLAG_PACK(Flag[(Num-1)])) { Packed_DS *Packed = PackDS(P->T_##Table, Dbase, Type, Var); FREE(Packed); } \
}

#define ODBMAC_TRACE_SCATTERPUT(Dbase, View, Table, Var, K1, K2, Num) \
    if (do_trace) { \
      ODB_Trace TracE; \
      TracE.handle = P->Handle; \
      TracE.msg = "Updating '" #Var "@" #Table "' in view '" #View "'"; \
      TracE.msglen = strlen(TracE.msg); \
      TracE.args[0] = P->Funcs->pool->poolno; \
      TracE.args[1] = Num; \
      TracE.args[2] = K1; \
      TracE.args[3] = K2; \
      TracE.args[4] = LdimD; \
      TracE.args[5] = ncount; \
      TracE.args[6] = out_of_range; \
      TracE.numargs = 7; \
      TracE.mode = -1; \
      codb_trace_(&TracE.handle, &TracE.mode, \
                  TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
    } \
    if (P->T_##Table->Var.d != Ptr || !Ptr || out_of_range > 0) { \
      fprintf(stderr,\
      "***Error in ScatterPut_VIEW(%s, col#%d, %s, d=%p, LdimD=%d, var=%s@%s, Ptr=%p, " \
      "Var.d=%p, Var.dlen=%d, ncount=%d, low=%d, high=%d, out_of_range=%d)\n", \
      #Dbase, Num, #View, D, LdimD, #Var, #Table, Ptr, \
      P->T_##Table->Var.d, P->T_##Table->Var.dlen, ncount, \
      low, high, out_of_range); \
      RAISE(SIGABRT); \
    }

#define Call_ScatterPut_VIEW(Dbase, Key, Num, View, Type, K1, K2, Table, Var, D, DataType, pos, len) { \
 if (Num <= Ncols && FLAG_UPDATE(Flag[(Num-1)]) && (ODBIT_test(P->can_UPDATE, Ncols, MAXBITS, Num-1, Num-1) == 1)) { \
    Type *Ptr = UseDSlong(P->T_##Table, Dbase, Type, P->T_##Table->Var); \
    int *idxaddr = &P->Index_##Table[K1]; \
    int ncount = K2 - K1; \
    int low = 0; \
    int high = P->T_##Table->Var.dlen - 1; \
    int out_of_range = 0; \
    codb_test_index_range_(idxaddr, &ncount, &low, &high, &out_of_range); \
    ODBMAC_TRACE_SCATTERPUT(Dbase, View, Table, Var, K1, K2, Num); \
    ODBScatterPutBits(K1, K2, P->Index_##Table, Ptr, &D[(Num-1)*LdimD], DataType, pos, len); \
 } \
 if (FLAG_PACK(Flag[(Num-1)])) { Packed_DS *Packed = PackDS(P->T_##Table, Dbase, Type, Var); FREE(Packed); } \
}

/* Macros for Load/Store tables */

#define Call_TABLE_Load(xname, pf, inc_pf) \
{ \
   int bytes = pf->common->is_considered ? pf->common->load(pf->data) : 0; \
   if (bytes < 0) { \
     fprintf(stderr,"***Error: Unable to load table '%s' (expected table '%s'): bytes=%d\n", \
             pf->common->name, #xname, bytes); \
     RAISE(SIGABRT); return bytes; \
   } \
   Nbytes += bytes; \
   if (inc_pf) pf = pf->next; \
}

#define Call_TABLE_Store(xname, pf, inc_pf) \
{ \
   int bytes = (pf->common->is_considered && pf->common->store) ? pf->common->store(pf->data) : 0; \
   if (bytes < 0) { \
     fprintf(stderr,"***Error: Unable to store table '%s' (expected table '%s') : bytes=%d\n", \
             pf->common->name, #xname, bytes); \
     RAISE(SIGABRT); return bytes; \
   } \
   Nbytes += bytes; \
   if (inc_pf) pf = pf->next; \
}

/* DS I/O */

#define Call_Read_DS(Dbase, fp_idx, filename, Nbytes, Type, DataType, Var)  \
{ \
  Nbytes += Read_DS(&P->Var, \
		    P->Handle, P->PoolNo, P->Nrows, P->Byteswap, P->IO_method, \
		    #Dbase, fp_idx, filename, \
	            DataType); \
}

#define Call_Write_DS(Dbase, fp_idx, filename, Nbytes, Type, DataType, Var)  \
{ \
  Nbytes += Write_DS(&P->Var, \
		     P->Handle, P->PoolNo, P->Nrows, P->Byteswap, P->IO_method, \
		     #Dbase, fp_idx, filename, \
	             DataType, \
                     (Packed_DS *(*)(void *))Dbase##_DoPackDS_##Type); /* a nice type-castink! */ \
}

#ifndef ODB_MAINCODE
#define DS_DummyPacks(Dbase, FType, Type) \
extern Type *Dbase##_unpack_##FType(const uint pd[], \
				    const int pdlen, int *dlen, int *method_used, uint datatype); \
extern uint *Dbase##_pack_##FType(const Type d[], \
				  const int dlen, int *method, int *pdlen, Bool avoid_copy);
#else
#define DS_DummyPacks(Dbase, FType, Type) \
PUBLIC Type \
*Dbase##_unpack_##FType(const uint pd[], \
   		        const int pdlen, int *dlen, int *method_used, uint datatype) \
{ \
  *dlen = -1; \
  fprintf(stderr,\
          "***Error: Shouldn't be calling this unpacking-function %s_unpack_%s() : method=%d\n", \
          #Dbase, #FType, *method_used); \
  RAISE(SIGABRT); \
  return NULL; \
} \
PUBLIC uint \
*Dbase##_pack_##FType(const Type d[], \
		      const int dlen, int *method, int *pdlen, Bool avoid_copy) \
{ \
  *pdlen = -1; \
  fprintf(stderr,\
          "***Error: Shouldn't be calling this packing-function for %s_pack_%s() : method=%d\n", \
          #Dbase, #FType, *method); \
  RAISE(SIGABRT); \
  return NULL; \
}
#endif

#ifndef ODB_MAINCODE
#define DS_Unpacking(Dbase, FType, Type) \
extern Type *Dbase##_DoUnpackDS_##Type(DeclareDS(Type, *Pvar), int Offset, int AllocMore);\
extern Type Dbase##_DoUnpackValueDS_##Type(DeclareDS(Type, *Pvar), int Offset, int Nalloc);
#else
#define DS_Unpacking(Dbase, FType, Type) \
PUBLIC Type *Dbase##_DoUnpackDS_##Type(DeclareDS(Type, *Pvar), int Offset, int AllocMore) \
{ \
 Type *Ptr = NULL; \
 coml_set_lockid_(&ODB_global_mylock[0]); \
 if (Pvar->is_packed || (AllocMore > Pvar->nalloc) || (!Pvar->d && Pvar->dlen == 0)) { \
  if (Pvar->is_packed) { \
    Pvar->d = Dbase##_unpack_##FType(Pvar->pd, Pvar->pdlen, \
                                     &Pvar->dlen, &Pvar->pmethod, Pvar->datatype); \
    ODB_packing_trace(0, \
		      Pvar->type, Pvar->name, Pvar->table, \
		      Pvar->pmethod, Pvar->pmethod_from_datatype, \
		      Pvar->dlen, Pvar->elemsize, Pvar->pdlen, Pvar->datatype); \
    if (!Pvar->d || Pvar->dlen < 0) { \
      fprintf(stderr,\
              "Unable to unpack '%s:%s@%s' : d=%p dlen=%d x %d, pdlen=%d, method=%d, orig_method=%d\n", \
              Pvar->type, Pvar->name, Pvar->table, Pvar->d, \
	      Pvar->dlen, Pvar->elemsize, Pvar->pdlen, \
	      Pvar->pmethod, Pvar->pmethod_from_datatype); \
      RAISE(SIGABRT); \
      return NULL; /* Error */ \
    } \
    FREE_pkdata(*Pvar); \
    Pvar->nalloc = Pvar->dlen; /* Guaranteed allocation */ \
  } \
  if (AllocMore > Pvar->nalloc) { \
    ALLOCMORE(Pvar->type, Pvar->name, Pvar->table, Pvar->d, Pvar->nalloc, AllocMore); \
    Pvar->nalloc = AllocMore; \
  } \
  /* Allocate at least one element (but keep dlen == 0) */ \
  if (!Pvar->d && Pvar->dlen == 0) { \
    Pvar->nalloc = 1; CALLOC(Pvar->d,Pvar->nalloc); \
  } \
 } \
 if (Pvar->d && (Offset == 0 || (Offset > 0 && Offset < Pvar->nalloc))) { \
   Type *base_addr = Pvar->d; \
   Ptr = &base_addr[Offset]; \
 } \
 else { \
   fprintf(stderr, \
   "***Error: Cannot deliver pointer to '%s:%s@%s' at Offset=%d : Conflicting d=%p and/or dlen=%d, nalloc=%d, allocmore=%d\n", \
           Pvar->type, Pvar->name, Pvar->table, Offset, Pvar->d, Pvar->dlen, Pvar->nalloc, AllocMore); \
   RAISE(SIGABRT); \
 } \
 coml_unset_lockid_(&ODB_global_mylock[0]); \
 return Ptr; \
} \
PUBLIC Type Dbase##_DoUnpackValueDS_##Type(DeclareDS(Type, *Pvar), int Offset, int Nalloc) \
{ \
  Type *P = Dbase##_DoUnpackDS_##Type(Pvar, Offset, Nalloc); \
  if (!P) { \
    fprintf(stderr, \
    "***Error: Unable to get data pointer in %s_DoUnpackValueDS_%s ; Offset=%d, Nalloc=%d\n", \
            #Dbase, #Type, Offset, Nalloc); \
            RAISE(SIGABRT); \
  } \
  return *P; \
}
#endif

#ifndef ODB_MAINCODE
#define DS_Packing(Dbase, FType, Type) \
extern Packed_DS *Dbase##_DoPackDS_##Type(DeclareDS(Type, *Pvar));
#else
#define DS_Packing(Dbase, FType, Type) \
PUBLIC Packed_DS *Dbase##_DoPackDS_##Type(DeclareDS(Type, *Pvar)) \
{ \
 Packed_DS *PDS; \
 ALLOC(PDS,1); \
 coml_set_lockid_(&ODB_global_mylock[1]); { \
  int pmethod_from_datatype_override = ODB_packing(&Pvar->pmethod_from_datatype); \
  uint *pd = NULL; \
  int pdlen = 0; \
  double bytes_in = 0, bytes_out = 0; \
  double CMAbytes_in = Pvar->dlen * sizeof(double); \
  PDS->on_error = 0; \
  if (Pvar->pmethod != pmethod_from_datatype_override) { \
    int enforce_packing = (ODB_packing(NULL) == -1) ? 1 : 0; \
    if (enforce_packing > 0) { \
      /* Enforce packing with the original packing method */ \
      /* May even require unpacking of a column to achieve desired effect */ \
      /* Warning: The following may break the IO_method == 5 (DCA-fetched) */ \
      Type *d = UseDS_no_hassle(Dbase, Type, *Pvar); /* Now unpacked */ \
      int packing_method_was = Pvar->pmethod; /* Record the packing method used */ \
      /* Set packing method back to the original, derived from datatype (unless overridden) */ \
      Pvar->pmethod = pmethod_from_datatype_override; \
      Pvar->pd = pd = Dbase##_pack_##FType(d, Pvar->dlen, &Pvar->pmethod, &pdlen, false); /* Re-pack */ \
      ODB_packing_trace(2, \
			Pvar->type, Pvar->name, Pvar->table, \
			packing_method_was, pmethod_from_datatype_override, \
			Pvar->dlen, Pvar->elemsize, pdlen, Pvar->datatype); \
      if (!pd || pdlen < 0) { PDS->on_error = 1; \
        fprintf(stderr,"***Error in enforcing packing\n"); RAISE(SIGABRT); return PDS; /* Error */ } \
      Pvar->pdlen = pdlen; \
      Pvar->is_packed = 1; \
      FREE_data(*Pvar); \
    } \
  } \
  Here_##Type: \
  if (Pvar->pmethod > 0) { \
    if (!Pvar->is_packed) { \
      if (Pvar->dlen > 0) { \
        int prtmsg, threshold; \
        double factor; \
        ODB_packing_setup(&prtmsg, &threshold, &factor); \
        Pvar->pd = pd = Dbase##_pack_##FType(Pvar->d, Pvar->dlen, &Pvar->pmethod, &pdlen, false); \
        ODB_packing_trace(1, \
	  		  Pvar->type, Pvar->name, Pvar->table, \
		  	  Pvar->pmethod, Pvar->pmethod_from_datatype, \
			  Pvar->dlen, Pvar->elemsize, pdlen, Pvar->datatype); \
        if (!pd || pdlen < 0) { PDS->on_error = 1; \
          fprintf(stderr,"***Error in packing\n"); RAISE(SIGABRT); return PDS; /* Error */ } \
        /* Turn packing off on the fly */ \
        bytes_in = BYTESIZE(Pvar->d); \
        bytes_out = BYTESIZE(pd); \
        if ( \
            ((bytes_in > threshold && bytes_out > bytes_in) || (bytes_out > factor*bytes_in))) { \
          if (prtmsg) { \
	    fprintf(stderr, \
	    "***Warning: Packed object (%.0f bytes) LARGER than unpacked (%.0f) : var='%s@%s'\n", \
	    bytes_out, bytes_in, Pvar->name, Pvar->table); \
	    fprintf(stderr, \
	    "            Due to exceeded thresholds (%d, %f) packing is now switched off\n", \
	    threshold, factor); \
	  } \
	  FREE_pkdata(*Pvar); \
	  Pvar->pmethod = 0; \
	  goto Here_##Type; \
        } \
      } \
      else { /* Pvar->dlen == 0 */ \
        FREE_pkdata(*Pvar); \
        Pvar->pmethod = 0; \
	goto Here_##Type; \
      } \
      FREE_data(*Pvar); \
      Pvar->is_packed = 1; \
    } \
    else { \
      pd = Pvar->pd; \
      pdlen = Pvar->pdlen; \
    } \
    PDS->pd = Pvar->pd = pd; \
    PDS->pdlen = Pvar->pdlen = pdlen; \
    PDS->opaque = NULL; \
    PDS->nbytes = 0; \
    bytes_in = BYTESIZE(Pvar->d); \
    bytes_out = BYTESIZE(Pvar->pd); \
  } \
  else { /* (Pvar->pmethod == 0) */ \
    PDS->pd = NULL; \
    PDS->pdlen = 0; \
    PDS->opaque = Pvar->d; \
    PDS->nbytes = BYTESIZE(Pvar->d); \
    bytes_out = PDS->nbytes; \
  } \
 } coml_unset_lockid_(&ODB_global_mylock[1]); \
 return PDS; \
}
#endif

#define PackDS(P, Dbase, Type, Var) \
( (P && P->IO_method == 5 && !P->Var.d && !P->Var.pd) ? \
     P->Var.d = DCA_fetch(P->Handle, #Dbase, P->Var.table, P->Var.name, P->PoolNo, 1, \
                       NULL, 0, &P->Var.dlen, NULL, NULL, NULL) : NULL, \
  Dbase##_DoPackDS_##Type(&P->Var) )

#define UseDS_no_hassle(Dbase, Type, Var)               Dbase##_DoUnpackDS_##Type(&Var, 0, 0) 
#define UseDS(P, Dbase, Type, Var)                      \
( (P && P->IO_method == 5 && !P->Var.d && !P->Var.pd) ? \
    (P->Var.d = DCA_fetch(P->Handle, #Dbase, P->Var.table, P->Var.name, P->PoolNo, 1, \
                       NULL, 0, &P->Var.dlen, NULL, NULL, NULL), \
     P->Var.nalloc = P->Var.dlen, P->Var.pd ? \
     (free(P->Var.pd), P->Var.d = NULL) : NULL, P->Var.pdlen = 0, P->Var.is_packed = 0) : 0, \
  Dbase##_DoUnpackDS_##Type(&P->Var, 0, 0) )
#define UseDSlong(P, Dbase, Type, Var)                      \
( (P && P->IO_method == 5 && !Var.d && !Var.pd) ? \
    (Var.d = DCA_fetch(P->Handle, #Dbase, Var.table, Var.name, P->PoolNo, 1, \
                       NULL, 0, &Var.dlen, NULL, NULL, NULL), \
     Var.nalloc = Var.dlen, Var.pd ? \
     (free(Var.pd), Var.d = NULL) : NULL, Var.pdlen = 0, Var.is_packed = 0) : 0, \
  Dbase##_DoUnpackDS_##Type(&Var, 0, 0) )
#define UseDSindex(P, Dbase, Type, Var, Index, lenIndex)          \
( (P && P->IO_method == 5 && !P->Var.d && !P->Var.pd) ? \
    (P->Var.d = DCA_fetch(P->Handle, #Dbase, P->Var.table, P->Var.name, P->PoolNo, 1, \
                       Index, lenIndex, &P->Var.dlen, NULL, NULL, NULL), \
     P->Var.nalloc = P->Var.dlen, P->Var.pd ? \
     (free(P->Var.pd), P->Var.d = NULL) : NULL, P->Var.pdlen = 0, P->Var.is_packed = 0) : 0, \
  Dbase##_DoUnpackDS_##Type(&P->Var, 0, 0) )
#define UseDSo(P, Dbase, Type, Var, Offset)             \
( (P && P->IO_method == 5 && !P->Var.d && !P->Var.pd) ? \
    (P->Var.d = DCA_fetch(P->Handle, #Dbase, P->Var.table, P->Var.name, P->PoolNo, 1, \
                       NULL, 0, &P->Var.dlen, NULL, NULL, NULL), \
     P->Var.nalloc = P->Var.dlen, P->Var.pd ? \
     (free(P->Var.pd), P->Var.d = NULL) : NULL, P->Var.pdlen = 0, P->Var.is_packed = 0) : 0, \
  Dbase##_DoUnpackDS_##Type(&P->Var, Offset, 0) )
#define UseDSoVal(P, Dbase, Type, Var, Offset)          \
( (P && P->IO_method == 5 && !P->Var.d && !P->Var.pd) ? \
    (P->Var.d = DCA_fetch(P->Handle, #Dbase, P->Var.table, P->Var.name, P->PoolNo, 1, \
                       NULL, 0, &P->Var.dlen, NULL, NULL, NULL), \
     P->Var.nalloc = P->Var.dlen, P->Var.pd ? \
     (free(P->Var.pd), P->Var.d = NULL) : NULL, P->Var.pdlen = 0, P->Var.is_packed = 0) : 0, \
  Dbase##_DoUnpackValueDS_##Type(&P->Var, Offset, 0) )
#define UseDSalloc(P, Dbase, Type, Var, Offset, Nalloc) \
( (P && P->IO_method == 5 && !P->Var.d && !P->Var.pd) ? \
    (P->Var.d = DCA_fetch(P->Handle, #Dbase, P->Var.table, P->Var.name, P->PoolNo, 1, \
                       NULL, 0, &P->Var.dlen, NULL, NULL, NULL), \
     P->Var.nalloc = P->Var.dlen, P->Var.pd ? \
     (free(P->Var.pd), P->Var.d = NULL) : NULL, P->Var.pdlen = 0, P->Var.is_packed = 0) : 0, \
  Dbase##_DoUnpackDS_##Type(&P->Var, Offset, Nalloc) )

/* #define UseDSoDirect(Var, Offset)                    (Var[Offset]) */

#if !defined(ODB_MAINCODE) && !defined(IS_a_VIEW)
#define InitDS(Type, DataType, Var, Table, PMethod) { \
  const int packing_method = PMethod; \
  { static char s[] = #Type; P->Var.type = s; } \
  { static char s[] = #Var; P->Var.name = s; } \
  { static char s[] = "/" #Var "/"; P->Var.xname = s; } \
  { static char s[] = #Table; P->Var.table = s; } \
  P->Var.datatype = DataType; /* Overwritten when unpacking/reading in a column */ \
  P->Var.elemsize = sizeof(Type); \
  P->Var.d = NULL; \
  P->Var.pd = NULL; \
  P->Var.dlen = 0; \
  P->Var.nalloc = 0; \
  P->Var.pdlen = 0; \
  P->Var.pmethod = (ODB_packing(NULL) == 0) ? 0 : ODB_packing(&packing_method); \
  P->Var.pmethod_from_datatype = packing_method; \
  P->Var.is_packed = 0; \
  P->Var.savelist = ODB_savelist(ODB_LABEL,#Var "@" #Table); \
  P->Var.saved_data = NULL; \
  P->Var.saved_data_nbytes = 0; \
  P->Var.saved_data_N[0] = 0; \
  P->Var.saved_data_N[1] = 0; \
}

#define PreInitTable(P, nc) { \
  P->Handle = -1; \
  P->PoolNo = PoolNo; \
  P->Funcs = NULL; \
  P->Is_loaded = 0; \
  P->Is_new = Is_new; \
  P->Swapped_out = 0; \
  P->Byteswap = 0; \
  P->IO_method = IO_method; \
  P->Created[0] = 0; \
  P->Created[1] = 0; \
  P->LastUpdated[0] = 0; \
  P->LastUpdated[1] = 0; \
  P->Ncols = nc; \
  P->Nrows = 0; \
  P->Nalloc = 0; \
  P->Numreqs = 0; \
}

#define Call_LookupTable(Table, T, Nrows, Ncols) \
  (void) Lookup_T_##Table(T, Nrows, Ncols)

#define DefineLookupTable(Table) \
PRIVATE int \
Lookup_T_##Table(void *T, int *Nrows, int *Ncols) \
{ \
  TABLE_##Table *P = T; \
  int PoolNo = P->PoolNo; \
  int Handle = P->Handle; \
  if (Nrows) *Nrows = 0; \
  if (Ncols) *Ncols = 0; \
  if (!P->Is_loaded && P->IO_method == 5) { \
    if (!DCA_getsize(Handle, ODB_LABEL, #Table, PoolNo, &P->Nrows, &P->Ncols)) { \
      int iret; \
      (void) DCA_alloc(Handle, ODB_LABEL, #Table, &iret); \
      (void) DCA_getsize(Handle, ODB_LABEL, #Table, PoolNo, &P->Nrows, &P->Ncols); \
    } \
    if (Nrows) *Nrows = P->Nrows; \
    P->Is_loaded = 1; \
  } \
  else if (Nrows && !P->Is_new && !P->Is_loaded && P->IO_method != 5) { \
    /* Can lookup only saved data */ \
    int rc, Nbytes = 0; \
    int close_too = 1; \
    int lookup_only = 1; \
    int fp_idx; \
    int swp; \
    uint Magic, Ninfo[2]; \
    const int two = 2; \
    char *filename = NULL; \
    /* ODB_Pool *Pool = P->Funcs->pool; */ \
    ODB_check_considereness(P->Funcs->common->is_considered, #Table); \
    MakeFileName(filename, ODB_LABEL, #Table, PoolNo); \
    /* fprintf(stderr,"Looking up TABLE %s from file %s ...\n",#Table, filename); */ \
    Nbytes = newio_Open32(&fp_idx, filename, ODB_LABEL, #Table, "lookup:open/read", \
                          "(lookup:open)", Handle, PoolNo, \
			  "r", close_too, lookup_only, Ninfo, 2); \
    swp = (Ninfo[0] == _BDO); \
    if (swp) { \
      swap4bytes_(Ninfo, &two); \
      newio_SetByteswap(&fp_idx, 1, Handle, PoolNo); \
    } \
    P->Byteswap = swp; \
    Magic = Ninfo[0]; \
    if (Magic != ODB_) { \
      fprintf(stderr,\
              "***Error: Invalid MAGIC-word: Database file '%s' is not ODB's\n",filename); \
      return -2; /* Error */ \
    } \
    if (Nrows) *Nrows = Ninfo[1]; \
    FREEX(filename); \
  } \
  else { \
    if (Nrows) *Nrows = P->Nrows; \
  } \
  if (Ncols) *Ncols = P->Ncols; \
  return 0; \
}

#define PreLoadTable(Dbase, Table) \
PUBLIC int \
Dbase##_Load_T_##Table(void *T) \
{ \
 int Nbytes = 0; \
 TABLE_##Table *P = T; \
 int PoolNo = P->PoolNo; \
 int Handle = P->Handle; \
 coml_set_lockid_(&ODB_global_mylock[2]); \
 if (!P->Is_loaded && P->IO_method == 5) { \
   if (!DCA_getsize(Handle, ODB_LABEL, #Table, PoolNo, &P->Nrows, &P->Ncols)) { \
     int iret; \
     (void) DCA_alloc(Handle, ODB_LABEL, #Table, &iret); \
     (void) DCA_getsize(Handle, ODB_LABEL, #Table, PoolNo, &P->Nrows, &P->Ncols); \
   } \
   P->Is_loaded = 1; \
 } \
 else if (!P->Is_loaded) { \
  int rc; \
  int fp_idx; \
  char *filename = NULL; \
  int Nrows=0, Ncols=0; \
  uint Magic, Ninfo[INFOLEN]; \
  int swp; \
  const int infolen = INFOLEN; \
  if (!P->Is_new && !P->Is_loaded) { /* Can only read saved data */ \
    int close_too = 0; \
    int lookup_only = 0; \
    FILE *do_trace = ODB_trace_fp(); \
    ODB_Pool *Pool = P->Funcs->pool; \
    ODB_check_considereness(P->Funcs->common->is_considered, #Table); \
    MakeFileName(filename, ODB_LABEL, #Table, PoolNo); \
    if (do_trace) { \
      ODB_Trace TracE; \
      TracE.handle = Handle; \
      TracE.msg = "Loading TABLE " #Table; \
      TracE.msglen = strlen(TracE.msg); \
      TracE.args[0] = PoolNo; \
      TracE.numargs = 1; \
      TracE.mode = 1; \
      codb_trace_(&TracE.handle, &TracE.mode, \
                  TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
    } \
    if (P->Swapped_out) { \
      (void) P->Funcs->common->init(P, Pool, P->Is_new, P->IO_method, 0, 1); \
      P->Swapped_out = 0; \
      P->Byteswap = 0; \
    } \
    Nbytes = newio_Open32(&fp_idx, filename, ODB_LABEL, #Table, "load:open/read", \
                          "(load:open)", Handle, PoolNo, \
			  "r", close_too, lookup_only, Ninfo, infolen); \
    swp = (Ninfo[0] == _BDO); \
    if (swp) { \
      swap4bytes_(Ninfo, &infolen); \
      newio_SetByteswap(&fp_idx, 1, Handle, PoolNo); \
    } \
    P->Byteswap = swp; \
    Magic = Ninfo[0]; \
    if (Magic != ODB_) \
      { fprintf(stderr,\
        "***Error: Magic != ODB_; Magic=%u, ODB_=%u\n",Magic,ODB_); \
        RAISE(SIGABRT); return -2; /* Error */ } \
    Nrows = Ninfo[1]; \
    if (Ninfo[2] == 0) Ninfo[2] = P->Ncols; \
    Ncols = Ninfo[2]; \
    if (Ncols > 0 && Ncols != P->Ncols) \
      { fprintf(stderr,\
                "***Error: Ncols != P->Ncols; Ncols=%d, P->Ncols=%d\n",Ncols,P->Ncols); \
        RAISE(SIGABRT); return -3; /* Error */ } \
    P->Created[0] = Ninfo[3]; \
    P->Created[1] = Ninfo[4]; \
    P->LastUpdated[0] = Ninfo[5]; \
    P->LastUpdated[1] = Ninfo[6]; \
    if (P->Nalloc < Nrows || Nrows == 0) { \
      int min_alloc = ODB_min_alloc(); \
      int inc_alloc; \
      P->Nalloc = RNDUP(Nrows, min_alloc); \
      inc_alloc = ODB_inc_alloc(P->Nalloc, min_alloc); \
      P->Nalloc = P->Nalloc + inc_alloc; \
    } \
    P->Nrows = Nrows

#define PostLoadTable(Table) \
    if (Nbytes > 0) { rc = newio_Close32(&fp_idx, filename, ODB_LABEL, #Table, "load:close", \
					 "(load:close)", P->Handle, P->PoolNo, Ninfo, INFOLEN \
					 ); \
		    } \
    FREEX(filename); \
    P->Is_loaded = 1; \
    if (do_trace) { \
      ODB_Trace TracE; \
      TracE.handle = P->Handle; \
      TracE.msg = "TABLE " #Table " loaded"; \
      TracE.msglen = strlen(TracE.msg); \
      TracE.args[0] = PoolNo; \
      TracE.args[1] = Nrows; \
      TracE.args[2] = Ncols; \
      TracE.args[3] = P->Nalloc; \
      TracE.args[4] = Nbytes; \
      TracE.numargs = 5; \
      TracE.mode = 0; \
      codb_trace_(&TracE.handle, &TracE.mode, \
                  TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
    } \
  } /* if (!P->Is_new && !P->Is_loaded) */ \
 } /* if (!P->Is_loaded) */ \
 coml_unset_lockid_(&ODB_global_mylock[2]); \
 return Nbytes; \
}

#define PreStoreTable(Dbase, Table) \
PUBLIC int \
Dbase##_Store_T_##Table(void *T) \
{ \
 int Nbytes = 0; \
 TABLE_##Table *P = T; \
 coml_set_lockid_(&ODB_global_mylock[3]); \
 if (P->Is_loaded && P->IO_method != 5) { \
  int rc; \
  int PoolNo = P->PoolNo; \
  int Handle = P->Handle; \
  int fp_idx; \
  char *filename = NULL; \
  char *table = #Table; \
  int Nrows=0, Ncols=0; \
  uint Ninfo[INFOLEN]; \
  int close_too = 0; \
  int lookup_only = 0; \
  FILE *do_trace = ODB_trace_fp(); \
  if (P->Swapped_out) goto return_Nbytes; \
  MakeFileName(filename, ODB_LABEL, table, PoolNo); \
  if (!P->Is_new && !P->Is_loaded) { \
    int size = P->Funcs->common->is_considered ? \
        newio_Size32(filename, ODB_LABEL, table, Handle, PoolNo) : 0; \
    if (size >= 0) { FREEX(filename); goto return_Nbytes; /* Is indeed created => return */ } \
  } \
  if (do_trace) { \
    ODB_Trace TracE; \
    TracE.handle = Handle; \
    TracE.msg = "Storing TABLE " #Table; \
    TracE.msglen = strlen(TracE.msg); \
    TracE.args[0] = PoolNo; \
    TracE.numargs = 1; \
    TracE.mode = 1; \
    codb_trace_(&TracE.handle, &TracE.mode, \
                TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
  } \
  if (P->Created[0] == 0 && P->Created[1] == 0) { \
    codb_datetime_(&P->Created[0], &P->Created[1]); \
    P->LastUpdated[0] = P->Created[0]; \
    P->LastUpdated[1] = P->Created[1]; \
  } \
  else { \
    codb_datetime_(&P->LastUpdated[0], &P->LastUpdated[1]); \
  } \
  Ninfo[0] = ODB_; \
  Ninfo[1] = Nrows = P->Nrows; \
  Ninfo[2] = Ncols = P->Ncols; \
  Ninfo[3] = P->Created[0]; \
  Ninfo[4] = P->Created[1]; \
  Ninfo[5] = P->LastUpdated[0]; \
  Ninfo[6] = P->LastUpdated[1]; \
  Nbytes = newio_Open32(&fp_idx, filename, ODB_LABEL, table, "store:open/write", \
		        "(store:open)", Handle, PoolNo, \
			"w", close_too, lookup_only, Ninfo, INFOLEN);

#define PostStoreTable(Table) \
  if (Nbytes > 0) { rc = newio_Close32(&fp_idx, filename, ODB_LABEL, table, "store:close", \
				       "(store:close)", Handle, PoolNo, Ninfo, INFOLEN \
				       ); \
		  } \
  FREEX(filename); \
  P->Is_new = 0; \
  if (do_trace) { \
    ODB_Trace TracE; \
    TracE.handle = Handle; \
    TracE.msg = "TABLE " #Table " stored"; \
    TracE.msglen = strlen(TracE.msg); \
    TracE.args[0] = PoolNo; \
    TracE.args[1] = Nrows; \
    TracE.args[2] = Ncols; \
    TracE.args[3] = Nbytes; \
    TracE.numargs = 4; \
    TracE.mode = 0; \
    codb_trace_(&TracE.handle, &TracE.mode, \
                TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
  } \
 } /* if (P->Is_loaded && P->IO_method != 5) */ \
return_Nbytes: \
 coml_unset_lockid_(&ODB_global_mylock[3]); \
 return Nbytes; \
}

#define DefineRemoveTable(Dbase, Table) \
PUBLIC int \
Dbase##_Remove_T_##Table(void *T) \
{ \
  TABLE_##Table *P = T; \
  int rc = 0; \
  int Nbytes = 0; \
  int PoolNo = P->PoolNo; \
  FILE *do_trace = ODB_trace_fp(); \
  if (do_trace) { \
    ODB_Trace TracE; \
    TracE.handle = P->Handle; \
    TracE.msg = "Removing TABLE " #Table; \
    TracE.msglen = strlen(TracE.msg); \
    TracE.args[0] = PoolNo; \
    TracE.numargs = 1; \
    TracE.mode = 0; \
    codb_trace_(&TracE.handle, &TracE.mode, \
                TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
  } \
  ODB_check_considereness(P->Funcs->common->is_considered, #Table); \
  Dbase##_Swapout_T_##Table(P); \
  P->Is_loaded = 1; \
  P->Swapped_out = 0; \
  P->Byteswap = 0; \
  P->Nrows = 0; \
  P->Nalloc = 0; \
  Nbytes = Dbase##_Store_T_##Table(P); \
  return Nbytes; \
}


#define PreGetTable(Dbase, Key, ExtType, Table) \
PUBLIC int \
Dbase##_##Key##Get_T_##Table(void *T, ExtType D[], \
			 int LdimD, int Nrows, int Ncols, \
                         int ProcID, const int Flag[], int row_offset) \
{ \
  TABLE_##Table *P = T; \
  int Count; \
  DRHOOK_START(Dbase##_##Key##Get_T_##Table); \
  ODBMAC_TABLE_DELAYED_LOAD(Table); \
  Count = MIN(Nrows, P->Nrows);

#define PostGetTable(Key, ExtType, Table) \
  DRHOOK_END(Count); \
  return Count; \
}

#define PrePutTable(Dbase, Key, ExtType, Table) \
PUBLIC int \
Dbase##_##Key##Put_T_##Table(void *T, const ExtType D[], \
			 int LdimD, int Nrows, int Ncols, \
                         int ProcID, const int Flag[]) \
{ \
  TABLE_##Table *P = T; \
  int Count = Nrows; \
  int Offset = P->Nrows; \
  boolean DoAlloc = 0; \
  boolean JustLoaded = 0; \
  DRHOOK_START(Dbase##_##Key##Put_T_##Table); \
  if (P->Nrows == 0 && (P->Swapped_out || !P->Is_loaded)) { \
    int Nbytes = 0; \
    Call_TABLE_Load(Table, P->Funcs, 0); \
    Offset = P->Nrows; \
    JustLoaded = P->Funcs->common->is_considered; \
  } \
  if (JustLoaded || P->Nalloc < (Offset + Count) || (Offset + Count) == 0) { \
    int min_alloc = ODB_min_alloc(); \
    int inc_alloc; \
    P->Nalloc = RNDUP(Offset + Count, min_alloc); \
    inc_alloc = ODB_inc_alloc(P->Nalloc, min_alloc); \
    P->Nalloc = P->Nalloc + inc_alloc; \
    DoAlloc = 1; \
  }

#define PostPutTable(Key, ExtType, Table) \
  P->Is_loaded = P->Funcs->common->is_considered; \
  P->Swapped_out = 0; \
  P->Byteswap = 0; \
  P->Nrows += Count; \
  DRHOOK_END(Count); \
  return P->Nrows; \
}

#endif /*  !defined(ODB_MAINCODE) && !defined(IS_a_VIEW) */


#ifdef ODB_MAINCODE
#define PreGetTable(Dbase, Key, ExtType, Table) \
extern int Dbase##_##Key##Get_T_##Table(void *T, ExtType D[], \
			 int LdimD, int Nrows, int Ncols, \
                         int ProcID, const int Flag[])

#define PrePutTable(Dbase, Key, ExtType, Table) \
extern int Dbase##_##Key##Put_T_##Table(void *T, const ExtType D[], \
			 int LdimD, int Nrows, int Ncols, \
                         int ProcID, const int Flag[])

#define PreLoadTable(Dbase, Table) \
extern int Dbase##_Load_T_##Table(void *T)

#define PreStoreTable(Dbase, Table) \
extern int Dbase##_Store_T_##Table(void *T)
#endif /*  ODB_MAINCODE */

#define ODBMAC_VIEW_TABLEDECL_NO_INDEX(name) \
  TABLE_##name *T_##name; \
  unsigned int *BitmapIndex_##name

#define ODBMAC_VIEW_TABLEDECL_WITH_INDEX(name) \
  ODBMAC_VIEW_TABLEDECL_NO_INDEX(name); \
  int *Index_##name; \
  int Allocated_##name


#define ODBMAC_CCL_V_PRE(view) \
  VIEW_##view *P = T; FREE(P->NrowVec); FREE(P->NrowOffset); P->Npes = 0; P->Nrows = 0

#define ODBMAC_LC_GETVAL(dbname, usdname, viewstr) \
  lc_USD_##usdname = ODB_getval_var(#dbname, "$" #usdname, viewstr, it)

#define ODBMAC_ADDR_TRIGGER(dbname, name, usdname, viewstr) \
  if (!Addr_trigger && strequ(name, "$" #usdname)) { \
    ODBMAC_LC_GETVAL(dbname, usdname, viewstr); \
    Addr_trigger = 1; \
  }

#define ODBMAC_PEINFO_BREAKLOOP() \
    if (!Addr && PE > PEstart && P->NrowVec && P->NrowOffset) { \
      break; /* Break from the PE-loop */ \
    }

#define ODBMAC_PEINFO_SETUP() \
  if (PEinfo) { \
    int Npes = P->Npes = PEinfo->npes; \
    FREE(P->NrowVec); \
    FREE(P->NrowOffset); \
    ALLOC(P->NrowVec, Npes); \
    ALLOC(P->NrowOffset, Npes); \
    P->Replicate_PE = PEinfo ? PEinfo->replicate_PE : 0; \
  }

#define ODBMAC_PEINFO_COPY() \
  if (PEinfo && P->NrowVec) { \
    for (PE=PEstart; PE<=PEend; PE++) { \
      PEinfo->nrowvec[PE-1] += P->NrowVec[PE-1]; \
    } /* for (PE=PEstart; PE<=PEend; PE++) */ \
  }

#define ODBMAC_PEINFO_INIT(view) \
  VIEW_##view *P = V; \
  if (PEinfo) { \
    PEinfo->addr = NULL; \
    PEinfo->varname = NULL; \
    PEinfo->varname_len = 0; \
    PEinfo->npes = P->Npes; \
    PEinfo->nrowvec = P->NrowVec; \
    PEinfo->replicate_PE = P->Replicate_PE; \
  }

#define ODBMAC_PEINFO_SELVIEW_SETUP() \
  FREE(P->NrowVec); FREE(P->NrowOffset); P->Npes = 0; \
  P->Replicate_PE = PEinfo ? PEinfo->replicate_PE : 0;

#define ODBMAC_PEINFO_OFFSET() \
  if (ProcID > 0 && ProcID <= Npes && P->NrowVec && P->NrowOffset) \
  { K1 = P->NrowOffset[ProcID-1]; K2 = K1 + P->NrowVec[ProcID-1]; }

#define ODBMAC_PEINFO_SKIP() \
  if (!Addr && PE > PEstart && P->NrowVec && P->NrowOffset) { \
    P->NrowVec[PE-1] = P->NrowVec[PEstart-1]; \
    P->NrowOffset[PE-1] = P->NrowOffset[PEstart-1]; \
    continue; /* Skip to the end of the PE-loop */ \
  }

#define ODBMAC_PEINFO_UPDATE_COUNTS() \
  if (P->NrowVec && P->NrowOffset) { \
    P->NrowVec[PE-1] = tmpcount; \
    P->NrowOffset[PE-1] = Count; \
  }

#define ODBMAC_TRACE_SWAPOUT(table,Ncols) \
  if (do_trace) { \
    ODB_Trace TracE; \
    TracE.handle = P->Handle; \
    TracE.msg = "TABLE " #table " swapped out"; \
    TracE.msglen = strlen(TracE.msg); \
    TracE.args[0] = PoolNo; \
    TracE.args[1] = Ncols; /* no. of cols released */ \
    TracE.args[2] = Count; /* no. of free()'s for data & packed data */ \
    TracE.args[3] = Nbytes; /* no. of bytes freed */ \
    TracE.numargs = 4; \
    TracE.mode = -1; \
    codb_trace_(&TracE.handle, &TracE.mode, \
      TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
  }

#define ODBMAC_TRACE_SELVIEW_SETUP(view, tables) \
  FILE *do_trace = ODB_trace_fp(); \
  ODB_Trace TracE; \
  static char *MsG[] = { \
    "Some table(s) {" tables "} maybe loaded on demand for view='" #view "'", \
    "PrS", \
    "PoS", \
    NULL \
  }

#define ODBMAC_TRACE_SELVIEW_PRE() \
  if (do_trace) { \
    TracE.handle = P->Handle; \
    TracE.numargs = 3; \
    TracE.args[0] = phase; \
    TracE.args[1] = P->PoolNo; \
  }

#define ODBMAC_TRACE_SELVIEW_0() \
  if (do_trace && Nbytes > 0) { \
    TracE.msg = MsG[0]; \
    TracE.msglen = strlen(MsG[0]); \
    TracE.args[2] = Nbytes; \
    TracE.mode = -1; \
    codb_trace_(&TracE.handle, &TracE.mode, \
        TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
  }

#define ODBMAC_TRACE_SELVIEW_1() \
  if (do_trace) { \
    TracE.msg = MsG[1]; \
    TracE.msglen = strlen(MsG[1]); \
    TracE.args[2] = 0; \
    TracE.mode = 1; \
    codb_trace_(&TracE.handle, &TracE.mode, \
        TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
  }

#define ODBMAC_TRACE_SELVIEW_POST() \
  if (do_trace) { \
    TracE.args[2] = CountPrS; \
    TracE.mode = 0; \
    codb_trace_(&TracE.handle, &TracE.mode, \
        TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
  }

#define ODBMAC_TRACE_SELVIEW_2() \
  if (do_trace && CountPrS > 0) { \
    TracE.msg = MsG[2]; \
    TracE.msglen = strlen(MsG[2]); \
    TracE.args[2] = 0; \
    TracE.mode = 1; \
    codb_trace_(&TracE.handle, &TracE.mode, \
        TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
  }

#define ODBMAC_TRACE_SELVIEW_LAST() \
  if (do_trace && CountPoS > 0) { \
    TracE.args[2] = CountPoS; \
    TracE.mode = 0; \
    codb_trace_(&TracE.handle, &TracE.mode, \
        TracE.msg, TracE.args, &TracE.numargs, TracE.msglen); \
  }

#define ODBMAC_ERRMSG_SELVIEW(view) \
  if (CountPrS != CountPoS) { \
    char msg[1024]; \
    snprintf(msg,sizeof(msg),"CountPrS=%d differs from CountPoS=%d in Sel_V_%s()",CountPrS,CountPoS,#view); \
    codb_abort_func_(msg,strlen(msg)); \
  }

#define ODBMAC_TABLESQL() if (sqlout) *sqlout = NULL; return 0

#define ODBMAC_VIEWSQL() \
  return ODBSQL_PrintGet(Sql, fp, mode, prefix, postfix, sqlout)

#define ODBMAC_SORTKEYS(view) \
  VIEW_##view *P = V; if (NSortKeys) *NSortKeys = P->NSortKeys; return P->SortKeys

#define ODBMAC_UPDATEINFO(view) \
  VIEW_##view *P = V; int j, Ncols; \
  Ncols = MIN(ncols, P->Ncols); \
  for (j=0; j<Ncols; j++) can_UPDATE[j] = \
    (ODBIT_test(P->can_UPDATE, Ncols, MAXBITS, j, j) == 1) ? 1 : 0; \
  return Ncols

#define ODBMAC_AGGRINFO(view) \
  VIEW_##view *P = V; int j, Ncols = P->Ncols, rc = P->numaggr; \
  if (aggr_func_flag) { \
    Ncols = MIN(ncols, P->Ncols); \
    for (j=0; j<Ncols; j++) aggr_func_flag[j] = P->aggr_func_flag[j]; \
  } \
  return rc /* number of columns having aggregate functions */

#define ODBMAC_GETINDEX(table) \
  if (strequ(Table,#table)) { *Nlen = P->Nrows; return P->Index_##table; }

#define ODBMAC_PUTINDEX(table) \
  if (strequ(Table,#table)) { \
    FREEINDEX(table); /* !!! Got possibly rid of an existing good index !!!*/ \
    if (by_address) { \
      P->Index_##table = idx; P->Allocated_##table = 0; \
    } else { int j; \
      ALLOCINDEX(table, Nidx); \
      for (j=0; j<Nidx; j++) P->Index_##table[j] = idx[j]; \
    } \
    P->Nrows = Nidx; /* !!! Resets Nrows just like that !!! */ \
    { /* Perform delayed load of this table -- if necessary */ \
      int Nbytes = 0; \
      ODBMAC_VIEW_DELAYED_LOAD(table); \
    } \
    return Nidx; \
  }

#define ODBMAC_DIM(view) \
  VIEW_##view *P = V; \
  if (Nrows)      { *Nrows = (ProcID > 0 && ProcID <= P->Npes && P->NrowVec) ? P->NrowVec[ProcID-1] : P->Nrows; } \
  if (Ncols)        *Ncols = P->Ncols; \
  if (Nrowoffset) { *Nrowoffset = (ProcID > 0 && ProcID <= P->Npes && P->NrowOffset) ? P->NrowOffset[ProcID-1] : 0; }

#define ODBMAC_VIEW_DELAYED_LOAD(xname) \
  if (!P->T_##xname->Is_loaded) { /* Delayed load */ \
    ODB_Funcs *pf = P->T_##xname->Funcs; \
    Call_TABLE_Load(xname, pf, 0); \
  }

#define ODBMAC_TABLE_DELAYED_LOAD(xname) \
  if (!P->Is_loaded) { /* Delayed load of existing data */ \
    int Nbytes = 0; \
    ODB_Funcs *pf = P->Funcs; \
    Call_TABLE_Load(xname, pf, 0); \
  }

#define ODBMAC_ASSIGN_TABLEDATA(tbl) \
  P->T_##tbl = NULL; \
  FORFUNC { if (pf->common->is_table && strequ(pf->common->name,"@" #tbl)) { P->T_##tbl = pf->data; break; } }

#define ODBMAC_INIT_IsConsidered(table, id) IsConsidered[id] = TableIsConsidered(#table)

#define ODBMAC_CREATE_TABLE(dbname, table, id, consider_me) \
  if (io_method != 5 || IsConsidered[id]) { \
    TABLE_##table *T_##table = dbname##_Init_T_##table(NULL, pool, is_new, io_method, it, 0); \
    ODB_Funcs *pf = T_##table->Funcs; nfuncs = ODB_add_funcs(pool, pf, it); \
    pf->common->is_considered = consider_me; \
  }

#define ODBMAC_COPY_COLAUX(name) \
if (colaux) { \
  int j, n = MIN(ColAux_len,colaux_len); \
  for (j=0; j<n; j++) colaux[j] = ColAux[j]; \
  filled = n; \
}

#endif /* ODB_GENCODE */

#endif
