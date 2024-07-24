#include "odb.h"

/* Generic entry points for every recognized database */

#if defined(DUMMY_STATIC_INIT)

/* Dummies */

#define Dummy_Static_Init(db) \
void \
db##_static_init() \
{ \
  char *msg = #db "_static_init(): You were not supposed to call this routine"; \
  int len = strlen(msg); \
  fprintf(stderr,"%s\n",msg); \
  codb_abort_func_(msg,len); \
} \
\
void \
db##_print_flags_file() \
{ \
  char *msg = #db "_print_flags_file(): You were not supposed to call this routine"; \
  int len = strlen(msg); \
  fprintf(stderr,"%s\n",msg); \
  codb_abort_func_(msg,len); \
}

#else 

/* Non-dummies */

#define Static_Init(db) if (strequ(p_dbname, #db)) { \
  extern void db##_static_init(); \
  ODB_add2funclist(#db, db##_static_init, 0); \
}

#if !defined(STATIC_C)
PUBLIC void
codb_set_entrypoint_(const char *dbname
		     /* Hidden arguments */
		     , int dbname_len) 
     /* Fortran callable */
{
  DECL_FTN_CHAR(dbname);
  ALLOC_FTN_CHAR(dbname);

#if !defined(CODB_SET_ENTRYPOINT_C)
  /* Using sed-command, define _MYDB_ and filter this file to produce an additional 
     entry point to your suit own database needs. 

     For example, database XYZ, do the following:

     (1) sed 's/_MYDB_/XYZ/' < $INCDIR/static.h > glue.c
     (2) cc -c -I$INCDIR [cflags] glue.c
     (3) f90 [linkerflags] mainprog.o glue.o -lodb -lXYZ [other_libs] -o a.out

     If you need more than just one new database to be introduced, you
     have to be more elaborate with the sed-command. For two databases,
     for instance XYZ & ABC, do the following:

     (1) sed 's/_MYDB_)/XYZ);Static_Init(ABC)/' < $INCDIR/static.h > glue.c
     (2) cc -c -I$INCDIR [cflags] glue.c
     (3) f90 [linkerflags] mainprog.o glue.o -lodb -lXYZ -lABC [other_libs] -o a.out

     For more than two databases: just alter the sed-command's filter accordingly!

     Examples assume that you have already managed to create libXYZ.a & libABC.a
     from XYZ.ddl & ABC.ddl, respectively.

     $INCDIR is the location of general include-files, say /ccvobs/odb/include.

     cc & f90 are C and Fortran90 compilers of a particular computer system and
     needs to be adjusted to suit your needs.
  */
  Static_Init(_MYDB_);
#endif /* if !defined(CODB_SET_ENTRYPOINT_C) */

  /* Recognized databases (add database entries as required) */

  /* Please check that for given database ABC you also have the dummy
     file ABC_static_init.c -- contents similar to the ECMA_static_init.c */

#if 0
  /* No predefined libs as of 11-Apr-2006/SS i.e. all libs as important */
  Static_Init(ECMA);
  Static_Init(ECMASCR);
  Static_Init(CCMA);
  Static_Init(MTOCOMP);
  Static_Init(PREODB);
#endif

  FREE_FTN_CHAR(dbname);
}
#endif /* if !defined(STATIC_C) */

#endif /* if defined(DUMMY_STATIC_INIT) ... else ... */
