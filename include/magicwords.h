
/* magicwords.h */

#ifndef _MAGICWORDS_H_
#define _MAGICWORDS_H_

#include <stdio.h>

/* 
   To obtain unsigned int values, use f.ex. commands like:

   echo -n "PCMA" | od -t u4 | head -1 | awk '{print $2}'
   and you should get 1095582544 on a LITTLE-endian machine
   and                1346587969 on a BIG-endian machine 
*/

/* These values are for big-endian machines */

#define  PCMA 1346587969U /* 'PCMA' */
#define  ODB_ 1329873457U /* 'ODB1' */
#define  ODBI 1329873481U /* 'ODBI' */
#define  MR2D 1297232452U /* 'MR2D' */
#define  HC32 1212363570U /* 'HC32' */
#define  ALGN 1095518030U /* 'ALGN' */
#define  MMRY 1296912985U /* 'MMRY' */
#define  IDXB 1229215810U /* 'IDXB' */
#define  IDXT 1229215828U /* 'IDXT' */
#define  DCA2 1145258290U /* 'DCA2' */
#define  OCAC 1329807683U /* 'OCAC' */
#define  ODBX 1329873496U /* 'ODBX' */

/* 
   In reverse, to obtain unsigned int values, use f.ex. commands like:

   echo -n "AMCP" | od -t u4 | head -1 | awk '{print $2}'
   and you should get 1095582544 on a BIG-endian machine
   and                1346587969 on a LITTLE-endian machine 
*/

/* These values are for little-endian machines */

#define  AMCP 1095582544U /* 'PCMA' , in reverse */
#define  _BDO  826426447U /* 'ODB1' , in reverse */
#define  IBDO 1229079631U /* 'ODBI' , in reverse */
#define  D2RM 1144148557U /* 'MR2D' , in reverse */
#define _23CH  842220360U /* 'HC32' , in reverse */
#define  NGLA 1313295425U /* 'ALGN' , in reverse */
#define  YRMM 1498565965U /* 'MMRY' , in reverse */
#define  BXDI 1113080905U /* 'IDXB' , in reverse */
#define  TXDI 1415070793U /* 'IDXT' , in reverse */
#define _2ACD  843137860U /* 'DCA2' , in reverse */
#define  CACO 1128350543U /* 'OCAC' , in reverse */
#define  XBDO 1480737871U /* 'ODBX' , in reverse */

/* Access functions; also Fortran-callable */

extern void get_magic_pcma_(const int *reversed, unsigned int *value);
extern void get_magic_odb__(const int *reversed, unsigned int *value);
extern void get_magic_odbi_(const int *reversed, unsigned int *value);
extern void get_magic_mr2d_(const int *reversed, unsigned int *value);
extern void get_magic_hc32_(const int *reversed, unsigned int *value);
extern void get_magic_algn_(const int *reversed, unsigned int *value);
extern void get_magic_mmry_(const int *reversed, unsigned int *value);
extern void get_magic_idxb_(const int *reversed, unsigned int *value);
extern void get_magic_idxt_(const int *reversed, unsigned int *value);
extern void get_magic_dca2_(const int *reversed, unsigned int *value);
extern void get_magic_ocac_(const int *reversed, unsigned int *value);
extern void get_magic_obdx_(const int *reversed, unsigned int *value);

/* Compressed file auto-detection (see aux/cma_open.c) */

extern const char *
Compression_Suffix_Check(const char *filename);

extern const char *
Compression_Magic_Check(FILE *fp, unsigned int *first_word, int close_fp);

/* Maximum # of columns permitted */

extern int ODB_maxcols();
extern void codb_maxcols_(int *maxcols);

/* ODB version ; shared with the odb98-compiler */

extern const char *
codb_versions_(double *major, double *minor, int *numeric, const int *check_IFS_CYCLE_env_first);

#endif /* _MAGICWORDS_H_ */
