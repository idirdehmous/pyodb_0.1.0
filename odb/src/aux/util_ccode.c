
/* util_ccode.c */

#include <stdio.h>
#include <string.h>
/* #include <malloc.h> */
#include <stdlib.h>
#include <signal.h>

#include "alloc.h"

#define FORTRAN_CALL

#ifdef CRAY
#include <fortran.h>
#else
#define _fcd         char *
#define _fcdtocp(x)  x
#endif

#define COPY_TO(to,from_ftn,len) \
{ strncpy((to),_fcdtocp((from_ftn)),(len)); (to)[(len)] = '\0'; }

#define COPY_FROM(to_ftn,from,len) \
{ strncpy(_fcdtocp(to_ftn),(from),(len)); }

#define STRINGLEN(x) strlen(_fcdtocp(x))

/* Not needed; We use -qextname while linking with Fortran (SS/23.07.2001)
#ifdef RS6K
#define util_cgetenv_  util_cgetenv
#define util_igetenv_  util_igetenv
#define util_abort_    util_abort
#define util_cputime_  util_cputime
#define util_walltime_ util_walltime
#define util_ihpstat_  util_ihpstat
#define util_remove_file_ util_remove_file
#endif
*/

#ifdef CRAY
#define util_cgetenv_  UTIL_CGETENV
#define util_igetenv_  UTIL_IGETENV
#define util_abort_    UTIL_ABORT
#define util_cputime_  UTIL_CPUTIME
#define util_walltime_ UTIL_WALLTIME
#define util_remove_file_ UTIL_REMOVE_FILE
#endif

FORTRAN_CALL
void util_cgetenv_(_fcd envname, _fcd def, _fcd output, 
		  int *actual_len
#ifndef CRAY
		 , int envname_len, int def_len, int output_len
#endif
) 
{
#ifdef CRAY
  int envname_len = _fcdlen(envname);
  int def_len     = _fcdlen(def);
  int output_len  = _fcdlen(output);
#endif
  char *p_envname = NULL;
  char *p;
  char *p_def = NULL;

  ALLOC(p_envname, envname_len+1);

  COPY_TO(p_envname, envname, envname_len);
  p = getenv(p_envname);
  FREE(p_envname);

  if (!p) {
    ALLOC(p_def, def_len+1);
    p = p_def;
    COPY_TO(p_def, def, def_len);
  }

  COPY_FROM(output, p, output_len);

  *actual_len = (strlen(p) < output_len) ? STRINGLEN(output) : output_len;
  if (*actual_len < output_len) {
    memset(&_fcdtocp(output)[*actual_len],' ',output_len - *actual_len);
  }

  FREE(p_def);
}


FORTRAN_CALL
void util_igetenv_(_fcd envname, int *def, int *output
#ifndef CRAY
		  , int envname_len
#endif
) 
{
#ifdef CRAY
  int envname_len = _fcdlen(envname);
#endif
  char *p_envname = NULL;
  char *p;

  ALLOC(p_envname, envname_len+1);

  COPY_TO(p_envname, envname, envname_len);
  p = getenv(p_envname);

  *output = p ? atoi(p) : *def;
  
  FREE(p_envname);
}


FORTRAN_CALL
void util_remove_file_(_fcd file, int *rc
#ifndef CRAY
		  , int file_len
#endif
) 
{
#ifdef CRAY
  int file_len = _fcdlen(file);
#endif
  char *p_file = NULL;

  ALLOC(p_file, file_len+1);
  
  COPY_TO(p_file, file, file_len);

  *rc = remove(p_file);
  
  FREE(p_file);
}

FORTRAN_CALL
void util_abort_()
{
  fprintf(stderr,"util_abort_(): Raising an abort ...\n");
  RAISE(SIGABRT);
}

/* Portable CPU-timer (User + Sys) ; also WALL CLOCK-timer */

/* 
   These have been moved to libifsaux.a, 
   file ifsaux/support/drhook.c on 16-Jan-2004/SS,
   to simplify linking
*/
