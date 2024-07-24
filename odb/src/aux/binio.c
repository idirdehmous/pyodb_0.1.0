#include <stdio.h>
/* #include <malloc.h> */
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "alloc.h"

/* Fortran-callable binary I/O utility */

#define BINIO_CLOSED 0
#define BINIO_FOPEN  1
#define BINIO_POPEN  2

typedef struct binio_ {
  FILE *io;
  char *name;
  char *mode;
  int kind;
} Binio_t;

PRIVATE Binio_t bin[FOPEN_MAX] = { 0 };

void 
binary_open_(int *channel,
	     const char *name,
	     const char *mode,
	     int *rc
	     /* Hidden arguments */
	     ,int name_len
	     ,int mode_len
	     )
{
  int j;
  int ret = 0;
  int kind = BINIO_CLOSED;
  FILE *io;
  char *Name = NULL;
  char *Mode = NULL;
  char *pName;
  char *pMode;

  ALLOC(Name, name_len + 1);
  for (j=0; j<name_len; j++) Name[j] = name[j];
  for (j=name_len-1; j>=0; j--) {
    if (!isspace(Name[j])) break;
    name_len--;
  }
  Name[name_len] = '\0';
  pName = Name;
  while (isspace(*pName)) pName++;

  ALLOC(Mode, mode_len + 1);
  for (j=0; j<mode_len; j++) Mode[j] = mode[j];
  for (j=mode_len-1; j>=0; j--) {
    if (!isspace(Mode[j])) break;
    mode_len--;
  }
  Mode[mode_len] = '\0';
  pMode = Mode;
  while (isspace(*pMode)) pMode++;

  if (pName[0] == '|') {
    pName++;
    io = popen(pName, pMode);
    kind = BINIO_POPEN;
  }
  else {
    io = fopen(pName, pMode);
    kind = BINIO_FOPEN;
  }

  if (!io) {
    perror(pName);
    ret = -1;
    goto finish;
  }

  *channel = fileno(io);
  bin[*channel].io = io;
  bin[*channel].name = STRDUP(pName);
  bin[*channel].mode = STRDUP(pMode);
  bin[*channel].kind = kind;

 finish:
  FREE(Name);
  *rc = ret;
}


void
binary_flush_(const int *channel,
	      int *rc)
{
  int ret = 0;
  FILE *io = bin[*channel].io;
  ret = fflush(io);

  /* finish: */
  *rc = ret;
}


void
binary_close_(int *channel,
	      int *rc)
{
  int ret = 0;
  FILE *io = bin[*channel].io;
  if (bin[*channel].kind == BINIO_FOPEN) {
    ret = fclose(io);
    bin[*channel].io = NULL;
    FREE(bin[*channel].name);
  }
  else if (bin[*channel].kind == BINIO_POPEN) {
    ret = pclose(io);
    bin[*channel].io = NULL;
    FREE(bin[*channel].name);
  }
  *channel = -1;

  /* finish: */
  *rc = ret;
}


void
binary_put_(const int *channel,
	    const char bytes[],
	    const int *nbytes,
	    int *rc)
{
  int ret = 0;
  FILE *io = bin[*channel].io;
  int nb;

  nb = fwrite(bytes, sizeof(char), *nbytes, io);
  if (nb != sizeof(char) * (*nbytes)) {
    perror("***Error in BINARY_PUT: fwrite");
    ret = -1;
    goto finish;
  }

  ret = nb;
 finish:
  *rc = ret;
}


void
binary_get_(const int *channel,
	    char bytes[],
	    const int *nbytes,
	    int *rc)
{
  int ret = 0;
  FILE *io = bin[*channel].io;
  int nb;

  nb = fread(bytes, sizeof(char), *nbytes, io);
  if (nb != sizeof(char) * (*nbytes)) {
    if (!feof(io)) {
      perror("***Error in BINARY_GET: fread");
      ret = -1;
      goto finish;
    }
  }

  ret = nb;
 finish:
  *rc = ret;
}


