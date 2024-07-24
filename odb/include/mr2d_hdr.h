#include <stdio.h>
#include <string.h>
#ifdef USE_MALLOC_H
#include <malloc.h>
#endif
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>

static char vers[]     = "1.2";
static char date_str[] = __DATE__;
static char time_str[] = __TIME__;

#if defined(CRAY) && !defined(T3D) && !defined(T3E)
#define SYSTEM_NAME "(CRAY PVP)"
#endif

#ifdef T3D
#define SYSTEM_NAME "(CRAY T3D)"
#endif

#ifdef T3E
#define SYSTEM_NAME "(CRAY T3E)"
#endif

#ifdef SGI
#define SYSTEM_NAME "(Silicon Graphics)"
#endif

#ifdef RS6K
#define SYSTEM_NAME "(IBM RS/6000)"
#endif

#ifdef VPP
#define SYSTEM_NAME "(Fujitsu VPP)"
#endif

#ifdef NECSX
#define SYSTEM_NAME "(NEC SX)"
#endif

#ifdef LINUX
#define SYSTEM_NAME "(LINUX)"
#endif

#ifndef SYSTEM_NAME
#define SYSTEM_NAME "(Unknown system)"
#endif

static char system_name[] = SYSTEM_NAME;

#include "magicwords.h"
#include "alloc.h"

#define FREEIOBUF(x) if (x.p) { FREE(x.p); x.p = NULL; } x.len = 0

#define SETBUF(fp, iobuf) { \
        ALLOC(iobuf.p, iobuf.len + 8); \
        if (iobuf.p) setvbuf(fp, (char *)iobuf.p, _IOFBF, iobuf.len); \
        else iobuf.len = 0; }

typedef struct iobuf_t {
  int            len;
  unsigned char *p;
} IObuf;

extern int filesize(const char *path);

extern int filesize_by_fp(FILE *fp, const char *path);

extern FILE *fopen_prealloc(const char *filename,
                            const char *mode,
                            int         prealloc,
                            int         extent);


#define has_no_percent_sign(s) (strchr(s,'%')==NULL)

static char *
KiloMegaGiga(char *s)
{
  const double maxint = 2147483647.0;
  char *p = s;
  int   len = strlen(s);
  char *last = &s[len-1];
  double num;

  if (*last == 'k' || *last == 'K') {
    *last = '\0';
    num = atoi(s) * 1024.0; /* multiplier: 1kB */
    num = MIN(num, maxint);
    num = MAX(0, num);
    ALLOC(p, 20);
    sprintf(p,"%0.f",num);
  }
  else if (*last == 'm' || *last == 'M') {
    *last = '\0';
    num = atoi(s) * 1048576.0; /* multiplier: 1MB */
    num = MIN(num, maxint);
    num = MAX(0, num);
    ALLOC(p, 20);
    sprintf(p,"%.0f",num);
  }
  else if (*last == 'g' || *last == 'G') {
    *last = '\0';
    num = atoi(s) * 1073741824.0; /* multiplier: 1GB */
    num = MIN(num, maxint);
    num = MAX(0, num);
    ALLOC(p, 20);
    sprintf(p,"%.0f",num);
  }

  return p;
}
