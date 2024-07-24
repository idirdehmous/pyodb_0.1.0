#ifndef _ODBCS_H_
#define _ODBCS_H_

/* odbcs.h */

/* ODB client/server */

/* Based on (late) W.Richard Stevens' & et.al. 
   "Unix Network Programming", Volume 1, 3rd edition, 2004 */

/* Thanks to Baudouin Raoult (ECMWF) for many constructive ideas */

/* If you definitely don't want ODB client/server to be include, 
   then specify 

      -DODBCS=0 

   while you compile, since -UODBCS may still pick ODBCS depending on ARCH */

#include "setodbcs.h"

#ifdef ODBCS

#include "odb.h"
#include "odbi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <signal.h>
#include <ctype.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <pwd.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#if !defined(CYGWIN)
#include <sys/socketvar.h>
#endif

#include "vparam.h"

#include "odbcstags.h"

#define ODBCS_MAXLINE         4096    /* max text line length */
#define ODBCS_INET_ADDRSTRLEN   46    /* accomodated IPv6 address (and IPv4 for sure) */
#define ODBCS_INFO              16    /* max general info array length */

#if defined(__cplusplus)
extern "C" {
#endif

#define SA      struct sockaddr

typedef void    Sigfunc(int);   /* for signal handlers */

int odbi_server(int sockfd, int timeout, int verbose);

/* odbcs_signal.c */

void     setup_sig(const char *, const int *, const int *, const int *);
void     sig_chld(int);
void     sig_term(int);
void     sig_int(int);
void     sig_alrm(int);
void     sig_alrm_server(int);
void     sig_ignore(int);

/* odbcs_error.c */

void     err_dump(const char *, ...);
void     err_msg(const char *, ...);
void     err_quit(const char *, ...);
void     err_ret(const char *, ...);
void     err_sys(const char *, ...);

/* odbcs_wrappers.c */

int      Accept(int, SA *, socklen_t *);

void     Bind(int, const SA *, socklen_t);

void     Close(int);
void     Connect(int, SA *, socklen_t, const char *, int, int, const char *, int);

void     Fclose(FILE *);
FILE    *Fdopen(int, const char *);
char    *Fgets(char *, int, FILE *);
FILE    *Fopen(const char *, const char *);
pid_t    Fork(void);
void     Fputs(const char *, FILE *);

const char *Inet_ntop(int, const void *, char *, size_t);
void        Inet_pton(int, const char *, void *);

void     Listen(int, int);

ssize_t  Readline(int, void *, size_t);
ssize_t  Readline_timeo(int, void *, size_t, int);
ssize_t  Readn(int, void *, size_t);
ssize_t  Readn_timeo(int, void *, size_t, int);

int      Select(int, fd_set *, fd_set *, fd_set *, struct timeval *);
void     Shutdown(int, int);
Sigfunc *Signal(int, Sigfunc *);
int      Socket(int, int, int);

pid_t    Waitpid(pid_t, int *, int);
ssize_t  Writen(int, const void *, size_t);

/* odbcs_conf.c */

typedef struct _odbcs_t {
  struct _odbcs_t *next;
  struct _odbcs_t *prev;
  char *line;
  /* binpath */
  char *odb_binpath; /* $ODB_ROOT/bin */
  /* fields */
  char *hostname;
  char *username;
  int port;
  int timeout;
  char *arch;
  char *cpu_type;
  int object_mode;
  char *odb_arch;
  char *odb_version;
  char *odb_compdir;
  char *odb_dir;
  char *odb_root;
} odbcs_t;

const odbcs_t *odbcs_conf_match(const char *host, odbcs_t *conf, char **truehostname);
odbcs_t *odbcs_conf_create();

#if defined(__cplusplus)
}
#endif

#endif /* ODBCS */

#endif /* _ODBCS_H_ */
