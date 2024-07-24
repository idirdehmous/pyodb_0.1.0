#include "odbcs.h"

#ifdef ODBCS

#include        "odbcs.h"

#include <syslog.h>

static void     err_doit(int, int, const char *, va_list);

/* Nonfatal error related to system call
 * Print message and return */

void
err_ret(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  err_doit(1, LOG_INFO, fmt, ap);
  va_end(ap);
  return;
}

/* Fatal error related to system call
 * Print message and terminate */

void
err_sys(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  err_doit(1, LOG_ERR, fmt, ap);
  va_end(ap);
  exit(1);
}

/* Fatal error related to system call
 * Print message, dump core, and terminate */

void
err_dump(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  err_doit(1, LOG_ERR, fmt, ap);
  va_end(ap);
  abort();                /* dump core and terminate */
  exit(1);                /* shouldn't get here */
}

/* Nonfatal error unrelated to system call
 * Print message and return */

void
err_msg(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  err_doit(0, LOG_INFO, fmt, ap);
  va_end(ap);
  return;
}

/* Fatal error unrelated to system call
 * Print message and terminate */

void
err_quit(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  err_doit(0, LOG_ERR, fmt, ap);
  va_end(ap);
  exit(1);
}

/* Print message and return to caller
 * Caller specifies "errnoflag" and "level" */

static void
err_doit(int errnoflag, int level, const char *fmt, va_list ap)
{
  int errno_save = errno;
  vfprintf(stderr, fmt, ap);
  fprintf(stderr,": [severity=%d] : %s\n",
	  level,
	  (errno_save > 0 || level == LOG_INFO) ? strerror(errno_save) : "Unknown error");
  fflush(stderr);
  return;
}

#else

void dummy_odbcs_error() { }

#endif /* ODBCS */
