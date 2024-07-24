
/* odbcs_signal.c */

#include "odbcs.h"

#ifdef ODBCS

static const char *Cmd = "";
static int Verbose = 0;
static int Timeout = -1;
static int Server_Timeout = -1;

extern const char *odb_resource_stamp_(const char *label); /* from ../lib.codb.c */

void
setup_sig(const char *cmd, const int *timeout, const int *verbose, const int *server_timeout)
{
  Cmd = STRDUP(cmd);
  if (timeout) Timeout = *timeout;
  if (verbose) Verbose = *verbose;
  if (server_timeout) Server_Timeout = *server_timeout;
}

void
sig_chld(int signo)
{
  if (signo == SIGCHLD) {
    pid_t   pid;
    int     status;
    while ( (pid = waitpid(-1, &status, WNOHANG)) > 0) {
      if (status != 0) {
	fprintf(stdout,
		"%s: child process %d terminated abnormally : status code = %d\n", 
		odb_resource_stamp_(Cmd), pid, status);
	fflush(stdout);
	if (Verbose) fprintf(stderr,
			     "%s: child process %d terminated (status=%d)\n", 
			     odb_resource_stamp_(Cmd), pid, status);
      }
    } /* while ... */
  } /* if (signo == SIGCHLD) */
  return;
}

void
sig_term(int signo)
{
  if (signo == SIGTERM) {
    fprintf(stderr,"%s: received SIGTERM\n", odb_resource_stamp_(Cmd));
    exit(signo);
  }
  return;
}

void
sig_int(int signo)
{
  if (signo == SIGINT) {
    fprintf(stderr,"%s: received SIGINT\n", odb_resource_stamp_(Cmd));
    exit(signo);
  }
  return;
}

void
sig_alrm(int signo)
{
  if (signo == SIGALRM) {
    alarm(0);
    fprintf(stderr,
	    "%s: received SIGALRM (reached timeout after %d seconds)\n", 
	    odb_resource_stamp_(Cmd), Timeout);
    exit(signo);
  }
  return;
}

void
sig_alrm_server(int signo)
{
  if (signo == SIGALRM) {
    alarm(0);
    fprintf(stderr,
	    "%s: received SIGALRM (the master server reached timeout after %d seconds)\n", 
	    odb_resource_stamp_(Cmd), Server_Timeout);
    exit(signo);
  }
  return;
}

void
sig_ignore(int signo)
{
  return;
}

#else

void dummy_odbcs_signal() { }

#endif
