
/* odbi_server_main.c */

/* Usage: odbi_server.x -p port_number (default=mod(uid,10000)+10000 and must be between MIN_PORT=10000 & MAX_PORT=20000)
                        -n listening_queue_length (the default backlog is 64)
			-i       (ignore SIGCHLD)
			-t timeout_in_seconds (for idling/waiting input from the client; def=TIMEOUT_DEFAULT secs)
			-v       (verbose)
			-b alternate $ODB_BINPATH
			-h hostname  (to fork off a remote server)
			-T server_self_timeout_in_seconds (terminate server after this many secs elapsed; def=2 x client t/o)
*/

#include "odbcs.h"

#ifdef ODBCS

#include "odbcsdefs.h"

#define SERVER_FLAGS "b:h:in:p:t:T:v"

extern const char *odb_resource_stamp_(const char *label); /* from ../lib.codb.c */

#define PUTENV(binpath) \
   { \
     char *env = NULL; \
     int len = STRLEN("ODB_BINPATH=") + STRLEN(binpath) + 1; \
     ALLOCX(env, len); \
     snprintf(env, len, "ODB_BINPATH=%s", binpath); \
     putenv(env); \
     FREEX(env); \
   }

int 
main(int argc, char *argv[])
{
  char                *cmd = argv[0];
  /* int                  serv_port = 11998; */
  int                  serv_port = PORT_DEFAULT;
  int                  ignore_sig_chld = 0;
  int                  listenq = LISTEN_QUEUE_LENGTH_DEFAULT;
  int                  listenfd, connfd;
  int                  timeout = TIMEOUT_DEFAULT;
  int                  timeout_given = 0;
  int                  self_timeout = SERVER_SELF_TIMEOUT(TIMEOUT_DEFAULT);
  int                  self_timeout_given = 0;
  int                  verbose = 0;
  char                *binpath = NULL;
  char                *hostname = NULL;
  pid_t                childpid, pid = getpid();
  socklen_t            clilen;
  struct sockaddr_in   cliaddr, servaddr;
  char hashspid[80];
  int c, errflg = 0;

  snprintf(hashspid,sizeof(hashspid),"#s[%d]",pid);
  
  while ((c = getopt(argc, argv, SERVER_FLAGS)) != -1) {
    switch (c) {
    case 'b': /* alternate $ODB_BINPATH */
      binpath = STRDUP(optarg);
      PUTENV(binpath);
      break;
    case 'h': /* remote hostname */
      hostname = STRDUP(optarg);
      break;
    case 'i': /* ignore SIGCHLD */
      ignore_sig_chld = 0;
      break;
    case 'n': /* listening queue length (i.e. backlog) */
      listenq = atoi(optarg);
      break;
    case 'p': /* port number (validity check delayed) */
      serv_port = atoi(optarg);
      break;
    case 't': /* timeout */
      timeout = atoi(optarg);
      timeout_given = 1;
      break;
    case 'T': /* server self-timeout */
      self_timeout = atoi(optarg);
      self_timeout_given = 1;
      break;
    case 'v': /* turn verbose on */
      verbose = 1;
      break;
    default:
      errflg++;
      break;
    }
  }

  if (serv_port < MIN_PORT || serv_port > MAX_PORT) {
    fprintf(stderr,"%s: ***Error: Port number (%d) must be between %d and %d, inclusive\n",
	    hashspid, serv_port, MIN_PORT, MAX_PORT);
    errflg++;
  }

  if (errflg) {
    fprintf(stderr,"%s: ***Error: Unable to start ODBI-server '%s'\n",hashspid,cmd);
    fprintf(stderr,
	    "%s: Usage: %s [-i] [-n backlog] [-p port] [-t timeout] [-v]"
	    " [-b binpath] [-h hostname] [-T server_timeout]\n",hashspid,cmd);
    return errflg;
  }

  if (!timeout_given) {
    char *env = getenv("ODBCS_TIMEOUT");
    if (!env) timeout = env ? atoi(env) : TIMEOUT_DEFAULT;
    if (timeout <= 0) timeout = TIMEOUT_DEFAULT;
    timeout_given = 1;
  }

  {
    static char newenv[40]; /* static because of "putenv()" */
    snprintf(newenv,sizeof(newenv),"ODBCS_TIMEOUT=%d",timeout);
    putenv(newenv);
  }

  if (!self_timeout_given) self_timeout = SERVER_SELF_TIMEOUT(timeout);

  if (!binpath) {
    char *env = getenv("ODB_BINPATH");
    if (env) {
      fprintf(stdout,"%s: $ODB_BINPATH is '%s'\n",odb_resource_stamp_(hashspid),env);
      binpath = STRDUP(env);
    }
    else {
      char *cmd_copy = STRDUP(cmd);
      char *last_slash = strrchr(cmd_copy,'/');
      if (!last_slash) binpath = STRDUP(".");
      else { *last_slash = '\0'; binpath = STRDUP(cmd_copy); }
      FREE(cmd_copy);
      fprintf(stdout,"%s: Unable to resolve $ODB_BINPATH ; using '%s'",odb_resource_stamp_(hashspid),binpath);
      PUTENV(binpath);
    }
  }

  fprintf(stdout, "%s: %s %s-n %d -p %d -t %d%s -b %s%s%s -T %d\n",
	  odb_resource_stamp_(hashspid),
	  cmd, 
	  ignore_sig_chld ? "-i " : "", 
	  listenq, 
	  serv_port, 
	  timeout,
	  verbose ? " -v" : "",
	  binpath,
	  hostname ? " -h" : "",
	  hostname ? hostname : "",
	  self_timeout);

  fflush(stdout);

  if (hostname) {
    /* Client will try to use odbi_proxy to (see aux/odbcs_wrappers.c for StartServer() 
       to start server on a remote host ;
       Please note that database "label" has been put to /dev/null to denote non-existent database */
    const char fmt[] = "%s/odbi_client.x -l /dev/null -H %s -P %d -T %d%s";
    char *cmd = NULL;
    int len = STRLEN(binpath) + STRLEN(hostname) + 100;
    ALLOCX(cmd, len);
    snprintf(cmd, len, fmt, binpath, hostname, serv_port, timeout, verbose ? " -Overbose" : "");
    if (verbose) fprintf(stdout, "%s: %s\n",odb_resource_stamp_(hashspid), cmd);
    fflush(stdout);
    system(cmd);
    FREEX(cmd);
    goto finish;
  }

  listenfd = Socket(AF_INET, SOCK_STREAM, 0);

  bzero(&servaddr, sizeof(servaddr));
  servaddr.sin_family      = AF_INET;
  servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  servaddr.sin_port        = htons(serv_port);

  Bind(listenfd, (SA *) &servaddr, sizeof(servaddr));

  Listen(listenfd, listenq);

  setup_sig(hashspid, &timeout, &verbose, &self_timeout); /* For use by signal handlers */

  Signal(SIGTERM, sig_term);      /* Catch SIGTERM and perform a graceful exit */
  Signal(SIGINT , sig_int);       /* Catch SIGINT and perform a graceful exit */

  if (ignore_sig_chld) {
    Signal(SIGCHLD, SIG_IGN);      /* ignore SIGCHLD => zombies on some systems */
  }
  else {
    Signal(SIGCHLD, sig_chld);     /* must call waitpid() */
  }

  for ( ; ; ) {
    /* The master server gets killed after this many seconds */
    alarm(self_timeout);
    Signal(SIGALRM, sig_alrm_server); 

    clilen = sizeof(cliaddr);
    if ( (connfd = accept(listenfd, (SA *) &cliaddr, &clilen)) < 0) {
      if (errno == EINTR)
        continue;               /* back to for ( ; ; ) */
      else
        err_sys("accept error");
    }

    if ( (childpid = Fork()) == 0) {  /* child process */
      alarm(0);              /* reset alarm */
      Signal(SIGALRM, sig_ignore);
      Close(listenfd);       /* close listening socket */
      return odbi_server(connfd, timeout, verbose);   /* invoke the real server */
    }

    if (verbose) fprintf(stderr,"%s: child process id %d has started\n",
			 odb_resource_stamp_(hashspid),childpid);

    Close(connfd);           /* parent closes connected socket */
  } /* for ( ; ; ) */

 finish:
  return 0;
}

#else

#include <stdio.h>

int 
main(int argc, char *argv[])
{
  fprintf(stderr,
	  "***Error: odbi_server has not been compiled for this machine. Check your #define ODBCS in source code.\n");
  return 1;
}

#endif
