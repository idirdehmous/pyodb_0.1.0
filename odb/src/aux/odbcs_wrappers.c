
/* odbcs_wrappers.c */

#include "odbcs.h"

#ifdef ODBCS

int
Accept(int fd, SA *sa, socklen_t *salenptr)
{
  int             n;
 again:
  if ( (n = accept(fd, sa, salenptr)) < 0) {
#ifdef  EPROTO
    if (errno == EPROTO || errno == ECONNABORTED)
#else
    if (errno == ECONNABORTED)
#endif
      goto again;
    else
      err_sys("accept error");
  }
  return(n);
}

void
Bind(int fd, const SA *sa, socklen_t salen)
{
  const int max_tries = 3;
  int try_count = 0;
  int sleep_secs = 10;
  
  for ( ; ; ) {
    int rc = bind(fd, sa, salen);
    if (rc < 0 && ++try_count > max_tries) {
      err_sys("bind error");
    }
    else if (rc < 0) {
      err_msg("bind failed; re-try#%d (out of %d) after %d secs",
	      try_count, max_tries, sleep_secs);
      sleep(sleep_secs);
      sleep_secs *= 2;
    }
    else
      break;
  }
}

void
Close(int fd)
{
  if (close(fd) == -1)
    err_sys("close error");
}

static char *
StartServer(const char *hostname, int port, int timeout, int verbose)
{
  static odbcs_t *conf = NULL;
  char *truehostname = NULL;
  const odbcs_t *match = NULL;
  if (!conf) conf = odbcs_conf_create();
  match = odbcs_conf_match(hostname, conf, &truehostname);
  if (match) {
    const char fmt[] =
      "/usr/bin/rsh %s -l %s "
      "'env _ARCH=%s _CPU_TYPE=%s _OBJECT_MODE=%d _ODB_VERSION=%s "
      "_ODB_ARCH=%s _ODB_DIR=%s _ODB_ROOT=%s "
      "%s/odbi_proxy -p %d -t %d"
      " %s </dev/null' &";
    char *cmd = NULL;
    int len = 1 + STRLEN(fmt);
    len += STRLEN(truehostname) + STRLEN(match->username);
    len += STRLEN(match->arch) + STRLEN(match->cpu_type) + 20 + STRLEN(match->odb_version);
    len += STRLEN(match->odb_arch) + STRLEN(match->odb_dir) + STRLEN(match->odb_root);
    len += STRLEN(match->odb_binpath) + 20 + 20;
    len += 20;
    ALLOCX(cmd,len);
    snprintf(cmd,len,fmt,
	     truehostname, match->username,
	     match->arch, match->cpu_type, match->object_mode, match->odb_version,
	     match->odb_arch, match->odb_dir, match->odb_root,
	     match->odb_binpath, port, timeout,
	     verbose ? "-v" : ">/dev/null");
    if (verbose) fprintf(stderr,"StartServer(%s, %d, %d, %d): %s\n",
			 truehostname,port,timeout,verbose,cmd);
    system(cmd);
    FREEX(cmd);
    sleep(2); /* Gives a little grace period for the server to get started ... */
  }
  else {
    err_sys("Unable to start server @ host = '%s' (port=%d,timeout=%d) via odbi_proxy. "
	    "Check your odbcs.conf file(s)\n",
	    hostname,port,timeout);
  }
  return truehostname;
}

void
Connect(int fd, SA *sa, socklen_t salen,
	const char *host, int port, int timeout, const char *hostname,
	int verbose)
{
  const int max_tries = 3;
  int try_count = 0;
  int sleep_secs = 5;
  int first_time = 1;
  char *THEhost = NULL;
  struct sockaddr_in *Servaddr = (struct sockaddr_in *)sa;
  char *ip_addr = STRDUP(host);

  if (!hostname) hostname = host;
  THEhost = STRDUP(hostname);
  
  for ( ; ; ) {
    int rc = connect(fd, sa, salen);
    if (rc < 0 && first_time) {
      char *truehostname = NULL;
      if (verbose) {
	err_msg("unable to connect (host=%s[%s], port=%d); trying to start server remotely",
		ip_addr,THEhost,port);
      }
      truehostname = StartServer(THEhost, port, timeout, verbose);
      if (truehostname) {
	FREE(THEhost);
	THEhost = truehostname;
	{
	  bzero(Servaddr, salen);
	  Servaddr->sin_family = AF_INET;
	  Servaddr->sin_port = htons(port);
	  struct hostent *h;
	  h = gethostbyname(THEhost);
	  if (h && h->h_addrtype == AF_INET) {
	    char **pptr;
	    char str[ODBCS_INET_ADDRSTRLEN];
	    const char *p = NULL;
	    pptr = h->h_addr_list;
	    for ( ; *pptr != NULL ; pptr++) {
	      p = Inet_ntop(h->h_addrtype, *pptr, str, sizeof(str));
	    }
	    if (p) {
	      FREE(ip_addr);
	      ip_addr = STRDUP(p);
	    }
	  }
	  Inet_pton(AF_INET, ip_addr, &Servaddr->sin_addr);
	}
      }
      first_time = 0;
    }
    else if (rc < 0 && ++try_count > max_tries) {
      err_sys("unable to connect (host=%s[%s], port=%d)",ip_addr,THEhost,port);
    }
    else if (rc < 0) {
      err_msg("unable to connect (host=%s[%s], port=%d); re-try#%d (out of %d) after %d secs",
	      ip_addr,THEhost,port,try_count, max_tries, sleep_secs);
      sleep(sleep_secs);
      sleep_secs *= 2;
    }
    else
      break;
  }
  FREE(ip_addr);
  FREE(THEhost);
}

void
Fclose(FILE *fp)
{
  if (fclose(fp) != 0)
    err_sys("fclose error");
}

FILE *
Fdopen(int fd, const char *mode)
{
  FILE    *fp;
  if ( (fp = fdopen(fd, mode)) == NULL)
    err_sys("fdopen error");
  return(fp);
}

char *
Fgets(char *ptr, int n, FILE *stream)
{
  char    *rptr;
  if ( (rptr = fgets(ptr, n, stream)) == NULL && ferror(stream))
    err_sys("fgets error");
  return (rptr);
}

FILE *
Fopen(const char *filename, const char *mode)
{
  FILE    *fp;
  if ( (fp = fopen(filename, mode)) == NULL)
    err_sys("fopen error");
  return(fp);
}

pid_t
Fork(void)
{
  pid_t   pid;
  if ( (pid = fork()) == -1)
    err_sys("fork error");
  return(pid);
}

void
Fputs(const char *ptr, FILE *stream)
{
  if (fputs(ptr, stream) == EOF)
    err_sys("fputs error");
}

const char *
Inet_ntop(int family, const void *addrptr, char *strptr, size_t len)
{
  const char      *ptr;
  if (strptr == NULL)             /* check for old code */
    err_quit("NULL 3rd argument to inet_ntop");
  if ( (ptr = inet_ntop(family, addrptr, strptr, len)) == NULL)
    err_sys("inet_ntop error");             /* sets errno */
  return(ptr);
}

void
Inet_pton(int family, const char *strptr, void *addrptr)
{
  int             n;
  if ( (n = inet_pton(family, strptr, addrptr)) < 0)
    err_sys("inet_pton error for %s", strptr);      /* errno set */
  else if (n == 0)
    err_quit("inet_pton error for %s", strptr);     /* errno not set */
  /* nothing to return */
}

void
Listen(int fd, int backlog)
{
  if (listen(fd, backlog) < 0)
    err_sys("listen error");
}

static int	read_cnt = 0;
static char	*read_ptr = NULL;
static char	read_buf[ODBCS_MAXLINE];

static ssize_t
my_read(int fd, char *ptr)
{
  if (read_cnt <= 0) {
  again:
    if ( (read_cnt = read(fd, read_buf, sizeof(read_buf))) < 0) {
      if (errno == EINTR)
	goto again;
      return(-1);
    } else if (read_cnt == 0)
      return(0);
    read_ptr = read_buf;
  }
  read_cnt--;
  *ptr = *read_ptr++;
  return(1);
}

static ssize_t
readline(int fd, void *vptr, size_t maxlen)
{
  ssize_t	n, rc;
  char	c, *ptr;
  ptr = vptr;
  for (n = 1; n < maxlen; n++) {
    if ( (rc = my_read(fd, &c)) == 1) {
      *ptr++ = c;
      if (c == '\n')
	break;	/* newline is stored, like fgets() */
    } else if (rc == 0) {
      *ptr = 0;
      return(n - 1);	/* EOF, n - 1 bytes were read */
    } else
      return(-1);		/* error, errno set by read() */
  }
  *ptr = 0;	/* null terminate like fgets() */
  return(n);
}

ssize_t
Readline(int fd, void *ptr, size_t maxlen)
{
  ssize_t		n = 0;
  if (ptr && maxlen > 0) {
    if ( (n = readline(fd, ptr, maxlen)) < 0)
      err_sys("readline error");
  }
  return(n);
}

ssize_t
Readline_timeo(int fd, void *ptr, size_t maxlen, int timeout)
{
  ssize_t		n = 0;
  if (ptr && maxlen > 0) {
    Sigfunc        *sigfunc;
    if (timeout > 0) {
      sigfunc = Signal(SIGALRM, sig_alrm);
      if (alarm(timeout) != 0)
	err_msg("Readline_timeo: alarm was already set");
    }
    n = Readline(fd, ptr, maxlen);
    if (timeout > 0) { /* Restore */
      alarm(0);
      Signal(SIGALRM, sigfunc);
    }
  }
  return(n);
}

static ssize_t /* Read "n" bytes from a descriptor. */
readn(int fd, void *vptr, size_t n)
{
  size_t  nleft;
  ssize_t nread;
  char    *ptr;

  ptr = vptr;
  nleft = n;
  while (nleft > 0) {
    if ( (nread = read(fd, ptr, nleft)) < 0) {
      if (errno == EINTR)
	nread = 0;              /* and call read() again */
      else
	return(-1);
    } else if (nread == 0)
      break;                          /* EOF */
    
    nleft -= nread;
    ptr   += nread;
  }
  return(n - nleft);              /* return >= 0 */
}

ssize_t
Readn(int fd, void *ptr, size_t nbytes)
{
  ssize_t         n = 0;
  if (ptr && nbytes > 0) {
    if ( (n = readn(fd, ptr, nbytes)) < 0)
      err_sys("readn error");
  }
  return(n);
}

ssize_t
Readn_timeo(int fd, void *ptr, size_t nbytes, int timeout)
{
  ssize_t         n = 0;
  if (ptr && nbytes > 0) {
    Sigfunc        *sigfunc;
    if (timeout > 0) {
      sigfunc = Signal(SIGALRM, sig_alrm);
      if (alarm(timeout) != 0)
	err_msg("Readn_timeo: alarm was already set");
    }
    n = Readn(fd, ptr, nbytes);
    if (timeout > 0) { /* Restore */
      alarm(0);
      Signal(SIGALRM, sigfunc);
    }
  }
  return(n);
}

int
Select(int nfds, 
       fd_set *readfds, fd_set *writefds, fd_set *exceptfds,
       struct timeval *timeout)
{
  int             n;
  if ( (n = select(nfds, readfds, writefds, exceptfds, timeout)) < 0)
    err_sys("select error");
  return(n);              /* can return 0 on timeout */
}

void
Shutdown(int fd, int how)
{
  if (shutdown(fd, how) < 0)
    err_sys("shutdown error");
}

#if 0
Sigfunc *
signal(int signo, Sigfunc *func)
{
  struct sigaction        act, oact;
  
  act.sa_handler = func;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  if (signo == SIGALRM) {
#ifdef  SA_INTERRUPT
    act.sa_flags |= SA_INTERRUPT;   /* SunOS 4.x */
#endif
  } else {
#ifdef  SA_RESTART
    act.sa_flags |= SA_RESTART;     /* SVR4, 44BSD */
#endif
  }
  if (sigaction(signo, &act, &oact) < 0)
    return(SIG_ERR);
  return(oact.sa_handler);
}
#endif

Sigfunc *
Signal(int signo, Sigfunc *func) /* for our signal() function */
{
  Sigfunc *sigfunc;
  if ( (sigfunc = signal(signo, func)) == SIG_ERR)
    err_sys("signal error");
  return(sigfunc);
}

int
Socket(int family, int type, int protocol)
{
  int             n;
  if ( (n = socket(family, type, protocol)) < 0)
    err_sys("socket error");
  return(n);
}

pid_t
Waitpid(pid_t pid, int *iptr, int options)
{
  pid_t   retpid;
  if ( (retpid = waitpid(pid, iptr, options)) == -1)
    err_sys("waitpid error");
  return(retpid);
}

static ssize_t /* Write "n" bytes to a descriptor. */
writen(int fd, const void *vptr, size_t n)
{
  size_t          nleft;
  ssize_t         nwritten;
  const char      *ptr;

  ptr = vptr;
  nleft = n;
  while (nleft > 0) {
    if ( (nwritten = write(fd, ptr, nleft)) <= 0) {
      if (nwritten < 0 && errno == EINTR)
	nwritten = 0;           /* and call write() again */
      else
	return(-1);                     /* error */
    }
    
    nleft -= nwritten;
    ptr   += nwritten;
  }
  return(n);
}

ssize_t
Writen(int fd, const void *ptr, size_t nbytes)
{
  ssize_t n = 0;
  if (ptr && nbytes > 0) {
    if ((n = writen(fd, ptr, nbytes)) != nbytes)
      err_sys("writen error");
  }
  return(n);
}

#else

void dummy_odbcs_wrappers() { }

#endif /* ODBCS */
