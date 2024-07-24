
/* odbi_server.c */

/* 
   Manages ODB client/server communication between
   client and askodb-script
*/

#include "odbcs.h"

#ifdef ODBCS

PRIVATE pid_t Pid = -1;
PRIVATE char *Server = "";

typedef struct _askodb_t {
  char *dbname;
  char *mode;
  char *datapath;
  char *poolmask;
  char *viewname;
  char *query_string;
  char *param;
  char *user;
  char *password;
  char *workdir;
  char *exe;
  char *clihost;
  char *cliaddr;
  char *binary;
  char *ncpus;
  char *outfil;
  int byteswap;
  int start_row;
  int maxrows;
  int silent;
  int verbose;
  int metadata;
  int clean;
  int io_method;
  int keep;
  int data_only;
  int bufsize;
  int timeout;
  int version;
  int odbsql;
} askodb_t;

PRIVATE askodb_t *
init_askodb_t(askodb_t *in)
{
  if (!in) CALLOC(in,1);
  if (!in->dbname)       in->dbname       = STRDUP("");
  if (!in->mode)         in->mode         = STRDUP("");
  if (!in->datapath)     in->datapath     = STRDUP("");
  if (!in->poolmask)     in->poolmask     = STRDUP("");
  if (!in->viewname)     in->viewname     = STRDUP("");
  if (!in->query_string) in->query_string = STRDUP("");
  if (!in->param)        in->param        = STRDUP("");
  if (!in->user)         in->user         = STRDUP("");
  if (!in->password)     in->password     = STRDUP("");
  if (!in->workdir)      in->workdir      = STRDUP("");
  if (!in->exe)          in->exe          = STRDUP("");
  if (!in->clihost)      in->clihost      = STRDUP("");
  if (!in->cliaddr)      in->cliaddr      = STRDUP("");
  if (!in->ncpus)        in->ncpus        = STRDUP("1");
  return in;
}

PRIVATE askodb_t *
create_askodb_t(const askodb_t *in)
{
  askodb_t *out = NULL;
  ALLOC(out,1);
  memcpy(out,in,sizeof(*out));
  if (in->dbname)       out->dbname       = STRDUP(in->dbname);
  if (in->mode)         out->mode         = STRDUP(in->mode);
  if (in->datapath)     out->datapath     = STRDUP(in->datapath);
  if (in->poolmask)     out->poolmask     = STRDUP(in->poolmask);
  if (in->viewname)     out->viewname     = STRDUP(in->viewname);
  if (in->query_string) out->query_string = STRDUP(in->query_string);
  if (in->param)        out->param        = STRDUP(in->param);
  if (in->user)         out->user         = STRDUP(in->user);
  if (in->password)     out->password     = STRDUP(in->password);
  if (in->workdir)      out->workdir      = STRDUP(in->workdir);
  if (in->exe)          out->exe          = STRDUP(in->exe);
  if (in->clihost)      out->clihost      = STRDUP(in->clihost);
  if (in->cliaddr)      out->cliaddr      = STRDUP(in->cliaddr);
  if (in->binary)       out->binary       = STRDUP(in->binary);
  if (in->ncpus)        out->ncpus        = STRDUP(in->ncpus);
  if (in->outfil)       out->outfil       = STRDUP(in->outfil);
  return out;
}

PRIVATE void
free_askodb_t(askodb_t *in, int free_all)
{
  if (in) {
    if ((free_all && in->dbname)       || STRLEN(in->dbname) == 0)       FREE(in->dbname);
    if ((free_all && in->mode)         || STRLEN(in->mode) == 0)         FREE(in->mode);
    if ((free_all && in->datapath)     || STRLEN(in->datapath) == 0)     FREE(in->datapath);
    if ((free_all && in->poolmask)     || STRLEN(in->poolmask) == 0)     FREE(in->poolmask);
    if ((free_all && in->viewname)     || STRLEN(in->viewname) == 0)     FREE(in->viewname);
    if ((free_all && in->query_string) || STRLEN(in->query_string) == 0) FREE(in->query_string);
    if ((free_all && in->param)        || STRLEN(in->param) == 0)        FREE(in->param);
    if ((free_all && in->user)         || STRLEN(in->user) == 0)         FREE(in->user);
    if ((free_all && in->password)     || STRLEN(in->password) == 0)     FREE(in->password);
    if ((free_all && in->workdir)      || STRLEN(in->workdir) == 0)      FREE(in->workdir);
    if ((free_all && in->exe)          || STRLEN(in->exe) == 0)          FREE(in->exe);
    if ((free_all && in->clihost)      || STRLEN(in->clihost) == 0)      FREE(in->clihost);
    if ((free_all && in->cliaddr)      || STRLEN(in->cliaddr) == 0)      FREE(in->cliaddr);
    if ((free_all && in->binary)       || STRLEN(in->binary) == 0)       FREE(in->binary);
    if ((free_all && in->ncpus)        || STRLEN(in->ncpus) == 0)        FREE(in->ncpus);
    if ((free_all && in->outfil)       || STRLEN(in->outfil) == 0)       FREE(in->outfil);
    if (free_all) FREE(in);
  }
}


PRIVATE askodb_t *
vparam(const askodb_t *ask, char *fmt, va_list ap)
{
  askodb_t *a = NULL;
  if (fmt) {
    static int cnt = 0;
    const char *delim = ";";
    char *token = strtok(fmt,delim);

    if (ask) {
      a = create_askodb_t(ask);
    }
    else {
      CALLOC(a,1);
    }

    while (token) {
      char *ss = STRDUP(token);
      char *t = strchr(ss,'=');
      char *lhs = ss;
      char *rhs = NULL;
      fmt = token;
	
      if (t) {
	rhs = STRDUP(t+1);
	*t = '\0'; /* affects "lhs" */
      }
	
      if (rhs) {
	if (*rhs == '%') {
	  char *s = strchr(fmt,'%');
	  if (s) {
	    int d;
	    fmt = s;
	    fmt++;
	    switch (*fmt++) {
	    case 's':
	      s = va_arg(ap, char *);
	      rhs = STRDUP(s);
	      break;
	    case 'd':
	      d = va_arg(ap, int);
	      ALLOC(rhs, 20);
	      snprintf(rhs, 20, "%d", d);
	    }
	  } /* if (*rhs == '%') */
	}
      }
	
      if (rhs) {
	if (strcaseequ(lhs,"dbname"))            a->dbname       = STRDUP(rhs);
	else if (strcaseequ(lhs,"mode"))         a->mode         = STRDUP(rhs);
	else if (strcaseequ(lhs,"datapath"))     a->datapath     = STRDUP(rhs);
	else if (strcaseequ(lhs,"poolmask"))     a->poolmask     = STRDUP(rhs);
	else if (strcaseequ(lhs,"viewname"))     a->viewname     = STRDUP(rhs);
	else if (strcaseequ(lhs,"query_string")) a->query_string = STRDUP(rhs);
	else if (strcaseequ(lhs,"user"))         a->user         = STRDUP(rhs);
	else if (strcaseequ(lhs,"password"))     a->password     = STRDUP(rhs);
	else if (strcaseequ(lhs,"workdir"))      a->workdir      = STRDUP(rhs);
	else if (strcaseequ(lhs,"exe"))          a->exe          = STRDUP(rhs);
	else if (strcaseequ(lhs,"clihost"))      a->clihost      = STRDUP(rhs);
	else if (strcaseequ(lhs,"cliaddr"))      a->cliaddr      = STRDUP(rhs);
	else if (strcaseequ(lhs,"binary"))       a->binary       = STRDUP(rhs);
	else if (strcaseequ(lhs,"ncpus"))        a->ncpus        = STRDUP(rhs);
	else if (strcaseequ(lhs,"outfil"))       a->outfil       = STRDUP(rhs);
	else if (strcaseequ(lhs,"start_row"))    a->start_row    = atoi(rhs);
	else if (strcaseequ(lhs,"maxrows"))      a->maxrows      = atoi(rhs);
	else if (strcaseequ(lhs,"silent"))       a->silent       = atoi(rhs);
	else if (strcaseequ(lhs,"verbose"))      a->verbose      = atoi(rhs);
	else if (strcaseequ(lhs,"metadata"))     a->metadata     = atoi(rhs);
	else if (strcaseequ(lhs,"clean"))        a->clean        = atoi(rhs);
	else if (strcaseequ(lhs,"io_method"))    a->io_method    = atoi(rhs);
	else if (strcaseequ(lhs,"iomethod"))     a->io_method    = atoi(rhs);
	else if (strcaseequ(lhs,"keep"))         a->keep         = atoi(rhs);
	else if (strcaseequ(lhs,"data_only"))    a->data_only    = atoi(rhs);
	else if (strcaseequ(lhs,"bufsize"))      a->bufsize      = atoi(rhs);
	else if (strcaseequ(lhs,"timeout"))      a->timeout      = atoi(rhs);
	else if (strcaseequ(lhs,"version"))      a->version      = atoi(rhs);
	else if (strcaseequ(lhs,"odbsql"))       a->odbsql       = atoi(rhs);
      }
      else {
	if (!strncaseequ(lhs,"no",2)) {
	  if (strcaseequ(lhs,"silent"))            a->silent    = 1;
	  else if (strcaseequ(lhs,"verbose"))      a->verbose   = 1;
	  else if (strcaseequ(lhs,"metadata"))     a->metadata  = 1;
	  else if (strcaseequ(lhs,"clean"))        a->clean     = 1;
	  else if (strcaseequ(lhs,"keep"))         a->keep      = 1;
	  else if (strcaseequ(lhs,"data_only"))    a->data_only = 1;
	  else if (strcaseequ(lhs,"odbsql"))       a->odbsql    = 1;
	}
	else {
	  if (strcaseequ(lhs,"nosilent"))          a->silent    = 0;
	  else if (strcaseequ(lhs,"noverbose"))    a->verbose   = 0;
	  else if (strcaseequ(lhs,"nometadata"))   a->metadata  = 0;
	  else if (strcaseequ(lhs,"noclean"))      a->clean     = 0;
	  else if (strcaseequ(lhs,"nokeep"))       a->keep      = 0;
	  else if (strcaseequ(lhs,"nodata_only"))  a->data_only = 0;
	  else if (strcaseequ(lhs,"noodbsql"))     a->odbsql    = 0;
	}
      }
      
      FREE(rhs);
      FREE(ss);

      token = strtok(NULL,delim);
    } /* while (token) */
  } /* if (fmt) */
  return a;
}

PRIVATE askodb_t *
run_askodb(const askodb_t *ask, int rawform, int dorun,
	   int sockfd, int timeout, int verbose,
	   const char *params, ...)
{
  askodb_t *a = NULL;
  if (params) {
    va_list ap;
    va_start(ap, params);
    {
      char *s, *fmt;
      int fmtlen = STRLEN(params) + 1;
      ALLOC(fmt,fmtlen);
      s = fmt;
      while (*params) {
	if (!isspace(*params)) *s++ = *params;
	params++;
      }
      *s = '\0';
      a = vparam(ask, fmt, ap);
      FREE(fmt);
    }
    va_end(ap);
  }
  else {
    a = create_askodb_t(ask);
  }

  if (!dorun) return a;

  if (a) {
    /* run askodb with the requested parameters */
    
    const char *askodb_cmd = "$ODB_BINPATH/askodb -C";
    char *cmd = NULL;
    int n, pipefd, len = STRLEN(askodb_cmd);
    FILE *fp;
    int istat = 0;
    free_askodb_t(a,0); /* Release char strings with STRLEN == 0 */
    if (a->ncpus) len += STRLEN(" -# ") + STRLEN(a->ncpus);
    if (a->binary) len += STRLEN(" -b ") + STRLEN(a->binary);
    if (a->outfil) len += STRLEN(" -O ") + STRLEN(a->outfil);
    if (a->clean) len += STRLEN(" -c");
    if (a->data_only) len += STRLEN(" -D");
    if (a->keep) len += STRLEN(" -k");
    if (!verbose || a->silent) len += STRLEN(" -s");
    if (verbose && a->verbose) len += STRLEN(" -v");
    if (a->io_method > 0 && a->io_method != 5) len += STRLEN(" -5");
    if (a->datapath) len += STRLEN(" -i ") + STRLEN(a->datapath) + 2;
    if (a->workdir) len += STRLEN(" -o ") + STRLEN(a->workdir);
    if (a->exe) len += STRLEN(" -x ") + STRLEN(a->exe);
    if (a->user) len += STRLEN(" -u ") + STRLEN(a->user);
    if (a->poolmask) len += STRLEN(" -p ") + STRLEN(a->poolmask) + 2;
    if (a->viewname) len += STRLEN(" -f ") + STRLEN(a->viewname);
    if (a->query_string) len += STRLEN(" -q ") + STRLEN(a->query_string) + 2;
    if (a->bufsize > 0)  len += STRLEN(" -B ") + 20; /* bufsize */
    if (a->metadata) len += STRLEN(" -m");
    else             len += STRLEN(" -l ") + 30; /* start_row & maxrows */
    if (a->param) len += STRLEN(" -V '") + STRLEN(a->param) + 1;
    if (a->version > 0) len += STRLEN(" -n ") + 20; /* version */ 
    if (a->odbsql == 0) len += STRLEN(" -F "); /* fast odbsql turned off */
    if (a->dbname) len += 1 + STRLEN(a->dbname);
    len++; /* for the '\0' at the end */

    ALLOC(cmd,len);
    strcpy(cmd,askodb_cmd);
    if (a->binary) {
      strcat(cmd," -b ");
      strcat(cmd,a->binary);
    }
    if (a->outfil) {
      strcat(cmd," -O ");
      strcat(cmd,a->outfil);
    }
    if (a->clean) strcat(cmd," -c");
    if (a->data_only) strcat(cmd," -D");
    if (a->keep) strcat(cmd," -k");
    if (!verbose || a->silent) strcat(cmd," -s");
    if (verbose && a->verbose) strcat(cmd," -v");
    if (a->io_method > 0 && a->io_method != 5) strcat(cmd," -5");
    if (a->datapath) {
      strcat(cmd," -i '");
      strcat(cmd,a->datapath);
      strcat(cmd,"'");
    }
    if (a->workdir) {
      strcat(cmd," -o ");
      strcat(cmd,a->workdir);
    }
    if (a->exe) {
      strcat(cmd," -x ");
      strcat(cmd,a->exe);
    }
    if (a->poolmask) {
      strcat(cmd," -p '");
      strcat(cmd,a->poolmask);
      strcat(cmd,"'");
    }
    if (a->user) {
      strcat(cmd," -u ");
      strcat(cmd,a->user);
    }
    if (a->viewname) {
      strcat(cmd," -f ");
      strcat(cmd,a->viewname);
    }
    if (a->query_string) {
      strcat(cmd," -q '");
      strcat(cmd,a->query_string);
      strcat(cmd,"'");
    }
    if (a->bufsize > 0)  {
      int curlen = STRLEN(cmd);
      char *pcmd = &cmd[curlen];
      snprintf(pcmd,len-curlen," -B %d",a->bufsize);
    }
    if (a->metadata) {
      strcat(cmd," -m");
    }
    else {
      int curlen = STRLEN(cmd);
      char *pcmd = &cmd[curlen];
      if (a->start_row > 1) {
	snprintf(pcmd,len-curlen," -l %d,%d",a->start_row,a->maxrows);
      } else {
	snprintf(pcmd,len-curlen," -l %d",a->maxrows);
      }
    }
    if (a->param) {
      strcat(cmd," -V '");
      strcat(cmd,a->param);
      strcat(cmd,"'");
    }
    if (a->version > 0) {
      int curlen = STRLEN(cmd);
      char *pcmd = &cmd[curlen];
      snprintf(pcmd,len-curlen," -n %d",a->version);
    }
    if (a->odbsql == 0) {
      strcat(cmd," -F");
    }
    if (a->dbname) {
      strcat(cmd," ");
      strcat(cmd,a->dbname);
    }

    /* run askodb and pipe stdout result back to socket, chunk-by-chunk */
    
    fprintf(stdout,"%s : \"%s\"\n",odb_resource_stamp_(Server),cmd);
    fflush(stdout);
    fp = popen(cmd,"r");
    pipefd = fileno(fp);
    if (verbose) fprintf(stderr,"%s: pipe fp=%p, fd=%d\n",Server,fp,pipefd);
    if (fp) {
      int errno_save = 0;
      char buf[ODBCS_MAXLINE];
      int first_time = 1;
      errno = 0;
      while ( (n = Readn_timeo(pipefd, buf, sizeof(buf), timeout)) > 0) {
	if (!rawform || first_time) {
	  Writen(sockfd, &n, sizeof(n));
	  if (verbose) fprintf(stderr,"%s: written %slength = %d\n",
			       Server,rawform ? "INITIAL " : "", n);
	}
	first_time = 0;
	Writen(sockfd, buf, n);
	if (verbose) {
	  fprintf(stderr,"%s: echoed from pipe to client: %d bytes\n",Server,n);
	  fflush(stderr);
	}
      }
      if (verbose) fprintf(stderr,"%s: Last n = %d\n",Server,n);
      if (n == 0) errno = 0;
      istat = pclose(fp);
      errno_save = errno;
      if (verbose) fprintf(stderr,"%s: pclose status = %d, errno = %d\n",Server,istat,errno_save);
      if (istat != 0 && errno_save == 0) istat = 0; /* Since nothing bad, I presume */
      if (istat != 0 || errno_save != 0) {
	FILE *errout = (istat != 0) ? stdout : stderr;
	char *errmsg = strerror(errno_save);
	fprintf(errout,
		"%s : status = %d [errno=%d: %s] : %s error(s) in \"%s\"\n",
		odb_resource_stamp_(Server),istat,errno_save,errmsg,
		(istat != 0) ? "Unidentified" : "Watch these",
		cmd);
	fflush(errout);
      }
    }
    else { /* failed to popen() altogether */
      fprintf(stdout,"%s : popen() failed for \"%s\"\n",odb_resource_stamp_(Server),cmd);
      fflush(stdout);
      istat = -1;
    }
    if (istat > 0) istat = -istat;
    n = (istat == 0) ? 0 : istat; /* An end of message : length == 0 or istat-error (< 0)*/
    Writen(sockfd, &n, sizeof(n));
    if (verbose) fprintf(stderr,"%s: written EOF-length/istat = %d\n",Server,n);

    /* clean up */

    FREE(cmd);
  }

  free_askodb_t(a,1);
  return NULL;
}

PRIVATE char *
read_str(const char *what, int sockfd, int timeout, int verbose, int byteswap)
{
  const int one = 1;
  int n, nbytes;
  char *buf = NULL;
  nbytes = Readn_timeo(sockfd, &n, sizeof(n), timeout);
  if (nbytes != sizeof(n))
    err_sys("read_str(%s;%s): unable to read length info (expected %d, got %d bytes)",
	    Server, what, sizeof(n), nbytes);
  if (byteswap) swap4bytes_(&n, &one);
  if (verbose) fprintf(stderr,"%s;%s: obtained length = %d\n",Server,what,n);
  ALLOC(buf, n + 1);
  nbytes = Readn_timeo(sockfd, buf, n, timeout);
  if (nbytes != n)
    err_sys("read_str(%s;%s): unable to read info from client (expected %d, got %d bytes)",
	    Server, what, n, nbytes);
  buf[n] = '\0';
  if (verbose) fprintf(stderr,"%s;%s: obtained info from client, %d bytes : %s\n",Server,what,n,buf);
  return buf;
}


PUBLIC int
odbi_server(int sockfd, int timeout, int verbose)
{
  int rc = 0;
  ssize_t    n;
  const unsigned int expected_magic_word = ODBI;
  unsigned int magic_word;
  askodb_t *a = NULL;
  int array[ODBCS_INFO];
  int connect_count = 0;
  int server_version = 0;

  memset(array,0,sizeof(array));

  Pid = getpid();
  ALLOC(Server,100);
  snprintf(Server,100,">s[%d]",Pid);

  a = init_askodb_t(NULL);
  a->silent = verbose ? 0 : 1;
  a->verbose = verbose;
  a->timeout = timeout;
  (void) codb_versions_(NULL, NULL, &server_version, NULL);
  a->version = server_version;
  a->odbsql = 1; /* By default *do use* the fast odbsql -approach (no compilations) */

  Signal(SIGCHLD,SIG_DFL);
  /* Signal(SIGCHLD,sig_chld); */
  Signal(SIGPIPE,SIG_IGN);

  /* Initial handshake */

  n = Readn_timeo(sockfd, &magic_word, sizeof(magic_word), timeout);
  if (n != sizeof(magic_word))
    err_sys("%s: initial handshake between client & server has failed",Server);

  a->byteswap = (magic_word != expected_magic_word);

  if (verbose) fprintf(stderr,"%s: byteswap = %d (expected=%u, got=%u)\n", 
		       Server, a->byteswap, expected_magic_word, magic_word);

  array[0] = expected_magic_word;
  array[1] = timeout;
  array[2] = verbose;
  array[3] = server_version;

  Writen(sockfd, array, sizeof(array));

  for ( ; ; ) { /* Loop until client is finished */
    const int one = 1;
    const int two = 2;
    const int three = 3;
    int limits[3];
    int nbytes, tag = -1;
    double value;

    nbytes = Readn_timeo(sockfd, &tag, sizeof(tag), timeout);
    if (nbytes != sizeof(tag))
      err_sys("odbi_server(%s): unable to read tag (expected %d, got %d bytes)",
	      Server, sizeof(tag), nbytes);

    if (a->byteswap) swap4bytes_(&tag, &one);

    if (verbose) fprintf(stderr,"%s: obtained tag = %d (%s)\n",
			 Server, tag,
			 (tag >= 0 && tag < ODBCS_MAXTAGS) ? odbcs_tagname[tag] : "????");
    
    switch (tag) {

    case ODBCS_TAG_CONNECT:
      if (connect_count > 0) break; /* allow only one connect/session */
      {
	askodb_t *asave = a;
	char *params = read_str("connect_data",sockfd,timeout,verbose,a->byteswap);
	a = run_askodb(asave, 0, 0,
		       sockfd,timeout,verbose,
		       params);
	FREE(params);
	free_askodb_t(asave,0);
	FREE(asave);
      }

      fprintf(stdout,
	      "%s : Connect user=%s@%s;ip_addr=%s;pw=%s;db=%s;mode=%s;datapath=%s;"
	      "workdir=%s;poolmask=%s;exe=%s;version=%d;odbsql=%d;timeout=%d\n",
	      odb_resource_stamp_(Server),
	      a->user, a->clihost, a->cliaddr,
	      a->password, a->dbname, a->mode, a->datapath, a->workdir,
	      a->poolmask, a->exe, a->version, a->odbsql, a->timeout);
      fflush(stdout);

      /* server imposed restrictions for defaults */
      if (!verbose) a->verbose = 0;
      if (!verbose) a->silent = 1;
      if (a->timeout > timeout) a->timeout = timeout;

      run_askodb(a, 0, 1,
		 sockfd,timeout,verbose,
		 "nometadata; maxrows=0; viewname=@");

      a->clean = 0; /* prevents further re-clean/re-compiles in this run */
      connect_count++;
      break;

    case ODBCS_TAG_DB_METADATA:
      run_askodb(a, 0, 1,
		 sockfd,timeout,verbose,
		 "metadata");
      break;

    case ODBCS_TAG_PREPARE_QUERY:
      FREE(a->viewname);
      a->viewname = read_str("viewname",sockfd,timeout,verbose,a->byteswap);
      FREE(a->query_string);
      a->query_string = read_str("query_string",sockfd,timeout,verbose,a->byteswap);
      if (STRLEN(a->query_string) == 0 || strstr(a->viewname,".so")) {
	FREE(a->query_string);
      }

      run_askodb(a, 0, 1,
		 sockfd,timeout,verbose,
		 "maxrows=0; nometadata");
      break;

    case ODBCS_TAG_FETCHFILE:
    case ODBCS_TAG_EXECUTE_QUERY:
      nbytes = Readn_timeo(sockfd, limits, sizeof(limits), timeout);
      if (nbytes != sizeof(limits))
	err_sys("odbi_server(%s): unable to read limits (expected %d, got %d bytes)",
		Server, sizeof(limits), nbytes);
      if (a->byteswap) swap4bytes_(limits, &three);

      a->start_row = limits[0];
      a->maxrows = limits[1];
      a->bufsize = limits[2];

      if (verbose)
	fprintf(stderr,"%s: limits are now as follows: start_row=%d, maxrows=%d, bufsize=%d\n",
		Server, a->start_row, a->maxrows, a->bufsize);
	
      if (tag == ODBCS_TAG_FETCHFILE) {

	FREE(a->outfil);
	a->outfil = read_str("outfil",sockfd,timeout,verbose,a->byteswap);
	FREE(a->binary);
	a->binary = read_str("binary",sockfd,timeout,verbose,a->byteswap);
	
	if (verbose)
	  fprintf(stderr,"%s: Fetching file='%s', format='%s'\n",
		  Server, a->outfil, a->binary);
	
	run_askodb(a, 1, 1,
		   sockfd,timeout,verbose,
		   "nometadata; data_only;");

      } 
      else if (tag == ODBCS_TAG_EXECUTE_QUERY) {
	run_askodb(a, 1, 1,
		   sockfd,timeout,verbose,
		   "nometadata; data_only; binary=%s",
		   a->binary ? a->binary : "binary");
      }
      break;

    case ODBCS_TAG_BIND_PARAM:
      {
	int len;
	char *c;
	char *new_param = read_str("param",sockfd,timeout,verbose,a->byteswap);
	nbytes = Readn_timeo(sockfd, &value, sizeof(value), timeout);
	if (nbytes != sizeof(value))
	  err_sys("odbi_server(%s): unable to read bind_param() value (expected %d, got %d bytes)",
		  Server, sizeof(value), nbytes);
	if (a->byteswap) swap8bytes_(&value, &one);
	if (!a->param) a->param = STRDUP("");
	len = STRLEN(a->param) + 1 + STRLEN(new_param) + 50;
	REALLOC(a->param,len);
	c = &a->param[STRLEN(a->param)];
	snprintf(c,len,";%s=%.14g",new_param,value);
	FREE(new_param);
      }
      break;

    case ODBCS_TAG_BYEBYE:
      fprintf(stdout,
	      "%s : Bye,bye user=%s@%s;ip_addr=%s;pw=%s;db=%s;mode=%s;datapath=%s;"
	      "workdir=%s;poolmask=%s;exe=%s;version=%d;odbsql=%d;timeout=%d\n",
	      odb_resource_stamp_(Server),
	      a->user, a->clihost, a->cliaddr,
	      a->password, a->dbname, a->mode, a->datapath, a->workdir,
	      a->poolmask, a->exe, a->version, a->odbsql, a->timeout);
      fflush(stdout);
      /* Finished */
      goto finish;

    case ODBCS_TAG_TIMEOUT:
      {
	int new_timeout = timeout;
	nbytes = Readn_timeo(sockfd, &new_timeout, sizeof(new_timeout), timeout);
	if (nbytes != sizeof(new_timeout))
	  err_sys("odbi_server(%s): unable to read new timeout value (expected %d, got %d bytes)",
		  Server, sizeof(new_timeout), nbytes);
	if (a->byteswap) swap4bytes_(&new_timeout, &one);
	if (new_timeout > 0) {
	  int old_timeout = timeout;
	  a->timeout = timeout = new_timeout;
	  if (verbose)
	    fprintf(stderr,"%s: new timeout is now %d secs (was %d)\n",
		    Server, a->timeout, old_timeout);
	}
      }
      break;

    case ODBCS_TAG_VERBOSE:
      {
	int new_verbose = verbose;
	nbytes = Readn_timeo(sockfd, &new_verbose, sizeof(new_verbose), timeout);
	if (nbytes != sizeof(new_verbose))
	  err_sys("odbi_server(%s): unable to read new verbose value (expected %d, got %d bytes)",
		  Server, sizeof(new_verbose), nbytes);
	if (a->byteswap) swap4bytes_(&new_verbose, &one);
	if (new_verbose == 0 || new_verbose == 1) {
	  int old_verbose = verbose;
	  a->verbose = verbose = new_verbose;
	  a->silent = verbose ? 0 : 1;
	  if (verbose || old_verbose)
	    fprintf(stderr,"%s: verbose is now turned %s\n",
		    Server, a->verbose ? "ON" : "OFF");
	}
      }
      break;

    default:
      fprintf(stderr,"%s: Unrecognized tag '%d'\n", Server, tag);
      rc = 1;
      goto finish;
    } /* switch (tag) */
  } /* for ( ; ; ) */

 finish:
  return rc;
}

#else

void dummy_odbi_server() { }

#endif

