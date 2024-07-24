
/* odbi_client.c */

/* 

   Contains the same functions as in ODBI_direct.c,
   but these are communicating with ODBI_server.c
   to exchange data with the "askodb", that is run by this server.
   The "askodb" in turn runs ODBI_direct.c locally on server's 
   -machine !!

   Advantage: direct & client -interface identical, and thus
   you can choose between static linkage or client/server
   model without changing your application.

   In addition client is the only one at the moment who can 
   also supply SQL's dynamically (on-the-fly) !!

*/

#define ODBI_CLIENT 1

#include "odbcs.h"

#ifdef ODBCS

#include "odb.h"

#include "odbi.h"

#include "pcma_extern.h"
#include "magicwords.h"
#include "cdrhook.h"

#include "odbi_struct.h"
#include "odbcsdefs.h"

#define DBCLI db->cli
#define QDB   q->db

typedef struct _db_chain_t {
  ODBI_db_t *db;
  struct _db_chain_t *next;
} db_chain_t;

static db_chain_t *dbch = NULL;

/* Local subroutines */

PRIVATE void
atexit_db_chain()
{
  db_chain_t *pdbch = dbch;
  while (pdbch) {
    if (pdbch->db) {
      /* This db-ptr has been forgotten to release */
      (int) ODBI_disconnect(pdbch->db);
    }
    pdbch = pdbch->next;
  } /* while (pdbch) */
}

PRIVATE void
add_db_chain(ODBI_db_t *db)
{
  if (db) {
    db_chain_t *pdbch = NULL;
    if (!dbch) {
      CALLOC(dbch, 1);
      /* The following should stop child process errors
	 whilst user has forgotten to call ODBI_disconnect */
      atexit(atexit_db_chain);
    }
    pdbch = dbch;
    for (;;) {
      if (!pdbch->db) {
	pdbch->db = db;
	break;
      }
      else {
	if (!pdbch->next) CALLOC(pdbch->next, 1);
	pdbch = pdbch->next;
      }
    } /* for (;;) */
  }
}

PRIVATE int
rm_db_chain(ODBI_db_t *db)
{
  int rc = 0;
  if (db) {
    db_chain_t *pdbch = dbch;
    while (pdbch) {
      if (pdbch->db == db) {
	pdbch->db = NULL;
	rc = 1;
	break;
      }
      pdbch = pdbch->next;
    } /* while (pdbch) */
  }
  return rc;
}

PRIVATE int
is_ipv4_addr(const char *ip_addr)
{
  int rc;
  int n1 = -1, n2 = -1, n3 = -1, n4 = -1;
  int n = sscanf(ip_addr,"%d.%d.%d.%d", &n1, &n2, &n3, &n4);
#if 0
  fprintf(stderr,
	  "client: is_ipv4_addr(%s) : n=%d, n1=%d, n2=%d, n3=%d, n4=%d\n",
	  ip_addr, n, n1, n2, n3, n4);
#endif
  rc = (n == 4
	&& n1 >= 0 && n1 <= 255
	&& n2 >= 0 && n2 <= 255
	&& n3 >= 0 && n3 <= 255
	&& n4 >= 0 && n4 <= 255);
  return rc;
}

PRIVATE int 
find_int(const char *s, 
	 const char *substr,
	 int *found)
{
  int rc = 0;
  if (found) *found = 0;
  if (s && substr) {
    char *p = strstr(s,substr);
    if (p) {
      p += STRLEN(substr);
      rc = atoi(p);
      if (found) *found = 1;
    }
  }
  return rc;
}

PRIVATE char *
find_str(const char *s, 
	 const char *substr,
	 int *found)
{
  char *x = NULL;
  if (found) *found = 0;
  if (s && substr) {
    char *p = strstr(s,substr);
    if (p) {
      char *nl;
      p += STRLEN(substr);
      nl = strchr(p,'\n');
      if (nl) {
	int len = nl - p;
	ALLOC(x,len+1);
	strncpy(x,p,len);
	x[len] = '\0';
      }
      else {
	x = STRDUP(p);
      }
      if (found) *found = 1;
    }
  }
  return x;
}

PRIVATE char *
find_strnum(const char *s, 
	    const char *substr0,
	    int number,
	    int *found)
{
  char *x = NULL;
  if (found) *found = 0;
  if (s && substr0 && number >= 0) {
    char *p = NULL;
    int lensubstr = STRLEN(substr0) + 20;
    char *substr;
    ALLOC(substr,lensubstr);
    sprintf(substr,"%s%d=",substr0,number);
    p = strstr(s,substr);
    if (p) {
      char *nl;
      p += STRLEN(substr);
      nl = strchr(p,'\n');
      if (nl) {
	int len = nl - p;
	ALLOC(x,len+1);
	strncpy(x,p,len);
	x[len] = '\0';
      }
      else {
	x = STRDUP(p);
      }
      if (found) *found = 1;
    }
    FREE(substr);
  }
  return x;
}

PUBLIC void 
Vparam(void *v, 
       int is_a_query,
       char *fmt, 
       va_list ap)
{
  ODBI_db_t *db = NULL;
  ODBI_query_t *q = NULL;

  if (is_a_query) q  = v;
  else            db = v;
  
#if 0
  fprintf(stderr,"Vparam(in): is_a_query=%d, v=%p, fmt=[%s]\n",
	  is_a_query, v, fmt ? fmt : NIL);
#endif

  if (v && fmt) {
    const char *delim = ";";
    char *token = strtok(fmt,delim);
    
    while (token) {
      char *ss = STRDUP(token);
      char *t = strchr(ss,'=');
      char *lhs = ss;
      union {
	char *s;
	int ii;
	double dd;
	FILE *fp;
      } rhs;
      int free_rhss = 0;
      int do_process = 0;

      fmt = token;
	
      if (t) {
	rhs.s = STRDUP(t+1);
	do_process = 1;
	*t = '\0'; /* affects "lhs" */
      }
	
      if (do_process) {
	do_process = 0;
	if (*rhs.s == '%') {
	  char *s = strchr(fmt,'%');
	  if (s) {
	    fmt = s;
	    fmt++;
	    switch (*fmt++) {
	    case 's':
	      FREE(rhs.s);
	      s = va_arg(ap, char *);
	      rhs.s = STRDUP(s);
	      /* fprintf(stderr,"Vparam(at %%s): '%s'\n",rhs.s); */
	      free_rhss = 1;
	      do_process = 1;
	      break;
	    case 'd':
	      FREE(rhs.s);
	      rhs.ii = va_arg(ap, int);
	      /* fprintf(stderr,"Vparam(at %%f): %d\n",rhs.ii); */
	      free_rhss = 0;
	      do_process = 1;
	      break;
	    case 'f':
	      FREE(rhs.s);
	      rhs.dd = va_arg(ap, double);
	      /* fprintf(stderr,"Vparam(at %%f): %g\n",rhs.dd); */
	      free_rhss = 0;
	      do_process = 1;
	      break;
	    case 'F':
	      FREE(rhs.s);
	      rhs.fp = va_arg(ap, FILE *);
	      /* fprintf(stderr,"Vparam(at %%F): %p, fileno=%d\n",rhs.fp,fileno(rhs.fp)); */
	      free_rhss = 0;
	      do_process = 1;
	      break;
	    }
	  } /* if (*rhs.s == '%') */
	}
      }
	
      if (do_process) {
	if (db) {
	  if (DBCLI) {
	    if      (strcaseequ(lhs,"host"))       DBCLI->host       = STRDUP(rhs.s);
	    else if (strcaseequ(lhs,"port"))       DBCLI->port       = rhs.ii;
	    else if (strcaseequ(lhs,"timeout"))    DBCLI->timeout    = rhs.ii;
	    else if (strcaseequ(lhs,"datapath"))   DBCLI->datapath   = STRDUP(rhs.s);
	    else if (strcaseequ(lhs,"poolmask"))   DBCLI->poolmask   = STRDUP(rhs.s);
	    else if (strcaseequ(lhs,"user"))       DBCLI->user       = STRDUP(rhs.s);
	    else if (strcaseequ(lhs,"clihost"))    DBCLI->clihost    = STRDUP(rhs.s);
	    else if (strcaseequ(lhs,"cliaddr"))    DBCLI->cliaddr    = STRDUP(rhs.s);
	    else if (strcaseequ(lhs,"silent"))     DBCLI->silent     = rhs.ii;
	    else if (strcaseequ(lhs,"verbose"))    DBCLI->verbose    = rhs.ii;
	    else if (strcaseequ(lhs,"metadata"))   DBCLI->metadata   = rhs.ii;
	    else if (strcaseequ(lhs,"clean"))      DBCLI->clean      = rhs.ii;
	    else if (strcaseequ(lhs,"keep"))       DBCLI->keep       = rhs.ii;
	    else if (strcaseequ(lhs,"bufsize"))    DBCLI->bufsize    = rhs.ii;
	    else if (strcaseequ(lhs,"odbsql"))     DBCLI->odbsql     = rhs.ii;
	  }
	  if (strcaseequ(lhs,"io_method"))       db->io_method     = rhs.ii;
	  else if (strcaseequ(lhs,"iomethod"))   db->io_method     = rhs.ii;
	  else if (strcaseequ(lhs,"fp"))         db->fp            = rhs.fp;
	}
      }
      else {
	if (db) {
	  if (DBCLI) {
	    if (strcaseequ(lhs,"silent"))          DBCLI->silent   = 1;
	    else if (strcaseequ(lhs,"verbose"))    DBCLI->verbose  = 1;
	    else if (strcaseequ(lhs,"metadata"))   DBCLI->metadata = 1;
	    else if (strcaseequ(lhs,"clean"))      DBCLI->clean    = 1;
	    else if (strcaseequ(lhs,"keep"))       DBCLI->keep     = 1;
	    else if (strcaseequ(lhs,"odbsql"))     DBCLI->odbsql   = 1;
	    if (strcaseequ(lhs,"nosilent"))        DBCLI->silent   = 0;
	    else if (strcaseequ(lhs,"noverbose"))  DBCLI->verbose  = 0;
	    else if (strcaseequ(lhs,"nometadata")) DBCLI->metadata = 0;
	    else if (strcaseequ(lhs,"noclean"))    DBCLI->clean    = 0;
	    else if (strcaseequ(lhs,"nokeep"))     DBCLI->keep     = 0;
	    else if (strcaseequ(lhs,"noodbsql"))   DBCLI->odbsql   = 0;
	  }
	}
      }
      
      if (free_rhss) FREE(rhs.s);
      FREE(ss);

      token = strtok(NULL,delim);
    } /* while (token) */
  } /* if (v && fmt) */
#if 0
  fprintf(stderr,"Vparam(out)\n");
#endif
}


PRIVATE char *
echo(int *retcode,
     const char *what,
     int infd, int outfd, 
     int timeout, int verbose, int byteswap, 
     int *Outlen)
{
  int nbytes, n;
  char *outbuf = NULL;
  int outlen = 0;
  int yksi = 1;
  if (Outlen) *Outlen = 0;
  if (retcode) *retcode = 0;
  if (verbose) fprintf(stderr,"client: now expecting '%s'\n",what);
  while ( (nbytes = Readn_timeo(infd, &n, sizeof(n), timeout)) > 0) {
    char *buf = NULL;
    char *whatmsg = "";
    const int one = 1;
    if (byteswap) swap4bytes_(&n, &one);
    if (n == 0) whatmsg = " (EOM)";
    else if (n < 0) whatmsg = " (FAILURE)";
    if (verbose) fprintf(stderr,"client(%s): read length = %d%s\n",
			 what, n, whatmsg);
    if (n == 0) break; /* end of message */
    if (n < 0) { /* popen() has failed or pclose() detected an error */
      int istat = n;
      err_msg("client: Server has detected a problem : status code = %d",istat);
      if (retcode) *retcode = istat;
      if (outbuf) FREE(outbuf);
      if (Outlen) *Outlen = 0;
      goto finish;
    }
    ALLOC(buf,n);
    nbytes = Readn_timeo(infd, buf, n, timeout);
    if (verbose) fprintf(stderr,"client(%s): read from socket, %d bytes\n",
			 what,n);
    if (Outlen) {
      REALLOC(outbuf, outlen + n + yksi);
      yksi = 0;
      memcpy(outbuf+outlen,buf,n);
      outlen += n;
    }
    if (outfd >= 0) {
      Writen(outfd, buf, n);
    }
    FREE(buf);
  }
  if (Outlen) {
    *Outlen = outlen;
    if (outlen > 0) outbuf[outlen] = '\0'; /* due to "yksi" */
  }
 finish:
  return outbuf;
}

/* Maximum name length (can be *INCREASED* via ODBI_maxnamelen(newlength)) */
PRIVATE int max_name_len = 64;
/***************************************************************************/
/*                                 PUBLIC                                  */
/***************************************************************************/
int ODBI_maxnamelen(int new_len)
{
  int old_len = max_name_len;
  DRHOOK_START(ODBI_maxnamelen);
  if (new_len >= max_name_len) max_name_len = new_len;
  DRHOOK_END(0);
  return old_len;
}

/* Max int */
#define ODBI_MAX_INT 2147483647

/* For db->poolstat[] */
#define ODBI_POOL_UNLOADED   0
#define ODBI_POOL_LOADED     1
#define ODBI_POOL_RELEASED   2


void ODBI_print_db_metadata(FILE *fp, 
			    void *vdb,
			    int complete_info)
{
  ODBI_db_t *db = vdb;
  DRHOOK_START(ODBI_print_db_metadata);
  
  if (db && complete_info) { 
    int retcode = 0;
    /* Database metadata (only complete info returned) */
    /* ODBI_connect already returns partial metadata */
    
    int tag = ODBCS_TAG_DB_METADATA;
    
    if (!fp) fp = db->fp;
    
    Writen(DBCLI->sockfd, &tag, sizeof(tag));
    if (DBCLI->verbose) fprintf(stderr,"client: written tag = %d (%s)\n",tag,odbcs_tagname[tag]);
    
    /* now reading from socket */
    echo(&retcode, odbcs_tagname[tag],
	 DBCLI->sockfd, fileno(fp), 
	 DBCLI->timeout, DBCLI->verbose, db->swapbytes, NULL);
  } /* if (db) */
  
  DRHOOK_END(0);
}


void ODBI_print_query_metadata(FILE *fp, 
			       void *vq)
{
  ODBI_query_t *q = vq;
  DRHOOK_START(ODBI_print_query_metadata);
  
  if (q) { /* Query metadata */
    /* not implemented yet */
  } /* if (q) */
  
  DRHOOK_END(0);
}


void *ODBI_connect(const char *dbname,
                   const char *params,
                   ...)
{
  FILE *fp = NULL;
  ODBI_db_t *db = NULL;
  DRHOOK_START(ODBI_connect);
  
  if (dbname) { /* Skip leading whitespace */
    while (*dbname && isspace(*dbname)) dbname++;
  }
  
  if (STRLEN(dbname) > 0) {
    int db_is_devnull = strequ(dbname,"/dev/null");
    char                *pdbname = STRDUP(dbname);
    /* int                  serv_port = 11998; */
    int                  serv_port = PORT_DEFAULT;
    /* int                  timeout = 600; */
    int                  timeout = TIMEOUT_DEFAULT;
    int                  timeout_env = 0;
    int                  timeout_changed = 0;
    int                  verbose = 0;
    int                  verbose_env = 0;
    int                  verbose_changed = 0;
    struct sockaddr_in   servaddr;
    const char          *IP_addr = "127.0.0.1"; /* i.e. localhost */
    int n;
    const unsigned int expected_magic_word = ODBI;
    unsigned int magic_word;
    int info[ODBCS_INFO];
    int datapath_found = 0;
    int server_version = 0;
    const int ref1_server_version = 323515; /* The version on which the new ODBCS_TAGs verbose & timeout became available */
    
    CALLOC(db,1);
    CALLOC(DBCLI,1);
    
    DBCLI->silent = 1;
    
    {
      char *env = getenv("ODBCS_TIMEOUT");
      if (env) {
	timeout_env = atoi(env);
	timeout_changed = 1;
      }
      else {
	timeout_env = TIMEOUT_DEFAULT;
	timeout_changed = 0;
      }
      if (timeout_env <= 0) {
	timeout_env = TIMEOUT_DEFAULT;
	timeout_changed = 0;
      }
    }
    DBCLI->timeout = timeout = timeout_env;

    {
      char *env = getenv("ODBCS_VERBOSE");
      if (env) {
	verbose_env = atoi(env);
	verbose_changed = 1;
      }
      else {
	verbose_env = 0;
	verbose_changed = 0;
      }
      if (verbose_env != 0 && verbose_env != 1) {
	verbose_env = 0;
	verbose_changed = 0;
      }
    }
    DBCLI->verbose = verbose = verbose_env;

    (void) codb_versions_(NULL, NULL, &DBCLI->version, NULL);
    DBCLI->odbsql = 1; /* By default *do use* fast odbsql -approach (no compilations) */

    if (params) {
      va_list ap;
      va_start(ap, params);
#if 0
      fprintf(stderr,"client: params='%s'\n",params);
#endif
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
	Vparam(db, 0, fmt, ap);
	FREE(fmt);
      }
      va_end(ap);
    }
    fp = db->fp;

    if (!datapath_found)
    {
      /* check whether dbname explicitly begins with "odb://", in which case
	 chase for "odb://host/datapath/DBNAME" or "odb://host:port/datapath/DBNAME" */

#if 0
      fprintf(stderr,"odb://>pdbname='%s'\n",pdbname);
      fprintf(stderr,"odb://>DBCLI->host='%s'\n",DBCLI->host ? DBCLI->host : NIL);
      fprintf(stderr,"odb://>DBCLI->datapath='%s'\n",DBCLI->datapath ? DBCLI->datapath : NIL);
      fprintf(stderr,"odb://>DBCLI->port=%d\n",DBCLI->port);
#endif
      if (strncaseequ(pdbname,"odb://",6)) {
	char *p = STRDUP(pdbname+6);
	char *colon = strchr(p,':');
	char *first_slash = strchr(p,'/');
	char *last_slash = strrchr(p,'/');
	if (colon) {
	  *colon = '\0';
	  DBCLI->port = atoi(colon+1);
	  FREE(DBCLI->host);
	  DBCLI->host = STRDUP(p);
	}
	if (last_slash) { /* xxx/path/xxx/DBNAME */
	  FREE(pdbname);
	  pdbname = STRDUP(last_slash+1);
	  *last_slash = '\0';
	}
	if (first_slash) {
	  FREE(DBCLI->datapath);
	  DBCLI->datapath = STRDUP(first_slash);
   
	  if (!colon) {
	    *first_slash = '\0';
	    FREE(DBCLI->host);
	    DBCLI->host = STRDUP(p);
	  }
	  datapath_found = 1;
	}
	FREE(p);
      } /* if (strncaseequ(pdbname,"odb://",6)) */
#if 0
      fprintf(stderr,"odb://<pdbname='%s'\n",pdbname);
      fprintf(stderr,"odb://<DBCLI->host='%s'\n",DBCLI->host ? DBCLI->host : NIL);
      fprintf(stderr,"odb://<DBCLI->datapath='%s'\n",DBCLI->datapath ? DBCLI->datapath : NIL);
      fprintf(stderr,"odb://<DBCLI->port=%d\n",DBCLI->port);
#endif
    }

    if (!datapath_found)
    {
      /* check whether dbname explicitly contains host and/or datapath + dbname
	 i.e. "rcp"-format : host:/datapath and DBNAME is guessed from datapath
	 Use odb://host:port/datapath/DBNAME if you want to supply port-number or DBNAME */

      char *p = STRDUP(pdbname);
      char *colon = strchr(p,':');
      char *last_slash = strrchr(p,'/');
#if 0
      fprintf(stderr,">pdbname='%s'\n",pdbname);
      fprintf(stderr,">DBCLI->host='%s'\n",DBCLI->host ? DBCLI->host : NIL);
      fprintf(stderr,">DBCLI->datapath='%s'\n",DBCLI->datapath ? DBCLI->datapath : NIL);
      fprintf(stderr,"p='%s'\n",p);
#endif
      if (last_slash) { /* xxx/path/xxx  and xxx is DBNAME */
	char *dot;
	FREE(pdbname);
	pdbname = STRDUP(last_slash+1);
/*AF */
    dot = strchr(pdbname,'.'); /* xxx can be DBNAME.* */
        if (dot)
	  *dot = '\0';       /* remove .* if xxx is DBNAME.* */

/* AF
	*last_slash = '\0';
     if dbname cannot be extracted from dbname input then choose option odb://
*/
      }
      if (colon) {
	*colon = '\0';
	FREE(DBCLI->host);
	DBCLI->host = STRDUP(p);  /* host */
      }
      if (last_slash) {
	FREE(DBCLI->datapath);
	if (colon) {
	  DBCLI->datapath = STRDUP(colon+1);  /* host:xxx/path/xxx */
	}
	else {
	  DBCLI->datapath = STRDUP(p); /* xxx/path/xxx */
	}
      }
#if 0
      fprintf(stderr,"<pdbname='%s'\n",pdbname);
      fprintf(stderr,"<DBCLI->host='%s'\n",DBCLI->host ? DBCLI->host : NIL);
      fprintf(stderr,"<DBCLI->datapath='%s'\n",DBCLI->datapath ? DBCLI->datapath : NIL);
#endif
      FREE(p);
      if (STRLEN(DBCLI->datapath) > 0) datapath_found = 1;
    }

    db->fp_opened_here = 0;
    if (!fp) {
      db->fp = fopen("/dev/null","w");
      if (db->fp) db->fp_opened_here = 1;
      else db->fp = stdout;
    }

    if (STRLEN(DBCLI->datapath) == 0) {
      err_msg("client: datapath not given");
      db = NULL;
      goto finish;
    }

    db->name = pdbname;
    db->mode = STRDUP("READONLY"); /* for now */

    DBCLI->hostname = STRDUP(DBCLI->host); /* Usually the non-IP-address hostname */
    if (STRLEN(DBCLI->host) > 0 && !is_ipv4_addr(DBCLI->host)) { /* get server's IP-address */
      int errflg = 0;
      char *host = STRDUP(DBCLI->host);
      struct hostent *h;
      h = gethostbyname(host);
      if (h && h->h_addrtype == AF_INET) {
	char **pptr;
	char str[ODBCS_INET_ADDRSTRLEN];
	const char *p = NULL;
	pptr = h->h_addr_list;
	for ( ; *pptr != NULL ; pptr++) {
	  p = Inet_ntop(h->h_addrtype, *pptr, str, sizeof(str));
	}
	if (p) {
	  FREE(DBCLI->host);
	  DBCLI->host = STRDUP(p);
	}
	else
	  errflg++;
      }
      else
	errflg++;
      if (errflg) {
	err_msg("client: unable to determine the server-host's (%s) IP-address", host);
	db = NULL;
	goto finish;
      }
      FREE(host);
    }
    else if (STRLEN(DBCLI->host) == 0) { /* assume server is a localhost */
      DBCLI->host = STRDUP(IP_addr);
    }
    if (DBCLI->port <= MIN_PORT) DBCLI->port = serv_port;

    DBCLI->sockfd = Socket(AF_INET, SOCK_STREAM, 0);

    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(DBCLI->port);
    Inet_pton(AF_INET, DBCLI->host, &servaddr.sin_addr);

    Connect(DBCLI->sockfd, (SA *) &servaddr, sizeof(servaddr), 
	    DBCLI->host, DBCLI->port, DBCLI->timeout, DBCLI->hostname,
	    DBCLI->verbose);

    /* Initial handshake : determine whether client/server have the same endianity */
    
    Writen(DBCLI->sockfd, &expected_magic_word, sizeof(expected_magic_word));

    n = Readn_timeo(DBCLI->sockfd, info, sizeof(info), 10); /* Give 10 seconds max to get this info */
    if (n != sizeof(info)) {
      err_msg("client: initial handshake failed");
      db = NULL;
      goto finish;
    }

    magic_word = info[0];
    db->swapbytes = (magic_word != expected_magic_word);

    if (db->swapbytes) {
      n = ODBCS_INFO - 1;
      swap4bytes_(&info[1], &n);
    }

    DBCLI->timeout = MAX(DBCLI->timeout, 10);
    timeout = DBCLI->timeout;
    if (info[1] < DBCLI->timeout) {
      DBCLI->timeout = timeout = info[1];
    }
    verbose = info[2];
    DBCLI->verbose |= verbose;

    server_version = info[3];

    if (DBCLI->verbose) {
      fprintf(stderr,"client: swapbytes = %d (expected=%u, got=%u); timeout=%d/verbose=%d/server_version=%d\n", 
	      db->swapbytes, expected_magic_word, magic_word,
	      DBCLI->timeout, DBCLI->verbose, server_version);
    }

    if (db_is_devnull) {
      /* This was for starting server@remote_host purposes only */
      (void) ODBI_disconnect(db);
      exit(0);
    }

    setup_sig("client",&DBCLI->timeout,&DBCLI->verbose,NULL);

    { /* for now */
      int n;
      int tag;
      char *params = NULL;

      tag = ODBCS_TAG_CONNECT;
      Writen(DBCLI->sockfd, &tag, sizeof(tag));
      if (DBCLI->verbose) fprintf(stderr,"client: written tag = %d (%s)\n",tag,odbcs_tagname[tag]);

      n = 0;
      if (STRLEN(db->name) > 0) n += STRLEN("dbname=") + STRLEN(db->name) + 1;
      if (STRLEN(db->mode) > 0) n += STRLEN("mode=") + STRLEN(db->mode) + 1;
      if (STRLEN(DBCLI->datapath) > 0) n += STRLEN("datapath=") + STRLEN(DBCLI->datapath) + 1;
      if (STRLEN(DBCLI->workdir) > 0) n += STRLEN("workdir=") + STRLEN(DBCLI->workdir) + 1;
      FREE(DBCLI->user);
      DBCLI->user = STRDUP(getlogin());
      if (STRLEN(DBCLI->user) == 0) {
	char *env = getenv("LOGNAME");
	if (!env) env = getenv("USER");
	if (env) {
	  FREE(DBCLI->user);
	  DBCLI->user = STRDUP(env);
	}
      }
      if (STRLEN(DBCLI->user) > 0) n += STRLEN("user=") + STRLEN(DBCLI->user) + 1;
      if (STRLEN(DBCLI->password) > 0) n += STRLEN("password=") + STRLEN(DBCLI->password) + 1;
      if (STRLEN(DBCLI->poolmask) > 0) n += STRLEN("poolmask=") + STRLEN(DBCLI->poolmask) + 1;
      if (STRLEN(DBCLI->exe) > 0) n += STRLEN("exe=") + STRLEN(DBCLI->exe) + 1;
      FREE(DBCLI->clihost);
      FREE(DBCLI->cliaddr);
      {
	int errflg = 0;
	char host[255];
	struct hostent *h;
	gethostname(host,sizeof(host));
	h = gethostbyname(host);
	if (h && h->h_addrtype == AF_INET) {
	  char **pptr;
	  char str[ODBCS_INET_ADDRSTRLEN];
	  const char *p = NULL;
	  pptr = h->h_addr_list;
	  DBCLI->clihost = STRDUP(h->h_name);
	  for ( ; *pptr != NULL ; pptr++) {
	    p = Inet_ntop(h->h_addrtype, *pptr, str, sizeof(str));
	  }
	  if (p) {
	    DBCLI->cliaddr = STRDUP(p);
	  }
	  else
	    errflg++;
	}
	else
	  errflg++;
	if (errflg) {
	  err_msg("client: unable to determine client-host's (%s) IP-address", host);
	  db = NULL;
	  goto finish;
	}
      }
      if (STRLEN(DBCLI->clihost) > 0) n += STRLEN("clihost=") + STRLEN(DBCLI->clihost) + 1;
      if (STRLEN(DBCLI->cliaddr) > 0) n += STRLEN("cliaddr=") + STRLEN(DBCLI->cliaddr) + 1;
      if (DBCLI->verbose) n += STRLEN("verbose") + 1;
      if (DBCLI->silent) n += STRLEN("silent") + 1;
      if (DBCLI->metadata) n += STRLEN("metadata") + 1;
      if (DBCLI->clean) n += STRLEN("clean") + 1;
      if (DBCLI->keep) n += STRLEN("keep") + 1;
      if (db->io_method > 0) n += STRLEN("io_method=") + 20;
      if (DBCLI->timeout > 0) n += STRLEN("timeout=") + 20;
      n += STRLEN("version=") + 20;
      n += ((DBCLI->odbsql == 0) ? STRLEN("noodbsql") : STRLEN("odbsql")) + 1;

      ALLOC(params,n);
      *params = '\0';

      if (STRLEN(db->name) > 0) 
	{ strcat(params,"dbname="); strcat(params,db->name); strcat(params,";"); }
      if (STRLEN(db->mode) > 0) 
	{ strcat(params,"mode="); strcat(params,db->mode); strcat(params,";"); }
      if (STRLEN(DBCLI->datapath) > 0) 
	{ strcat(params,"datapath="); strcat(params,DBCLI->datapath); strcat(params,";"); }
      if (STRLEN(DBCLI->workdir) > 0) 
	{ strcat(params,"workdir="); strcat(params,DBCLI->workdir); strcat(params,";"); }
      if (STRLEN(DBCLI->user) > 0) 
	{ strcat(params,"user="); strcat(params,DBCLI->user); strcat(params,";"); }
      if (STRLEN(DBCLI->password) > 0) 
	{ strcat(params,"password="); strcat(params,DBCLI->password); strcat(params,";"); }
      if (STRLEN(DBCLI->poolmask) > 0) 
	{ strcat(params,"poolmask="); strcat(params,DBCLI->poolmask); strcat(params,";"); }
      if (STRLEN(DBCLI->exe) > 0) 
	{ strcat(params,"exe="); strcat(params,DBCLI->exe); strcat(params,";"); }
      if (STRLEN(DBCLI->clihost) > 0) 
	{ strcat(params,"clihost="); strcat(params,DBCLI->clihost); strcat(params,";"); }
      if (STRLEN(DBCLI->cliaddr) > 0) 
	{ strcat(params,"cliaddr="); strcat(params,DBCLI->cliaddr); strcat(params,";"); }
      if (DBCLI->verbose) 
	{ strcat(params,"verbose"); strcat(params,";"); }
      if (DBCLI->silent) 
	{ strcat(params,"silent"); strcat(params,";"); }
      if (DBCLI->metadata) 
	{ strcat(params,"metadata"); strcat(params,";"); }
      if (DBCLI->clean) 
	{ strcat(params,"clean"); strcat(params,";"); }
      if (DBCLI->keep) 
	{ strcat(params,"keep"); strcat(params,";"); }
      if (db->io_method > 0) { 
	char s[19]; 
	snprintf(s,sizeof(s),"%d;",db->io_method);
	strcat(params,"io_method="); 
	strcat(params,s); 
      }
      if (DBCLI->timeout > 0) { 
	char s[19];
	snprintf(s,sizeof(s),"%d;",DBCLI->timeout);
	strcat(params,"timeout=");
	strcat(params,s); 
      }
      {
	char s[19];
	snprintf(s,sizeof(s),"%d;",DBCLI->version);
	strcat(params,"version=");
	strcat(params,s); 
      }
      if (DBCLI->odbsql == 0)
	{ strcat(params, "noodbsql"); strcat(params,";"); }
      else
	{ strcat(params, "odbsql"); strcat(params,";"); }

      n = STRLEN(params);
      Writen(DBCLI->sockfd, &n, sizeof(n));
      if (DBCLI->verbose) fprintf(stderr,"client: written length = %d\n", n);
      Writen(DBCLI->sockfd, params, n);
      if (DBCLI->verbose) fprintf(stderr,"client: written params = %d bytes\n", n);

      FREE(params);

      /* now reading from socket */
      {
	int retcode = 0;
	int outlen = 0;
	char *out = 
	  echo(&retcode, 
	       odbcs_tagname[tag],
	       DBCLI->sockfd, -1, 
	       DBCLI->timeout, DBCLI->verbose, db->swapbytes, &outlen);
	if (retcode != 0 || !out) { /* Failure */
	  db = NULL;
	  goto finish;
	}
	/* Writen(fileno(db->fp), out, outlen); */
	{ /* Get some (partial) DB metadata */
	  int found = 0;
	  db->npools = find_int(out,"\nnumber_of_pools=",&found);
	  if (DBCLI->verbose) fprintf(stderr,"client: number_of_pools=%d, found=%d\n",
				      db->npools, found);
	  db->io_method = find_int(out,"\nio_method=",&found);
	  if (DBCLI->verbose) fprintf(stderr,"client: io_method=%d, found=%d\n",
				      db->npools, found);
	}
	FREE(out);
      }

      if (verbose_changed || timeout_changed) {
	setup_sig("client",&DBCLI->timeout,&DBCLI->verbose,NULL);
      }

      if (server_version >= ref1_server_version) {
	/* Send the current values of timeout & verbose (i.e. possibly different values 
	   than the server is having right now), but only if they have changed from their defaults */

	if (verbose_changed) {
	  tag = ODBCS_TAG_VERBOSE;
	  Writen(DBCLI->sockfd, &tag, sizeof(tag));
	  if (DBCLI->verbose || verbose_env) fprintf(stderr,"client: written tag = %d (%s)\n",tag,odbcs_tagname[tag]);
	  Writen(DBCLI->sockfd, &verbose_env, sizeof(verbose_env));
	  if (DBCLI->verbose || verbose_env) fprintf(stderr,"client: new verbose value = %d\n",verbose_env);
	  DBCLI->verbose = verbose_env;
	}

	if (timeout_changed) {
	  tag = ODBCS_TAG_TIMEOUT;
	  Writen(DBCLI->sockfd, &tag, sizeof(tag));
	  if (DBCLI->verbose) fprintf(stderr,"client: written tag = %d (%s)\n",tag,odbcs_tagname[tag]);
	  Writen(DBCLI->sockfd, &timeout_env, sizeof(timeout_env));
	  if (DBCLI->verbose) fprintf(stderr,"client: new timeout value = %d secs\n",timeout_env);
	  DBCLI->timeout = timeout_env;
	}
      } /* if (server_version >= ref1_server_version) */
    }

  } /* if (dbname) */

  add_db_chain(db);
 finish:
  DRHOOK_END(0);
  return db;
}


void *ODBI_prepare(void *vdb,
		   const char *viewname,
		   const char *query_string)
{
  char *out = NULL;
  ODBI_db_t *db = vdb;
  ODBI_query_t *q = NULL;
  DRHOOK_START(ODBI_prepare);

  if (DBCLI->verbose) 
    fprintf(stderr,
	    "client[ODBI_prepare]: viewname=\"%s\", query_string=\"%s\"\n",
	    viewname ? viewname : NIL,
	    query_string ? query_string : NIL);
	  
  if (db) {
    db->nquery++;
    REALLOC(db->query, db->nquery);
    q = &db->query[db->nquery-1];
    q->name = viewname ? STRDUP(viewname) : STRDUP("@");
    q->is_table = (*q->name == '@') ? 1 : 0;
    q->query_string = NULL;
    if (!q->is_table && STRLEN(query_string) > 0) 
      q->query_string = STRDUP(query_string);
    else 
      q->query_string = STRDUP("");
    q->fpcache = NULL; /* for now */
    q->a = NULL;
    q->poolno = 0;
    q->nrows = 0;
    q->ncols = 0;
    q->ncols_fixed = 0;
    q->ncols_all = 0;
    q->start_row = 1;
    q->maxrows = ODBI_MAX_INT;
    q->ntot = 0;
    q->nra = 0;
    q->row_offset = 0;
    q->string_count = 0;
    QDB = db;
    
    if (q) {
      int tag = ODBCS_TAG_PREPARE_QUERY;
      int n;

      db = QDB;
      
      Writen(DBCLI->sockfd, &tag, sizeof(tag));
      if (DBCLI->verbose) fprintf(stderr,"client: written tag = %d (%s)\n",tag,odbcs_tagname[tag]);
      
      n = STRLEN(q->name);
      Writen(DBCLI->sockfd, &n, sizeof(n));
      if (DBCLI->verbose) fprintf(stderr,"client: written length = %d\n", n);
      Writen(DBCLI->sockfd, q->name, n);
      if (DBCLI->verbose) fprintf(stderr,"client: written viewname = %d bytes\n", n);

      n = STRLEN(q->query_string);
      Writen(DBCLI->sockfd, &n, sizeof(n));
      if (DBCLI->verbose) fprintf(stderr,"client: written length = %d\n", n);
      Writen(DBCLI->sockfd, q->query_string, n);
      if (DBCLI->verbose) fprintf(stderr,"client: written query_string = %d bytes\n", n);

      /* now reading from socket */

      {
	int retcode = 0;
	int outlen = 0;
	out = /* a null-terminated string with actual length of outlen */
	  echo(&retcode, 
	       odbcs_tagname[tag],
	       DBCLI->sockfd, -1,
	       DBCLI->timeout, DBCLI->verbose, db->swapbytes, &outlen);
	if (retcode != 0 || !out || outlen <= 0) { /* Failure */
	  q = NULL;
	  goto finish;
	}
	Writen(fileno(db->fp), out, outlen);
	if (out) {
	  int found = 0;
	  q->ncols_all = q->ncols_fixed = find_int(out,"\nnumber_of_columns=",&found);
	  if (DBCLI->verbose) fprintf(stderr,"client: number_of_columns=%d, found=%d\n",
				      q->ncols_fixed, found);
	  if (found) {
	    int jcol;
	    int min_bufsize = MIN(10000,100*q->ncols_fixed);
	    ODBI_limits(q,NULL,NULL,&min_bufsize);
	    q->string_count = 0;
	    CALLOC(q->cols, q->ncols_all);
	    for (jcol=0; jcol<q->ncols_all; jcol++) {
	      char *s = find_strnum(out,"\ncolumn_",jcol+1,&found); /* don't be fooled by extra_column_ */
	      if (DBCLI->verbose) fprintf(stderr,"client: jcol=%d, s='%s'\n",jcol,s);
	      if (found && s) {
		char *p;
		char delim = *s;
		q->cols[jcol].type = STRDUP(s+1);
		p = strchr(q->cols[jcol].type,delim);
		if (p) {
		  *p = '\0';
		  q->cols[jcol].name = STRDUP(p+1);
		  p = strchr(q->cols[jcol].name,delim);
		  if (p) *p = '\0';
		}
		else {
		  q->cols[jcol].name = STRDUP("????");
		}
		q->cols[jcol].dtnum = get_dtnum(q->cols[jcol].type);
		if (q->cols[jcol].dtnum == DATATYPE_STRING) q->string_count++;
		if (DBCLI->verbose) fprintf(stderr,"client: <col#%d> type='%s', name='%s', dtnum=%d\n",
					    jcol+1, q->cols[jcol].type, 
					    q->cols[jcol].name, q->cols[jcol].dtnum);
	      }
	      FREE(s);
	    }
	  }
	  else {
	    /* Failure */
	    q = NULL;
	    goto finish;
	  }
	}
      }
    }
  }
 finish:
  FREE(out);
  DRHOOK_END(0);
  return q;
}


int ODBI_execute(void *vq)
{
  ODBI_query_t *q = vq;
  int rc = 0;
  DRHOOK_START(ODBI_execute);
  if (q) {
    static int cnt = 0;
    const int tag = ODBCS_TAG_EXECUTE_QUERY;
    const int three = 3;
    int limits[3];
    ODBI_db_t *db = QDB;
    int bufsize = DBCLI->bufsize;
    int start_row = q->start_row;
    int maxrows = MIN(bufsize/q->ncols_fixed, q->maxrows);

    if (maxrows <= 0) goto finish; /* We are done */

    Writen(DBCLI->sockfd, &tag, sizeof(tag));
    if (DBCLI->verbose) fprintf(stderr,"client: written tag = %d (%s)\n",tag,odbcs_tagname[tag]);
      
    limits[0] = start_row;
    limits[1] = maxrows;
    limits[2] = bufsize;

    Writen(DBCLI->sockfd, &limits, sizeof(limits));

    if (DBCLI->verbose) fprintf(stderr,
				"client#%d written limits[ODBI_execute] : %d, %d, %d\n",
				++cnt, limits[0],limits[1],limits[2]);

    /* now reading from socket */

    {
      const int one = 1;
      int nbytes;
      double drc = 0;
      int len = 0;
      int n = 0;
      q->ncols = q->ncols_fixed;
      nbytes = Readn_timeo(DBCLI->sockfd, &len, sizeof(len), DBCLI->timeout);
      if (nbytes != sizeof(len)) {
	int istat = -1;
	err_msg("client: Server has detected a problem while in ODBI_execute()/INITIAL : status code = %d",
		  istat);
	rc = istat;
	goto finish;
      }
      if (db->swapbytes) swap4bytes_(&len,&one);
      if (DBCLI->verbose) fprintf(stderr,"client: obtained INITIAL len = %d, nbytes = %d\n",len,nbytes);
      if (len > 0) {
	while ((nbytes = 
		Readn_timeo(DBCLI->sockfd, &drc, sizeof(drc), DBCLI->timeout)) == sizeof(drc)) {
	  int nold = q->nrows * q->ncols; /* length of q->a so far */
	  int inc_rows = 0;
	  if (db->swapbytes) swap8bytes_(&drc,&one);
	  n = drc;
	  if (DBCLI->verbose) fprintf(stderr,
				      "client: [ODBI_execute] nold = %d, n = %d, q->a=%p\n",
				      nold,n,q->a);
	  if (n <= 0) break; /* check the case "n < 0" later on ... */
	  REALLOC(q->a, nold + n);
	  nbytes = Readn_timeo(DBCLI->sockfd, &q->a[nold], sizeof(double) * n, DBCLI->timeout);
	  if (nbytes != sizeof(double)*n) {
	    int istat = -2;
	    err_msg("client: Server has detected a problem while in ODBI_execute()/DATA : status code = %d",
		    istat);
	    rc = istat;
	    goto finish;
	  }
	  inc_rows = n/q->ncols;
	  if (DBCLI->verbose) fprintf(stderr,
				      "client: [ODBI_execute] inc_rows=%d, n=%d, nrows=%d, ncols=%d\n",
				      inc_rows, n, q->nrows, q->ncols);
	  if (db->swapbytes) {
	    swap8bytes_(&q->a[nold], &n);
	    if (q->string_count > 0) { /* swap strings back */
	      int jrow, jcol;
	      double *d = &q->a[nold];
	      for (jcol=0; jcol<q->ncols; jcol++) {
		if (q->cols[jcol].dtnum == DATATYPE_STRING) {
		  for (jrow=0; jrow<inc_rows; jrow++) {
		    int offset = jrow * q->ncols + jcol;
		    swap8bytes_(&d[offset], &one);
		  } /* for (jrow=0; jrow<inc_rows; jrow++) */
		} /* if (q->cols[jcol].dtnum == DATATYPE_STRING) */
	      } /* for (jcol=0; jcol<q->ncols; jcol++) */
	    } /* if (q->string_count > 0) */
	  }
#if 0
	  {
	    int jj, nn = nold + n;
	    fprintf(stderr,"--> data items [%d:%d)=\n",nold,nn);
	    for (jj=nold ; jj<nn; jj++) {
	      fprintf(stderr,"jj=%d : %.14g\n",jj,q->a[jj]);
	    }
	    fprintf(stderr,"<-- end of data items\n");
	  }
#endif
	  q->nrows += inc_rows;
	  rc += n;
	  q->start_row += inc_rows;
	  q->maxrows -= inc_rows;
	} /* while */
	/* Read the final EOF-length away from the socket */
	nbytes = Readn_timeo(DBCLI->sockfd, &len, sizeof(len), DBCLI->timeout);
	if (db->swapbytes) swap4bytes_(&len,&one);
	if (DBCLI->verbose) fprintf(stderr,"client: obtained EOF-length = %d, nbytes = %d\n",len,nbytes);
	if (len < 0) { /* popen() has failed or pclose() detected an error */
	  int istat = len;
	  err_msg("client: Server has detected a problem while in ODBI_execute()/EOF-processing : status code = %d",
		  istat);
	  rc = istat;
	  goto finish;
	}
      }
    }
  }
 finish:
  DRHOOK_END(rc);
  return rc;
}


int ODBI_fetchonerow_array(void *vq,
			int *nrows,
			int *ncols,
			double d[],
			int nd)
{
 return ODBI_fetchrow_array(vq,nrows,ncols,d,nd);
}

int ODBI_fetchrow_array(void *vq,
			int *nrows,
			int *ncols,
			double d[],
			int nd)
{
  ODBI_query_t *q = vq;
  int rc = 0;
  DRHOOK_START(ODBI_fetchrow_array);
  if (nrows) *nrows = 0;
  if (ncols) *ncols = 0;
  if (q && d) {
    ODBI_db_t *db = QDB;
    int navail = q->nrows * q->ncols;
    if (ncols) *ncols = q->ncols_fixed;
    if (DBCLI->verbose)
      fprintf(stderr,
	      "client[1]: row_offset=%d, navail=%d, nrows=%d, ncols=%d, ncols_fixed=%d, a=%p\n",
	      q->row_offset, navail, q->nrows, q->ncols, q->ncols_fixed, q->a);
    if (q->row_offset == q->nrows) {
      FREE(q->a);
      q->row_offset = q->nrows = 0;
      if (q->maxrows > 0) {
	navail = ODBI_execute(q); /* get next chunk */
	if (navail < 0) { /* Failure */
	  rc = navail;
	  goto finish;
	}
      }
      else {
	navail = 0; /* we are done already */
      }
    }
    if (DBCLI->verbose)
      fprintf(stderr,
	      "client[2]: row_offset=%d, navail=%d, nrows=%d, ncols=%d, ncols_fixed=%d, a=%p\n",
	      q->row_offset, navail, q->nrows, q->ncols, q->ncols_fixed, q->a);
    if (navail > 0 && q->a) {
      int j, offset = q->row_offset * q->ncols;
      rc = MIN(navail-offset, nd);
    if (DBCLI->verbose)
      fprintf(stderr,
	      "client[3]: offset=%d, navail=%d, nd=%d, rc=%d\n",
	      offset, navail, nd, rc);
      for (j=0; j<rc; j++) d[j] = q->a[j+offset];
      if (nrows) *nrows = rc/q->ncols;
      if (ncols) *ncols = q->ncols;
      q->row_offset += rc/q->ncols;
    }
    if (DBCLI->verbose)
      fprintf(stderr,
	      "client[4]: row_offset=%d, navail=%d, nrows=%d, ncols=%d, ncols_fixed=%d, a=%p\n",
	      q->row_offset, navail, q->nrows, q->ncols, q->ncols_fixed, q->a);
  }
 finish:
  DRHOOK_END(nrows ? *nrows : 0);
  return rc;
}


int ODBI_finish(void *vq)
{
  ODBI_query_t *q = vq;
  int rc = 0;
  DRHOOK_START(ODBI_finish);
  /* not implemented yet */
  DRHOOK_END(0);
  return rc;
}


int ODBI_disconnect(void *vdb)
{
  ODBI_db_t *db = vdb;
  int rc = 0;
  DRHOOK_START(ODBI_disconnect);
  if (db) {
    int j;
    const int save = 0;
    if (DBCLI) {
      int tag = ODBCS_TAG_BYEBYE;
      Writen(DBCLI->sockfd, &tag, sizeof(tag));
      if (DBCLI->verbose) fprintf(stderr,"client: written tag = %d (%s)\n",tag,odbcs_tagname[tag]);
      Shutdown(DBCLI->sockfd,SHUT_WR); /* used to be: Close(DBCLI->sockfd); */
      FREE(DBCLI->host);
      FREE(DBCLI->hostname);
      FREE(DBCLI->datapath);
      FREE(DBCLI->poolmask);
      FREE(DBCLI->user);
      FREE(DBCLI->password);
      FREE(DBCLI->workdir);
      FREE(DBCLI->exe);
      FREE(DBCLI->clihost);
      FREE(DBCLI->cliaddr);
    }
    FREE(db->name);
    FREE(db->mode);
    for (j=0; j<db->nquery; j++) {
      ODBI_query_t *q = &db->query[j];
      /* (void) ODBI_finish(q); */ /* for now */
      FREE(q->name);
      FREE(q->query_string);
      if (q->cols) {
	int jcol;
	for (jcol=0; jcol<q->ncols_all; jcol++) {
	  FREE(q->cols[jcol].name);
	  FREE(q->cols[jcol].type);
	}
	FREE(q->cols);
      }
    }
    FREE(db->query);
    FREE(db->poolstat);
    if (db->fp_opened_here) fclose(db->fp);
    rm_db_chain(db);
    FREE(db);
  }
  DRHOOK_END(0);
  return rc;
}


double ODBI_bind_param(void *vq, 
		       const char *param_name, 
		       double new_value)
{
  ODBI_query_t *q = vq;
  double old_value = 0;
  DRHOOK_START(ODBI_bind_param);
  if (param_name && q) {
    ODBI_db_t *db = QDB;
    int len;
    int tag = ODBCS_TAG_BIND_PARAM;
    if (*param_name == '$') param_name++;
    len = STRLEN(param_name);
    Writen(DBCLI->sockfd, &tag, sizeof(tag));
    if (DBCLI->verbose) fprintf(stderr,"client: written tag = %d (%s)\n",tag,odbcs_tagname[tag]);
    Writen(DBCLI->sockfd, &len, sizeof(len));
    if (DBCLI->verbose) fprintf(stderr,"client: written length = %d\n",len);
    Writen(DBCLI->sockfd, param_name, len);
    if (DBCLI->verbose) fprintf(stderr,"client: written param name of length %d bytes\n",len);
    Writen(DBCLI->sockfd, &new_value, sizeof(new_value));
    if (DBCLI->verbose) fprintf(stderr,"client: written param itself, length %d bytes\n",len);
  }
  DRHOOK_END(0);
  return old_value;
}


void ODBI_limits(void *vq,
		 int *start_row,
		 int *maxrows,
		 int *bufsize)
{
  ODBI_query_t *q = vq;
  DRHOOK_START(ODBI_limits);
  if (q) {
    ODBI_db_t *db = QDB;

    if (start_row) {
      if (*start_row <= 0) *start_row = 1;
      q->start_row = *start_row;
    }

    if (maxrows) {
      if (*maxrows < 0) *maxrows = ODBI_MAX_INT;
      q->maxrows = *maxrows;
    }

    if (bufsize) {
      if (*bufsize < q->ncols_fixed) *bufsize = q->ncols_fixed;
      DBCLI->bufsize = *bufsize;
    }
  }
  DRHOOK_END(0);
}


int ODBI_fetchfile(void *vq,
		   const char *filename,
		   const char *fileformat)
{
#if 0
  const char dbgfile[] = "/tmp/fetchfile.client";
#else
  const char dbgfile[] = "/dev/null";
#endif
  FILE *dbgfp = fopen(dbgfile, "w");
  int rc = 0;
  ODBI_query_t *q = vq;
  DRHOOK_START(ODBI_fetchfile);
  setvbuf(dbgfp, (char *)NULL, _IONBF, 0);
  if (q && filename && fileformat) {
    int len;
    const int tag = ODBCS_TAG_FETCHFILE;
    const int one = 1;
    const int three = 3;
    int limits[3];
    ODBI_db_t *db = QDB;
    int bufsize = DBCLI->bufsize;
    int start_row = q->start_row;
    int maxrows = q->maxrows;
    int nbytes;

    /* tag */
    Writen(DBCLI->sockfd, &tag, sizeof(tag));
    if (DBCLI->verbose) fprintf(stderr,"client: written tag = %d (%s)\n",tag,odbcs_tagname[tag]);
      
    /* limits */
    limits[0] = start_row;
    limits[1] = maxrows;
    limits[2] = bufsize;

    Writen(DBCLI->sockfd, &limits, sizeof(limits));

    if (DBCLI->verbose) fprintf(stderr,
				"client written limits[ODBI_fetchfile] : %d, %d, %d\n",
				limits[0],limits[1],limits[2]);

    /* filename */
    len = STRLEN(filename);
    Writen(DBCLI->sockfd, &len, sizeof(len));
    if (DBCLI->verbose) fprintf(stderr,"client: written filename length = %d\n",len);
    Writen(DBCLI->sockfd, filename, len);
    if (DBCLI->verbose) fprintf(stderr,"client: written filename = '%s'\n",filename);

    /* fileformat */
    len = STRLEN(fileformat);
    Writen(DBCLI->sockfd, &len, sizeof(len));
    if (DBCLI->verbose) fprintf(stderr,"client: written fileformat length = %d\n",len);
    Writen(DBCLI->sockfd, fileformat, len);
    if (DBCLI->verbose) fprintf(stderr,"client: written fileformat = '%s'\n",fileformat);

    /* recv from server */

    nbytes = Readn_timeo(DBCLI->sockfd, &len, sizeof(len), DBCLI->timeout);
    if (nbytes != sizeof(len)) {
      int istat = -1;
      err_msg("client: Server has detected a problem while in ODBI_fetchfile()/INITIAL : status code = %d",
	      istat);
      rc = istat;
      goto finish;
    }
    if (db->swapbytes) swap4bytes_(&len,&one);
    if (DBCLI->verbose) fprintf(stderr,"client: obtained INITIAL len = %d, nbytes = %d\n",len,nbytes);

    /* receive contents of the file */

    if (len > 0) {
      int timeout = MIN(10,DBCLI->timeout); /* No more that 10 sec wait accepted here; even that's too much */
      int filesize = 0;
      FILE *fp = NULL;

      /* get the file size (even zero length) */
      nbytes = Readn_timeo(DBCLI->sockfd, &filesize, sizeof(filesize), timeout);
      if (nbytes != sizeof(filesize)) {
	int istat = -2;
	err_msg("client: Server has detected a problem while in ODBI_fetchfile(), "
		"total file length: status code = %d",
		istat);
	rc = istat;
	goto finish;
      }
      if (db->swapbytes) swap4bytes_(&filesize,&one);
      if (DBCLI->verbose) fprintf(stderr,"client: obtained total file length = %d (nbytes = %d)\n",filesize,nbytes);

      if (filesize > 0) {
	nbytes = 0;
	fp = Fopen(filename, "w");
	fprintf(dbgfp, "filename = '%s' opened fp=%p, fileno(fp)=%d, filesize = %d\n",filename,fp,fileno(fp),filesize);
	if (fp) {
	  int nread = 0;
	  char buf[IO_BUFSIZE_DEFAULT];
	  while (nread < filesize) {
	    int nlen = MIN(sizeof(buf), filesize - nread);
	    nbytes = Readn_timeo(DBCLI->sockfd, buf, sizeof(*buf) * nlen, timeout);
	    fprintf(dbgfp, "\tnbytes = %d, nlen = %d [IO_BUFSIZE_DEFAULT = %d]\n",
		    nbytes, nlen, IO_BUFSIZE_DEFAULT);
	    if (nbytes > 0) {
	      int nwritten = fwrite(buf, sizeof(*buf), nbytes, fp);
	      fprintf(dbgfp, "\t\tnwritten = %d bytes [incr]\n",nwritten);
	      if (nwritten != nbytes) {
		err_msg("client: Cannot write desired no. of bytes (%d) to file '%s' [format='%s']. "
			"Written only %d bytes of %d. Is client's disk full or is there a permission problem ?",
			nbytes, filename, fileformat, nwritten, nbytes);
		rc = -4;
		goto finish;
	      }
	      nread += nwritten;
	      fprintf(dbgfp, "\t\tnread = %d bytes so far; filesize = %d bytes\n",nread,filesize);
	    }
	    else {
	      break;
	    }
	  } /* while (nread < filesize) */
	  Fclose(fp);
	  if (nread != filesize) {
	    err_msg("client: Couldn't write all data to file '%s' [format='%s']. "
		    "Expected filesize %d, but written %d bytes",
		    filename, fileformat, filesize, nread);
	    rc = -5;
	    goto finish;
	  }
	  rc = filesize;
	}
	else {
	  err_msg("client: Unable to open file '%s' [format='%s'] for writing", filename, fileformat);
	  rc = -3;
	  goto finish;
	}
      } /* if (filesize > 0) */
      else {
	err_msg("client: Unable to create file '%s' [format='%s'] : filesize = %d\n",
		filename, fileformat, filesize);
	rc = -6;
	goto finish;
      }

      /* Read the final EOF-length away from the socket */
      nbytes = Readn_timeo(DBCLI->sockfd, &len, sizeof(len), timeout);
      if (db->swapbytes) swap4bytes_(&len,&one);
      if (DBCLI->verbose) fprintf(stderr,"client: obtained EOF-length = %d, nbytes = %d\n",len,nbytes);
      if (len < 0) { /* popen() has failed or pclose() detected an error */
	int istat = len;
	err_msg("client: Server has detected a problem while in ODBI_fetchfile()/EOF-processing : status code = %d",
		istat);
	rc = istat;
	goto finish;
      }

    } /* if (len > 0) */

  finish:
    ODBI_finish(q);

  }
  DRHOOK_END(rc >= 0 ? rc : 0);
  fprintf(dbgfp,"rc = %d\n",rc);
  fclose(dbgfp);
  return rc;
}


#include "odbi_shared.c"

#else

void dummy_ODBI_client() { }
void Vparam() {}
#endif
