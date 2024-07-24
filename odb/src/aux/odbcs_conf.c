/* odbcs_conf.c */

#include "odbcs.h"

#ifdef ODBCS

#include "odbcsdefs.h"
#include "cmaio.h"

/* 

   Reads ODB client/server configuration file(s) in the following order :
   
   $ODB_SYSPATH/odbcs.conf
   ~/odbcs.conf i.e. $HOME/odbcs.conf
   ./odbcs.conf
   $ODBCS_CONF (= environment variable pointing to a file name)

   Each successive read overrides any previous definitions.

   Author: Sami Saarinen, ECMWF, 10-Dec-2007

*/

/* 

   Format of the odbcs.conf -files : each line contains colon (:) separated entries as follows:

   Please note that the default separator ':' could be changed for every input line as long as it doesn't exits
   in any of the data fields. If you want to change it, just supply required separator as the first character
   for each line.

   :<hostname>:<username>:<port>:<timeout>:<$ARCH>:<$CPU_TYPE>:<$OBJECT_MODE>:<$ODB_ARCH>:<$ODB_VERSION>:<$ODB_COMPDIR>:<$ODB_DIR>:<$ODB_ROOT>:
        0           1        2        3       4         5              6           7           8              9              10         11

   where the fields 0 to 11 are :

   0 <hostname>     = hostname to connect to
                      The * supplies default values for any non-specified machines.
                      You can also supply any unix-wildcard f.ex. ^bee* would cover all hostnames starting with bee

   1 <username>     = username to be used to /usr/bin/rsh into this hostname
                      If left empty, then use the same username where the client is current logged-on

   2 <port>         = Port number to connect the server with
                      When left empty, then port id is calculated from the client user id (uid) modulo 10,000 plus 10,000

   3 <timeout>      = A timeout imposed with this host.
                      The default is TIMEOUT_DEFAULT seconds.

   4 <$ARCH>        = ARCH'itecture of the hostname
                      If not given, then the default is determined (guessed) by using either 'arch' or 'uname' commands

   5 <$CPU_TYPE>    = CPU_TYPE of the hostname
                      If not given, then the default is figured out from the $ARCH

   6 <$OBJECT_MODE> = OBJECT_MODE of the hostname ; either 32 or 64
                      If not given, then the default is assumed to be 32.

   7 <$ODB_ARCH>    = ODB-architecture ; usually the same as $ARCH
                      If not defined, then the same as $ARCH

   8 <$ODB_VERSION> = ODB-version
                      If not given, then use the same as on the client.
		      And is still not defined, set to "current".

   9 <$ODB_COMPDIR> = ODB f90 compiler used
                      If not given, then the default is figured out from the $ARCH

  10 <$ODB_DIR>     = ODB installation base directory e.g. /usr/local/apps/odb
                      If not given, then use either the default (supplied via hostname=*) or as on the client

  11 <$ODB_ROOT>    = ODB installation version base root directory e.g. /usr/local/apps/odb/current/pgf90/ILP32
                      If not given, then set to $ODB_DIR/$ODB_VERSION/$ODB_COMPDIR/{ILP|LP}{$OBJECT_MODE}
		      where ILP32 used for $OBJECT_MODE = 32, and LP64 for $OBJECT_MODE = 64

*/



#define GUESS "_UNDEF_"
#define IS_GUESS(s) (STRLEN(s) == 0 || strequ(s,GUESS))


PRIVATE const char *fldname[] = {
  "hostname",
  "username",
  "port (int)",
  "timeout (int)",
  "arch",
  "cpu_type",
  "object_mode (int)",
  "odb_arch",
  "odb_version",
  "odb_compdir",
  "odb_dir",
  "odb_root",
  NULL
};


PRIVATE odbcs_t *the_default = NULL;
PRIVATE odbcs_t *rock_bottom = NULL;


PRIVATE odbcs_t *
conf_read(const char *file, odbcs_t *conf)
{
  FILE *fp = NULL;
  int iounit = -1;
  int iret = 0;
  int perror_onoff = -1;
  const int on = 1;
  const int off = 0;
  odbcs_t *pc = rock_bottom;

  if (!pc) {
    int len;
    char *s;
    const char fmt_star[] = ":*:::%d:";
    CALLOC(pc, 1);
    len = STRLEN(fmt_star) + 20;
    ALLOC(s, len);
    snprintf(s,len,fmt_star,TIMEOUT_DEFAULT); /* Supply any-host, no username, no port, timeout */
    pc->line = s;
    rock_bottom = conf = the_default = pc;
  }
  else
    pc = conf;

  cma_set_perror_(&off, &perror_onoff); /* temporarely disable perror() messages */
  cma_open_(&iounit, file, "r", &iret, STRLEN(file), 1);
  fp = (iounit >= 0 && iret == 1) ? CMA_get_fp(&iounit) : NULL;
  if (fp) {
    char line[ODBCS_MAXLINE];

    while (pc && pc->next) pc = pc->next;

    while (fgets(line, sizeof(line), fp)) {
      char *nl = strchr(line,'\n');
      char *s = line;
      while (*s && isspace(*s)) ++s;
      if (nl) *nl = '\0';
      /* if first 2 chars are ":<", then this must be some example line --> skip */
      if (STRLEN(s) > 0 && !strnequ(s,":<",2)) {
	odbcs_t *pc_prev = pc;
	odbcs_t *next = NULL;
	CALLOC(next,1);
	next->line = STRDUP(s);
	pc->next = next;
	rock_bottom = pc = next;
	pc->prev = pc_prev;
      }
    } /* while (fgets(line, sizeof(line), fp)) */

    cma_close_(&iounit, &iret);
  }
  if (perror_onoff == on) { /* switch perror() output back on */
    cma_set_perror_(&on, &perror_onoff);
  }
  return conf;
}


PRIVATE char *
trim_token(const char *tin, char delim, int *skipchars)
{
  char *s = NULL;
  char *talloc = STRDUP(tin);
  char *t = talloc;
  char *next_delim = strchr(t, delim);
  if (next_delim) {
    char *start;
    int len;
    *next_delim = '\0';
    *skipchars = STRLEN(talloc) + 1;
    while (*t && isspace(*t)) ++t;
    start = t;
    while (*t && !isspace(*t)) ++t;
    len = t - start;
    ALLOC(s,len+1);
    strncpy(s, start, len);
    s[len] = '\0';
  }
  else {
    *skipchars = 0;
    s = STRDUP("");
  }
  FREE(talloc);
  return s;
}


PRIVATE odbcs_t *
conf_make(odbcs_t *conf)
{
  odbcs_t *pc = conf;
  while (pc) {
    char *token = pc->line;
    char delim = *token++;
    int token_num = 0;
    do {
      int num;
      char *env;
      char *s = NULL;
      int len;
      s = trim_token(token, delim, &len);
      token += len;

      switch (token_num) {

      case 0: /* hostname */
	pc->hostname = STRDUP(s);
	if (strequ(pc->hostname,"*")) the_default = pc;
	break;

      case 1: /* username */
	if (STRLEN(s) == 0) {
	  char *env = getenv("USER");
	  if (!env) env = getenv("LOGNAME");
	  pc->username = env ? STRDUP(env) : STRDUP("unknown_user");
	}
	else
	  pc->username = STRDUP(s);
	break;

      case 2: /* port */
	num = atoi(s);
	pc->port = (num < MIN_PORT || num > MAX_PORT) ? PORT_DEFAULT : num;
	break;

      case 3: /* timeout */
	num = atoi(s);
	pc->timeout = (num < 0) ? TIMEOUT_DEFAULT : num;
	break;

      case 4: /* $ARCH */
	pc->arch = (STRLEN(s) == 0) ? STRDUP(GUESS) : STRDUP(s);
	break;

      case 5: /* $CPU_TYPE */
	pc->cpu_type = (STRLEN(s) == 0) ? STRDUP(GUESS) : STRDUP(s);
	break;

      case 6: /* $OBJECT_MODE */
	num = atoi(s);
	pc->object_mode = (num == 32 || num == 64) ? num : 32;
	break;

      case 7: /* $ODB_ARCH */
	pc->odb_arch = (STRLEN(s) == 0) ? STRDUP(pc->arch) : STRDUP(s);
	break;

      case 8: /* $ODB_VERSION */
	env = (STRLEN(s) == 0) ? getenv("ODB_VERSION") : s;
	pc->odb_version = (STRLEN(env) == 0) ? STRDUP("current") : STRDUP(env);
	break;

      case 9: /* $ODB_COMPDIR */
	pc->odb_compdir = (STRLEN(s) == 0) ? STRDUP(GUESS) : STRDUP(s);
	break;

      case 10: /* $ODB_DIR */
	pc->odb_dir = (STRLEN(s) == 0) ? STRDUP(GUESS) : STRDUP(s);
	break;

      case 11: /* $ODB_ROOT */
	pc->odb_root = (STRLEN(s) == 0) ? STRDUP(GUESS) : STRDUP(s);
	break;
      } /* switch (token_num) */

      FREE(s);
    } while (++token_num <= 11);

    FREE(pc->line);
    pc = pc->next;
  }

  /* Resolve missing items in the default */

  pc = the_default;
  if (pc) {
    char *env;

    if (IS_GUESS(pc->arch)) {
      env = getenv("ARCH");
      pc->arch = env ? STRDUP(env) : STRDUP(GUESS);
    }

    if (IS_GUESS(pc->cpu_type)) {
      env = getenv("CPU_TYPE");
      FREE(pc->cpu_type);
      pc->cpu_type = env ? STRDUP(env) : STRDUP(GUESS);
    }
    
    if (IS_GUESS(pc->odb_arch)) {
      env = getenv("ODB_ARCH");
      FREE(pc->odb_arch);
      if (env) {
	pc->odb_arch = STRDUP(env);
      }
      else if (STRLEN(pc->arch) > 0) {
	pc->odb_arch = STRDUP(pc->arch);
      }
      else {
	pc->odb_arch = STRDUP(GUESS);
      }
    }

    if (IS_GUESS(pc->odb_compdir)) {
      env = getenv("ODB_COMPDIR");
      FREE(pc->odb_compdir);
      pc->odb_compdir = env ? STRDUP(env) : STRDUP(GUESS);
    }

    if (IS_GUESS(pc->odb_dir)) {
      env = getenv("ODB_DIR");
      FREE(pc->odb_dir);
      pc->odb_dir = env ? STRDUP(env) : STRDUP(GUESS);
    }

    if (IS_GUESS(pc->odb_root)) {
      env = getenv("ODB_ROOT");
      FREE(pc->odb_root);
      pc->odb_root = env ? STRDUP(env) : STRDUP(GUESS);
    }  

  } /* if (pc) */

  /* 2nd pass : try to resolve as many GUESS'es as possible */

  pc = conf;
  while (pc) {

    if (pc != the_default) {

      if (IS_GUESS(pc->arch)) {
	FREE(pc->arch);
	pc->arch = STRDUP(the_default->arch);
      }

      if (IS_GUESS(pc->odb_arch)) {
	const char *default_odb_arch = (STRLEN(pc->arch) > 0) ? pc->arch : the_default->arch;
	FREE(pc->odb_arch);
	pc->odb_arch = STRDUP(default_odb_arch);
      }

      if (IS_GUESS(pc->odb_dir)) {
	FREE(pc->odb_dir);
	pc->odb_dir = STRDUP(the_default->odb_dir);
      }
    } /* if (pc != the_default) */

    /* In the following the "pc" could also point to "the_default" */

    if (IS_GUESS(pc->odb_root) && !IS_GUESS(pc->odb_dir) && !IS_GUESS(pc->odb_compdir)) {
      char *s = pc->odb_root;
      int len = STRLEN(pc->odb_dir) + STRLEN(pc->odb_version) + STRLEN(pc->odb_compdir) + 30;
      ALLOC(pc->odb_root, len);
      snprintf(pc->odb_root, len, "%s/%s/%s/%s%d",
	       pc->odb_dir, pc->odb_version, pc->odb_compdir,
	       (pc->object_mode == 32) ? "ILP" : "LP",
	       pc->object_mode);
      FREE(s);
    }
    
    if (!pc->odb_binpath) {
      if (!IS_GUESS(pc->odb_root)) {
	int len = STRLEN(pc->odb_root) + STRLEN("/bin") + 1;
	ALLOC(pc->odb_binpath, len);
	snprintf(pc->odb_binpath, len, "%s/bin", pc->odb_root);
      }
      else {
	char *env = getenv("ODB_BINPATH");
	pc->odb_binpath = env ? STRDUP(env) : STRDUP(GUESS);
      }
    }

    pc = pc->next;
  } /* while (pc) */

  return conf;
}


PRIVATE int
find_any(const char *s, const char *delims)
{
  int found = 0;
  if (s && delims) {
    while (*delims) {
      if (strchr(s,*delims)) {
	found = 1;
	break;
      }
      ++delims;
    }
  }
  return found;
}


PUBLIC const odbcs_t *
odbcs_conf_match(const char *host, odbcs_t *conf, char **truehostname)
{
  const odbcs_t *pc = rock_bottom;
  char *THEhost = NULL;
  if (strequ(host, "localhost")) {
    char thishost[255];
    gethostname(thishost,sizeof(thishost));
    THEhost = STRDUP(thishost);
  }
  else
    THEhost = STRDUP(host);
  while (pc) {
    /* Try exact match first */
    if (strcaseequ(pc->hostname, THEhost)) goto finish;
    /* Then a wildcard match */
    if (find_any(pc->hostname,"^*[]{}|(),$") &&
	ODB_Common_StrEqual(pc->hostname, THEhost, 0, NULL, 1)) goto finish;
    pc = pc->prev;
  }
 finish:
  if (truehostname) *truehostname = STRDUP(THEhost);
  FREE(THEhost);
  if (!pc) pc = the_default;
  return pc;
}


PUBLIC odbcs_t *
odbcs_conf_create()
{
  odbcs_t *conf = NULL;
  const char *conf_files[] = { 
    "$ODB_SYSPATH/odbcs.conf",
    "$HOME/odbcs.conf",
    "./odbcs.conf",
    "$ODBCS_CONF",
    NULL 
  };
  const char **file = conf_files;
  while (*file) {
    conf = conf_read(*file, conf);
    ++file;
  }
  conf = conf_make(conf);
  return conf;
}


#if 0

/* Just testing ... */

void
mainio_()
{
  odbcs_t *conf = NULL;
  char *file = "odbcs.conf";
  conf = conf_read(file, conf);
  conf = conf_make(conf);
  {
    const char *hosts[] = { "hpce", "gylfi", "bee04", "ecgate", "localhost", NULL };
    const char **h = hosts;
    while (*h) {
      char *truehostname = NULL;
      const odbcs_t *match = odbcs_conf_match(*h, conf, &truehostname);
      printf("host = '%s', truehostname = '%s'",*h,truehostname);
      if (match) {
	printf(" -- matched [via '%s']!!\n",match->hostname);
	printf("/usr/bin/rsh %s -l %s "
	       "'env _ARCH=%s _CPU_TYPE=%s _OBJECT_MODE=%d _ODB_ARCH=%s _ODB_COMPDIR=%s "
	       "_ODB_DIR=%s _ODB_ROOT=%s "
	       "%s/odbi_proxy -p %d -t %d < /dev/null'\n",
	       truehostname, match->username,
	       match->arch, match->cpu_type, match->object_mode, match->odb_arch, match->odb_compdir,
	       match->odb_dir, match->odb_root,
	       match->odb_binpath, match->port, match->timeout);
      }
      else
	printf(" -- no match\n");
      FREE(truehostname);
      ++h;
    }
  }
}
#endif


#endif /* ODBCS */
