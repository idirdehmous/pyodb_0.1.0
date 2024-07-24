#include "iostuff.h"

typedef struct known_cmd_t {
  /* Only "char *suffix" can be set to NULL */
  char *label;
  char *packcmd;
  char *pack_to;
  char *unpackcmd;
  char *unpack_from;
  char *suffix;
  Boolean has_incore; /* true if in-memory rather than unix-command is available */
} Known_CMD;


char *
IOknowncmd(const char *label, Boolean read_only, Boolean *has_incore, char **suffix, const IObuf *iobuf)
{
  static const Known_CMD kcmd[] = {
    /* 
       Note the double '%%' in %%s is needed, if %d is also present in the same line.
       This is due to double filtering; after the first filtering/internal write (snprintf)
       we want to obviously retain the final '%s' for filename
    */
    { "#ddgzip",
      "gzip -c "                                , " | dd of=%%s bs=%d 2>/dev/null", /* note %%s */
      "dd if=%%s bs=%d 2>/dev/null | gunzip -c ", " ",                              /* note %%s */
      ".gz", true }, /* "true" meant to be used with zlib */
    { "#gzip",
      "gzip -c "  , " > %s", 
      "gunzip -c ", " < %s" , 
      ".gz", true }, /* "true" meant to be used with zlib */
    { "#bzip2",
      "bzip2 -c "  , " > %s", 
      "bunzip2 -c ", " < %s", 
      ".bz2", false },
    { "#compress",
      "compress -c -f "  , " > %s", 
      "uncompress -c ", " < %s", 
      ".Z", false },
    { "#pack",
      "pack -f "  , " %s", 
      "unpack ", " %s" , 
      ".z", false },
    { "#zip",
      "zip -c "  , " %s", 
      "unzip -c ", " %s" , 
      ".zip", false },
    { "#pcma",
      "pcma  -w %d ", " -o %%s", /* note %%s */
      "upcma -r %d ", " -i %%s", /* note %%s */
      NULL, false },
    { "#tfecfs",
      "to_ecfs "  , " %s", 
      "from_ecfs ", " %s", 
      NULL, false },
    { "#user",
      "$ODB_USER_PACK "  , " %%s %d", /* note %%s */
      "$ODB_USER_UNPACK ", " %%s %d", /* note %%s */
      NULL, false },
    NULL
  };

  char *cmd = NULL;

  if (has_incore) *has_incore = false;
  if (suffix) *suffix = NULL;

  if (label && *label == '#') {
    int j = 0;
    char *plabel = STRDUP(label);
    char *p = strchr(plabel,' ');
    char *flags = NULL;

    if (p) { /* " for example, from IOASSIGN : ...|#gzip -1v */
      int flags_offset = (p - plabel);
      flags = STRDUP(plabel+flags_offset);
      *p = '\0';
    }
    
    for (;;) {
      const Known_CMD *pcmd = &kcmd[j];
      
      if (!pcmd->label) break; /* Last */
      
      if (strequ(plabel, pcmd->label)) { /* Matched !! */
	int cmdlen = 0;
	int buflen = IO_BUFSIZE_DEFAULT;
	const char *cmdpre = NULL;
	const char *cmdpost = NULL;

	if (iobuf) {
	  buflen = iobuf->len;
	  if (buflen < 512 || buflen > 1000000000) buflen = IO_BUFSIZE_DEFAULT;
	}

	if (read_only) {
	  if (pcmd->unpackcmd[0] == '$') {
	    char *env = getenv(&pcmd->unpackcmd[1]);
	    if (!env) {
	      fprintf(stderr,
		      "IOknowncmd(): Unable to unpack '%s': Variable '%s' is not defined\n",
		      label, pcmd->unpackcmd);
	      break; /* for (;;) */
	    }
	  }
	  cmdpre = pcmd->unpackcmd;
	  cmdpost = pcmd->unpack_from;
	}
	else {
	  if (pcmd->packcmd[0] == '$') {
	    char *env = getenv(&pcmd->packcmd[1]);
	    if (!env) {
	      fprintf(stderr,
		      "IOknowncmd(): Unable to pack '%s': Variable '%s' is not defined\n",
		      label, pcmd->packcmd);
	      break; /* for (;;) */
	    }
	  }
	  cmdpre = pcmd->packcmd;
	  cmdpost = pcmd->pack_to;
	}

	cmdlen = strlen(cmdpre) + strlen(cmdpost);
	if (flags) cmdlen += strlen(flags);
	cmdlen++;

	ALLOC(cmd,cmdlen);

	strcpy(cmd,cmdpre);
	if (flags) strcat(cmd,flags);
	strcat(cmd,cmdpost);

	if (strstr(cmd,"%d")) {
	  char *tmp = NULL;
	  int tmplen = strlen(cmd) + 30; /* Accomodate value for "%d" */
	  ALLOC(tmp,tmplen);
	  snprintf(tmp,tmplen,cmd,buflen);
	  FREE(cmd);
	  cmd = tmp;
	}

	if (has_incore) *has_incore = pcmd->has_incore;
	if (suffix && pcmd->suffix) *suffix = STRDUP(pcmd->suffix);

	break; /* for (;;) */
      }

      j++;
    } /* for (;;) */
    
    FREE(plabel);
    if (flags) FREE(flags);
  } /* if (label && *label == '#') */

  if (!cmd) cmd = STRDUP(label); /* STRDUP will return blank string if label points to nothing */

#if 0
  if (label && cmd) fprintf(stderr,"IOknowncmd(%s, etc.) --> '%s'\n",label,cmd);
#endif

  return cmd;
}
