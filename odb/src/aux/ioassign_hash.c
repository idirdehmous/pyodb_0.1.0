#include "iostuff.h"
#include "ioassign.h"

static const int IOASSIGN_hashsize = 123457U; /* hardcoded for now */

PRIVATE uint
IOASSIGN_Hash(const char *s)
{ 
  uint hashval = 0;
  for (; *s ; s++) {
    hashval = (*s) + 31U * hashval;
  }
  hashval = hashval % IOASSIGN_hashsize;
  return hashval;
}

PUBLIC uint
IOassign_hash(const char *s)
{
  uint hash;
  int hps = has_percent_sign(s);
  char *x = hps ? STRDUP(s) : (char *)s;
  if (hps) {
    char *p = strchr(x,'%');
    if (p) *p = '\0'; /* Truncate "x" at '%'-sign */
  }
  hash = IOASSIGN_Hash(x);
  if (hps) FREE(x);
  return hash;
}

PUBLIC Ioassign *
IOassign_lookup(const char *s, Ioassign *pstart)
{
  int found = 0;
  Ioassign *p = pstart;
  uint hash = p ? IOassign_hash(s) : 0;
  while (p) {
    if (p->hash == hash) {
      char *env = IOresolve_env(p->filename);
      found = IOstrequ(env,s,NULL,NULL,p->fromproc, p->toproc);
      FREE(env);
      if (found) break; /* A match found */
    }
    p = p->next;
  }
  return found ? p : NULL;
}
