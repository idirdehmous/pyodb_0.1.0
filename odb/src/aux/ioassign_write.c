#include "iostuff.h"
#include "ioassign.h"

int 
IOassign_write(const char *IOASSIGN, 
                      int  pbcount,
	        Ioassign  *ioassign_start)
{
  int rc = 0;

  /* Open IOASSIGN datafile for output */

  FILE *fp = IOASSIGN ? fopen(IOASSIGN, "w") : stdout;
  
  if (!fp) {
    /* Error : Couldn't open IOASSIGN-file for write */
    perror(IOASSIGN ? IOASSIGN : "<stdout>");
    rc = -1;
  }
  else {
    /* Write IOASSIGN data */
    Ioassign *p = ioassign_start;

    while ( p && pbcount > 0 ) {

      if (p->filename) {
	
	fprintf(fp,"%s %s %d %d %d %d %d %d %d %d %d %d %d",
		p->filename, p->aliasname,
		p->numbins,
		p->readbufsize, p->writebufsize,
		p->prealloc, p->extent,
		p->compression, p->blocksize,
		p->concat, p->maxproc,
      p->fromproc, p->toproc);

	fprintf(fp,"%s%s\n",
		p->pipecmd ? "|" : "\0",
		p->pipecmd ? p->pipecmd : "\0");
      }

      p = p->next;
      pbcount--;
    }

    if (IOASSIGN) fclose(fp);
  }

  return rc;
}
