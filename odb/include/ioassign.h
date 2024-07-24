#ifndef _IOASSIGN_H_
#define _IOASSIGN_H_

/* ioassign.h */

typedef struct ioassign_t {
  unsigned int hash;   /* Internal hash */

  char *filename; 
  char *aliasname;

  int numbins;

  int readbufsize;
  int writebufsize;

  int prealloc;
  int extent;

  int compression;     /* 0=none or via pipe; 1,2,..,5,..=internal packing method */
  int blocksize;       /* Max. no. of words to pack in one go (specific to internal packing) */

  int concat;          /* Request for file concatenation option across PE (for example)
			  0=no concat; >0 no. of individual files forming a concatenated file.
			  For write-only files : concatenate "concat" no. of files into one;
			  For read-only files  : can be used to cross check the concatenation */

  int maxproc;         /* If > 0, then filename or aliasname may still contain %d's */
  int fromproc;        /* For use with %d in alias/filenames to specify applicable range of pools */
  int toproc;          /*         " "                                                             */

  char *pipecmd;

  struct ioassign_t *next;
} Ioassign;


extern int 
IOassign_read(const char  *IOASSIGN, 
	             int  *pbcount_out, 
	        Ioassign **ioassign_start_out, 
	        Ioassign **ioassign_out);

extern int 
IOassign_read_incore(const char  *IOASSIGN, 
	             int  *pbcount_out, 
		     Ioassign **ioassign_start_out, 
		     Ioassign **ioassign_out);

extern int 
IOassign_write(const char *IOASSIGN, 
                      int  pbcount,
	         Ioassign *ioassign_start); 

extern unsigned int
IOassign_hash(const char *s);

extern Ioassign *
IOassign_lookup(const char *s, Ioassign *pstart);


#endif
