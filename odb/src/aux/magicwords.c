
/* magicwords.c */

#include "alloc.h"
#include "magicwords.h"

/* Access functions; also Fortran-callable */

void get_magic_pcma_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? PCMA : AMCP;
}

void get_magic_odb__(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? ODB_ : _BDO;
}

void get_magic_odbi_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? ODBI : IBDO;
}

void get_magic_mr2d_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? MR2D : D2RM;
}

void get_magic_hc32_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? HC32 : _23CH;
}

void get_magic_algn_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? ALGN : NGLA;
}

void get_magic_mmry_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? MMRY : YRMM;
}

void get_magic_idxb_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? IDXB : BXDI;
}

void get_magic_idxt_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? IDXT : TXDI;
}

void get_magic_dca2_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? DCA2 : _2ACD;
}

void get_magic_ocac_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? OCAC : CACO;
}

void get_magic_odbx_(const int *reversed, unsigned int *value)
{
  *value = (!*reversed) ? ODBX : XBDO;
}

/* Compressed file auto-detection (see aux/cma_open.c & ioknowncmd.c) */

#include <string.h>
#include <stdlib.h>

static const char *Compression_schemes[4] = {
  "#ddgzip", /* w/o dd I/O-buffering : "#gzip" */
  "#pack",
  "#compress",
  "#zip"
};

const char *
Compression_Suffix_Check(const char *filename)
{
  const char *scheme_change = NULL;
  const char *suffix = filename ? strrchr(filename, '.') : NULL;
  if (suffix) {
    if (strequ(suffix,".gz")) {
      /* This looks like a gzipped file !! */
      scheme_change = Compression_schemes[0];
    }
    else if (strequ(suffix,".z")) {
      /* This looks like a pack'ed file !! */
      scheme_change = Compression_schemes[1];
    }
    else if (strequ(suffix,".Z")) {
      /* This looks like a compress'ed file !! */
      scheme_change = Compression_schemes[2];
    }
    else if (strequ(suffix,".zip")) {
      /* This looks like a pkzip'ed (zip) file !! */
      scheme_change = Compression_schemes[3];
    }
  }
  return scheme_change;
}

const char *
Compression_Magic_Check(FILE *fp, unsigned int *first_word, int close_fp)
{
  const char *scheme_change = NULL;
  if (fp && first_word) {
    /* Borrowed from gzip-1.2.4 source code, file 'gzip.h' (there they were macro-keywords) */
    static const char PACK_MAGIC[]     = "\037\036"; /* Magic header for packed files */
    static const char GZIP_MAGIC[]     = "\037\213"; /* Magic header for gzip files, 1F 8B */
    static const char OLD_GZIP_MAGIC[] = "\037\236"; /* Magic header for gzip 0.5 = freeze 1.x */
    static const char LZH_MAGIC[]      = "\037\240"; /* Magic header for SCO LZH Compress files*/
    static const char PKZIP_MAGIC[]    = "\120\113\003\004"; /* Magic header for pkzip files */

    const unsigned char *magic = (const unsigned char *)first_word;
    fread(first_word, sizeof(*first_word), 1, fp);
  
    if (memcmp(magic, GZIP_MAGIC, 2) == 0 || 
	memcmp(magic, OLD_GZIP_MAGIC, 2) == 0) {
      /* This looks like a gzipped file !! */
      scheme_change = Compression_schemes[0];
    }
    else if (memcmp(magic, PACK_MAGIC, 2) == 0) {
      /* This looks like a pack'ed file !! */
      scheme_change = Compression_schemes[1];
    }
    else if (memcmp(magic, LZH_MAGIC, 2) == 0) {
      /* This looks like a compress'ed file !! */
      scheme_change = Compression_schemes[2];
    }
    else if (memcmp(magic, PKZIP_MAGIC, 4) == 0) {
      /* This looks like a pkzip'ed (zip) file !! */
      scheme_change = Compression_schemes[3];
    }
    if (close_fp) fclose(fp);
  }
  return scheme_change;
}

/* Maximum # of columns permitted */

/* Note: Shared between ODB/SQL compiler and libodb.a
   If you make sure you're consistent, use the same ODB_MAXCOLS to override */

int ODB_maxcols()
{
  static int Maxcols = 99999; /* Reasonably big, ugh ? */
  static int first_time = 1;
  if (first_time) { /* Not thread safe, but used by ODB/SQL compiler, too */
    char *env = getenv("ODB_MAXCOLS");
    int value = Maxcols;
    if (env) value = atoi(env);
    if (value > 1000 && value < 1000000000) Maxcols = value;
    first_time = 0;
  }
  return Maxcols;
}

void codb_maxcols_(int *maxcols)
{
  if (maxcols) *maxcols = ODB_maxcols();
}
