
/* Lempel-Ziv-Welch -style of packing (non-vectorizable; some routines are ... now!) */

#include "pcma.h"
#include <signal.h>

/*
   ECMWF's LZW packing bugfixed by Sami Saarinen 1-Dec-2000
   Added OpenMP safety & some vectorization 30-Nov-2004
 */

#define MAXCODE(n)      ((1 << (n)) - 1)
#define CHECK_GAP       10000           /* enc_ratio check interval */

#define BITS_MIN        9               /* start with 9 bits */
#define BITS_MAX        12              /* max of 12 bit strings */
#define CODE_CLEAR      256             /* code to clear string table */
#define CODE_EOI        257             /* end-of-information code */
#define CODE_FIRST      258             /* first free code entry */
#define CODE_MAX        MAXCODE(BITS_MAX)
#if 1
#define HSIZE           9001            /* 91% occupancy */
#define HSHIFT          (8-(16-13))
#else
#define HSIZE           5003            /* 80% occupancy */
#define HSHIFT          (8-(16-12))
#endif

PRIVATE const unsigned char masks[] =  {
  0x00, 0x01, 0x03, 0x07, 0x0F, 0x1F, 0x3F, 0x7F, 0xFF,
};

/*====================================================================*/

#define OUT(x,str,fail) \
{unsigned char _c = x; \
 if (++outcount>outlen) {errflg++; if (fail) Rabort(str,__FILE__,__LINE__);} \
 else *outbuffer++ = _c; }

#define PTROUT(x,str,fail) \
{unsigned char _c = x; \
 if (++(*outcount)>(*outlen)) {(*errflg)++; if (fail) Rabort(str,__FILE__,__LINE__);} \
 else *outbuffer++ = _c; }

#define NEXTBYTE(c,buf,str) \
{ if (++incount>inlen) {errflg++; Rabort(str,__FILE__,__LINE__);} \
  else c = *buf++; }


#define CODE(str) (code < CODE_MAX) ? code : Rabort(str,__FILE__,__LINE__)

/*====================================================================*/

PRIVATE int
Rabort(const char *str, const char *file, int line)
{
  fprintf(stderr,"***Error: LZW packing/unpacking failed at %s:%s, line#%d\n",file,str,line);
  RAISE(SIGABRT);
  exit(1);
  return 0;
}

PRIVATE unsigned char *
write_code(unsigned int code,
           int nbits,
	   int           *errflg,
	   unsigned char *outbuffer,
	   int           *outlen,
           int           *bitio_used,
	   unsigned int  *bitio_buffer,
	   int           *outcount,
	   const char *str)
{
  if (nbits + (*bitio_used) > sizeof(*bitio_buffer)*8) {
    while (*bitio_used >= 8) {
      unsigned char c = ((*bitio_buffer) >> (8*sizeof(*bitio_buffer) - 8));
      PTROUT(c,str,0);
      *bitio_buffer <<= 8;
      *bitio_used    -= 8;
    }
  }
  code <<= (sizeof(code)*8 - nbits - (*bitio_used));
  *bitio_buffer += code;
  *bitio_used   += nbits;
  return outbuffer;
}

#define \
flush_code() \
{ \
  while (bitio_used > 0) { \
    unsigned char c = (bitio_buffer >> (8*sizeof(bitio_buffer) - 8)); \
    OUT(c,"flush_code",0); \
    bitio_buffer <<= 8; \
    bitio_used    -= 8; \
  } \
}

#define \
init_table(table, \
	   size) \
{ \
  int j; \
  /* The following routine is now inlined & vectorized (size == HSIZE) */ \
  /* while (size--) *table++ = -1; */ \
  for (j=0; j<size; j++) table[j] = -1; \
}


PRIVATE unsigned int 
read_code(const unsigned char *p,
	  int *bitp,
	  int nbits,
	  int inlen)
{
  int    len  = nbits;
  int    skip = *bitp;
  int         s = skip%8;
  int         n = 8-s;
  unsigned int  ret = 0;
  const unsigned char *p_orig = p;

  skip = (skip >> 3);
  if (skip >= inlen) { ret=CODE_EOI; goto finish; }

  p += skip; /* skip the bytes */
  
  if (s) {
    if (p - p_orig + 1 == inlen) { ret=CODE_EOI; goto finish; }
    ret = masks[n] & *p++;
    len -= n;
  }
  
  while (len >= 8) {
    if (p - p_orig + 1 == inlen) { ret=CODE_EOI; goto finish; }
    ret = (ret<<8) + *p++;
    len -= 8;
  }

  ret = (ret << len) + (*p >> (8-len));
  
  *bitp += nbits;

  if (p - p_orig >= inlen) {
    fprintf(stderr,"read_code: out of bounds by %lld bytes\n",
	    (long long int)(p - p_orig - inlen + 1));
    Rabort("read_code",__FILE__,__LINE__);
  }

 finish:  
  return ret;
}


void
lzw_unpack_(const unsigned char ibuf[],
	    const int *Ilen,
	    unsigned char obuf[], 
	    const int *Olen,
	    int *retcode)
{
  int ilen = *Ilen;
  int olen = *Olen;
  int prefix[HSIZE];
  unsigned char suffix[CODE_MAX+1];
  unsigned char stack [HSIZE];
  
  unsigned char *stackp = stack;
  int code,firstchar, oldcode, incode;
  int i;
  int bitp = 0;
  int maxcode;
  int first_free;
  int nbits;

  /* "globals" that used to be non-threadsafe */
  int            errflg       = 0;
  unsigned char *outbuffer    = obuf;
  int            outlen       = olen;
  int            inlen        = ilen;
  int            bitio_used   = 0;
  unsigned int   bitio_buffer = 0;
  int            outcount     = 0;
  int            incount      = 0;

  for (i=0; i<256; i++) suffix[i] = (unsigned char)i;
  
  maxcode    = MAXCODE(BITS_MIN) - 1;
  nbits      = BITS_MIN;
  first_free = CODE_FIRST;

  while ((code = read_code(ibuf,&bitp,nbits,inlen)) != CODE_EOI) {
    if (code == CODE_CLEAR) {
      bzero(prefix, sizeof(prefix));
      
      nbits      = BITS_MIN;
      maxcode    = MAXCODE(BITS_MIN) - 1;
      first_free = CODE_FIRST;
      
      if ((code = read_code(ibuf,&bitp,nbits,inlen)) == CODE_EOI) break;
      
      OUT(code,"lzw_unpack_[code]",1);
      
      oldcode = firstchar = code;
      continue;
    }

    incode = code;

    if (code >= first_free) {
      /* code not in table */
      *stackp++ = firstchar;
      if(stackp - stack > sizeof(stack)) {
	Rabort("Ooops.... error in lzw[*stackp++ = firstchar]",
	       __FILE__,__LINE__);
      }
      code = oldcode;
    }
    
    while (code >= 256) {
      *stackp++ = suffix[CODE("while (code >= 256):suffix")];
      if(stackp - stack > sizeof(stack)) {
	Rabort("Ooops.... error in lzw[*stackp++ = suffix[code]]",
	       __FILE__,__LINE__);
      }
      code      = prefix[CODE("while (code >= 256):prefix")];
    }

    *stackp++ = firstchar = suffix[CODE("*stackp++ = firstchar:suffix")];
    
    if(stackp - stack > sizeof(stack)) {
      Rabort("Ooops.... error in lzw[*stackp++ = firstchar = suffix[code]]",
	     __FILE__,__LINE__);
    }

    do {
      if (outcount >= outlen) break;
      OUT(*--stackp,"lzw_unpack_[stackp]",1);
    } while (stackp > stack);

    if ((code = first_free) < CODE_MAX) {
      prefix[CODE("if ((code = first_free) < CODE_MAX):prefix")] = oldcode;
      suffix[CODE("if ((code = first_free) < CODE_MAX):suffix")] = firstchar;
      first_free++;
      
      if (first_free > maxcode) {
	nbits++;
	if (nbits > BITS_MAX) nbits = BITS_MAX;
	maxcode = MAXCODE(nbits) - 1;
      }
    }

    oldcode = incode;
  } /* while ((code = read_code(ibuf,&bitp,nbits,inlen)) != CODE_EOI) */

  *retcode = errflg ? -outcount : outcount;
}


void
lzw_pack_(const unsigned char ibuf[], 
	  const int *Ilen,
	  unsigned char obuf[],
	  const int *Olen,
	  int *retcode)
{
  int ilen = *Ilen;
  int olen = *Olen;
  int string;
  int h,disp,ent;
  int maxcode;
  int first_free;
  int nbits;
  
  int stringtab[HSIZE];
  int codetab[HSIZE];

  /* "globals" that used to be non-threadsafe */
  int            errflg       = 0;
  unsigned char *outbuffer    = obuf;
  int            outlen       = olen;
  int            inlen        = ilen;
  int            bitio_used   = 0;
  unsigned int   bitio_buffer = 0;
  int            outcount     = 0;
  int            incount      = 0;

  bzero(codetab, sizeof(codetab));

  nbits      = BITS_MIN;
  
  init_table(stringtab,HSIZE);
  first_free = CODE_FIRST;
  
  maxcode = MAXCODE(BITS_MIN);

  outbuffer =
    write_code(CODE_CLEAR,nbits,
	       &errflg, outbuffer, &outlen, &bitio_used, &bitio_buffer, &outcount,
	       "lzw_pack_[write_code(CODE_CLEAR)]");

  NEXTBYTE(ent,ibuf,"lzw_pack_[ent]");

  while (--ilen > 0) {
    int c;
    
    NEXTBYTE(c,ibuf,"lzw_pack_[c]");

    string = ((int)c << BITS_MAX) + ent;
    h = (c << HSHIFT) ^ ent;        /* xor hashing */
    
    /* while(h<0) h+=HSIZE; h %= HSIZE; */
    
    if (stringtab[h] == string) {
      ent = codetab[h];
      continue;
    }

    if (stringtab[h] >= 0) {
      /*
       * Primary hash failed, check secondary hash.
       */
      disp = HSIZE - h;
      if (h == 0)
	disp = 1;
      do {
	if ((h -= disp) < 0)
	  h += HSIZE;
	if (stringtab[h] == string) {
	  ent = codetab[h];
	  goto hit;
	}
      } while (stringtab[h] >= 0);
    }

    /*
     * New entry, emit code and add to table.
     */

    outbuffer =
      write_code(ent,nbits,
		 &errflg, outbuffer, &outlen, &bitio_used, &bitio_buffer, &outcount,
		 "lzw_pack_[write_code(ent)]");
    
    ent          = c;
    codetab[h]   = first_free++;
    stringtab[h] = string;
    
    if (first_free == CODE_MAX - 1) {
      /* table is full, emit clear code and reset */
      
      /* ratio = 0; */
      init_table(stringtab,HSIZE);
      first_free = CODE_FIRST;
      outbuffer =
	write_code(CODE_CLEAR,nbits,
		   &errflg, outbuffer, &outlen, &bitio_used, &bitio_buffer, &outcount,
		   "lzw_pack_[write_code(CODE_CLEAR;'if (first_free == CODE_MAX - 1)')]");
      nbits   = BITS_MIN;
      maxcode =  MAXCODE(BITS_MIN);
      
    } else {
      
      /*
       * If the next entry is going to be too big for
       * the code size, then increase it, if possible.
       */
      
      if (first_free > maxcode) {
	nbits++;
	if(nbits > BITS_MAX) Rabort("nbits > BITS_MAX",__FILE__,__LINE__);
	maxcode =  MAXCODE(nbits);
      }
    }

hit:
    ;
  }

  outbuffer =
    write_code(ent,nbits,
	       &errflg, outbuffer, &outlen, &bitio_used, &bitio_buffer, &outcount,
	       "lzw_pack_[write_code(ent) at end]");
  outbuffer =
    write_code(CODE_EOI,nbits,
	       &errflg, outbuffer, &outlen, &bitio_used, &bitio_buffer, &outcount,
	       "lzw_pack_[write_code(CODE_EOI)]");
  flush_code();

  *retcode = errflg ? -outcount : outcount;
}
