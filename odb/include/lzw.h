#ifndef _LZW_H_
#define _LZW_H_

#include <stdio.h>
#include <string.h>
#ifdef USE_MALLOC_H
#include <malloc.h>
#endif
#include <stdlib.h>
#include <signal.h>

/*
   ECMWF's LZW packing bugfixed by Sami Saarinen 1-Dec-2000
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

typedef int           John;   /* could be long, too; 32-bit minimum */
typedef unsigned int  uJohn;  /* could be unsigned long, too; 32-bit minimum */

#define NUM8 8L

static John   incount = 0;
static John   outcount = 0;
static John   bitio_used   = 0;
static uJohn bitio_buffer = 0;

static John     errflg = 0;
static John     outlen = 0;
static John     inlen = 0;
static unsigned char *outbuffer = 0;

static unsigned char masks[] =  {
  0x00, 0x01, 0x03, 0x07, 0x0F, 0x1F, 0x3F, 0x7F, 0xFF,
};

/*====================================================================*/

#define OUT(x,str,fail) \
{unsigned char _c = x; \
 if (++outcount>outlen) {errflg++; if (fail) Rabort(str,__FILE__,__LINE__);} \
 else *outbuffer++ = _c; }

#define NEXTBYTE(c,buf,str) \
{ if (++incount>inlen) {errflg++; Rabort(str,__FILE__,__LINE__);} \
  else c = *buf++; }


#define CODE(str) (code < CODE_MAX) ? code : Rabort(str,__FILE__,__LINE__)

/*====================================================================*/

static int
Rabort(const char *str, const char *file, int line)
{
  fprintf(stderr,"***Error: LZW packing/unpacking failed at %s:%s, line %d\n",file,str,line);
  RAISE(SIGABRT);
  exit(1);
  return 0;
}

static void 
write_code(uJohn code,
	   John nbits,
	   const char *str)
{
  if (nbits + bitio_used > sizeof(bitio_buffer)*NUM8) {
    while (bitio_used >= NUM8) {
      unsigned char c = (bitio_buffer >> (NUM8*sizeof(bitio_buffer) - NUM8));
      OUT(c,str,0);
      bitio_buffer <<= NUM8;
      bitio_used    -= NUM8;
    }
  }
  code <<= (sizeof(code)*NUM8 - nbits - bitio_used);
  bitio_buffer += code;
  bitio_used   += nbits;
}

static void 
flush_code(void)
{
  while (bitio_used > 0) {
    unsigned char c = (bitio_buffer >> (NUM8*sizeof(bitio_buffer) - NUM8));
    OUT(c,"flush_code",0);
    bitio_buffer <<= NUM8;
    bitio_used    -= NUM8;
  }
}

static void 
init_table(register int *table,
	   register int size)
{
  while (size--) *table++ = -1;
}


static uJohn 
read_code(const unsigned char *p,
	  John *bitp,
	  John nbits)
{
  John    len  = nbits;
  John    skip = *bitp;
  int         s = skip%8;
  int         n = 8-s;
  uJohn  ret = 0;
  const unsigned char *p_orig = p;

  skip = (skip >> 3);
  if (skip >= inlen) return CODE_EOI;

  p += skip; /* skip the bytes */
  
  if (s) {
    if (p - p_orig + 1 == inlen) return CODE_EOI;
    ret = masks[n] & *p++;
    len -= n;
  }
  
  while (len >= 8) {
    if (p - p_orig + 1 == inlen) return CODE_EOI;
    ret = (ret<<8) + *p++;
    len -= 8;
  }

  ret = (ret << len) + (*p >> (8-len));
  
  *bitp += nbits;

  if (p - p_orig >= inlen) {
    fprintf(stderr,"read_code: out of bounds by %d bytes\n",
	    p - p_orig - inlen + 1);
    Rabort("read_code",__FILE__,__LINE__);
  }
  
  return ret;
}


void
lzw_unpack_(const unsigned char ibuf[],
	    const John *Ilen,
	    unsigned char obuf[], 
	    const John *Olen,
	    John *retcode)
{
  John ilen = *Ilen;
  John olen = *Olen;
  short  prefix[HSIZE];
  unsigned char suffix[CODE_MAX+1];
  unsigned char stack [HSIZE];
  
  unsigned char *stackp = stack;
  John code,firstchar, oldcode, incode;
  int i;
  John bitp = 0;
  int maxcode;
  John  first_free;
  John  nbits;

  for(i=0;i<256;i++) suffix[i] = (unsigned char)i;
  
  /* Initialize globals */

  errflg       = 0;
  outbuffer    = obuf;
  outlen       = olen;
  inlen        = ilen;
  bitio_used   = 0;
  bitio_buffer = 0;
  outcount     = 0;
  incount      = 0;

  maxcode    = MAXCODE(BITS_MIN) - 1;
  nbits      = BITS_MIN;
  first_free = CODE_FIRST;

  while ((code = read_code(ibuf,&bitp,nbits)) != CODE_EOI) {
    if (code == CODE_CLEAR) {
      bzero(prefix, sizeof(prefix));
      
      nbits      = BITS_MIN;
      maxcode    = MAXCODE(BITS_MIN) - 1;
      first_free = CODE_FIRST;
      
      if ((code = read_code(ibuf,&bitp,nbits)) == CODE_EOI) break;
      
      OUT(code,"lzw_decode[code]",1);
      
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
      OUT(*--stackp,"lzw_decode[stackp]",1);
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
  } /* while ((code = read_code(ibuf,&bitp,nbits)) != CODE_EOI) */

  *retcode = errflg ? -outcount : outcount;
}


void
lzw_pack_(const unsigned char ibuf[], 
	  const John *Ilen,
	  unsigned char obuf[],
	  const John *Olen,
	  John *retcode)
{
  John ilen = *Ilen;
  John olen = *Olen;
  John string;
  int h,disp,ent;
  int maxcode;
  John  first_free;
  John  nbits;
  
  int   stringtab[HSIZE];
  short codetab[HSIZE];

  bzero(codetab, sizeof(codetab));

  /* Initialize globals */
  
  errflg       = 0;
  outbuffer    = obuf;
  outlen       = olen;
  inlen        = ilen;
  bitio_used   = 0;
  bitio_buffer = 0;
  outcount     = 0;
  incount      = 0;
  
  nbits      = BITS_MIN;
  
  init_table(stringtab,HSIZE);
  first_free = CODE_FIRST;
  
  maxcode = MAXCODE(BITS_MIN);
  
  write_code(CODE_CLEAR,nbits,"lzw_encode[write_code(CODE_CLEAR)]");

  NEXTBYTE(ent,ibuf,"lzw_encode[ent]");

  while (--ilen > 0) {
    int c;
    
    NEXTBYTE(c,ibuf,"lzw_encode[c]");

    string = ((John)c << BITS_MAX) + ent;
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

    write_code(ent,nbits,"lzw_encode[write_code(ent)]");
    
    ent          = c;
    codetab[h]   = first_free++;
    stringtab[h] = string;
    
    if (first_free == CODE_MAX - 1) {
      /* table is full, emit clear code and reset */
      
      /* ratio = 0; */
      init_table(stringtab,HSIZE);
      first_free = CODE_FIRST;
      write_code(CODE_CLEAR,nbits,
      "lzw_encode[write_code(CODE_CLEAR;'if (first_free == CODE_MAX - 1)')]");
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

  write_code(ent,nbits,"lzw_encode[write_code(ent) at end]");
  write_code(CODE_EOI,nbits,"lzw_encode[write_code(CODE_EOI)]");
  flush_code();

  *retcode = errflg ? -outcount : outcount;
}

#endif
