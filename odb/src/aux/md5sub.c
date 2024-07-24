/*
 * This code implements the MD5 message-digest algorithm.
 * The algorithm is due to Ron Rivest.	This code was
 * written by Colin Plumb in 1993, no copyright is claimed.
 * This code is in the public domain; do with it what you wish.
 *
 * Equivalent code is available from RSA Data Security, Inc.
 * This code has been tested against that, and is equivalent,
 * except that you don't need to include two pages of legalese
 * with every copy.
 *
 * To compute the message digest of a chunk of bytes, declare an
 * MD5Context structure, pass it to MD5Init, call MD5Update as
 * needed on buffers full of bytes, and then call MD5Final, which
 * will fill a supplied 16-byte array with the digest.
 */

/* Brutally hacked by John Walker back from ANSI C to K&R (no
   prototypes) to maintain the tradition that Netfone will compile
   with Sun's original "cc". */

/* Back to ANSI-C by Sami Saarinen, ECMWF */

#include <ctype.h>
#include "odbmd5.h"

PRIVATE void MD5Transform(uint32 buf[4], uint32 in[16]);

#ifdef LITTLE
int odbmd5_is_little = 1;
#else
int odbmd5_is_little = 0;
#endif

#define PRTFUNC if (!psign) fprintf

PUBLIC int MD5_str2sign(const char *s, unsigned char sign[16])
{
  char *argv[2];
  int rc = 0;
  int arglen = 2 + STRLEN(s) + 1;
  char *arg = NULL;
  ALLOC(arg, arglen);
  snprintf(arg, arglen, "-D%s", s);
  argv[0] = "<undef>";
  argv[1] = arg;
  rc = MD5_signature(2, argv, sign);
  FREE(arg);
  return rc;
}

PUBLIC int MD5_signature(int argc, char *argv[], unsigned char sign[16])
{
  int i, j, opt, cdata = false, docheck = false, showfile = true, f = 0;
  unsigned int bp;
  char *cp, *clabel, *ifname, *hexfmt = "%02X";
  FILE *in = stdin;
  FILE *out = stdout;
  unsigned char buffer[IO_BUFSIZE_DEFAULT];
  unsigned char signature[16], csig[16];
  struct MD5Context md5c;
  unsigned char *psign = sign;

  if (psign) {
    memset(psign, 0, 16);
  }

  for (i = 1; i < argc; i++) {
    cp = argv[i];
    if (*cp == '-') {
      if (STRLEN(cp) == 1) {
	i++;
	break;	    	      /* -  --  Mark end of options; balance are files */
      }
      opt = *(++cp);
      if (islower(opt)) opt = toupper(opt);

      switch (opt) {

      case 'C':             /* -Csignature  --  Check signature, set return code */
	docheck = true;
	if (STRLEN(cp + 1) != 32) {
	  docheck = false;
	}
	memset(csig, 0, 16);
	clabel = cp + 1;
	for (j = 0; j < 16; j++) {
	  if (isxdigit((int) clabel[0]) && isxdigit((int) clabel[1]) &&
	      sscanf((cp + 1 + (j * 2)), hexfmt, &bp) == 1) {
	    csig[j] = (unsigned char) bp;
	  } 
	  else {
	    docheck = false;
	    break;
	  }
	  clabel += 2;
	}
	if (!docheck) {
	  PRTFUNC(stderr, "Error in signature specification.  Must be 32 hex digits.\n");
	  return 2;
	}
	break;

      case 'D':             /* -Dtext  --  Compute signature of given text */
	MD5Init(&md5c);
	MD5Update(&md5c, (unsigned char *) (cp + 1), STRLEN(cp + 1));
	cdata = true;
	f++;	      /* Mark no infile argument needed */
	break;
		    
      case 'L':   	      /* -L  --  Use lower case letters as hex digits */
	hexfmt = "%02x";
	break;

      case 'N':   	      /* -N  --  Don't show file name after sum */
	showfile = false;
	break;
		    
      case 'O':   	      /* -Ofname  --  Write output to fname (- = stdout) */
	cp++;
	if (strcmp(cp, "-") != 0) {
	  if (out != stdout) {
	    PRTFUNC(stderr, "Redundant output file specification.\n");
	    return 2;
	  }
	  if ((out = fopen(cp, "w")) == NULL) {
	    PRTFUNC(stderr, "Cannot open output file %s\n", cp);
	    return 2;
	  }
	}
	break;

      case '?':             /* -U, -? -H  --  Print how to call information. */
      case 'H':
      case 'U':
	PRTFUNC(stdout,"\nMD5  --  Calculate MD5 signature of file.  Usage:");
	PRTFUNC(stdout,"\n         odbmd5sum [ options ] [file ...]");
	PRTFUNC(stdout,"\n");
	PRTFUNC(stdout,"\n         options:");
	PRTFUNC(stdout,"\n              -csig   Check against sig, set exit status 0 = OK");
	PRTFUNC(stdout,"\n              -dtext  Compute signature of text argument");
	PRTFUNC(stdout,"\n              -l      Use lower case letters for hexadecimal digits");
	PRTFUNC(stdout,"\n              -n      Do not show file name after sum");
	PRTFUNC(stdout,"\n              -ofname Write output to fname (- = stdout)");
	PRTFUNC(stdout,"\n              -u      Print this message");
	PRTFUNC(stdout,"\n              -v      Print version information");
#if 0
	PRTFUNC(stdout,"\n");
	PRTFUNC(stdout,"\nby John Walker  --  http://www.fourmilab.ch/");
	PRTFUNC(stdout,"\nVersion %s\n", codb_versions_(NULL,NULL,NULL,NULL));
	PRTFUNC(stdout,"\nThis program is in the public domain.\n");
	PRTFUNC(stdout,"\n");
#endif
	return sign ? 3 : 0;
		    
      case 'V':   	      /* -V  --  Print version number */
	PRTFUNC(stdout,"%s\n", codb_versions_(NULL,NULL,NULL,NULL));
	return sign ? 3 : 0;
      }
    } 
    else {
      break;
    }
  }
    
  if (cdata && (i < argc)) {
    PRTFUNC(stderr, "Cannot specify both -d option and input file.\n");
    return 2;
  }
    
  if ((i >= argc) && (f == 0)) {
    f++;
  }
    
  for (; (f > 0) || (i < argc); i++) {
    if ((!cdata) && (f > 0)) {
      ifname = "-";
    } 
    else {
      ifname = argv[i];
    }
    f = 0;

    if (!cdata) {
	
      /* If the data weren't supplied on the command line with
	 the "-d" option, read it now from the input file. */
	
      if (strcmp(ifname, "-") != 0) {
	if ((in = fopen(ifname, "rb")) == NULL) {
	  PRTFUNC(stderr, "Cannot open input file %s\n", ifname);
	  return 2;
	}
      } 
      else {
	in = stdin;
      }

#ifdef _WIN32
      /** Warning!  On systems which distinguish text mode and
	  binary I/O (MS-DOS, Macintosh, etc.) the modes in the open
	  statement for "in" should have forced the input file into
	  binary mode.  But what if we're reading from standard
	  input?  Well, then we need to do a system-specific tweak
	  to make sure it's in binary mode.  While we're at it,
	  let's set the mode to binary regardless of however fopen
	  set it.

	  The following code, conditional on _WIN32, sets binary
	  mode using the method prescribed by Microsoft Visual C 7.0
	  ("Monkey C"); this may require modification if you're
	  using a different compiler or release of Monkey C.	If
	  you're porting this code to a different system which
	  distinguishes text and binary files, you'll need to add
	  the equivalent call for that system. */

      _setmode(_fileno(in), _O_BINARY);
#endif
    
      MD5Init(&md5c);
      while ((j = (int) fread(buffer, 1, sizeof(buffer), in)) > 0) {
	MD5Update(&md5c, buffer, (unsigned) j);
      }
    }
    MD5Final(signature, &md5c);

    if (docheck) {
      docheck = false;
      for (j = 0; j < sizeof(signature); j++) {
	if (signature[j] != csig[j]) {
	  docheck = true;
	  break;
	}
      }
      if (i < (argc - 1)) {
	PRTFUNC(stderr, "Only one file may be tested with the -c option.\n");
	return 2;
      }
    } 
    else if (psign) {
      memcpy(psign, signature, 16);
    }
    else {
      for (j = 0; j < sizeof(signature); j++) {
	PRTFUNC(out, hexfmt, signature[j]);
      }
      if ((!cdata) && showfile) {
	PRTFUNC(out, "  %s", (in == stdin) ? "-" : ifname);
      }
      PRTFUNC(out, "\n");
    }
  }

  return docheck ? 1 : 0;
}


PUBLIC char *MD5_sign2hex(const unsigned char sign[16], int lowercase)
{
  char *str = NULL;
  if (sign) {
    int j;
    char tmp[33];
    char *hexfmt = (lowercase) ? "%02x" : "%02X";
    char *ptmp = tmp;
    for (j=0; j<16; j++) {
      snprintf(ptmp, 3, hexfmt, sign[j]);
      ptmp += 2;
    }
    *ptmp = '\0';
    str = STRDUP(tmp);
  }
  return str;
}


PRIVATE void byteReverse(unsigned char *buf, unsigned int longs)
{
  /* Not called on little-endian machine */
  uint32 t;
  do {
    t = (uint32) ((unsigned int) buf[3] << 8 | buf[2]) << 16 |
      ((unsigned int) buf[1] << 8 | buf[0]);
    *(uint32 *) buf = t;
    buf += 4;
  } while (--longs);
}

#define BYTEREV(buf, longs) if (!odbmd5_is_little) byteReverse(buf, longs)


/*
 * Start MD5 accumulation.  Set bit count to 0 and buffer to mysterious
 * initialization constants.
 */

PUBLIC void MD5Init(struct MD5Context *ctx)
{
  ctx->buf[0] = 0x67452301;
  ctx->buf[1] = 0xefcdab89;
  ctx->buf[2] = 0x98badcfe;
  ctx->buf[3] = 0x10325476;
  
  ctx->bits[0] = 0;
  ctx->bits[1] = 0;
}

/*
 * Update context to reflect the concatenation of another buffer full
 * of bytes.
 */

PUBLIC void MD5Update(struct MD5Context *ctx, unsigned char *buf, unsigned int len)
{
  uint32 t;
  
  /* Update bitcount */
  
  t = ctx->bits[0];
  if ((ctx->bits[0] = t + ((uint32) len << 3)) < t)
    ctx->bits[1]++; 	/* Carry from low to high */
  ctx->bits[1] += len >> 29;
  
  t = (t >> 3) & 0x3f;	/* Bytes already in shsInfo->data */
  
  /* Handle any leading odd-sized chunks */
  
  if (t) {
    unsigned char *p = (unsigned char *) ctx->in + t;
    
    t = 64 - t;
    if (len < t) {
      memcpy(p, buf, len);
      return;
    }
    memcpy(p, buf, t);
    BYTEREV(ctx->in, 16);
    MD5Transform(ctx->buf, (uint32 *) ctx->in);
    buf += t;
    len -= t;
  }

  /* Process data in 64-byte chunks */

  while (len >= 64) {
    memcpy(ctx->in, buf, 64);
    BYTEREV(ctx->in, 16);
    MD5Transform(ctx->buf, (uint32 *) ctx->in);
    buf += 64;
    len -= 64;
  }

  /* Handle any remaining bytes of data. */
  
  memcpy(ctx->in, buf, len);
}

/*
 * Final wrapup - pad to 64-byte boundary with the bit pattern 
 * 1 0* (64-bit count of bits processed, MSB-first)
 */

PUBLIC void MD5Final(unsigned char digest[16], struct MD5Context *ctx)
{
  unsigned int count;
  unsigned char *p;
  
  /* Compute number of bytes mod 64 */
  count = (ctx->bits[0] >> 3) & 0x3F;
  
  /* Set the first char of padding to 0x80.  This is safe since there is
     always at least one byte free */
  p = ctx->in + count;
  *p++ = 0x80;
  
  /* Bytes of padding needed to make 64 bytes */
  count = 64 - 1 - count;
  
  /* Pad out to 56 mod 64 */
  if (count < 8) {
    /* Two lots of padding:  Pad the first block to 64 bytes */
    memset(p, 0, count);
    BYTEREV(ctx->in, 16);
    MD5Transform(ctx->buf, (uint32 *) ctx->in);
    
    /* Now fill the next block with 56 bytes */
    memset(ctx->in, 0, 56);
  } else {
    /* Pad block to 56 bytes */
    memset(p, 0, count - 8);
  }
  BYTEREV(ctx->in, 14);

  /* Append length in bits and transform */
  ((uint32 *) ctx->in)[14] = ctx->bits[0];
  ((uint32 *) ctx->in)[15] = ctx->bits[1];
  
  MD5Transform(ctx->buf, (uint32 *) ctx->in);
  BYTEREV((unsigned char *) ctx->buf, 4);
  memcpy(digest, ctx->buf, 16);
  memset(ctx, 0, sizeof(ctx));        /* In case it's sensitive */
}


/* The four core functions - F1 is optimized somewhat */

/* #define F1(x, y, z) (x & y | ~x & z) */
#define F1(x, y, z) (z ^ (x & (y ^ z)))
#define F2(x, y, z) F1(z, x, y)
#define F3(x, y, z) (x ^ y ^ z)
#define F4(x, y, z) (y ^ (x | ~z))

/* This is the central step in the MD5 algorithm. */
#define MD5STEP(f, w, x, y, z, data, s) \
	( w += f(x, y, z) + data,  w = w<<s | w>>(32-s),  w += x )

/*
 * The core of the MD5 algorithm, this alters an existing MD5 hash to
 * reflect the addition of 16 longwords of new data.  MD5Update blocks
 * the data and converts bytes into longwords for this routine.
 */

PRIVATE void MD5Transform(uint32 buf[4], uint32 in[16])
{
  register uint32 a, b, c, d;
  
  a = buf[0];
  b = buf[1];
  c = buf[2];
  d = buf[3];

  MD5STEP(F1, a, b, c, d, in[0] + 0xd76aa478, 7);
  MD5STEP(F1, d, a, b, c, in[1] + 0xe8c7b756, 12);
  MD5STEP(F1, c, d, a, b, in[2] + 0x242070db, 17);
  MD5STEP(F1, b, c, d, a, in[3] + 0xc1bdceee, 22);
  MD5STEP(F1, a, b, c, d, in[4] + 0xf57c0faf, 7);
  MD5STEP(F1, d, a, b, c, in[5] + 0x4787c62a, 12);
  MD5STEP(F1, c, d, a, b, in[6] + 0xa8304613, 17);
  MD5STEP(F1, b, c, d, a, in[7] + 0xfd469501, 22);
  MD5STEP(F1, a, b, c, d, in[8] + 0x698098d8, 7);
  MD5STEP(F1, d, a, b, c, in[9] + 0x8b44f7af, 12);
  MD5STEP(F1, c, d, a, b, in[10] + 0xffff5bb1, 17);
  MD5STEP(F1, b, c, d, a, in[11] + 0x895cd7be, 22);
  MD5STEP(F1, a, b, c, d, in[12] + 0x6b901122, 7);
  MD5STEP(F1, d, a, b, c, in[13] + 0xfd987193, 12);
  MD5STEP(F1, c, d, a, b, in[14] + 0xa679438e, 17);
  MD5STEP(F1, b, c, d, a, in[15] + 0x49b40821, 22);
  
  MD5STEP(F2, a, b, c, d, in[1] + 0xf61e2562, 5);
  MD5STEP(F2, d, a, b, c, in[6] + 0xc040b340, 9);
  MD5STEP(F2, c, d, a, b, in[11] + 0x265e5a51, 14);
  MD5STEP(F2, b, c, d, a, in[0] + 0xe9b6c7aa, 20);
  MD5STEP(F2, a, b, c, d, in[5] + 0xd62f105d, 5);
  MD5STEP(F2, d, a, b, c, in[10] + 0x02441453, 9);
  MD5STEP(F2, c, d, a, b, in[15] + 0xd8a1e681, 14);
  MD5STEP(F2, b, c, d, a, in[4] + 0xe7d3fbc8, 20);
  MD5STEP(F2, a, b, c, d, in[9] + 0x21e1cde6, 5);
  MD5STEP(F2, d, a, b, c, in[14] + 0xc33707d6, 9);
  MD5STEP(F2, c, d, a, b, in[3] + 0xf4d50d87, 14);
  MD5STEP(F2, b, c, d, a, in[8] + 0x455a14ed, 20);
  MD5STEP(F2, a, b, c, d, in[13] + 0xa9e3e905, 5);
  MD5STEP(F2, d, a, b, c, in[2] + 0xfcefa3f8, 9);
  MD5STEP(F2, c, d, a, b, in[7] + 0x676f02d9, 14);
  MD5STEP(F2, b, c, d, a, in[12] + 0x8d2a4c8a, 20);
  
  MD5STEP(F3, a, b, c, d, in[5] + 0xfffa3942, 4);
  MD5STEP(F3, d, a, b, c, in[8] + 0x8771f681, 11);
  MD5STEP(F3, c, d, a, b, in[11] + 0x6d9d6122, 16);
  MD5STEP(F3, b, c, d, a, in[14] + 0xfde5380c, 23);
  MD5STEP(F3, a, b, c, d, in[1] + 0xa4beea44, 4);
  MD5STEP(F3, d, a, b, c, in[4] + 0x4bdecfa9, 11);
  MD5STEP(F3, c, d, a, b, in[7] + 0xf6bb4b60, 16);
  MD5STEP(F3, b, c, d, a, in[10] + 0xbebfbc70, 23);
  MD5STEP(F3, a, b, c, d, in[13] + 0x289b7ec6, 4);
  MD5STEP(F3, d, a, b, c, in[0] + 0xeaa127fa, 11);
  MD5STEP(F3, c, d, a, b, in[3] + 0xd4ef3085, 16);
  MD5STEP(F3, b, c, d, a, in[6] + 0x04881d05, 23);
  MD5STEP(F3, a, b, c, d, in[9] + 0xd9d4d039, 4);
  MD5STEP(F3, d, a, b, c, in[12] + 0xe6db99e5, 11);
  MD5STEP(F3, c, d, a, b, in[15] + 0x1fa27cf8, 16);
  MD5STEP(F3, b, c, d, a, in[2] + 0xc4ac5665, 23);
  
  MD5STEP(F4, a, b, c, d, in[0] + 0xf4292244, 6);
  MD5STEP(F4, d, a, b, c, in[7] + 0x432aff97, 10);
  MD5STEP(F4, c, d, a, b, in[14] + 0xab9423a7, 15);
  MD5STEP(F4, b, c, d, a, in[5] + 0xfc93a039, 21);
  MD5STEP(F4, a, b, c, d, in[12] + 0x655b59c3, 6);
  MD5STEP(F4, d, a, b, c, in[3] + 0x8f0ccc92, 10);
  MD5STEP(F4, c, d, a, b, in[10] + 0xffeff47d, 15);
  MD5STEP(F4, b, c, d, a, in[1] + 0x85845dd1, 21);
  MD5STEP(F4, a, b, c, d, in[8] + 0x6fa87e4f, 6);
  MD5STEP(F4, d, a, b, c, in[15] + 0xfe2ce6e0, 10);
  MD5STEP(F4, c, d, a, b, in[6] + 0xa3014314, 15);
  MD5STEP(F4, b, c, d, a, in[13] + 0x4e0811a1, 21);
  MD5STEP(F4, a, b, c, d, in[4] + 0xf7537e82, 6);
  MD5STEP(F4, d, a, b, c, in[11] + 0xbd3af235, 10);
  MD5STEP(F4, c, d, a, b, in[2] + 0x2ad7d2bb, 15);
  MD5STEP(F4, b, c, d, a, in[9] + 0xeb86d391, 21);

  buf[0] += a;
  buf[1] += b;
  buf[2] += c;
  buf[3] += d;
}
