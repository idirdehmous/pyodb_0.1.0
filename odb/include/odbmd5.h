
/* odbmd5.h */

#ifndef ODBMD5_H
#define ODBMD5_H

#include "alloc.h"
#include "magicwords.h"

typedef unsigned int uint32;

struct MD5Context {
        uint32 buf[4];
        uint32 bits[2];
        unsigned char in[64];
};

extern void MD5Init(struct MD5Context *ctx);
extern void MD5Update(struct MD5Context *ctx, unsigned char *buf, unsigned int len);
extern void MD5Final(unsigned char digest[16], struct MD5Context *ctx);

extern int MD5_signature(int argc, char *argv[], unsigned char sign[16]);
extern char *MD5_sign2hex(const unsigned char sign[16], int lowercase);
extern int MD5_str2sign(const char *s, unsigned char sign[16]);

#endif
