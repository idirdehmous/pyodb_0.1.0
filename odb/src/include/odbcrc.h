/* odbcrc.h */

extern unsigned int
ODB_pp_cksum32(int nbuf, unsigned int nCRC);

extern unsigned int
ODB_pp_cksum32but64len(long long int nbuf, unsigned int nCRC);

extern unsigned long long int
ODB_pp_cksum64(long long int nbuf, unsigned long long int nCRC);

extern unsigned int
ODB_cksum32(const char *buf, int nbuf, unsigned int nCRC);

extern unsigned long long int
ODB_cksum64(const char *buf, long long int nbuf, unsigned long long int nCRC);

extern void
fodb_crc32_(const void *vbuf, const int *pnbuf,
	    unsigned int *pnCRC /* Note: An in & out -variable */);

extern void
fodb_crc64_(const void *vbuf, const long long int *pnbuf,
	    unsigned long long int *pnCRC /* Note: An in & out -variable */);
