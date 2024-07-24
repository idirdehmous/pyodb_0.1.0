#ifndef _SETODBCS_H_
#define _SETODBCS_H_

/* setodbcs.h */

/* To set up the ODBCS-option properly */

#undef TEST_ODBCS

#ifdef ODBCS

#define TEST_ODBCS ODBCS

#else 

/* Add new ARCH here (defined(ARCH)) after checking their implementation on 
   TCP/IP & IPv4 communication exists.
   The new ARCH will then be automatically supported */

#if (defined(LINUX) && !defined(CRAYXT)) || defined(RS6K) || defined(SUN4) || defined(HPPA) || defined(SGI)
#define TEST_ODBCS 1
#else
#define TEST_ODBCS 0
#endif

#endif /* ODBCS */

#if TEST_ODBCS == 0
#undef ODBCS
#else
#undef ODBCS
#define ODBCS 1
#endif

#undef TEST_ODBCS

#endif /* _SETODBCS_H_ */
