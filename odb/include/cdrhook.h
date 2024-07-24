#ifndef _CDRHOOK_H_
#define _CDRHOOK_H_

/* This is near-replica of ifsaux/include/drhook.h for use (mainly) by generated C-codes.
   Keep macros consistent with the original !! */

extern int drhook_lhook;

/**** C-interface to Dr.Hook ****/

extern void
Dr_Hook(const char *name, int option, double *handle, 
        const char *filename, int sizeinfo,
        int name_len, int filename_len);

#define DRHOOK_START_RECUR(name,recur) \
  static const char *drhook_name = #name; \
  static const int drhook_name_len = sizeof(#name) - 1; /* Compile time eval */ \
  static const char *drhook_filename = __FILE__; \
  static const int drhook_filename_len = sizeof(__FILE__) - 1; /* Compile time eval */ \
  double zhook_handle; \
  if (!recur && drhook_lhook) Dr_Hook(drhook_name, 0, &zhook_handle, \
                                      drhook_filename, 0, \
                                      drhook_name_len, drhook_filename_len); {

#define DRHOOK_START(name) DRHOOK_START_RECUR(name,0)

#define DRHOOK_START_BY_STRING_RECUR(name, recur) \
  static const char *drhook_name = name; \
  static const int drhook_name_len = sizeof(name) - 1; /* Compile time eval */ \
  static const char *drhook_filename = __FILE__; \
  static const int drhook_filename_len = sizeof(__FILE__) - 1; /* Compile time eval */ \
  double zhook_handle; \
  if (!recur && drhook_lhook) Dr_Hook(drhook_name, 0, &zhook_handle, \
                                      drhook_filename, 0, \
                                      drhook_name_len, drhook_filename_len); {

#define DRHOOK_START_BY_STRING(name) DRHOOK_START_BY_STRING_RECUR(name,0)

#define DRHOOK_RETURN_RECUR(sizeinfo,recur) \
  if (!recur && drhook_lhook) Dr_Hook(drhook_name, 1, &zhook_handle, \
                                      drhook_filename, sizeinfo, \
                                      drhook_name_len, drhook_filename_len)

#define DRHOOK_RETURN(sizeinfo) DRHOOK_RETURN_RECUR(sizeinfo,0)

#define DRHOOK_END_RECUR(sizeinfo,recur) ; } DRHOOK_RETURN_RECUR(sizeinfo,recur)

#define DRHOOK_END(sizeinfo) DRHOOK_END_RECUR(sizeinfo,0) 

#endif  /* _CDRHOOK_H_ */
