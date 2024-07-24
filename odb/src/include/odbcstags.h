#ifndef _ODBCSTAGS_H_
#define _ODBCSTAGS_H_

/* odbcstags.h */

#include "setodbcs.h"

#ifdef ODBCS

#define ODBCS_TAG_DB_METADATA            0
#define ODBCS_TAG_PREPARE_QUERY          1
#define ODBCS_TAG_EXECUTE_QUERY          2
#define ODBCS_TAG_BIND_PARAM             3
#define ODBCS_TAG_CONNECT                4
#define ODBCS_TAG_BYEBYE                 5
#define ODBCS_TAG_TIMEOUT                6
#define ODBCS_TAG_VERBOSE                7
#define ODBCS_TAG_FETCHFILE              8

static const char *
odbcs_tagname[] = {
  "DB_METADATA",
  "PREPARE_QUERY",
  "EXECUTE_QUERY",
  "BIND_PARAM",
  "CONNECT",
  "BYEBYE",
  "TIMEOUT",
  "VERBOSE",
  "FETCHFILE"
};

#define ODBCS_MAXTAGS (sizeof(odbcs_tagname)/sizeof(*odbcs_tagname))

#endif

#endif /* _ODBCSTAGS_H_ */
