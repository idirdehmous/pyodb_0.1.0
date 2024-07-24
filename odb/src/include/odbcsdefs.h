#ifndef _ODBCSDEFS_H_
#define _ODBCSDEFS_H_

/* odbcsdefs.h */

#include <unistd.h>
#include <sys/types.h>

#define MIN_PORT 10000
#define MAX_PORT 20000

#define DB_DEFAULT   "ECMA"
#define HOST_DEFAULT "127.0.0.1"
#define PORT_DEFAULT  ((getuid()%(MAX_PORT-MIN_PORT)) + MIN_PORT)
#define TIMEOUT_DEFAULT 3600
#define DATAPATH_DEFAULT "/tmp"
#define SERVER_SELF_TIMEOUT(x) (2 * (x))
#define LISTEN_QUEUE_LENGTH_DEFAULT 64

#endif /* _ODBCSDEFS_H_ */
