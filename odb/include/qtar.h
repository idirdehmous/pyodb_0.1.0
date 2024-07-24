#ifndef _QTAR_H_
#define _QTAR_H_

/* qtar.h */

#include "odb.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extern int CloseQTAR(int fd);
extern int OpenQTAR();
extern int UpdateQTAR(int fd);
extern int ExtractQTAR(int fd);
extern int DeleteQTAR(int fd);
extern int TocQTAR(int fd);
extern int QTAR_main(int argc, char *argv[]);

#endif
