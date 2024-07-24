#ifndef _HISTORY_H_
#define _HISTORY_H_

/* history.h */

#include "alloc.h"
#include <unistd.h>
#include <ctype.h>
#include <time.h>

extern const char *AddHistory(const char *cmd, const int *Date, const int *Time);
extern const char *GetHistory(int next);
extern void PrintHistory(FILE *fp);
extern char *LoadHistory(const char *name);
extern char *SaveHistory(const char *name, int nlastlines);
extern void DelHistory();

#endif /* _HISTORY_H_ */
