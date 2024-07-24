#ifndef _VPARAM_H_
#define _VPARAM_H_

#include <stdarg.h>

/* odbi_client.c */

void Vparam(void *v, int is_a_query, char *fmt, va_list ap);

#endif /* _VPARAM_H_ */


