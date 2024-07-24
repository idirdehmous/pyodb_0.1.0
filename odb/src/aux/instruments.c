#define _GNU_SOURCE
#include <dlfcn.h>

#include <stdlib.h>
#include <stdio.h>

#define TRACE_FD 3

void __cyg_profile_func_enter (void *, void *)
   __attribute__((no_instrument_function));

void __cyg_profile_func_enter (void *func,  void *caller)
{
  static FILE* trace = NULL;
  Dl_info info;

  if (trace == NULL) {
    trace = fdopen(TRACE_FD, "w");
    if (trace == NULL) abort();
    setbuf(trace, NULL);
  }
  if (dladdr(func, &info))
    fprintf (trace, "%p [%s] %s\n",
             func,
             info.dli_fname ? info.dli_fname : "?",
             info.dli_sname ? info.dli_sname : "?");
}

