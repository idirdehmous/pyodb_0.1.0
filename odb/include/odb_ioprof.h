
/* odb_ioprof.h */

enum { 
  ioprof_read    = 1,
  ioprof_write   = 2,
  ioprof_open    = 4,
  ioprof_close   = 8,
  ioprof_iolock  = 16,
  ioprof_rmfile  = 32
};

extern void Profile_newio32_init(int myproc);
extern void Profile_newio32_start(int what, int n);
extern void Profile_newio32_end(int what, int n);
extern void Profile_newio32_flush();
