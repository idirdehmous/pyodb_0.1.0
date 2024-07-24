#ifndef _NEWIO_H_
#define _NEWIO_H_

/* newio.h */

#include "odb.h"
#include "odb_ioprof.h"
#include "cmaio.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/* Table data */

typedef struct io_tbl_t {
  struct io_pool_t *mypool;
  char *tblname;
  int known_size; /* from TOC or reads/write */
  int updated; /* i.e. any writes issued */
  ll_t offset;
  int nrows;
  int ncols;
  /* Incore table file */
  int open_mode;
  char *file;
  char *incore;
  int n_incore;
  int n_alloc;
  int incore_ptr;
  int in_use;
  /* QTAR-purposes only */
  char *read_cmd;
  char *delete_cmd;
} IO_tbl_t;

/* Pool data */

typedef struct io_pool_t {
  struct io_db_t *mydb;
  int poolno;
  int maxtables;
  int ntables;
  IO_tbl_t *tbl;
  /* Pool file (QTAR purposes) */
  char *file;
  int file_exist;
  char *last_write_cmd;
} IO_pool_t;

/* Database */

typedef struct io_db_t {
  char *dbname;
  int in_use;
  int handle;
  int is_new;
  int is_readonly;
  int req_byteswap;
  int io_method;
  int npools;
  IO_pool_t *pool;
  int maxpools;
  int *poolaccess;
} IO_db_t;

#endif
