
/* odbi_struct.h */

typedef struct _ODBI_col_t {
  char *name;
  char *nickname;
  char *type;
  unsigned int dtnum; /* see "privpub.h" */
  /* the following 2: see "info.h" */
  int bitpos;
  int bitlen;
} ODBI_col_t;

typedef struct _ODBI_query_t {
  char *name;
  char *query_string;
  FILE *fpcache;
  int vhandle;
  int poolno;
  int ntot;
  int nrows;
  int ncols;
  int nra;
  int row_offset;
  double *a; /* data matrix in native (F90) layout : a(nra,0:ncols) */
  struct _ODBI_db_t *db;
  int ncols_fixed;
  int ncols_all; /* ordinary columns + bit members */
  struct _ODBI_col_t *cols;
  int start_row;
  int maxrows;
  int is_table;
  int string_count;
  /* For odbi_direct only */
  /* Only applicable to fast_odbsql == 1 */
  void *info; /* see info_t in "info.h" */
  int need_global_picture;
  int first_time;
  int all_processed;
  int re_eval_info;
} ODBI_query_t;

/* start client specific */
typedef struct _ODBI_db_cli_t {
  int   port;
  char *host; /* hostname or ip-address */
  char *hostname; /* normally meant to be the hostname, not ip-address */
  char *datapath;
  char *poolmask;
  char *user;
  char *password;
  char *workdir;
  char *exe;
  char *clihost;
  char *cliaddr;
  int   sockfd;
  int   metadata;
  int   verbose;
  int   silent;
  int   timeout;
  int   clean;
  int   keep;
  int   bufsize;
  int   version;
  int   odbsql;
} ODBI_db_cli_t;
/* end client specific */

typedef struct _ODBI_db_t {
  FILE *fp;
  int fp_opened_here;
  char *name;
  char *mode;
  int handle;
  int npools;
  int io_method;
  int nquery;
  int *poolstat;
  struct _ODBI_query_t *query;
  int swapbytes;
  int ntables;
  char **tables;
  /* for client/server-model only */
  ODBI_db_cli_t *cli;
} ODBI_db_t;
