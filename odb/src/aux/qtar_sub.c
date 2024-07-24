
/* qtar_sub.c */

#include "qtar.h"

#define QTAR_FLAGS "b:cCdDf:H:m:MR:stuvVx"

#define USAGE \
  "Usage: %s\n" \
  "       [-b I/O_bufsize; default=%d bytes]\n" \
  "       [-d(elete_member(s)_from_qtar-archive)]\n" \
  "       [-D (debug ON)]\n" \
  "       [-H size (file header size; default=%d bytes)]\n" \
  "       [-c(reate_new_qtar-archive)]\n" \
  "       [-C(ompress) [not in use yet]]\n" \
  "       [-M(atrix_dimensions_will_be_sought)\n" \
  "       [-R alloc_roundup_for_a_member_file; default=%d bytes]\n" \
  "       [-s (read/write member-data from/to Unix-stream/pipe)]\n" \
  "       [-t(able_of_contents_of_qtar-archive)]\n" \
  "       [-u(pdate_qtar-archive)]\n" \
  "       [-v(erbose)]\n" \
  "       [-V(ersion)]\n" \
  "       [-x (extract_member(s)_from_qtar-archive)]\n" \
  "       [-m member1] [-m member2] ... [-m memberN]\n" \
  "       [-f qtarfile]\n" \
  "       [qtarfile] [member1] [member2] ... [memberN]\n"

#define QTAR         1364476242U
#define QTAR_STR    "QTAR"
#define QTARFILE    "QTARFILE"

#define HDRSIZE_DEFAULT 65536
#define ALLOC_RNDUP     1024
#define IO_BUFSIZE      1048576
#define PERMS 0644

PRIVATE int standalone_job = 0;

PRIVATE char *qtarfile = NULL;
PRIVATE char *option = "";
PRIVATE int compress = 0;
PRIVATE int errflg = 0;
PRIVATE int debug = 0;
PRIVATE int delete = 0;
PRIVATE int create = 0;
PRIVATE int update = 0;
PRIVATE int verbose = 0;
PRIVATE int Version = 0;
PRIVATE int extract = 0;
PRIVATE int toc = 0;
PRIVATE int hdrsize = HDRSIZE_DEFAULT;
PRIVATE int is_stream = 0;
PRIVATE int io_bufsize = IO_BUFSIZE;
PRIVATE int alloc_rndup = ALLOC_RNDUP;
PRIVATE int matrix_dim = 0;

PRIVATE const char hdr_fmt[] = "%s\n%d %d %lld %d\n";
PRIVATE const int nlcnt_hdr_fmt = 2; /* newline count in header-format */

PRIVATE const char mem_fmt[] = "%s %s %lld %lld %lld %d %d %lld %lld %lld %lld %lld %lld %lld %d %d\n";
PRIVATE const int nlcnt_mem_fmt = 1; /* newline count in member-format */

PRIVATE const char eofhdr_fmt[] = "\f\n";
PRIVATE const int nlcnt_eofhdr_fmt = 1; /* newline count in EOF header-format */

#define WORDLEN sizeof(ll_t)

typedef struct member_t {
  char *file;
  char *alias; /* future */
  ll_t offset;
  ll_t bytes; /* no. of bytes BEFORE packing */
  ll_t freespace;
  int update_cnt;
  int pmethod; /* future : packing [method] if > 0; no packing = 0 */
  ll_t packed_bytes; /* future : no. of bytes AFTER packing */
  /* future */
  ll_t mode;
  ll_t uid;
  ll_t gid;
  ll_t atim;
  ll_t mtim;
  ll_t ctim;
  /* end of future */
  /* matrix dimensions */
  int nrows;
  int ncols;
  struct member_t *next;
} Member_t;

typedef struct file_t {
  char *file;
  ll_t bytes;
  int nrows;
  int ncols;
  Member_t *m;
  struct file_t *next;
} File_t;

PRIVATE int Nf = 0;
PRIVATE File_t *f = NULL;
PRIVATE File_t *p_f = NULL;

PRIVATE int Nmem = 0;
PRIVATE Member_t *memfile = NULL;
PRIVATE Member_t *p_memfile = NULL;

PRIVATE ll_t next_free_offset = 0;
PRIVATE int ngaps = 0; /* Future extension to handle gaps i.e. fragmentation in QTARFILE */

#define OPEN_MACRO(fd, file, flags, perms) \
  { fd = open(file, flags, perms); \
    if (fd<0) myexit(1,"qtar[%s]: fd=%d=open(file=%s)",option,fd,file); }

#define READ_MACRO(n, fd, c, len)  \
  { n = read(fd, c, len); \
    if (n<0) myexit(1,"qtar[%s]: n=%lld=read(fd=%d,c=%p,len=%lld)",option,n,fd,c,len); }

#define WRITE_MACRO(n, fd, c, len)  \
  { n = write(fd, c, len); \
    if (n<0) myexit(1,"qtar[%s]: n=%lld=write(fd=%d,c=%p,len=%lld)",option,n,fd,c,len); }

#define CLOSE_MACRO(fd) \
  { int n = close(fd); \
    if (n<0) myexit(1,"qtar[%s]: n=%d=close(fd=%d)",option,n,fd); }

#define LSEEK_MACRO(fd, offset, whence) \
  { int n = lseek(fd, offset, whence); \
    if (n<0) myexit(1,"qtar[%s]: n=%d=lseek(fd=%d,offset=%lld,whence=%d)",\
                       option,n,fd,offset,(int)whence); }

PRIVATE void
errmsg(const char *format, ...)
{
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  fprintf(stderr,"\n");
  va_end(args);
  errflg++;
}


PRIVATE void
myexit(int rc, const char *format, ...)
{
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  fprintf(stderr,": rc=%d\n",rc);
  va_end(args);
  if (!standalone_job) RAISE(SIGABRT); /* to get a proper traceback */
  exit(rc);
}


PRIVATE int
FileExist(const char *filename)
{
  int exist = 0;
  struct stat buf;
  if (stat(filename,&buf) == 0) {
    /* Caveat: We do not check if this file is a directory or some
       other special file. Perhaps we should */
    exist = 1;
  }
  return exist;
}


PRIVATE int
FileSize(const char *filename)
{
  int bytes = 0;
  struct stat buf;
  if (stat(filename,&buf) == 0) bytes = buf.st_size;
  return bytes;
}


PRIVATE Member_t *
PresentMember(const char *filename)
{
  Member_t *m = NULL;
  Member_t *mf = memfile;
  while (mf) { /* Currently only a linear search */
    if (strcmp(mf->file,filename) == 0) {
      /* Found */
      m = mf;
      break;
    }
    mf = mf->next;
  }
  return m;
}


PRIVATE Member_t *
AddMember(const char *filename, const char *alias, 
	  ll_t offset, ll_t bytes, ll_t freespace, 
	  int update_cnt, int nrows, int ncols,
	  int replace)
{
  Member_t *mf = PresentMember(filename);
  if (!mf) replace = 0;
  if (!mf || replace) {
    if (!mf) ALLOC(mf,1);
    mf->offset = offset;
    mf->bytes = bytes;
    mf->freespace = freespace;
    mf->update_cnt = update_cnt;
    mf->pmethod = 0;            /* not used yet */
    mf->packed_bytes = bytes; /* not used yet */
    mf->alias = alias ? STRDUP(alias) : STRDUP(filename); /* not used yet */
    mf->mode = -1;  /* not used yet */
    mf->uid = -1;   /* not used yet */
    mf->gid = -1;   /* not used yet */
    mf->atim = -1;  /* not used yet */
    mf->mtim = -1;  /* not used yet */
    mf->ctim = -1;  /* not used yet */
    mf->nrows = nrows;
    mf->ncols = ncols;
    if (!replace) {
      mf->file = STRDUP(filename);
      mf->next = NULL;
      if (!memfile) {
	p_memfile = memfile = mf;
      }
      else {
	p_memfile->next = mf;
	p_memfile = mf;
      }
      Nmem++;
    }
  }
  return mf;
}


PRIVATE File_t *
AddFile(const char *filename)
{
  File_t *xf;
  ALLOC(xf,1);
  xf->file = STRDUP(filename);
  xf->bytes = 0;
  xf->nrows = 0;
  xf->ncols = 0;
  xf->m = NULL;
  xf->next = NULL;
  if (!f) {
    p_f = f = xf;
  }
  else {
    p_f->next = xf;
    p_f = xf;
  }
  Nf++;
  return xf;
}


PRIVATE void
SetFileSizes()
{
  if (!is_stream) {
    File_t *xf = f;
    while (xf) {
      xf->bytes = FileSize(xf->file);
      xf = xf->next;
    }
  }
}


PRIVATE int
CountMember(const Member_t *m)
{
  int n = 0;
  while (m) {
    if (m->offset >= 0) n++;
    m = m->next;
  }
  return n;
}


PRIVATE int 
WriteChunk(int fd, const char *c, ll_t bytes)
{
  ll_t rem = bytes;
  ll_t rc = 0;
  for (;;) {
    ll_t n, len = MIN(io_bufsize, rem);
    if (len <= 0) break;
    WRITE_MACRO(n, fd, c, len);
    if (n > 0) {
      c += n;
      rem -= n;
      rc += n;
    }
    else /* Error (the stmt "rc != bytes" below will catch it) */
      break;
  } 
  if (rc != bytes) myexit(1,"Error in WriteChunk(): rc != bytes; rc=%d, bytes=%d",rc,bytes);
  return bytes;
}


PRIVATE int 
ReadChunk(int fd, char *c, ll_t bytes)
{
  ll_t rem = bytes;
  ll_t rc = 0;
  for (;;) {
    ll_t n, len = MIN(io_bufsize, rem);
    if (len <= 0) break;
    READ_MACRO(n, fd, c, len);
    if (n > 0) {
      c += n;
      rem -= n;
      rc += n;
    }
    else /* Error (the stmt "rc != bytes" below will catch it) */
      break;
  } 
  if (rc != bytes) myexit(1,"Error in ReadChunk(): rc != bytes; rc=%d, bytes=%d",rc,bytes);
  return bytes;
}


PRIVATE void
Size2Stream(ll_t nbytes, ll_t nrows, ll_t ncols, const char *file)
{
  if (is_stream) {
    int fd_out = fileno(stdout);
    ll_t dim[3];
    int nwrite;
    int nsize;
    if (matrix_dim) {
      dim[0] = nbytes; dim[1] = nrows; dim[2] = ncols;
      nsize = sizeof(dim);
    }
    else {
      dim[0] = nbytes; dim[1] = 0; dim[2] = 0;
      nsize = sizeof(nbytes);
    }
    nwrite = WriteChunk(fd_out, (const char *)dim, nsize);
    if (nwrite != nsize) {
      myexit(1, 
	     "Size2Stream(): Unable to write size information on member-file='%s' to stream", 
	     file);
    }
  }
}

PRIVATE void
ExtractMember(int fd, const Member_t *m)
{
  if (m) {
    int fd_out = (is_stream) ? fileno(stdout) : -1;
    char *c;
    ll_t rem = m->bytes;
    ll_t rc = 0;
    ALLOC(c,io_bufsize);
    LSEEK_MACRO(fd, hdrsize + m->offset, SEEK_SET);
    if (!is_stream) {
      OPEN_MACRO(fd_out, m->file, (O_WRONLY | O_CREAT | O_TRUNC), PERMS);
    }
    else { /* Write size info first to stream */
      Size2Stream(m->bytes, m->nrows, m->ncols, m->file);
    }
    for (;;) {
      ll_t n, len = MIN(io_bufsize, rem);
      if (len <= 0) break;
      n = ReadChunk(fd, c, len);
      if (n > 0) {
	WriteChunk(fd_out, c, n);
	rem -= n;
	rc += n;
      }
    }
    if (!is_stream) CLOSE_MACRO(fd_out);
    FREE(c);
  }
}


PRIVATE void
UpdateMember(int fd, Member_t *m)
{
  if (m) {
    int fd_in = (is_stream) ? fileno(stdin) : -1;
    char *c;
    ll_t rem = m->bytes;
    ll_t rc = 0;
    ALLOC(c,io_bufsize);
    m->update_cnt++;
    LSEEK_MACRO(fd, hdrsize + m->offset, SEEK_SET);
    if (!is_stream) OPEN_MACRO(fd_in, m->file, O_RDONLY, 0);
    for (;;) {
      ll_t n, len = MIN(io_bufsize, rem);
      if (len <= 0) break;
      n = ReadChunk(fd_in, c, len);
      if (n > 0) {
	WriteChunk(fd, c, n);
	rem -= n;
	rc += n;
      }
    }
    FREE(c);
    LSEEK_MACRO(fd, m->freespace, SEEK_CUR);
    if (!is_stream) CLOSE_MACRO(fd_in);
  }
}

	
PRIVATE int
WriteHDR(int fd)
{
  int nmem = 0;
  {
    Member_t *m = memfile;
    char *hdr;
    char *phdr;
    int inc;
    ALLOC(hdr,hdrsize);
    phdr = hdr;
    nmem = CountMember(m);
    memset(hdr,'\n',hdrsize);
    inc = sprintf(phdr, hdr_fmt, QTAR_STR, hdrsize, nmem, next_free_offset, ngaps);
    while(m) {
      if (m->offset >= 0) {
	phdr += inc;
	inc = sprintf(phdr, mem_fmt,
		      m->file, m->alias, 
		      m->offset, m->bytes, m->freespace, m->update_cnt, m->pmethod, m->packed_bytes,
		      m->mode, m->uid, m->gid, m->atim, m->mtim, m->ctim, m->nrows, m->ncols);
      }
      m = m->next;
    }
    phdr += inc;
    inc = sprintf(phdr, eofhdr_fmt);
    LSEEK_MACRO(fd, 0, SEEK_SET); /* Rewound */
    WriteChunk(fd, hdr, hdrsize);
    FREE(hdr);
  }
  return nmem;
}


PRIVATE int
ReadHDR(int fd)
{
  char s[8192], alias[8192];
  int j, nmem, nelem;
  FILE *fp = fdopen(fd, "r");
  rewind(fp);
  nelem = fscanf(fp, hdr_fmt, s, &hdrsize, &nmem, &next_free_offset, &ngaps);
  if (strcmp(s,QTAR_STR) != 0) myexit(1,"File %s is not a QTAR-file",qtarfile);
  if (nelem != 5) myexit(2,"Corrupted QTAR-file %s",qtarfile);
  for (j=0; j<nmem; j++) {
    ll_t offset, bytes, freespace;
    int update_cnt, pmethod;
    ll_t packed_bytes;
    ll_t mode, uid, gid, atim, mtim, ctim;
    int nrows, ncols;
    nelem = fscanf(fp, mem_fmt,
		   s, alias, 
		   &offset, &bytes, &freespace, &update_cnt, &pmethod, &packed_bytes,
		   &mode, &uid, &gid, &atim, &mtim, &ctim, &nrows, &ncols);
    if (nelem != 16) {
      myexit(3,"Invalid header in QTAR-file %s: nelem=%d at j=%d (nmem=%d)",qtarfile,nelem,j,nmem);
    }
    (void) AddMember(s, alias, offset, bytes, freespace, update_cnt, nrows, ncols, 1);
    if (offset + bytes + freespace > next_free_offset) {
      next_free_offset = offset + bytes + freespace;
      next_free_offset = RNDUP(next_free_offset, WORDLEN);
    }
  } /* for (j=0; j<nmem; j++) */
  return hdrsize;
}


PUBLIC int
CloseQTAR(int fd)
{
  CLOSE_MACRO(fd);
  return 0;
}


PUBLIC int
OpenQTAR()
{
  int fd;
  int other = (update || delete) ? O_RDWR : O_RDONLY;
  int flags = create ? (O_WRONLY | O_CREAT | O_TRUNC) : other;
  int perms = create ? PERMS : 0;

  OPEN_MACRO(fd, qtarfile, flags, perms);

  if (create) {
    WriteHDR(fd);
    CloseQTAR(fd);
    OPEN_MACRO(fd, qtarfile, other, perms);
  }

  return fd;
}


PUBLIC int 
UpdateQTAR(int fd)
{
  File_t *xf = f;
  int nmem = 0;
  if (xf) {
    char *key = NULL;
    Member_t *m;
    int hdrsize_new = ReadHDR(fd);
    xf = f;
    while (xf) {
      if (is_stream) {
	/* Obtain size information from stream */
	int fd_in = fileno(stdin);
	ll_t dim[3];
	ll_t nbytes;
	int nread;
	if (matrix_dim) {
	  nread = ReadChunk(fd_in, (char *)dim, sizeof(dim));
	}
	else {
	  nread = ReadChunk(fd_in, (char *)&nbytes, sizeof(nbytes));
	}
	if (( matrix_dim && nread == sizeof(dim)) ||
	    (!matrix_dim && nread == sizeof(nbytes))) {
	  if (!matrix_dim) {
	    dim[0] = nbytes; dim[1] = 0; dim[2] = 0;
	  }
	  xf->bytes = dim[0];
	  xf->nrows = dim[1];
	  xf->ncols = dim[2];
	}
	else {
	  myexit(1, 
		 "UpdateQTAR: Unable to obtain size information on member-file='%s' from stream", 
		 xf->file);
	}
      } /* if (is_stream) */
      if (xf->bytes >= 0) {
	m = PresentMember(xf->file);
	if (m) {
	  ll_t total = m->bytes + m->freespace;
	  if (xf->bytes > total) { 
	    /* Doesn't fit into the existing slot */
	    m->bytes = xf->bytes;
	    m->offset = next_free_offset;
	    m->freespace = RNDUP(m->bytes, alloc_rndup) - m->bytes;
	    next_free_offset += m->bytes + m->freespace;
	    next_free_offset = RNDUP(next_free_offset, WORDLEN);
	    m->freespace = (next_free_offset - m->offset) - m->bytes;
	    if (verbose) key = "u+";
	    if (ngaps <= 0) ngaps--; /* fragmentation count ... for now */
	  }
	  else {
	    /* Fits into the existing slot */
	    m->freespace = total - xf->bytes;
	    m->bytes = xf->bytes;
	    if (verbose) key = "u";
	  }
	  m->nrows = xf->nrows;
	  m->ncols = xf->ncols;
	}
	else { /* not present ==> create it */
	  ll_t freespace = RNDUP(xf->bytes, alloc_rndup) - xf->bytes;
	  m = AddMember(xf->file, NULL, next_free_offset, xf->bytes, freespace, 
			xf->nrows, xf->ncols, -1, 0);
	  next_free_offset += xf->bytes + freespace;
	  next_free_offset = RNDUP(next_free_offset, WORDLEN);
	  m->freespace = (next_free_offset - m->offset) - m->bytes;
	  if (verbose) key = "a";
	}
	UpdateMember(fd, m);
	if (key) fprintf(stderr,"%s %s %lld %lld %lld\n",key,m->file,m->bytes,m->offset,m->freespace);
	nmem++;
      }
      xf = xf->next;
    }
    if (nmem > 0) WriteHDR(fd);
  }
  return nmem;
}


PUBLIC int 
ExtractQTAR(int fd)
{
  File_t *xf = f;
  int nmem = 0;
  int hdrsize_new = ReadHDR(fd);
  if (xf) {
    xf = f;
    while (xf) {
      Member_t *m = PresentMember(xf->file);
      if (m) {
	ExtractMember(fd, m);
	nmem++;
      }      
      else {
	Size2Stream(0, 0, 0, xf->file);
      }
      xf = xf->next;
    }
  }
  else {
    Member_t *m = memfile;
    while (m) {
      if (m) {
	ExtractMember(fd, m);
	nmem++;
      }
      m = m->next;
    }
  }
  return nmem;
}


PUBLIC int 
DeleteQTAR(int fd)
{
  int ndel = 0;
  File_t *xf = f;
  if (xf) {
    int hdrsize_new = ReadHDR(fd);
    while (xf) {
      Member_t *m = PresentMember(xf->file);
      if (m) {
	m->freespace += m->bytes;
	m->bytes = 0;
	m->nrows = 0;
	ndel++;
      }
      xf = xf->next;
    }
    if (ndel > 0) WriteHDR(fd);
  }
  return ndel;
}


PUBLIC int 
TocQTAR(int fd)
{
  File_t *xf = f;
  Member_t *m;
  int nmem;
  hdrsize = ReadHDR(fd);
  nmem = (xf) ? 0 : Nmem;
  if (xf) {
    while (xf) {
      m = PresentMember(xf->file);
      if (m) nmem++;
      xf = xf->next;
    }
    if (!is_stream) fprintf(stdout,"%d %d %lld %d\n",hdrsize,nmem,next_free_offset,ngaps);
    else            fprintf(stdout,"%d\n",nmem);
    xf = f;
    while (xf) {
      m = PresentMember(xf->file);
      if (m) {
	if (!is_stream) fprintf(stdout,"%s %lld %lld %lld %d %d\n",m->file,m->offset,m->bytes,m->freespace,m->nrows,m->ncols);
	else            fprintf(stdout,"%s %lld %lld %d %d\n",m->file,m->offset,m->bytes,m->nrows,m->ncols);
      }
      xf = xf->next;
    }
  }
  else {
    m = memfile;
    if (!is_stream) fprintf(stdout,"%d %d %lld %d\n",hdrsize,nmem,next_free_offset,ngaps);
    else            fprintf(stdout,"%d\n",nmem);
    while (m) {
      if (!is_stream) fprintf(stdout,"%s %lld %lld %lld %d %d\n",m->file,m->offset,m->bytes,m->freespace,m->nrows,m->ncols);
      else            fprintf(stdout,"%s %lld %lld %d %d\n",m->file,m->offset,m->bytes,m->nrows,m->ncols);
      m = m->next;
    }
  }
  return nmem;
}

PUBLIC int
QTAR_main(int argc, char *argv[])
{
  int c;
  int fd = -1;
  int saved_argc = argc;
  char **saved_argv = argv;
  int exist;

  standalone_job = 1;

  while ((c = getopt(argc, argv, QTAR_FLAGS)) != -1) {
    switch (c) {
    case 'f': /* qtar-file */
      if (!qtarfile) qtarfile = STRDUP(optarg);
      else errmsg("Multiple QTAR-files supplied");
      break;
    case 'm': /* a member-file */
      AddFile(optarg);
      break;
    case 'D': /* Debug ON */
      debug = 1;
      break;
    case 'd': /* Delete */
      if (!toc && !update && !extract) delete = 1;
      else errmsg("-d -t -u -x flags are mutually exclusive");
      break;
    case 'x': /* Extract */
      if (!toc && !update && !delete) extract = 1;
      else errmsg("-d -t -u -x flags are mutually exclusive");
      break;
    case 't': /* Table of contents */
      if (!update && !extract && !delete) toc = 1;
      else errmsg("-d -t -u -x flags are mutually exclusive");
      break;
    case 'C': /* Compress */
      compress = 1;
      break;
    case 'c': /* Create new */
      create = 1;
      /* no break; i.e. fall through */
    case 'u': /* Update */
      if (!toc && !extract && !delete) update = 1;
      else errmsg("-d -t -u -x flags are mutually exclusive");
      break;
    case 'v': /* Verbose */
      verbose = 1;
      break;
    case 'V': /* Version */
      Version = 1;
      break;
    case 's': /* read/write from/to stream i.e. pipe */
      is_stream = 1;
      break;
    case 'M': /* Respect matrix dimensions */
      matrix_dim = 1;
      break;
    case 'b': /* I/O-bufsize */
      io_bufsize = atoi(optarg);
      if (io_bufsize < HDRSIZE_DEFAULT) io_bufsize = HDRSIZE_DEFAULT;
      io_bufsize = RNDUP(io_bufsize, WORDLEN);
      break;
    case 'R': /* Alloc roundup for each member file */
      alloc_rndup = atoi(optarg);
      if (alloc_rndup < WORDLEN) alloc_rndup = WORDLEN;
      alloc_rndup = RNDUP(alloc_rndup, WORDLEN);
      break;
    case 'H': /* Change HDR-size (minimum is HDRSIZE_DEFAULT, though) */
      hdrsize = atoi(optarg);
      if (hdrsize < HDRSIZE_DEFAULT) hdrsize = HDRSIZE_DEFAULT;
      hdrsize = RNDUP(hdrsize, WORDLEN);
      break;
    default:
      errflg++;
      break;
    } /* switch (c) */
  } /* while */

  if (argc - 1 >= optind) {
    if (!qtarfile) qtarfile = STRDUP(argv[optind]);
    else AddFile(argv[optind]);
    optind++;
  }

  if (argc > optind) {
    /* More than one file supplied */
    for ( ; optind < argc; optind++) {
      AddFile(argv[optind]);
    }
  }

  /* Default is QTARFILE or its environment value, if not supplied */
  if (!qtarfile) {
    char *env = getenv(QTARFILE);
    qtarfile = env ? STRDUP(env) : STRDUP(QTARFILE);
  }

  exist = FileExist(qtarfile);
  if (!exist) {
    if (update || delete) create = 1;
    if (!delete && !toc) {
      if (!create) errmsg("QTARFILE %s does not exist",qtarfile);
    }
  }

  if (!create && !update && !toc && !extract && !delete) {
    errmsg("None of the options -c, -d, -t, -u or -x were supplied");
  }

  /* to remove unnecessary complaints/warning msgs from compilation */
  if (Version); /* Not implemented yet */
  if (debug); /* Do nothing */
  if (nlcnt_hdr_fmt); /* do nothing */
  if (nlcnt_mem_fmt); /* do nothing */
  if (nlcnt_eofhdr_fmt); /* do nothing */

  if (errflg) {
    fprintf(stderr,USAGE,argv[0],IO_BUFSIZE,HDRSIZE_DEFAULT,ALLOC_RNDUP);
    {
      int j;
      fprintf(stderr,"*** Supplied arguments (count=%d) ***\n",saved_argc);
      for (j=0; j<saved_argc; j++) {
	fprintf(stderr,"%s ",saved_argv[j]);
      }
      fprintf(stderr,"\n");
    }
    return errflg;
  }

  if (toc && !exist) {
    if (is_stream) fprintf(stdout,"0\n");
    return 0; /* Do nothing */
  }

  if (create && exist) (void) remove(qtarfile);

  if (compress); /* Not implemented yet */

  /* Record the master option */
  if (update)  option="-u";
  if (update && create)  option="-c -u";
  if (extract) option="-x";
  if (delete)  option="-d";
  if (delete && create)  option="-c -d";
  if (toc)     option="-t";

  if (update) SetFileSizes();

  fd = OpenQTAR();
  if (update)  UpdateQTAR(fd);
  if (extract) ExtractQTAR(fd);
  if (delete)  DeleteQTAR(fd);
  if (toc)     TocQTAR(fd);
  CloseQTAR(fd);

  return errflg;
}
