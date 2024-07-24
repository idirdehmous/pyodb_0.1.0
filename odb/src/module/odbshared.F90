MODULE odbshared

USE PARKIND1  ,ONLY : JPIM     ,JPRB

USE odbmp ! The message passing module
USE str, only : toupper, tolower, sadjustl, sadjustr
USE odbsort, only : keysort

IMPLICIT NONE
SAVE
PUBLIC

INTERFACE ODB_map_vpools
MODULE PROCEDURE & 
  & ODB_map_vpools_direct, &
  & ODB_map_vpools_fromfile
END INTERFACE

#ifndef USE_CTRIM
#define ctrim(x) x
#define CTRIM(x) x
#endif

#define trimadjL(x) trim(sadjustl(x))
#define trimadjR(x) trim(sadjustr(x))

TYPE prt_t
SEQUENCE
INTEGER(KIND=JPIM) :: id
INTEGER(KIND=JPIM) :: len
character(len=40) :: fmt
character(len=40) :: cfmt
END TYPE prt_t

INTEGER(KIND=JPIM), parameter :: ODB_MAXBITS = 32
INTEGER(KIND=JPIM), parameter :: ODB_MAXROWS = 2147483647
INTEGER(KIND=JPIM), parameter :: ODB_SIZEOF_INT   = 4
INTEGER(KIND=JPIM), parameter :: ODB_SIZEOF_REAL4 = 4
INTEGER(KIND=JPIM), parameter :: ODB_SIZEOF_REAL8 = 8

TYPE(prt_t)&
     &pint, puint, preal, pstring, &
     &pbitf, &! (ODB_MAXBITS), 
     &punknown, phex,&
     &pyyyymmdd, phhmmss, &
     &pyyyymmdd_pdt, phhmmss_pdt, &
     &pdbidx, &
     &plinkoffset_t, plinklen_t, pvarprec

INTEGER(KIND=JPIM), parameter :: flag_update = 1
INTEGER(KIND=JPIM), parameter :: flag_pack   = 2
INTEGER(KIND=JPIM), parameter :: flag_get    = flag_update
INTEGER(KIND=JPIM), parameter :: flag_put    = flag_update
INTEGER(KIND=JPIM), parameter :: def_flag    = flag_update ! i.e. GET all, PUT all cols
INTEGER(KIND=JPIM), parameter :: flag_free   = 4

!-- Maximum number of characters in a Fortran90 string (i.e. character(len=maxstrlen))
INTEGER(KIND=JPIM), parameter :: maxstrlen = 32767 ! 65536

INTEGER(KIND=JPIM), parameter :: maxvarlen = 512
INTEGER(KIND=JPIM), parameter :: maxfilen  = 512
INTEGER(KIND=JPIM), parameter :: dbnamelen = 80
REAL(KIND=JPRB),    parameter :: rmaxint = 2147483647.0_JPRB

!-- Note: UNDEFDB brought over from /cc/rd/ifs/module/yomdb.F90 on 20-Mar-2002 by SS
#ifdef RS6K
!-- in the following if 0x7FF7FFFF is used as 4-byte initialization string, would enable better
!   error detection for automatic arrays
!   Use also -qinitauto=7FF7FFFF (Fortran)
INTEGER(KIND=JPIM), parameter :: UNDEFDB =  2146959359 ! "ODB_UNDEF" ; the same as 0x7FF7FFFF
#else
!-- in the following if 0x8B8B8B8B used as an index value on FUJITSU, it triggers SIGBUS error
INTEGER(KIND=JPIM), parameter :: UNDEFDB = -1953789045 ! "ODB_UNDEF" ; the same as 0x8B8B8B8B
#endif
INTEGER(KIND=JPIM) :: UNDEFDB_actual = UNDEFDB

INTEGER(KIND=JPIM), parameter :: ODB_NMDI =  2147483647
REAL(KIND=JPRB),    parameter :: ODB_RMDI = -2147483647.0_JPRB

INTEGER(KIND=JPIM), parameter :: ODB_HDR_ALIGNED = 1
INTEGER(KIND=JPIM), parameter :: ODB_BODY_ALIGNED = 2

!--   Newline after certain no. of columns (in prtdata)
INTEGER(KIND=JPIM), parameter :: def_wrapcol = 2147483647
INTEGER(KIND=JPIM)            ::     wrapcol = def_wrapcol

!--   Show database index
INTEGER(KIND=JPIM), parameter :: def_showdbidx = 0
INTEGER(KIND=JPIM)            ::     showdbidx = 0

!--   I/O locking
INTEGER(KIND=JPIM), parameter :: def_haveiolock = 0
INTEGER(KIND=JPIM)            ::     haveiolock = 0

!--   Max no. of simultaneously open DBs
INTEGER(KIND=JPIM), parameter :: def_maxhandle = 10
INTEGER(KIND=JPIM)            :: maxhandle = 0

!--   Determine the leading dimension calculation method
#if defined(VPP) || defined(NECSX)
!-- Vector machines
INTEGER(KIND=JPIM), parameter :: def_lda_method = 0 ! i.e. ld = n + mod(n+1,2)
#else
!-- Scalar machines (most/all of them aren't keen to have power-of-two leading dimension)
INTEGER(KIND=JPIM), parameter :: def_lda_method = 2 ! i.e. ld = n, but not power of 2 (which case n+1)
#endif
!INTEGER(KIND=JPIM)            :: lda_method = def_lda_method 
INTEGER(KIND=JPIM)            :: lda_method = 1 ! By default do *not* add +1 (SS/26-Feb-2014)

!-- Used in odb_*array_dump()-routines
INTEGER(KIND=JPIM)            :: max_dump_count = 100 ! Redefine via env. ODB_ARRAY_MAX_DUMP_COUNT
INTEGER(KIND=JPIM)            ::     dump_count = 0

!-- Default I/O-method
INTEGER(KIND=JPIM)            :: def_io_method = 1

!-- Default OpenMP I/O choice (0=off, 1=on)
INTEGER(KIND=JPIM)            :: def_io_openmp = 0

TYPE db_t
logical :: inuse
logical :: newdb
logical :: altered
logical :: readonly
logical :: rewrite_flags
INTEGER(KIND=JPIM) :: io_method
INTEGER(KIND=JPIM) :: glbNpools
! glbMaxpoolno: if NOT readonly, then == glbNpools
!               otherwise up to glbNpools before (the first) ODB_addpools()
INTEGER(KIND=JPIM) :: glbMaxpoolno
INTEGER(KIND=JPIM) :: locNpools
INTEGER(KIND=JPIM) :: iounit
INTEGER(KIND=JPIM) :: ntables
INTEGER(KIND=JPIM) :: naid  ! 0 or 5 for io_method=4
INTEGER(KIND=JPIM) :: nfileblocks ! ... of nested iomap-files
INTEGER(KIND=JPIM) :: CreationDT(0:1)
INTEGER(KIND=JPIM) :: AnalysisDT(0:1)
REAL(KIND=JPRB) :: Version_Major, Version_Minor
character(len=dbnamelen) :: name
INTEGER(KIND=JPIM), pointer :: poolidx(:)
INTEGER(KIND=JPIM), pointer :: ioaid(:,:,:) ! glbNpools x ntables x naid
INTEGER(KIND=JPIM), pointer :: grpsize(:)   ! IO_GRPSIZE per each ciomap-file ; maxsize <= glbNpools
character(len=maxvarlen), pointer :: ctables(:) ! cached tablenames; size=ntables
character(len=maxfilen) , pointer :: ciomap(:) ! Aux. FBLOCK I/O-map files; maxsize <= glbNpools
REAL(KIND=JPRB), pointer :: io_volume(:)  ! used with IO_OPENMP=1 upon write; size=ntables
INTEGER(KIND=JPIM) :: nvmap ! 0 = no mapping, >0 : mapping active, <0 : mapping [temporarely] passive
INTEGER(KIND=JPIM), pointer :: vpoolmap(:) ! Virtual pool map vpoolmap(poolno) --> virtual poolno
INTEGER(KIND=JPIM), pointer :: vpoolmap_idx(:) ! Indexing to make ODB_vpool2pool() fast
END TYPE db_t

TYPE(db_t), allocatable :: db(:)
logical :: db_initialized = .FALSE.
logical :: db_trace = .FALSE.

CONTAINS


#ifdef USE_CTRIM
FUNCTION ctrim(c) result(s)
character(len=*), intent(in) :: c
character(len=len_trim(c)+1) s
s = c(1:len(s)-1)//char(0)
END FUNCTION ctrim
#endif


FUNCTION ODB_lda(n, method) RESULT(lda)
!-- Leading dimension
INTEGER(KIND=JPIM), intent(in) :: n
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: method
INTEGER(KIND=JPIM) :: lda, m, use_method, iarg
logical :: isPowerOfTwo
isPowerOfTwo(iarg) = (IAND(iarg,-iarg) == iarg)

m = max(1,n) ! make sure at least one row gets allo-gated

if (present(method)) then
  use_method = method
else
  use_method = lda_method
endif

if (use_method == 0) then
  lda = m + mod(m+1,2)
else if (use_method == 1) then
  lda = m
else if (use_method == 2) then
!-- the same as #1, but also checks that m is not a power of 2 
!   i.e. 2**int_x, int_x = [1..31] ; otherwise bad on some RISC-machines
!  if (ANY(m == (/2,4,8,16,32,64,128,256,512,&
!          &1024,2048,4096,8192,16384,32768,65536,131072,262144,&
!          &524288,1048576,2097152,4194304,8388608,16777216,33554432,67108864,&
!          &134217728,268435456,536870912,1073741824/))) m = m + 1
!-- Hey, the following is much better (09/03/2005 by SS) !!
  if (isPowerOfTwo(m)) m = m + 1
  lda = m
else
  lda = m
endif
END FUNCTION ODB_lda


SUBROUTINE ODB_abort(routine, s, code, really_abort)
implicit none
character(len=*), intent(in) :: routine
character(len=*), intent(in), optional :: s
INTEGER(KIND=JPIM),        intent(in), optional :: code
logical,          intent(in), optional :: really_abort
CALL ODBMP_abort(routine, s, code, really_abort)
END SUBROUTINE ODB_abort


SUBROUTINE dummy_load(func, cond)
implicit none
external func
logical, optional :: cond
if (present(cond)) then
  if (cond) call func()
endif
END SUBROUTINE dummy_load


FUNCTION free_iounit() RESULT(kunit)
implicit none
INTEGER(KIND=JPIM) :: j, kunit
logical LLopened
kunit = -1
do j=99, 0, -1
  INQUIRE(unit=j, opened=LLopened)
  if (.not.LLopened) then
    kunit = j
    return
  endif
enddo
END FUNCTION free_iounit


FUNCTION odbHcheck(handle, routine, abort) RESULT(okay)
implicit none
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)  :: routine
logical, intent(in), optional :: abort
logical okay
okay = (handle > 0 .and. handle <= maxhandle)
if (.not.okay) then
  CALL ODB_abort(routine,&
       & 'Invalid handle. Hint: Increase max. no. of open handles via export ODB_MAXHANDLE=value',&
       & handle,abort)
endif
END FUNCTION odbHcheck


FUNCTION ODB_get_vpools(handle, poolnos) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle
INTEGER(KIND=JPIM), intent(out), optional :: poolnos(:)
INTEGER(KIND=JPIM) :: rc, n, npools
rc = 0
if (odbHcheck(handle,'ODB_get_vpools')) then
  npools = db(handle)%glbNpools ! Can't be larger than this
  n = abs(db(handle)%nvmap)
  rc = -n
  if (n > 0 .and. associated(db(handle)%vpoolmap)) then
     if (present(poolnos)) then
        if (size(poolnos) >= npools) then
           poolnos(1:npools) = db(handle)%vpoolmap(1:npools)
           rc = npools
        endif
     endif
  endif
endif
END FUNCTION ODB_get_vpools


FUNCTION ODB_unmap_vpools(handle) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle
INTEGER(KIND=JPIM) :: rc
rc = 0
if (odbHcheck(handle,'ODB_unmap_vpools')) then
  if (associated(db(handle)%vpoolmap)) then
    deallocate(db(handle)%vpoolmap)
    nullify(db(handle)%vpoolmap)
  endif
  if (associated(db(handle)%vpoolmap_idx)) then
    deallocate(db(handle)%vpoolmap_idx)
    nullify(db(handle)%vpoolmap_idx)
  endif
  db(handle)%nvmap = 0
endif
END FUNCTION ODB_unmap_vpools


FUNCTION ODB_map_vpools_direct(handle, poolnos, verbose) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle
INTEGER(KIND=JPIM), intent(in) :: poolnos(:)
logical, intent(in), optional :: verbose
logical :: LLverbose
INTEGER(KIND=JPIM) :: j, rc, npools, iret
LLverbose = .FALSE.
if (present(verbose)) LLverbose = verbose
rc = 0
if (odbHcheck(handle,'ODB_map_vpools_direct')) then
  rc = ODB_unmap_vpools(handle)
  rc = size(poolnos)
  npools = db(handle)%glbNpools
  if (rc >= npools) then
    allocate(db(handle)%vpoolmap(npools))
    db(handle)%vpoolmap(1:npools) = poolnos(1:npools)
    if (LLverbose) then
      write(0,'(1x,a,i10)') 'ODB_map_vpools: Poolno ==> Virtual pool mapping : npools = ',npools
      do j=1,npools
        write(0,'(1x,i12," ==> ",i12)') j,db(handle)%vpoolmap(j)
      enddo
    endif
    allocate(db(handle)%vpoolmap_idx(npools))
    CALL keysort(iret, db(handle)%vpoolmap, npools, &
         & descending=.TRUE., index=db(handle)%vpoolmap_idx, init=.TRUE.)
    db(handle)%nvmap = npools
    rc = npools
  else
    rc = -npools
  endif
endif
END FUNCTION ODB_map_vpools_direct


FUNCTION ODB_map_vpools_fromfile(handle, file, verbose) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle
character(len=*), intent(in)   :: file
logical, intent(in), optional  :: verbose
logical :: LLverbose
INTEGER(KIND=JPIM) :: rc, npools, j, poolno, vpoolno, k
INTEGER(KIND=JPIM), allocatable :: poolnos(:)
INTEGER(KIND=JPIM), parameter :: iu = 45
rc = 0
LLverbose = .FALSE.
if (present(verbose)) LLverbose = verbose
if (odbHcheck(handle,'ODB_map_fromfile')) then
  rc = ODB_unmap_vpools(handle)
  rc = 0
  open(unit=iu,file=file,status='old',err=99)
  npools = db(handle)%glbNpools ! Can't be larger than this
  allocate(poolnos(npools))
  poolnos(:) = -abs(ODB_NMDI)
  k = 0
  do j=1,npools
    read(iu,*,end=98,err=98) poolno, vpoolno
    if (poolno >= 1 .and. poolno <= npools) then
      poolnos(poolno) = vpoolno
      k = k + 1
    endif
  enddo
98 continue
  close(iu)
  if (k > 0) then
    rc = ODB_map_vpools_direct(handle, poolnos, verbose=verbose)
  endif
  if (allocated(poolnos)) deallocate(poolnos)
99 continue
endif
END FUNCTION ODB_map_vpools_fromfile


FUNCTION ODB_toggle_vpools(handle, toggle) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle
INTEGER(KIND=JPIM), intent(in), optional :: toggle
INTEGER(KIND=JPIM) :: rc, itoggle
rc = 0
if (odbHcheck(handle,'ODB_toggle_vpools')) then
  rc = db(handle)%nvmap ! save the old value
  if (present(toggle)) then
    itoggle = toggle
  else ! reverse the previous on/off status to off/on (unless nvmap == 0)
    if (rc > 0) then
      itoggle = -1 ! switch from ON to OFF 
    else
      itoggle = +1 ! switch from OFF to ON
    endif
  endif
  if (itoggle > 0) then ! turn ON
    db(handle)%nvmap = +abs(db(handle)%nvmap)
  else                  ! turn OFF
    db(handle)%nvmap = -abs(db(handle)%nvmap)
  endif  
endif
END FUNCTION ODB_toggle_vpools


FUNCTION ODB_pool2vpool(handle, poolno) RESULT(rc)
! Given true pool number [1..npools], returns virtual i.e. user/mapped pool number
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle
INTEGER(KIND=JPIM), intent(in) :: poolno ! true pool number
INTEGER(KIND=JPIM) :: rc, n, npools
rc = poolno ! The ultimate default : i.e. no mapping
if (odbHcheck(handle,'ODB_pool2vpool')) then
  n = db(handle)%nvmap
  if (n > 0 .and. associated(db(handle)%vpoolmap)) then ! I.e. mapping exists
    if (poolno >= 1 .and. poolno <= npools) then
       rc = db(handle)%vpoolmap(poolno) ! The user/mapped pool numbe
    else
       ! By default: Mapping exists, but poolno not mapped (usually due to user error)
       rc = -abs(ODB_NMDI)
    endif
  endif
endif
END FUNCTION ODB_pool2vpool


FUNCTION ODB_vpool2pool(handle, vpoolno) RESULT(rc)
! Given virtual i.e. user/mapped pool number, returns true pool number [1..npools]
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle
INTEGER(KIND=JPIM), intent(in) :: vpoolno ! virtual i.e. user/mapped pool number
INTEGER(KIND=JPIM) :: rc, n, npools, j, k
rc = vpoolno ! By default : no mapping i.e. assume vpoolno == poolno
if (odbHcheck(handle,'ODB_vpool2pool')) then
  n = db(handle)%nvmap
  if (n > 0 .and. associated(db(handle)%vpoolmap) &
          & .and. associated(db(handle)%vpoolmap_idx)) then ! I.e. mapping exists
    do k=1,npools
       j = db(handle)%vpoolmap_idx(k) ! Descending order i.e. largest first
       if (db(handle)%vpoolmap(j) == vpoolno) then
          rc = j
          exit
       else if (db(handle)%vpoolmap(j) < vpoolno) then
          rc = -abs(ODB_NMDI)
          exit ! Cannot be found
       endif
    enddo
  endif
endif
END FUNCTION ODB_vpool2pool


FUNCTION get_poolno(handle, poolno) RESULT(ipoolno)
implicit none
INTEGER(KIND=JPIM), intent(in)           :: handle
INTEGER(KIND=JPIM), intent(in), optional :: poolno
INTEGER(KIND=JPIM) :: ipoolno, j, locNpools
logical valid

ipoolno = -1 ! By default: Any pool that "ODBMP_myproc" is responsible for

if (present(poolno)) then
  ipoolno = ODB_vpool2pool(handle,poolno)
  if (ipoolno > 0) then
    if (ODBMP_physproc(ipoolno) /= ODBMP_myproc) then
      ipoolno = 0  ! It is not my pool
    endif
  endif
  if (ipoolno > db(handle)%glbMaxpoolno) ipoolno = 0
endif

if (handle > 0 .and. handle <= maxhandle) then
  locNpools = db(handle)%locNpools
  if (ipoolno > 0) then ! Check validity
    valid = .FALSE.
    LOOP: do j=1,locNpools
      if (ipoolno == db(handle)%poolidx(j)) then
        valid = .TRUE.
        exit LOOP
      endif
    enddo LOOP
    if (.not. valid) ipoolno = -2
  else if (ipoolno == -1) then ! If "-1", check if only one pool for this PE
    if (locNpools == 1) ipoolno = db(handle)%poolidx(1) ! Set exactly to this poolno for performance
  endif
endif

END FUNCTION get_poolno



FUNCTION ODB_valid_poolno(poolno) RESULT(valid)
implicit none
INTEGER(KIND=JPIM), intent(in) :: poolno
logical valid
valid = (poolno == -1 .or. (poolno >= 1 .and. poolno <= 2147483647))
END FUNCTION ODB_valid_poolno



SUBROUTINE no_abort()
implicit none
return
END SUBROUTINE no_abort



FUNCTION get_handle() RESULT(rc)
implicit none
INTEGER(KIND=JPIM) :: j, rc
if (.not. db_initialized) CALL init_db()
rc = -1
do j=1,maxhandle
  if (.not.db(j)%inuse) then
    rc = j
    return
  endif
enddo
END FUNCTION get_handle



SUBROUTINE init_db()
implicit none
INTEGER(KIND=JPIM) :: j, trace_on, rc, idummy(1), ilen, ibytes
INTEGER(KIND=JPIM), allocatable :: ioassign(:)
external odb_debug_print
external cma_open, cma_close, cma_readb, cma_writeb
external codb_twindow, codb_tdiff
external codb_trace, codb_filesize, codb_datetime
external codb_abort_func
external cmpl_abort
external codb_putenv, codb_pause, codb_system, codb_getpid
external codb_getenv, codb_set_entrypoint, codb_wait, codb_subshell
!      external abort

if (db_initialized) return

CALL cODB_register_abort_func('cmpl_abort')
!      CALL cODB_register_abort_func('abort')

CALL ODBMP_init()

if (ODBMP_myproc < 1 .or. ODBMP_nproc < 1) then
  CALL ODB_abort('init_db','Invalid myproc and/or nproc')
  return
endif


CALL codb_putenv('IOASSIGN_INCORE=0')
if (ODBMP_nproc > 1) then
!-- Make sure that only one MPI-task reads the IOASSIGN
!   and then replicates the contents to other tasks
  CALL ODBMP_trace(.FALSE.)
  if (ODBMP_myproc == 1) then
    CALL get_incore_ioassign(idummy,0,rc) ! rc = # of bytes in IOASSIGN-file
  else
    rc = 0
  endif
  CALL ODBMP_global('MAX',rc)
  ibytes = rc
  ilen = (ibytes + ODB_SIZEOF_INT - 1) / ODB_SIZEOF_INT
  if (ilen > 0) then
    allocate(ioassign(ilen))
    ioassign(:) = 0
    if (ODBMP_myproc == 1) then
      CALL get_incore_ioassign(ioassign,ibytes,rc)
    endif
    CALL ODBMP_distribute(ioassign,ilen,0,rc)
    CALL put_incore_ioassign(ioassign,ibytes,rc)
    deallocate(ioassign)
    CALL codb_putenv('IOASSIGN_INCORE=1')
  endif
endif

CALL cODB_init_omp_locks()

trace_on = 0
CALL cODB_trace_init(trace_on)
db_trace = (trace_on > 0)

CALL ODBMP_trace(db_trace)

CALL cODB_set_signals()

CALL util_igetenv('ODB_WRAPCOL', def_wrapcol, wrapcol)
if (wrapcol <= 0) wrapcol = def_wrapcol

CALL util_igetenv('ODB_SHOWDBIDX', def_showdbidx, showdbidx)
if (showdbidx > 0) then
  showdbidx = 1
else
  showdbidx = 0
endif

CALL util_igetenv('ODB_IO_LOCK', def_haveiolock, haveiolock)
if (showdbidx <= 0) haveiolock = 0

CALL util_igetenv('ODB_LDA_METHOD', def_lda_method, lda_method)

CALL util_igetenv('ODB_MAXHANDLE', def_maxhandle, maxhandle)
if (maxhandle <= 0) maxhandle = def_maxhandle
CALL cODB_alloc_poolmask(maxhandle)

allocate(db(maxhandle))

do j=1,maxhandle
  db(j)%inuse = .FALSE.
  db(j)%newdb = .TRUE.
  db(j)%altered = .FALSE.
  db(j)%readonly = .FALSE.
  db(j)%io_method = 1
  db(j)%glbNpools = 0
  db(j)%locNpools = 0
  db(j)%iounit = -1
  db(j)%ntables = 0
  db(j)%naid = 0
  db(j)%nfileblocks = 0
  db(j)%CreationDT(:) = 0
  db(j)%AnalysisDT(:) = 0
  db(j)%name = '(not defined)'
  nullify(db(j)%poolidx)
  nullify(db(j)%ioaid)
  nullify(db(j)%grpsize)
  nullify(db(j)%ctables)
  nullify(db(j)%ciomap)
  nullify(db(j)%io_volume)
  nullify(db(j)%vpoolmap)
  nullify(db(j)%vpoolmap_idx)
  db(j)%nvmap = 0
enddo

pint          = prt_t( 1, 13, '(1x,a12)', "%s%*lld"//char(0))
puint         = prt_t( 2, 13, '(1x,a12)', "%s%*llu"//char(0))
preal         = prt_t( 3, 22, '(1x,1p,g21.14)', "%s%*.14g"//char(0))
pstring       = prt_t( 4, 12, "(2x,'''',a8,'''')", "%s%*s'%8s'"//char(0))
pbitf         = prt_t( 5, 34, '(2x,b32.32)', 'N/A')
phex          = prt_t( 6, 12, '(2x,"0x",z8.8)', "%s%*s0x%8.8llx"//char(0))
pyyyymmdd     = prt_t( 7, 14, '(6x,i8.8)', "%s%*.8lld"//char(0))
! But when ODB_PRINT_PRETTY_DATE_TIME=1, then use the following format below DD-mon-YYYY
pyyyymmdd_pdt = prt_t(17, 14, '(3x,i2.2,"-",a3,"-",i4.4)', "%s%s"//char(0))
phhmmss       = prt_t( 8, 12, '(6x,i6.6)', "%s%*.6lld"//char(0))
! But when ODB_PRINT_PRETTY_DATE_TIME=1, the use the following format below HH:MM:SS
phhmmss_pdt   = prt_t(18, 12, '(4x,i2.2,":",i2.2,":",i2.2)', "%s%s"//char(0))
pdbidx        = prt_t( 9, 20, '(i10,i10.10)', "%s%10u%10.10u"//char(0))
pvarprec      = prt_t(10, 22, '(1x,1p,g21.14)', "%s%*.*g"//char(0))
plinkoffset_t = prt_t( 1, 13, '(1x,a12)', "%s%*lld"//char(0))
plinklen_t    = prt_t( 1, 13, '(1x,a12)', "%s%*lld"//char(0))
punknown      = prt_t(-1, 12, '(6x,"<what>")','N/A')

#ifdef DYNAMIC_LINKING
!-- Enforce some routines into executable even if they
!   were not directly called.
!   They may be used by the dynamically linked objects.
!   These externals will in turn bring more code in.

CALL dummy_load(odb_debug_print)
CALL dummy_load(cma_open)
CALL dummy_load(cma_close)
CALL dummy_load(cma_readb)
CALL dummy_load(cma_writeb)
CALL dummy_load(codb_twindow)
CALL dummy_load(codb_tdiff)
CALL dummy_load(codb_trace)
CALL dummy_load(codb_filesize)
CALL dummy_load(codb_datetime)
CALL dummy_load(codb_abort_func)
CALL dummy_load(codb_putenv)
CALL dummy_load(codb_getenv)
CALL dummy_load(codb_pause)
CALL dummy_load(codb_system)
CALL dummy_load(codb_getpid)
CALL dummy_load(codb_set_entrypoint)
CALL dummy_load(codb_wait)
CALL dummy_load(codb_subshell)

CALL cdummy_load()
#endif

db_initialized = .TRUE.
END SUBROUTINE init_db

#ifndef USE_CTRIM
#undef ctrim
#undef CTRIM
#endif

#undef trimadjL
#undef trimadjR

END MODULE odbshared
