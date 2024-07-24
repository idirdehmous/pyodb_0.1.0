MODULE odb

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK, DR_HOOK
USE MPL_MODULE, ONLY : MPL_GETARG
USE OML_MOD    ,ONLY : OML_IN_PARALLEL, OML_NUM_THREADS

#ifdef NAG
use f90_unix_io,  only: flush
#endif

USE odbshared ! The shared (mainly internal) stuff
USE odbmp ! The message passing module
USE odbutil ! Misc utilities
USE str, only : toupper, sadjustl, sadjustr
USE odbsort, only : keysort
USE odbiomap
USE odbgetput, only : ODB_get, ODB_put, ODB_gethandle, ODB_getsize

IMPLICIT NONE
SAVE
PRIVATE

#ifndef USE_CTRIM
#define ctrim(x) x
#define CTRIM(x) x
#endif

#define trimadjL(x) trim(sadjustl(x))
#define trimadjR(x) trim(sadjustr(x))

#include "fodb_checkviewreg.h"
#include "msgpass_loaddata.h"
#include "msgpass_storedata.h"
#include "msgpass_loadobs.h"
#include "msgpass_storeobs.h"

INTERFACE ODB_distribute
MODULE PROCEDURE & 
  & ODB_distribute_str  , ODB_distribute_vecstr, &
  & ODB_distribute_int  , ODB_distribute_vecint, &
  & ODB_distribute_real8, ODB_distribute_vecreal8
END INTERFACE

public :: ODB_open
public :: ODB_load
public :: ODB_store
public :: ODB_select
public :: ODB_cancel
public :: ODB_close
public :: ODB_swapout
public :: ODB_init
public :: ODB_end
public :: ODB_distribute
public :: ODB_pack
public :: ODB_unpack
public :: ODB_addpools
public :: ODB_remove
public :: ODB_release

!-- Very dangerous routines; avoid using these two unless you know
!   what you're doing; especially true with ODB_putindices !!
public :: ODB_getindices
public :: ODB_putindices

CONTAINS

SUBROUTINE ODB_getindices(handle, view, table, &
      nrows, ncols, nstart_pos, &
      poolno, indices, using)
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle
character(len=*), intent(in) :: view, table
INTEGER(KIND=JPIM), intent(out) :: nrows, ncols
INTEGER(KIND=JPIM), intent(in) :: nstart_pos
INTEGER(KIND=JPIM), intent(in) :: poolno
INTEGER(KIND=JPIM), intent(out), OPTIONAL :: indices(:,:)
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: using
INTEGER(KIND=JPIM) :: idxlen, ipoolno
character(len=maxvarlen), allocatable :: tablez(:)
logical :: is_table, all_tables
INTEGER(KIND=JPIM) :: ntables, rc, jt, using_it
INTEGER(KIND=JPIM) :: info(8)
character(len=256) clinfo
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_GETINDICES',0,ZHOOK_HANDLE)

nrows = 0
ncols = 0

if (odbHcheck(handle,'ODB_getindices')) then
  is_table = .FALSE.
  if (len(view) >= 1) is_table = (view(1:1) == '@')
  if (is_table) goto 99999

  ipoolno = get_poolno(handle, poolno)

  if (ipoolno == -1 .or. .not. ODB_valid_poolno(ipoolno)) then
    CALL ODB_abort('ODB_getindices',&
      &'Specific pool number is missing for '//&
      &'view="'//trim(view)//'", table="'//trim(table)//'"',&
      &ipoolno)
    rc = -1
    goto 99999
  endif

  using_it = 0
  if (present(using)) using_it = using

  all_tables = .TRUE.
  if (len(table) >= 1) all_tables = (table == '*')

  if (all_tables) then
    ntables = ODB_getnames(handle, view, 'table')
  else
    ntables = 1
  endif

!-- Get minimal dimensions for indices(:,:)-array : nrows x ncols

  rc = ODB_getsize(handle, view, nrows, ncols, poolno=ipoolno, using=using)
  ncols = ntables ! ncols re-set (has here a different meaning than usually)

  if (nrows == 0 .or. .not.present(indices)) goto 99999

  allocate(tablez(ntables))

  if (all_tables) then
    ntables = ODB_getnames(handle, view, 'table', tablez)
    do jt=1,ntables
      if (tablez(jt)(1:1) == '@') tablez(jt) = tablez(jt)(2:)
    enddo
  else
    if (len(table) >= 2) then
      if (table(1:1) == '@') then
        tablez(1) = table(2:)
      else
        tablez(1) = table
      endif
    else
      tablez(1) = table
    endif
   endif

  idxlen = min(nrows,size(indices, dim=1)-nstart_pos+1)
  ntables = min(ntables,size(indices, dim=2))

  if (db_trace) then
    info(1) = ipoolno
    info(2) = nrows
    info(3) = ncols
    info(4) = nstart_pos
    info(5) = idxlen
    info(6) = ntables
    info(7) = size(indices, dim=1)
    info(8) = size(indices, dim=2)
    clinfo = 'ODB_getindices:'//view
    CALL cODB_trace(handle, -1, clinfo, info, size(info))
  endif

  do jt=1,ntables
    CALL codb_getindex( &
      handle, ipoolno, &
      ctrim(view), ctrim(tablez(jt)), &
      idxlen, indices(nstart_pos,jt), &
      rc, using_it)
    if (db_trace) then
      info(1) = ipoolno
      info(2) = nstart_pos
      info(3) = idxlen
      info(4) = rc
      info(5) = jt
      clinfo = 'ODB_getindices:'//trim(view)//'['//trim(tablez(jt))//']'
      CALL cODB_trace(handle, -1, clinfo, info, 5)
    endif
    if (rc == -2) cycle
    if (rc /= idxlen) then
      CALL ODB_abort('ODB_getindices',&
        &'codb_getindex failed for '//&
        &'view="'//trim(view)//'", table="'//trim(tablez(jt))// &
        &'"; poolno in message code.',&
        &ipoolno)
      goto 99999
    endif
  enddo

  deallocate(tablez)
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_GETINDICES',1,ZHOOK_HANDLE)
END SUBROUTINE ODB_getindices


SUBROUTINE ODB_putindices(handle, view, table, &
      nrows, ncols, nstart_pos, &
      poolno, indices, using)
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle
character(len=*), intent(in) :: view, table
INTEGER(KIND=JPIM), intent(in) :: nrows, ncols, nstart_pos
INTEGER(KIND=JPIM), intent(in) :: poolno
INTEGER(KIND=JPIM), intent(in) :: indices(:,:)
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: using
INTEGER(KIND=JPIM) :: idxlen, ipoolno
character(len=maxvarlen), allocatable :: tablez(:)
logical :: is_table, all_tables
INTEGER(KIND=JPIM) :: ntables, rc, jt, using_it
INTEGER(KIND=JPIM) :: info(8)
character(len=256) clinfo
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_PUTINDICES',0,ZHOOK_HANDLE)

if (odbHcheck(handle,'ODB_putindices')) then
  is_table = .FALSE.
  if (len(view) >= 1) is_table = (view(1:1) == '@')
  if (is_table) goto 99999

  ipoolno = get_poolno(handle, poolno)

  if (ipoolno == -1 .or. .not. ODB_valid_poolno(ipoolno)) then
    CALL ODB_abort('ODB_putindices',&
      &'Specific pool number is missing for '//&
      &'view="'//trim(view)//'", table="'//trim(table)//'"',&
      &ipoolno)
    rc = -1
    goto 99999
  endif

  using_it = 0
  if (present(using)) using_it = using

  all_tables = .TRUE.
  if (len(table) >= 1) all_tables = (table == '*')

  if (all_tables) then
    ntables = ODB_getnames(handle, view, 'table')
  else
    ntables = 1
  endif

  allocate(tablez(ntables))

  if (all_tables) then
    ntables = ODB_getnames(handle, view, 'table', tablez)
    do jt=1,ntables
      if (tablez(jt)(1:1) == '@') tablez(jt) = tablez(jt)(2:)
    enddo
  else
    if (len(table) >= 2) then
      if (table(1:1) == '@') then
        tablez(1) = table(2:)
      else
        tablez(1) = table
      endif
    else
      tablez(1) = table
    endif
  endif

  idxlen = min(nrows,size(indices, dim=1)-nstart_pos+1)
  ntables = min(ncols,ntables,size(indices, dim=2))

  if (db_trace) then
    info(1) = ipoolno
    info(2) = nrows
    info(3) = ncols
    info(4) = nstart_pos
    info(5) = idxlen
    info(6) = ntables
    info(7) = size(indices, dim=1)
    info(8) = size(indices, dim=2)
    clinfo = 'ODB_putindices:'//view
    CALL cODB_trace(handle, -1, clinfo, info, size(info))
  endif

  do jt=1,ntables
    CALL codb_putindex( &
      handle, ipoolno, &
      ctrim(view), ctrim(tablez(jt)), &
      idxlen, indices(nstart_pos,jt), &
      rc, using_it)
    if (db_trace) then
      info(1) = ipoolno
      info(2) = nstart_pos
      info(3) = idxlen
      info(4) = rc
      info(5) = jt
      clinfo = 'ODB_putindices:'//trim(view)//'['//trim(tablez(jt))//']'
      CALL cODB_trace(handle, -1, clinfo, info, 5)
    endif
    if (rc == 0) cycle
    if (rc /= idxlen) then
      CALL ODB_abort('ODB_putindices',&
        &'codb_putindex failed for '//&
        &'view="'//trim(view)//'", table="'//trim(tablez(jt))//&
        &'"; poolno in message code.',&
        &ipoolno)
      goto 99999
    endif
  enddo

  deallocate(tablez)
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_PUTINDICES',1,ZHOOK_HANDLE)
END SUBROUTINE ODB_putindices


FUNCTION ODB_init(myproc, nproc, pid, tid, ntid) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(out), optional :: myproc, nproc, pid
INTEGER(KIND=JPIM), intent(out), optional :: tid, ntid
INTEGER(KIND=JPIM), external :: get_max_threads, get_thread_id
INTEGER(KIND=JPIM) :: rc
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_INIT',0,ZHOOK_HANDLE)
rc = 0
if (.not. db_initialized) CALL init_db()
if (present(myproc)) myproc = ODBMP_myproc
if (present(nproc)) nproc = ODBMP_nproc
if (present(pid)) CALL cODB_getpid(pid)
if (present(tid)) tid = get_thread_id()
if (present(ntid)) ntid = get_max_threads()
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_INIT',1,ZHOOK_HANDLE)
END FUNCTION ODB_init



FUNCTION ODB_end() RESULT(rc)
implicit none
INTEGER(KIND=JPIM) :: rc
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_END',0,ZHOOK_HANDLE)
rc = ODBMP_end()
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_END',1,ZHOOK_HANDLE)
END FUNCTION ODB_end




FUNCTION ODB_close(handle, save) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in)           :: handle
logical, intent(in), optional :: save
INTEGER(KIND=JPIM) :: rc
INTEGER(KIND=JPIM) :: j, k, npoolidx
INTEGER(KIND=JPIM) :: iounit, ntables, glbNpools, locNpools
character(len=maxvarlen), allocatable :: ctable(:)
INTEGER(KIND=JPIM) :: isave(1), tmp, jtbl, ichunk, iret, inumt
INTEGER(KIND=JPIM) :: iprevious_numt
INTEGER(KIND=JPIM), external :: get_max_threads
INTEGER(KIND=JPIM) :: use_new_msgpass
character(len=dbnamelen) dbname
REAL(KIND=JPRB) ZHOOK_HANDLE, zwaltim(2), zsum_datavolume
REAL(KIND=JPRB), allocatable :: zdv(:)
character(len=maxvarlen), allocatable :: CLtables(:)
INTEGER(KIND=JPIM), allocatable :: io_order(:)
logical :: LLsave
REAL(KIND=JPRB), external    :: util_walltime

IF (LHOOK) CALL DR_HOOK('ODB:ODB_CLOSE',0,ZHOOK_HANDLE)

rc = 0
if (odbHcheck(handle, 'ODB_close')) then
  isave = 0              ! By default: Do NOT save
  if (present(save)) then
    if (save) isave = 1
  endif
  if (db(handle)%readonly) isave = 0
  LLsave = (isave(1) == 1)

  dbname = db(handle)%name

  CALL cODB_trace(handle, 1,'ODB_close:'//dbname, isave, 1)

  if (LLsave) then
    rc = ODB_store(handle)
  endif

!--   Database files altered on any PEs ?
  if (db(handle)%readonly) then
    db(handle)%altered = .FALSE.
  else
    tmp = 0
    if (db(handle)%altered .or. db(handle)%newdb) tmp = 1
    CALL ODBMP_global('MAX', tmp)
    db(handle)%altered = (tmp == 1)
  endif

  if (db(handle)%altered .and. LLsave) then

    if (ODBMP_myproc == 1) then
!-- Remove possible .odbprune_done -file
      call codb_remove_file('$ODB_SRCPATH_'//trim(db(handle)%name)//'/.odbprune_done',iret)
    endif

    glbNpools = db(handle)%glbNpools
    ntables   = db(handle)%ntables

!--   Message-pass obs. data back to I/O PEs and perform write, if applicable

    if (db(handle)%io_method == 4) then
      CALL util_igetenv('ODB_NEW_MSGPASS', 1, use_new_msgpass)
      if (use_new_msgpass == 1)then
        CALL msgpass_storeobs(handle, rc)
      else
        CALL msgpass_storedata(handle, rc)
      endif
    endif ! if (db(handle)%io_method == 4) then ...

!-- Update I/O-map file(s)
    if (db(handle)%naid > 0) then
      CALL write_iomap(handle, rc)
    endif

!--   Update metadata (on PE#1)

    if (ODBMP_myproc == 1) then
!--   Open and write part#1
      CALL cma_open(iounit, dbname, 'w', rc)
      if (rc < 1) then
        CALL ODB_abort('ODB_close',&
         &'Cannot open metadata for DB="'//&
         &trim(dbname)//'"',&
         &rc)
        rc = -1
        goto 99999
      endif

      CALL cODB_write_metadata(&
       &handle, iounit, glbNpools, &
       &db(handle)%CreationDT, db(handle)%AnalysisDT,&
       &db(handle)%Version_Major, db(handle)%Version_Minor,&
       &db(handle)%io_method,&
       &rc)
      if (rc /= ntables) then
        CALL ODB_abort('ODB_close',&
         &'Unable to write metadata (part#1) for DB="'//&
         &trim(dbname)//'"',&
         &rc)
        rc = -2
        goto 99999
      endif
    endif

!--   Gather TABLE filesizes (on every PE) and communicate
!     the local contributions to PE#1
    rc = ODB_getnames(handle, '*', 'table')
    if (rc /= ntables) then
      CALL ODB_abort('ODB_close',&
       &'Invalid no. of tables found in DB="'//&
       &trim(dbname)//'"',&
       &rc)
      rc = -3
    endif

    locNpools = db(handle)%locNpools

    if (ODBMP_myproc == 1) then
!--   Write part#2 
      CALL cODB_write_metadata2(&
       &handle, iounit, glbNpools, ntables,&
       &rc)
      if (rc /= ntables) then
        CALL ODB_abort('ODB_close',&
         &'Unable to write metadata (part#2) for DB="'//&
         &trim(dbname)//'"',&
         &rc)
        rc = -4
        goto 99999
      endif
!--   Write part#3 (set variables)
      CALL cODB_write_metadata3(handle, iounit, rc)
      if (rc < 0) then
        CALL ODB_abort('ODB_close',&
         &'Unable to write metadata (part#3) for DB="'//&
         &trim(dbname)//'"',&
         &rc)
        rc = -5
        goto 99999
      endif
!-- Close
      CALL cma_close(iounit, rc)
    endif ! if (ODBMP_myproc == 1)

  endif                  ! if (db(handle)%altered .and. LLsave)

  if (db(handle)%rewrite_flags .and. LLsave) then
    call cODB_print_flags_file(dbname, ODBMP_myproc, rc)
  endif

  CALL cODB_close(handle, 0, rc)

  if (rc < 0) then
    CALL ODB_abort('ODB_close(cODB_close)',&
     &'Unable to close DB="'//&
     &trim(dbname)//'"',&
     &rc)
    goto 99999
  endif

  CALL newio_end32(handle, rc)
  if (rc < 0) then
    CALL ODB_abort('ODB_close(newio_end32)',&
     &'Error in finishing DB-I/O="'//&
     &trim(dbname)//'"',&
     &rc)
    goto 99999
  endif

  if (associated(db(handle)%poolidx)) then
    deallocate(db(handle)%poolidx)
    nullify(db(handle)%poolidx)
  endif

  if (associated(db(handle)%ctables)) then
    deallocate(db(handle)%ctables)
    nullify(db(handle)%ctables)
  endif

  if (associated(db(handle)%ioaid)) then
    deallocate(db(handle)%ioaid)
    nullify(db(handle)%ioaid)
  endif

  if (associated(db(handle)%ciomap)) then
    deallocate(db(handle)%ciomap)
    nullify(db(handle)%ciomap)
  endif

  if (associated(db(handle)%grpsize)) then
    deallocate(db(handle)%grpsize)
    nullify(db(handle)%grpsize)
  endif

  if (associated(db(handle)%io_volume)) then
    deallocate(db(handle)%io_volume)
    nullify(db(handle)%io_volume)
  endif

  if (associated(db(handle)%vpoolmap)) then
    deallocate(db(handle)%vpoolmap)
    nullify(db(handle)%vpoolmap)
  endif

  if (associated(db(handle)%vpoolmap_idx)) then
    deallocate(db(handle)%vpoolmap_idx)
    nullify(db(handle)%vpoolmap_idx)
  endif

  db(handle)%nvmap = 0

  db(handle)%altered = .FALSE.
  db(handle)%inuse = .FALSE.

  CALL codb_end_poolmask(handle)

  CALL cODB_trace(handle, 0,'ODB_close:'//dbname, isave, 1)
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_CLOSE',1,ZHOOK_HANDLE)
END FUNCTION ODB_close



FUNCTION ODB_open(dbname, status, npools, old_npools, maxpoolno) RESULT(handle)
implicit none
character(len=*), intent(in)    :: dbname
character(len=*), intent(in)    :: status
INTEGER(KIND=JPIM), intent(inout)        :: npools
! old_npools: Existing npools in the database before ODB_open
INTEGER(KIND=JPIM), intent(out), OPTIONAL:: old_npools
!  maxpoolno: The actual maximum poolno to be used 
!             == "npools", if updatable database
!     Can be  <= "npools", if read/only database
INTEGER(KIND=JPIM), intent(out), OPTIONAL:: maxpoolno
INTEGER(KIND=JPIM) :: rc, handle
INTEGER(KIND=JPIM) :: iret, itmp
character(len=len(status)) CL_status
INTEGER(KIND=JPIM) :: j, iounit, prev_npools, max_poolno, io_method, io_method_env
INTEGER(KIND=JPIM) :: shared_info(4) ! 1=newdb, 2=readonly, 3=io_method, 4=io_method_env
logical newdb, okay
logical readonly
character(len=len(dbname) + 40) :: srcpath_dbname, datapath_dbname
character(len=len(dbname) + 40) :: srcpath, datapath
logical LLsrcpath_empty, LLdatapath_empty
character(len=1024)             :: env_srcpath_dbname, env_datapath_dbname
character(len=1024)             :: env_srcpath, env_datapath
character(len=1024)             :: pwd, env_ioassign
INTEGER(KIND=JPIM) :: idummy, idummy_arr(2)
character(len=2048) clinfo
character(len=maxstrlen) a_out
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_OPEN',0,ZHOOK_HANDLE)

handle = get_handle()
okay = odbHcheck(handle, 'ODB_open')

if (db_trace) then
  clinfo = 'ODB_open:'//trim(dbname)//':'//trim(status)
  CALL cODB_trace(handle, 1, clinfo, idummy_arr, 0)
endif

if (.not. okay) goto 99999

!-- Moved here from open_db() to abort very early on
call mpl_getarg(0, a_out)
call cODB_static_init(ctrim(a_out), trim(dbname), ODBMP_myproc, rc)

if (rc < 0) then
  write(0,*)'***Error: Unrecognized ODB-database "'//trim(dbname)//'"'
  write(0,*)'   Please check that you have linked the application'
  write(0,*)'    with -l'//trim(dbname)//' and (the most recent) _odb_glue.o'
  write(0,*)'   Before that you may have to run : create_odbglue '//trim(dbname)
  CALL ODB_abort('open_db','Unrecognized database DB="'//trim(dbname)//'"',rc)
  goto 99999
endif

!-- Get current directory 
!   Please do not rely on $PWD, since on some system it is not 
!   automatically updated, when 'cd' to another dir ;-(
!--call util_cgetenv('PWD', '.', pwd, idummy)
call codb_getcwd(pwd)

if (db_trace) then
  clinfo = 'ODB_open: PWD='//trim(pwd)
  CALL cODB_trace(handle, 1, clinfo, idummy_arr, 0)
endif

!-- Check for ODB_SRCPATH
srcpath = 'ODB_SRCPATH'
call util_cgetenv(trim(srcpath), ' ', env_srcpath, idummy)
LLsrcpath_empty = (env_srcpath == ' ')

!-- Check for ODB_SRCPATH_<dbname>
!     If not found, check for ODB_SRCPATH and 
!       if found, then set ODB_SRCPATH_<dbname> to ODB_SRCPATH,
!       else set ODB_SRCPATH_<dbname> to current directory
!   If ODB_SRCPATH was not set, then set it to the just-updated ODB_SRCPATH_<dbname>
     
srcpath_dbname = 'ODB_SRCPATH_'//trim(dbname)
call util_cgetenv(trim(srcpath_dbname), ' ', env_srcpath_dbname, idummy)

if (env_srcpath_dbname  == ' ') then
  if (LLsrcpath_empty) then
    clinfo = trim(srcpath_dbname)//'='//trim(pwd)
  else
    clinfo = trim(srcpath_dbname)//'='//trim(env_srcpath)
  endif
  call codb_putenv(clinfo)
  call util_cgetenv(trim(srcpath_dbname), ' ', env_srcpath_dbname, idummy)
endif

if (db_trace) then
  clinfo = 'ODB_open: ODB_SRCPATH_'//trim(dbname)//'='//trim(env_srcpath_dbname)
  CALL cODB_trace(handle, 1, clinfo, idummy_arr, 0)
endif

if (LLsrcpath_empty) then
  clinfo = trim(srcpath)//'='//trim(env_srcpath_dbname)
  call codb_putenv(clinfo)
  call util_cgetenv(trim(srcpath), ' ', env_srcpath, idummy)
endif

if (db_trace) then
  clinfo = 'ODB_open: ODB_SRCPATH'//'='//trim(env_srcpath)
  CALL cODB_trace(handle, 1, clinfo, idummy_arr, 0)
endif

!-- Check for ODB_DATAPATH
datapath = 'ODB_DATAPATH'
call util_cgetenv(trim(datapath), ' ', env_datapath, idummy)
LLdatapath_empty = (env_datapath == ' ')

!-- Check for ODB_DATAPATH_<dbname>
!     If not found, check for ODB_DATAPATH and 
!       if found, then set ODB_DATAPATH_<dbname> to ODB_DATAPATH,
!       else set ODB_DATAPATH_<dbname> to ODB_SRCPATH_<dbname>
!   If ODB_DATAPATH was not set, then set it to the just-updated ODB_DATAPATH_<dbname>
     
datapath_dbname = 'ODB_DATAPATH_'//trim(dbname)
call util_cgetenv(trim(datapath_dbname), ' ', env_datapath_dbname, idummy)

if (env_datapath_dbname  == ' ') then
  if (LLdatapath_empty) then
    clinfo = trim(datapath_dbname)//'='//trim(env_srcpath)
  else
    clinfo = trim(datapath_dbname)//'='//trim(env_datapath)
  endif
  call codb_putenv(clinfo)
  call util_cgetenv(trim(datapath_dbname), ' ', env_datapath_dbname, idummy)
endif

if (db_trace) then
  clinfo = 'ODB_open: ODB_DATAPATH_'//trim(dbname)//'='//trim(env_datapath_dbname)
  CALL cODB_trace(handle, 1, clinfo, idummy_arr, 0)
endif

if (LLdatapath_empty) then
  clinfo = trim(datapath)//'='//trim(env_datapath_dbname)
  call codb_putenv(clinfo)
  call util_cgetenv(trim(datapath), ' ', env_datapath, idummy)
endif

if (db_trace) then
  clinfo = 'ODB_open: ODB_DATAPATH'//'='//trim(env_datapath)
  CALL cODB_trace(handle, 1, clinfo, idummy_arr, 0)
endif

!-- Check for IOASSIGN
call util_cgetenv('IOASSIGN',' ', env_ioassign, idummy)
if (env_ioassign == ' ') then
  call preferred_dbname_ioassign(trim(dbname))
endif

CL_status = trim(sadjustl(status))
CALL toupper(CL_status)

io_method = ODB_NMDI ! undefined
readonly = .FALSE.
rc = 1
iounit = -1
shared_info(:) = 0

if (ODBMP_myproc == 1) then ! Only PE#1 does this

CALL util_igetenv('ODB_IO_METHOD_'//trim(dbname), ODB_NMDI, io_method)
if (db_trace) then
  clinfo = 'ODB_open: ODB_IO_METHOD_'//trim(dbname)
  idummy_arr(1) = io_method
  CALL cODB_trace(handle, 1, clinfo, idummy_arr, 1)
endif
if (io_method == ODB_NMDI) then ! i.e. still undefined; try another env-variable
  CALL util_igetenv('ODB_IO_METHOD', def_io_method, io_method)
  if (db_trace) then
    clinfo = 'ODB_open: ODB_IO_METHOD'
    idummy_arr(1) = io_method
    CALL cODB_trace(handle, 1, clinfo, idummy_arr, 1)
  endif
endif
io_method_env = io_method
if (io_method <= 0) io_method = def_io_method

100 continue
rc = 1
iounit = -1

select case (CL_status)

case ('NEW', 'CREATE')
  CALL cma_open(iounit, dbname, 'w', rc)
  if (rc < 1) then
    write(0,*)'***Error: Unable to open ODB-database "'//trim(dbname)//'"'
    write(0,*)'   Please check that your ODB_SRCPATH_'//trim(dbname)//' is set'
    write(0,*)'   Currently it is assumed to be "'//trim(env_srcpath_dbname)//'"'
    CALL ODB_abort('ODB_open',&
     &'Cannot create DB="'//&
     &trim(dbname)//'"',&
     &rc)
    rc = -1
    goto 9999
  endif
  newdb = .TRUE.

case ('OLD', 'R/W', 'READ WRITE', 'READWRITE', 'READ/WRITE')
  CALL cma_open(iounit, dbname, 'r', rc)
  if (rc < 1) then
    CALL cODB_remove_file(dbname, rc) ! remove zero-length file, typically points to /path/<dbname>.dd
    write(0,*)'***Error: Unable to open ODB-database "'//trim(dbname)//'"'
    write(0,*)'   Please check that your ODB_SRCPATH_'//trim(dbname)//' is set'
    write(0,*)'   Currently it is assumed to be "'//trim(env_srcpath_dbname)//'"'
    CALL ODB_abort('ODB_open',&
     &'Cannot open DB="'//&
     &trim(dbname)//'"',&
     &rc)
    rc = -2
    goto 9999
  endif
  newdb = .FALSE.

case ('R/O','READONLY','READ','READ ONLY','READ/ONLY','R')
  readonly = .TRUE.
  !-- Automatically set I/O-method to 5, if conditions are right i.e.
  !   one of the directories { $ODB_SRCPATH_<dbname>/dca, $ODB_SRCPATH/dca, ./dca} exist
  !-- Also, if I/O-method was set to 5 (via ODB_IO_METHOD_<dbname> or ODB_IO_METHOD)
  !   and NONE of the aforementioned directories exist, the I/O-method is reverted
  !   back to default (which is not 5)
  if (db_trace) then
    clinfo = 'ODB_open: Before detect_dcadir(): I/O-method, method_env ='
    idummy_arr(1) = io_method
    idummy_arr(2) = io_method_env
    CALL cODB_trace(handle, 1, clinfo, idummy_arr, 2)
  endif
  itmp = io_method_env
  CALL cODB_detect_dcadir(dbname, io_method_env)
  if (io_method_env <= 0) io_method_env = def_io_method
  if (itmp /= io_method_env) io_method = io_method_env
  if (db_trace) then
    clinfo = 'ODB_open:  After detect_dcadir(): I/O-method, method_env ='
    idummy_arr(1) = io_method
    idummy_arr(2) = io_method_env
    CALL cODB_trace(handle, 1, clinfo, idummy_arr, 2)
  endif
  CL_status = 'OLD'
  goto 100

case ('*', 'UNKNOWN', 'UPDATE', 'APPEND', 'ADD', 'WRITE', 'W', 'A')
  CALL cma_open(iounit, dbname, 'r', rc)
  if (rc == -1) then
    CL_status = 'NEW'
    goto 100
  else if (rc < 1) then
    write(0,*)'***Error: Unable to open ODB-database "'//trim(dbname)//'"'
    write(0,*)'   Please check that your ODB_SRCPATH_'//trim(dbname)//' is set'
    write(0,*)'   Currently it is assumed to be "'//trim(env_srcpath_dbname)//'"'
    CALL ODB_abort('ODB_open',&
     &'Error in opening DB="'//&
     &trim(dbname)//'"',&
     &rc)
    rc = -3
    goto 9999
  endif
  newdb = .FALSE.

case default
  CALL ODB_abort(&
        &'ODB_open',&
        &'Unrecognized 2nd arg (status flag)="'//trim(status)//&
        &'" for DB-name="'//&
        &trim(dbname)//'"')
  rc = -4
  goto 9999
end select

if (newdb)    shared_info(1) = 1
if (readonly) shared_info(2) = 1
shared_info(3) = io_method
shared_info(4) = io_method_env

!!write(0,*)'Shared info#1: ', shared_info(:)

endif ! if (ODBMP_myproc == 1) then ...

CALL ODBMP_global('SUM', shared_info)
!!write(0,*)'Shared info#2: ', shared_info(:)
newdb         = (shared_info(1) == 1)
readonly      = (shared_info(2) == 1)
io_method     = shared_info(3)
io_method_env = shared_info(4)

iret = 0
if (rc == 1) then
  CALL open_db(handle, dbname, &
               npools, prev_npools, max_poolno, &
               iounit, newdb, readonly, io_method, io_method_env, rc)
  if (rc > 0) then
    if (present(old_npools)) old_npools = prev_npools
    if (present(maxpoolno))  maxpoolno = max_poolno
    if (ODBMP_myproc == 1) then
      CALL cma_close(iounit, iret)
      if (iret /= 0) then
        CALL ODB_abort('ODB_open: cma_close', dbname, iret)
        rc = -6
        goto 9999
      endif
    endif
  endif
endif

if (rc <= 0) then
  CALL ODB_abort('ODB_open', dbname, rc)
  rc = -5
  goto 9999
endif

!--   Common exit path
9999 continue

if (rc <= 0) handle = rc

if (db_trace) then
  clinfo = 'ODB_open:'//trim(dbname)//':'//trim(status)
  idummy_arr(1) = npools
  CALL cODB_trace(handle, 0, clinfo, idummy_arr, 1)
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_OPEN',1,ZHOOK_HANDLE)
END FUNCTION ODB_open


FUNCTION ODB_store(handle, poolno, sync) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in)  :: handle
INTEGER(KIND=JPIM), intent(in), optional :: poolno
logical,   intent(in), optional :: sync
INTEGER(KIND=JPIM) :: rc, ipoolno, iret, rcsave
INTEGER(KIND=JPIM) :: idummy_arr(2), io_method
INTEGER(KIND=JPIM), parameter :: enforce = 0
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_STORE',0,ZHOOK_HANDLE)

rc = 0
if (odbHcheck(handle, 'ODB_store')) then
  if (db(handle)%readonly) goto 99999
  ipoolno = get_poolno(handle, poolno)
  io_method = db(handle)%io_method
  idummy_arr(1) = ipoolno
  idummy_arr(2) = io_method
  CALL cODB_trace(handle, 1, 'ODB_store', idummy_arr, 2)
  CALL cODB_store(handle, ipoolno, io_method, rc)
  rcsave = rc
  ! puts ALL tables to disk, if not already (depends on actual io_method, though!)
  CALL newio_flush32(handle, ipoolno, enforce, -1, '*', rc) !
  idummy_arr = rc
  CALL cODB_trace(handle, 0, 'ODB_store', idummy_arr, 1)
  if (rc < 0) then
    CALL ODB_abort('ODB_store',&
     &'Cannot store DB="'//trim(db(handle)%name)//'"',&
     &rc)
    goto 99999
  endif
  db(handle)%altered = db(handle)%altered .or. (rcsave > 0)
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_STORE',1,ZHOOK_HANDLE)
END FUNCTION ODB_store



FUNCTION ODB_load(handle, poolno, sync) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in)  :: handle
INTEGER(KIND=JPIM), intent(in), optional :: poolno
logical,   intent(in), optional :: sync
INTEGER(KIND=JPIM) :: rc, ipoolno, iret
INTEGER(KIND=JPIM) :: idummy_arr(2), io_method
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_LOAD',0,ZHOOK_HANDLE)

rc = 0
if (odbHcheck(handle, 'ODB_load')) then
  ipoolno = get_poolno(handle, poolno)
  io_method = db(handle)%io_method
  idummy_arr(1) = ipoolno
  idummy_arr(2) = io_method
  CALL cODB_trace(handle, 1, 'ODB_load', idummy_arr, 2)
  CALL cODB_load(handle, ipoolno, io_method, rc)
  idummy_arr = rc
  CALL cODB_trace(handle, 0, 'ODB_load', idummy_arr, 1)
  if (rc < 0) then
    CALL ODB_abort('ODB_load',&
    &'Cannot load DB="'//trim(db(handle)%name)//'"',&
     &rc)
    goto 99999
  endif
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_LOAD',1,ZHOOK_HANDLE)
END FUNCTION ODB_load


FUNCTION ODB_remove(handle, dtname, poolno) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)         :: handle
character(len=*), intent(in)  :: dtname
INTEGER(KIND=JPIM), intent(in), optional :: poolno
logical LLcheck, is_table
INTEGER(KIND=JPIM) ipoolno, rc
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_REMOVE',0,ZHOOK_HANDLE)
is_table = .FALSE.
if (len(dtname) >= 1) then
  is_table = (dtname(1:1) == '@')
endif
rc = 0
LLcheck = odbHcheck(handle, 'ODB_remove')
if (LLcheck .and. is_table) then
  if (.NOT.db(handle)%readonly) then
    ipoolno = get_poolno(handle, poolno)
    CALL cODB_remove(handle, ipoolno, ctrim(dtname), rc)
    if (rc < 0) then
      CALL ODB_abort('ODB_remove',&
       &'Removal of TABLE="'//&
       &trim(dtname)//'" has failed',&
       &rc)
      goto 99999
    endif
  endif
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_REMOVE',1,ZHOOK_HANDLE)
END FUNCTION ODB_remove


RECURSIVE &
FUNCTION ODB_select(handle, dtname, nrows, ncols, nra,&
     &poolno,&
     &setvars, values,&
     &PEvar, replicate_PE, sync, npes_override, recursion, inform_progress, &
     &using) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)           :: handle
INTEGER(KIND=JPIM), intent(out)          :: nrows, ncols
INTEGER(KIND=JPIM), intent(out), optional:: nra
character(len=*), intent(in)  :: dtname
INTEGER(KIND=JPIM), intent(in), optional :: poolno, replicate_PE, npes_override, using
character(len=*), intent(in), optional :: PEvar
character(len=*), intent(in), optional :: setvars(:)
REAL(KIND=JPRB)            , intent(in), optional :: values(:)
logical,   intent(in), optional :: sync, recursion, inform_progress
logical has_PEvar, has_vars, is_table, LLcheck, is_replicate
logical present_PEvar, present_replicate_PE
logical has_aggrfuncs, LLrecursion, LLswapout, LLinform_progress, LLomp_okay
INTEGER(KIND=JPIM) :: rc, ipoolno, npes, j, info(4), using_it, my_it
INTEGER(KIND=JPIM), allocatable :: nrowvec(:)
character(len=maxvarlen) CL_PEvar
INTEGER(KIND=JPIM) :: nvars, ireplicate, vhandle, ipes, iaggrcols
INTEGER(KIND=JPIM) :: ira, iphase_id, irows, icols, igrp
INTEGER(KIND=JPIM) :: locNpools, npoolidx, ipoolno_save, iret, ngrp, irc
REAL(KIND=JPRB) :: dummy
REAL(KIND=JPRB), allocatable :: oldvalues(:)
REAL(KIND=JPRB), allocatable :: dtmp(:,:)
INTEGER(KIND=JPIM) :: glbNpools, i_inform_progress
INTEGER(KIND=JPIM) :: idummy, idummy_arr(0)
character(len=256) clinfo
INTEGER(KIND=JPIM), external :: get_thread_id
REAL(KIND=JPRB) ZHOOK_HANDLE, ZHOOK_OMP

LLrecursion = .FALSE.
if (present(recursion)) LLrecursion = recursion

IF (LHOOK .and. .not.LLrecursion) CALL DR_HOOK('ODB:ODB_SELECT',0,ZHOOK_HANDLE)

is_table = .FALSE.
if (len(dtname) >= 1) is_table = (dtname(1:1) == '@')

rc = 0
nrows = 0
ncols = 0
ireplicate = 0
is_replicate = .FALSE.

present_PEvar = .FALSE.
if (present(PEvar)) then
  present_PEvar = (len_trim(PEvar) > 0)
endif

present_replicate_PE = .FALSE.
if (present(replicate_PE)) then
  present_replicate_PE = (replicate_PE == -1 .or. replicate_PE >= 1)
endif

ipes = 0
if (present(npes_override)) then
  if (npes_override /= ODB_NMDI .and. npes_override > 0) ipes = npes_override
endif

!--   Mutually exclusive, thus can't co-exist ...
if (present_PEvar .and. present_replicate_PE) then
  CALL ODB_abort('ODB_select',&
   &'PEvar and replicate_PE mutually exclusive'//&
   &' in VIEW/TABLE="'//trim(dtname)//'"')
  goto 99999
endif

LLcheck = odbHcheck(handle, 'ODB_select')

has_PEvar = .FALSE.
if (present_PEvar .and. .not. is_table) then
  glbNpools = db(handle)%glbNpools
  npes = glbNpools
  CL_PEvar = PEvar
  has_PEvar = .TRUE.
else if (present_replicate_PE) then
  is_replicate = .TRUE.
  CL_PEvar = 'dummy'
  npes = 1
  ireplicate = replicate_PE
  if (ireplicate >= 1) then
    ireplicate = min(ireplicate,ODBMP_nproc)
  endif
  has_PEvar = .TRUE.
endif

if (.not.has_PEvar) then
  npes = 0
  ireplicate = 0
endif

if (.not.has_PEvar .or. is_replicate) ipes = 0

!-- The following mod allows shuffle-procedure to loop
!   over input data more/less than glbNpools in the input database
if (ipes > 0) npes = ipes

using_it = 0
if (present(using)) using_it = using
my_it = get_thread_id()

if (db_trace) then
  if (has_PEvar) then
    info(1) = npes
    info(2) = ireplicate
    info(3) = ODB_getval(handle, CL_PEvar, dtname)
    info(4) = using_it
    clinfo = 'ODB_select:'//trim(dtname)//'; npes, ireplicate, '//trim(CL_PEvar)//'='
    CALL cODB_trace(handle, 1, clinfo, info, 4)
  else
    CALL cODB_trace(handle, 1,'ODB_select:'//dtname, idummy_arr, 0)
  endif
endif

nvars = 0
if (present(setvars) .and. present(values)) then
  nvars = min(size(setvars), size(values))
endif

has_vars = (nvars > 0)

LLinform_progress=.FALSE.
if (present(inform_progress)) LLinform_progress=inform_progress
if (ODBMP_nproc > 1) LLinform_progress=.FALSE. ! Otherwise messy output 

if (LLcheck) then
  ipoolno = get_poolno(handle, poolno)

  if (has_vars) then
    allocate(oldvalues(nvars))
    do j=1,nvars
      if (is_table) then
        oldvalues(j) = ODB_setval(handle, setvars(j), values(j), using=using_it)
      else
        oldvalues(j) = ODB_setval(handle, setvars(j), values(j), viewname=dtname, using=using_it)
      endif
    enddo
  endif

  CALL fODB_checkviewreg(handle, ctrim(dtname), vhandle, using=using)

  has_aggrfuncs = ODB_has_aggrfuncs(handle, dtname, ncols=iaggrcols, &
       & phase_id=iphase_id, poolno=ipoolno, using=using)

#if 0
  write(0,'(1x,a,2L3,3i12)') &
       & 'odb.F90: LLrecursion, has_aggrfuncs, iaggrcols, iphase_id, ipoolno = ',&
       & LLrecursion, has_aggrfuncs, iaggrcols, iphase_id, ipoolno
#endif

  if (has_PEvar) then
    allocate(nrowvec(npes))

    vhandle = ODB_gethandle(handle, dtname, using=using)
    CALL cODB_save_peinfo(vhandle, ireplicate, npes, using_it)

    CALL cODB_mp_select(handle, ipoolno,&
     &ctrim(dtname), nrowvec, ncols, rc,&
     &ctrim(CL_PEvar), npes, ireplicate, using_it)

    if (rc >= 0) then
      if (is_replicate) then
        CALL ODBMP_global('SUM', nrowvec)
        nrows = sum(nrowvec(:))
      else
        CALL ODBMP_global('SUM', nrowvec)
        nrows = 0
        do j=1,npes
          if (ODBMP_physproc(j) == ODBMP_myproc) then
            nrows = nrows + nrowvec(j)
          endif
        enddo
      endif
    else
      CALL ODB_abort('ODB_select',&
       &'Multiprocessor SELECT failed in VIEW="'//&
       &trim(dtname)//'"',&
       &rc)
      goto 99999
    endif

    deallocate(nrowvec)
  else
    if (has_aggrfuncs .and. iphase_id == 0 .and. ipoolno == -1 .and. .not.LLrecursion .and. my_it == 1) then
      ipoolno_save = ipoolno
      locNpools = db(handle)%locNpools
      LLomp_okay = .not. OML_IN_PARALLEL() & ! Prevent nested OpenMP-parallelism
                 & .AND. .not.present(using) ! and avoid already defined thread id
      nrows = 0
      irc = 0 ! Can't have "rc" in REDUCTION-clause, since it's already as the function RESULT
!$OMP PARALLEL PRIVATE(npoolidx,ipoolno,iret,irows,icols,ZHOOK_OMP) &
!$OMP&         REDUCTION(+:irc)                                        IF (LLomp_okay)
      CALL DR_HOOK('ODB:ODB_SELECT>OMP-1',0,ZHOOK_OMP)
!$OMP DO SCHEDULE(DYNAMIC)
      do npoolidx=1,locNpools
        ipoolno = db(handle)%poolidx(npoolidx)
        iret = ODB_select(handle, dtname, irows, icols, poolno=ipoolno, &
             &            recursion=.TRUE., inform_progress=LLinform_progress)
        irc = irc + iret
#if 0
        write(0,'(1x,a,4i12)') &
             & 'odb.F90[1]: ipoolno, iret, irows, icols = ',ipoolno, iret, irows, icols
#endif
      enddo
!$OMP END DO
      CALL DR_HOOK('ODB:ODB_SELECT>OMP-1',1,ZHOOK_OMP)
!$OMP END PARALLEL
      rc = irc
      ipoolno = ipoolno_save
      irc = ODB_getsize(handle, dtname, nrows, ncols, poolno=ipoolno) ! Get reliable "nrows" & "ncols"
#if 0
      write(0,'(1x,a,4i12)') &
           & 'odb.F90> ipoolno,  rc , nrows, ncols = ',ipoolno,  rc , nrows, ncols
#endif
      iphase_id = -1 ! To avoid calling ODB_get/ODB_put below again
    else ! i.e. .NOT. (has_aggrfuncs .and. iphase_id == 0 .and. ipoolno == -1 .and. .not.LLrecursion .and. my_it == 1)
      i_inform_progress = 0
      if (LLinform_progress) i_inform_progress = 1
      CALL cODB_select(handle, ipoolno, ctrim(dtname), nrows, ncols, rc, i_inform_progress, using_it)
    endif
  endif

  if (rc < 0) then
    CALL ODB_abort('ODB_select',&
     &'Cannot select VIEW/TABLE="'//trim(dtname)//'"',&
     &rc)
    goto 99999
  endif

  if (has_aggrfuncs .and. nrows > 0 .and. ncols > 0) then

    if (has_aggrfuncs .and. iphase_id == 0) then
    ! Perform phase#0 processing of aggregate functions and save results into intermediate data structure
    ! Cancel original data indices, but keep intermediate C-structures (pf->tmp) untouched
    ! Perform pool-by-pool, if ipoolno = -1 (else-block)
    ! Normally called recursively i.e. ipoolno is > 0

      ngrp = 0 ! Total no. of rows AFTER aggregate phase#0
      icols = ncols
      locNpools = db(handle)%locNpools

#if 0
      write(0,'(1x,a,4i12)') &
           & 'odb.F90[2]: nrows, ncols, ipoolno, locNpools = ',&
           &           nrows, ncols, ipoolno, locNpools
#endif

      LLswapout = (db(handle)%io_method == 5)
      if (ipoolno > 0) then
        irows = nrows
        ira = ODB_lda(irows)
        allocate(dtmp(1:ira,0:icols))
        igrp = ODB_get(handle, dtname, dtmp, irows, ncols=icols, poolno=ipoolno, &
             & sorted=.FALSE., process_select_distinct=.FALSE.) ! Do aggr. phase#0
#if 0
        write(0,'(1x,a,5i12)') &
             & 'odb.F90[2]: ipoolno, ira, igrp, irows, icols =',ipoolno, ira, igrp, irows, icols
#endif
        if (igrp > 0) then
          ngrp = ngrp + igrp
          rc = ODB_put(handle, dtname, dtmp, igrp, ncols=icols, poolno=ipoolno, &
             & store_intermed=.TRUE.,using=1) ! Now tid#1 can access this again
        endif
        deallocate(dtmp)
        if (LLswapout) then ! Saves plenty of memory in aggregate func calc
          rc = ODB_swapout(handle, dtname, poolno=ipoolno, delete_intermed=.FALSE.)
        else
          rc = ODB_cancel(handle, dtname, poolno=ipoolno, delete_intermed=.FALSE.)
        endif
      else ! ipoolno = -1
        ipoolno_save = ipoolno
        LLomp_okay = .not. OML_IN_PARALLEL() & ! Prevent nested OpenMP-parallelism
                   & .AND. .not.present(using) ! and avoid already defined thread id
!$OMP PARALLEL PRIVATE(npoolidx,ipoolno,iret,irows,icols,dtmp,ira,igrp,ZHOOK_OMP) &
!$OMP&         REDUCTION(+:ngrp)                                                     IF (LLomp_okay)
      CALL DR_HOOK('ODB:ODB_SELECT>OMP-2',0,ZHOOK_OMP)
!$OMP DO SCHEDULE(DYNAMIC)
        do npoolidx=1,locNpools
          ipoolno = db(handle)%poolidx(npoolidx)
          irows = 0
          iret = ODB_getsize(handle, dtname, irows, icols, nra=ira, poolno=ipoolno)
          if (irows > 0) then
#if 0
            write(0,'(1x,a,5i12)') &
                 & 'odb.F90:[1] ipoolno, ira, iret, irows, icols =',ipoolno, ira, iret, irows, icols
#endif
            allocate(dtmp(1:ira,0:icols))
            igrp = ODB_get(handle, dtname, dtmp, irows, ncols=icols, poolno=ipoolno, &
                 & sorted=.FALSE., process_select_distinct=.FALSE.) ! Do aggr. phase#0
#if 0
            write(0,'(1x,a,5i12)') &
                 & 'odb.F90:[2] ipoolno, ira, igrp, irows, icols =',ipoolno, ira, igrp, irows, icols
#endif
            ngrp = ngrp + igrp
            iret = ODB_put(handle, dtname, dtmp, igrp, ncols=icols, poolno=ipoolno, &
                 & store_intermed=.TRUE.,using=1) ! Now tid#1 can access this again
            deallocate(dtmp)
          endif
          if (LLswapout) then ! Saves plenty of memory in aggregate func calc
            iret = ODB_swapout(handle, dtname, poolno=ipoolno, delete_intermed=.FALSE.)
          else
            iret = ODB_cancel(handle, dtname, poolno=ipoolno, delete_intermed=.FALSE.)
          endif
        enddo
!$OMP END DO
      CALL DR_HOOK('ODB:ODB_SELECT>OMP-2',1,ZHOOK_OMP)
!$OMP END PARALLEL
        ipoolno = ipoolno_save
      endif

      rc = ngrp
      nrows = rc
    endif
  endif

  if (has_vars) then
    do j=1,nvars
      if (is_table) then
        dummy = ODB_setval(handle, setvars(j), oldvalues(j), using=using_it)
      else
        dummy = ODB_setval(handle, setvars(j), oldvalues(j), viewname=dtname, using=using_it)
      endif
    enddo
    deallocate(oldvalues)
  endif
endif

if (present(nra)) then
  nra = ODB_lda(nrows)
endif

if (db_trace) then
  info(1) = -nrows
  if (present(nra)) info(1) = nra
  info(2) = nrows
  info(3) = ncols
  CALL cODB_trace(handle, 0,'ODB_select:'//dtname, info, 3)
endif
99999 continue
IF (LHOOK .and. .not.LLrecursion) CALL DR_HOOK('ODB:ODB_SELECT',1,ZHOOK_HANDLE)
END FUNCTION ODB_select



FUNCTION ODB_cancel(handle, dtname, poolno, delete_intermed, using) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)  :: dtname
INTEGER(KIND=JPIM), intent(in), optional :: poolno, using
logical, intent(in), optional :: delete_intermed
INTEGER(KIND=JPIM) :: rc, ipoolno, info(4), idel, using_it
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_CANCEL',0,ZHOOK_HANDLE)

rc = 0
if (odbHcheck(handle, 'ODB_cancel')) then
  ipoolno = get_poolno(handle, poolno)
  idel = 1
  if (present(delete_intermed)) then
    if (.not.delete_intermed) idel = 0
  endif
  using_it = 0
  if (present(using)) using_it = using
  CALL cODB_cancel(handle, ipoolno, ctrim(dtname), rc, idel, using_it)
  if (db_trace) then
    info(1) = ipoolno
    info(2) = rc
    info(3) = idel
    info(4) = using_it
    CALL cODB_trace(handle, -1,'ODB_cancel:'//dtname, info, size(info))
  endif
  if (rc < 0) then
    CALL ODB_abort('ODB_cancel',&
     &'Cannot cancel request on VIEW/TABLE="'//&
     &trim(dtname)//'"',&
     &rc)
    goto 99999
  endif
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_CANCEL',1,ZHOOK_HANDLE)
END FUNCTION ODB_cancel



FUNCTION ODB_swapout(handle, dtname, poolno, save, repack, delete_intermed, using) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)  :: dtname
INTEGER(KIND=JPIM), intent(in), optional :: poolno, using
logical, intent(in), optional :: save, repack, delete_intermed
INTEGER(KIND=JPIM) :: rc, rctot, j, ntables
INTEGER(KIND=JPIM) :: ipoolno, isave, info(3), idummy_arr(1), ilen, ilen_table, idel, using_it
logical is_table, LLslow, LLrepack
character(len=maxvarlen), allocatable :: cltable(:)
character(len=maxvarlen) CLname
character(len=maxstrlen) CL_str
INTEGER(KIND=JPIM), parameter :: enforce = 0
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_SWAPOUT',0,ZHOOK_HANDLE)

is_table = .FALSE.
if (len(dtname) >= 1) then
  is_table = (dtname(1:1) == '@')
endif

rc = 0
!if (.not. is_table) goto 99999

if (odbHcheck(handle, 'ODB_swapout')) then
  ipoolno = get_poolno(handle, poolno)
  isave = 0              ! By default: Do NOT save
  if (present(save)) then
    if (save) isave = 1
  endif
  LLrepack = .FALSE.
  if (present(repack)) then
    LLrepack = repack .and. (isave == 1 .and. db(handle)%io_method == 4)
  endif

  idel = 1
  if (present(delete_intermed)) then
    if (.not.delete_intermed) idel = 0
  endif

  using_it = 0
  if (present(using)) using_it = using

  if (db(handle)%readonly) isave = 0 ! Prevent saving, but proceed with memory release
  if (db_trace) then
    info(1) = isave
    info(2) = ipoolno
    info(3) = idel
    CALL cODB_trace(handle, 1, 'ODB_swapout:'//dtname,&
     &info, size(info))
  endif

  rctot = 0
  if (is_table) then
    if (LLrepack) rc = ODB_pack(handle, ctrim(dtname), ipoolno)
    CALL cODB_swapout(handle, ipoolno, ctrim(dtname), isave, rc, idel, using_it)
    if (isave == 1) then
      CALL newio_flush32(handle, ipoolno, enforce, 1, trim(dtname), rc) ! puts THIS table to disk, if not already
    endif
    rctot = rc
  else if (dtname == '*') then ! Get ALL the tables
    ntables = ODB_getnames(handle, '*', 'table')
    allocate(cltable(ntables))
    ntables = ODB_getnames(handle, '*', 'table', cltable)
    do j=1,ntables
      if (LLrepack) rc = ODB_pack(handle, ctrim(cltable(j)), ipoolno)
      CALL cODB_swapout(handle, ipoolno, ctrim(cltable(j)), isave, rc, idel, using_it)
      if (rc < 0) then
        CLname = cltable(j)
        goto 99
      endif
      rctot = rctot + rc
    enddo
    if (isave == 1) then
      CALL newio_flush32(handle, ipoolno, enforce, -1, '*', rc) ! puts ALL tables to disk, if not already
    endif
  else ! Get all tables associated with this view
    ntables = ODB_getnames(handle, dtname, 'table')
    allocate(cltable(ntables))
    ntables = ODB_getnames(handle, dtname, 'table', cltable)
    CALL cODB_cancel(handle, ipoolno, ctrim(dtname), rc, idel, using_it)
    do j=1,ntables
      if (LLrepack) rc = ODB_pack(handle, ctrim(cltable(j)), ipoolno)
      CALL cODB_swapout(handle, ipoolno, ctrim(cltable(j)), isave, rc, idel, using_it)
      if (rc < 0) then
        CLname = cltable(j)
        goto 99
      endif
      rctot = rctot + rc
    enddo
    if (isave == 1) then
      LLslow = .FALSE.
!! overkill; we don't need this --> CALL codb_strblank(CL_str) ! faster initialization
      CL_str(1:1) = '/'
      ilen = 1
      do j=1,ntables
        ilen_table = len_trim(cltable(j))
        if (ilen+ilen_table+1 > maxstrlen) then
          LLslow = .TRUE.
          exit
        endif
        CL_str(ilen+1:ilen+ilen_table+1) = cltable(j)(1:ilen_table)//'/'
        ilen = ilen + ilen_table + 1
      enddo
      if (LLslow) then ! Adopt one-by-one method, since CL_str was too short
        do j=1,ntables
          CALL newio_flush32(handle, ipoolno, enforce, 1, trim(cltable(j)), rc) ! puts THIS table to disk, if not already
        enddo
      else
        CALL newio_flush32(handle, ipoolno, enforce, ntables, CL_str(1:ilen), rc) ! puts THESE tables to disk, if not already
      endif
    endif ! if (isave == 1) then
  endif
99 continue
  if (allocated(cltable)) deallocate(cltable)
  if (rc < 0) then
    idummy_arr = rc
    CALL cODB_trace(handle, 0, 'ODB_swapout:'//dtname, idummy_arr, 1)
    CALL ODB_abort('ODB_swapout',&
     &'Unable to swapout TABLE="'//&
     &trim(CLname)//'"',&
     &rc)
    goto 99999
  else
    idummy_arr = rctot
    CALL cODB_trace(handle, 0, 'ODB_swapout:'//dtname, idummy_arr, 1)
  endif
  db(handle)%altered = db(handle)%altered .or. (isave == 1 .and. rctot > 0)
  rc = rctot
endif
98 continue
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_SWAPOUT',1,ZHOOK_HANDLE)
END FUNCTION ODB_swapout


FUNCTION ODB_distribute_vecreal8(s, target) RESULT(rc)
REAL(KIND=JPRB), intent(inout) :: s(:)
INTEGER(KIND=JPIM), intent(in), optional :: target
INTEGER(KIND=JPIM) :: rc, recip
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_VECREAL8',0,ZHOOK_HANDLE)
if (.not. db_initialized) CALL init_db()
if (present(target)) then
!--   target == 0 : PE#1 distributes to all other PEs
!             < 0 : I'll receive from abs(target)
!             > 0 : I'll send to target
  recip = target
else
!--   PE#1 distributes to all other PEs
  recip = 0
endif
CALL ODBMP_distribute(s,size(s),recip,rc)
if (rc /= size(s)) then
  CALL ODB_abort('ODB_distribute_vecreal8','Data delivery failed',rc)
  goto 99999
endif
rc = size(s)
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_VECREAL8',1,ZHOOK_HANDLE)
END FUNCTION ODB_distribute_vecreal8

FUNCTION ODB_distribute_real8(s, target) RESULT(rc)
REAL(KIND=JPRB), intent(inout) :: s
INTEGER(KIND=JPIM), intent(in), optional :: target
INTEGER(KIND=JPIM) :: rc
REAL(KIND=JPRB) tmp(1)
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_REAL8',0,ZHOOK_HANDLE)
tmp(1) = s
rc = ODB_distribute(tmp, target)
s = tmp(1)
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_REAL8',1,ZHOOK_HANDLE)
END FUNCTION ODB_distribute_real8


FUNCTION ODB_distribute_vecint(s, target) RESULT(rc)
INTEGER(KIND=JPIM), intent(inout) :: s(:)
INTEGER(KIND=JPIM), intent(in), optional :: target
INTEGER(KIND=JPIM) :: rc, recip
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_VECINT',0,ZHOOK_HANDLE)
if (.not. db_initialized) CALL init_db()
if (present(target)) then
!--   target == 0 : PE#1 distributes to all other PEs
!             < 0 : I'll receive from abs(target)
!             > 0 : I'll send to target
  recip = target
else
!--   PE#1 distributes to all other PEs
  recip = 0
endif
CALL ODBMP_distribute(s,size(s),recip,rc)
if (rc /= size(s)) then
  CALL ODB_abort('ODB_distribute_vecint','Data delivery failed',rc)
  goto 99999
endif
rc = size(s)
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_VECINT',1,ZHOOK_HANDLE)
END FUNCTION ODB_distribute_vecint


FUNCTION ODB_distribute_int(s, target) RESULT(rc)
INTEGER(KIND=JPIM), intent(inout) :: s
INTEGER(KIND=JPIM), intent(in), optional :: target
INTEGER(KIND=JPIM) :: rc
INTEGER(KIND=JPIM) itmp(1)
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_INT',0,ZHOOK_HANDLE)
itmp(1) = s
rc = ODB_distribute(itmp, target)
s = itmp(1)
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_INT',1,ZHOOK_HANDLE)
END FUNCTION ODB_distribute_int

FUNCTION ODB_distribute_str(s, target) RESULT(rc)
character(len=*), intent(inout) :: s
INTEGER(KIND=JPIM), intent(in), optional :: target
INTEGER(KIND=JPIM) :: rc, recip, lens
external cODB_idistribute
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_STR',0,ZHOOK_HANDLE)
if (.not. db_initialized) CALL init_db()
if (present(target)) then
!--   target == 0 : PE#1 distributes to all other PEs
!             < 0 : I'll receive from abs(target)
!             > 0 : I'll send to target
  recip = target
else
!--   PE#1 distributes to all other PEs
  recip = 0
endif
lens = len(s)
CALL cODB_distribute_str(s,cODB_idistribute,recip,rc)
if (rc /= lens) then
  CALL ODB_abort('ODB_distribute_str','Data delivery failed',rc)
  goto 99999
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_STR',1,ZHOOK_HANDLE)
END FUNCTION ODB_distribute_str

FUNCTION ODB_distribute_vecstr(s, target) RESULT(rc)
character(len=*), intent(inout) :: s(:)
INTEGER(KIND=JPIM), intent(in), optional :: target
INTEGER(KIND=JPIM) :: rc, recip, lens
external cODB_idistribute
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_VECSTR',0,ZHOOK_HANDLE)
if (.not. db_initialized) CALL init_db()
if (present(target)) then
!--   target == 0 : PE#1 distributes to all other PEs
!             < 0 : I'll receive from abs(target)
!             > 0 : I'll send to target
  recip = target
else
!--   PE#1 distributes to all other PEs
  recip = 0
endif
lens = len(s) * size(s)
if (lens > 0) then
  CALL cODB_distribute_vecstr(s(1),cODB_idistribute,recip,size(s),rc)
else
  rc = 0
endif
if (rc /= lens) then
  CALL ODB_abort('ODB_distribute_vecstr','Data delivery failed',rc)
  goto 99999
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_DISTRIBUTE_VECSTR',1,ZHOOK_HANDLE)
END FUNCTION ODB_distribute_vecstr

!-- Private routines




SUBROUTINE open_db(handle, dbname, &
                 npools, old_npools, maxpoolno, &
                 iounit, newdb, readonly, io_method, io_method_env, rc)
implicit none
INTEGER(KIND=JPIM), intent(in)  :: handle, iounit, io_method, io_method_env
logical, intent(in)  :: newdb, readonly
INTEGER(KIND=JPIM), intent(inout) :: npools
INTEGER(KIND=JPIM), intent(out) :: old_npools, maxpoolno
character(len=*), intent(in) :: dbname
INTEGER(KIND=JPIM), intent(out) :: rc
INTEGER(KIND=JPIM) :: j, npoolidx, is_new, is_readonly, jtbl, iret
INTEGER(KIND=JPIM) :: glbNpools, locNpools, ntables, naid, io_grpsize
INTEGER(KIND=JPIM) :: tmp(2), ichunk, iadd_vars
INTEGER(KIND=JPIM) :: pool_excess, io_method_updated
INTEGER(KIND=JPIM) :: inumt, iprevious_numt
INTEGER(KIND=JPIM) :: use_new_msgpass
REAL(KIND=JPRB) ZHOOK_HANDLE, zwaltim(2), zsum_datavolume
REAL(KIND=JPRB), allocatable :: zdv(:)
INTEGER(KIND=JPIM), allocatable :: io_order(:)
INTEGER(KIND=JPIM), allocatable :: iconsider(:)
character(len=maxvarlen), allocatable :: CLtables(:)
REAL(KIND=JPRB), external    :: util_walltime
INTEGER(KIND=JPIM), external :: get_max_threads
logical :: LLrewrite_flags
INTEGER(KIND=JPIM) idummy
character(len=256) CLsrcpath
character(len=256) CLdatapath
character(len=256) CLidxpath
character(len=256) pwd
IF (LHOOK) CALL DR_HOOK('ODB:OPEN_DB',0,ZHOOK_HANDLE)

rc = 0
LLrewrite_flags = .FALSE.

call cODB_linkDB(dbname, rc)

if (rc < 0) then
  CALL ODB_abort('open_db','Unable to link DB="'//trim(dbname)//'"',rc)
  goto 99999
endif

ntables = rc

db(handle)%name = dbname
db(handle)%ntables = ntables

io_method_updated = io_method

if (.not. newdb) then
  if (ODBMP_myproc == 1) then
    CALL cODB_read_metadata(&
     &handle, iounit, glbNpools, &
     &db(handle)%CreationDT, db(handle)%AnalysisDT,&
     &db(handle)%Version_Major, db(handle)%Version_Minor,&
     &io_method_updated,&
     &rc)
    write(0,*) 'There are rc = ', rc, ' and ntables = ', ntables
    if (rc /= ntables) then
      CALL ODB_abort('open_db',&
       &'Inconsistent no. of tables (part#1), DB="'//&
       &trim(dbname)//'"',&
       &rc, really_abort=.FALSE.) ! bail out
       if (.not. readonly) LLrewrite_flags = .TRUE.
!      rc = -2
!      goto 99999
    endif
    npools = glbNpools
  endif
else
  CALL cODB_datetime(db(handle)%CreationDT(0), db(handle)%CreationDT(1))
  CALL cODB_analysis_datetime(&
   &db(handle)%AnalysisDT(0), db(handle)%AnalysisDT(1))
  CALL cODB_versions(db(handle)%Version_Major, db(handle)%Version_Minor, idummy, 0)
  if (ODBMP_myproc == 1) npools = max(npools, ODBMP_nproc) ! Make sure it is >= ODBMP_nproc at least
endif

if (ODBMP_myproc == 1) then
  tmp(1) = npools
  tmp(2) = io_method_updated
else
  tmp(:) = 0
endif

CALL ODBMP_global('SUM',tmp)
npools = tmp(1)
glbNpools = npools
old_npools = 0
if (.not. newdb) old_npools = npools
io_method_updated = tmp(2)
db(handle)%io_method = io_method_updated

if (readonly .and. io_method_env == 5) db(handle)%io_method = 5 ! Via DCA; read/only access !

npoolidx = 0
do j=1,npools
  if (ODBMP_physproc(j) == ODBMP_myproc) npoolidx = npoolidx + 1
enddo

locNpools = npoolidx
allocate(db(handle)%poolidx(locNpools))
db(handle)%glbNpools = glbNpools
db(handle)%glbMaxpoolno = glbNpools
db(handle)%locNpools = locNpools

npoolidx = 0
do j=1,glbNpools
  if (ODBMP_physproc(j) == ODBMP_myproc) then
    npoolidx = npoolidx + 1
    db(handle)%poolidx(npoolidx) = j
  endif
enddo

is_new = 0
if (newdb) is_new = 1

CALL codb_init_poolmask(handle, db(handle)%name, glbNpools)

iadd_vars = 1
do j=1,locNpools
  npoolidx = db(handle)%poolidx(j)
  if (readonly) then
    CALL cODB_in_permanent_poolmask(handle, npoolidx, rc)
  else
    rc = 1
  endif
  if (rc == 1) then ! In R/O-mode initialize only those pools that are in permanent poolmask
    CALL cODB_create_pool(handle, db(handle)%name, npoolidx, is_new, db(handle)%io_method, iadd_vars, rc)
    if (rc /= npoolidx) then
      rc = -(1000 + abs(npoolidx))
      goto 99999
    endif
    iadd_vars = 0
  endif
enddo

if (.not. newdb .and. .not. readonly) then
  if (ODBMP_myproc == 1) then
    CALL cODB_read_metadata2(&
     &handle, iounit, glbNpools, ntables,&
     &rc)
    if (rc /= ntables) then
      CALL ODB_abort('open_db',&
       &'Inconsistent no. of tables (part#2), DB="'//&
       &trim(dbname)//'"',&
       &rc, really_abort=.FALSE.) ! bail out
       LLrewrite_flags = .TRUE.
!      rc = -4
!      goto 99999
    endif
  endif
endif

db(handle)%inuse = .TRUE.
db(handle)%newdb = newdb
!*AF 07/02/10 not the best way to do it; we always rewrite IO/map file...
!*AF To check and improve...
!*AF db(handle)%altered = LLrewrite_flags
db(handle)%altered = .TRUE.
db(handle)%iounit = iounit
db(handle)%readonly = readonly

is_readonly = 0
if (readonly) is_readonly = 1

if ((newdb .or. LLrewrite_flags) .and. .not.readonly) then
  db(handle)%rewrite_flags = .TRUE.
else
  db(handle)%rewrite_flags = .FALSE.
endif

if (ODBMP_myproc == 1) then
  write(0,*) 'OPEN_DB: handle=',handle
  write(0,*) 'DataBase-name="'//trim(db(handle)%name)//'"'
  call codb_getcwd(pwd)
  call util_cgetenv('ODB_SRCPATH_'//trim(db(handle)%name), &
       & trim(pwd), CLsrcpath, idummy)
  write(0,*) 'ODB_SRCPATH_'//trim(db(handle)%name)//'="'//CLsrcpath(1:idummy)//'"'
  call util_cgetenv('ODB_DATAPATH_'//trim(db(handle)%name), &
       & trim(CLsrcpath), CLdatapath, idummy)
  write(0,*) 'ODB_DATAPATH_'//trim(db(handle)%name)//'="'//CLdatapath(1:idummy)//'"'
  call util_cgetenv('ODB_IDXPATH_'//trim(db(handle)%name), &
       & trim(CLsrcpath)//'/idx', CLidxpath, idummy)
  write(0,*) 'ODB_IDXPATH_'//trim(db(handle)%name)//'="'//CLidxpath(1:idummy)//'"'
  write(0,*) 'inuse ? ',db(handle)%inuse
  write(0,*) 'newdb ? ',db(handle)%newdb
  write(0,*) 'readonly ? ',db(handle)%readonly
  write(0,*) 'rewrite_flags-file ? ',db(handle)%rewrite_flags
  write(0,*) 'no. of global pools = ',db(handle)%glbNpools
  write(0,*) 'global max poolno = ',db(handle)%glbMaxpoolno
  write(0,*) 'no. of local pools = ',db(handle)%locNpools
  write(0,*) 'max. no. of rows per pool = ',ODB_MAXROWS
  write(0,*) 'I/O-method = ',db(handle)%io_method
  write(0,*) 'iounit = ',iounit
  write(0,*) 'ntables = ',ntables
  write(0,*) 'no. of MPI-tasks = ',ODBMP_nproc
  write(0,*) 'no. of OpenMP-threads = ',get_max_threads()
!         do j=1,db(handle)%locNpools
!            write(0,*) 'Local pool index#',j,' = ',db(handle)%poolidx(j)
!         enddo
endif

CALL newio_start32(db(handle)%name, &
                   handle, maxhandle, db(handle)%io_method, &
                   db(handle)%ntables, is_new, is_readonly, &
                   db(handle)%glbNpools, db(handle)%locNpools, db(handle)%poolidx, &
                   rc)

if (rc < 0) then
  CALL ODB_abort('ODB_open(open_db(newio_start32))',&
   &'Error in initializing DB-I/O structures="'//&
   &trim(db(handle)%name)//'"',&
   &rc)
  goto 99999
endif

naid = 0
if (db(handle)%io_method == 4) then
  naid = IONAID
  glbNpools = db(handle)%glbNpools
  if (associated(db(handle)%ioaid)) deallocate(db(handle)%ioaid)
  allocate(db(handle)%ioaid(glbNpools,ntables,naid))
  db(handle)%ioaid(:,:,:) = 0
  if (associated(db(handle)%ciomap)) deallocate(db(handle)%ciomap)
  if (associated(db(handle)%grpsize)) deallocate(db(handle)%grpsize)
  if (ODBMP_myproc == 1) then
    allocate(db(handle)%ciomap(glbNpools))
    db(handle)%ciomap(:) = ' '
  endif
  allocate(db(handle)%grpsize(glbNpools))
  db(handle)%grpsize(:) = 0
endif
db(handle)%naid = naid
db(handle)%nfileblocks = 0

!-- If running with more processors than no. of pools, it used to fail
!   Now (26/9/2001) we add some pools and update the db(handle)% -structure

if (ODBMP_nproc > glbNpools) then
  pool_excess = ODBMP_nproc - npools
  npools = ODB_addpools(handle, pool_excess)
endif

maxpoolno = db(handle)%glbMaxpoolno

CALL metaddl_write(handle, iounit)

if (.NOT.db(handle)%newdb) then
  if (db(handle)%naid > 0) then ! Fill in ioaid-array from iomap-file(s)
    CALL read_iomap(handle, rc)
  endif

  if (db(handle)%io_method == 4) then
!--   Read and message-pass obs. data to other processors
    CALL util_igetenv('ODB_NEW_MSGPASS', 1, use_new_msgpass)
    if (use_new_msgpass == 1)then
      CALL msgpass_loadobs(handle, rc)
    else
      CALL msgpass_loaddata(handle, rc)
    endif
  endif ! if (db(handle)%io_method == 4) then ...
endif

if (db(handle)%naid > 0) then
  if (db(handle)%newdb) then ! have to get started somehow ...
    if (ODBMP_myproc == 1) then
      db(handle)%ciomap(1) = trim(db(handle)%name)//'.iomap' ! The default IOMAP-file
      db(handle)%nfileblocks = 1
    endif
    CALL util_igetenv('ODB_IO_GRPSIZE', 0, io_grpsize)
    if (io_grpsize <= 0) then ! try $NPES_AN
      CALL util_igetenv('NPES_AN', 0, io_grpsize)
    endif
    if (io_grpsize <= 0) io_grpsize = ODBMP_nproc
    io_grpsize = min(io_grpsize, db(handle)%glbNpools)
    db(handle)%grpsize(1) = io_grpsize
    !-- Every PE does the following, since trivial ==> 
    !   no need for message passing via routine comm_iomap()
    do j=1,db(handle)%glbNpools
      db(handle)%ioaid(j,:,IOAID_FBLOCK)  = 1
      db(handle)%ioaid(j,:,IOAID_GRPSIZE) = io_grpsize
      db(handle)%ioaid(j,:,IOAID_POOLNO)  = j
      db(handle)%ioaid(j,:,IOAID_FILENO)  = 0
      db(handle)%ioaid(j,:,IOAID_OFFSET)  = 0
      db(handle)%ioaid(j,:,IOAID_LENGTH)  = 0
      db(handle)%ioaid(j,:,IOAID_NROWS)   = 0
      db(handle)%ioaid(j,:,IOAID_NCOLS)   = 0
    enddo
  endif
endif

if (ODBMP_myproc == 1) then
  write(0,*)'END of OPEN_DB'
endif

rc = db(handle)%locNpools
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:OPEN_DB',1,ZHOOK_HANDLE)
END SUBROUTINE open_db


SUBROUTINE metaddl_write(handle, iounit)
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle, iounit
INTEGER(KIND=JPIM) :: ioddl, rc
character(len=256) clinfo
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:METADDL_WRITE',0,ZHOOK_HANDLE)
if (db(handle)%newdb .AND. ODBMP_myproc == 1) then
  CALL cODB_write_metadata(&
   &handle, iounit, &
   &db(handle)%glbNpools,&
   &db(handle)%CreationDT, db(handle)%AnalysisDT,&
   &db(handle)%Version_Major, db(handle)%Version_Minor,&
   &db(handle)%io_method,&
   &rc)
  if (rc /= db(handle)%ntables) then
    CALL ODB_abort('open_db',&
     &'Unable to write metadata (part#1) for DB="'//&
     &trim(db(handle)%name)//'"',&
     &rc)
    rc = -5
    goto 99999
  endif
  CALL cODB_write_metadata2(&
   &handle, iounit, &
   &db(handle)%glbNpools,&
   &db(handle)%ntables,&
   &rc)
  if (rc /= db(handle)%ntables) then
    CALL ODB_abort('open_db',&
     &'Unable to write metadata (part#2) for DB="'//&
     &trim(db(handle)%name)//'"',&
     &rc)
    rc = -6
    goto 99999
  endif
  CALL cODB_write_metadata3(handle, iounit, rc)
  if (rc < 0) then
    CALL ODB_abort('open_db',&
     &'Unable to write metadata (part#3) for DB="'//&
     &trim(db(handle)%name)//'"',&
     &rc)
     rc = -7
     goto 99999
  endif

  clinfo = trim(db(handle)%name)//'.sch'
  CALL cma_open(ioddl, clinfo, 'w', rc)
  if (rc < 0) then
    CALL ODB_abort('open_db',&
      &'Unable to open SCHEMA/DDL-file '//trim(db(handle)%name)//'.sch for DB="'//&
     &trim(db(handle)%name)//'"',&
     &rc)
     rc = -8
     goto 99999
  endif
  CALL write_ddl(handle, ioddl, rc)
  if (rc < 0) then
    CALL ODB_abort('open_db',&
      &'Unable to write SCHEMA/DDL-file '//trim(db(handle)%name)//'.sch for DB="'//&
     &trim(db(handle)%name)//'"',&
     &rc)
     rc = -9
     goto 99999
  endif
  CALL cma_close(ioddl, rc)
  if (rc < 0) then
    CALL ODB_abort('open_db',&
      &'Unable to close SCHEMA/DDL-file '//trim(db(handle)%name)//'.sch for DB="'//&
     &trim(db(handle)%name)//'"',&
     &rc)
     rc = -10
     goto 99999
  endif
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:METADDL_WRITE',1,ZHOOK_HANDLE)
END SUBROUTINE metaddl_write


FUNCTION ODB_addpools(handle, pool_excess,old_npools,maxpoolno) RESULT(rc)
!== Adds more pools on-the-fly
!   Please make sure NO OUTSTANDING QUERIES are present
!   i.e. no ODB_select() + ODB_get() processed and ODB_put() being waited
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle, pool_excess
INTEGER(KIND=JPIM), intent(out), OPTIONAL :: old_npools,maxpoolno
INTEGER(KIND=JPIM) :: npools, j, npoolidx, ntables, naid, rc
INTEGER(KIND=JPIM) :: glbNpools, locNpools, excess, is_new, is_readonly
INTEGER(KIND=JPIM) :: old_glbNpools, old_locNpools, io_grpsize
INTEGER(KIND=JPIM) :: idim1, idim2, idim3
INTEGER(KIND=JPIM) :: j1, j2, j3, istart, iadd_vars
logical ldummy, LL_copied
INTEGER(KIND=JPIM), allocatable :: tmp2(:,:)
INTEGER(KIND=JPIM), allocatable :: ioaid(:,:,:)
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_ADDPOOLS',0,ZHOOK_HANDLE)

ldummy = odbHcheck(handle, 'ODB_addpools')

!-- Initialize
rc = db(handle)%glbNpools
if (present(old_npools)) old_npools = db(handle)%glbNpools
if (present(maxpoolno))  maxpoolno = db(handle)%glbMaxpoolno

!== PE#1 rules !!
if (ODBMP_myproc == 1) then
  excess = pool_excess
else
  excess = 0
endif

CALL ODBMP_global('SUM',excess) ! Every processor must participate 

if (excess <= 0) goto 99999

if (ODBMP_myproc == 1) then
  write(0,*) 'ODB_addpools: (handle,dbname)=',handle,trim(db(handle)%name), &
             ' : adding ',excess,' pools'
  write(0,*) '              Old no. of global pools = ',db(handle)%glbNpools
  write(0,*) '              Old global max. poolno = ',db(handle)%glbMaxpoolno
  write(0,*) '              Old no. of local pools = ',db(handle)%locNpools
endif

old_glbNpools = db(handle)%glbNpools
old_locNpools = db(handle)%locNpools

npools = db(handle)%glbNpools + excess

glbNpools = npools
db(handle)%glbNpools = glbNpools
if (.NOT.db(handle)%readonly) db(handle)%glbMaxpoolno = glbNpools

npoolidx = 0
do j=1,npools
  if (ODBMP_physproc(j) == ODBMP_myproc) npoolidx = npoolidx + 1
enddo

locNpools = npoolidx
db(handle)%locNpools = locNpools
if (associated(db(handle)%poolidx)) deallocate(db(handle)%poolidx)
allocate(db(handle)%poolidx(locNpools))
db(handle)%glbNpools = glbNpools
db(handle)%locNpools = locNpools

npoolidx = 0
do j=1,glbNpools ! re-create
  if (ODBMP_physproc(j) == ODBMP_myproc) then
    npoolidx = npoolidx + 1
    db(handle)%poolidx(npoolidx) = j
  endif
enddo

CALL codb_init_poolmask(handle, db(handle)%name, glbNpools)

ntables = db(handle)%ntables

if (ODBMP_myproc == 1) then
  write(0,*) 'DataBase-name="'//trim(db(handle)%name)//'" : Situation after ODB_addpools()'
  write(0,*) 'New no. of global pools = ',db(handle)%glbNpools
  write(0,*) 'New global max poolno = ',db(handle)%glbMaxpoolno
  write(0,*) 'New no. of local pools = ',db(handle)%locNpools
  write(0,*) 'max. no. of rows per pool = ',ODB_MAXROWS
endif

if (present(maxpoolno))  maxpoolno = db(handle)%glbMaxpoolno

is_readonly = 0
if (db(handle)%readonly) is_readonly = 1

iadd_vars = 1
is_new = 1
do j=old_locNpools+1,locNpools ! ... just for the NEW local pools
  npoolidx = db(handle)%poolidx(j)
  if (db(handle)%readonly) then
    CALL cODB_in_permanent_poolmask(handle, npoolidx, rc)
  else
    rc = 1
  endif
  if (rc == 1) then ! In R/O-mode initialize only those pools that are in permanent poolmask
    CALL cODB_create_pool(handle, db(handle)%name, npoolidx, is_new, db(handle)%io_method, iadd_vars, rc)
    if (rc /= npoolidx) then
      rc = -(1000 + abs(npoolidx))
      goto 99999
    endif
    iadd_vars = 0
  endif
enddo

is_new = 0
if (db(handle)%newdb) is_new = 1

!- Just update, or if initialization wasn't done before this call, then do it now
CALL newio_start32(db(handle)%name, &
                   handle, maxhandle, db(handle)%io_method, &
                   db(handle)%ntables, is_new, is_readonly, &
                   db(handle)%glbNpools, db(handle)%locNpools, db(handle)%poolidx, &
                   rc)

if (rc < 0) then
  CALL ODB_abort('ODB_addpools(newio_start32)',&
   &'Error in updating DB-I/O structures="'//&
   &trim(db(handle)%name)//'"',&
   &rc)
  goto 99999
endif

LL_copied = .FALSE.
naid = 0
if (db(handle)%io_method == 4) then
  naid = IONAID
  glbNpools = db(handle)%glbNpools
  LL_copied = associated(db(handle)%ioaid)
  if (LL_copied) then ! copy
    idim1 = size(db(handle)%ioaid, dim=1)
    idim2 = size(db(handle)%ioaid, dim=2)
    idim3 = size(db(handle)%ioaid, dim=3)
    allocate(ioaid(idim1, idim2, idim3))
    ioaid(:,:,:) = db(handle)%ioaid(:,:,:)
    deallocate(db(handle)%ioaid)
    allocate(db(handle)%ioaid(glbNpools,ntables,naid)) ! new allocation
    db(handle)%ioaid(:,:,:) = 0
    do j3=1,min(idim3,naid)
      do j2=1,min(idim2,ntables)
        do j1=1,min(idim1,glbNpools)
          db(handle)%ioaid(j1,j2,j3) = ioaid(j1,j2,j3)
        enddo
      enddo
    enddo
    deallocate(ioaid)
  else
    allocate(db(handle)%ioaid(glbNpools,ntables,naid))
    db(handle)%ioaid(:,:,:) = 0
  endif
  if (.not.LL_copied) then
    if (associated(db(handle)%ciomap)) deallocate(db(handle)%ciomap)
    if (associated(db(handle)%grpsize)) deallocate(db(handle)%grpsize)
    if (ODBMP_myproc == 1) then
      allocate(db(handle)%ciomap(glbNpools))
      db(handle)%ciomap(:) = ' '
    endif
    allocate(db(handle)%grpsize(glbNpools))
    db(handle)%grpsize(:) = 0
  endif
endif
db(handle)%naid = naid
if (.not.LL_copied) db(handle)%nfileblocks = 0

if (db(handle)%naid > 0) then ! have to get started somehow ...
  if (db(handle)%newdb) then
    if (ODBMP_myproc == 1) then
      db(handle)%ciomap(1) = trim(db(handle)%name)//'.iomap' ! The default IOMAP-file
      db(handle)%nfileblocks = 1
    endif
    CALL util_igetenv('ODB_IO_GRPSIZE', 0, io_grpsize)
    if (io_grpsize <= 0) then ! try $NPES_AN
      CALL util_igetenv('NPES_AN', 0, io_grpsize)
    endif
    if (io_grpsize <= 0) io_grpsize = ODBMP_nproc
    db(handle)%grpsize(1) = io_grpsize
    io_grpsize = min(io_grpsize, db(handle)%glbNpools)
  else
    io_grpsize = 0
  endif
  !-- Every PE does the following, since trivial ==> 
  !   no need for message passing via routine comm_iomap()
  istart = 1
  if (LL_copied .and. .not.db(handle)%newdb) istart = old_glbNpools+1
  do j=istart,db(handle)%glbNpools
    db(handle)%ioaid(j,:,IOAID_FBLOCK)  = 1
    db(handle)%ioaid(j,:,IOAID_GRPSIZE) = io_grpsize
    db(handle)%ioaid(j,:,IOAID_POOLNO)  = j
    db(handle)%ioaid(j,:,IOAID_FILENO)  = 0
    db(handle)%ioaid(j,:,IOAID_OFFSET)  = 0
    db(handle)%ioaid(j,:,IOAID_LENGTH)  = 0
    db(handle)%ioaid(j,:,IOAID_NROWS)   = 0
    db(handle)%ioaid(j,:,IOAID_NCOLS)   = 0
  enddo
endif

rc = npools
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_ADDPOOLS',1,ZHOOK_HANDLE)
END FUNCTION ODB_addpools


FUNCTION ODB_pack(handle, dtname, poolno) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)    :: dtname
INTEGER(KIND=JPIM), intent(in), optional :: poolno
INTEGER(KIND=JPIM) :: rc, ipoolno, ipack
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_PACK',0,ZHOOK_HANDLE)
rc = 0
ipack = 1
if (odbHcheck(handle, 'ODB_pack')) then
  ipoolno = get_poolno(handle, poolno)
  ipack = 1
  CALL cODB_packer(handle, ipoolno, ctrim(dtname), ipack, rc)
  if (rc < 0) then
    CALL ODB_abort('ODB_packer',&
     &'Cannot pack TABLE="'//trim(dtname)//'"',&
     &rc)
    goto 99999
  endif
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_PACK',1,ZHOOK_HANDLE)
END FUNCTION ODB_pack


FUNCTION ODB_unpack(handle, dtname, poolno) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)    :: dtname
INTEGER(KIND=JPIM), intent(in), optional :: poolno
INTEGER(KIND=JPIM) :: rc, ipoolno, ipack
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_UNPACK',0,ZHOOK_HANDLE)
rc = 0
if (odbHcheck(handle, 'ODB_unpack')) then
  ipoolno = get_poolno(handle, poolno)
  ipack = 0
  CALL cODB_packer(handle, ipoolno, ctrim(dtname), ipack, rc)
  if (rc < 0) then
    CALL ODB_abort('ODB_packer',&
     &'Cannot unpack TABLE="'//trim(dtname)//'"',&
     &rc)
    goto 99999
  endif
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_UNPACK',1,ZHOOK_HANDLE)
END FUNCTION ODB_unpack



FUNCTION ODB_release(handle, poolno, delete_intermed) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)           :: handle
INTEGER(KIND=JPIM), intent(in), optional :: poolno
logical, intent(in), optional :: delete_intermed
INTEGER(KIND=JPIM) :: rc, ipoolno, glbNpools, j
INTEGER(KIND=JPIM) :: fast_physproc, xfast
REAL(KIND=JPRB) ZHOOK_HANDLE
fast_physproc(xfast) = mod(xfast-1,ODBMP_nproc)+1
IF (LHOOK) CALL DR_HOOK('ODB:ODB_RELEASE',0,ZHOOK_HANDLE)
rc = 0
if (odbHcheck(handle, 'ODB_release')) then
  !-- play safe and allow this routine for READONLY databases only
  if (db(handle)%readonly) then
    ipoolno = get_poolno(handle, poolno)
    rc = ODB_swapout(handle, '*', poolno=ipoolno, save=.FALSE., delete_intermed=delete_intermed)
    if (ipoolno == -1) then
      glbNpools = db(handle)%glbNpools
      do j=1,glbNpools
        if (fast_physproc(j) == ODBMP_myproc) then
          ipoolno = j
          CALL newio_release_pool32(handle, ipoolno, rc)
        endif
      enddo
    else
      CALL newio_release_pool32(handle, ipoolno, rc)
    endif
  endif ! if (db(handle)%readonly) ...
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_RELEASE',1,ZHOOK_HANDLE)
END FUNCTION ODB_release

#ifndef USE_CTRIM
#undef ctrim
#undef CTRIM
#endif

#undef trimadjL
#undef trimadjR

END MODULE odb
