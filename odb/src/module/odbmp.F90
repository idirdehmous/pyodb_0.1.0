#ifdef RS6K
@PROCESS NOCHECK
#endif
MODULE odbmp
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK, DR_HOOK

#ifdef NAG
use f90_unix_io, only: flush
#endif
USE str, only : sadjustl, sadjustr

IMPLICIT NONE
SAVE
PRIVATE

#ifndef USE_CTRIM
#define ctrim(x) x
#define CTRIM(x) x
#endif

INTEGER(KIND=JPIM), parameter :: ntree = 2

INTEGER(KIND=JPIM), parameter :: Success = 0

INTEGER(KIND=JPIM), parameter :: rootPE = 1
INTEGER(KIND=JPIM), parameter :: comm = 0
INTEGER(KIND=JPIM), parameter :: sync = 0
INTEGER(KIND=JPIM), parameter :: block = 0

INTEGER(KIND=JPIM) :: ODBMP_nproc  = 0
INTEGER(KIND=JPIM) :: ODBMP_myproc = 0
logical :: mp_trace = .FALSE.
logical :: mp_initialized_here = .FALSE.

INTERFACE ODBMP_global
MODULE PROCEDURE &
     &ODBMP_iglobal       , ODBMP_dglobal       , &
     &ODBMP_iglobal_2d    , ODBMP_dglobal_2d    , &
     &ODBMP_iglobal_3d    , ODBMP_dglobal_3d    , &
     &ODBMP_iglobal_scalar, ODBMP_dglobal_scalar
END INTERFACE

INTERFACE ODBMP_exchange1
MODULE PROCEDURE & 
  & ODBMP_iexchange1, ODBMP_dexchange1
END INTERFACE

INTERFACE ODBMP_exchange2
MODULE PROCEDURE & 
  & ODBMP_iexchange2, ODBMP_dexchange2
END INTERFACE

INTERFACE ODBMP_distribute
MODULE PROCEDURE &
  ODBMP_idistribute, ODBMP_ddistribute
END INTERFACE

PUBLIC :: ODBMP_nproc, ODBMP_myproc
PUBLIC :: ODBMP_abort
PUBLIC :: ODBMP_init
PUBLIC :: ODBMP_end
PUBLIC :: ODBMP_global
PUBLIC :: ODBMP_sync
PUBLIC :: ODBMP_exchange1
PUBLIC :: ODBMP_exchange2
PUBLIC :: ODBMP_distribute
PUBLIC :: ODBMP_testready
PUBLIC :: ODBMP_locking
PUBLIC :: ODBMP_trace
PUBLIC :: ODBMP_physproc
PUBLIC :: ODBMP_setup_exchange

#define trimadjL(x) trim(sadjustl(x))
#define trimadjR(x) trim(sadjustr(x))

CONTAINS

!---  Private routines ---

!---  Public routines ---

FUNCTION ODBMP_physproc(x) RESULT(physproc)
INTEGER(KIND=JPIM), intent(in) :: x
INTEGER(KIND=JPIM) :: physproc
!physproc = min(max(1,mod(x-1,ODBMP_nproc)+1),ODBMP_nproc)
physproc = mod(x-1,ODBMP_nproc)+1
END FUNCTION ODBMP_physproc


FUNCTION ODBMP_setup_exchange(opponent, start_proc) &
         RESULT(n)
!-- Setup tournament table approach in order to exchange
!   messages with pairs of processors simultaneously
USE MPL_MODULE
implicit none
INTEGER(KIND=JPIM), intent(out) :: opponent(:)
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: start_proc
INTEGER(KIND=JPIM) :: k, n, istart_proc

CALL MPL_TOUR_TABLE(opponent,KEVEN=n)

if (present(start_proc)) then
  istart_proc = start_proc
else
  istart_proc = 0
endif

do k=1,n
  if (opponent(k) > 0 .and. opponent(k) <= ODBMP_nproc) then
    opponent(k) = opponent(k) + istart_proc
  else
    opponent(k) = -1
  endif
enddo
END FUNCTION ODBMP_setup_exchange


SUBROUTINE ODBMP_trace(trace_on)
implicit none
logical, intent(in) :: trace_on
mp_trace = trace_on
END SUBROUTINE ODBMP_trace

FUNCTION ODBMP_locking(lockid, onoff) RESULT(rc)
implicit none
INTEGER(KIND=JPIM), intent(in) :: lockid, onoff
INTEGER(KIND=JPIM) rc, info(2), i
INTEGER(KIND=JPIM), parameter :: maxlocks = 64
logical, save :: locks(0:maxlocks) = (/(.FALSE., i=0,maxlocks)/)
INTEGER(KIND=JPIM) id
rc = 0
if (onoff /= -1) then
  id = lockid
else
  id = 13
endif
if (mp_trace) then
  info(1) = id
  info(2) = onoff
  CALL cODB_trace(-1, 2,'ODBMP_locking', info, size(info))
endif
if (onoff == 1) then
  if (id >= 0 .and. id <= maxlocks) then
    if (.not.locks(id)) CALL mpe_lock(id, rc)
  else
    CALL mpe_lock(id, rc)
  endif
  if (id >= 0 .and. id <= maxlocks) locks(id) = .TRUE.
else if (onoff == 0) then
  if (id >= 0 .and. id <= maxlocks) then
    if (locks(id)) CALL mpe_unlock(id, rc)
  else
    CALL mpe_unlock(id, rc)
  endif  
  if (id >= 0 .and. id <= maxlocks) locks(id) = .FALSE.
else if (onoff == -1) then
!-- A special case: enforce locking/unlocking using lock#13
  if (locks(id)) then  ! Already locked ==> then unlock
    CALL mpe_unlock(id, rc)
    locks(id) = .FALSE.
  else
    CALL mpe_lock(id, rc)
    locks(id) = .TRUE.
  endif
endif
END FUNCTION ODBMP_locking


SUBROUTINE ODBMP_abort(routine, s, code, really_abort)
USE OML_MOD, ONLY : OML_MY_THREAD
implicit none
character(len=*), intent(in) :: routine
character(len=*), intent(in), optional :: s
INTEGER(KIND=JPIM),          intent(in), optional :: code
logical,          intent(in), optional :: really_abort
logical do_abort
character(len=20) errwarn
INTEGER(KIND=JPIM) :: idummy(1)

errwarn = '*** Error'
do_abort = .TRUE.

if (present(really_abort)) then
  if (.not. really_abort) errwarn = '*** Warning'
  do_abort = really_abort
endif

write(0,'(1x,a,i3,a)')trim(errwarn)//' in routine "'//trimadjL(routine)//&
          '" (thread#',OML_MY_THREAD(),')'

if (present(s))    write(0,*) trimadjL(s)
if (present(code)) write(0,*) 'Message code =',code

if (do_abort) then
  if (mp_trace) then
    if (present(code)) then
      CALL cODB_trace(-1, 2,'ODBMP_abort:'//routine, (/code/), 1)
    else
      CALL cODB_trace(-1, 2,'ODBMP_abort:'//routine, idummy, 0)
    endif

    CALL cODB_trace_end()
  endif

#ifdef RS6K
  CALL XL__TRBK()
#endif

  if (present(s)) then
    CALL cODB_abort_func(trim(routine)//' : '//trim(s)//char(0))
  else
    CALL cODB_abort_func(trim(routine)//' : '//char(0))
  endif
endif
END SUBROUTINE ODBMP_abort



SUBROUTINE ODBMP_init(LDsync)

#ifdef USE_8_BYTE_WORDS
  Use mpi4to8, Only : MPI_INITIALIZED => MPI_INITIALIZED8
#endif
USE MPL_MODULE
implicit none
logical, intent(in), optional :: LDsync
INTEGER(KIND=JPIM) :: rc, mbxsize, ienforce, iverbose
logical, save :: first_time = .TRUE.
integer :: llinit
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODBMP:ODBMP_INIT',0,ZHOOK_HANDLE)
if (first_time) then
  if (MPL_NUMPROC == -1) then
    !-- Thanks to Andreas Rhodin, DWD 27/08/2004
    CALL MPI_INITIALIZED (llinit, rc) ! A direct call to MPI-routine, not via MPL ;-(
    CALL MPL_INIT(LDINFO=.FALSE.) ! Regardless of LLinit, still need to get ECMWF MPL local variables in sync
    mp_initialized_here = llinit == 0
  endif
  ODBMP_nproc = MPL_NPROC()
  ODBMP_myproc = MPL_MYRANK()
  first_time = .FALSE.
  CALL cODB_init(ODBMP_myproc, ODBMP_nproc)
!-- Not needed since MPL-library has already taken care of this shitoger
!  if (ODBMP_nproc > 1) then
!    ienforce = 0
!    iverbose = 0
!    CALL fODB_propagate_env(ienforce, iverbose)
!  endif
  if (present(LDsync)) then
    if (LDsync) CALL ODBMP_sync()
  else
    CALL ODBMP_sync()
  endif
endif
IF (LHOOK) CALL DR_HOOK('ODBMP:ODBMP_INIT',1,ZHOOK_HANDLE)
END SUBROUTINE ODBMP_init



FUNCTION ODBMP_end() RESULT(rc)
USE MPL_MODULE
implicit none
INTEGER(KIND=JPIM) :: rc
INTEGER(KIND=JPIM), parameter :: iperform(1) = 1
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODBMP:ODBMP_END',0,ZHOOK_HANDLE)
rc=0
if (mp_trace) then
  CALL cODB_trace(-1, 2,'ODBMP_end',iperform, 1)
endif
if (mp_initialized_here) CALL MPL_END(KERROR=rc)
IF (LHOOK) CALL DR_HOOK('ODBMP:ODBMP_END',1,ZHOOK_HANDLE)
END FUNCTION ODBMP_end



SUBROUTINE ODBMP_sync(where)
USE MPL_MODULE
implicit none
INTEGER(KIND=JPIM), intent(in), optional :: where
INTEGER(KIND=JPIM) :: rc, trace_data(2), trace_len
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODBMP:ODBMP_SYNC',0,ZHOOK_HANDLE)
!      write(0,*)'ODBMP_sync: On PE#',ODBMP_myproc
if (ODBMP_nproc > 1) then
  if (mp_trace) then
    trace_len = 0
    trace_len = trace_len + 1
    trace_data(trace_len) = ODBMP_nproc
    if (present(where)) then
      trace_len = trace_len + 1
      trace_data(trace_len) = where
    endif
    CALL cODB_trace(-1, 2,'ODBMP_sync',trace_data,trace_len)
  endif
  CALL MPL_BARRIER
endif
IF (LHOOK) CALL DR_HOOK('ODBMP:ODBMP_SYNC',1,ZHOOK_HANDLE)
END SUBROUTINE ODBMP_sync



FUNCTION ODBMP_testready(with) RESULT(is_ready)
USE MPL_MODULE
implicit none
INTEGER(KIND=JPIM), intent(in) :: with
INTEGER(KIND=JPIM) :: rc
INTEGER(KIND=JPIM), parameter :: msgtag = 1996
INTEGER(KIND=JPIM), parameter :: msglen = 1
INTEGER(KIND=JPIM) :: target, msg, info(4)
INTEGER(KIND=JPIM) :: recv_count, recv_from, recv_tag
logical is_ready

is_ready = .TRUE.
target = max(1,min(abs(with),ODBMP_nproc))
!      write(0,*)'ODBMP_testready: On PE#',ODBMP_myproc,
!     $     ':',target,':',with
if (target /= ODBMP_myproc) then
  if (with > 0) then
!--   I'll send() an acknowledgement that I'm ready now
    msg = ODBMP_myproc
    if (mp_trace) then
      info(1) = target
      info(2) = msglen
      info(3) = msgtag
      CALL cODB_trace(-1, 2,'ODBMP_testready:send',info,3)
    endif
    CALL MPL_SEND(msg,kdest=target,ktag=msgtag)
  else if (with < 0) then
!--   I'll wait in recv() until the recipient is ready
    if (mp_trace) then
      info(1) = target
      info(2) = msglen
      info(3) = msgtag
      CALL cODB_trace(-1, 1,'ODBMP_testready:recv',info,3)
    endif
    CALL MPL_RECV(msg,ksource=target,ktag=msgtag,&
     &kount=recv_count,kfrom=recv_from,krecvtag=recv_tag,kerror=rc)
    if (mp_trace) then
      info(1) = recv_from
      info(2) = recv_count
      info(3) = recv_tag
      info(4) = rc
      CALL cODB_trace(-1, 0,'ODBMP_testready:recv',info,4)
    endif
    if (rc == Success) rc = recv_count
    is_ready = (&
     &msg == target .and. &
     &recv_from == target .and.&
     &recv_tag == msgtag .and.&
     &rc == 1)
  endif
endif
END FUNCTION ODBMP_testready

#undef INT_VERSION
#undef REAL_VERSION

#define INT_VERSION 4
#include "fodbmp.h"
#include "fodbmp1.h"
#include "fodbmp2.h"
#undef INT_VERSION

#define REAL_VERSION 8
#include "fodbmp.h"
#include "fodbmp1.h"
#include "fodbmp2.h"
#undef REAL_VERSION

#ifndef USE_CTRIM
#undef ctrim
#undef CTRIM
#endif

#undef trimadjL
#undef trimadjR

END MODULE odbmp
