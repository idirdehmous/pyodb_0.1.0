#if INT_VERSION == 4

#define GEN_TYPE          INTEGER(KIND=JPIM)
#define GEN_MSGTYPE       1
#define GEN_GLOPER_SC     ODBMP_iglobal_scalar
#define GEN_GLOPER        ODBMP_iglobal
#define GEN_GLOPER_2D     ODBMP_iglobal_2d
#define GEN_GLOPER_3D     ODBMP_iglobal_3d
#define GEN_GLOPER_NAME  'ODBMP:ODBMP_IGLOBAL'
#define GEN_DISTRIBUTE_1D       ODBMP_idistribute
#define GEN_DISTRIBUTE_1D_NAME 'ODBMP:ODBMP_IDISTRIBUTE'

#elif REAL_VERSION == 8

#define GEN_TYPE          REAL(KIND=JPRB)
#define GEN_MSGTYPE       8
#define GEN_GLOPER_SC     ODBMP_dglobal_scalar
#define GEN_GLOPER        ODBMP_dglobal
#define GEN_GLOPER_2D     ODBMP_dglobal_2d
#define GEN_GLOPER_3D     ODBMP_dglobal_3d
#define GEN_GLOPER_NAME  'ODBMP:ODBMP_DGLOBAL'
#define GEN_DISTRIBUTE_1D       ODBMP_ddistribute
#define GEN_DISTRIBUTE_1D_NAME 'ODBMP:ODBMP_DDISTRIBUTE'

#else

  ERROR in programming : No datatype given (should never have ended up here)

#endif

#if defined(INT_VERSION) || defined(REAL_VERSION)

SUBROUTINE GEN_GLOPER_SC &
    &(operation,&
    &v, root, ldreprod)
implicit none
character(len=*), intent(in)  :: operation
GEN_TYPE,       intent(inout) :: v
INTEGER(KIND=JPIM), intent(in), optional :: root
logical, intent(in), optional :: ldreprod
GEN_TYPE v1d(1)
logical LLcopy
if (ODBMP_nproc <= 1) return
v1d(1) = v
CALL &
    GEN_GLOPER (operation, v1d, root, ldreprod)
if (present(root)) then
  LLcopy = .FALSE.
  if (root == ODBMP_myproc .or. root == -1) LLcopy = .TRUE.
else
  LLcopy = .TRUE.
endif
if (LLcopy) v = v1d(1)
END SUBROUTINE

SUBROUTINE GEN_GLOPER_2D &
    &(operation,&
    &v, root, ldreprod)
implicit none
character(len=*), intent(in)  :: operation
GEN_TYPE,       intent(inout) :: v(:,:)
INTEGER(KIND=JPIM), intent(in), optional :: root
logical, intent(in), optional :: ldreprod
GEN_TYPE v1d(size(v))
INTEGER(KIND=JPIM) :: nr, nc
INTEGER(KIND=JPIM) :: i, j, k
logical LLcopy
if (ODBMP_nproc <= 1) return
nr = size(v,dim=1)
nc = size(v,dim=2)
k = 0
do j=1,nc
  do i=1,nr
    k = k + 1
    v1d(k) = v(i,j)
  enddo
enddo
CALL &
     GEN_GLOPER (operation, v1d, root, ldreprod)
if (present(root)) then
  LLcopy = .FALSE.
  if (root == ODBMP_myproc .or. root == -1) LLcopy = .TRUE.
else
  LLcopy = .TRUE.
endif
if (LLcopy) then
  k = 0
  do j=1,nc
    do i=1,nr
      k = k + 1
      v(i,j) = v1d(k)
    enddo
  enddo
endif
END SUBROUTINE

SUBROUTINE GEN_GLOPER_3D &
    &(operation,&
    &v, root, ldreprod)
implicit none
character(len=*), intent(in)  :: operation
GEN_TYPE,       intent(inout) :: v(:,:,:)
INTEGER(KIND=JPIM), intent(in), optional :: root
logical, intent(in), optional :: ldreprod
GEN_TYPE v1d(size(v))
INTEGER(KIND=JPIM) :: nr, nc, nt
INTEGER(KIND=JPIM) :: i, j, t, k
logical LLcopy
if (ODBMP_nproc <= 1) return
nr = size(v,dim=1)
nc = size(v,dim=2)
nt = size(v,dim=3)
k = 0
do t=1,nt
  do j=1,nc
    do i=1,nr
      k = k + 1
      v1d(k) = v(i,j,t)
    enddo
  enddo
enddo
CALL &
     GEN_GLOPER (operation, v1d, root, ldreprod)
if (present(root)) then
  LLcopy = .FALSE.
  if (root == ODBMP_myproc .or. root == -1) LLcopy = .TRUE.
else
  LLcopy = .TRUE.
endif
if (LLcopy) then
  k = 0
  do t=1,nt
    do j=1,nc
      do i=1,nr
        k = k + 1
        v(i,j,t) = v1d(k)
      enddo
    enddo
  enddo
endif
END SUBROUTINE


SUBROUTINE GEN_GLOPER &
    &(operation,&
    &v, root, ldreprod)
USE MPL_MODULE
implicit none
character(len=*), intent(in)  :: operation
GEN_TYPE,       intent(inout) :: v(:)
INTEGER(KIND=JPIM), intent(in), optional :: root
logical, intent(in), optional :: ldreprod
GEN_TYPE tmp(size(v))
logical LLcopy
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK(GEN_GLOPER_NAME,0,ZHOOK_HANDLE)
if (ODBMP_nproc <= 1) goto 99999
if (present(root)) then
  LLcopy = .FALSE.
  if (root == ODBMP_myproc .or. root == -1) LLcopy = .TRUE.
else
  LLcopy = .TRUE.
endif
if (LLcopy) then ! "v" gets updated directly
  CALL MPL_ALLREDUCE(v,operation,LDREPROD=ldreprod)
else ! Do *not* update values of "v"
  tmp = v
  CALL MPL_ALLREDUCE(tmp,operation,LDREPROD=ldreprod)
endif
99999 continue
IF (LHOOK) CALL DR_HOOK(GEN_GLOPER_NAME,1,ZHOOK_HANDLE)
END SUBROUTINE


SUBROUTINE GEN_DISTRIBUTE_1D &
  (s, slen, with, rc)
USE MPL_MODULE
implicit none
INTEGER(KIND=JPIM), intent(in)    :: slen, with
GEN_TYPE , intent(inout) :: s(slen)
INTEGER(KIND=JPIM), intent(out)   :: rc
INTEGER(KIND=JPIM), parameter :: msgtype = GEN_MSGTYPE
INTEGER(KIND=JPIM), parameter :: msgtag = 1997
INTEGER(KIND=JPIM) :: target, info(4)
INTEGER(KIND=JPIM) :: recv_count, recv_from, recv_tag
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK(GEN_DISTRIBUTE_1D_NAME,0,ZHOOK_HANDLE)
if (ODBMP_nproc > 1) then
  if (with == 0) then
!--   PE#1 (=rootPE) distributes, all other PEs receive;
!     All in one routine
    if (mp_trace) then
      info(1) = rootPE
      info(2) = slen
      info(3) = msgtag
      info(4) = msgtype
      CALL cODB_trace(-1, 1, &
          & GEN_DISTRIBUTE_1D_NAME &
          & //':broadcast',info,4)
    endif
    CALL MPL_BROADCAST(s(:),kroot=rootPE,ktag=msgtag,kerror=rc)
    if (mp_trace) then
      info(1) = rootPE
      info(2) = slen
      info(3) = msgtag
      info(4) = rc
      CALL cODB_trace(-1, 0, &
          & GEN_DISTRIBUTE_1D_NAME &
          & //':broadcast',info,4)
    endif
    if (rc == Success) rc = slen
  else
    target = max(1,min(abs(with),ODBMP_nproc))
    if (with > 0) then
!--   I'll send()
      if (mp_trace) then
        info(1) = target
        info(2) = slen
        info(3) = msgtag
        info(4) = msgtype
        CALL cODB_trace(-1, 2, &
          & GEN_DISTRIBUTE_1D_NAME &
          & //':send',info,4)
      endif
      CALL MPL_SEND(s(:),kdest=target,ktag=msgtag,kerror=rc)
      if (rc == Success) rc = slen
    else
!--   I'll recv()
      if (mp_trace) then
        info(1) = target
        info(2) = slen
        info(3) = msgtag
        info(4) = msgtype
        CALL cODB_trace(-1, 1, &
          & GEN_DISTRIBUTE_1D_NAME &
          & //':recv',info,4)
      endif
      CALL MPL_RECV(s(:),ksource=target,ktag=msgtag,&
       &kount=recv_count,kfrom=recv_from,krecvtag=recv_tag,kerror=rc)
      if (mp_trace) then
        info(1) = recv_from
        info(2) = recv_count
        info(3) = recv_tag
        info(4) = rc
        CALL cODB_trace(-1, 0, &
          & GEN_DISTRIBUTE_1D_NAME &
          & //':recv',info,4)
      endif
      if (rc == Success) rc = recv_count
    endif
  endif
else
!--   Only one PE; do nothing
  rc = slen
endif
99999 continue
IF (LHOOK) CALL DR_HOOK(GEN_DISTRIBUTE_1D_NAME,1,ZHOOK_HANDLE)
END SUBROUTINE

#ifndef NO_UNDEF
#undef GEN_TYPE
#undef GEN_MSGTYPE
#undef GEN_GLOPER_SC
#undef GEN_GLOPER
#undef GEN_GLOPER_2D
#undef GEN_GLOPER_3D
#undef GEN_GLOPER_NAME
#undef GEN_DISTRIBUTE_1D
#undef GEN_DISTRIBUTE_1D_NAME
#endif

#endif
