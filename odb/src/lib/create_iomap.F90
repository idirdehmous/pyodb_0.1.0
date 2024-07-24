SUBROUTINE create_iomap(khandle, kfirst_call, kmaxpools, &
 & kpoff, kfblk, cdfile, kgrpsize, &
 & ktblno, cdtbl, &
 & kpoolno, kfileno, koffset, klength, krows, kcols, &
 & kret) 
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
use odb_module
implicit none
INTEGER(KIND=JPIM), intent(in)  :: khandle, kfblk, kpoff, kgrpsize, krows, kcols
INTEGER(KIND=JPIM), intent(in)  :: kfirst_call, kmaxpools
INTEGER(KIND=JPIM), intent(in)  :: ktblno, kpoolno, kfileno, koffset, klength
INTEGER(KIND=JPIM), intent(out) :: kret

character(len=*), intent(in) :: cdfile, cdtbl
INTEGER(KIND=JPIM) :: itblno, ipoolno, ntables, jt, j, naid
logical :: has_at, LL_first
character(len=maxvarlen), POINTER :: cltable(:)
INTEGER(KIND=JPIM), POINTER :: ioaid(:,:,:) ! Shorthand
REAL(KIND=JPRB) :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('CREATE_IOMAP',0,ZHOOK_HANDLE)
kret = 0

if (ODBMP_myproc /= 1 .AND. LHOOK) CALL DR_HOOK('CREATE_IOMAP',1,ZHOOK_HANDLE)
if (ODBMP_myproc /= 1)      return
if (.NOT.db(khandle)%inuse .AND. LHOOK) CALL DR_HOOK('CREATE_IOMAP',1,ZHOOK_HANDLE)
if (.NOT.db(khandle)%inuse) return

naid = db(khandle)%naid

if (naid == 0 .AND. LHOOK) CALL DR_HOOK('CREATE_IOMAP',1,ZHOOK_HANDLE)
if (naid == 0)  return

if (kfblk <= 0 .AND. LHOOK) CALL DR_HOOK('CREATE_IOMAP',1,ZHOOK_HANDLE)
if (kfblk <= 0)             return

LL_first = (kfirst_call == 1)

if (LL_first .AND. kfblk > db(khandle)%nfileblocks) then
  db(khandle)%ciomap(kfblk) = cdfile
  db(khandle)%grpsize(kfblk) = kgrpsize
  db(khandle)%nfileblocks = max(db(khandle)%nfileblocks, kfblk)
  if (.not.associated(db(khandle)%ctables)) then ! Enforce db(khandle)%ctables(1:ntables) into the cache
!    write(0,*) 'create_iomap.F90: before ODB_getnames(... "table")'
    ntables = ODB_getnames(khandle, '*', 'table')
!    write(0,*) 'create_iomap.F90: ntables[1]=',ntables
    allocate(cltable(ntables))
    ntables = ODB_getnames(khandle, '*', 'table', cltable)
!    write(0,*) 'create_iomap.F90: ntables[2]=',ntables
    deallocate(cltable)
    nullify(cltable)
  endif
endif

cltable => db(khandle)%ctables
ntables = size(cltable)

has_at = .FALSE.
if (len(cdtbl) > 0) then
  has_at = (cdtbl(1:1) == '@')
endif

itblno = -1
if (ktblno >= 1 .AND. ktblno <= ntables) then
  if ((has_at .AND. cltable(ktblno) == cdtbl) .OR. &
     & cltable(ktblno)(2:) == cdtbl) then 
    itblno = ktblno
  endif
endif

if (itblno == -1) then ! search ...
  LOOP: do jt=1,ntables
    if ((has_at .AND. cltable(jt) == cdtbl) .OR. &
       & cltable(jt)(2:) == cdtbl) then 
      itblno = jt
      exit LOOP
    endif
  enddo LOOP
endif

nullify(cltable)

if (itblno == -1) then
  CALL ODB_abort('CREATE_IOMAP()', 'Unable to locate table="'//trim(cdtbl)//'"', ktblno)
endif

ioaid => db(khandle)%ioaid

if (LL_first) then ! Supply sane initial settings for ALL tables in the pool-range
  ioaid(kpoff+1:kpoff+kmaxpools, :, IOAID_FBLOCK)  = kfblk
  ioaid(kpoff+1:kpoff+kmaxpools, :, IOAID_GRPSIZE) = kgrpsize
  do j=1,kmaxpools
    ipoolno = kpoff + j
    ioaid(ipoolno, :, IOAID_POOLNO) = j
  enddo
endif

ipoolno = kpoff + kpoolno
ioaid(ipoolno, itblno, IOAID_POOLNO)  = kpoolno ! A possible override
ioaid(ipoolno, itblno, IOAID_FILENO)  = kfileno
ioaid(ipoolno, itblno, IOAID_OFFSET)  = koffset
ioaid(ipoolno, itblno, IOAID_LENGTH)  = klength
ioaid(ipoolno, itblno, IOAID_NROWS)   = krows
ioaid(ipoolno, itblno, IOAID_NCOLS)   = kcols

nullify(ioaid)
IF (LHOOK) CALL DR_HOOK('CREATE_IOMAP',1,ZHOOK_HANDLE)

END SUBROUTINE create_iomap
