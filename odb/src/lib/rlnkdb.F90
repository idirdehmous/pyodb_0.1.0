SUBROUTINE rlnkdb(parent, kpa, pchild, kch, klink, kret)

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK

implicit none

INTEGER(KIND=JPIM), intent(in)  :: kpa, kch
REAL(KIND=JPRB),    intent(in)  :: parent(:), pchild(:)
INTEGER(KIND=JPIM), intent(out) :: klink(:), kret


!-- Correct code (even if some parents have no childs)
INTEGER(KIND=JPIM) :: actlen(kpa) ! automatic array
INTEGER(KIND=JPIM) :: jpa, icount, target
INTEGER(KIND=JPIM) :: jchstart, jchend, jch
REAL(KIND=JPRB) :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('RLNKDB',0,ZHOOK_HANDLE)
kret = 0
if (kpa > 0) then
  jchend = 0
  do jpa=1,kpa
    target = parent(jpa)
    jchstart = jchend + 1
    icount = 0
    do jch=jchstart,kch
      if (pchild(jch) /= target) exit
      icount = icount + 1
    enddo
    actlen(jpa) = icount
    jchend = jchend + icount
  enddo
  klink(1) = 1
  do jpa=2,kpa+1
    klink(jpa) = klink(jpa-1) + actlen(jpa-1)
  enddo
  kret = kpa+1
endif
IF (LHOOK) CALL DR_HOOK('RLNKDB',1,ZHOOK_HANDLE)

END SUBROUTINE rlnkdb
