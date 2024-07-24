SUBROUTINE iolockdb(konoff)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
USE odbshared, only : haveiolock
USE odbmp    , only : ODBMP_locking
implicit none
INTEGER(KIND=JPIM), intent(in) :: konoff
INTEGER(KIND=JPIM) :: iret
if (haveiolock > 0 .or. konoff == -1)  &
 & iret = ODBMP_locking(haveiolock, konoff) 
END SUBROUTINE iolockdb
