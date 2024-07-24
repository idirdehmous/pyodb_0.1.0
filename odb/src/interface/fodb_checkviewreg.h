INTERFACE
SUBROUTINE fODB_checkviewreg(khandle, cdview, kret, using)
USE PARKIND1  ,ONLY : JPIM
implicit none
INTEGER(KIND=JPIM), intent(in) :: khandle
character(len=*), intent(in) :: cdview
INTEGER(KIND=JPIM), intent(out) :: kret
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: using
END SUBROUTINE fODB_checkviewreg
END INTERFACE
