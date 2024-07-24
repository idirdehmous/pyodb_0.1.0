INTERFACE
SUBROUTINE append_num(cdout, cdin, knum, cdelim, kdoutlen)
USE PARKIND1  ,ONLY : JPIM
implicit none
character(len=*), intent(out) :: cdout
character(len=*), intent(in) :: cdin
character(len=*), intent(in), OPTIONAL :: cdelim
INTEGER(KIND=JPIM), intent(in) :: knum
INTEGER(KIND=JPIM), intent(out), OPTIONAL :: kdoutlen
END SUBROUTINE append_num
END INTERFACE
