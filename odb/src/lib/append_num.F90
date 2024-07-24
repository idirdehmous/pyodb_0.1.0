SUBROUTINE append_num(cdout, cdin, knum, cdelim, kdoutlen)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
implicit none
character(len=*), intent(out) :: cdout
character(len=*), intent(in) :: cdin
character(len=*), intent(in), OPTIONAL :: cdelim
INTEGER(KIND=JPIM), intent(in) :: knum
INTEGER(KIND=JPIM), intent(out), OPTIONAL :: kdoutlen
character(len=10) :: ch
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('APPEND_NUM',0,ZHOOK_HANDLE)
write(ch,'(i10)') abs(knum)
if (knum >= 0) then
  if (present(cdelim)) then
    cdout = trim(cdin)//trim(cdelim)//trim(adjustl(ch))
  else
    cdout = trim(cdin)//trim(adjustl(ch))
  endif
else ! turn '-' (sign/dash) into '_' (underscore) when no cdelim given
  if (present(cdelim)) then
    cdout = trim(cdin)//trim(cdelim)//'-'//trim(adjustl(ch))
  else
    cdout = trim(cdin)//'_'//trim(adjustl(ch))
  endif
endif
if (present(kdoutlen)) kdoutlen = len_trim(cdout)
IF (LHOOK) CALL DR_HOOK('APPEND_NUM',1,ZHOOK_HANDLE)
END SUBROUTINE append_num
