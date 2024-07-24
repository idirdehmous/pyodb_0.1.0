SUBROUTINE fODB_checkviewreg(khandle, cdview, kret, using)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
USE odb_module
implicit none
INTEGER(KIND=JPIM), intent(in) :: khandle
character(len=*), intent(in) :: cdview
INTEGER(KIND=JPIM), intent(out) :: kret
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: using
INTEGER(KIND=JPIM) j
logical is_table, is_star
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('FODB_CHECKVIEWREG',0,ZHOOK_HANDLE)

is_table = .FALSE.
if (len(cdview) >= 1) is_table = (cdview(1:1) == '@')

is_star = .FALSE.
if (.not.is_table) then
  if (len(cdview) >= 1) is_star = (cdview(1:1) == '*')
endif

if (is_table .or. is_star) then
  kret = -1
  do j=1,len_trim(cdview)
    kret = kret - ichar(cdview(j:j)) ! a stupid negative return code
  enddo
else ! call ODB_addview() if cdview has not been registered yet
  kret = ODB_gethandle(khandle, cdview, addview=.TRUE., using=using)
endif

IF (LHOOK) CALL DR_HOOK('FODB_CHECKVIEWREG',1,ZHOOK_HANDLE)
END SUBROUTINE fODB_checkviewreg
