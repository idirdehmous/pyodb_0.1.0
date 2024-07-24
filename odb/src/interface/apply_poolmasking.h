INTERFACE
SUBROUTINE apply_poolmasking(khandle, kversion, &
                             cdlabel, kvlabel, &
                             ktslot, kobstype, kcodetype, ksensor, &
                             ksubtype, kbufrtype, ksatinst)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
implicit none
INTEGER(KIND=JPIM), intent(in) :: khandle, kversion, kvlabel
character(len=*), intent(in) :: cdlabel
INTEGER(KIND=JPIM), intent(in) :: ktslot, kobstype, kcodetype, ksensor
INTEGER(KIND=JPIM), intent(in) :: ksubtype, kbufrtype, ksatinst
END SUBROUTINE apply_poolmasking
END INTERFACE
