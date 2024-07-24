INTERFACE
SUBROUTINE msgpass_storedata(khandle, kret)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
implicit none
INTEGER(KIND=JPIM), intent(in)  :: khandle ! database handle
INTEGER(KIND=JPIM), intent(out) :: kret    ! return code
END SUBROUTINE msgpass_storedata
END INTERFACE
