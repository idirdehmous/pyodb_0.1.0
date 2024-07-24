INTERFACE
SUBROUTINE fwrite_iomap(khandle, kret)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
implicit none
INTEGER(KIND=JPIM), intent(in)    :: khandle
INTEGER(KIND=JPIM), intent(out)   :: kret
END SUBROUTINE fwrite_iomap
END INTERFACE
