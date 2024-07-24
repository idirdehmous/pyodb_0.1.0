INTERFACE
SUBROUTINE rlnkdb(parent, kpa, pchild, kch, klink, kret)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
implicit none
INTEGER(KIND=JPIM), intent(in)  :: kpa, kch
REAL(KIND=JPRB),    intent(in)  :: parent(:), pchild(:)
INTEGER(KIND=JPIM), intent(out) :: klink(:), kret
END SUBROUTINE rlnkdb
END INTERFACE
