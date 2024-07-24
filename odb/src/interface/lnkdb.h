SUBROUTINE lnkdb(kparent, kpa, kchild, kch, klink, kret)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
implicit none
INTEGER(KIND=JPIM), intent(in)  :: kpa, kch
INTEGER(KIND=JPIM), intent(in)  :: kparent(kpa), kchild(kch)
INTEGER(KIND=JPIM), intent(out) :: klink(kpa+1), kret
END SUBROUTINE lnkdb
