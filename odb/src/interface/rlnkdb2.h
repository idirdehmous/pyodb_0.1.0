INTERFACE
SUBROUTINE rlnkdb2(koffset, &
                  parr_in, klda, krows, kcol1, kcol2, &
                  ktarget, karr_out)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
implicit none
INTEGER(KIND=JPIM), intent(in)  :: koffset
INTEGER(KIND=JPIM), intent(in)  :: klda, krows, kcol1, kcol2, ktarget
REAL(KIND=JPRB)   , intent(in)  :: parr_in(klda, kcol1:kcol2)
INTEGER(KIND=JPIM), intent(out) :: karr_out(krows + 1)
END SUBROUTINE rlnkdb2
END INTERFACE
