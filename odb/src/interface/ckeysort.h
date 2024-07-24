SUBROUTINE ckeysort(pa, kra, krows, kcols, &
                   & keys, k_nkeys, &
                   & kidx, k_nidx, k_init_idx, &
                   & kret)

USE PARKIND1  ,ONLY : JPIM     ,JPRB

implicit none

INTEGER(KIND=JPIM), intent(in)    :: kra, krows, kcols, k_nkeys, k_nidx, k_init_idx
REAL(KIND=JPRB), intent(inout)    :: pa(kra, kcols)
INTEGER(KIND=JPIM), intent(in)    :: keys(k_nkeys)
INTEGER(KIND=JPIM), intent(inout) :: kidx(k_nidx)
INTEGER(KIND=JPIM), intent(out)   :: kret

END SUBROUTINE ckeysort
