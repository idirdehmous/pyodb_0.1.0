SUBROUTINE create_iomap(khandle, kfirst_call, kmaxpools, &
 & kpoff, kfblk, cdfile, kgrpsize, &
 & ktblno, cdtbl, &
 & kpoolno, kfileno, koffset, klength, krows, kcols, &
 & kret) 
USE PARKIND1  ,ONLY : JPIM     ,JPRB
implicit none
INTEGER(KIND=JPIM), intent(in)  :: khandle, kfblk, kpoff, kgrpsize, krows, kcols
INTEGER(KIND=JPIM), intent(in)  :: kfirst_call, kmaxpools
INTEGER(KIND=JPIM), intent(in)  :: ktblno, kpoolno, kfileno, koffset, klength
INTEGER(KIND=JPIM), intent(out) :: kret
END SUBROUTINE create_iomap
