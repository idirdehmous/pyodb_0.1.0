SUBROUTINE ctxprint(print_it, msg, dbname)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK

!-- A dummy context print for IFS-independent libodbcore
implicit none
INTEGER(KIND=JPIM)       , intent(in) :: print_it
character(len=*), intent(in) :: msg, dbname
END SUBROUTINE ctxprint
