FUNCTION get_num_threads() RESULT(inumt)
USE PARKIND1  ,ONLY : JPIM
USE oml_mod, only : OML_NUM_THREADS
implicit none
INTEGER(KIND=JPIM) :: inumt
inumt = 1
!$ inumt = OML_NUM_THREADS()
END FUNCTION get_num_threads
