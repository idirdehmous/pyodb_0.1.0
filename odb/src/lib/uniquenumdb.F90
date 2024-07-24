#ifdef RS6K
@PROCESS NOEXTCHK
#endif
SUBROUTINE uniquenumdb(p, kldim, kvals, klen, keff)

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK

implicit none
INTEGER(KIND=JPIM), intent(in)  :: klen, kldim, kvals
REAL(KIND=JPRB),    intent(in)  :: p(kldim, klen)
INTEGER(KIND=JPIM), intent(out) :: keff
INTEGER(KIND=JPIM), parameter :: UI_RATIO = 2 ! sizeof(real(8)) / sizeof(integer(4))
INTEGER(KIND=JPIM) :: hash(klen)
INTEGER(KIND=JPIM) :: i, itag, is_unique, ihash
REAL(KIND=JPRB) :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('UNIQUENUMDB',0,ZHOOK_HANDLE)
keff = 0
if (klen <= 1) then
  keff = klen
  IF (LHOOK) CALL DR_HOOK('UNIQUENUMDB',1,ZHOOK_HANDLE)
  return
endif

CALL cODB_hash_set_lock()
CALL cODB_hash_init()

CALL cODB_vechash(&
 & kvals * UI_RATIO, &
 & kldim * UI_RATIO,&
 & klen, &
 & p(1,1), hash(1)) 

keff = 0
do i=1,klen
  itag = i
  CALL cODB_d_unique( &
   & kvals, p(1,i), hash(i), &
   & is_unique, itag, ihash) 
  if (is_unique == 1) keff = keff + 1
enddo

CALL cODB_hash_init()
CALL cODB_hash_unset_lock()

IF (LHOOK) CALL DR_HOOK('UNIQUENUMDB',1,ZHOOK_HANDLE)

END SUBROUTINE uniquenumdb
