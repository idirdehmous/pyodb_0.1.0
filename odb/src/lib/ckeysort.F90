SUBROUTINE ckeysort(pa, kra, krows, kcols, &
                   & keys, k_nkeys, &
                   & kidx, k_nidx, k_init_idx, &
                   & kret)

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
USE ECSORT_MIX    ,ONLY : KEYSORT

implicit none

INTEGER(KIND=JPIM), intent(in)    :: kra, krows, kcols, k_nkeys, k_nidx, k_init_idx
REAL(KIND=JPRB), intent(inout)    :: pa(kra, kcols)
INTEGER(KIND=JPIM), intent(in)    :: keys(k_nkeys)
INTEGER(KIND=JPIM), intent(inout) :: kidx(k_nidx)
INTEGER(KIND=JPIM), intent(out)   :: kret

logical :: LLinit, LLdescending
REAL(KIND=JPRB) :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('CKEYSORT',0,ZHOOK_HANDLE)

kret = krows

if (krows > 1) then
   if (k_nidx < krows) then
      if (k_nkeys > 0) then
         if (k_nkeys == 1 .and. kcols == 1) then
            LLdescending = (keys(1) < 0)
            CALL keysort(kret, pa(1:kra,1), krows, descending=LLdescending)
         else
            CALL keysort(kret, pa, krows, multikey=keys)
         endif
      else
         CALL keysort(kret, pa, krows)
      endif
   else ! use index
      LLinit = (k_init_idx > 0)
      if (k_nkeys > 0) then
         CALL keysort(kret, pa, krows, multikey=keys, index=kidx, init=LLinit)
      else
         CALL keysort(kret, pa, krows, index=kidx, init=LLinit)
      endif
   endif
endif

IF (LHOOK) CALL DR_HOOK('CKEYSORT',1,ZHOOK_HANDLE)

END SUBROUTINE ckeysort
