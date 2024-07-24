MODULE LOCAL_TRAFOS
!**** LOCAL_TRAFOS transform localized quantities to other quantities

!     Purpose.
!     --------
!     Transform wind speed, direction to components and vice versa
!     Other functions also: value extracted from an array with an index
!        defined from a second array

!**   Interface.
!     ----------
!        USE LOCAL_TRAFOS

!     Author.
!     -------
!        H. Hersbach     ECMWF

!     Modifications.
!     --------------
!        Original: 2007-04-11

!        2009-09-04 C. Payan IVALFROMIDX added
!            allows to match an integer in an array from the index of an 
!            other integer's array

!     ------------------------------------------------------------------


USE PARKIND1  ,ONLY : JPIM, JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK

IMPLICIT NONE

PRIVATE
PUBLIC UCOM, VCOM, UV2FF, UV2DD, IVALFROMIDX

REAL(KIND=JPRB) ,   PARAMETER :: PARC=360._JPRB
REAL(KIND=JPRB) ,   PARAMETER :: DEGCON=3.14159265358979_JPRB/180._JPRB


CONTAINS

!***********************************************************************
FUNCTION UCOM(PDD,PFF)
REAL(KIND=JPRB)                  :: UCOM 
REAL(KIND=JPRB)   ,INTENT(IN)    :: PDD,PFF 
REAL(KIND=JPRB)                  :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:UCOM',0,ZHOOK_HANDLE)
UCOM = -PFF * SIN(PDD*DEGCON)
IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:UCOM',1,ZHOOK_HANDLE)
END FUNCTION UCOM


!***********************************************************************
FUNCTION VCOM(PDD,PFF)
REAL(KIND=JPRB)                  :: VCOM 
REAL(KIND=JPRB)   ,INTENT(IN)    :: PDD,PFF 
REAL(KIND=JPRB)                  :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:VCOM',0,ZHOOK_HANDLE)
VCOM = -PFF * COS(PDD*DEGCON)
IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:VCOM',1,ZHOOK_HANDLE)
END FUNCTION VCOM


!***********************************************************************
FUNCTION UV2FF(PUU,PVV)
REAL(KIND=JPRB)                  :: UV2FF 
REAL(KIND=JPRB)   ,INTENT(IN)    :: PUU,PVV
REAL(KIND=JPRB)                  :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:UV2FF',0,ZHOOK_HANDLE)
UV2FF =SQRT(PUU*PUU+PVV*PVV)
IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:UV2FF',1,ZHOOK_HANDLE)
END FUNCTION UV2FF


!***********************************************************************
FUNCTION UV2DD(PUU,PVV)
REAL(KIND=JPRB)                  :: UV2DD
REAL(KIND=JPRB)   ,INTENT(IN)    :: PUU,PVV
REAL(KIND=JPRB)                  :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:UV2DD',0,ZHOOK_HANDLE)

IF (PUU == 0._JPRB .AND. PVV == 0._JPRB) THEN
   UV2DD = 0.5_JPRB*PARC
ELSE
   UV2DD =MOD(PARC+ATAN2(-PUU,-PVV)/DEGCON,PARC)
ENDIF
      
IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:UV2DD',1,ZHOOK_HANDLE)
END FUNCTION UV2DD

!***********************************************************************
FUNCTION IVALFROMIDX(KTABVAL,KTABIDX,KCOND,KDEFAULT)
INTEGER(KIND=JPIM)               :: IVALFROMIDX
INTEGER(KIND=JPIM) ,DIMENSION(:) ,INTENT(IN)  :: KTABVAL ,KTABIDX
INTEGER(KIND=JPIM) ,INTENT(IN)   :: KCOND ,KDEFAULT
INTEGER(KIND=JPIM)               :: JJ ,IMXIDX
REAL(KIND=JPRB)                  :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:IVALFROMIDX',0,ZHOOK_HANDLE)

IVALFROMIDX=KDEFAULT
IMXIDX=MIN(SIZE(KTABIDX),SIZE(KTABVAL))

DO JJ=1,IMXIDX
  IF (KTABIDX(JJ)==KCOND) THEN
    IVALFROMIDX=KTABVAL(JJ)
    EXIT
  ENDIF
ENDDO

IF (LHOOK) CALL DR_HOOK('LOCAL_TRAFOS:IVALFROMIDX',1,ZHOOK_HANDLE)
END FUNCTION IVALFROMIDX

!***********************************************************************
END MODULE LOCAL_TRAFOS
