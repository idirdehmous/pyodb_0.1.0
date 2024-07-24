SUBROUTINE GAUAW_ODB(PA,PW,K,KRET)

!**** *GAUAW_ODB* - COMPUTE ABSCISSAS AND WEIGHTS FOR *GAUSSIAN INTEGRATION.

!     PURPOSE.
!     --------

!          *GAUAW_ODB* IS CALLED TO COMPUTE THE ABSCISSAS AND WEIGHTS REQUIR
!     TO PERFORM *GAUSSIAN INTEGRATION.

!**   INTERFACE.
!     ----------

!          *CALL* *GAUAW_ODB(PA,PW,K)*

!               *PA*     - ARRAY, LENGTH AT LEAST *K,* TO RECEIVE ABSCISSAS.
!               *PW*     - ARRAY, LENGTH AT LEAST *K,* TO RECEIVE WEIGHTS.

!     METHOD.
!     -------

!     -------

!          THE ZEROS OF THE *BESSEL FUNCTIONS ARE USED AS STARTING
!     APPROXIMATIONS FOR THE ABSCISSAS. NEWTON ITERATION IS USED TO
!     IMPROVE THE VALUES TO WITHIN A TOLLERENCE OF *EPS.*

!     EXTERNAL.
!     ---------

!          *BSSLZR_ODB* - ROUTINE TO OBTAIN ZEROS OF *BESSEL FUNCTIONS.

USE PARKIND1  ,ONLY : JPIM     ,JPRB

IMPLICIT NONE

#include "bsslzr_odb.h"

INTEGER(KIND=JPIM), intent(in)  :: K
REAL(KIND=JPRB), intent(out)    :: PA(K),PW(K)
INTEGER(KIND=JPIM), intent(out) :: KRET
! === END OF INTERFACE BLOCK ===

REAL(KIND=JPRB) :: ZEPS = 1E-14_JPRB
REAL(KIND=JPRB) :: ZPI, ZC, ZXZ, ZKM2, ZKM1, ZFN, ZPK, ZKMRK, ZSP, ZVSP
INTEGER(KIND=JPIM) :: IFK, IKK, JS, ITER, JN, IL

!     ------------------------------------------------------------------

!*         1.     SET CONSTANTS AND FIND ZEROS OF BESSEL FUNCTION.
!                 --- --------- --- ---- ----- -- ------ ---------

KRET = 0

ZPI=2.0_JPRB*ASIN(1.0_JPRB)
ZC=(1.0_JPRB-(2.0_JPRB/ZPI)**2)*0.25_JPRB
IFK=K
IKK=K/2

CALL BSSLZR_ODB(PA,IKK)

DO JS=1,IKK
  ZXZ=COS(PA(JS)/SQRT((IFK+0.5_JPRB)**2+ZC))
!*                 GIVING THE FIRST APPROXIMATION FOR *ZXZ.*
  ITER=0

!     ------------------------------------------------------------------

!*         2.     COMPUTE ABSCISSAS AND WEIGHTS.
!                 ------- --------- --- -------

!*         2.1     SET VALUES FOR NEXT ITERATION.

  210    CONTINUE
  ZKM2=1.0_JPRB
  ZKM1=ZXZ
  ITER=ITER+1
  IF(ITER > 10) GO TO 300

!*         2.2     COMPUTATION OF THE *LEGENDRE POLYNOMIAL.

  DO JN=2,K
    ZFN=JN
    ZPK=((2.0_JPRB*ZFN-1.0_JPRB)*ZXZ*ZKM1-(ZFN-1.0_JPRB)*ZKM2)/ZFN
    ZKM2=ZKM1
    ZKM1=ZPK
  ENDDO

  ZKM1=ZKM2
  ZKMRK=(IFK*(ZKM1-ZXZ*ZPK))/(1.0_JPRB-ZXZ**2)
  ZSP=ZPK/ZKMRK
  ZXZ=ZXZ-ZSP
  ZVSP=ABS(ZSP)
  IF(ZVSP > ZEPS) GO TO 210

!*         2.3     ABSCISSAS AND WEIGHTS.

  PA(JS)=ZXZ
  PW(JS)=(2.0_JPRB*(1.0_JPRB-ZXZ**2))/(IFK*ZKM1)**2

!*         2.4     ODD *K* COMPUTATION OF WEIGHT AT THE EQUATOR.

  IF (K /= IKK*2) THEN
    PA(IKK+1)=0.
    ZPK=2.0_JPRB/IFK**2

    DO JN=2,K,2
      ZFN=JN
      ZPK=ZPK*ZFN**2/(ZFN-1.0_JPRB)**2
    ENDDO

    PW(IKK+1)=ZPK
  ELSE

!*         2.5     USE SYMMETRY TO OBTAIN REMAINING VALUES.

    DO JN=1,IKK
      IL=K+1-JN
      PA(IL)=-PA(JN)
      PW(IL)=PW(JN)
    ENDDO

  ENDIF
ENDDO

RETURN

!     ------------------------------------------------------------------

!*         3.     ERROR PROCESSING.
!                 ----- -----------

300 CONTINUE
!WRITE(6,9901)
! 9901 FORMAT(//,'  GAUAW_ODB FAILED TO CONVERGE AFTER 10 ITERATIONS.')
!STOP
KRET = 10

!     ------------------------------------------------------------------

END
