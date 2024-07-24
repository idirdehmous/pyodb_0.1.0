FUNCTION DATASTREAM(BUFRTYPE,BUFRSUBTYPE,&
 &   GEN_CENTRE,GEN_SUBCENTRE,SATID)

USE PARKIND1  ,ONLY : JPRB

IMPLICIT NONE

REAL(KIND=JPRB)   :: DATASTREAM

REAL(KIND=JPRB), INTENT(IN)   :: BUFRTYPE
REAL(KIND=JPRB), INTENT(IN)   :: BUFRSUBTYPE
REAL(KIND=JPRB), INTENT(IN)   :: GEN_CENTRE
REAL(KIND=JPRB), INTENT(IN)   :: GEN_SUBCENTRE
REAL(KIND=JPRB), INTENT(IN)   :: SATID


!  11/11/2008   I. Genkova     Modify db MODIS datastream
!======================================


DATASTREAM = 0._JPRB          ! Global data is default

IF(          BUFRTYPE    == 3._JPRB            & ! SATEM
 &     .AND. BUFRSUBTYPE == 55._JPRB ) THEN      ! ATOVS

  ! Special streams for ATOVS data
  !-------------------------------

  IF(          GEN_CENTRE    == 254._JPRB      &   ! EUMETSAT
   &     .AND. GEN_SUBCENTRE > 0._JPRB ) THEN      ! Some sub-centre
    DATASTREAM = 1._JPRB                            ! EARS

  ELSEIF(     GEN_CENTRE <= 69._JPRB .OR. GEN_CENTRE == 110 ) THEN      
    DATASTREAM = 2._JPRB                            ! Pacific RARS

  ENDIF

ELSEIF(       BUFRTYPE    == 5._JPRB            & ! SATOB/AMV
 &      .AND. BUFRSUBTYPE == 87._JPRB ) THEN      ! Ext. AMV with QI

  ! Special streams for AMVs 
  !-------------------------



  IF( ( GEN_CENTRE == 173._JPRB .OR. GEN_CENTRE == 176._JPRB  & ! CIMSS
   &    .OR.( GEN_CENTRE == 160._JPRB              & ! NESDIS
   &          .AND. GEN_SUBCENTRE > 0._JPRB )  )   &
   &  .AND. ( SATID == 783 .OR. SATID == 784) ) THEN
    DATASTREAM = 1._JPRB                             ! Direct broadcast MODIS
  ENDIF

ELSEIF(       BUFRTYPE    == 12._JPRB            & ! SCATT
 &      .AND. BUFRSUBTYPE == 139._JPRB           & ! ASCAT
 &      .AND. GEN_SUBCENTRE > 0._JPRB  ) THEN      ! Non-EUMETSAT 
    DATASTREAM = 1._JPRB                           ! EARS
END IF


END FUNCTION DATASTREAM
