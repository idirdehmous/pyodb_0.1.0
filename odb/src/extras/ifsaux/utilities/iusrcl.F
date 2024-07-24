      INTEGER FUNCTION IUSRCL()
      INTEGER TIMENOWHIGH
      DOUBLE PRECISION AMICCLK
C
C   MACHINE-DEPENDENT TIMING ROUTINE
C
C----------------------------------------------------------------------
C Author.
C -------
C    Ute Gaertel, Wolfgang Joppich, Anton Schueller   *GMD*
C
C Modifications.
C -------------
C    Original : 93-01-15
C    Modified : 93-03-15
C    Modified : 93-08-25
C----------------------------------------------------------------------
C
C------CM 5
C
CCM5  integer cmmd_node_timer_clear,cmmd_node_timer_start,
CCM5 *        cmmd_node_timer_stop,i,istat
CCM5  double precision time,cmmd_node_timer_elapsed
CCM5  data istat /0/
CCM5  save istat
CCM5
CCM5  if (istat.eq.0) then
CCM5    istat = 1
CCM5    i = cmmd_node_timer_clear(0)
CCM5    i = cmmd_node_timer_start(0)
CCM5  endif
CCM5    i = cmmd_node_timer_stop(0)
CCM5    time = cmmd_node_timer_elapsed(0)
CCM5    iusrcl = nint(time*1.0d3)
CCM5    i = cmmd_node_timer_start(0)
      double precision util_walltime
      double precision util_cputime
      IUSRCL = 0
C
C------iPSC/2,  iPSC/860
C
C     IUSRCL = MCLOCK()
C
C------Parsytec GCel
C
C     IUSRCL = TIMENOWHIGH()
C
C------IBM RS 6000, Meiko, SGI
C
CCSB  IUSRCL = 1000 * SECOND()
ccli  IUSRCL = 10 * MCLOCK()
c     IUSRCL =      MCLOCK()
#if defined(SGI) || defined(VPP)
      IUSRCL = 1000 * util_walltime()
c      IUSRCL = 1000 * util_cputime()
#endif
C
C
C------SUN 
C
C3LI  IUSRCL = 1000 
C
C
C------nCUBE/2
C
C     IUSRCL = INT (AMICCLK()/1000)
#ifdef CRAY
#ifndef T3D
C
C------Cray C90
C
      IUSRCL = INT( RTC()*4.186D-6 )
#else
C------Cray T3D
C
      IUSRCL = INT( RTC()*6.667D-6 )
c--      IUSRCL = 1000 * util_walltime()
#endif
#endif
C


      END
