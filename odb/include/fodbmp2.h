#if INT_VERSION == 4

#define GEN_TYPE          INTEGER(KIND=JPIM)
#define GEN_MSGTYPE       1
#define GEN_SIZEOF_DATA   4
#define GEN_XCHANGE2       ODBMP_iexchange2
#define GEN_XCHANGE2_NAME 'ODBMP:ODBMP_IEXCHANGE2'

#elif REAL_VERSION == 8

#define GEN_TYPE          REAL(KIND=JPRB)
#define GEN_MSGTYPE       8
#define GEN_SIZEOF_DATA   8
#define GEN_XCHANGE2       ODBMP_dexchange2
#define GEN_XCHANGE2_NAME 'ODBMP:ODBMP_DEXCHANGE2'

#else

  ERROR in programming : No datatype given (should never have ended up here)

#endif

#if defined(INT_VERSION) || defined(REAL_VERSION)

SUBROUTINE GEN_XCHANGE2 &
    &(to, nlastrow, &
    &from, nrows, ncols, &
    &takethis)
USE MPL_MODULE
USE OML_MOD, ONLY : OML_SET_LOCK, OML_UNSET_LOCK
implicit none

INTEGER(KIND=JPIM), intent(in)  :: nrows(:), ncols
INTEGER(KIND=JPIM), intent(out) :: nlastrow
GEN_TYPE, intent(out)  :: to(:,0:)
GEN_TYPE, intent(in)   :: from(:,0:)
logical , intent(in)   :: takethis(0:)

INTEGER(KIND=JPIM) :: ldimto, ldimfrom
INTEGER(KIND=JPIM) :: i, ii, j, jcol, jroc
INTEGER(KIND=JPIM) :: icols, icolumn(ncols+1)
INTEGER(KIND=JPIM) :: istat_from, ilen_from
INTEGER(KIND=JPIM) :: istat_to  , ilen_to
INTEGER(KIND=JPIM) :: nrows_tmp(size(nrows))
GEN_TYPE, allocatable :: tmp_from(:), tmp_to(:)
character(len=256) CLtmp
character(len=20) CLenv
logical, save :: first_time = .TRUE.
INTEGER(KIND=JPIM), save :: imax = 0
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK(GEN_XCHANGE2_NAME,0,ZHOOK_HANDLE)

ldimto   = size(to, dim=1)
ldimfrom = size(from, dim=1)

nlastrow = sum(nrows(:))

!-- nobody sends/receives anything ==> suppress message passing
if (nlastrow <= 0) then
  IF (LHOOK) CALL DR_HOOK(GEN_XCHANGE2_NAME,1,ZHOOK_HANDLE)
  return 
endif

if (nlastrow > ldimto) then
  write(0,*)'***Error: Recv-array is too small in '// &
             GEN_XCHANGE2_NAME
  CALL ODBMP_abort(GEN_XCHANGE2_NAME,'Recv-array is too small',ldimto-nlastrow)
endif

if (first_time) then
!$ CALL OML_SET_LOCK()
 if (first_time) then
  CALL cODB_getenv('ODB_MPEXCHANGE2_PACKMAX', CLenv) ! in bytes
  if (CLenv == ' ') then
!-- the default: a rudimentary control to prevent excessive memory usage on recv-side
!!    imax = MPL_MBX_SIZE/GEN_SIZEOF_DATA ! now in respective words
    imax = 0
  else
    read(CLenv,*) imax
    imax = imax/GEN_SIZEOF_DATA ! now in respective words
  endif
  first_time =.FALSE.
 endif
!$ CALL OML_UNSET_LOCK()
endif

ilen_from = 0
ilen_to = 0
icols = 0
icolumn(:) = -1
do j=0,ncols
  if (j > 0 .and. .not. takethis(j)) cycle ! Always take col#0, however!
  icols = icols + 1
  icolumn(icols) = j
enddo

ilen_from = icols * nrows(ODBMP_myproc)
ilen_to   = icols * nlastrow

!-- to be done : blocking i.e. 'nproma'-like approach in splitting

if (icols > 1 .and. ilen_to < imax) then
#ifndef VPP
  write(CLtmp,'(a,i12,a,i12)') GEN_XCHANGE2_NAME &
     //' via single allgatherv : recv-size ',ilen_to,' < limit ',imax
#endif
allocate(tmp_from(ilen_from), stat=istat_from)
  if (istat_from /= 0) then
    write(0,*)'***Error: Unable to allocate memory (',ilen_from,' words) for "tmp_from" in '// &
               GEN_XCHANGE2_NAME
    CALL ODBMP_abort(GEN_XCHANGE2_NAME,'Unable to allocate memory for "tmp_from"',istat_from)
  endif

  allocate(tmp_to(ilen_to), stat=istat_to)
  if (istat_to /= 0) then
    write(0,*)'***Error: Unable to allocate memory (',ilen_to,' words) for "tmp_to" in '// &
               GEN_XCHANGE2_NAME
    CALL ODBMP_abort(GEN_XCHANGE2_NAME,'Unable to allocate memory for "tmp_to"',istat_to)
  endif

  i = 0
  do jcol=1,icols
    j = icolumn(jcol)
    tmp_from(i+1:i+nrows(ODBMP_myproc)) = from(1:nrows(ODBMP_myproc),j)
    i = i + nrows(ODBMP_myproc)
  enddo
  nrows_tmp(:) = icols * nrows(:)

  CALL MPL_ALLGATHERV(tmp_from, tmp_to, nrows_tmp, &
                     & cdstring= GEN_XCHANGE2_NAME )

  deallocate(tmp_from)

  i = 0
  ii = 0
  do jroc=1,ODBMP_nproc
    do jcol=1,icols
      j = icolumn(jcol)
      to(i+1:i+nrows(jroc),j) = tmp_to(ii+1:ii+nrows(jroc))
      ii = ii + nrows(jroc)
    enddo
    i = i + nrows(jroc)
  enddo

  deallocate(tmp_to)
else
#ifndef VPP
  if (icols > 1) then
    write(CLtmp,'(a,i5,a,i12,a,i12)') GEN_XCHANGE2_NAME &
       //' via multiple (',icols,') allgathervs : recv-size ',ilen_to,' >= limit ',imax
  endif
#endif
  do j=0,ncols
    if (j > 0 .and. .not. takethis(j)) cycle ! Always take col#0, however!
CALL MPL_ALLGATHERV(from(1:nrows(ODBMP_myproc),j), &
                       & to(1:nlastrow,j), nrows, &
                       & cdstring= GEN_XCHANGE2_NAME )
  enddo
endif

IF (LHOOK) CALL DR_HOOK(GEN_XCHANGE2_NAME,1,ZHOOK_HANDLE)
END SUBROUTINE

#ifndef NO_UNDEF
#undef GEN_TYPE
#undef GEN_MSGTYPE
#undef GEN_SIZEOF_DATA
#undef GEN_XCHANGE2
#undef GEN_XCHANGE2_NAME
#endif

#endif
