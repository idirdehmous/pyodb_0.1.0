module odbstat
USE PARKIND1  ,ONLY : JPIM,JPIB,JPRB
USE YOMHOOK   ,ONLY : LHOOK, DR_HOOK
USE odbutil, only : odb_getnames
USE odbshared, only : ODB_NMDI

implicit none

private

INTEGER(KIND=JPIM), parameter :: odb_statlen = 7
INTEGER(KIND=JPIM), parameter :: odb_statcharlen = 8

character(len=odb_statcharlen), parameter :: odb_statbuf(odb_statlen) = (/ &
     & '  Count ', &
     & 'Ignored ', &
     & '    Min ', &
     & '    Max ', &
     & 'Average ', &
     & ' St.Dev ', &
     & '    RMS '/)
     

public :: odb_statlen, odb_statcharlen
public :: odb_statbuf
public :: odb_collectstat
public :: odb_makestat

contains

subroutine odb_collectstat(handle, dtname, d, nrows, ncols, stat)
implicit none
INTEGER(KIND=JPIM), intent(in) :: handle, nrows, ncols
character(len=*), intent(in)   :: dtname
REAL(KIND=JPRB), intent(in)    :: d(:,:)
REAL(KIND=JPRB), intent(inout), optional :: stat(:,:)
INTEGER(KIND=JPIM) ndtypes, j
character(len=64) CLdtypes(ncols)
logical LLmask
INTEGER(KIND=JPIM) jc, nrows_out, ncols_out
REAL(KIND=JPRB) ZHOOK_HANDLE
if (.not.present(stat)) return
if (size(stat) == 0) return
IF (LHOOK) CALL DR_HOOK('ODBSTAT:ODB_COLLECTSTAT',0,ZHOOK_HANDLE)
nrows_out = min(nrows, size(d,dim=1))
ndtypes = ODB_getnames(handle, dtname, 'datatype', CLdtypes)
ncols_out = min(ndtypes, ncols, size(d,dim=2), size(stat,dim=2))
if (size(stat, dim=1) >= odb_statlen .and. nrows_out > 0 .and. ncols_out > 0) then
  do jc=1,ncols_out
    !-- check first if initialization is required
    if (stat(1,jc) == 0) then
      stat(:,jc) = 0
      stat(3,jc) =  ODB_NMDI ! Min
      stat(4,jc) = -ODB_NMDI ! Max
    endif

    stat(1,jc) = stat(1,jc) + nrows_out ! total count so far (valid + non-valid data)

    if (CLdtypes(jc) == 'string') then
      stat(2,jc) = 0 ! Non-valid data for statistics calculation throughout
    else
      do j=1,nrows_out
        LLmask = (abs(d(j,jc)) /= ODB_NMDI) ! Take only those which aren't missing data
        if (LLmask) then
          stat(2,jc) = stat(2,jc) + 1                ! No. of valid data
          stat(3,jc) = min(stat(3,jc), d(j,jc))      ! Min
          stat(4,jc) = max(stat(4,jc), d(j,jc))      ! Max
          stat(5,jc) = stat(5,jc) + d(j,jc)          ! Sum(d)
          stat(6,jc) = stat(6,jc) + d(j,jc)*d(j,jc)  ! Sum(d^2)
        endif
      enddo ! do j=1,nrows_out
    endif
  enddo ! do jc=1,ncols_out
endif
IF (LHOOK) CALL DR_HOOK('ODBSTAT:ODB_COLLECTSTAT',1,ZHOOK_HANDLE)
end subroutine odb_collectstat


subroutine odb_makestat(final_stat, nrows, ncols, stat)
implicit none
INTEGER(KIND=JPIM), intent(in) :: nrows, ncols
REAL(KIND=JPRB), intent(out)   :: final_stat(:,:)
REAL(KIND=JPRB), intent(in)    :: stat(:,:)
REAL(KIND=JPRB) rnn, avg
INTEGER(KIND=JPIM) jc, nrows_out, ncols_out
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODBSTAT:ODB_MAKESTAT',0,ZHOOK_HANDLE)
nrows_out = min(nrows, size(final_stat,dim=1), size(stat,dim=1))
ncols_out = min(ncols, size(final_stat,dim=2), size(stat,dim=2))
if (nrows_out >= odb_statlen .and. ncols_out > 0) then
  do jc=1,ncols_out
    rnn = stat(2,jc) ! No. of valid data
    final_stat(1,jc) = rnn ! No. of valid data
    final_stat(2,jc) = stat(1,jc) - rnn ! No. of missing/ignored/string data
    final_stat(3,jc) = stat(3,jc)
    final_stat(4,jc) = stat(4,jc)
    if (rnn > 0) then
      avg = stat(5,jc)/rnn ! Average
    else
      avg = 0
    endif
    final_stat(5,jc) = avg
    if (rnn > 1) then ! Standard deviation
      final_stat(6,jc) = (stat(6,jc) - rnn * (avg**2))/(rnn - 1)
      if (final_stat(6,jc) > 0) then ! some security due to round of errors
        final_stat(6,jc) = sqrt(final_stat(6,jc))
      else
        final_stat(6,jc) = 0
      endif
    else
      final_stat(6,jc) = 0
    endif
    if (rnn > 0) then ! RMS
      if (stat(6,jc) > 0) then
        final_stat(7,jc) = sqrt(stat(6,jc)/rnn)
      else
        final_stat(7,jc) = 0
      endif
    else
      final_stat(7,jc) = 0
    endif
  enddo
endif
IF (LHOOK) CALL DR_HOOK('ODBSTAT:ODB_MAKESTAT',1,ZHOOK_HANDLE)
end subroutine odb_makestat

end module odbstat
