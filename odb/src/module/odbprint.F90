#ifdef RS6K
@PROCESS NOOPTIMIZE NOEXTCHK
#endif

MODULE odbprint

USE PARKIND1  ,ONLY : JPIM,JPIB,JPRB
USE YOMHOOK   ,ONLY : LHOOK, DR_HOOK

USE odbshared
USE odbmp
USE odbutil
USE odb
USE odbgetput
USE odbstat

#ifdef NAG
use f90_unix_io, only: flush
#endif

implicit none

SAVE
PRIVATE

#define trimadjL(x) trim(sadjustl(x))
#define trimadjR(x) trim(sadjustr(x))

public :: ODB_print

CONTAINS

FUNCTION ODB_print(handle, dtname, &
     &file, poolno, maxpoolno, &
     &setvars, values,&
     &PEvar, replicate_PE,&
     &show_DB_index, inform_progress, &
     &open_file, close_file, append_mode, &
     &select_query, cancel_query, &
     &latitude, longitude, color, llc_idx, &
     &start, limit, got, print_title, print_timingstat, print_summary, stat) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)  :: dtname
character(len=*), intent(in), optional :: file
character(len=*), intent(in), optional :: setvars(:)
REAL(KIND=JPRB) , intent(in), optional :: values(:)
REAL(KIND=JPRB) , intent(inout), optional :: stat(:,:) ! size= (>=6 x ncols) to be meaningful
INTEGER(KIND=JPIM), intent(in), optional :: poolno, replicate_PE, maxpoolno
INTEGER(KIND=JPIM), intent(out), optional :: got
logical, intent(in), optional :: show_DB_index, inform_progress
logical, intent(in), optional :: open_file, close_file, append_mode
logical, intent(in), optional :: select_query, cancel_query
logical, intent(in), optional :: print_title, print_timingstat, print_summary
REAL(KIND=JPRB), POINTER, optional :: latitude(:), longitude(:), color(:)
INTEGER(KIND=JPIM), intent(in), optional :: llc_idx(:), start, limit
character(len=*), intent(in), optional :: PEvar

character(len=1), parameter :: cr = char(13)
INTEGER(KIND=JPIM) :: nrows, ncols, nalloc, ilow, istart, ilimit, nn, jc
INTEGER(KIND=JPIM), save :: ilen = 0
INTEGER(KIND=JPIM) :: iret, j, pe, ipoolno, inc, icol
INTEGER(KIND=JPIM) :: rc, pemin, pemax
INTEGER(KIND=JPIM) :: nglb_rows(ODBMP_nproc)
logical is_table, LLopen_file, LLclose_file, LLappend_mode, LLstdout
logical LLselect, LLcancel, LLprthdr, LLprttimingstat, LLprtsummary, LLprtunderline
logical LLinform_progress
REAL(KIND=JPRB), allocatable :: d(:,:)
INTEGER(KIND=JPIM), allocatable, save :: itype(:), inclen(:)
character(len=maxvarlen), allocatable, save :: names(:)
character(len=maxvarlen), allocatable :: types(:), ftntypes(:)
character(len=maxstrlen)     CL_file
INTEGER(KIND=JPIM) :: idummy, idummy_arr(0), nrows_global, imode, inilen, istatlen
INTEGER(KIND=JPIM), save :: nrows_total
character(len=80) :: clprtfmt, clprtfile
INTEGER(KIND=JPIM) iprtfmt_len, iprtfmt, iat, iprtfile_len
INTEGER(KIND=JPIM) :: icache, imaxpoolno
INTEGER(KIND=JPIM), parameter :: allowed_cache_size_in_bytes = 262144 ! bytes
REAL(KIND=JPRB) :: zwall(2)
REAL(KIND=JPRB), external :: util_walltime  ! from ifsaux/support/drhook.c
REAL(KIND=JPRB) :: RAD2DEG, x
REAL(KIND=JPRB) :: RPI, RADIANS, RDEGREE
RAD2DEG(x) = RDEGREE * x
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODBPRINT:ODB_PRINT',0,ZHOOK_HANDLE)

!-- Consistent with obsproc/control/sunumc.F90
RPI    =2.0_JPRB*ASIN(1.0_JPRB)
RADIANS=RPI/180._JPRB
RDEGREE=180._JPRB/RPI

is_table = .FALSE.
if (len(dtname) >= 1) then
  is_table = (dtname(1:1) == '@')
endif

CALL cODB_trace(handle, 1, 'ODB_print:'//dtname,idummy_arr, 0)

if (showdbidx == 0) then
  ilow = 1
else
  ilow = 0
endif

LLopen_file = .TRUE.
if (present(open_file)) LLopen_file = open_file

LLclose_file = .TRUE.
if (present(close_file)) LLclose_file = close_file

LLappend_mode = .FALSE.
if (present(append_mode)) LLappend_mode = append_mode

LLselect = .TRUE.
if (present(select_query)) LLselect = select_query

LLcancel = .TRUE.
if (present(cancel_query)) LLcancel = cancel_query

if (present(show_DB_index)) then
  if (show_DB_index) then
    ilow = 0
  else
    ilow = 1
  endif
endif

if (present(got)) got = 0

LLprthdr = .TRUE.
if (present(print_title)) LLprthdr = print_title

LLprttimingstat = .TRUE.
if (present(print_timingstat)) LLprttimingstat = print_timingstat

inilen = 0
if (present(stat)) then
  if (size(stat) > 0) inilen = odb_statcharlen
endif

LLprtunderline = .FALSE.
LLprtsummary = .FALSE.
if (present(print_summary)) LLprtsummary = print_summary
if (LLprtsummary .and. inilen == 0) LLprtsummary = .FALSE.
if (LLprtsummary) then
  LLprthdr = .FALSE.
  LLprttimingstat = .FALSE.
  LLselect = .FALSE.
  LLcancel = .FALSE.
  istatlen = odb_statlen
else
  istatlen = 0
endif

LLinform_progress=.FALSE.

rc = 0
nrows_global = 0

if (present(file)) then
  CALL cODB_envtransf(trim(file), CL_file, iret)
else
  CL_file = trim(dtname)//'.rpt'
endif

LLstdout = .FALSE.
CALL util_cgetenv('ODB_REPORTER', 'stdout', clprtfile, iprtfile_len)
if (clprtfile == 'stdout' .or. clprtfile == 'stderr') then
  LLstdout = .TRUE.
  CL_file = clprtfile
endif

if (odbHcheck(handle, 'ODB_print')) then
  ipoolno = get_poolno(handle, poolno)

  if (LLopen_file) then
    if (allocated(itype)) deallocate(itype)
    if (allocated(inclen)) deallocate(inclen)
    if (allocated(names)) deallocate(names)

    nrows_total = 0
    nrows_global = 0
    zwall(:) = 0

    if (ODBMP_myproc == 1.and.present(inform_progress)&
     &   .and.present(maxpoolno)) then
       if (inform_progress .and. .not.LLstdout) then
          imaxpoolno = maxpoolno
          if (imaxpoolno <= 0) imaxpoolno = db(handle)%glbNpools
          call codb_set_progress_bar("iounit", 0)
          call codb_set_progress_bar("maxpool", imaxpoolno)
          call codb_set_progress_bar("totalrows", 0)
          call codb_progress_bar(0, trim(dtname), &
          & 0, imaxpoolno, nrows_global, nrows_total, &
          & zwall(2) - zwall(1), 0)
          LLinform_progress = .TRUE.
       endif
    endif
  endif

  zwall(1) = util_walltime()
  if (LLselect) then
    iret = ODB_select(handle, dtname, nrows, ncols,&
         & poolno=ipoolno, nra=nalloc,&
         & setvars=setvars, values=values,&
         & PEvar=PEvar, replicate_PE=replicate_PE, inform_progress=LLinform_progress)
  else
    iret = ODB_getsize(handle, dtname, nrows, ncols,&
         & poolno=ipoolno, nra=nalloc)
  endif

  if (LLprtsummary) then
    if (size(stat,dim=2) >= ncols) then
      nrows = odb_statlen
    else
      nrows = 0
    endif
  endif

  rc = nrows

  if (LLprtsummary .and. nrows > 0) then
    nalloc = ODB_lda(nrows)
    allocate(d(nalloc, 0:ncols))
    d(:,0) = 0 ! dummy; not used; only here for consistency
    call ODB_makestat(d(:,1:),nrows,ncols,stat)
    LLprtunderline = .TRUE.
  else if (nrows > 0) then
    istart = 1
    if (present(start)) istart = max(1,start)
    ilimit = nrows
    if (present(limit)) ilimit = max(0,min(nrows,limit))
    nalloc = ODB_lda(ilimit)

    allocate(d(nalloc, 0:ncols))
    nrows = ilimit
    rc = ODB_get(handle, dtname, d, nrows, ncols=ncols, &
         &       poolno=ipoolno, start=istart, limit=ilimit, inform_progress=LLinform_progress)
    if (present(got)) got = nrows
    if (present(llc_idx)) then
      if (size(llc_idx) >= 2) then
        icol = abs(llc_idx(1))
        if (icol >= 1 .and. icol <= ncols) then
          if (llc_idx(1) == -icol) then ! convert radians to degrees
            do j=1,nrows
              d(j,icol) = rad2deg(d(j,icol))
            enddo
          endif
          if (present(latitude)) then
            if (associated(latitude)) deallocate(latitude)
            allocate(latitude(nrows))
            latitude(1:nrows) = d(1:nrows,icol)
          endif
        endif
        icol = abs(llc_idx(2))
        if (icol >= 1 .and. icol <= ncols) then
          if (llc_idx(2) == -icol) then ! convert radians to degrees
            do j=1,nrows
              d(j,icol) = rad2deg(d(j,icol))
            enddo
          endif
          if (present(longitude)) then
            if (associated(longitude)) deallocate(longitude)
            allocate(longitude(nrows))
            longitude(1:nrows) = d(1:nrows,icol)
          endif
        endif
      endif
      if (size(llc_idx) >= 3 .and. present(color)) then
        icol = llc_idx(3)
        if (icol >= 1 .and. icol <= ncols) then
          if (associated(color)) deallocate(color)
          allocate(color(nrows))
          color(1:nrows) = d(1:nrows,icol)
        endif
      endif
    endif ! if (present(llc_idx)) then
    call ODB_collectstat(handle,dtname,d(:,1:),nrows,ncols,stat)
  endif

  if (LLcancel) then
    iret = ODB_cancel(handle, dtname, poolno=ipoolno)
  endif

  nglb_rows(:) = 0
  nglb_rows(ODBMP_myproc) = nrows
  CALL ODBMP_global('SUM',nglb_rows)
  nrows_global = sum(nglb_rows)
  nrows_total = nrows_total + nrows_global
  rc = nrows_global
  zwall(2) = util_walltime()

  if (ODBMP_myproc == 1.and.present(inform_progress)&
     &.and.present(maxpoolno)) then
    if (inform_progress .and. .not.LLstdout) then
      imaxpoolno = maxpoolno
      if (imaxpoolno <= 0) imaxpoolno = db(handle)%glbNpools
      call codb_progress_bar(0, trim(dtname), &
         & ipoolno, imaxpoolno, nrows_global, nrows_total, &
         & zwall(2) - zwall(1), 0)
    endif
  endif

!         write(0,*)'ODB_print<select('//trim(dtname)//'): PE#',
!     $        ODBMP_myproc,':',nrows,':',ncols

  if (ODBMP_myproc == 1.and.LLopen_file) then
    imode = 0
    if (LLappend_mode) imode = 1
    CALL cODB_openprt(trim(CL_file), imode, iret)
  endif

  if (nrows_global <= 0) goto 100

  if (allocated(itype) .and. allocated(inclen) .and. allocated(names)) goto 110

  if (allocated(itype))  deallocate(itype)
  if (allocated(inclen)) deallocate(inclen)
  if (allocated(names))  deallocate(names)

  allocate(itype(ilow:ncols))
  allocate(inclen(ilow:ncols))
  allocate(names(ilow:ncols))

  allocate(types(ilow:ncols))
  allocate(ftntypes(ilow:ncols))

  CALL util_cgetenv('ODB_PRINT_FORMAT', 'STD', clprtfmt, iprtfmt_len)
  call toupper(clprtfmt)
  if (clprtfmt == 'XL' .or. clprtfmt == 'EXCEL') then
    iprtfmt = 1 ! Microsoft Excel
  else if (clprtfmt == 'SIMUL' .or. clprtfmt == 'SIMULOBS') then
    iprtfmt = 2 ! Ready for simulated obs
  else
    iprtfmt = 0
  endif

  if (ODBMP_myproc == 1.and.LLopen_file) then
    if (.not.is_table .and. iprtfmt /= 2) then
      CALL cODB_sqlprint(handle, dtname, iret)
    endif
  endif

  ilen = 1 ! Space for initial blank (or colon)
  ilen = ilen + inilen ! from existence of stat --> odb_statcharlen, if present(stat) & size(stat) > 0
  ilen = ilen + ncols ! Space for "ncols"-newlines (just if wrapcol = 1)
  if (ilow == 0) then
    itype(ilow) = pdbidx%id
    if (is_table) then
      names(ilow) = 'DB-index@T='//trim(dtname(2:))
    else
      names(ilow) = 'DB-index@V='//trim(dtname(1:))
    endif
    types(ilow) = 'long long int'
    ftntypes(ilow) = 'INTEGER(8)'
    inc = pdbidx%len
    inc = max(inc-1,len_trim('DB-index'))
    if (is_table) then
      inc = max(inc,len_trim('@T='//trim(dtname(2:))))
    else
      inc = max(inc,len_trim('@V='//trim(dtname(1:))))
    endif
    inclen(ilow) = inc+1 ! +1 for delimiter
    ilen = ilen + inc+1
  endif

  iret = ODB_getnames(handle, dtname,'name', names(1:ncols))
  iret = ODB_getnames(handle, dtname,'type', types(1:ncols))
  iret = ODB_getnames(handle, dtname,'ftntype', ftntypes(1:ncols))

  do j=1,ncols
    iat = scan(names(j),'@',back=.TRUE.)
    if (iat > 0) then
      inclen(j) = max(iat-1,len_trim(names(j)(iat:)))
    else
      inclen(j) = len_trim(names(j))
    endif
    if (ftntypes(j)(1:8) == 'INTEGER(' .and.&
     &types(j)(1:8) == 'Bitfield' ) then
      if (index(names(j),'.') == 0 .and. iprtfmt == 0) then
        itype(j) = pbitf%id
        inc = pbitf%len
      else
        itype(j) = puint%id
        inc = puint%len
      endif
    else if (types(j)(1:8) == 'Bitfield' ) then ! A result of nickname fugding : x as "Bitfield:x@table"
      itype(j) = pbitf%id
      inc = pbitf%len
    else if ( ftntypes(j)(1:7) == 'REAL(8)' .and.&
     &types(j)(1:7) == 'string ') then
      itype(j) = pstring%id
      inc = pstring%len
    else if (ftntypes(j)(1:5) == 'REAL(') then
      itype(j) = preal%id
      inc = preal%len
    else if (ftntypes(j)(1:8) == 'INTEGER(' .and.(&
     &types(j)(1:3) == 'hex'&
     &.or. types(j)(1:4) == 'bufr'&
     &.or. types(j)(1:4) == 'grib'&
     &)) then
      itype(j) = phex%id
      inc = phex%len
    else if (ftntypes(j)(1:8) == 'INTEGER(' .and.&
     &types(j)(1:8) == 'yyyymmdd') then
      itype(j) = pyyyymmdd%id
      inc = pyyyymmdd%len
    else if (ftntypes(j)(1:8) == 'INTEGER(' .and.&
     &types(j)(1:8) == 'hhmmss') then
      itype(j) = phhmmss%id
      inc = phhmmss%len
    else if (ftntypes(j)(1:8) == 'INTEGER(') then
      itype(j) = pint%id
      inc = pint%len
    else
      itype(j) = punknown%id
      inc = punknown%len
    endif
    if (LLprtsummary .and. nrows > 0) itype(j) = pvarprec%id
!    write(0,*)'LLprtsummary, nrows, j, itype(j), ilen=',LLprtsummary, nrows, j, itype(j), ilen
!    write(0,*) inclen(j),inc-1,iat,j,', names(j)="'//names(j)//'"'
    inc = max(inc-1,inclen(j))
    inclen(j) = inc+1 ! +1 for delimiter
    ilen = ilen + inc
  enddo

  ilen = max(3*len_trim(dtname),132,ilen)
  ilen = ilen + mod(ilen+1,2)

  deallocate(types)
  deallocate(ftntypes)

 110  continue

! Cache size: (for sizing dtmp(:,:) in prtdata)
! ==========
!
! (ncols - ilow + 1) * ODB_SIZEOF_REAL8 * icache  < allowed_cache_size_in_bytes
!
! --> icache = max(1,allowed_cache_size_in_bytes/((ncols - ilow + 1) * ODB_SIZEOF_REAL8))
!

  icache = max(1,allowed_cache_size_in_bytes/((ncols - ilow + 1) * ODB_SIZEOF_REAL8))

  if (ODBMP_myproc == 1) then
    if (.not.present(replicate_PE)) then
      pemin = 1
      pemax = ODBMP_nproc
    else
      if (replicate_PE == -1) then
        pemin = 1
      else
        pemin = replicate_PE
      endif
      pemax = pemin
    endif
    do pe=pemin,pemax
      CALL prtdata(&
       &handle, ipoolno, &
       &dtname, pe, &
       &trim(CL_file), itype, inclen, names, d, &
       &ilow, nglb_rows(pe), ncols, ilen, inilen, &
       &nglb_rows, zwall(2) - zwall(1),&
       &iprtfmt, LLprthdr, LLprttimingstat, &
       &LLprtsummary, LLprtunderline, istatlen, icache)
    enddo
  else
    pe = 0
    if (.not.present(replicate_PE)) then
      pe = ODBMP_myproc
    else
      if (replicate_PE == ODBMP_myproc) then
        pe = ODBMP_myproc
      endif
    endif
    if (pe > 0) then
      CALL prtdata(&
       &handle, ipoolno, &
       &dtname, pe,&
       &trim(CL_file), itype, inclen, names, d, &
       &ilow, nglb_rows(pe), ncols, ilen, inilen, &
       &nglb_rows, zwall(2) - zwall(1),&
       &iprtfmt, LLprthdr, LLprttimingstat, &
       &LLprtsummary, LLprtunderline, istatlen, icache)
    endif
  endif

 100  continue
  if (allocated(d)) deallocate(d)

  if (LLclose_file) then
    if (allocated(itype)) deallocate(itype)
    if (allocated(inclen)) deallocate(inclen)
    if (allocated(names)) deallocate(names)
    ilen = 0
  endif

  if (ODBMP_myproc == 1) then
    if (LLclose_file) then
      CALL cODB_closeprt(iret)
    else
      CALL cODB_flushprt(iret)
    endif
  endif

  if (.not.LLstdout .and. ODBMP_myproc == 1.and. .not.present(file)) then
    call flush(0)
    call flush(6)
    call codb_system('cat '//trim(CL_file))
    call util_remove_file(trim(CL_file), iret)
  endif

  CALL ODBMP_sync()
endif

CALL cODB_trace(handle, 0, 'ODB_print:'//dtname,idummy_arr, 0)
IF (LHOOK) CALL DR_HOOK('ODBPRINT:ODB_PRINT',1,ZHOOK_HANDLE)
END FUNCTION ODB_print


SUBROUTINE prtdata(&
     &handle, poolno, &
     &dtname, target,&
     &filename, itype, kinc, names, d, &
     &ilow, nrows, ncols, ilen, inilen, &
     &nglb_rows, deltaw, iprtfmt, print_title, print_timingstat, &
     &print_summary, print_underline, istatlen, ncache)
implicit none
INTEGER(KIND=JPIM), intent(in)          :: handle, nrows, ncols, ilow, inilen
INTEGER(KIND=JPIM), intent(in)          :: ilen, poolno, iprtfmt, ncache
INTEGER(KIND=JPIM), intent(in)          :: itype(ilow:), kinc(ilow:), target
character(len=*), intent(in) :: dtname, filename, names(ilow:)
REAL(KIND=JPRB), intent(in)             :: d(:,0:), deltaw
INTEGER(KIND=JPIM), intent(in)          :: nglb_rows(:), istatlen
logical, intent(in)                     :: print_title, print_timingstat, print_summary, print_underline

character(len=maxvarlen) buf
REAL(KIND=JPRB) :: dtmp(ilow:ncols, ncache), dd
INTEGER(KIND=JPIM), parameter :: idata_col_def = 7 ! Bits == 111
INTEGER(KIND=JPIM) :: idata_col
INTEGER(KIND=JPIM), parameter :: rootPE = 1
INTEGER(KIND=JPIM) :: i, j, jj, idot, iret, iexp, ipar, iat, itmp
INTEGER(KIND=JPIM) :: jc, jsta, jend, recip
INTEGER(KIND=JPIM) :: k, ib, ich, iline, j1, j2
INTEGER(KIND=JPIM) :: idate, itime
logical is_table, has_members, ok
logical LLsend, LLrecv, LLfill, LLio
character(len= 2) cdelim
character(len= 6) cmyproc
character(len= 8) cdate
character(len=10) ctime
character(len= 5) clabel
character(len=20) crows, ccols, ctmp
character(len=maxstrlen) cinfo
character(len=*), parameter :: hash    = '#'
character(len=*), parameter :: colon   = ':'
character(len=*), parameter :: space   = ' '
character(len=*), parameter :: comma   = ','
character(len=*), parameter :: null    = char(0)
character(len=*), parameter :: tab     = char(9)
character(len=*), parameter :: newline = char(10)
character(len=1) colon_char
INTEGER(KIND=JPIM) :: icolon_len
INTEGER(KIND=JPIM), parameter :: jpfirst = 6
#if __GNUC__ == 5
character(len=ilen) cline(jpfirst+nrows+1) ! workaround for regression in GNU 5.x (see ODB-231)
#else
character(len=ilen), allocatable :: cline(:)
#endif
INTEGER(KIND=JPIM) :: nlines, trace_info(6), len_statbuf, size_statbuf
logical, allocatable :: LLskip_this(:)
INTEGER(KIND=JPIM), parameter :: iprint_mdi_def = 1 ! print NULL=1 (def), print the value itself=0
INTEGER(KIND=JPIM), parameter :: iprint_pdt_def = 0 ! pretty date/time=1, the usual=0 (def)
INTEGER(KIND=JPIM) iprint_mdi, iprint_pdt, iprtimm, imod, ilim
INTEGER(KIND=JPIM), allocatable :: poolnos(:), rownums(:)
logical LLprint_mdi, LLprint_pdt, LLprint_imm
REAL(KIND=JPRB) ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('ODBPRINT:PRTDATA',0,ZHOOK_HANDLE)

!--   (a) Only PE#1 does I/O
!     (b) All other PEs send data to PE#1

!      write(0,*)'prtdata('//trim(dtname)//') : PE#',
!     $     ODBMP_myproc,':',target

if (LHOOK .and. ODBMP_myproc /= rootPE .and. ODBMP_myproc /= target) &
      & CALL DR_HOOK('ODBPRINT:PRTDATA',1,ZHOOK_HANDLE)
if (            ODBMP_myproc /= rootPE .and. ODBMP_myproc /= target) return

if (db_trace) then
  trace_info(:) = (/target, ilow, nrows, ncols, ilen, 0/)
  CALL cODB_trace(handle, 1,'prtdata:'//filename, &
                  trace_info, size(trace_info))
endif

LLio = (ODBMP_myproc == rootPE)

if (target == rootPE .and. LLio) then
  LLsend = .FALSE.
  LLrecv = .FALSE.
  LLfill = .TRUE.
else if (target == ODBMP_myproc) then
  LLsend = .TRUE.
  LLrecv = .FALSE.
  LLfill = .TRUE.
else
  LLsend = .FALSE.
  LLrecv = .TRUE.
  LLfill = .FALSE.
endif

if (LLio) then
  recip = target
else
  recip = -rootPE
endif

iprtimm = 0
if (ODBMP_nproc == 1) iprtimm = 1
LLprint_imm = (iprtimm == 1)

#if __GNUC__ != 5
if (LLprint_imm) then
  allocate(cline(jpfirst+1))
else
  allocate(cline(jpfirst+nrows+1))
endif
#endif

if (.not. LLfill) goto 9999

len_statbuf = 0
size_statbuf = istatlen
if (size_statbuf > 0) len_statbuf = min(inilen,odb_statcharlen)
if (len_statbuf <= 0) size_statbuf = 0 ! disabled again
!  write(0,*)'statbuf: size_statbuf, len_statbuf=',size_statbuf, len_statbuf
!  write(0,*)'odb_statbuf(:)='
!  do i=1,size_statbuf
!    write(0,*) i, '"'//odb_statbuf(i)//'"'
!  enddo

!write(cmyproc,'(i6)') ODBMP_myproc
write(cmyproc,'(i6)') poolno
cmyproc = trim(sadjustl(cmyproc))
!CALL DATE_AND_TIME(cdate, ctime)
CALL cODB_datetime(idate, itime)
cdate= ' '
write(cdate(1:8),'(i8.8)') idate
ctime=' '
write(ctime(1:6),'(i6.6)') itime

is_table = (dtname(1:1) == '@')
clabel = 'VIEW'
if (is_table) clabel = 'TABLE'

write(crows,'(i20)') nrows
write(ccols,'(i20)') ncols

CALL util_cgetenv('ODB_PRINT_COLON', colon, colon_char, icolon_len)

if (iprtfmt == 2) then ! simulated obs
  iline=1
  write(cline(iline),'(a)') &
      & '#'//trimadjL(dtname)//newline//&
      & '#/nrows='//trimadjL(crows)//newline//&
      & '#/ncols='//trimadjL(ccols)
  iline=2
  write(ctmp,'(i20)') ilen
  write(cline(iline),'(a)') &
      & '#/poolno='//trimadjL(cmyproc)//newline//&
      & '#/width='//trimadjL(ctmp)//newline//&
      & '#/end'
else
  iline = 1
  write(cline(iline),'(a)')&
       &colon_char//space//trim(clabel)//'="'//&
       &trimadjL(dtname)//&
       &'" on '//trim(cdate)//' at '//trim(ctime(1:6))

  iline = 2
  write(cline(iline),'(a)')&
       &colon_char//' Pool#'//trim(cmyproc)//': no. of rows x cols = '//&
       &trimadjL(crows)//&
       &' x '//&
       &trimadjL(ccols)
endif

has_members = .FALSE.

if (ilow == 0) then
  jsta = 1
  jend = ncols + 1
  if (nrows > 0) then
    allocate(poolnos(nrows))
    iret = odb_control_word_info(d(:,0),poolnos=poolnos) ! testing the vector-version
    allocate(rownums(nrows))
    iret = odb_control_word_info(d,rownums=rownums) ! this is the matrix-version
  endif
else
  jsta = 1
  jend = ncols
endif

CALL util_igetenv('ODB_PRINT_DATA_COL', idata_col_def, idata_col)
allocate(LLskip_this(jsta:jend))
LLskip_this(jsta:jend) = .FALSE.

CALL util_igetenv('ODB_PRINT_MDI', iprint_mdi_def , iprint_mdi)
LLprint_mdi = (iprint_mdi /= 0)

CALL util_igetenv('ODB_PRINT_PRETTY_DATE_TIME', iprint_pdt_def , iprint_pdt)
LLprint_pdt = (iprint_pdt /= 0)

!--   Print variable names (w/o member names, though)
iline = 3
if (iprtfmt == 2) then ! simulated obs
  cline(iline) = space
else
  cline(iline) = colon_char
endif
k = 1 + inilen
do jc=jsta,jend
  j = jc
  if (jc > ncols) j = ilow
  if (itype(j) == pbitf%id) then
    if (IAND(idata_col,8) /= 8) then ! Check for 4th bit = 1 ?
      LLskip_this(jc) = .TRUE.
      cycle
    endif
  endif
  buf = trimadjL(names(j))
  iat = scan(buf,'@',back=.TRUE.)
  if (iat > 0) buf(iat:) = space
  if (buf(1:11) == 'LINKOFFSET(') then
    if (IAND(idata_col,4) /= 4) then ! Check for 3rd bit = 1 ?
      LLskip_this(jc) = .TRUE.
      cycle
    endif
    buf(1:11) = space
    ipar = index(buf,')')
    if (ipar > 0) buf(ipar:) = space
    buf = trimadjL(buf)//'.offset'
  else if (buf(1:8) == 'LINKLEN(') then
    if (IAND(idata_col,2) /= 2) then ! Check for 2nd bit = 1 ?
      LLskip_this(jc) = .TRUE.
      cycle
    endif
    buf(1:8) = space
    ipar = index(buf,')')
    if (ipar > 0) buf(ipar:) = space
    buf = trimadjL(buf)//'.len'
  endif
  if (IAND(idata_col,1) /= 1) then ! Check for 1st bit = 1 ?
    LLskip_this(jc) = .TRUE.
    cycle
  endif
  cline(iline)(k+1:k+kinc(j)) = space//trim(buf)
  cline(iline)(k+1:k+kinc(j)) = sadjustr(cline(iline)(k+1:k+kinc(j)))
  k = k + kinc(j)
enddo

iline = 4
if (has_members) then
!--   Bitfield members present ==> print member names
  cline(iline) = colon_char
  k = 1 + inilen
  do jc=jsta,jend
    if (LLskip_this(jc)) cycle
    j = jc
    if (jc > ncols) j = ilow
    if (itype(j) == puint%id) then
      buf = trimadjL(names(j))
      idot = index(buf,'.')
      if (idot > 0) then
        buf(:idot) = space
        buf = '.'//trimadjL(buf)
        iat = scan(buf,'@',back=.TRUE.)
        if (iat > 0) buf(iat:) = space
      else
        buf = space
      endif
    else
      buf = space
    endif
    cline(iline)(k+1:k+kinc(j)) = space//trim(buf)
    cline(iline)(k+1:k+kinc(j)) = sadjustr(cline(iline)(k+1:k+kinc(j)))
    k = k + kinc(j)
  enddo
else
  cline(iline) = null ! ==> This line will not be printed at all
endif

!--   Print TABLE names the variables belong to
iline = 5
cline(iline) = colon_char
k = 1 + inilen
do jc=jsta,jend
  if (LLskip_this(jc)) cycle
  j = jc
  if (jc > ncols) j = ilow
  buf = trimadjL(names(j))
  iat = scan(buf,'@',back=.TRUE.)
  if (iat > 0) then
    buf(:iat) = space
    buf = '@'//trimadjL(buf)
  endif
  cline(iline)(k+1:k+kinc(j)) = space//trim(buf)
  cline(iline)(k+1:k+kinc(j)) = sadjustr(cline(iline)(k+1:k+kinc(j)))
  k = k + kinc(j)
enddo

if (iprtfmt == 2) then ! simulated obs
  cline(iline) = null ! ==> This line will not be printed at all
endif

!--   Handle underline
iline = 6
cline(iline) = cline(iline-1)
do j=2,ilen
  if (cline(iline)(j:j) /= space) cline(iline)(j:j) = '='
enddo

if (LLio .and. (print_title .or. print_underline)) then
  if (print_title) then
    if (target == rootPE .and. iprtfmt == 0) then
      write(ccols,'(i20)') count(.not.LLskip_this(:))
      write(ctmp,'(i20)') size(nglb_rows)
      cinfo = colon_char//space//trimadjL(ccols)//space//trimadjL(ctmp)
      do j=1,size(nglb_rows)
        write(ctmp,'(i20)') nglb_rows(j)
        cinfo = trimadjL(cinfo)//space//trimadjL(ctmp)
      enddo
      CALL cODB_lineprt(trim(cinfo)//newline, iret)
    endif
    j1 = 1
    j2 = jpfirst
  else if (print_underline) then
    j1 = jpfirst
    j2 = j1
  endif
  nlines = 0
  do iline=j1,j2
    if (cline(iline)(1:1) /= null) then
      CALL cODB_lineprt(trim(cline(iline))//newline, iret)
      nlines = nlines + 1
    endif
  enddo
endif

do i=1,nrows
  imod = mod(i-1,ncache) + 1
  if (imod == 1) then
     ilim = min(ncache,nrows-i+1)
     do j=ilow,ncols
        do k=1,ilim
           dtmp(j,k) = d(i+k-1,j)
        enddo
     enddo
  endif

  if (LLprint_imm) then
    iline = jpfirst + 1
  else
    iline = jpfirst + i
  endif

  k = 1 + inilen
  cline(iline)(1:k) = space
  if (size_statbuf > 0 .and. i <= size_statbuf) then
    cline(iline)(1:k) = colon_char//odb_statbuf(i)(1:len_statbuf)
!    write(0,*) i,iline,k,'"'//cline(iline)(1:k)//'"'
  endif
  if (LLprint_imm) CALL cODB_lineprt(cline(iline)(1:k), iret)

!  if (iprtfmt == 0) then
!    cdelim = space//null
!  else
    cdelim = null
!  endif
  do jc=jsta,jend
    if (LLskip_this(jc)) cycle
    j = jc
    if (jc > ncols) j = ilow
    dd = dtmp(j,imod)
    if (itype(j) == pdbidx%id) then
      CALL cODB_fill_ctrlw(iprtimm,cdelim,pdbidx%cfmt,poolnos(i),rownums(i),cline(iline)(k+1:k+kinc(j)),iret)
    else if (itype(j) == pint%id) then
      if (LLprint_mdi .and. abs(dd) == ODB_NMDI) then
        CALL cODB_fill_NULL(iprtimm,cdelim,pint%len,cline(iline)(k+1:k+kinc(j)),iret)
      else
        CALL cODB_fill_longlongint(iprtimm,cdelim,pint%cfmt,dd,cline(iline)(k+1:k+kinc(j)),iret)
      endif
    else if (itype(j) == puint%id) then
      if (LLprint_mdi .and. abs(dd) == ODB_NMDI) then
        CALL cODB_fill_NULL(iprtimm,cdelim,puint%len,cline(iline)(k+1:k+kinc(j)),iret)
      else
        CALL cODB_fill_ulonglongint(iprtimm,cdelim,puint%cfmt,dd,cline(iline)(k+1:k+kinc(j)),iret)
      endif
    else if (itype(j) == preal%id) then
      if (LLprint_mdi .and. abs(dd) == ODB_NMDI) then
        CALL cODB_fill_NULL(iprtimm,cdelim,preal%len,cline(iline)(k+1:k+kinc(j)),iret)
      else
        CALL cODB_fill_dble(iprtimm,cdelim,preal%cfmt,dd,cline(iline)(k+1:k+kinc(j)),iret)
      endif
    else if (itype(j) == pvarprec%id) then
      if (LLprint_mdi .and. abs(dd) == ODB_NMDI) then
        CALL cODB_fill_NULL(iprtimm,cdelim,kinc(j),cline(iline)(k+1:k+kinc(j)),iret)
      else
        CALL cODB_fill_varprec(iprtimm,cdelim,pvarprec%cfmt,dd,cline(iline)(k+1:k+kinc(j)),iret)
      endif
    else if (itype(j) == pstring%id) then
      CALL cODB_fill_string(iprtimm,cdelim,pstring%cfmt,dd,cline(iline)(k+1:k+kinc(j)),iret)
    else if (itype(j) == pbitf%id) then
      CALL cODB_d2u(dd, itmp)
      CALL cODB_fill_bitfield(iprtimm,cdelim,pbitf%len,itmp,cline(iline)(k+1:k+kinc(j)),iret)
    else if (itype(j) == phex%id) then
      CALL cODB_d2u(dd, itmp)
      CALL cODB_fill_truehex(iprtimm,cdelim,phex%cfmt,itmp,cline(iline)(k+1:k+kinc(j)),iret)
    else if (itype(j) == pyyyymmdd%id) then
      if (LLprint_mdi .and. abs(dd) == ODB_NMDI) then
         CALL cODB_fill_NULL(iprtimm,cdelim,pyyyymmdd%len,cline(iline)(k+1:k+kinc(j)),iret)
      else
         itmp = int(dd)
         if (LLprint_pdt) then
           CALL cODB_fill_yyyymmdd(iprtimm,cdelim,pyyyymmdd_pdt%cfmt,itmp,&
                & cline(iline)(k+1:k+kinc(j)),iret)
         else
           CALL cODB_fill_trueint(iprtimm,cdelim,pyyyymmdd%cfmt,itmp,cline(iline)(k+1:k+kinc(j)),iret)
         endif
      endif
    else if (itype(j) == phhmmss%id) then
      if (LLprint_mdi .and. abs(dd) == ODB_NMDI) then
         CALL cODB_fill_NULL(iprtimm,cdelim,phhmmss%len,cline(iline)(k+1:k+kinc(j)),iret)
      else
         itmp = int(dd)
         if (LLprint_pdt) then
           CALL cODB_fill_hhmmss(iprtimm,cdelim,phhmmss_pdt%cfmt,itmp,&
                & cline(iline)(k+1:k+kinc(j)),iret)
         else
           CALL cODB_fill_trueint(iprtimm,cdelim,phhmmss%cfmt,itmp,cline(iline)(k+1:k+kinc(j)),iret)
         endif
      endif
    else
       CALL cODB_fill_UNKNOWN(iprtimm,cdelim,punknown%len,cline(iline)(k+1:k+kinc(j)),iret)
    endif

    k = k + kinc(j)
    if (jc < jend .and. mod(jc,wrapcol) == 0) then
      if (LLprint_imm) then
         CALL cODB_lineprt(newline, iret)
      else
         cline(iline)(k+1:k+1) = newline
      endif
      k = k + 1
    endif
    if (iprtfmt == 1) then ! Microsoft Excel
      cdelim = tab//null
    endif
  enddo ! do jc=jsta,jend
  if (LLprint_imm) CALL cODB_lineprt(newline, iret)
enddo ! do i=1,nrows

if (print_timingstat) then
!-- The last line
  iline = size(cline)
  if (iprtfmt == 2) then
    cline(iline) = space
  else
    write(cline(iline),'(a,i12,a,f7.3,a)')&
       &colon_char//space//trim(clabel)//'="'//&
       &trimadjL(dtname)//&
       &'" : ',nrows,' row(s) in ',deltaw,' secs (pool#'//trim(cmyproc)//')'
  endif
  if (LLprint_imm) CALL cODB_lineprt(trim(cline(iline))//newline, iret)
endif

9999 continue

if (ODBMP_testready(recip)) then
  if (LLsend) then
    iret = ODB_distribute(cline, rootPE)
  else if (LLrecv) then
    iret = ODB_distribute(cline, -target)
  endif
endif

if (LLio .and. .not.LLprint_imm) then
  do iline=jpfirst+1,size(cline)
    if (cline(iline)(1:1) /= null) then
      CALL cODB_lineprt(trim(cline(iline))//newline, iret)
      nlines = nlines + 1
    endif
  enddo
endif

if (allocated(LLskip_this)) deallocate(LLskip_this)
#if __GNUC__ != 5
if (allocated(cline)) deallocate(cline)
#endif
if (allocated(poolnos)) deallocate(poolnos)
if (allocated(rownums)) deallocate(rownums)

if (db_trace) then
  trace_info(:) = (/target, ilow, nrows, ncols, ilen, nlines/)
  CALL cODB_trace(handle, 0,'prtdata:'//filename, &
                  trace_info, size(trace_info))
endif
IF (LHOOK) CALL DR_HOOK('ODBPRINT:PRTDATA',1,ZHOOK_HANDLE)
END SUBROUTINE prtdata

#undef trimadjL
#undef trimadjR

END MODULE odbprint
