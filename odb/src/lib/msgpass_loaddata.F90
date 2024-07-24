SUBROUTINE msgpass_loaddata(khandle, kret)

! A routine to exchange ODB-data between processors
! Called from module ODB: ODB_open():open_db 

! P. Marguinaud : 10-10-2013 : Nullify pointers

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
use oml_mod, only : oml_set_lock, oml_unset_lock, oml_my_thread
USE odb_module
USE mpl_module
USE odbio_msgpass
USE str, ONLY : sadjustl 

implicit none

INTEGER(KIND=JPIM), intent(in)  :: khandle ! database handle
INTEGER(KIND=JPIM), intent(out) :: kret    ! return code

INTEGER(KIND=JPIM)              :: io_method, jt, j, jj, ntables, iret, maxbytes
INTEGER(KIND=JPIM)              :: nw, nwmax, nbytes, nrows, ncols, npad, igrpsize, ipoolno, npools
INTEGER(KIND=JPIM)              :: itag, pe, destPE, io, isendreq, max_iope, iprev, idx
INTEGER(KIND=JPIM), allocatable :: itags(:,:)
INTEGER(KIND=JPIM)              :: iexpect_len
INTEGER(KIND=JPIM), allocatable :: iope_map(:,:)
character(len=maxvarlen), allocatable :: cltable(:)
logical, allocatable            :: LL_mypool(:), LL_include_tbl(:)
logical                         :: LL_readonly
logical                         :: LL_check_len
logical, save                   :: LL_first_time = .TRUE.
logical, save                   :: LL_write_empty = .FALSE.
INTEGER(KIND=JPIM), save        :: iwrite_empty = 0
INTEGER(KIND=JPIM), parameter   :: io_filesize_default = 32 ! in megabytes; see ../aux/newio.c, too
INTEGER(KIND=JPIM), save        :: io_filesize = 1
INTEGER(KIND=JPIM), save        :: io_grpsize = -1
INTEGER(KIND=JPIM), parameter   :: io_trace_default = 0
INTEGER(KIND=JPIM), save        :: io_trace = 0
INTEGER(KIND=JPIM), parameter   :: io_msgpass_trace_default = 0
INTEGER(KIND=JPIM), save        :: io_msgpass_trace = 0
logical, save                   :: LLiotrace = .FALSE.
logical, save                   :: LLiomsgpasstrace = .FALSE.
character(len=1), parameter     :: CLtab = char(9)
character(len=1)                :: CLmode='r', CLtmp
character(len=maxvarlen)        :: dbname, CLtbl, CLbytes
character(len=4096)             :: clenv, filename, CLdirname, CLbasename

INTEGER(KIND=JPIM), parameter   :: maxsize_mb = 1000 ! Maximum filesize in megabytes for HC32-file
INTEGER(KIND=JPIM)              :: nread
INTEGER(KIND=JPIM)              :: jmin, jmax, it, is_compressed
INTEGER(KIND=JPIM), allocatable :: istat(:,:,:) ! nstat x npools x nactive_tables
INTEGER(KIND=JPIM), parameter   :: NHDR = 6
INTEGER(KIND=JPIM)              :: ihdr(NHDR), idum(1), this_io_grpsize, itables, cur_io_grpsize
INTEGER(KIND=JPIM)              :: nactive_tables, jtact, iope, icnt_not_active
INTEGER(KIND=JPIM)              :: itime(8),imsgpass_Time,imsgpass_Date,i_consider,jsta, i_write
INTEGER(KIND=JPIM), pointer     :: incore(:)
!J---Start---
INTEGER(KIND=JPIM), parameter   :: MMSGSND = 1000
INTEGER(KIND=JPIM), pointer     :: jncore(:)
INTEGER(KIND=JPIM)              :: MJNCORE = 10000000
INTEGER(KIND=JPIM)              :: msgoff, msgsnd, njmax
INTEGER(KIND=JPIM)              :: jsendreq(MMSGSND)
!J---End---
INTEGER(KIND=JPIM), save        :: hc32(0:1) = (/0,0/) ! "HC32" & its reverse/byteswap (to be filled) 
INTEGER(KIND=JPIM), parameter   :: reset_updated_flag = 1
INTEGER(KIND=JPIM), POINTER     :: ioaid(:,:,:) ! A shorthand notation
REAL(KIND=JPRB), allocatable    :: data_bytes(:) ! nactive_tables
REAL(KIND=JPRB)                 :: datavolume(0:ODBMP_nproc), waltim(2), sum_datavolume
INTEGER(KIND=JPIM)              :: nfiles, idummy_arr(1), npermcnt
INTEGER(KIND=JPIM), allocatable :: perm_list(:), global_pool_offset(:)
logical, allocatable            :: LL_poolmask(:), LL_filemask(:), LL_lastpool(:)
REAL(KIND=JPRB), external       :: util_walltime  ! now [23/1/04] from ifsaux/support/drhook.c
INTEGER(KIND=JPIM)              :: fast_physproc, xfast, nfblk, irefblk
INTEGER(KIND=JPIM)              :: imp_type
LOGICAL                         :: L2,L3,L4
INTEGER(KIND=JPIM)              :: nfileno
REAL(KIND=JPRB)                 :: ZHOOK_HANDLE


fast_physproc(xfast) = mod(xfast-1,ODBMP_nproc)+1
IF (LHOOK) CALL DR_HOOK('MSGPASS_LOADDATA',0,ZHOOK_HANDLE)

incore => null ()
jncore => null ()
ioaid  => null ()

call MPL_barrier(cdstring='MSGPASS_LOADDATA')

if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_LOADDATA : Begin'
kret = 0
itables = 0
it = oml_my_thread()

if (.NOT.db(khandle)%inuse) goto 99999

io_method = db(khandle)%io_method
if (io_method /= 4) goto 99999 ! only applicable for ODB_IO_METHOD=4

LL_readonly = db(khandle)%readonly

#if defined(NECSX) && defined(BOM)
imp_type = JP_BLOCKING_STANDARD
#else
imp_type = JP_NON_BLOCKING_STANDARD
#endif

dbname = db(khandle)%name

CALL ODBMP_sync()

waltim(1) = util_walltime()

if (ODBMP_myproc == 1) then
  call date_and_time (values=itime)
  imsgpass_time=itime(5)*10000+itime(6)*100+itime(7)
  imsgpass_date=itime(1)*10000+itime(2)*100+itime(3)
  write(0,'(1x,a,i8.8,2x,i6.6)') &
  '=== MSGPASS_LOADDATA of db="'//trim(dbname)// &
  '" for mode='//CLmode//' started on ',imsgpass_date,imsgpass_time
endif

datavolume(:) = 0
nfiles = 0 ! no. of opened files on this PE

if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_LOADDATA ==='
if (LL_first_time) then
!$ CALL OML_SET_LOCK()
!$OMP FLUSH(LL_first_time)
 if (LL_first_time) then
  !-- HC32-magic number
  CALL get_magic_hc32(0, hc32(0))
  !-- HC32-magic number byteswap i.e. reverse
  CALL get_magic_hc32(1, hc32(1))
  !-- I/O tracing
  CALL util_igetenv('ODB_IO_TRACE', io_trace_default, io_trace)
  LLiotrace = (io_trace /= 0)
  !-- Message pass tracing
  CALL util_igetenv('ODB_IO_MSGPASS_TRACE', io_msgpass_trace_default, io_msgpass_trace)
  LLiomsgpasstrace = (io_msgpass_trace /= 0)
  !-- Make sure these stay consistent with the ones obtained in ../aux/newio.c !
  CALL util_igetenv('ODB_IO_FILESIZE', io_filesize_default, io_filesize) ! in megabytes
  if (io_filesize <= 0) io_filesize = io_filesize_default
  if (io_filesize > maxsize_mb) io_filesize = maxsize_mb ! never exceed this many megabytes
  CALL util_igetenv('ODB_IO_GRPSIZE', 0, io_grpsize)
  if (io_grpsize <= 0) then ! try $NPES_AN
    CALL util_igetenv('NPES_AN', 0, io_grpsize)
  endif
  if (io_grpsize <= 0) io_grpsize = ODBMP_nproc
  CALL util_igetenv('ODB_WRITE_EMPTY_FILES', 0, iwrite_empty)
!*AF  LL_write_empty = (iwrite_empty /= 0)
  LL_write_empty = (iwrite_empty == 1)
  if (ODBMP_myproc == 1) then
    write(0,*)'                 HC32=',hc32
    write(0,*)'         ODB_IO_TRACE=',io_trace
    write(0,*)' ODB_IO_MSGPASS_TRACE=',io_msgpass_trace
    write(0,*)'      ODB_IO_FILESIZE=',io_filesize,' MBytes'
    write(0,*)'       ODB_IO_GRPSIZE=',io_grpsize
    write(0,*)'ODB_WRITE_EMPTY_FILES=',iwrite_empty, LL_write_empty
  endif
  LL_first_time = .FALSE.
!$OMP FLUSH(LL_first_time)
 endif
!$ CALL OML_UNSET_LOCK()
endif

if (ODBMP_myproc == 1) then
  write(0,*) ODBMP_myproc,&
   & ': MSGPASS_LOADDATA() khandle=',&
   & khandle
endif

npools = db(khandle)%glbNpools
this_io_grpsize = min(npools,io_grpsize) 

!if (ODBMP_myproc == 1) write(0,*) 'this_io_grpsize, npools=',&
!                                   this_io_grpsize, npools

allocate(LL_poolmask(npools))
LL_poolmask(1:npools) =  .TRUE. ! true means that take all these pools

allocate(LL_filemask(npools))
allocate(LL_lastpool(npools))
if (LL_readonly) then
  CALL cODB_get_permanent_poolmask(khandle, 0, idummy_arr, npermcnt)
  npermcnt = abs(npermcnt)
  if (npermcnt > 0) then
    allocate(perm_list(npermcnt))
    perm_list(:) = 0
    CALL cODB_get_permanent_poolmask(khandle, npermcnt, perm_list, iret)
    LL_poolmask(1:npools) = .FALSE. ! By default: do *not* take these pools
    do j=1,npermcnt
      jj = perm_list(j)
      if (jj >= 1 .and. jj <= npools)  LL_poolmask(jj) = .TRUE.
    enddo
    deallocate(perm_list)
  endif
endif

if (ODBMP_myproc == 1) then
  if (ALL(LL_poolmask(1:npools))) then
    write(0,'(1x,a,i5,a,i5,a)') &
      & 'MSGPASS_LOADDATA: All pools in range [1..npools] will processed'
  else
    write(0,*)'MSGPASS_LOADDATA: Only the following pools will be processed:'
    jj = 0
    do j=1,npools
      if (LL_poolmask(j)) then
        write(0,'(1x,i7)',advance='no') j
        jj = jj + 1
        if (mod(jj,10) == 0 .AND. j < npools) write(0,*)
      endif
    enddo
    if (jj > 0) write(0,*)
  endif
endif

CALL comm_iomap(khandle, 1, iret) ! Get up to date IOAID

ioaid => db(khandle)%ioaid

allocate(global_pool_offset(npools))
global_pool_offset(:) = 0
nfblk = db(khandle)%nfileblocks
ipoolno = 0 
irefblk = 0
do j=1,npools
  if (ioaid(j,1,IOAID_FBLOCK) /= irefblk) then
    irefblk = ioaid(j,1,IOAID_FBLOCK)
    ipoolno = j - 1
  endif
  global_pool_offset(j) = ipoolno
enddo

ntables = ODB_getnames(khandle, '*', 'table')
allocate(cltable(ntables))
ntables = ODB_getnames(khandle, '*', 'table', cltable)

allocate(LL_mypool(npools))
allocate(LL_include_tbl(ntables))

do j=1,npools
  pe = fast_physproc(j)
  LL_mypool(j) = (pe == ODBMP_myproc)
enddo

!-- New approach (5-Sep-2006/SS)
do jt=1,ntables
  CALL cODB_table_is_considered(cltable(jt), i_consider)
  LL_include_tbl(jt) = (i_consider == 1)
enddo

icnt_not_active = COUNT(.not.LL_include_tbl(:))
nactive_tables = ntables - icnt_not_active
if (ODBMP_myproc == 1) then
  write(0,*)'***INFO: Considering the following ',&
   & nactive_tables,' tables for database="'//trim(dbname)//'" :' 
! do jt=1,ntables
!   if (LL_include_tbl(jt)) write(0,*)'  '//trim(cltable(jt))
! enddo
  if (icnt_not_active == 0) then
    write(0,*)'***INFO: All tables considered'
  endif
endif

do jt=1,ntables
  i_consider = 1
  if (.not.LL_include_tbl(jt)) i_consider = 0
  CALL cODB_consider_table(khandle, trim(cltable(jt)), i_consider, iret)
enddo

if (nactive_tables <= 0) then
  if (ODBMP_myproc == 1) write(0,*)'***INFO: No active tables'
  kret = 0
  goto 99999
endif

allocate(istat(nstat,npools,nactive_tables))

allocate(iope_map(npools+1,nactive_tables))

allocate(data_bytes(nactive_tables))


!-- Get current status of incore items
istat(:,:,:) = 0
jtact = 0
STAT_LOOP: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle STAT_LOOP 
  jtact = jtact + 1

!-- Strip off the leading '@'-character
  CLtbl = cltable(jt)(2:)

!-- Gather size information of incore items for memory allocations
  do j=1,npools
    if (LL_mypool(j)) then
      CALL newio_status_incore32(khandle, j, istat(1,j,jtact), nstat, CLtbl, iret)
!*AF      write(0,*) 'msgpass_loaddata table = ', trim(CLtbl), ' pool = ', j, ' nrows = ', istat(2,j,jtact), ' ncols = ', istat(3,j,jtact), &
!*AF                 'updated = ', istat(4,j,jtact)
    endif
  enddo
enddo STAT_LOOP

!-- Make istat-information globally available
CALL ODBMP_global('MAX', istat)

!J---Start----
!-- Initialisation for NON BLOCKING message passing------------
call allocate_jncore(jncore, MJNCORE,iret)
njmax=MJNCORE
msgsnd = 0
msgoff = 0
!J---End---

!-- Get iope_map for ALL applicable tables in one go
iope_map(:,:) = -1 ! Means: Any PE
jtact = 0
LOAD_IOPES: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle LOAD_IOPES
  jtact = jtact + 1
  data_bytes(jtact) = 0

!-- If any bytes have been loaded, then prevent overwriting existing incore-structures from disk
  maxbytes = MAXVAL(istat(ISTAT_NBYTES,1:npools,jtact))
  if (maxbytes > 0) cycle LOAD_IOPES  ! already loaded --> skip

!-- Strip off the leading '@'-character
  CLtbl = cltable(jt)(2:)

!-- Determine which pools have files present ==> pool owner-PEs become I/O-PEs
  do j=1,npools
    !-- ipoolno below is LOCAL (not global) poolno with respect to fileblock ioaid(j,jt,IOAID_FBLOCK) in concern
    if (ioaid(j,jt,IOAID_GRPSIZE) > 0) then
      ipoolno = mod(ioaid(j,jt,IOAID_POOLNO)-1,ioaid(j,jt,IOAID_GRPSIZE)) + 1 
    else
      ipoolno = 0
    endif
    if ( LL_mypool(j) .and. ipoolno > 0 .and. &
       & ipoolno == ioaid(j,jt,IOAID_FILENO) ) then

!-- The expected length in bytes can be calculated as follows :
      jmin = max(j,1)
      jmax = min(j+ioaid(j,jt,IOAID_GRPSIZE)-1,size(ioaid,dim=1))
      iexpect_len = SUM(  ioaid(jmin:jmax,jt,IOAID_LENGTH), &
                  & mask=(ioaid(jmin:jmax,jt,IOAID_FILENO) == ipoolno .and. &
                  &       ioaid(jmin:jmax,jt,IOAID_FBLOCK) == ioaid(j,jt,IOAID_FBLOCK)))

      LL_check_len = .TRUE.
      if (iexpect_len > 0) then
        iexpect_len = iexpect_len + sizeof_int   ! Plus length of the MAGIC-word (HC32)
        CALL codb_tablesize(dbname, CLtbl, j, iret)
        if (iret > 0 .and. iret /= iexpect_len) then
           CALL makefilename(dbname, CLtbl, j, filename)
           CALL cma_is_externally_compressed(trim(filename), 'r', is_compressed)
!             write(0,*)'msgpass_loaddata: filename='//trim(filename)//' : is_compressed=',is_compressed
!             write(0,*)'msgpass_loaddata: iret,iexpect_len=',iret,iexpect_len
           LL_check_len = (is_compressed == 0)
        endif
      else
        iret = 0
      endif

      if (LLiotrace .or. LLiomsgpasstrace) then 
        write(0,'(1x,i5,a,2i12,5i7)') &
        & ODBMP_myproc,': tablesize for "'//trim(CLtbl)//'" & poolno=',&
        & iret,ioaid(j,jt,IOAID_LENGTH),j,ioaid(j,jt,IOAID_POOLNO),&
        & ioaid(j,jt,IOAID_FILENO),jtact,ipoolno
      endif

      if (iret == iexpect_len .or. .not.LL_check_len) then
        if (iret > 0) then
          iope_map(j,jtact) = ODBMP_myproc
          data_bytes(jtact) = data_bytes(jtact) + iret
        endif
      else
        write(0,*) ODBMP_myproc,': Expected length disagrees with the actual'
        write(0,'(1x,i5,a,3i12)') ODBMP_myproc,': table, poolno, iret, iexpect_len : "'// &
        &                                   trim(CLtbl)//'"', j, iret, iexpect_len
        CALL ODB_abort('MSGPASS_LOADDATA', &
                      &'Expected length disagrees with the actual', &
                      & iret)
      endif
    endif
  enddo
enddo LOAD_IOPES

!-- Make iope-information globally available
call ODBMP_global('MAX', iope_map)

!-- Make data_bytes-information available for PE#1
CALL ODBMP_global('SUM', data_bytes, LDREPROD=.false., root=1)

!-- Debug output
if ( (LLiotrace .or. LLiomsgpasstrace) .and. ODBMP_myproc == 1) then 
  jtact = 0
  do jt=1,ntables
    if (.not.LL_include_tbl(jt)) cycle
    jtact = jtact + 1
    CLtbl = cltable(jt)(2:)
    write(0,'(1x,2(a,i5),a,f20.1,a)') &
    & 'I/O-pe map for table "'//trim(CLtbl)//'", pool range=[1..npools], total ',&
    & data_bytes(jtact),' bytes :'
    write(0,'((10(1x,i5)))' ) (iope_map(j,jtact),j=1,npools)
  enddo
endif

!-- Predefine message tags for all used table,pools combinations 
!   (global not local - as need to know tags of incoming messages from other tasks)
! Loop through all table/pool combinations and determine if a message will need to be sent,
!   if so, then define a tag for the send/recv to use. 
allocate(itags(npools+1,ntables))
itags(:,:) = -1
itag = 0
TABLE_LOOP: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle TABLE_LOOP
  jj = 1
  do while (jj <= npools)
    if (LL_poolmask(jj)) then
      j = jj
      if (ioaid(j,jt,IOAID_LENGTH) > 0) then
           itags(jj,jt) = itag
      itag = itag + 1
      endif
    endif
    jj = jj + 1
  enddo ! jj <=npools 
enddo TABLE_LOOP
if (ODBMP_myproc == 1) write(0,*) 'Created itags array with MAX: ',MAXVAL(itags(:,:)), ' and MIN: ',MINVAL(itags(:,:))



!-- A procedure for loading data from disk (horizontal concatenation; io_method=4)
jtact = 0
LOAD_TABLE: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle LOAD_TABLE
  jtact = jtact + 1

!-- Strip off the leading '@'-character
  CLtbl = cltable(jt)(2:)

!-- If all PEs in the receiving end, then there is obviously no data behind any PE !
  max_iope = MAXVAL(iope_map(1:npools,jtact))
  if (max_iope == -1) then
!     if (ODBMP_myproc == 1) write(0,*) &
!      & '***Warning: LOADing table="'//trim(CLtbl)//'" skipped since empty (or already loaded)' 
    cycle LOAD_TABLE
  endif

  if (ODBMP_myproc == 1) then
    write(CLbytes,'(f20.1)') data_bytes(jtact)
    jj = index(CLbytes,'.')
    if (jj > 0) CLbytes(jj:) = ' '
    write(0,'(1x,a)')'LOADing table="'//trim(CLtbl)//'" total = '//&
     & trim(CLbytes)//' bytes' 
  endif

!-- Worker-loop: perform read-I/O & send/recv data
  isendreq=0
  nread = 0
  io = -1
  iprev = -1
  CALL codb_strblank(filename)

  LL_filemask(:) = .FALSE. ! .TRUE. only if file is to be processed at all
  LL_lastpool(:) = .FALSE. ! .TRUE. only if pool is beyond the last to be processed in its file

  j = 1
  jj = 1
  iprev = 0
  FILEMASK_LOOP: do while (j <= npools)
    if (LL_poolmask(j)) then
      LL_filemask(jj) = .TRUE.
      LL_filemask(j) = .TRUE.
      iprev = j ! last pool so far which we care about for this file #jj
    endif
    if (j < npools) then
      !-- detect the change of file: next chunk will be read from different file
      if ( ioaid(j+1,jt,IOAID_FBLOCK) /= ioaid(j,jt,IOAID_FBLOCK) .OR. &
          (ioaid(j+1,jt,IOAID_LENGTH)  > 0 .AND. &
           ioaid(j+1,jt,IOAID_OFFSET) <= ioaid(j,jt,IOAID_OFFSET))) then
        if (iprev > 0) LL_lastpool(iprev+1:j) = .TRUE. ! skip the reading these pools
        iprev = 0
        jj = j + 1
      endif
    endif
    j = j + 1
  enddo FILEMASK_LOOP
  j=min(npools,j)
  if (iprev > 0) LL_lastpool(iprev+1:j) = .TRUE. ! skip the reading these pools

  if ( (LLiotrace .or. LLiomsgpasstrace) .AND. ODBMP_myproc == 1) then
    write(0,*)'LL_filemask='
    write(0,'((32(1x,L1)))') (LL_filemask(j),j=1,npools)
    write(0,*)'LL_poolmask='
    write(0,'((32(1x,L1)))') (LL_poolmask(j),j=1,npools)
    write(0,*)'LL_lastpool='
    write(0,'((32(1x,L1)))') (LL_lastpool(j),j=1,npools)
  endif

  jj = 1
  LOAD_LOOP: do while (jj <= npools)
   if (LL_filemask(jj) .AND. .not.LL_lastpool(jj)) then
    nw = 0

    if (iope_map(jj,jtact) == ODBMP_myproc) then
      jsta = jj

      j = jj
      cur_io_grpsize = ioaid(j,jt,IOAID_GRPSIZE)

      nread = 0
      CALL io_checkopen(dbname, filename, CLtbl, io, j, nread, cur_io_grpsize, LDread=.TRUE.,&
                        LLiotrace=LLiotrace, hc32=hc32, global_pool_offset=global_pool_offset, &
                        kfileno=nfileno, kret=iret)

      iprev = -1
      IO_LOOP: DO

        if (ioaid(j,jt,IOAID_LENGTH) > 0) then
          ihdr(:) = 0
          CALL cma_readi(io, ihdr(1), NHDR, iret) ! readI ==> prepare for be2le conversion
          if (iret == -1) then ! Unexpected EOF encountered
            CALL io_error('***MSGPASS_LOADDATA(io_error): Unexpected EOF encountered while reading file', &
             & iret, NHDR, j, filename, kdata=ioaid(jsta:j,jt,IOAID_OFFSET)) 
          endif          

          if (iret /= NHDR) CALL io_error('***MSGPASS_LOADDATA(io_error): Unable to read header data', iret, NHDR, j, &
           & filename, kdata=ihdr(1:NHDR)) 
          
          ipoolno = ihdr(1)
          igrpsize = ihdr(2)
          nbytes = ihdr(3)
          nrows = ihdr(4)
          ncols = ihdr(5)
          npad = ihdr(6) ! ... not used for anything (else) yet ...
          nw = NHDR + (nbytes + sizeof_int - 1)/sizeof_int

          if (ioaid(j,jt,IOAID_LENGTH) /= nw * sizeof_int) then
            CALL io_error('***MSGPASS_LOADDATA(io_error): Inconsistent no. of bytes', &
             & nw * sizeof_int, ioaid(j,jt,IOAID_LENGTH), j, filename, &
             & kdata=ioaid(jsta:j,jt,IOAID_LENGTH)) 
          endif

          if (ioaid(j,jt,IOAID_OFFSET) /= nread) then
            CALL io_error('***MSGPASS_LOADDATA(io_error): Data expected from different byte-offset', &
             & nread, ioaid(j,jt,IOAID_OFFSET), j, &
             & filename, kdata=ioaid(jsta:j,jt,IOAID_OFFSET)) 
          endif

          nread = nread + iret * sizeof_int

          destPE = fast_physproc(j)

          if (LL_poolmask(j)) then
            CALL allocate_incore(incore, nw + 1, iret)
            incore(1:NHDR) = ihdr(1:NHDR)
            CALL cma_readb(io, incore(NHDR+1), (nw - NHDR) * sizeof_int, iret) ! readB; raw data xfer
            if (iret /= (nw - NHDR) * sizeof_int) then
              CALL io_error('***MSGPASS_LOADDATA(io_error): Unable to read data', iret, &
                            (nw - NHDR) * sizeof_int, j, filename)
            endif
          else ! just seek forward [=1] (should work for gzip'ped files, too; don't rewind though!)
            CALL cma_seekb(io, (nw - NHDR) * sizeof_int, 1, iret)
            if (iret /= 0) then
              CALL io_error('***MSGPASS_LOADDATA(io_error): Unable to seek past data', iret, &
                            (nw - NHDR) * sizeof_int, j, filename)
            endif
            iret = (nw - NHDR) * sizeof_int
          endif
          nread = nread + iret

          if (LL_poolmask(j)) then ! care only about poolmask'ed ones
            if (LL_mypool(j)) then ! Copy-to-myself
              CALL newio_put_incore32(khandle, j, incore(NHDR+1), nbytes, &
                                      nrows, ncols, CLtbl, iret)
              if (iret /= nbytes) then
                CALL io_error('***MSGPASS_LOADDATA(io_error): Unable to [1]put_incore() correct no. of bytes', &
                              iret, nbytes, j, filename)
              endif
            else
            if (LLiomsgpasstrace) &
                & write(0,'(i5,a,i5,a,2i12)') &
                & ODBMP_myproc,':LOAD: Sending to ',destPE,', tag,nw=',itag,nw
!J---Start----
!J              if (iwrite_empty == 2) then
!J                CALL mpl_send(incore(1:nw), KDEST=destPE, KTAG=itag, &
!J                   & CDSTRING='LOAD: Send data to data-owner PE; table=@'//trim(CLtbl))
!j              else
!J                CALL mpl_send(incore(1:nw), KDEST=destPE, KTAG=itag, &
!J                   & KMP_TYPE=imp_type, KREQUEST=ISENDREQ, &
!J                   & CDSTRING='LOAD: Send data to data-owner PE; table=@'//trim(CLtbl))
!J              end if
              if(msgoff+nw > njmax .or. msgsnd .eq. MMSGSND) then
                if(msgsnd > 0 ) then
!                   write(0,*)'msgsnd,MMSGSND',msgsnd,MMSGSND
                  CALL mpl_wait(jncore, KREQUEST=JSENDREQ(1:msgsnd),CDSTRING="MSGPASS_LOADDATA Wait")
                endif
                if(nw .gt. njmax) then
                  njmax=nw
                  CALL allocate_jncore(jncore, njmax, iret)
                endif
                msgsnd=0
                msgoff=0
              endif
              jncore(msgoff+1:msgoff+nw)=incore(1:nw)
              msgsnd=msgsnd+1

            itag = itags(j,jt)

              if (itag == -1) CALL ODB_abort('MSGPASS_LOADDATA', 'itag not set correctly for mpl_send ',iret)

              CALL mpl_send(jncore(msgoff+1:msgoff+nw), KDEST=destPE, KTAG=itag, &
                  & KMP_TYPE=imp_type, KREQUEST=JSENDREQ(msgsnd), &
                  & CDSTRING='LOAD: Send data to data-owner PE; table=@'//trim(CLtbl))
              msgoff=msgoff+nw

!J---End----
              if (LLiomsgpasstrace) &
                & write(0,'(i5,a,i5,a,2i12)') &
                & ODBMP_myproc,':LOAD: Sent to ',destPE,', tag,isendreq=',itag,isendreq
!J              if (iwrite_empty /=2 .and. imp_type == JP_NON_BLOCKING_STANDARD) CALL mpl_wait(incore, KREQUEST=ISENDREQ)
              if (LLiomsgpasstrace) &
                & write(0,'(i5,a,i12)') &
                & ODBMP_myproc,':LOAD: Wait finished on isendreq=',ISENDREQ
            endif
!            else
!              if (LL_mypool(j) .AND. LLiotrace) &
!                write(0,*) ODBMP_myproc,': Rejecting pool#',j,' due to permanent poolmask'
          endif
          iprev = ioaid(j,jt,IOAID_OFFSET)
        endif ! if (ioaid(j,jt,IOAID_LENGTH) > 0)

        j = j + 1
        jj = j
        if (j > npools) exit IO_LOOP

        if (LL_lastpool(j)) exit IO_LOOP

        if (ioaid(j,jt,IOAID_FBLOCK) /= ioaid(j-1,jt,IOAID_FBLOCK)) exit IO_LOOP

        if (ioaid(j,jt,IOAID_LENGTH) > 0 .AND. &
         & ioaid(j,jt,IOAID_OFFSET) <= iprev) exit IO_LOOP 
      ENDDO IO_LOOP

      CALL io_close(filename, io, nread, j-1, nfiles, datavolume, LLiotrace, iret)
      cycle LOAD_LOOP

    ELSEIF (LL_mypool(jj)) then
      j = jj
      pe = iope_map(j,jtact)
      nw = ioaid(j,jt,IOAID_LENGTH)/sizeof_int

      itag = itags(j,jt)

      if (LL_poolmask(j) .AND. nw > 0) then
        CALL allocate_incore(incore, nw + 1, iret)
        if (LLiomsgpasstrace) &
          & write(0,'(i5,a,i5,a,2i12)') &
          & ODBMP_myproc,':LOAD: Receiving from ',pe,', tag,nw=',itag,nw
        if (pe == -1) then ! "ANY"-source : at present KSOURCE-parameter must be left out
          if (itag == -1) CALL ODB_abort('MSGPASS_LOADDATA', 'itag not set correctly for mpl_recv ',iret)
          CALL mpl_recv(incore(1:nw), KTAG=itag, KOUNT=iret, &
           & KFROM=pe,   &! Upon return the "pe" contains the actual "from-PE"
           & CDSTRING='LOAD: Recv data from I/O-PE; table=@'//trim(CLtbl)) 
        else
          if (itag == -1) CALL ODB_abort('MSGPASS_LOADDATA', 'itag not set correctly for mpl_recv ',iret)
          CALL mpl_recv(incore(1:nw), KSOURCE=pe, KTAG=itag, KOUNT=iret, &
           & CDSTRING='LOAD: Recv data from I/O-PE; table=@'//trim(CLtbl)) 
        endif
        if (LLiomsgpasstrace) &
          & write(0,'(i5,a,i5,a,2i12)') &
          & ODBMP_myproc,':LOAD: Received from ',pe,', tag,iret=',itag,iret

        if (iret /= nw) then
          CALL io_error('***MSGPASS_LOADDATA(io_error): Unable to receive correct amount of data from I/O-PE', &
           & iret, nw, j, filename)
        endif

        nbytes = incore(3)
        nrows = incore(4)
        ncols = incore(5)
        CALL newio_put_incore32(khandle, j, incore(NHDR+1), nbytes, &
         & nrows, ncols, CLtbl, iret) 
        if (iret /= nbytes) then
          CALL io_error('***MSGPASS_LOADDATA(io_error): Unable to [2]put_incore() correct no. of bytes', &
                        iret, nbytes, j, filename)
        endif
!        else if (.not.LL_poolmask(j) .AND. nw > 0) then
!          if (LLiotrace) &
!            write(0,*) ODBMP_myproc,': Rejecting pool#',j,' due to permanent poolmask'
      endif ! if (LL_poolmask(j) .AND. nw > 0) else if ...
    endif ! if (iope_map(jj,jtact) == ODBMP_myproc) then ... else if (LL_mypool(jj)) ...

   endif ! if (LL_filemask(jj)) then ...
   jj = jj + 1
  enddo LOAD_LOOP
!J---Start----
  if(msgsnd > 0 ) then
    CALL mpl_wait(jncore, KREQUEST=JSENDREQ(1:msgsnd),CDSTRING="MSGPASS_LOADDATA Wait")
  endif
  msgsnd=0
  msgoff=0
!J---End----

  CALL io_close(filename, io, nread, npools, nfiles, datavolume, LLiotrace, iret)

  itables = itables + 1
enddo LOAD_TABLE

kret = itables ! No. of tables stored/loaded

datavolume(0) = nfiles
CALL ODBMP_global('SUM', datavolume, LDREPROD=.false., root=1)
nfiles = datavolume(0)

waltim(2) = util_walltime()

if (ODBMP_myproc == 1) then
  call date_and_time (values=itime)
  imsgpass_time=itime(5)*10000+itime(6)*100+itime(7)
  imsgpass_date=itime(1)*10000+itime(2)*100+itime(3)
  sum_datavolume = sum(datavolume(1:ODBMP_nproc))/(1024 * 1024)

  write(0,'(1x,a,i8.8,2x,i6.6,/, &
   &        1x,a,f8.1,a,i5,a,f7.2,a,f8.1,a,i3,a)') &
   & '=== MSGPASS_LOADDATA of db="'//trim(dbname)// &
   & '" for mode='//CLmode//' on all tables ended on ',imsgpass_date,imsgpass_time,&
   & '=== MSGPASS_LOADDATA : ',sum_datavolume,' MBytes (',&
   & nfiles,' files) in ',waltim(2) - waltim(1),&
   & ' secs (',sum_datavolume/(waltim(2) - waltim(1) + epsilon(waltim(1))),&
   & ' MB/s) (it#',it ,')'
endif

99999 continue

if (allocated(iope_map))           deallocate(iope_map)
if (allocated(LL_mypool))          deallocate(LL_mypool)
if (allocated(LL_include_tbl))     deallocate(LL_include_tbl)
if (associated(incore))             deallocate(incore)
!J---Start---
if (associated(jncore))             deallocate(jncore)
!J---End---
if (allocated(istat))              deallocate(istat)
if (allocated(data_bytes))         deallocate(data_bytes)
if (allocated(cltable))            deallocate(cltable)
if (allocated(LL_poolmask))        deallocate(LL_poolmask)
if (allocated(LL_filemask))        deallocate(LL_filemask)
if (allocated(LL_lastpool))        deallocate(LL_lastpool)
if (allocated(global_pool_offset)) deallocate(global_pool_offset)
if (allocated(itags))              deallocate(itags)

nullify(ioaid)
if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_LOADDATA : End'
IF (LHOOK) CALL DR_HOOK('MSGPASS_LOADDATA',1,ZHOOK_HANDLE)
      
END SUBROUTINE msgpass_loaddata

