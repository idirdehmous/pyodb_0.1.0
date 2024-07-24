SUBROUTINE msgpass_storedata(khandle, kret)

! A routine to exchange ODB-data between processors
! Called from module ODB: ODB_close()

! P. Marguinaud : 10-10-2013 : Nullify pointers

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
use oml_mod   ,only : oml_set_lock, oml_unset_lock, oml_my_thread
USE odb_module
USE mpl_module
USE odbio_msgpass
USE str       ,ONLY : sadjustl 

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
logical, allocatable            :: LL_mypool(:), LL_include_tbl(:), LL_write_tbl(:), LL_has_backup(:,:)
logical                         :: LL_readonly
logical                         :: LL_check_len
logical, save                   :: LL_first_time = .TRUE.
logical, save                   :: LL_write_empty = .FALSE.
INTEGER(KIND=JPIM), save        :: iwrite_empty = 0
INTEGER(KIND=JPIM), parameter   :: io_filesize_default = 32 ! in megabytes; see ../aux/newio.c, too
INTEGER(KIND=JPIM), save        :: io_filesize = 1
INTEGER(KIND=JPIM), save        :: io_grpsize = -1
INTEGER(KIND=JPIM), save        :: io_backup_enable = 0 ! By default do *NOT* enable back ups (thanks Bob C.!!)
INTEGER(KIND=JPIM), parameter   :: io_trace_default = 0
INTEGER(KIND=JPIM), save        :: io_trace = 0
INTEGER(KIND=JPIM), parameter   :: io_msgpass_trace_default = 0
INTEGER(KIND=JPIM), save        :: io_msgpass_trace = 0
logical, save                   :: LLiotrace = .FALSE.
logical, save                   :: LLiomsgpasstrace = .FALSE.
character(len=1), parameter     :: CLtab = char(9)
character(len=1)                :: CLmode='w', CLtmp
character(len=maxvarlen)        :: dbname, CLtbl, CLbytes
character(len=4096)             :: clenv, filename, CLdirname, CLbasename

INTEGER(KIND=JPIM), parameter   :: maxsize_mb = 1000 ! Maximum filesize in megabytes for HC32-file
INTEGER(KIND=JPIM)              :: nwrite, nfileno
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
INTEGER(KIND=JPIM)              :: MJNCORE = 20000000
INTEGER(KIND=JPIM)              :: msgoff, msgsnd, njmax, j1, j2, jreset
INTEGER(KIND=JPIM)              :: jsendreq(MMSGSND)
!J---End---
INTEGER(KIND=JPIM), save        :: hc32(0:1) = (/0,0/) ! "HC32" & its reverse/byteswap (to be filled) 
INTEGER(KIND=JPIM), parameter   :: reset_updated_flag = 1
INTEGER(KIND=JPIM), POINTER     :: ioaid(:,:,:) ! A shorthand notation
REAL(KIND=JPRB), allocatable    :: data_bytes(:) ! nactive_tables
REAL(KIND=JPRB)                 :: datavolume(0:ODBMP_nproc), waltim(2), sum_datavolume
INTEGER(KIND=JPIM)              :: nfiles, idummy_arr, npermcnt
INTEGER(KIND=JPIM), allocatable :: perm_list(:), global_pool_offset(:)
REAL(KIND=JPRB), external       :: util_walltime  ! now [23/1/04] from ifsaux/support/drhook.c
INTEGER(KIND=JPIM)              :: fast_physproc, xfast, nfblk, irefblk
INTEGER(KIND=JPIM)              :: imp_type
LOGICAL                         :: L2,L3,L4
REAL(KIND=JPRB)                 :: ZHOOK_HANDLE

real(KIND=JPRB)                 :: t0,timef,ttab,tloop,ttloop
real(KIND=JPRB)                 :: tprebar,tpostbar
real(KIND=JPRB), allocatable    :: ttwrite(:),ttsend(:),ttrecv(:),ttwait(:),ttclose(:)
real(KIND=JPRB), allocatable    :: tbar(:), ttable(:), ttcomm(:)
real(KIND=JPRB), allocatable    :: sendmb(:),recvmb(:)
integer(kind=JPIM), allocatable :: sendcnt(:),recvcnt(:)
integer(kind=JPIM)              :: JWRITE=0

fast_physproc(xfast) = mod(xfast-1,ODBMP_nproc)+1
IF (LHOOK) CALL DR_HOOK('MSGPASS_STOREDATA',0,ZHOOK_HANDLE)

incore => null ()
jncore => null ()
ioaid  => null ()

call MPL_barrier(cdstring='MSGPASS_STOREDATA')

if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_STOREDATA : Begin'
kret = 0
itables = 0
it = oml_my_thread()

if (.NOT.db(khandle)%inuse) goto 99999

io_method = db(khandle)%io_method
if (io_method /= 4) goto 99999 ! only applicable for ODB_IO_METHOD=4

LL_readonly = db(khandle)%readonly

if (LL_readonly) goto 99999

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
  '=== MSGPASS_STOREDATA of db="'//trim(dbname)// &
  '" for mode='//CLmode//' started on ',imsgpass_date,imsgpass_time
endif

datavolume(:) = 0
nfiles = 0 ! no. of opened files on this PE

if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_STOREDATA ==='
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
  CALL util_igetenv('ODB_IO_BACKUP_ENABLE', 0, io_backup_enable)
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
    write(0,*)' ODB_IO_BACKUP_ENABLE=',io_backup_enable
  endif
  LL_first_time = .FALSE.
!$OMP FLUSH(LL_first_time)
 endif
!$ CALL OML_UNSET_LOCK()
endif

if (ODBMP_myproc == 1) then
  write(0,*) ODBMP_myproc,&
   & ': MSGPASS_STOREDATA() khandle=',&
   & khandle
endif

npools = db(khandle)%glbNpools
this_io_grpsize = min(npools,io_grpsize) 

!if (ODBMP_myproc == 1) write(0,*) 'this_io_grpsize, npools=',&
!                                   this_io_grpsize, npools

if (ODBMP_myproc == 1) then
  write(0,'(1x,a,i5,a,i5,a)') &
    & 'MSGPASS_STOREDATA: All pools in range [1..npools] will processed'
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
allocate(LL_write_tbl(ntables))

if(JWRITE>0) then
  allocate(ttable(ntables))
  allocate(tbar(ntables))
  allocate(ttwrite(ntables))
  allocate(ttsend(ntables))
  allocate(ttrecv(ntables))
  allocate(ttwait(ntables))
  allocate(ttclose(ntables))
  allocate(ttcomm(ntables))
  allocate(sendmb(ntables))
  allocate(recvmb(ntables))
  allocate(sendcnt(ntables))
  allocate(recvcnt(ntables))
endif

do j=1,npools
  pe = fast_physproc(j)
  LL_mypool(j) = (pe == ODBMP_myproc)
enddo

!-- New approach (5-Sep-2006/SS)
do jt=1,ntables
  CALL cODB_table_is_considered(cltable(jt), i_consider)
  LL_include_tbl(jt) = (i_consider == 1)
enddo

!-- Anne Fouilloux (23/02/10)
do jt=1,ntables
  CALL cODB_table_is_writable(cltable(jt), i_write)
  LL_write_tbl(jt) = (i_write == 1)
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

if (nactive_tables <= 0) then
  if (ODBMP_myproc == 1) write(0,*)'***INFO: No active tables'
  kret = 0
  goto 99999
endif

allocate(istat(nstat,npools,nactive_tables))

allocate(iope_map(npools+1,1))
if (io_backup_enable /= 0) then
  allocate(LL_has_backup(npools,ntables))
  LL_has_backup(:,:) = .FALSE.
endif

allocate(data_bytes(nactive_tables))

!-- Get current status of incore items
istat(:,:,:) = 0
jtact = 0
STAT_LOOP: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle STAT_LOOP 
  if (.not.LL_write_tbl(jt)) cycle STAT_LOOP  ! do not proceed if the table is not writable (store only)
  jtact = jtact + 1

!-- Strip off the leading '@'-character
  CLtbl = cltable(jt)(2:)

!-- Gather size information of incore items for memory allocations
  do j=1,npools
    if (LL_mypool(j)) then
      CALL newio_status_incore32(khandle, j, istat(1,j,jtact), nstat, CLtbl, iret)
!*AF      write(0,*) 'msgpass_storedata table = ', trim(CLtbl), ' pool = ', j, ' nrows = ', istat(2,j,jtact), ' ncols = ', istat(3,j,jtact), &
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
! msgsnd = 0
! msgoff = 0
!J---End---



!-- Predefine message tags for all used table,pools combinations 
!  (global not local - as need to tags of incoming messages from other tasks)
! Loop through all table/pool combinations and determine if a message will need to be sent,
!   if so, then define a tag for the send/recv to use. 
allocate(itags(npools+1,ntables))
itags(:,:) = -1
itag = 0
jtact =0
TABLE_LOOP: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle TABLE_LOOP
  if (.not.LL_write_tbl(jt)) cycle TABLE_LOOP ! do not proceed if table is not writable
  jtact = jtact + 1

  if (MAXVAL(istat(ISTAT_UPDATED,1:npools,jtact)) <= 0) then
    cycle TABLE_LOOP
  endif

  maxbytes = MAXVAL(istat(ISTAT_NBYTES,1:npools,jtact))
  if (.not. LL_write_empty .AND. maxbytes <= 0) then
    cycle TABLE_LOOP
  endif

  j2=0
  jreset=0
  STORE_WHILE0: do while (j2.lt.npools)
   j1=j2+1
   STORE_SEND_LOOP0: do j=j1,npools    ! Send Loop

    nbytes = istat(ISTAT_NBYTES,j,jtact)
    nrows = istat(ISTAT_NROWS,j,jtact)
    if ((nbytes > 0 .and. (iwrite_empty /= 2 .or. nrows > 0)) .OR. (nbytes <= 0 .AND. LL_write_empty)) then
   itags(j,jt) = itag
        itag = itag + 1
    endif
    j2=j  ! Last j successfully completed
   enddo STORE_SEND_LOOP0
  enddo STORE_WHILE0
enddo TABLE_LOOP

!-- A procedure for storing data to disk (horizontal concatenation; io_method=4)
jtact = 0
if(JWRITE>0) then
  ttloop=0.d0
  ttwrite=0.d0
  ttrecv=0.d0
  ttsend=0.d0
  ttwait=0.d0
  ttclose=0.d0
  sendmb=0.d0
  recvmb=0.d0
  sendcnt=0
  recvcnt=0
  call MPL_BARRIER()
  ttab=timef()
  tpostbar=timef()
endif

STORE_TABLE: do jt=1,ntables
  if(JWRITE>0) then
    tprebar=timef()
    ttable(jt-1)=tprebar-tpostbar
    call MPL_BARRIER()
    tpostbar=timef()
    tbar(jt-1)=tpostbar-tprebar
  endif
  if (.not.LL_include_tbl(jt)) cycle STORE_TABLE
  if (.not.LL_include_tbl(jt)) cycle STORE_TABLE
  if (.not.LL_write_tbl(jt)) cycle STORE_TABLE ! do not proceed if table is not writable
  jtact = jtact + 1

!-- Strip off the leading '@'-character
  CLtbl = cltable(jt)(2:)
  if(JWRITE>0) write(0,*) "JJJ jt, TABLE =",jt,trim(CLtbl) 

!-- No updates found ==> bail out & continue with the next table
  if (MAXVAL(istat(ISTAT_UPDATED,1:npools,jtact)) <= 0) then
!   if (ODBMP_myproc == 1) write(0,*) &
!    & '***Warning: STOREing table="'//trim(CLtbl)//'" skipped since not updated' 
    cycle STORE_TABLE
  endif

!-- To start with : *rename* any existing table-files into backup file
  do j=1,npools
!! The following call to codb_remove_tablefile() has been removed since
!! if job finished/aborted before updated version has been written to disk
!! the whole database would be corrupt (SS/4-Jul-2003)
!! Instead, backup files will be created and then removed at the very end
!!      if (LL_mypool(j)) CALL codb_remove_tablefile(dbname, CLtbl, j, iret)
    if (io_backup_enable /= 0) then
      if (LL_mypool(j)) then ! More failsafe
        CALL makefilename(dbname, CLtbl, j, filename)
        CALL true_dirname_and_basename(trim(filename), CLdirname, CLbasename)
        CALL codb_rename_file(trim(filename), trim(CLdirname)//'/'//trim(CLbasename)//'.BACKUP', iret)
        LL_has_backup(j,jt) = (iret == 0)
      endif
    endif
    ioaid(j,jt,IOAID_FILENO) = 0
    ioaid(j,jt,IOAID_OFFSET) = 0
    ioaid(j,jt,IOAID_LENGTH) = 0
    if (ioaid(j,jt,IOAID_GRPSIZE) == 0) ioaid(j,jt,IOAID_GRPSIZE) = this_io_grpsize
    ioaid(j,jt,IOAID_NROWS)  = 0
  enddo

!-- If no. of bytes to be stored never > 0, then bail out & continue with the next table
  maxbytes = MAXVAL(istat(ISTAT_NBYTES,1:npools,jtact))
  if (.not. LL_write_empty .AND. maxbytes <= 0) then
!     if (ODBMP_myproc == 1) write(0,*) &
!      & '***Warning: STOREing table="'//trim(CLtbl)//'" skipped since empty' 
    cycle STORE_TABLE
  endif

!-- Allocate data buffer
  nwmax = NHDR + max(1,(maxbytes + sizeof_int - 1)/sizeof_int)
  CALL allocate_incore(incore, nwmax, iret)
!J---Start---
  if(nwmax .gt. njmax) then
    njmax=nwmax
    CALL allocate_jncore(jncore, njmax, iret)
  endif 
!J---End---

!-- Determine I/O-PEs
  CALL make_iopes(iope_map, jtact, io_filesize, this_io_grpsize, npools, global_pool_offset, istat)

!-- Gather total no. of bytes information in advance
  data_bytes(jtact) = 0
  do j=1,npools
    nw = 0
    cur_io_grpsize = ioaid(j,jt,IOAID_GRPSIZE)
    nbytes = istat(ISTAT_NBYTES,j,jtact)
    nrows = istat(ISTAT_NROWS,j,jtact)
!*AF  if ODB_WRITE_EMPTY_FILES=2 we do not really write...
    if ((nbytes > 0 .and. (iwrite_empty /= 2 .or. nrows > 0)) .OR. (nbytes <= 0 .AND. LL_write_empty)) then
      nw = NHDR + (nbytes + sizeof_int - 1)/sizeof_int
    endif
    if (iope_map(j,1) /= iope_map(j+1,1) .OR. &
       & mod(j-global_pool_offset(j), cur_io_grpsize) == 0 .OR. &
       & j == npools) then 
      !-- Account for yet another file ending and allocate space for "HC32" 
      !   i.e. 4 bytes which was so far unaccounted for when file was "opened"
      nw = nw + 1
    endif
    data_bytes(jtact) = data_bytes(jtact) + nw * sizeof_int
  enddo

  if (ODBMP_myproc == 1) then
    write(CLbytes,'(f20.1)') data_bytes(jtact)
    jj = index(CLbytes,'.')
    if (jj > 0) CLbytes(jj:) = ' '
    write(0,'(1x,a)')'STOREing table="'//trim(CLtbl)//'" total = '//&
     & trim(CLbytes)//' bytes' 
  endif

!-- Worker-loop: send/recv data and perform write-I/O
  isendreq=0
  nwrite = 0
  nfileno = 0
  io = -1
  CALL codb_strblank(filename)

  if(JWRITE>1)  then
    if(trim(CLtbl).eq."body") then
      do j=1,npools
        write(0,'(a,a,a,3i4)') "JJJ CLtbl,j,pe,iope",trim(CLtbl), &
          & " ",j,fast_physproc(j),fast_physproc(iope_map(j,1))
      enddo
    endif
  endif
  if(JWRITE>0) tloop=timef()
!J---Start---
!-- Initialisation for NON BLOCKING message passing------------
  msgsnd = 0
  msgoff = 0
  j2=0
  jreset=0
  STORE_WHILE: do while (j2.lt.npools)
   j1=j2+1
!  STORE_SEND_LOOP: do j=1,npools    ! Send Loop
   STORE_SEND_LOOP: do j=j1,npools    ! Send Loop
!J---End---

    itag = itags(j,jt)

    iope = fast_physproc(iope_map(j,1))
    pe = fast_physproc(j)
    nw = 0
    cur_io_grpsize = ioaid(j,jt,IOAID_GRPSIZE)
    nbytes = istat(ISTAT_NBYTES,j,jtact)
    nrows = istat(ISTAT_NROWS,j,jtact)
!*AF  if ODB_WRITE_EMPTY_FILES=2 we do not really write...
    if ((nbytes > 0 .and. (iwrite_empty /= 2 .or. nrows > 0)) .OR. (nbytes <= 0 .AND. LL_write_empty)) then
!*AF        write(0,*) 'pool = ', j, ' LL_mypool(j) = ', LL_mypool(j), ' nrows = ', nrows, iwrite_empty, nbytes
      if (LL_mypool(j)) then ! This is my pool
        nrows = istat(ISTAT_NROWS,j,jtact)
        ncols = istat(ISTAT_NCOLS,j,jtact)
        npad = 1
        nw = NHDR + (nbytes + sizeof_int - 1)/sizeof_int
        if (mod(nbytes,sizeof_int) /= 0) incore(nw) = 0 ! make sure the last word is FULLY zeroed first
        ipoolno = mod(j-1-global_pool_offset(j),cur_io_grpsize) + 1 ! Pool# relative to the beginning of this IO_GRP
        incore(1) = ipoolno
        incore(2) = cur_io_grpsize
        incore(3) = nbytes
        incore(4) = nrows
        incore(5) = ncols
        incore(6) = npad ! Data padded to the nearest INTEGER*4 word boundary
        CALL newio_get_incore32(khandle, j, incore(NHDR + 1), maxbytes, &
         & reset_updated_flag, CLtbl, iret) 
        if (iret /= nbytes) then
          CALL io_error('***MSGPASS_STOREDATA(io_error): Unable to get_incore() correct no. of bytes', iret, nbytes, j, &
           & filename, kdata=incore(1:NHDR)) 
        endif

        nw = NHDR + (nbytes + sizeof_int - 1)/sizeof_int

!J      if (iope /= ODBMP_myproc) then ! I am *not* an I/O-PE ==> send to the I/O-PE
                                       ! Send always
          destPE = iope
          if (LLiomsgpasstrace) &
            & write(0,'(i5,a,i5,a,2i12)') &
            & ODBMP_myproc,':STORE: Sending to ',destPE,', tag,nw=',itag,nw
          if(msgoff+nw > njmax .or. msgsnd .eq. MMSGSND) then
            if(j>j1) then
              jreset=jreset+1
              if(JWRITE>0) write(0,*) "JJJ Reset jncore: jreset,jt,j,j1,msgoff,nw,MJNCORE,msgsnd,MSGSND=", &
                 & jreset,jt,j,j1,msgoff,nw,njmax,msgsnd,MMSGSND
              EXIT
            else
              if(msgsnd .gt. 0) then
                if(JWRITE>0) t0=timef()
                  CALL mpl_wait(jncore, KREQUEST=JSENDREQ(1:msgsnd),CDSTRING="MSGPASS_STOREDATA Wait")
                if(JWRITE>0) ttwait(jt)=ttwait(jt)+timef()-t0
              endif
              msgsnd=0
              msgoff=0
            endif
          endif
          msgsnd=msgsnd+1
          jncore(msgoff+1:msgoff+nw)=incore(1:nw)
          if(JWRITE>0) then
            t0=timef()
            sendmb(jt)=sendmb(jt)+4.0d-6*nw
            sendcnt(jt)=sendcnt(jt)+1
          endif
          if (itag == -1) CALL ODB_abort('MSGPASS_STOREDATA', 'Aborting : itag not set correctly for mpl_send ',iret)
          CALL mpl_send(jncore(msgoff+1:msgoff+nw), KDEST=destPE, KTAG=itag, &
             & KMP_TYPE=imp_type,KREQUEST=JSENDREQ(msgsnd), &
             & CDSTRING='STORE: Send data to I/O-PE; table=@'//trim(CLtbl)) 
          if(JWRITE>0) ttsend(jt)=ttsend(jt)+timef()-t0
          msgoff=msgoff+nw
          if (LLiomsgpasstrace) &
            & write(0,'(i5,a,i5,a,2i12)') &
            & ODBMP_myproc,':STORE: Sent to ',destPE,', tag,isendreq=',itag,isendreq
!J          if (iwrite_empty /= 2 .and. imp_type == JP_NON_BLOCKING_STANDARD) CALL mpl_wait(incore, KREQUEST=ISENDREQ)
!J      endif  ! Am sending always
      endif
    endif ! if (nbytes > 0 .OR. (nbytes <= 0 .AND. ...
    j2=j  ! Last j successfully completed
  enddo STORE_SEND_LOOP

  if(JWRITE>0) then
    if(jreset == 0)  then
      call mpl_barrier()
      write(0,*) "JJJ Calling Barrier: jt=",jt
    endif
  endif

! STORE_RECV_LOOP: do j=1,npools ! Recv loop
  STORE_RECV_LOOP: do j=j1,j2                                ! Recv

    itag = itags(j,jt)

    iope = fast_physproc(iope_map(j,1))
    pe = fast_physproc(j)
    nw = 0
    cur_io_grpsize = ioaid(j,jt,IOAID_GRPSIZE)
    nbytes = istat(ISTAT_NBYTES,j,jtact)
    nrows = istat(ISTAT_NROWS,j,jtact)
!*AF  if ODB_WRITE_EMPTY_FILES=2 we do not really write...
    if ((nbytes > 0 .and. (iwrite_empty /= 2 .or. nrows > 0)) .OR. (nbytes <= 0 .AND. LL_write_empty)) then
!*AF        write(0,*) 'pool = ', j, ' LL_mypool(j) = ', LL_mypool(j), ' nrows = ', nrows, iwrite_empty, nbytes
      if (iope == ODBMP_myproc) then
        if (LLiomsgpasstrace) &
          & write(0,'(i5,a,i5,a,2i12)') &
          & ODBMP_myproc,':STORE: Receiving from ',pe,', tag,nw(max)=',itag,size(incore)
        if(JWRITE>0) t0=timef()
        if (itag == -1) CALL ODB_abort('MSGPASS_STOREDATA', 'Aborting: itag not set correctly for mpl_recv ',iret)
        CALL mpl_recv(incore, KSOURCE=pe, KTAG=itag, KOUNT=nw, &
         & CDSTRING='STORE: Recv data for I/O-PE; table=@'//trim(CLtbl)) 
        if(JWRITE>0) then
          ttrecv(jt)=ttrecv(jt)+timef()-t0
          recvmb(jt)=recvmb(jt)+4.0d-6*nw
          recvcnt(jt)=recvcnt(jt)+1
        endif
        if (LLiomsgpasstrace) &
          & write(0,'(i5,a,i5,a,2i12)') &
          & ODBMP_myproc,':STORE: Received from ',pe,', tag,nw=',itag,nw
      endif
    endif ! if (nbytes > 0 .OR. (nbytes <= 0 .AND. ...

    if (nw > 0 .AND. iope == ODBMP_myproc) then 
      !-- I am an I/O-PE, but not necessarely the owner of the pool;
      !   however, I know the file offsets & lengths, so I should record them in ioaid

      CALL io_checkopen(dbname, filename, CLtbl, io, j, nwrite, cur_io_grpsize, LDread=.FALSE., &
                        LLiotrace=LLiotrace, hc32=hc32, global_pool_offset=global_pool_offset, &
                        kfileno=nfileno, kret=iret)
      nrows = incore(4)
      ncols = incore(5)

      if(JWRITE>1) write(0,*) "JJJ write bytes,filename=",4*nw,trim(filename)
      if(JWRITE>0) t0=timef()
      CALL cma_writei(io, incore(1), NHDR, iret) ! writeI; prepare for be2le conversion
      if (iret /= NHDR) CALL io_error('***MSGPASS_STOREDATA(io_error): Unable to write data header', iret, NHDR, j, &
       & filename, kdata=incore(1:NHDR)) 
  
      CALL cma_writeb(io, incore(NHDR+1), (nw - NHDR) * sizeof_int, iret) ! writeB; raw data xfer
      if (iret /= (nw - NHDR) * sizeof_int) then
        CALL io_error('***MSGPASS_STOREDATA(io_error): Unable to write actual data', iret, &
                      (nw - NHDR) * sizeof_int, j, filename)
      endif
      if(JWRITE>0) ttwrite(jt)=ttwrite(jt)+timef()-t0

      ioaid(j,jt,IOAID_FILENO) = nfileno
      ioaid(j,jt,IOAID_OFFSET) = nwrite
      ioaid(j,jt,IOAID_LENGTH) = nw * sizeof_int
      ioaid(j,jt,IOAID_NROWS)  = nrows
      ioaid(j,jt,IOAID_NCOLS)  = ncols

      nwrite = nwrite + nw * sizeof_int
    endif

    if (io /= -1 .AND. &
       & (iope_map(j,1) /= iope_map(j+1,1) .OR. &
       & mod(j-global_pool_offset(j), cur_io_grpsize) == 0)) then 
      if(JWRITE>0) write(0,*) "JJJ close table,filename=",trim(CLtbl)," ",trim(filename)
      if(JWRITE>0) t0=timef()
      CALL io_close(filename, io, nwrite, j, nfiles, datavolume, LLiotrace, iret)
      if(JWRITE>0) ttclose(jt)=ttclose(jt)+timef()-t0
    endif
   enddo STORE_RECV_LOOP
   if(JWRITE>0) ttloop=ttloop+timef()-tloop
!J---Start----
  enddo STORE_WHILE
  if(msgsnd.ge.1) then
    if(JWRITE>0) t0=timef()
    CALL mpl_wait(jncore, KREQUEST=JSENDREQ(1:msgsnd),CDSTRING="MSGPASS_STOREDATA Wait")
    if(JWRITE>0) ttwait(jt)=ttwait(jt)+timef()-t0
    msgsnd=0
    msgoff=0
  endif
!J---End----

  if(JWRITE>0) write(0,*) "JJJ close table,filename=",trim(CLtbl)," ",trim(filename)
  if(JWRITE>0) t0=timef()
  CALL io_close(filename, io, nwrite, npools, nfiles, datavolume, LLiotrace, iret)
  if(JWRITE>0) ttclose(jt)=ttclose(jt)+timef()-t0

  itables = itables + 1
enddo STORE_TABLE

if(JWRITE>0) then
  ttable(ntables)=timef()-tpostbar
  tprebar=timef()
  call mpl_barrier()
  tbar(ntables)=timef()-tprebar
  ttab=timef()-ttab
  do jt=1,ntables
    ttcomm(jt)=ttsend(jt)+ttrecv(jt)+ttwait(jt)
  enddo
  write(0,'(a,10f10.0)') "JJJ time tab,loop=",ttab,ttloop
  write(0,'(a,50f9.0)') "JJJ send MB     =",(sendmb(jt),jt=1,ntables)
  write(0,'(a,50i9)')   "JJJ send count  =",(sendcnt(jt),jt=1,ntables)
  write(0,'(a,50f9.0)') "JJJ send time   =",(ttsend(jt),jt=1,ntables)
  write(0,'(a,50f9.0)') "JJJ wait time   =",(ttwait(jt),jt=1,ntables)
  write(0,'(a,50f9.0)') "JJJ close time  =",(ttclose(jt),jt=1,ntables)
  write(0,'(a,50f9.0)') "JJJ recv MB     =",(recvmb(jt),jt=1,ntables)
  write(0,'(a,50i9)')   "JJJ recv count  =",(recvcnt(jt),jt=1,ntables)
  write(0,'(a,50f9.0)') "JJJ recv time   =",(ttrecv(jt),jt=1,ntables)
  write(0,'(a,50f9.0)') "JJJ comm time   =",(ttcomm(jt),jt=1,ntables)
  write(0,'(a,50f9.0)') "JJJ write time  =",(ttwrite(jt),jt=1,ntables)
  write(0,'(a,50f9.0)') "JJJ table time  =",(ttable(jt),jt=1,ntables)
  write(0,'(a,50f9.0)') "JJJ barier time =",(tbar(jt),jt=1,ntables)
  deallocate(ttable,tbar,ttcomm)
  deallocate(ttwrite,ttsend,ttrecv,ttwait,ttclose,sendmb,recvmb,sendcnt,recvcnt)
endif

kret = itables ! No. of tables stored/loaded

CALL comm_iomap(khandle, 2, iret) ! Propagate up to date IOAID's

!-- Remove backup files where created
jtact = 0
REMOVE_BACKUPS: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle REMOVE_BACKUPS
  if (.not.LL_write_tbl(jt)) cycle REMOVE_BACKUPS ! do not proceed if table is not writable
  jtact = jtact + 1

!-- Strip off the leading '@'-character
  CLtbl = cltable(jt)(2:)

  if (io_backup_enable /= 0) then
    do j=1,npools
      if (LL_has_backup(j,jt)) then
        CALL makefilename(dbname, CLtbl, j, filename)
        CALL true_dirname_and_basename(trim(filename), CLdirname, CLbasename)
        CALL codb_remove_file(trim(CLdirname)//'/'//trim(CLbasename)//'.BACKUP', iret)
      endif
    enddo
  endif
enddo REMOVE_BACKUPS

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
   & '=== MSGPASS_STOREDATA of db="'//trim(dbname)// &
   & '" for mode='//CLmode//' on all tables ended on ',imsgpass_date,imsgpass_time,&
   & '=== MSGPASS_STOREDATA : ',sum_datavolume,' MBytes (',&
   & nfiles,' files) in ',waltim(2) - waltim(1),&
   & ' secs (',sum_datavolume/(waltim(2) - waltim(1) + epsilon(waltim(1))),&
   & ' MB/s) (it#',it ,')'
endif

99999 continue


if (allocated(iope_map))           deallocate(iope_map)
if (allocated(LL_has_backup))      deallocate(LL_has_backup)
if (allocated(LL_mypool))          deallocate(LL_mypool)
if (allocated(LL_include_tbl))     deallocate(LL_include_tbl)
if (allocated(LL_write_tbl))     deallocate(LL_write_tbl)
if (associated(incore))             deallocate(incore)
!J---Start---
if (associated(jncore))             deallocate(jncore)
!J---End---
if (allocated(istat))              deallocate(istat)
if (allocated(data_bytes))         deallocate(data_bytes)
if (allocated(cltable))            deallocate(cltable)
if (allocated(global_pool_offset)) deallocate(global_pool_offset)
if (allocated(itags))              deallocate(itags)

nullify(ioaid)
if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_STOREDATA : End'
IF (LHOOK) CALL DR_HOOK('MSGPASS_STOREDATA',1,ZHOOK_HANDLE)

END SUBROUTINE msgpass_storedata

