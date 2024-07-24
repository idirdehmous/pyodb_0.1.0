MODULE odbio_msgpass
! A module used by both msgpass_loaddata and msgpass_storedata 

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
USE odb_module
USE MPL_MODULE
use oml_mod, only : oml_set_lock, oml_unset_lock, oml_my_thread

IMPLICIT NONE

SAVE
PUBLIC
INTEGER(KIND=JPIM), parameter   :: nstat = 4
INTEGER(KIND=JPIM), parameter   :: ISTAT_NBYTES  = 1
INTEGER(KIND=JPIM), parameter   :: ISTAT_NROWS   = 2
INTEGER(KIND=JPIM), parameter   :: ISTAT_NCOLS   = 3
INTEGER(KIND=JPIM), parameter   :: ISTAT_UPDATED = 4

INTEGER(KIND=JPIM), parameter   :: sizeof_int = ODB_SIZEOF_INT

CONTAINS




SUBROUTINE io_error(cdmsg, krc, kexpected, kpoolno, filename, kdata)

IMPLICIT NONE
character(len=*), intent(in)            :: cdmsg
INTEGER(KIND=JPIM), intent(in)          :: krc, kexpected, kpoolno
INTEGER(KIND=JPIM), intent(in),optional :: kdata(:)
INTEGER(KIND=JPIM)                      :: isize, ilen
character(len=4096)                     :: filename
REAL(KIND=JPRB)                         :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('IO_ERROR',0,ZHOOK_HANDLE)
write(0,*) ODBMP_myproc,': '//cdmsg
ilen = len_trim(filename)
if (ilen > 0) write(0,*) ODBMP_myproc,': filename="'//filename(1:ilen)//'"'
write(0,*) ODBMP_myproc,': krc, kexpected, kpoolno=',krc, kexpected, kpoolno
if (present(kdata)) then
  isize = size(kdata)
  if (isize > 0) then
    write(0,*) ODBMP_myproc,': kdata(1:',isize,')=',kdata(:)
  endif
endif
CALL ODB_abort('ODBIO_MSGPASS(io_error)', cdmsg, krc)
IF (LHOOK) CALL DR_HOOK('IO_ERROR',1,ZHOOK_HANDLE)
END SUBROUTINE io_error





SUBROUTINE allocate_incore(incore, kw, kretcode)

IMPLICIT NONE
INTEGER(KIND=JPIM), intent(in)               :: kw
INTEGER(KIND=JPIM), intent(out)              :: kretcode
! Can't have INTENT on NEC for POINTER
! INTEGER(KIND=JPIM), pointer, intent(out)   :: incore(:)
INTEGER(KIND=JPIM), pointer                  :: incore(:)

INTEGER(KIND=JPIM)                           :: isize
REAL(KIND=JPRB)                              :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('ALLOCATE_INCORE',0,ZHOOK_HANDLE)
isize = 0
if (associated(incore)) isize = size(incore)
if (isize == 0 .OR. isize < kw) then
  if (associated(incore)) deallocate(incore)
  allocate(incore(kw))
  isize = size(incore)
endif
kretcode = isize
IF (LHOOK) CALL DR_HOOK('ALLOCATE_INCORE',1,ZHOOK_HANDLE)
END SUBROUTINE allocate_incore





!J---Start----
SUBROUTINE allocate_jncore(jncore, kw, kretcode)

IMPLICIT NONE
INTEGER(KIND=JPIM), intent(in)               :: kw
INTEGER(KIND=JPIM), intent(out)              :: kretcode
! Can't have INTENT on NEC for POINTER
! INTEGER(KIND=JPIM), pointer, intent(out) :: jncore(:)
INTEGER(KIND=JPIM), pointer :: jncore(:)

INTEGER(KIND=JPIM)                           :: isize
REAL(KIND=JPRB)                              :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('ALLOCATE_JNCORE',0,ZHOOK_HANDLE)
isize = 0
if (associated(jncore)) isize = size(jncore)
if (isize == 0 .OR. isize < kw) then
  if (associated(jncore)) deallocate(jncore)
  allocate(jncore(kw))
  isize = size(jncore)
endif
kretcode = isize
IF (LHOOK) CALL DR_HOOK('ALLOCATE_JNCORE',1,ZHOOK_HANDLE)
END SUBROUTINE allocate_jncore
!J---End----





SUBROUTINE make_iopes(iope_map, kjtact, kfilesize, kgrpsize, knpools, &
                      global_pool_offset, istat)

IMPLICIT NONE
INTEGER(KIND=JPIM), intent(in) :: kfilesize  ! suggested max filesize ; in megabytes
INTEGER(KIND=JPIM), intent(in) :: kgrpsize   ! max. no. of consecutive PEs' data, that
                                             ! can be concatenated into one file
INTEGER(KIND=JPIM), intent(in) :: knpools    ! no. of pools
INTEGER(KIND=JPIM), intent(in) :: kjtact      
INTEGER(KIND=JPIM), intent(out):: iope_map(:,:)
INTEGER(KIND=JPIM), intent(in) :: istat(:,:,:) ! nstat x npools x nactive_tables
INTEGER(KIND=JPIM), intent(in) :: global_pool_offset(:)
INTEGER(KIND=JPIM)             :: pe, jm
REAL(KIND=JPRB)                :: rfsize, rsum
REAL(KIND=JPRB)                :: ZHOOK_HANDLE
integer(kind=jpim), save       :: istart = 0
!-- threshold filesize in bytes

IF (LHOOK) CALL DR_HOOK('MAKE_IOPES',0,ZHOOK_HANDLE)
rfsize = kfilesize
rfsize = rfsize * (1024 * 1024)
iope_map(:,1) = 0
istart = mod(istart,kgrpsize)
pe = 1+istart
istart=istart+1
rsum = 0.0_JPRB
do jm=1,knpools
  rsum = rsum + istat(ISTAT_NBYTES,jm,kjtact)
  if (mod(jm-global_pool_offset(jm),kgrpsize) == 1 .OR.  &! groupsize limit exceeded
     & rsum > rfsize) then         ! total filesize reached threshold 
    pe = pe+1
    rsum = istat(ISTAT_NBYTES,jm,kjtact)
  endif
  iope_map(jm,1) = pe  ! ... actually the poolno, where the I/O starts / switches over ;
                       ! before use the iope_map(jm,1) is converted to iope via fastphysproc()
enddo
IF (LHOOK) CALL DR_HOOK('MAKE_IOPES',1,ZHOOK_HANDLE)
END SUBROUTINE make_iopes





SUBROUTINE makefilename(CD_dbname, CD_table, kpool, CD_filename)

IMPLICIT NONE
character(len=*), intent(in)  :: CD_dbname, CD_table
INTEGER(KIND=JPIM), intent(in)         :: kpool
character(len=*), intent(out) :: CD_filename
character(len=20) ::  CLpool
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('MAKEFILENAME',0,ZHOOK_HANDLE)
write(CLpool,*) kpool
CD_filename = trim(CD_dbname)//'.'//trim(CD_table)//'.'//trim(adjustl(CLpool))
IF (LHOOK) CALL DR_HOOK('MAKEFILENAME',1,ZHOOK_HANDLE)
END SUBROUTINE makefilename





SUBROUTINE true_dirname_and_basename(CD_filein, CD_dirname, CD_basename)

IMPLICIT NONE
character(len=*), intent(in)  :: CD_filein
character(len=*), intent(out) :: CD_dirname, CD_basename
character(len=len(CD_dirname) + len(CD_basename)) CLname
INTEGER(KIND=JPIM) :: ipos, iretcode
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('TRUE_DIRNAME_AND_BASENAME',0,ZHOOK_HANDLE)
CALL codb_truename(CD_filein, CLname, iretcode)
ipos = SCAN(CLname(1:iretcode),'/',back=.TRUE.) ! Get the last '/' (if any)
if (ipos <= 0) then
  CD_dirname = '.'
  CD_basename = CLname(1:iretcode)
else
  CD_dirname = CLname(1:ipos-1)
  CD_basename = CLname(ipos+1:iretcode)
endif
IF (LHOOK) CALL DR_HOOK('TRUE_DIRNAME_AND_BASENAME',1,ZHOOK_HANDLE)
END SUBROUTINE true_dirname_and_basename





SUBROUTINE io_checkopen(CD_dbname, CD_filename, CLtbl, kio, kpool, kupdval, &
                        kgrpsize, LDread, LLiotrace, hc32, global_pool_offset, &
                        kfileno, kret)

IMPLICIT NONE
CHARACTER(len=*), intent(in)      :: CD_dbname, CLtbl
CHARACTER(len=*), intent(out)     :: CD_filename
INTEGER(KIND=JPIM), intent(inout) :: kio, kupdval
INTEGER(KIND=JPIM), intent(out)   :: kfileno, kret
INTEGER(KIND=JPIM), intent(in)    :: kpool, kgrpsize
INTEGER(KIND=JPIM), intent(in)    :: global_pool_offset(:)
LOGICAL, intent(in)               :: LDread, LLiotrace
CHARACTER(len=maxvarlen)          :: CLpool
INTEGER(KIND=JPIM)                :: icat
INTEGER(KIND=JPIM), intent(in)    :: hc32(0:1)  ! "HC32" & its reverse/byteswap 

REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('IO_CHECKOPEN',0,ZHOOK_HANDLE)

if (kio == -1) then
  CALL makefilename(CD_dbname, CLtbl, kpool, CD_filename)
  
  if (LDread) then
    
    if (LLiotrace) write(0,*) ODBMP_myproc,': Opening file="'//trim(CD_filename)//'" for reading'
    if (LLiotrace) write(0,*) ODBMP_myproc,': kpool, kgrpsize=',kpool, kgrpsize
    CALL cma_open(kio, trim(CD_filename), "r", kret)
    kupdval = 0
    if (kret /= 1) CALL io_error('Unable to open file for reading', kret, 1, kpool, CD_filename)
    
    CALL cma_readi(kio, icat, 1, kret) ! readI; prepare for be2le conversion
    if (kret /= 1) CALL io_error('Unable to read file-format entry', kret, 1, kpool, CD_filename)
    if (icat /= hc32(0) .and. icat /= hc32(1)) then
      CALL io_error('Invalid file format', icat, hc32(0), kpool, CD_filename, hc32)
    else if (icat == hc32(1)) then ! Apparently streams byteswapping need to be reversed
      CALL cma_set_byteswap(kio, 1, kret)
      kret = 1 
    endif
    kupdval = kupdval + kret * sizeof_int
  else
    
    if (LLiotrace) write(0,*) ODBMP_myproc,': Opening file="'//trim(CD_filename)//'" for writing'
    kfileno = mod(kpool-1-global_pool_offset(kpool), kgrpsize) + 1 ! Probably a bug here (fix asap)
!-- The most likely version ?
!   kfileno = mod(kpool-1-global_pool_offset(kpool), kgrpsize) + kpool ???
    if (LLiotrace) write(0,'(i5,a,4i12)') &
         & ODBMP_myproc,': kfileno, kpool, kgrpsize, global_pool_offset(kpool)=',&
         &                 kfileno, kpool, kgrpsize, global_pool_offset(kpool)
    CALL cma_open(kio, trim(CD_filename), "w", kret)
    
    kupdval = 0
    CALL cma_writei(kio, hc32(0), 1, kret) ! writeI; prepare for be2le conversion
    
    if (kret /= 1) CALL io_error('Unable to write file-format entry', kret, 1, kpool, CD_filename)
    kupdval = kupdval + kret * sizeof_int
  endif
  
endif
IF (LHOOK) CALL DR_HOOK('IO_CHECKOPEN',1,ZHOOK_HANDLE)
END SUBROUTINE io_checkopen








SUBROUTINE io_close(CD_filename, kio, kupdval, kpool, knfiles, datavolume, LLiotrace, kret)

IMPLICIT NONE
CHARACTER(len=*), intent(in)      :: CD_filename
INTEGER(KIND=JPIM), intent(inout) :: kio, kupdval, knfiles
INTEGER(KIND=JPIM), intent(out)   :: kret
INTEGER(KIND=JPIM), intent(in)    :: kpool
REAL(KIND=JPRB), intent(inout)    :: datavolume(0:ODBMP_nproc)
LOGICAL, intent(in)               :: LLiotrace

REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('IO_CLOSE',0,ZHOOK_HANDLE)
if (kio /= -1) then
  datavolume(ODBMP_myproc) = datavolume(ODBMP_myproc) + kupdval
  knfiles = knfiles + 1
  if (LLiotrace) then
    write(0,'(i5,a,i12,a,i10)') &
     & ODBMP_myproc,': Closing file="'//trim(CD_filename)//'" at position ',&
     & kupdval,' bytes @ pool#',kpool 
  endif
  CALL cma_close(kio, kret)
  kio = -1
  kupdval = 0
  CALL codb_strblank(CD_filename)
endif
IF (LHOOK) CALL DR_HOOK('IO_CLOSE',1,ZHOOK_HANDLE)
END SUBROUTINE io_close
      


SUBROUTINE calculate_global_local_pool_offset(global_pool_offset,npools,ioaid,khandle)
!***************************************************************
! Calculate offset between
! local pool numbers (within a fileblock) and global pool numbers
! using IOAID db info
!***************************************************************

IMPLICIT NONE
INTEGER(KIND=JPIM), intent(inout)                  :: global_pool_offset(:)
INTEGER(KIND=JPIM), intent(in)                     :: npools
INTEGER(KIND=JPIM), intent(in)                     :: ioaid(:,:,:)
INTEGER(KIND=JPIM), intent(in)                     :: khandle ! database handle

INTEGER(KIND=JPIM)                                 :: ipoolno
INTEGER(KIND=JPIM)                                 :: irefblk
INTEGER(KIND=JPIM)                                 :: j, nfblk
REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('CALCULATE_GLOBAL_LOCAL_POOL_OFFSET',0,ZHOOK_HANDLE)

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

IF (LHOOK) CALL DR_HOOK('CALCULATE_GLOBAL_LOCAL_POOL_OFFSET',1,ZHOOK_HANDLE)

END SUBROUTINE calculate_global_local_pool_offset


SUBROUTINE construct_mask_of_considered_tables(khandle,dbname,ntables,cltable,LL_include_tbl,nactive_tables)
!***************************************************************
! Construct mask of considered tables
!***************************************************************

IMPLICIT NONE
CHARACTER(len=maxvarlen), intent(in)               :: dbname
INTEGER(KIND=JPIM), intent(in)                     :: khandle ! database handle
INTEGER(KIND=JPIM), intent(in)                     :: ntables
CHARACTER(len=maxvarlen), intent(in), allocatable  :: cltable(:)
LOGICAL, intent(inout), allocatable                :: LL_include_tbl(:)
INTEGER(KIND=JPIM), intent(out)                    :: nactive_tables

INTEGER(KIND=JPIM)                                 :: jt
INTEGER(KIND=JPIM)                                 :: i_consider, icnt_not_active, iret
REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('CONSTRUCT_MASK_OF_CONSIDERED_TABLES',0,ZHOOK_HANDLE)

!-- New approach (5-Sep-2006/SS)
do jt=1,ntables
  CALL cODB_table_is_considered(cltable(jt), i_consider)
  LL_include_tbl(jt) = (i_consider == 1)
enddo

icnt_not_active = COUNT(.not.LL_include_tbl(:))
nactive_tables = ntables - icnt_not_active

!***************************************************************
! Output diagnostics: Considered tables
!***************************************************************
if (ODBMP_myproc == 1) then
  write(0,*)'***INFO: Considering the following ',&
   & nactive_tables,' tables for database="'//trim(dbname)//'" :' 
  if (icnt_not_active == 0) then
    write(0,*)'***INFO: All tables considered'
  endif
endif

!***************************************************************
! Restore considered tables info... ?
!***************************************************************
do jt=1,ntables
  i_consider = 1
  if (.not.LL_include_tbl(jt)) i_consider = 0
  CALL cODB_consider_table(khandle, trim(cltable(jt)), i_consider, iret)
enddo

IF (LHOOK) CALL DR_HOOK('CONSTRUCT_MASK_OF_CONSIDERED_TABLES',1,ZHOOK_HANDLE)

END SUBROUTINE construct_mask_of_considered_tables


SUBROUTINE construct_active_table_id_map(active_table_id,LL_include_tbl,ntables)
IMPLICIT NONE
LOGICAL, intent(in)                                :: LL_include_tbl(:)
INTEGER(KIND=JPIM), intent(inout)                  :: active_table_id(:)
INTEGER(KIND=JPIM), intent(in)                     :: ntables
INTEGER(KIND=JPIM)                                 :: jt,jtact
REAL(KIND=JPRB)                                   :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('CONSTRUCT_ACTIVE_TABLE_ID_MAP',0,ZHOOK_HANDLE)

active_table_id(:)=-1
jtact = 0
do jt=1,ntables
  if (LL_include_tbl(jt)) then
    jtact = jtact + 1
    active_table_id(jt) = jtact
  endif
enddo

IF (LHOOK) CALL DR_HOOK('CONSTRUCT_ACTIVE_TABLE_ID_MAP',1,ZHOOK_HANDLE)

END SUBROUTINE construct_active_table_id_map



SUBROUTINE get_status_of_incore_data(istat,npools,ntables,active_table_id,cltable,khandle,LL_mypool,LL_include_tbl,LL_write_tbl)
!***************************************************************
! Get current status of incore items -> ISTAT
!***************************************************************

IMPLICIT NONE
INTEGER(KIND=JPIM), intent(inout)                  :: istat(:,:,:)
INTEGER(KIND=JPIM), intent(in)                     :: npools,ntables
INTEGER(KIND=JPIM), intent(in)                     :: active_table_id(:)
INTEGER(KIND=JPIM), intent(in)                     :: khandle ! database handle
CHARACTER(len=maxvarlen), intent(in)               :: cltable(:)
LOGICAL, intent(in)                                :: LL_mypool(:)
LOGICAL, intent(in)                                :: LL_include_tbl(:)
LOGICAL, intent(in), OPTIONAL                      :: LL_write_tbl(:)

!Local
INTEGER(KIND=JPIM)                                 :: j,jt,jtact
CHARACTER(len=maxvarlen)                           :: CLtbl
INTEGER(KIND=JPIM)                                 :: iret
 REAL(KIND=JPRB)                                   :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('GET_STATUS_OF_INCORE_DATA',0,ZHOOK_HANDLE)

istat(:,:,:) = 0
jtact = 0
STAT_LOOP: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle STAT_LOOP 
  if (present(LL_write_tbl)) then
    if (.not.LL_write_tbl(jt)) cycle STAT_LOOP  ! do not proceed if the table is not writable (store only)
  end if
  jtact = active_table_id(jt)

!-- Strip off the leading '@'-character
  CLtbl = cltable(jt)(2:)

!-- Gather size information of incore items for memory allocations
  do j=1,npools
    if (LL_mypool(j)) then
      CALL newio_status_incore32(khandle, j, istat(1,j,jtact), nstat, CLtbl, iret)
!*AF      write(0,*) 'msgpass_loaddata table = ', trim(CLtbl), ' pool = ', j, ' nrows = ', istat(2,j,jtact), &
!*AF                 ' ncols = ', istat(3,j,jtact), 'updated = ', istat(4,j,jtact)
    endif
  enddo
enddo STAT_LOOP

IF (LHOOK) CALL DR_HOOK('GET_STATUS_OF_INCORE_DATA',1,ZHOOK_HANDLE)

END SUBROUTINE get_status_of_incore_data




SUBROUTINE output_db_diagnostics(waltim,CL_called_from,khandle,dbname,LLiotrace,CLmode,io_filesize, &
                                 io_filesize_default,io_grpsize,hc32,LL_first_time,LL_backup)
!***************************************************************
! Output diagnostics: database info
!***************************************************************
IMPLICIT NONE
REAL(KIND=JPRB), intent(inout)            :: waltim(2)
CHARACTER(len=maxvarlen), intent(in)      :: dbname, CL_called_from
CHARACTER(len=1),intent(in)               :: CLmode
INTEGER(KIND=JPIM), intent(in)            :: khandle ! database handle
INTEGER(KIND=JPIM), intent(inout)         :: io_filesize
INTEGER(KIND=JPIM), intent(in)            :: io_filesize_default

INTEGER(KIND=JPIM), intent(inout)         :: io_grpsize
LOGICAL, intent(inout)                    :: LLiotrace
LOGICAL, intent(in), OPTIONAL             :: LL_backup
LOGICAL, intent(inout)                    :: LL_first_time

INTEGER(KIND=JPIM), intent(in)            :: hc32(0:1)

LOGICAL                                   :: LLiomsgpasstrace
LOGICAL, save                             :: LL_write_empty = .FALSE.

INTEGER(KIND=JPIM), parameter             :: io_trace_default = 0
INTEGER(KIND=JPIM), save                  :: io_trace = 0
INTEGER(KIND=JPIM)                        :: io_msgpass_trace = 0
INTEGER(KIND=JPIM), parameter             :: io_msgpass_trace_default = 0
INTEGER(KIND=JPIM), save                  :: iwrite_empty = 0
INTEGER(KIND=JPIM)                        :: itime(8)
INTEGER(KIND=JPIM)                        :: imsgpass_time, imsgpass_date
REAL(KIND=JPRB)                           :: ZHOOK_HANDLE
INTEGER(KIND=JPIM), parameter             :: maxsize_mb = 1000 ! Maximum filesize in megabytes for HC32-file
INTEGER(KIND=JPIM), save                  :: io_backup_enable = 0 ! By default do *NOT* enable back ups (thanks Bob C.!!)
REAL(KIND=JPRB), external                 :: util_walltime  ! now [23/1/04] from ifsaux/support/drhook.c

IF (LHOOK) CALL DR_HOOK('OUTPUT_DB_DIAGNOSTICS',0,ZHOOK_HANDLE)

waltim(1) = util_walltime()

if (ODBMP_myproc == 1) then
  call date_and_time (values=itime)
  imsgpass_time=itime(5)*10000+itime(6)*100+itime(7)
  imsgpass_date=itime(1)*10000+itime(2)*100+itime(3)
  write(0,'(1x,a,i8.8,2x,i6.6)') &
  '=== '//trim(CL_called_from)//' of db="'//trim(dbname)// &
  '" for mode='//CLmode//' started on ',imsgpass_date,imsgpass_time
endif

if (ODBMP_myproc == 1) write(0,*)'=== '//trim(CL_called_from)//' ==='
if (LL_first_time) then
!$ CALL OML_SET_LOCK()
!$OMP FLUSH(LL_first_time)
 if (LL_first_time) then
  !-- HC32-magic number
  call get_magic_hc32(0, hc32(0))
  !-- HC32-magic number byteswap i.e. reverse
  call get_magic_hc32(1, hc32(1))
  !-- I/O tracing
  call util_igetenv('ODB_IO_TRACE', io_trace_default, io_trace)
  LLiotrace = (io_trace /= 0)
  !-- Message pass tracing
  call util_igetenv('ODB_IO_MSGPASS_TRACE', io_msgpass_trace_default, io_msgpass_trace)
  LLiomsgpasstrace = (io_msgpass_trace /= 0)
  !-- Make sure these stay consistent with the ones obtained in ../aux/newio.c !
  call util_igetenv('ODB_IO_FILESIZE', io_filesize_default, io_filesize) ! in megabytes
  if (io_filesize <= 0) io_filesize = io_filesize_default
  if (io_filesize > maxsize_mb) io_filesize = maxsize_mb ! never exceed this many megabytes
  call util_igetenv('ODB_IO_GRPSIZE', 0, io_grpsize)
  if (io_grpsize <= 0) then ! try $NPES_AN
    call util_igetenv('NPES_AN', 0, io_grpsize)
  endif
  if (io_grpsize <= 0) io_grpsize = ODBMP_nproc
  if(present(LL_backup)) call util_igetenv('ODB_IO_BACKUP_ENABLE', 0, io_backup_enable)
  call util_igetenv('ODB_WRITE_EMPTY_FILES', 0, iwrite_empty)
!*AF  LL_write_empty = (iwrite_empty /= 0)
  LL_write_empty = (iwrite_empty == 1)
  if (ODBMP_myproc == 1) then
    write(0,*)'                 HC32=',hc32
    write(0,*)'         ODB_IO_TRACE=',io_trace
    write(0,*)' ODB_IO_MSGPASS_TRACE=',io_msgpass_trace
    write(0,*)'      ODB_IO_FILESIZE=',io_filesize,' MBytes'
    write(0,*)'       ODB_IO_GRPSIZE=',io_grpsize
    write(0,*)'ODB_WRITE_EMPTY_FILES=',iwrite_empty, LL_write_empty
    if(present(LL_backup)) write(0,*)' ODB_IO_BACKUP_ENABLE=',io_backup_enable
  endif
  LL_first_time = .FALSE.
!$OMP FLUSH(LL_first_time)
 endif
!$ CALL OML_UNSET_LOCK()
endif

if (ODBMP_myproc == 1) then
  write(0,*) ODBMP_myproc,&
   & ': '//trim(CL_called_from)//'() khandle=',&
   & khandle
endif

IF (LHOOK) CALL DR_HOOK('OUTPUT_DB_DIAGNOSTICS',1,ZHOOK_HANDLE)

END SUBROUTINE output_db_diagnostics





SUBROUTINE output_diagnostics_final(waltim,CL_called_from,datavolume,dbname,CLmode,it,nfiles)
!***************************************************************
! Output diagnostics: Final timings
!***************************************************************

IMPLICIT NONE
REAL(KIND=JPRB), intent(in)               :: datavolume(0:ODBMP_nproc)
REAL(KIND=JPRB), intent(inout)            :: waltim(2)
INTEGER(KIND=JPIM), intent(in)            :: it
INTEGER(KIND=JPIM), intent(in)            :: nfiles
CHARACTER(len=maxvarlen), intent(in)      :: dbname, CL_called_from
CHARACTER(len=1), intent(in)              :: CLmode
INTEGER(KIND=JPIM)                        :: imsgpass_time,imsgpass_date
REAL(KIND=JPRB)                           :: sum_datavolume
INTEGER(KIND=JPIM)                        :: itime(8)
REAL(KIND=JPRB)                           :: ZHOOK_HANDLE
REAL(KIND=JPRB), external                 :: util_walltime  ! now [23/1/04] from ifsaux/support/drhook.c

IF (LHOOK) CALL DR_HOOK('OUTPUT_DIAGNOSTICS_FINAL',0,ZHOOK_HANDLE)

waltim(2) = util_walltime()

if (ODBMP_myproc == 1) then
  call date_and_time (values=itime)
  imsgpass_time=itime(5)*10000+itime(6)*100+itime(7)
  imsgpass_date=itime(1)*10000+itime(2)*100+itime(3)
  sum_datavolume = sum(datavolume(1:ODBMP_nproc))/(1024 * 1024)

  write(0,'(1x,a,i8.8,2x,i6.6,/, &
   &        1x,a,f8.1,a,i5,a,f7.2,a,f8.1,a,i3,a)') &
   & '=== '//trim(CL_called_from)//' of db="'//trim(dbname)// &
   & '" for mode='//CLmode//' on all tables ended on ',imsgpass_date,imsgpass_time,&
   & '=== '//trim(CL_called_from)//' : ',sum_datavolume,' MBytes (',&
   & nfiles,' files) in ',waltim(2) - waltim(1),&
   & ' secs (',sum_datavolume/(waltim(2) - waltim(1) + epsilon(waltim(1))),&
   & ' MB/s) (it#',it ,')'
endif

IF (LHOOK) CALL DR_HOOK('OUTPUT_DIAGNOSTICS_FINAL',1,ZHOOK_HANDLE)

END SUBROUTINE output_diagnostics_final





FUNCTION same_file_as_previous(j,ioaid,istat,rsum,kjt,kjtact,kfilesize,kgrpsize,global_pool_offset,LL_data_in_file,LL_load, &
                               last_pool_containing_data) RESULT(LLis_same)

 INTEGER(KIND=JPIM), intent(in)                     :: j
 INTEGER(KIND=JPIM), intent(in)                     :: ioaid(:,:,:)
 INTEGER(KIND=JPIM), intent(in)                     :: istat(:,:,:)
 REAL(KIND=JPRB),    intent(inout)                  :: rsum
 INTEGER(KIND=JPIM), intent(in)                     :: kjt, kjtact,last_pool_containing_data
 INTEGER(KIND=JPIM), intent(in)                     :: kfilesize  ! suggested max filesize ; in megabytes
 INTEGER(KIND=JPIM), intent(in)                     :: kgrpsize
 INTEGER(KIND=JPIM), intent(in)                     :: global_pool_offset(:)
 LOGICAL, intent(in)                                :: LL_load ! true if loaddata mode, false if storedata mode
 LOGICAL, intent(out)                               :: LL_data_in_file
 LOGICAL                                            :: LLis_same
 INTEGER(KIND=JPIM)                                 :: rfsize
 REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE

 IF (LHOOK) CALL DR_HOOK('SAME_FILE_AS_PREVIOUS',0,ZHOOK_HANDLE)
 LL_data_in_file = .FALSE.

 if (j==1) then
    LLis_same = .FALSE.    ! First pool so a new file.
    rsum = rsum + istat(ISTAT_NBYTES,j,kjtact)  ! start accumulating file size with the first pool
    if ((ioaid(j,kjt,IOAID_LENGTH) > 0).or.(istat(ISTAT_NBYTES,j,kjtact)>0)) LL_data_in_file = .TRUE.
    IF (LHOOK) CALL DR_HOOK('SAME_FILE_AS_PREVIOUS',1,ZHOOK_HANDLE)
    return
 else ! Not first pool
    if (LL_load) then ! Load data mode
    !if (ioaid(j,kjt,IOAID_FILENO) /= 0)then  ! IO file numbers already set so use them
      if (ioaid(j,kjt,IOAID_LENGTH) == 0) then ! For cases where pools of data aren't contiguous but still in same file.
        LLis_same = .TRUE.
        IF (LHOOK) CALL DR_HOOK('SAME_FILE_AS_PREVIOUS',1,ZHOOK_HANDLE)
        return
      endif
      if (ioaid(j,kjt,IOAID_FILENO) /= ioaid(last_pool_containing_data,kjt,IOAID_FILENO))then
        ! Current file number different from previous
        LLis_same = .FALSE.
        LL_data_in_file = .TRUE.
        IF (LHOOK) CALL DR_HOOK('SAME_FILE_AS_PREVIOUS',1,ZHOOK_HANDLE)
        return
      else
        LLis_same = .TRUE.
        IF (LHOOK) CALL DR_HOOK('SAME_FILE_AS_PREVIOUS',1,ZHOOK_HANDLE)
        return
      endif
    else    ! Store data mode
      rfsize = kfilesize
      rfsize = rfsize * (1024 * 1024)
      rsum = rsum + istat(ISTAT_NBYTES,j,kjtact)
      if (mod(j-global_pool_offset(j),kgrpsize) == 1 .OR.  &   ! groupsize limit exceeded
        & rsum > rfsize) then                                  ! total filesize reached threshold 
        rsum = istat(ISTAT_NBYTES,j,kjtact)
        LLis_same = .FALSE.
        LL_data_in_file = .TRUE.
        IF (LHOOK) CALL DR_HOOK('SAME_FILE_AS_PREVIOUS',1,ZHOOK_HANDLE)
        return
      else
        LLis_same = .TRUE.
        IF (LHOOK) CALL DR_HOOK('SAME_FILE_AS_PREVIOUS',1,ZHOOK_HANDLE)
        return
      end if
    endif
 endif

 IF (LHOOK) CALL DR_HOOK('SAME_FILE_AS_PREVIOUS',1,ZHOOK_HANDLE)
END FUNCTION same_file_as_previous



SUBROUTINE assign_iopes(iope_map, ioaid, istat, kfilesize, kgrpsize, knpools, &
                      global_pool_offset, LL_include_tbl, kntables, active_table_id, LL_load)
 !*********************************************************************
 ! Designates which PEs are IOPEs for each pool for the given table
 !
 ! Input:
 !
 ! Output:
 !    iope_map :  array specifying iopes for every pool and table
 !
 ! Peter Lean     November 2014
 !*********************************************************************
 INTEGER(KIND=JPIM), intent(inout)                  :: iope_map(:,:)
 INTEGER(KIND=JPIM), intent(in)                     :: ioaid(:,:,:)
 INTEGER(KIND=JPIM), intent(in)                     :: istat(:,:,:)
 INTEGER(KIND=JPIM), intent(in)                     :: kntables
 INTEGER(KIND=JPIM), intent(in)                     :: kfilesize  ! suggested max filesize ; in megabytes
 INTEGER(KIND=JPIM), intent(in)                     :: kgrpsize
 INTEGER(KIND=JPIM), intent(in)                     :: knpools
 INTEGER(KIND=JPIM), intent(in)                     :: global_pool_offset(:)
 INTEGER(KIND=JPIM), intent(in)                     :: active_table_id(:)
 LOGICAL, intent(in)                                :: LL_include_tbl(:)
 LOGICAL, intent(in)                                :: LL_load  ! true if LOADDATA, false if STOREDATA

 LOGICAL                                            :: LL_data_in_file, LL_data_in_table
 INTEGER(KIND=JPIM)                                 :: cur_iope, jm, jt, jtact, iope_inc, last_pool_containing_data
 REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE
 REAL(KIND=JPRB)                                    :: rsum

 IF (LHOOK) CALL DR_HOOK('ASSIGN_IOPES',0,ZHOOK_HANDLE)
 jtact = -1
 cur_iope = 0
 iope_inc = 1

 LL_data_in_file = .FALSE.
 do jt=1,kntables
  if (LL_include_tbl(jt)) then
    last_pool_containing_data = 1
    jtact = active_table_id(jt)
    if (LL_load) then
      LL_data_in_table = (maxval(ioaid(1:knpools,jt,IOAID_LENGTH))>0)
    else ! store
      LL_data_in_table = (maxval(istat(ISTAT_NBYTES,1:knpools,jtact))>0)
    endif
    if (LL_data_in_table) then ! Only proceed if there is data in this table
      cur_iope = cur_iope + iope_inc ! If there will be data in this table then assign a new iope for it
      ! Loop over all pools
      rsum = 0.0_JPRB
      do jm=1,knpools
        if (.NOT. same_file_as_previous(jm,ioaid,istat,rsum,jt,jtact,kfilesize,kgrpsize,global_pool_offset, &
                                        LL_data_in_file, LL_load, last_pool_containing_data))then
            if (LL_data_in_file) then 
              cur_iope = cur_iope + iope_inc
            endif
          endif
          iope_map(jm,jtact) = cur_iope        ! before use the iope_map(jm,1) is converted to iope via fastphysproc()
          if (LL_load) then
            if (ioaid(jm,jt,IOAID_LENGTH)>0) last_pool_containing_data = jm
          else
            last_pool_containing_data =jm
          endif
      enddo
    else
      iope_map(1:knpools,jtact) = cur_iope        ! before use the iope_map(jm,1) is converted to iope via fastphysproc()
    endif
  endif
 enddo
 IF (LHOOK) CALL DR_HOOK('ASSIGN_IOPES',1,ZHOOK_HANDLE)
END SUBROUTINE assign_iopes




SUBROUTINE calculate_table_sizes(dbname, cltable, ioaid, istat, data_bytes, npools, ntables, LL_include_tbl, LL_mypool, &
                                 LLiotrace,LLiomsgpasstrace,maxbytes,active_table_id)

IMPLICIT NONE

CHARACTER(len=maxvarlen), intent(in)               :: dbname, cltable(:)
INTEGER(KIND=JPIM), intent(in)                     :: ioaid(:,:,:)
INTEGER(KIND=JPIM), intent(in)                     :: istat(:,:,:)
INTEGER(KIND=JPIM), intent(in)                     :: npools, ntables, active_table_id(:)
REAL(KIND=JPRB), intent(inout)                     :: data_bytes(:) ! nactive_tables
LOGICAL, intent(in)                                :: LL_include_tbl(:)
LOGICAL, intent(in)                                :: LL_mypool(:)
LOGICAL, intent(in)                                :: LLiotrace
LOGICAL, intent(in)                                :: LLiomsgpasstrace
INTEGER(KIND=JPIM),intent(out)                     :: maxbytes
INTEGER(KIND=JPIM)                                 :: jtact, jt, j, ipoolno, jmin, jmax, iexpect_len
INTEGER(KIND=JPIM)                                 :: iret
CHARACTER(len=maxvarlen)                           :: CLtbl
LOGICAL                                            :: LL_check_len
INTEGER(KIND=JPIM)                                 :: is_compressed
REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE
CHARACTER(len=4096)                                :: filename

IF (LHOOK) CALL DR_HOOK('CALCULATE_TABLE_SIZES',0,ZHOOK_HANDLE)

jtact = 0
TABLE_LOOP: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle TABLE_LOOP
  jtact = active_table_id(jt)
  data_bytes(jtact) = 0

!-- If any bytes have been loaded, then prevent overwriting existing incore-structures from disk
  maxbytes = MAXVAL(istat(ISTAT_NBYTES,1:npools,jtact))
  if (maxbytes > 0) cycle TABLE_LOOP  ! already loaded --> skip

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
        call codb_tablesize(dbname, CLtbl, j, iret)
        if (iret > 0 .and. iret /= iexpect_len) then
           call makefilename(dbname, CLtbl, j, filename)
           call cma_is_externally_compressed(trim(filename), 'r', is_compressed)
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
          data_bytes(jtact) = data_bytes(jtact) + iret
        endif
      else
        write(0,*) ODBMP_myproc,': Expected length disagrees with the actual'
        write(0,'(1x,i5,a,3i12)') ODBMP_myproc,': table, poolno, iret, iexpect_len : "'// &
        &                                   trim(CLtbl)//'"', j, iret, iexpect_len
        CALL ODB_abort('MSGPASS_LOADOBS', &
                      &'Expected length disagrees with the actual', &
                      & iret)
      endif
    endif
  enddo
enddo TABLE_LOOP

IF (LHOOK) CALL DR_HOOK('CALCULATE_TABLE_SIZES',1,ZHOOK_HANDLE)

END SUBROUTINE calculate_table_sizes





SUBROUTINE send_message_to_pe(nw,incore,jncore,msgoff,destPE,itag,imp_type,jsendreq,LLiomsgpasstrace, CLtbl,nsent)

INTEGER(KIND=JPIM), intent(in)                 :: nw
INTEGER(KIND=JPIM), pointer, intent(in)        :: incore(:)
INTEGER(KIND=JPIM), pointer, intent(inout)     :: jncore(:)
INTEGER(KIND=JPIM), intent(inout)              :: msgoff
INTEGER(KIND=JPIM), intent(in)                 :: destPE,itag
INTEGER(KIND=JPIM), intent(in)                 :: imp_type
INTEGER(KIND=JPIM), intent(inout)              :: jsendreq(:)
LOGICAL, intent(in)                            :: LLiomsgpasstrace
CHARACTER(len=maxvarlen), intent(in)           :: CLtbl
INTEGER(KIND=JPIM), intent(inout)              :: nsent
INTEGER(KIND=JPIM)                             :: iret
REAL(KIND=JPRB)                                :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('SEND_MESSAGE_TO_PE',0,ZHOOK_HANDLE)

if (LLiomsgpasstrace) &
    & write(0,'(i5,a,i5,a,2i12)') &
    & ODBMP_myproc,':LOAD: Sending to ',destPE,', tag,nw=',itag, nw

!if(msgoff+nw > njmax) then
!  if(nw .gt. njmax) then
!    njmax=nw
!    CALL allocate_jncore(jncore, njmax, iret)
!  endif
!  msgoff=0
!endif
jncore(msgoff+1:msgoff+nw)=incore(1:nw)
nsent=nsent+1

if (itag == -1) CALL ODB_abort('MSGPASS:', 'itag not set correctly for mpl_send ',iret) 

CALL mpl_send(jncore(msgoff+1:msgoff+nw), KDEST=destPE, KTAG=itag, &
    & KMP_TYPE=imp_type, KREQUEST=JSENDREQ(nsent), &
    & CDSTRING='LOAD: Send data to data-owner PE; table=@'//trim(CLtbl))
msgoff=msgoff+nw

if (LLiomsgpasstrace) then
  write(0,'(i5,a,i5,a,2i12)') &
  & ODBMP_myproc,':LOAD: Sent to ',destPE,', tag',itag
endif

IF (LHOOK) CALL DR_HOOK('SEND_MESSAGE_TO_PE',1,ZHOOK_HANDLE)
END SUBROUTINE send_message_to_pe





SUBROUTINE receive_message_for_this_pool_table(pe,nw,itag,j,LLiomsgpasstrace,incore,filename,CLtbl,CL_called_from)
INTEGER(KIND=JPIM), intent(inout)              :: pe, nw 
INTEGER(KIND=JPIM), intent(in)                 :: itag, j
LOGICAL, intent(in)                            :: LLiomsgpasstrace
INTEGER(KIND=JPIM), pointer, intent(inout)     :: incore(:)
CHARACTER(len=4096), intent(in)                :: filename
CHARACTER(len=maxvarlen)                       :: CLtbl, CL_called_from

INTEGER(KIND=JPIM)                             :: iret 
REAL(KIND=JPRB)                                :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('RECEIVE_MESSAGE_FOR_THIS_POOL',0,ZHOOK_HANDLE)

incore(:) = 0
!CALL allocate_incore(incore, nw + 1, iret)

if (LLiomsgpasstrace) &
  & write(0,'(i5,a,i5,a,2i12)') &
  & ODBMP_myproc,':LOAD: Receiving from ',pe,', tag,nw=',itag,nw
if (pe == -1) then ! "ANY"-source : at present KSOURCE-parameter must be left out
  if (itag == -1) CALL ODB_abort('MSGPASS_LOADOBS', 'itag not set correctly for mpl_recv ',iret)
  CALL mpl_recv(incore, KTAG=itag, KOUNT=iret, &
    & KFROM=pe,   &! Upon return the "pe" contains the actual "from-PE"
    & CDSTRING='LOAD: Recv data from I/O-PE; table=@'//trim(CLtbl)) 
else
  if (itag == -1) CALL ODB_abort('MSGPASS_LOADOBS', 'itag not set correctly for mpl_recv ',iret)
  CALL mpl_recv(incore, KSOURCE=pe, KTAG=itag, KOUNT=iret, &
    & CDSTRING='Recv data from I/O-PE; table=@'//trim(CLtbl)) 
endif
if (LLiomsgpasstrace) &
  & write(0,'(i5,a,i5,a,2i12)') &
  & ODBMP_myproc,':LOAD: Received from ',pe,', tag,iret=',itag,iret


if (iret /= nw) then
  CALL io_error('***'//trim(CL_called_from)//'(io_error): Unable to receive correct amount of data from I/O-PE', &
    & iret, nw, j,filename)
endif

IF (LHOOK) CALL DR_HOOK('RECEIVE_MESSAGE_FOR_THIS_POOL',1,ZHOOK_HANDLE)
END SUBROUTINE receive_message_for_this_pool_table





SUBROUTINE write_pool_to_hc32_file(incore,ioaid,iope_map,io,dbname,Cltbl,j,jt,jtact,NHDR,datavolume,nw,nwrite,nfiles,nfileno, &
                                   cur_io_grpsize,LLiotrace,hc32,global_pool_offset)

INTEGER(KIND=JPIM), pointer, intent(in)            :: incore(:)
INTEGER(KIND=JPIM), intent(inout)                  :: ioaid(:,:,:)
INTEGER(KIND=JPIM), intent(in)                     :: iope_map(:,:)
INTEGER(KIND=JPIM), intent(inout)                  :: io
CHARACTER(len=maxvarlen), intent(in)               :: dbname,CLtbl
INTEGER(KIND=JPIM), intent(in)                     :: j,jt,jtact
INTEGER(KIND=JPIM), intent(in)                     :: NHDR
REAL(KIND=JPRB), intent(inout)                     :: datavolume(0:ODBMP_nproc)
INTEGER(KIND=JPIM), intent(inout)                  :: nw
INTEGER(KIND=JPIM), intent(inout)                  :: nwrite
INTEGER(KIND=JPIM), intent(inout)                  :: nfiles,nfileno
INTEGER(KIND=JPIM), intent(in)                     :: cur_io_grpsize
LOGICAL, intent(in)                                :: LLiotrace
INTEGER(KIND=JPIM), intent(in)                     :: hc32(0:1)
INTEGER(KIND=JPIM), intent(in)                     :: global_pool_offset(:)

CHARACTER(len=4096)                                :: filename
INTEGER(KIND=JPIM)                                 :: iret, nrows, ncols
REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE

integer(kind=JPIM)                                 :: JWRITE=0

IF (LHOOK) CALL DR_HOOK('WRITE_POOL_TO_HC32_FILE',0,ZHOOK_HANDLE)
 call codb_strblank(filename)

 call io_checkopen(dbname, filename, CLtbl, io, j, nwrite, cur_io_grpsize, LDread=.FALSE., &
                        LLiotrace=LLiotrace, hc32=hc32, global_pool_offset=global_pool_offset, &
                        kfileno=nfileno, kret=iret)
 nrows = incore(4)
 ncols = incore(5)

 if(JWRITE>1) write(0,*) "JJJ write bytes,filename=",4*nw,trim(filename)

 ! Write header
 call cma_writei(io, incore(1), NHDR, iret) ! writeI; prepare for be2le conversion

 if (iret /= NHDR) CALL io_error('***MSGPASS_STOREDATA(io_error): Unable to write data header', iret, NHDR, j, &
   & filename, kdata=incore(1:NHDR)) 
 
 ! Write (packed) content
 call cma_writeb(io, incore(NHDR+1), (nw - NHDR) * sizeof_int, iret) ! writeB; raw data xfer
 if (iret /= (nw - NHDR) * sizeof_int) then
   CALL io_error('***MSGPASS_STOREDATA(io_error): Unable to write actual data', iret, &
                (nw - NHDR) * sizeof_int, j, filename)
 endif

 ! Update ioaid metadata info
 ioaid(j,jt,IOAID_FILENO) = nfileno
 ioaid(j,jt,IOAID_OFFSET) = nwrite
 ioaid(j,jt,IOAID_LENGTH) = nw * sizeof_int
 ioaid(j,jt,IOAID_NROWS)  = nrows
 ioaid(j,jt,IOAID_NCOLS)  = ncols

 nwrite = nwrite + nw * sizeof_int

 ! Close file if necessary
 if (io /= -1 .AND. &
   & (iope_map(j,jtact) /= iope_map(j+1,jtact) .OR. &
   & mod(j-global_pool_offset(j), cur_io_grpsize) == 0)) then 
     if(JWRITE>0) write(0,*) "JJJ close table,filename=",trim(CLtbl)," ",trim(filename)

     call io_close(filename, io, nwrite, j, nfiles, datavolume, LLiotrace, iret)
 endif

IF (LHOOK) CALL DR_HOOK('WRITE_POOL_TO_HC32_FILE',1,ZHOOK_HANDLE)
END SUBROUTINE write_pool_to_hc32_file









SUBROUTINE read_next_pool_from_hc32_file(dbname, filename, CLtbl, incore,ioaid,global_pool_offset,npools,io,  &
      &                                  hc32,NHDR,j,jt,ipoolno,igrpsize,nbytes,nrows,ncols,npad,nw, &
      &                                  jsta,nread, nfiles, datavolume, LL_poolmask, LL_lastpool, LLiotrace, &
      &                                  prev_fblk, prev_fileno)
CHARACTER(len=maxvarlen), intent(in)               :: dbname,CLtbl
CHARACTER(len=4096), intent(inout)                 :: filename
INTEGER(KIND=JPIM), pointer, intent(inout)         :: incore(:)
INTEGER(KIND=JPIM), intent(in)                     :: ioaid(:,:,:)
INTEGER(KIND=JPIM), intent(in)                     :: global_pool_offset(:)
INTEGER(KIND=JPIM), intent(in)                     :: hc32(0:1),j,jt,jsta
INTEGER(KIND=JPIM), intent(in)                     :: NHDR,npools
INTEGER(KIND=JPIM), intent(inout)                  :: nw, nread,nfiles,io
REAL(KIND=JPRB), intent(inout)                     :: datavolume(0:ODBMP_nproc)
INTEGER(KIND=JPIM), intent(out)                    :: ipoolno,igrpsize,nbytes,nrows,ncols,npad
INTEGER(KIND=JPIM), intent(inout)                  :: prev_fileno, prev_fblk
LOGICAL, intent(in)                                :: LL_poolmask(:), LL_lastpool(:), LLiotrace

INTEGER(KIND=JPIM)                                 :: iret, cur_io_grpsize, nfileno
INTEGER(KIND=JPIM)                                 :: ihdr(NHDR)

REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('READ_NEXT_POOL_FROM_HC32_FILE',0,ZHOOK_HANDLE)

  ! Close odb HC32 data file if required
  if ((io /= -1).and. &
    & ((j > npools).OR.LL_lastpool(j).OR. &
    & (ioaid(j,jt,IOAID_FBLOCK) /= prev_fblk).OR. &
    & (ioaid(j,jt,IOAID_FILENO) /= prev_fileno)))then
    call io_close(filename, io, nread, j, nfiles, datavolume, LLiotrace, iret)
    io = -1
  endif

  prev_fileno = ioaid(j,jt,IOAID_FILENO)
  prev_fblk   = ioaid(j,jt,IOAID_FBLOCK)

  cur_io_grpsize = ioaid(j,jt,IOAID_GRPSIZE)

  CALL io_checkopen(dbname, filename, CLtbl, io, j, nread, cur_io_grpsize, LDread=.TRUE.,&
                    LLiotrace=LLiotrace, hc32=hc32, global_pool_offset=global_pool_offset, &
                    kfileno=nfileno, kret=iret)


  ihdr(:) = 0
  CALL cma_readi(io, ihdr(1), NHDR, iret) ! readI ==> prepare for be2le conversion
  if (iret == -1) then ! Unexpected EOF encountered
    CALL io_error('***MSGPASS_LOADOBS(io_error): Unexpected EOF encountered while reading file', &
      & iret, NHDR, j, filename, kdata=ioaid(jsta:j,jt,IOAID_OFFSET)) 
  endif          

  if (iret /= NHDR) CALL io_error('***MSGPASS_LOADOBS(io_error): Unable to read header data', iret, NHDR, j, &
    & filename, kdata=ihdr(1:NHDR)) 
  
  ipoolno = ihdr(1)
  igrpsize = ihdr(2)
  nbytes = ihdr(3)
  nrows = ihdr(4)
  ncols = ihdr(5)
  npad = ihdr(6) ! ... not used for anything (else) yet ...
  nw = NHDR + (nbytes + sizeof_int - 1)/sizeof_int

  if (ioaid(j,jt,IOAID_LENGTH) /= nw * sizeof_int) then
    CALL io_error('***MSGPASS_LOADOBS(io_error): Inconsistent no. of bytes', &
      & nw * sizeof_int, ioaid(j,jt,IOAID_LENGTH), j, filename, &
      & kdata=ioaid(jsta:j,jt,IOAID_LENGTH)) 
  endif

  if (ioaid(j,jt,IOAID_OFFSET) /= nread) then
    CALL io_error('***MSGPASS_LOADOBS(io_error): Data expected from different byte-offset', &
      & nread, ioaid(j,jt,IOAID_OFFSET), j, &
      & filename, kdata=ioaid(jsta:j,jt,IOAID_OFFSET)) 
  endif

  nread = nread + iret * sizeof_int

  if (LL_poolmask(j)) then
    CALL allocate_incore(incore, nw + 1, iret)
    incore(1:NHDR) = ihdr(1:NHDR)
    CALL cma_readb(io, incore(NHDR+1), (nw - NHDR) * sizeof_int, iret) ! readB; raw data xfer
    if (iret /= (nw - NHDR) * sizeof_int) then
      CALL io_error('***MSGPASS_LOADOBS(io_error): Unable to read data', iret, &
                    (nw - NHDR) * sizeof_int, j, filename)
    endif
  else ! just seek forward [=1] (should work for gzip'ped files, too; don't rewind though!)
    CALL cma_seekb(io, (nw - NHDR) * sizeof_int, 1, iret)
    if (iret /= 0) then
      CALL io_error('***MSGPASS_LOADOBS(io_error): Unable to seek past data', iret, &
                    (nw - NHDR) * sizeof_int, j, filename)
    endif
    iret = (nw - NHDR) * sizeof_int
  endif
  nread = nread + iret
  
IF (LHOOK) CALL DR_HOOK('READ_NEXT_POOL_FROM_HC32_FILE',1,ZHOOK_HANDLE)
END SUBROUTINE read_next_pool_from_hc32_file





SUBROUTINE get_incore_data_for_this_pool_table(khandle,ioaid,istat,NHDR,j,jt,jtact,CLtbl,this_io_grpsize, &
                                               global_pool_offset,nw,maxbytes,incore)

INTEGER(KIND=JPIM), intent(in)               :: khandle ! database handle
INTEGER(KIND=JPIM), intent(in)               :: istat(:,:,:) ! nstat x npools x nactive_tables
INTEGER(KIND=JPIM), intent(in)               :: ioaid(:,:,:)
INTEGER(KIND=JPIM), intent(in)               :: j,jt, jtact, this_io_grpsize
CHARACTER(len=maxvarlen), intent(in)         :: CLtbl
INTEGER(KIND=JPIM), intent(in)               :: global_pool_offset(:)
INTEGER(KIND=JPIM), intent(in)               :: NHDR
INTEGER(KIND=JPIM), intent(out)              :: nw
INTEGER(KIND=JPIM), intent(in)               :: maxbytes
INTEGER(KIND=JPIM), pointer,intent(inout)    :: incore(:)

CHARACTER(len=4096)                          :: filename
INTEGER(KIND=JPIM), parameter                :: reset_updated_flag = 1
INTEGER(KIND=JPIM)                           :: nrows,ncols,npad,nbytes
INTEGER(KIND=JPIM)                           :: ipoolno,iret,cur_io_grpsize
REAL(KIND=JPRB)                              :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('GET_INCORE_DATA_FOR_THIS_POOL_TABLE',0,ZHOOK_HANDLE)

  call codb_strblank(filename)
  cur_io_grpsize = ioaid(j,jt,IOAID_GRPSIZE)
  if (cur_io_grpsize == 0) cur_io_grpsize = this_io_grpsize
  nbytes = istat(ISTAT_NBYTES,j,jtact)
  nrows  = istat(ISTAT_NROWS,j,jtact)
  ncols  = istat(ISTAT_NCOLS,j,jtact)
  npad   = 1
  nw     = NHDR + (nbytes + sizeof_int - 1)/sizeof_int

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

IF (LHOOK) CALL DR_HOOK('GET_INCORE_DATA_FOR_THIS_POOL_TABLE',1,ZHOOK_HANDLE)
END SUBROUTINE get_incore_data_for_this_pool_table




SUBROUTINE put_data_incore_for_this_pool_table(khandle,CLtbl,filename,j,incore,NHDR)

INTEGER(KIND=JPIM), intent(in)               :: khandle ! database handle
CHARACTER(len=maxvarlen), intent(in)         :: CLtbl
CHARACTER(len=4096),intent(in)               :: filename
INTEGER(KIND=JPIM), intent(in)               :: j,NHDR
INTEGER(KIND=JPIM), pointer,intent(in)       :: incore(:)

INTEGER(KIND=JPIM)                           :: iret,nbytes,nrows,ncols
REAL(KIND=JPRB)                              :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('PUT_DATA_INCORE_FOR_THIS_POOL_TABLE',0,ZHOOK_HANDLE)


  nbytes = incore(3)
  nrows  = incore(4)
  ncols  = incore(5)

  CALL newio_put_incore32(khandle, j, incore(NHDR+1), nbytes, &
                          nrows, ncols, CLtbl, iret)
  if (iret /= nbytes) then
    CALL io_error('***MSGPASS_LOADOBS(io_error): Unable to [1]put_incore() correct no. of bytes', &
                  iret, nbytes, j, filename)
  endif


IF (LHOOK) CALL DR_HOOK('PUT_DATA_INCORE_FOR_THIS_POOL_TABLE',1,ZHOOK_HANDLE)
END SUBROUTINE put_data_incore_for_this_pool_table




SUBROUTINE define_filemask_for_this_table(LL_filemask, LL_lastpool, LL_poolmask, jt, npools, ioaid)

LOGICAL, intent(inout)                             :: LL_filemask(:)
LOGICAL, intent(inout)                             :: LL_lastpool(:)
LOGICAL, intent(in)                                :: LL_poolmask(:)
INTEGER(KIND=JPIM), intent(in)                     :: npools, jt
INTEGER(KIND=JPIM), intent(in)                     :: ioaid(:,:,:)

INTEGER(KIND=JPIM)                                 :: j,jj,iprev
REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('DEFINE_FILEMASK_FOR_THIS_TABLE',0,ZHOOK_HANDLE)

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

IF (LHOOK) CALL DR_HOOK('DEFINE_FILEMASK_FOR_THIS_TABLE',1,ZHOOK_HANDLE)
END SUBROUTINE define_filemask_for_this_table


SUBROUTINE diagnostic_output_table(data_bytes,jtact,CLtbl,CL_called_from)
REAL(KIND=JPRB), intent(in)                        :: data_bytes(:) ! nactive_tables
INTEGER(KIND=JPIM), intent(in)                     :: jtact
CHARACTER(len=maxvarlen), intent(in)               :: CLtbl
CHARACTER(len=maxvarlen), intent(in)               :: CL_called_from

INTEGER(KIND=JPIM)                                 :: jj
character(len=maxvarlen)                           :: CLbytes
character(len=maxvarlen)                           :: CLverb
REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('DIAGNOSTIC_OUTPUT_TABLE',0,ZHOOK_HANDLE)

  if (ODBMP_myproc == 1) then
    write(CLbytes,'(f20.1)') data_bytes(jtact)
    jj = index(CLbytes,'.')
    if (jj > 0) CLbytes(jj:) = ' '
    if (trim(CL_called_from)=='MSGPASS_STOREOBS') CLverb = 'STORE'
    if (trim(CL_called_from)=='MSGPASS_LOADOBS') CLverb = 'LOAD'
    write(0,'(1x,a)') trim(CLverb)//'ing table="'//trim(CLtbl)//'" total = '//&
    & trim(CLbytes)//' bytes'
  endif

IF (LHOOK) CALL DR_HOOK('DIAGNOSTIC_OUTPUT_TABLE',1,ZHOOK_HANDLE)
END SUBROUTINE diagnostic_output_table




SUBROUTINE calculate_size_of_output_file(istat,ioaid,iope_map,this_io_grpsize,global_pool_offset,data_bytes,ntables,npools, &
                                         NHDR,iwrite_empty, LL_write_empty, active_table_id)
INTEGER(KIND=JPIM), intent(in)              :: istat(:,:,:) ! nstat x npools x nactive_tables
INTEGER(KIND=JPIM), intent(in)              :: ioaid(:,:,:)
INTEGER(KIND=JPIM), intent(in)              :: iope_map(:,:)
INTEGER(KIND=JPIM), intent(in)              :: active_table_id(:)
INTEGER(KIND=JPIM), intent(in)              :: this_io_grpsize
INTEGER(KIND=JPIM), intent(in)              :: global_pool_offset(:)
REAL(KIND=JPRB), intent(inout)              :: data_bytes(:) ! nactive_tables
INTEGER(KIND=JPIM),intent(in)               :: ntables,npools,NHDR,iwrite_empty
LOGICAL, intent(in)                         :: LL_write_empty

INTEGER(KIND=JPIM)                          :: nw,j,jt,jtact,nbytes,cur_io_grpsize,nrows
REAL(KIND=JPRB)                             :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('CALCULATE_SIZE_OF_OUTPUT_FILE',0,ZHOOK_HANDLE)

do jt=1,ntables
!-- Gather total no. of bytes information in advance
  jtact = active_table_id(jt)
  if (jtact /= -1)then
    data_bytes(jtact) = 0
    do j=1,npools
      nw = 0
      cur_io_grpsize = ioaid(j,jt,IOAID_GRPSIZE)
      if (cur_io_grpsize == 0) cur_io_grpsize = this_io_grpsize
      nbytes = istat(ISTAT_NBYTES,j,jtact)
      nrows = istat(ISTAT_NROWS,j,jtact)
!*AF  if ODB_WRITE_EMPTY_FILES=2 we do not really write...
      if ((nbytes > 0 .and. (iwrite_empty /= 2 .or. nrows > 0)) .OR. (nbytes <= 0 .AND. LL_write_empty)) then
        nw = NHDR + (nbytes + sizeof_int - 1)/sizeof_int
      endif
      if (iope_map(j,jtact) /= iope_map(j+1,jtact) .OR. &
         & mod(j-global_pool_offset(j), cur_io_grpsize) == 0 .OR. &
         & j == npools) then 
        !-- Account for yet another file ending and allocate space for "HC32" 
        !   i.e. 4 bytes which was so far unaccounted for when file was "opened"
        nw = nw + 1
      endif
      data_bytes(jtact) = data_bytes(jtact) + nw * sizeof_int
    enddo
  endif
enddo
IF (LHOOK) CALL DR_HOOK('CALCULATE_SIZE_OF_OUTPUT_FILE',1,ZHOOK_HANDLE)

END SUBROUTINE calculate_size_of_output_file


SUBROUTINE allocate_incore_jncore(incore,jncore,istat,npools,jtact,NHDR,maxbytes)

INTEGER(KIND=JPIM), pointer, intent(inout)     :: incore(:)
INTEGER(KIND=JPIM), pointer, intent(inout)     :: jncore(:)
INTEGER(KIND=JPIM), intent(in)                 :: istat(:,:,:) ! nstat x npools x nactive_tables
INTEGER(KIND=JPIM), intent(in)                 :: npools,jtact,NHDR
INTEGER(KIND=JPIM), intent(inout)              :: maxbytes

INTEGER(KIND=JPIM)                             :: nwmax, iret
REAL(KIND=JPRB)                                :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('ALLOCATE_INCORE_JNCORE',0,ZHOOK_HANDLE)

  !-- Allocate data buffer
    maxbytes = MAXVAL(istat(ISTAT_NBYTES,1:npools,jtact))
    nwmax = NHDR + max(1,(maxbytes + sizeof_int - 1)/sizeof_int)
    CALL allocate_incore(incore, nwmax, iret)
  
IF (LHOOK) CALL DR_HOOK('ALLOCATE_INCORE_JNCORE',1,ZHOOK_HANDLE)

END SUBROUTINE allocate_incore_jncore





SUBROUTINE make_temporary_backups_of_table_files(dbname,CLtbl,jt,npools,LL_mypool,LL_has_backup,ioaid,this_io_grpsize)

CHARACTER(len=maxvarlen), intent(in)               :: dbname
CHARACTER(len=maxvarlen), intent(in)               :: CLtbl
INTEGER(KIND=JPIM), intent(in)                     :: jt, npools, this_io_grpsize
LOGICAL, intent(in)                                :: LL_mypool(:)
LOGICAL, allocatable, intent(inout)                :: LL_has_backup(:,:)
INTEGER(KIND=JPIM), intent(inout)                  :: ioaid(:,:,:)

INTEGER(KIND=JPIM), save                           :: io_backup_enable = 0 ! By default do *NOT* enable back ups (thanks Bob C.!!)
INTEGER(KIND=JPIM)                                 :: j, iret
CHARACTER(len=4096)                                :: filename, CLdirname, CLbasename
REAL(KIND=JPRB)                                    :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('MAKE_TEMPORARY_BACKUPS_OF_TABLE_FILES',0,ZHOOK_HANDLE)

!-- To start with : *rename* any existing table-files into backup file
  do j=1,npools
!! The following call to codb_remove_tablefile() has been removed since
!! if job finished/aborted before updated version has been written to disk
!! the whole database would be corrupt (SS/4-Jul-2003)
!! Instead, backup files will be created and then removed at the very end
!!      if (LL_mypool(j)) CALL codb_remove_tablefile(dbname, CLtbl, j, iret)
    if (io_backup_enable /= 0) then
      if (LL_mypool(j)) then ! More failsafe
        call makefilename(dbname, CLtbl, j, filename)
        call true_dirname_and_basename(trim(filename), CLdirname, CLbasename)
        call codb_rename_file(trim(filename), trim(CLdirname)//'/'//trim(CLbasename)//'.BACKUP', iret)
        LL_has_backup(j,jt) = (iret == 0)  ! True if a backup has been made
      endif
    endif
    ! Reset ioaid for this pool,table
    ioaid(j,jt,IOAID_FILENO) = 0
    ioaid(j,jt,IOAID_OFFSET) = 0
    ioaid(j,jt,IOAID_LENGTH) = 0
    if (ioaid(j,jt,IOAID_GRPSIZE) == 0) ioaid(j,jt,IOAID_GRPSIZE) = this_io_grpsize
    ioaid(j,jt,IOAID_NROWS)  = 0
  enddo

IF (LHOOK) CALL DR_HOOK('MAKE_TEMPORARY_BACKUPS_OF_TABLE_FILES',1,ZHOOK_HANDLE)

END SUBROUTINE make_temporary_backups_of_table_files


END MODULE odbio_msgpass
