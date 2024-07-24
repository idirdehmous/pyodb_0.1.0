SUBROUTINE msgpass_storeobs(khandle, kret)

! A routine to exchange ODB-data between processors
! Called from module ODB: ODB_close()

! P. Marguinaud : 10-10-2013 : Nullify pointers
! P. Lean       : 25-11-2014 : Code refactor to allow greater parallelism in io for improved scalability.

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
USE oml_mod   ,only : oml_set_lock, oml_unset_lock, oml_my_thread
USE odb_module
USE mpl_module
USE odbio_msgpass
USE str       ,ONLY : sadjustl 

IMPLICIT NONE

INTEGER(KIND=JPIM), intent(in)  :: khandle ! database handle
INTEGER(KIND=JPIM), intent(out) :: kret    ! return code

INTEGER(KIND=JPIM)              :: io_method, jt, j, ntables, iret, maxbytes
INTEGER(KIND=JPIM)              :: nw, nwmax, nbytes, npools
INTEGER(KIND=JPIM)              :: itag, pe, destPE, io
INTEGER(KIND=JPIM), allocatable :: itags(:,:)
INTEGER(KIND=JPIM), allocatable :: iope_map(:,:)
CHARACTER(len=maxvarlen), allocatable :: cltable(:)
LOGICAL, allocatable            :: LL_mypool(:), LL_include_tbl(:), LL_write_tbl(:), LL_has_backup(:,:)
LOGICAL                         :: LL_readonly
LOGICAL, save                   :: LL_first_time = .TRUE.
INTEGER(KIND=JPIM), parameter   :: io_filesize_default = 32 ! in megabytes; see ../aux/newio.c, too
INTEGER(KIND=JPIM), save        :: io_filesize = 1
INTEGER(KIND=JPIM), save        :: io_grpsize = -1
INTEGER(KIND=JPIM), save        :: io_backup_enable = 0 ! By default do *NOT* enable back ups (thanks Bob C.!!)
INTEGER(KIND=JPIM), save        :: iwrite_empty = 0
LOGICAL, save                   :: LL_write_empty = .FALSE.
LOGICAL, save                   :: LLiotrace = .FALSE.
LOGICAL, save                   :: LLiomsgpasstrace = .FALSE.
CHARACTER(len=1)                :: CLmode='w'
CHARACTER(len=maxvarlen)        :: dbname, CLtbl, CL_called_from
CHARACTER(len=4096)             :: filename
CHARACTER(len=4096)             :: CLdirname, CLbasename
INTEGER(KIND=JPIM)              :: nwrite
INTEGER(KIND=JPIM)              :: it
INTEGER(KIND=JPIM), allocatable :: istat(:,:,:) ! nstat x npools x nactive_tables
INTEGER(KIND=JPIM), parameter   :: NHDR = 6
INTEGER(KIND=JPIM)              :: this_io_grpsize, itables, cur_io_grpsize
INTEGER(KIND=JPIM)              :: nactive_tables, jtact
INTEGER(KIND=JPIM)              :: i_write
INTEGER(KIND=JPIM), pointer     :: incore(:)
INTEGER(KIND=JPIM), pointer     :: jncore(:)
INTEGER(KIND=JPIM)              :: msgoff, nsent, nfileno
INTEGER(KIND=JPIM), allocatable :: jsendreq(:)
INTEGER(KIND=JPIM), allocatable :: active_table_id(:)
INTEGER(KIND=JPIM), save        :: hc32(0:1) = (/0,0/) ! "HC32" & its reverse/byteswap (to be filled) 
INTEGER(KIND=JPIM), pointer     :: ioaid(:,:,:) ! A shorthand notation
REAL(KIND=JPRB), allocatable    :: data_bytes(:) ! nactive_tables
REAL(KIND=JPRB)                 :: datavolume(0:ODBMP_nproc), waltim(2)
INTEGER(KIND=JPIM)              :: nfiles
INTEGER(KIND=JPIM), allocatable :: global_pool_offset(:)
INTEGER(KIND=JPIM)              :: fast_physproc, xfast, n_messages_required
INTEGER(KIND=JPIM)              :: imp_type
REAL(KIND=JPRB)                 :: ZHOOK_HANDLE

REAL(KIND=JPRB)                 :: timef,ttab,ttloop
REAL(KIND=JPRB)                 :: tprebar,tpostbar
REAL(KIND=JPRB), allocatable    :: ttwrite(:),ttsend(:),ttrecv(:),ttwait(:),ttclose(:)
REAL(KIND=JPRB), allocatable    :: tbar(:), ttable(:), ttcomm(:)
REAL(KIND=JPRB), allocatable    :: sendmb(:),recvmb(:)
INTEGER(kind=JPIM), allocatable :: sendcnt(:),recvcnt(:)
INTEGER(kind=JPIM)              :: JWRITE=0

fast_physproc(xfast) = mod(xfast-1,ODBMP_nproc)+1
IF (LHOOK) CALL DR_HOOK('MSGPASS_STOREOBS',0,ZHOOK_HANDLE)

incore => null ()
jncore => null ()
ioaid  => null ()

CL_called_from = 'MSGPASS_STOREOBS'
call MPL_barrier(cdstring='MSGPASS_STOREOBS')

if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_STOREOBS : Begin'
kret = 0
itables = 0
it = oml_my_thread()

!**********************************************
! Pick up some database metadata
!**********************************************
LL_readonly = db(khandle)%readonly
io_method = db(khandle)%io_method
npools = db(khandle)%glbNpools


!**********************************************
! Skip message passing if necessary
!**********************************************
if (.NOT.db(khandle)%inuse) goto 99999

if (io_method /= 4) goto 99999 ! only applicable for ODB_IO_METHOD=4

if (LL_readonly) goto 99999


!***************************************************************
! Define whether using blocking or non-blocking message passing
!***************************************************************
#if defined(NECSX) && defined(BOM)
imp_type = JP_BLOCKING_STANDARD
#else
imp_type = JP_NON_BLOCKING_STANDARD
#endif

dbname = db(khandle)%name

call ODBMP_sync()

!***************************************************************
! Output diagnostics: database info
!***************************************************************
call output_db_diagnostics(waltim,CL_called_from,khandle,dbname,LLiotrace,CLmode,io_filesize,io_filesize_default, &
                           io_grpsize,hc32,LL_first_time,.TRUE.)

! Intialize a few things
datavolume(:) = 0    ! Total volume of data stored on disk (by this PE)
nfiles = 0           ! Number of opened files on this PE
this_io_grpsize = min(npools,io_grpsize)   ! This has to come after output_db_diagnostics call

!***************************************************************
! Get up-to-date IOAID database metadata
!***************************************************************
call comm_iomap(khandle, 1, iret) ! Get up to date IOAID
ioaid => db(khandle)%ioaid

!***************************************************************
! Calculate offset between
! local pool numbers (within a fileblock) and global pool numbers
! using IOAID db info
!***************************************************************
allocate(global_pool_offset(npools))
call calculate_global_local_pool_offset(global_pool_offset,npools,ioaid,khandle)

!***************************************************************
! Pick up table names
!***************************************************************
ntables = ODB_getnames(khandle, '*', 'table')
allocate(cltable(ntables))
ntables = ODB_getnames(khandle, '*', 'table', cltable)

allocate(LL_mypool(npools))
allocate(LL_include_tbl(ntables))
allocate(LL_write_tbl(ntables))

! Allocate storage arrays for extra diagnostic output
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


!***************************************************************
! Construct mask of pools owned by this task
!***************************************************************
do j=1,npools
  pe = fast_physproc(j)
  LL_mypool(j) = (pe == ODBMP_myproc)
enddo

!***************************************************************
! Construct mask of considered tables -> LL_include_tbl and nactive_tables
!***************************************************************
call construct_mask_of_considered_tables(khandle,dbname,ntables,cltable,LL_include_tbl, nactive_tables)

! If no tables to load then skip to end
if (nactive_tables <= 0) then
  if (ODBMP_myproc == 1) write(0,*)'***INFO: No active tables'
  kret = 0
  goto 99999
endif

!***************************************************************
! Construct map between table id and active table id -> active_table_id
!***************************************************************
allocate(active_table_id(ntables))
call construct_active_table_id_map(active_table_id,LL_include_tbl,ntables)

!***************************************************************
! Construct mask of writable tables -> LL_write_tbl
!***************************************************************
!-- Anne Fouilloux (23/02/10)
do jt=1,ntables
  call cODB_table_is_writable(cltable(jt), i_write)
  LL_write_tbl(jt) = (i_write == 1)
enddo


! Allocate backup mask
if (io_backup_enable /= 0) then
  allocate(LL_has_backup(npools,ntables))
  LL_has_backup(:,:) = .FALSE.
endif


!***************************************************************
! Define ISTAT: current status of incore items
!***************************************************************
allocate(istat(nstat,npools,nactive_tables))
call get_status_of_incore_data(istat,npools,ntables,active_table_id,cltable,khandle,LL_mypool,LL_include_tbl,LL_write_tbl)

!-- Make istat-information globally available
call ODBMP_global('MAX', istat)

!***************************************************************
! Initialisation for NON BLOCKING message passing------------
!***************************************************************


!***************************************************************
!-- Main loops
!***************************************************************

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

!************************************************************
! Define IOPEs for all tables and pools
!************************************************************
allocate(iope_map(npools+1,nactive_tables))
iope_map(:,:) = -1 ! Means: Any PE
call assign_iopes(iope_map, ioaid, istat, io_filesize, this_io_grpsize, npools, global_pool_offset, LL_include_tbl, &
                  ntables, active_table_id,.FALSE.)

!***************************************************************
! Calculate output file sizes -> data_bytes
!***************************************************************
allocate(data_bytes(nactive_tables))
data_bytes(:) = 0
call calculate_size_of_output_file(istat,ioaid,iope_map,this_io_grpsize,global_pool_offset,data_bytes,ntables,npools,NHDR, &
                                   iwrite_empty, LL_write_empty, active_table_id)

!***************************************************************
! Predefine message tags for all used table,pools combinations 
!   (global not local - as need to know tags of incoming messages from other tasks)
! Loop through all table/pool combinations and determine if a message will need to be sent,
!   if so, then define a tag for the send/recv to use. 
!***************************************************************
n_messages_required = 0
allocate(itags(npools+1,ntables))
itags(:,:) = -1
itag = 0
msgoff = 0
nwrite=0
do jt=1,ntables
  jtact = active_table_id(jt)
  if (LL_include_tbl(jt).and.LL_write_tbl(jt).and.(maxval(istat(ISTAT_NBYTES,1:npools,jtact))>0).and.maxval(istat(ISTAT_UPDATED, &
                                  1:npools,jtact))>0)then  ! If table updated, has non-zero data and is write enabled 
    do j=1,npools
      destPE = fast_physproc(iope_map(j,jtact))
        nw = NHDR + (istat(ISTAT_NBYTES,j,jtact) + sizeof_int - 1)/sizeof_int 
        if (LL_mypool(j)) then     ! If message needs to be sent for this PE then 
              msgoff = msgoff + nw ! Add up total volume of data that needs to be sent by this PE as required for jncore allocation
              n_messages_required = n_messages_required + 1
        endif
        nbytes = istat(ISTAT_NBYTES,j,jtact)
        if (nbytes > 0) then
          itags(j,jt) = itag
          itag = itag + 1
        endif
    enddo
  endif
enddo
if (ODBMP_myproc == 1) write(0,*) 'Created itags array with MAX: ',MAXVAL(itags(:,:)), ' and MIN: ',MINVAL(itags(:,:))

call allocate_jncore(jncore, msgoff, iret)
allocate(jsendreq(n_messages_required))

!************************************************************
! Send section
!************************************************************
nsent = 0
msgoff = 0
do jt=1,ntables
  jtact = active_table_id(jt)
  io = -1
  call codb_strblank(filename)
  if (LL_include_tbl(jt).and.LL_write_tbl(jt).and.(maxval(istat(ISTAT_NBYTES,1:npools,jtact))>0).and.maxval(istat(ISTAT_UPDATED, &
                                           1:npools,jtact))>0)then  ! If table updated, has non-zero data and is write enabled 
    CLtbl = cltable(jt)(2:) ! Strip off the @
    do j=1,npools
      destPE = fast_physproc(iope_map(j,jtact))
      if (destPE/=ODBMP_myproc)then  ! Only get data and send message if iope is not this pe (otherwise, get data in the write loop directly)
          if (LL_mypool(j)) then
            nbytes = istat(ISTAT_NBYTES,j,jtact)
            if (nbytes > 0) then
              nw = NHDR + (istat(ISTAT_NBYTES,j,jtact) + sizeof_int - 1)/sizeof_int
              call allocate_incore_jncore(incore,jncore,istat,npools,jtact,NHDR,maxbytes)
              call get_incore_data_for_this_pool_table(khandle,ioaid,istat,NHDR,j,jt,jtact,CLtbl,this_io_grpsize, &
                                                       global_pool_offset,nw,maxbytes,incore)
              itag = itags(j,jt)
              call send_message_to_pe(nw,incore,jncore,msgoff,destPE,itag,imp_type,jsendreq,LLiomsgpasstrace, CLtbl,nsent)
            endif
          endif
      endif
    enddo
  endif
enddo

!************************************************************
! Receive/write section
!************************************************************
do jt=1,ntables
  jtact = active_table_id(jt)
  io = -1
  nfileno=0
  if (LL_include_tbl(jt).and.LL_write_tbl(jt).and.(maxval(istat(ISTAT_NBYTES,1:npools,jtact))>0).and.maxval(istat(ISTAT_UPDATED, &
                                            1:npools,jtact))>0)then  ! If table updated, has non-zero data and is write enabled 
    CLtbl = cltable(jt)(2:) ! Strip off the @
    call make_temporary_backups_of_table_files(dbname,CLtbl,jt,npools,LL_mypool,LL_has_backup,ioaid,this_io_grpsize)
    call diagnostic_output_table(data_bytes,jtact,CLtbl,CL_called_from)
    ! Allocate incore
    maxbytes = MAXVAL(istat(ISTAT_NBYTES,1:npools,jtact))
    nwmax = NHDR + max(1,(maxbytes + sizeof_int - 1)/sizeof_int)
    CALL allocate_incore(incore, nwmax, iret)
    do j=1,npools
      if (fast_physproc(iope_map(j,jtact))==ODBMP_myproc) then
        nbytes = istat(ISTAT_NBYTES,j,jtact)
        cur_io_grpsize = ioaid(j,jt,IOAID_GRPSIZE)
        if (nbytes > 0) then
          nw = NHDR + (istat(ISTAT_NBYTES,j,jtact) + sizeof_int - 1)/sizeof_int
          itag = itags(j,jt)
          pe = fast_physproc(j)  ! Source PE
          if (pe/=ODBMP_myproc)then  ! If source pe isn't this pe, then receive message
              call receive_message_for_this_pool_table(pe,nw,itag,j,LLiomsgpasstrace,incore,filename,CLtbl,CL_called_from)
          else  ! if source pe is this pe, then just get data from incore
              call get_incore_data_for_this_pool_table(khandle,ioaid,istat,NHDR,j,jt,jtact,CLtbl,this_io_grpsize, &
                                                       global_pool_offset,nw,maxbytes,incore)
          endif
          call write_pool_to_hc32_file(incore,ioaid,iope_map,io,dbname,Cltbl,j,jt,jtact,NHDR,datavolume,nw,nwrite,nfiles,nfileno, &
                                       cur_io_grpsize,LLiotrace,hc32,global_pool_offset)
        endif
        ! Close if end of this group (e.g. last pool in current ECMA.x)
        if ((io /= -1).and.(mod(j-global_pool_offset(j), cur_io_grpsize) == 0))then
          call io_close(filename, io, nwrite, npools, nfiles, datavolume, LLiotrace, iret)
        endif
      endif
    enddo
    call io_close(filename, io, nwrite, npools, nfiles, datavolume, LLiotrace, iret)
  endif ! LL_write_tbl
enddo

!************************************************************
! Debug output
!************************************************************
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

call comm_iomap(khandle, 2, iret) ! Propagate up to date IOAID's


!************************************************************
! Remove backup files where created
!************************************************************
jtact = 0
REMOVE_BACKUPS: do jt=1,ntables
  if (.not.LL_include_tbl(jt)) cycle REMOVE_BACKUPS
  if (.not.LL_write_tbl(jt)) cycle REMOVE_BACKUPS ! do not proceed if table is not writable
  jtact = active_table_id(jt)

!-- Strip off the leading '@'-character
  CLtbl = cltable(jt)(2:)

  if (io_backup_enable /= 0) then
    do j=1,npools
      if (LL_has_backup(j,jt)) then
        call makefilename(dbname, CLtbl, j, filename)
        call true_dirname_and_basename(trim(filename), CLdirname, CLbasename)
        call codb_remove_file(trim(CLdirname)//'/'//trim(CLbasename)//'.BACKUP', iret)
      endif
    enddo
  endif
enddo REMOVE_BACKUPS

datavolume(0) = nfiles
call ODBMP_global('SUM', datavolume, LDREPROD=.false., root=1)
nfiles = datavolume(0)

call mpl_barrier()

!************************************************************
! Output diagnostics
!************************************************************
call output_diagnostics_final(waltim,CL_called_from,datavolume,dbname,CLmode,it,nfiles)

99999 continue

!************************************************************
! Tidy up
!************************************************************
if (allocated(iope_map))           deallocate(iope_map)
if (allocated(LL_has_backup))      deallocate(LL_has_backup)
if (allocated(LL_mypool))          deallocate(LL_mypool)
if (allocated(LL_include_tbl))     deallocate(LL_include_tbl)
if (allocated(LL_write_tbl))       deallocate(LL_write_tbl)
if (associated(incore))            deallocate(incore)
if (associated(jncore))            deallocate(jncore)
if (allocated(istat))              deallocate(istat)
if (allocated(data_bytes))         deallocate(data_bytes)
if (allocated(cltable))            deallocate(cltable)
if (allocated(global_pool_offset)) deallocate(global_pool_offset)
if (allocated(itags))              deallocate(itags)
if (allocated(jsendreq))           deallocate(jsendreq)
if (allocated(active_table_id))    deallocate(active_table_id)

nullify(ioaid)
if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_STOREOBS : End'
IF (LHOOK) CALL DR_HOOK('MSGPASS_STOREOBS',1,ZHOOK_HANDLE)

END SUBROUTINE msgpass_storeobs

