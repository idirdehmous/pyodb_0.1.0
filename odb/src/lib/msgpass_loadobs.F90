SUBROUTINE msgpass_loadobs(khandle, kret)

! A routine to exchange ODB-data between processors
! Called from module ODB: ODB_open():open_db 

! P. Marguinaud : 10-10-2013 : Nullify pointers
! P. Lean       : 25-11-2014 : Code refactor to allow greater parallelism in io for improved scalability.

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
USE oml_mod, only : oml_set_lock, oml_unset_lock, oml_my_thread
USE odb_module
USE mpl_module
USE odbio_msgpass
USE str, ONLY : sadjustl 

implicit none

INTEGER(KIND=JPIM), intent(in)  :: khandle ! database handle
INTEGER(KIND=JPIM), intent(out) :: kret    ! return code

INTEGER(KIND=JPIM)              :: io_method, jt, j, jj, ntables, iret, maxbytes
INTEGER(KIND=JPIM)              :: nw, nbytes, nrows, ncols, npad, igrpsize, ipoolno, npools
INTEGER(KIND=JPIM)              :: itag, pe, destPE, io
INTEGER(KIND=JPIM), allocatable :: itags(:,:)
INTEGER(KIND=JPIM), allocatable :: iope_map(:,:)
CHARACTER(len=maxvarlen), allocatable :: cltable(:)
LOGICAL, allocatable            :: LL_mypool(:), LL_include_tbl(:)
LOGICAL                         :: LL_readonly
LOGICAL, save                   :: LL_first_time = .TRUE.
INTEGER(KIND=JPIM), parameter   :: io_filesize_default = 32 ! in megabytes; see ../aux/newio.c, too
INTEGER(KIND=JPIM), save        :: io_filesize = 1
INTEGER(KIND=JPIM), save        :: io_grpsize = -1
LOGICAL, save                   :: LLiotrace = .FALSE.
LOGICAL, save                   :: LLiomsgpasstrace = .FALSE.
CHARACTER(len=1)                :: CLmode='r'
CHARACTER(len=maxvarlen)        :: dbname, CLtbl, CL_called_from
CHARACTER(len=4096)             :: filename
INTEGER(KIND=JPIM)              :: jsta   ! start pool for the currently open file
INTEGER(KIND=JPIM)              :: nread
INTEGER(KIND=JPIM)              :: it
INTEGER(KIND=JPIM), allocatable :: istat(:,:,:) ! nstat x npools x nactive_tables
INTEGER(KIND=JPIM), parameter   :: NHDR = 6
INTEGER(KIND=JPIM)              :: this_io_grpsize, n_tables_read,prev_fblk,prev_fileno
INTEGER(KIND=JPIM)              :: nactive_tables, jtact
INTEGER(KIND=JPIM), pointer     :: incore(:)
INTEGER(KIND=JPIM), pointer     :: jncore(:)
INTEGER(KIND=JPIM)              :: msgoff, nsent
INTEGER(KIND=JPIM), allocatable :: jsendreq(:)
INTEGER(KIND=JPIM), save        :: hc32(0:1) = (/0,0/) ! "HC32" & its reverse/byteswap (to be filled) 
INTEGER(KIND=JPIM), pointer     :: ioaid(:,:,:) ! A shorthand notation
REAL(KIND=JPRB), allocatable    :: data_bytes(:) ! nactive_tables
INTEGER(KIND=JPIM), allocatable :: active_table_id(:)
REAL(KIND=JPRB)                 :: datavolume(0:ODBMP_nproc), waltim(2)
INTEGER(KIND=JPIM)              :: nfiles, idummy_arr, npermcnt
INTEGER(KIND=JPIM), allocatable :: perm_list(:), global_pool_offset(:)
LOGICAL, allocatable            :: LL_poolmask(:), LL_filemask(:), LL_lastpool(:)
INTEGER(KIND=JPIM)              :: fast_physproc, xfast, n_messages_required
INTEGER(KIND=JPIM)              :: imp_type
INTEGER(KIND=JPIM)              :: nfileno
REAL(KIND=JPRB)                 :: ZHOOK_HANDLE

fast_physproc(xfast) = mod(xfast-1,ODBMP_nproc)+1
IF (LHOOK) CALL DR_HOOK('MSGPASS_LOADOBS',0,ZHOOK_HANDLE)

incore => null ()
jncore => null ()
ioaid  => null ()

CL_called_from = 'MSGPASS_LOADOBS'
call MPL_barrier(cdstring='MSGPASS_LOADOBS')

if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_LOADOBS : Begin'
kret = 0
n_tables_read = 0
it = oml_my_thread()

!**********************************************
! Pick up some database metadata
!**********************************************
io_method = db(khandle)%io_method
LL_readonly = db(khandle)%readonly
dbname = db(khandle)%name
npools = db(khandle)%glbNpools

!**********************************************
! Skip message passing if necessary
!**********************************************
if (.NOT.db(khandle)%inuse) goto 99999
if (io_method /= 4) goto 99999 ! only applicable for ODB_IO_METHOD=4

!***************************************************************
! Define whether using blocking or non-blocking message passing
!***************************************************************
#if defined(NECSX) && defined(BOM)
imp_type = JP_BLOCKING_STANDARD
#else
imp_type = JP_NON_BLOCKING_STANDARD
#endif

call ODBMP_sync()

!***************************************************************
! Output diagnostics: database info
!***************************************************************
call output_db_diagnostics(waltim,CL_called_from,khandle,dbname,LLiotrace,CLmode,io_filesize,io_filesize_default, &
                           io_grpsize,hc32,LL_first_time)
! Initialize a few variables
datavolume(:) = 0
nfiles = 0 ! no. of opened files on this PE
this_io_grpsize = min(npools,io_grpsize)

allocate(LL_poolmask(npools))
allocate(LL_filemask(npools))
allocate(LL_lastpool(npools))
allocate(LL_mypool(npools))

!***************************************************************
! Define poolmask
!***************************************************************
LL_poolmask(1:npools) =  .TRUE. ! true means that take all these pools
if (LL_readonly) then
  call cODB_get_permanent_poolmask(khandle, 0, idummy_arr, npermcnt)
  npermcnt = abs(npermcnt)
  if (npermcnt > 0) then
    allocate(perm_list(npermcnt))
    perm_list(:) = 0
    call cODB_get_permanent_poolmask(khandle, npermcnt, perm_list, iret)
    LL_poolmask(1:npools) = .FALSE. ! By default: do *not* take these pools
    do j=1,npermcnt
      jj = perm_list(j)
      if (jj >= 1 .and. jj <= npools)  LL_poolmask(jj) = .TRUE.
    enddo
    deallocate(perm_list)
  endif
endif


!***************************************************************
! Output diagnostics: poolmask
!***************************************************************
if (ODBMP_myproc == 1) then
  if (all(LL_poolmask(1:npools))) then
    write(0,'(1x,a,i5,a,i5,a)') &
      & 'MSGPASS_LOADOBS: All pools in range [1..npools] will processed'
  else
    write(0,*)'MSGPASS_LOADOBS: Only the following pools will be processed:'
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


!***************************************************************
! Get up-to-date IOAID database metadata
!***************************************************************
call comm_iomap(khandle, 1, iret)
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

allocate(LL_include_tbl(ntables))

!***************************************************************
! Construct mask of considered tables -> LL_include_tbl
!***************************************************************
call construct_mask_of_considered_tables(khandle,dbname,ntables,cltable,LL_include_tbl,nactive_tables)

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
! Construct mask of pools owned by this task
!***************************************************************
do j=1,npools
  pe = fast_physproc(j)
  LL_mypool(j) = (pe == ODBMP_myproc)
enddo


!***************************************************************
! Get current status of incore items -> ISTAT
!***************************************************************
allocate(istat(nstat,npools,nactive_tables))
call get_status_of_incore_data(istat,npools,ntables,active_table_id,cltable,khandle,LL_mypool,LL_include_tbl)

!-- Make istat-information globally available
call ODBMP_global('MAX', istat)


!************************************************************
! Define IOPEs - IOPE_MAP maps each pool, table to an IO task
!************************************************************
allocate(iope_map(npools+1,nactive_tables))
iope_map(:,:) = -1 ! Means: Any PE
call assign_iopes(iope_map, ioaid, istat, io_filesize, this_io_grpsize, npools, global_pool_offset, LL_include_tbl, &
                  ntables, active_table_id, .TRUE.)


!************************************************************
! Calculate volume of data in each considered table
!************************************************************
allocate(data_bytes(nactive_tables))
data_bytes(:) = 0
call calculate_table_sizes(dbname, cltable, ioaid, istat, data_bytes, npools, ntables, LL_include_tbl, LL_mypool, &
                           LLiotrace, LLiomsgpasstrace, maxbytes, active_table_id)

!-- Make data_bytes-information available for PE#1
call ODBMP_global('SUM', data_bytes, LDREPROD=.false., root=1)

!***************************************************************
! Debug output
!***************************************************************
if ( (LLiotrace .or. LLiomsgpasstrace) .and. ODBMP_myproc == 1) then 
  jtact = 0
  do jt=1,ntables
    if (.not.LL_include_tbl(jt)) cycle
    jtact = active_table_id(jt)
    CLtbl = cltable(jt)(2:)
    write(0,'(1x,2(a,i5),a,f20.1,a)') &
    & 'I/O-pe map for table "'//trim(CLtbl)//'", pool range=[1..npools], total ',&
    & data_bytes(jtact),' bytes :'
    write(0,'((10(1x,i5)))' ) (iope_map(j,jtact),j=1,npools)
  enddo
endif

!***************************************************************
! Predefine message tags for all used table,pools combinations 
!   (global not local - as need to know tags of incoming messages from other tasks)
! Loop through all table/pool combinations and determine if a message will need to be sent,
!   if so, then define a tag for the send/recv to use. 
!***************************************************************
allocate(itags(npools+1,ntables))
itags(:,:) = -1
itag = 0 
msgoff = 0
n_messages_required = 0

do jt=1,ntables
  if (LL_include_tbl(jt)) then
    !call define_filemask_for_this_table(LL_filemask, LL_lastpool, LL_poolmask, jt, npools, ioaid)
    jtact = active_table_id(jt)
    if (maxval(ioaid(1:npools,jt,IOAID_LENGTH),dim=1)>0) then
      do j=1,npools
        if (LL_poolmask(j)) then
          if (ioaid(j,jt,IOAID_LENGTH)>0) then
            destPE = fast_physproc(j)
            !if (destPE /= ODBMP_myproc)then ! send message to owner pe, only if this pe is not the owner
              itags(j,jt) = itag
              itag = itag + 1
              if (fast_physproc(iope_map(j,jtact))==ODBMP_myproc) then ! If this task is an iope for this pool/table
  
                nw = ioaid(j,jt,IOAID_LENGTH)/sizeof_int
                msgoff = msgoff + nw ! Add up total volume of data that needs to be sent by this PE as
                                     ! required for jncore allocation
                n_messages_required = n_messages_required + 1
              endif
            !endif
          endif
        endif
      enddo
    endif
  endif
enddo
if (ODBMP_myproc == 1) write(0,*) 'Created itags array with MAX: ',maxval(itags(:,:)), ' and MIN: ',minval(itags(:,:))

call allocate_jncore(jncore, msgoff, iret)

allocate(jsendreq(n_messages_required))


!***************************************************************
!-- Main loops
!***************************************************************

!***************************************************************
! Initialisation for NON BLOCKING message passing------------
!***************************************************************
nsent = 0
msgoff = 0

!************************************************************
! Read / send section
!************************************************************
nsent = 0
nread = 0
nfiles = 0
datavolume(:) = 0    ! Total volume of data stored on disk (by this PE)

! If this PE is an IOPE for any tables/pools then read those files
do jt=1,ntables
  if (LL_include_tbl(jt)) then
    jtact = active_table_id(jt)
    if (maxval(ioaid(1:npools,jt,IOAID_LENGTH),dim=1)>0) then
    !if (maxval(iope_map(1:npools,jtact))/=-1) then  ! only proceed with this table if there is data in it
      CLtbl = cltable(jt)(2:) !-- Strip off the leading '@'-character
      call diagnostic_output_table(data_bytes,jtact,CLtbl,CL_called_from)
      io = -1 ! reset file handle as new table must also mean a new file
      prev_fblk = -1
      prev_fileno = -1
      call define_filemask_for_this_table(LL_filemask, LL_lastpool, LL_poolmask, jt, npools, ioaid)
      do j=1,npools
        if (fast_physproc(iope_map(j,jtact))==ODBMP_myproc) then  ! If this pe is an iope for this pool/table
          ! If non-zero data for this pool then read data from file
          if (LL_poolmask(j).and.(ioaid(j,jt,IOAID_LENGTH)>0)) then
            nw = ioaid(j,jt,IOAID_LENGTH)/sizeof_int
            call allocate_incore(incore, nw + 1, iret)
            call read_next_pool_from_hc32_file(dbname, filename, CLtbl, incore,ioaid,global_pool_offset,npools,io,hc32,NHDR,j,   &
      &                                        jt,ipoolno,igrpsize,nbytes,nrows,ncols,npad,nw,jsta,nread, &
      &                                        nfiles,datavolume,LL_poolmask,LL_lastpool,LLiotrace,prev_fblk,prev_fileno)
            ! Send message (if required)
            destPE = fast_physproc(j)
            if (destPE /= ODBMP_myproc)then ! send message to owner pe, only if this pe is not the owner
                itag = itags(j,jt)
                call send_message_to_pe(nw,incore,jncore,msgoff,destPE,itag,imp_type,jsendreq,LLiomsgpasstrace, CLtbl,nsent)
            else ! if this pe owns the pool, then no need to send message - put incore directly
                call allocate_incore(incore, nw + 1, iret)
                call put_data_incore_for_this_pool_table(khandle,CLtbl,filename,j,incore,NHDR)
            endif
          end if
        endif
      enddo
      call io_close(filename, io, nread, npools, nfiles, datavolume, LLiotrace, iret)
      n_tables_read = n_tables_read + 1
    endif
  endif
enddo

!************************************************************
! Recieve section
!************************************************************
do jt=1,ntables
  if (LL_include_tbl(jt)) then
    CLtbl = cltable(jt)(2:) !-- Strip off the leading '@'-character
    jtact = active_table_id(jt)
    if (maxval(ioaid(1:npools,jt,IOAID_LENGTH),dim=1)>0) then
      do j=1,npools
        pe = fast_physproc(iope_map(j,jtact))   ! Source PE
        if (pe /= ODBMP_myproc)then ! If source pe is not this PE, then receive message, &
                                    ! otherwise the data has already been put incore so do nothing
          if (LL_mypool(j)) then
            nw = ioaid(j,jt,IOAID_LENGTH)/sizeof_int
            if (LL_poolmask(j) .AND. nw > 0) then
              itag = itags(j,jt)
              call allocate_incore(incore, nw + 1, iret)
              call receive_message_for_this_pool_table(pe,nw,itag,j,LLiomsgpasstrace,incore,filename,CLtbl,CL_called_from)
              call put_data_incore_for_this_pool_table(khandle,CLtbl,filename,j,incore,NHDR)
            endif
          endif
        endif
      enddo
    endif
  endif
enddo

kret = n_tables_read ! Return code of msgpass_loadobs = No. of tables stored/loaded

datavolume(0) = nfiles
call ODBMP_global('SUM', datavolume, LDREPROD=.false., root=1)
nfiles = datavolume(0)

!***************************************************************
! Output: Summary
!***************************************************************
call output_diagnostics_final(waltim,CL_called_from,datavolume,dbname,CLmode,it,nfiles)

99999 continue

!***************************************************************
! Tidy up
!***************************************************************
if (allocated(iope_map))           deallocate(iope_map)
if (allocated(LL_mypool))          deallocate(LL_mypool)
if (allocated(LL_include_tbl))     deallocate(LL_include_tbl)
if (associated(incore))            deallocate(incore)
if (associated(jncore))            deallocate(jncore)
if (allocated(istat))              deallocate(istat)
if (allocated(data_bytes))         deallocate(data_bytes)
if (allocated(cltable))            deallocate(cltable)
if (allocated(LL_poolmask))        deallocate(LL_poolmask)
if (allocated(LL_filemask))        deallocate(LL_filemask)
if (allocated(LL_lastpool))        deallocate(LL_lastpool)
if (allocated(global_pool_offset)) deallocate(global_pool_offset)
if (allocated(itags))              deallocate(itags)
if (allocated(jsendreq))           deallocate(jsendreq)
if (allocated(active_table_id))    deallocate(active_table_id)

nullify(ioaid)
if (ODBMP_myproc == 1) write(0,*)'=== MSGPASS_LOADOBS : End'
IF (LHOOK) CALL DR_HOOK('MSGPASS_LOADOBS',1,ZHOOK_HANDLE)
      
END SUBROUTINE msgpass_loadobs

