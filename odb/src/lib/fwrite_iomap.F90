SUBROUTINE fwrite_iomap(khandle, kret)

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK

USE odb_module , tmp_fwrite_iomap => fwrite_iomap

implicit none

INTEGER(KIND=JPIM), intent(in)    :: khandle
INTEGER(KIND=JPIM), intent(out)   :: kret

INTEGER(KIND=JPIM), parameter :: fmt  = 1 ! Format/flavor of iomap-file(s)
INTEGER(KIND=JPIM), parameter :: npad = 1 ! Always pad data bytes to the next INTEGER*4 boundary
INTEGER(KIND=JPIM), parameter :: lenmult = 1 ! Unit of offsets & lengths is 1 byte
character(len=*), parameter :: NL  = char(10)  ! Newline-character
character(len=*), parameter :: TAB = char( 9)  ! TAB-character
INTEGER(KIND=JPIM), parameter :: EOR = -1 ! End of record -marker
INTEGER(KIND=JPIM) :: jfblk, io, rc, ntables, npools, ilen, nfblk
INTEGER(KIND=JPIM) :: jt, j, jj, ipools, imax, imin, ngrpsize, io_method, naid
INTEGER(KIND=JPIM) :: nrows, ncols
INTEGER(KIND=JPIM) :: ifileno, ioffset
logical LLfix
!!logical LL_dormant
character(len=80) ::  CLbuf
character(len=maxfilen) :: CLiomap_file
character(len=maxvarlen), allocatable :: cltable(:)
INTEGER(KIND=JPIM), POINTER :: ioaid(:,:,:) ! Shorthand
INTEGER(KIND=JPIM), ALLOCATABLE :: ipoolset(:)
REAL(KIND=JPRB) :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('FWRITE_IOMAP',0,ZHOOK_HANDLE)
kret = 0

naid = db(khandle)%naid
ioaid => db(khandle)%ioaid

if (naid > 0 .AND. ODBMP_myproc == 1) then ! Metadata written by PE#1 only
  ntables = ODB_getnames(khandle, '*', 'table')
  allocate(cltable(ntables))
  ntables = ODB_getnames(khandle, '*', 'table', cltable)

  npools = db(khandle)%glbNpools
  allocate(ipoolset(npools))

  nfblk = db(khandle)%nfileblocks
  io_method = db(khandle)%io_method

  IOMAP_BLOCK_LOOP: do jfblk=1,nfblk
    if (      COUNT(ioaid(:,:,IOAID_FBLOCK) == jfblk) > 0  ) then
      imax = MAXVAL(ioaid(:,:,IOAID_POOLNO), &
       & MASK=ioaid(:,:,IOAID_FBLOCK) == jfblk) 
      imin = MINVAL(ioaid(:,:,IOAID_POOLNO), &
       & MASK=ioaid(:,:,IOAID_FBLOCK) == jfblk) 
      imin = max(1,imin)
      ipools = imax - imin + 1
    else
      ipools = 0
    endif
    
!!    if (ipools <= 0) cycle IOMAP_BLOCK_LOOP ! Quite bad ;-( Shouldn't we abort ??

    CLiomap_file = db(khandle)%ciomap(jfblk)
    ngrpsize = db(khandle)%grpsize(jfblk)
    io = -1
    write(0,'(a)') "Opening IOMAP-file='"//trim(CLiomap_file)//"' for writing"
    CALL cma_open(io, trim(CLiomap_file), "w", rc)

    write(CLbuf,'(i15)') fmt
    write(0,'(a,i1)') TAB//'Format=',fmt
    ilen = len_trim(CLbuf)
    CALL cma_writeb(io, CLbuf(1:ilen)//NL, ilen+1, rc)

    CLbuf = CLiomap_file
    ilen = len_trim(CLbuf)
    CALL cma_writeb(io, CLbuf(1:ilen)//NL, ilen+1, rc)

    write(CLbuf,'(3i15)') ntables, ipools, ngrpsize ! Can't have Npools here
    ilen = len_trim(CLbuf)
    CALL cma_writeb(io, CLbuf(1:ilen)//NL, ilen+1, rc)

    write(CLbuf,'(3i15)') npad, io_method, lenmult
    ilen = len_trim(CLbuf)
    CALL cma_writeb(io, CLbuf(1:ilen)//NL, ilen+1, rc)

    write(0,'(a,i2,a,i3,a,i5,a,i5)') &
     & TAB//'File block#',jfblk,': tables=',ntables,&
     & ', maxpools=',ipools,', grpsize=',ngrpsize 

    TABLE_LOOP: do jt=1,ntables
      ncols = MAXVAL(ioaid(1:npools,jt,IOAID_NCOLS))
      if (ncols <= 0) ncols = EOR
      write(CLbuf,'(2i15,1x,a)') jt, ncols, trim(cltable(jt))
      ilen = len_trim(CLbuf)
      CALL cma_writeb(io, CLbuf(1:ilen)//NL, ilen+1, rc)

      ipoolset(:) = 0
      do j=1,npools
         if ( ioaid(j,jt,IOAID_FBLOCK) == jfblk .AND. &
            & ioaid(j,jt,IOAID_LENGTH) > 0 ) then
           ipoolset(j) = j
         endif
      enddo

      !-- 26/6/2008 by SS: ioaid(j,jt,IOAID_FILENO) could be wrong due to 
      !   $ODB_IO_BACKUP_ENABLE being zero (0) and use odb odbprune-script. Fix it.
      !   Discovered by Yannick Tremolet, ECMWF, when running 48h (long) time window 4dvar:
      !   He specified too low NPES_AN in prepIFS and then changed the # of PEs
      !   on the fly in SMS. As result odbprune-script stripped off some data files ;-(

      LLfix = .FALSE.
      ifileno = 0
      ioffset = 2147483647

      do jj=1,npools
        j = ipoolset(jj)
        if ( j > 0 ) then
          if (ioaid(j,jt,IOAID_OFFSET) < ioffset) then ! New file
            LLfix = (ioaid(j,jt,IOAID_FILENO) /= ioaid(j,jt,IOAID_POOLNO))
            if (LLfix) ifileno = ioaid(j,jt,IOAID_POOLNO) ! This is the fix
          endif
          if (LLfix) ioaid(j,jt,IOAID_FILENO) = ifileno
          ioffset = ioaid(j,jt,IOAID_OFFSET) + ioaid(j,jt,IOAID_LENGTH)
        endif
      enddo ! jj=1,npools

!!      LL_dormant = .TRUE.
      do jj=1,npools
        j = ipoolset(jj)
        if ( j > 0 ) then
          write(CLbuf,'(5i15)') &
           & ioaid(j,jt,IOAID_POOLNO),  &! Original LOCAL pool#
           & ioaid(j,jt,IOAID_FILENO),  &! Now fixed i.e. $NPES_AN 
           & ioaid(j,jt,IOAID_OFFSET), &
           & ioaid(j,jt,IOAID_LENGTH), &
           & ioaid(j,jt,IOAID_NROWS) 
          ilen = len_trim(CLbuf)
          CALL cma_writeb(io, CLbuf(1:ilen)//NL, ilen+1, rc)
!!          LL_dormant = .FALSE. ! At least one record
        endif ! if (ioaid(j,jt,IOAID_FBLOCK) == jfblk ...
      enddo ! jj=1,npools

!!      if (LL_dormant) then
!!        write(0,'(a)') TAB//">>> Warning: Dormant table='"//trim(cltable(jt))//"'"
!!      endif

      write(CLbuf,'(5i15)') EOR, EOR, EOR, EOR, EOR
      ilen = len_trim(CLbuf)
      CALL cma_writeb(io, CLbuf(1:ilen)//NL, ilen+1, rc)
    enddo TABLE_LOOP ! jt=1,ntables

    !-- End of tables (and EOF) for this iomap-file
    write(CLbuf,'(2i15,1x,a)') EOR, EOR, 'EOF'
    ilen = len_trim(CLbuf)
    CALL cma_writeb(io, CLbuf(1:ilen)//NL, ilen+1, rc)

    CALL cma_close(io, rc)
    io = -1
  enddo IOMAP_BLOCK_LOOP

  deallocate(ipoolset)
  deallocate(cltable)
  nullify(ioaid)
endif
IF (LHOOK) CALL DR_HOOK('FWRITE_IOMAP',1,ZHOOK_HANDLE)

END SUBROUTINE fwrite_iomap

