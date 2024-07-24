MODULE odbiomap

USE PARKIND1  ,ONLY : JPIM     ,JPRB

USE odbshared
USE odbmp
USE odbutil

implicit none

SAVE
PUBLIC

!-- I/O-aid cases
INTEGER(KIND=JPIM), parameter :: IOAID_FBLOCK  = 1
INTEGER(KIND=JPIM), parameter :: IOAID_GRPSIZE = 2
INTEGER(KIND=JPIM), parameter :: IOAID_POOLNO  = 3
INTEGER(KIND=JPIM), parameter :: IOAID_FILENO  = 4
INTEGER(KIND=JPIM), parameter :: IOAID_OFFSET  = 5
INTEGER(KIND=JPIM), parameter :: IOAID_LENGTH  = 6
INTEGER(KIND=JPIM), parameter :: IOAID_NROWS   = 7
INTEGER(KIND=JPIM), parameter :: IOAID_NCOLS   = 8
INTEGER(KIND=JPIM), parameter :: IONAID = 8 ! Always max. of IOAID_*'s

#include "fwrite_iomap.h"

CONTAINS

SUBROUTINE comm_iomap(khandle, kcase, kret)
implicit none
INTEGER(KIND=JPIM), intent(in)  :: khandle, kcase
INTEGER(KIND=JPIM), intent(out) :: kret

INTEGER(KIND=JPIM) naid, j, jt
character(len=1) CLenv
INTEGER(KIND=JPIM), POINTER :: ioaid(:,:,:), grpsize(:) ! A shorthand notations

kret = 0
naid = db(khandle)%naid
ioaid => db(khandle)%ioaid
grpsize => db(khandle)%grpsize

if (naid > 0) then
  if (kcase == 0) then
    CALL ODBMP_global('MAX', grpsize)
  else if (kcase == 1) then ! First thing in msgpass_loaddata / msgpass_storedata
    CALL ODBMP_global('MAX', ioaid)
  else if (kcase == 2) then ! Last thing in msgpass_storedata (if STOREing)
    CALL ODBMP_global('MAX', ioaid)
  endif
  if (ODBMP_myproc == 1 .and. (kcase == 1 .OR. kcase == 2)) then
    CALL codb_getenv('ODB_PRINT_IOAID', CLenv)
    if (CLenv == '1') then
      write(0,*)'>>> COMM_IOMAP : kcase=',kcase,' <<<'
      write(0,'(a4,1x,a5,1x,a5,1x,a5,1x,a12,1x,a12,1x,a12,1x,a5)') &
        'FBLK','GRPSZ','POOL#', &
        'FILE#','OFFSET','LENGTH', &
        'NROWS', 'NCOLS'
      do jt=1,size(ioaid,dim=2)
        write(0,*)'==> Tblno#',jt
        do j=1,size(ioaid,dim=1)
          write(0,'(i4,1x,i5,1x,i5,1x,i5,1x,i12,1x,i12,1x,i12,1x,i5)') &
          ioaid(j,jt,1:naid) ! assuming 8 entries
        enddo
      enddo
      write(0,*)'>>> End of COMM_IOMAP print <<<'
    endif
  endif
endif ! if (naid > 0) ...
END SUBROUTINE comm_iomap



SUBROUTINE read_iomap(khandle, kret)
implicit none
INTEGER(KIND=JPIM), intent(in)    :: khandle
INTEGER(KIND=JPIM), intent(out)   :: kret

INTEGER(KIND=JPIM) :: naid, ifblk, ipool_offset
character(len=maxfilen) :: CLiomap_file

kret = 0
naid = db(khandle)%naid

if (naid > 0) then
  if (ODBMP_myproc == 1) then ! Metadata read by PE#1 only
    CLiomap_file = trim(db(khandle)%name)//'.iomap'
    ifblk = 0
    ipool_offset = 0
    CALL cread_iomap(khandle, trim(CLiomap_file), ifblk, &
                     ipool_offset, kret)
  else
    ifblk = 0
  endif
  CALL comm_iomap(khandle, 0, kret)
  kret = ifblk
endif
END SUBROUTINE read_iomap


SUBROUTINE write_iomap(khandle, kret)
implicit none
INTEGER(KIND=JPIM), intent(in)    :: khandle
INTEGER(KIND=JPIM), intent(out)   :: kret
CALL fwrite_iomap(khandle, kret)
END SUBROUTINE write_iomap

END MODULE odbiomap
