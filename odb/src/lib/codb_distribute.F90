!OPTION! -O nodarg
! E. Sevault 18-May-2009 : SX9 compiler de-optimisation for safety reasons.

SUBROUTINE cODB_idistribute(s, slen, with, rc)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE odbmp, only : ODBMP_distribute
implicit none
INTEGER(KIND=JPIM), intent(in)    :: slen, with
INTEGER(KIND=JPIM), intent(inout) :: s(slen)
INTEGER(KIND=JPIM), intent(out)   :: rc
CALL ODBMP_distribute(s, slen, with, rc)
END SUBROUTINE cODB_idistribute

SUBROUTINE codb_distribute_str(s, func, with, rc)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE odbshared, only : ODB_SIZEOF_INT
implicit none
INTEGER(KIND=JPIM), intent(in)  :: with
INTEGER(KIND=JPIM), intent(out) :: rc
character(len=*), intent(inout) :: s
external func
INTEGER(KIND=JPIM), parameter :: sizeof_int = ODB_SIZEOF_INT
INTEGER(KIND=JPIM) :: slen, xlen
INTEGER(KIND=JPIM), allocatable :: x(:)
slen = len(s)
xlen = (slen+sizeof_int-1)/sizeof_int
allocate(x(xlen))
call ctransfer(1,1,s,x)
CALL func(x, xlen, with, rc)
call ctransfer(-1,1,s,x)
deallocate(x)
if (rc == xlen) rc = slen
END SUBROUTINE codb_distribute_str

SUBROUTINE codb_distribute_vecstr(s, func, with, dim2len, rc)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE odbshared, only : ODB_SIZEOF_INT
implicit none
INTEGER(KIND=JPIM), intent(in)  :: with, dim2len
INTEGER(KIND=JPIM), intent(out) :: rc
character(len=*), intent(inout) :: s(dim2len)
external func
INTEGER(KIND=JPIM), parameter :: sizeof_int = ODB_SIZEOF_INT
INTEGER(KIND=JPIM) :: slen, xlen
INTEGER(KIND=JPIM), allocatable :: x(:)
slen = len(s) * dim2len
xlen = (slen+sizeof_int-1)/sizeof_int
allocate(x(xlen))
call ctransfer(1,dim2len,s,x)
CALL func(x, xlen, with, rc)
call ctransfer(-1,dim2len,s,x)
deallocate(x)
if (rc == xlen) rc = slen
END SUBROUTINE codb_distribute_vecstr
