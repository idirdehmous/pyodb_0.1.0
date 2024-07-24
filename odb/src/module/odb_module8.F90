!
! Wrapper functions for 8-byte-word implementation
! See original ODB_...() for inline documentation of arguments/variables
!
Module ODB_MODULE8
#ifdef USE_8_BYTE_WORDS

Use PARKIND1, Only : JPIM, JPRB, JPRM

Implicit NONE

Public

INTERFACE ODB_map_vpools
MODULE PROCEDURE &
  & ODB_map_vpools_direct, &
  & ODB_map_vpools_fromfile
END INTERFACE

INTERFACE ODB_distribute
MODULE PROCEDURE &
  & ODB_distribute_str, ODB_distribute_vecstr, &
  & ODB_distribute_int, ODB_distribute_vecint, &
  & ODB_distribute_real8, ODB_distribute_vecreal8
END INTERFACE

INTERFACE ODB_setval
MODULE PROCEDURE &
  & ODB_dsetval, ODB_isetval
END INTERFACE

INTERFACE ODB_duplchk
MODULE PROCEDURE &
  & ODB_iduplchk, ODB_dduplchk
END INTERFACE

INTERFACE ODB_groupify
MODULE PROCEDURE &
  & ODB_igroupify!, ODB_dgroupify
END INTERFACE

INTERFACE ODB_tolsearch
MODULE PROCEDURE &
  & ODB_itolsearch, ODB_dtolsearch
END INTERFACE

INTERFACE ODB_binsearch
MODULE PROCEDURE &
  & ODB_ibinsearch, ODB_dbinsearch
END INTERFACE

INTERFACE ODB_aggregate
MODULE PROCEDURE &
  & ODB_iaggregate, ODB_daggregate
END INTERFACE

INTERFACE ODB_control_word_info
MODULE PROCEDURE &
  & ODB_control_word_info_vector, ODB_control_word_info_matrix
END INTERFACE

!INTERFACE ODBMP_global
!MODULE PROCEDURE &
!     &ODBMP_iglobal!       , ODBMP_dglobal       , &
!     &ODBMP_iglobal_2d    , ODBMP_dglobal_2d    , &
!     &ODBMP_iglobal_3d    , ODBMP_dglobal_3d    , &
!     &ODBMP_iglobal_scalar, ODBMP_dglobal_scalar
!END INTERFACE

!private ODBMP_iglobal
!private ODBMP_dglobal
!private ODBMP_iglobal_2d
!private ODBMP_dglobal_2d
!private ODBMP_iglobal_3d
!private ODBMP_dglobal_3d
!private ODBMP_iglobal_scalar
!private ODBMP_dglobal_scalar

private :: ODB_distribute_str
private :: ODB_distribute_vecstr
private :: ODB_distribute_int
private :: ODB_distribute_vecint
private :: ODB_distribute_real8
private :: ODB_distribute_vecreal8
private :: ODB_dsetval
private :: ODB_isetval
private :: ODB_iduplchk
private :: ODB_dduplchk
private :: ODB_igroupify
private :: ODB_dgroupify
private :: ODB_itolsearch
private :: ODB_dtolsearch
private :: ODB_ibinsearch
private :: ODB_dbinsearch
private :: ODB_iaggregate
private :: ODB_daggregate
private :: ODB_control_word_info_vector
private :: ODB_control_word_info_matrix

Contains

!
! ODB_open ============================================================
!
Integer(Kind=8) Function ODB_open(DBNAME, STATUS, NPOOLS, OLD_NPOOLS, &
                                  MAXPOOLNO)

  Use odb_module, only : ODB_open4 => ODB_open

  Character(Len=*), Intent(IN) :: DBNAME, STATUS
  Integer(Kind=8), Intent(INOUT) :: NPOOLS
  Integer(Kind=8), Intent(OUT), Optional :: OLD_NPOOLS, MAXPOOLNO

  Integer(Kind=JPIM) :: HANDLE4 = 0, NPOOLS4, OLD_NPOOLS4, MAXPOOLNO4

  integer :: o=0,o1=0,o2=0

  if (present(old_npools)) o1=1
  if (present(maxpoolno)) o2=2
  o=o1+o2

  NPOOLS4 = NPOOLS

  select case(o)
  case(0)
     HANDLE4=ODB_open4(dbname, status, npools4)
  case(1)
     HANDLE4=ODB_open4(dbname, status, npools4, old_npools=old_npools4)
  case(2)
     HANDLE4=ODB_open4(dbname, status, npools4, maxpoolno=maxpoolno4)
  case(3)
     HANDLE4=ODB_open4(dbname, status, npools4, old_npools=old_npools4, maxpoolno=maxpoolno4)
  end select

  NPOOLS = NPOOLS4
  If (PRESENT(OLD_NPOOLS)) OLD_NPOOLS = OLD_NPOOLS4
  If (PRESENT(MAXPOOLNO))  MAXPOOLNO  = MAXPOOLNO4
  ODB_open = HANDLE4

End Function ODB_open

!
! ODB_init ============================================================
!
Integer(Kind=8) Function ODB_init(myproc, nproc, pid, tid, ntid)

  Use odb_module, only : ODB_init4 => ODB_init

  Integer(Kind=8), Intent(OUT), Optional :: myproc, nproc, pid, tid, ntid

  Integer(Kind=JPIM) :: myproc4 = -1, nproc4 = -1, pid4 = -1,          &
                        tid4 = -1, ntid4 = -1, RC4 = 0

  integer :: o=0,o1=0,o2=0,o3=0,o4=0,o5=0

  if (present(myproc)) o1=1
  if (present(nproc)) o2=2
  if (present(pid)) o3=4
  if (present(tid)) o4=8
  if (present(ntid)) o5=16
  o=o1+o2+o3+o4+o5

  select case(o)
  case(0)
     RC4=ODB_init4()
  case(1)
     RC4=ODB_init4(myproc=myproc4)
  case(2)
     RC4=ODB_init4(nproc=nproc4)
  case(3)
     RC4=ODB_init4(myproc=myproc4, nproc=nproc4)
  case(4)
     RC4=ODB_init4(pid=pid4)
  case(5)
     RC4=ODB_init4(myproc=myproc4, pid=pid4)
  case(6)
     RC4=ODB_init4(nproc=nproc4, pid=pid4)
  case(7)
     RC4=ODB_init4(myproc=myproc4, nproc=nproc4, pid=pid4)
  case(8)
     RC4=ODB_init4(tid=tid4)
  case(9)
     RC4=ODB_init4(myproc=myproc4, tid=tid4)
  case(10)
     RC4=ODB_init4(nproc=nproc4, tid=tid4)
  case(11)
     RC4=ODB_init4(myproc=myproc4, nproc=nproc4, tid=tid4)
  case(12)
     RC4=ODB_init4(pid=pid4, tid=tid4)
  case(13)
     RC4=ODB_init4(myproc=myproc4, pid=pid4, tid=tid4)
  case(14)
     RC4=ODB_init4(nproc=nproc4, pid=pid4, tid=tid4)
  case(15)
     RC4=ODB_init4(myproc=myproc4, nproc=nproc4, pid=pid4, tid=tid4)
  case(16)
     RC4=ODB_init4(ntid=ntid4)
  case(17)
     RC4=ODB_init4(myproc=myproc4, ntid=ntid4)
  case(18)
     RC4=ODB_init4(nproc=nproc4, ntid=ntid4)
  case(19)
     RC4=ODB_init4(myproc=myproc4, nproc=nproc4, ntid=ntid4)
  case(20)
     RC4=ODB_init4(pid=pid4, ntid=ntid4)
  case(21)
     RC4=ODB_init4(myproc=myproc4, pid=pid4, ntid=ntid4)
  case(22)
     RC4=ODB_init4(nproc=nproc4, pid=pid4, ntid=ntid4)
  case(23)
     RC4=ODB_init4(myproc=myproc4, nproc=nproc4, pid=pid4, ntid=ntid4)
  case(24)
     RC4=ODB_init4(tid=tid4, ntid=ntid4)
  case(25)
     RC4=ODB_init4(myproc=myproc4, tid=tid4, ntid=ntid4)
  case(26)
     RC4=ODB_init4(nproc=nproc4, tid=tid4, ntid=ntid4)
  case(27)
     RC4=ODB_init4(myproc=myproc4, nproc=nproc4, tid=tid4, ntid=ntid4)
  case(28)
     RC4=ODB_init4(pid=pid4, tid=tid4, ntid=ntid4)
  case(29)
     RC4=ODB_init4(myproc=myproc4, pid=pid4, tid=tid4, ntid=ntid4)
  case(30)
     RC4=ODB_init4(nproc=nproc4, pid=pid4, tid=tid4, ntid=ntid4)
  case(31)
     RC4=ODB_init4(myproc=myproc4, nproc=nproc4, pid=pid4, tid=tid4, ntid=ntid4)
  end select

  If (PRESENT(myproc)) myproc = myproc4
  If (PRESENT(nproc))  nproc  = nproc4
  If (PRESENT(pid))    pid    = pid4
  If (PRESENT(tid))    tid    = tid4
  If (PRESENT(ntid))   ntid   = ntid4
  ODB_init = RC4

End Function ODB_init

!
! ODB_addview =========================================================
!
Integer(Kind=8) Function ODB_addview(HANDLE, DTNAME, VIEWFILE, SELECT, &
                                     UNIQUEBY, FROM, WHERE, ORDERBY,   &
                                     SORTBY, QUERY, SET, ABORT)

  Use odb_module, only : ODB_addview4 => ODB_addview

  Integer(Kind=8), Intent(IN) :: HANDLE
  Character(Len=*), Intent(IN) :: DTNAME
  Character(Len=*), Intent(IN), Optional :: VIEWFILE, SELECT, UNIQUEBY,&
                                            FROM, WHERE, ORDERBY,      &
                                            SORTBY, QUERY
  Character(Len=*), Dimension(:), Intent(IN), Optional :: SET
  Logical(Kind=8), Intent(IN), Optional :: ABORT

  Integer(Kind=JPIM) :: HANDLE4, RC4 = 0
  Logical(Kind=JPIM) :: ABORT4 = .FALSE.

  integer :: o=0,o1=0

  if (present(abort)) o1=1
  o=o1

  HANDLE4 = HANDLE
  If (PRESENT(ABORT)) ABORT4 = ABORT

  select case(o)
  case(0)
     RC4=ODB_addview4(handle4, dtname, viewfile, select, uniqueby, from, where, orderby, sortby, query, set)
  case(1)
     RC4=ODB_addview4(handle4, dtname, viewfile, select, uniqueby, from, where, orderby, sortby, query, set, &
                      abort=abort4)
  end select

  ODB_addview = RC4

End Function ODB_addview

!
! ODB_select ==========================================================
!
Integer(Kind=8) Function ODB_select(HANDLE, DTNAME, NROWS, NCOLS, NRA, &
                                    POOLNO, SETVARS, VALUES, PEVAR,    &
                                    REPLICATE_PE, SYNC, NPES_OVERRIDE)

  Use odb_module, only : ODB_select4 => ODB_select

  Integer(Kind=8), Intent(IN) :: HANDLE
  Character(Len=*), Intent(IN) :: DTNAME
  Integer(Kind=8), Intent(OUT) :: NROWS, NCOLS
  Integer(Kind=8), Intent(OUT), Optional :: NRA
  Integer(Kind=8), Intent(IN), Optional :: POOLNO, REPLICATE_PE,       &
                                           NPES_OVERRIDE
  Character(Len=*), Dimension(:), Intent(IN), Optional :: SETVARS
  Real(Kind=8), Dimension(:), Intent(IN), Optional :: VALUES
  Character(Len=*), Intent(IN), Optional :: PEVAR
  Logical(Kind=8), Intent(IN), Optional :: SYNC

  Integer(Kind=JPIM) :: HANDLE4, RC4 = 0, NROWS4, NCOLS4, NRA4,        &
                        POOLNO4 = -1, REPLICATE_PE4 = -1,              &
                        NPES_OVERRIDE4 = -1
  Logical(Kind=JPIM) :: SYNC4 = .FALSE.

  integer :: o=0,o1=0,o2=0,o3=0,o4=0,o5=0

  if (present(nra)) o1=1
  if (present(poolno)) o2=2
  if (present(replicate_pe)) o3=4
  if (present(sync)) o4=8
  if (present(npes_override)) o5=16
  o=o1+o2+o3+o4+o5

  HANDLE4 = HANDLE
  If (PRESENT(POOLNO))        POOLNO4        = POOLNO
  If (PRESENT(REPLICATE_PE))  REPLICATE_PE4  = REPLICATE_PE
  If (PRESENT(NPES_OVERRIDE)) NPES_OVERRIDE4 = NPES_OVERRIDE
  If (PRESENT(SYNC))          SYNC4          = SYNC

  select case(o)
  case(0)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar)
  case(1)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4)
  case(2)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     poolno=poolno4)
  case(3)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, poolno=poolno4)
  case(4)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     replicate_pe=replicate_pe4)
  case(5)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, replicate_pe=replicate_pe4)
  case(6)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     poolno=poolno4, replicate_pe=replicate_pe4)
  case(7)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, poolno=poolno4, replicate_pe=replicate_pe4)
  case(8)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     sync=sync4)
  case(9)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, sync=sync4)
  case(10)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     poolno=poolno4, sync=sync4)
  case(11)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, poolno=poolno4, sync=sync4)
  case(12)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     replicate_pe=replicate_pe4, sync=sync4)
  case(13)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, replicate_pe=replicate_pe4, sync=sync4)
  case(14)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     poolno=poolno4, replicate_pe=replicate_pe4, sync=sync4)
  case(15)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, poolno=poolno4, replicate_pe=replicate_pe4, sync=sync4)
  case(16)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     npes_override=npes_override4)
  case(17)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, npes_override=npes_override4)
  case(18)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     poolno=poolno4, npes_override=npes_override4)
  case(19)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, poolno=poolno4, npes_override=npes_override4)
  case(20)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     replicate_pe=replicate_pe4, npes_override=npes_override4)
  case(21)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, replicate_pe=replicate_pe4, npes_override=npes_override4)
  case(22)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     poolno=poolno4, replicate_pe=replicate_pe4, npes_override=npes_override4)
  case(23)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, poolno=poolno4, replicate_pe=replicate_pe4, npes_override=npes_override4)
  case(24)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     sync=sync4, npes_override=npes_override4)
  case(25)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, sync=sync4, npes_override=npes_override4)
  case(26)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     poolno=poolno4, sync=sync4, npes_override=npes_override4)
  case(27)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, poolno=poolno4, sync=sync4, npes_override=npes_override4)
  case(28)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     replicate_pe=replicate_pe4, sync=sync4, npes_override=npes_override4)
  case(29)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, replicate_pe=replicate_pe4, sync=sync4, npes_override=npes_override4)
  case(30)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     poolno=poolno4, replicate_pe=replicate_pe4, sync=sync4, npes_override=npes_override4)
  case(31)
     RC4=ODB_select4(handle4, dtname, nrows4, ncols4, setvars=setvars, values=values, pevar=pevar, &
                     nra=nra4, poolno=poolno4, replicate_pe=replicate_pe4, sync=sync4, npes_override=npes_override4)
  end select

  NROWS = NROWS4
  NCOLS = NCOLS4
  IF (PRESENT(NRA)) NRA = NRA4
  ODB_select = RC4

End Function ODB_select

!
! ODB_release ==========================================================
!
Integer(Kind=8) Function ODB_release(HANDLE, POOLNO)

  Use odb_module, only : ODB_release4 => ODB_release

  Integer(Kind=8), Intent(IN) :: HANDLE
  Integer(Kind=8), Intent(IN), Optional :: POOLNO

  Integer(Kind=JPIM) :: HANDLE4 = 0, POOLNO4 = -1, RC4 = 0

  integer :: o=0,o1=0

  if (present(poolno)) o1=1
  o=o1

  HANDLE4 = HANDLE
  If (PRESENT(POOLNO)) POOLNO4 = POOLNO

  select case(o)
  case(0)
     RC4=ODB_release4(handle4)
  case(1)
     RC4=ODB_release4(handle4, poolno=poolno4)
  end select

  ODB_release = RC4

End Function ODB_release

Subroutine ODB_getindices(handle, view, table, nrows, ncols, nstart_pos, poolno, indices)

  Use odb_module, only : ODB_getindices4 => ODB_getindices

  implicit none

  INTEGER(KIND=8), intent(in)            :: handle
  character(len=*), intent(in)           :: view, table
  INTEGER(KIND=8), intent(out)           :: nrows, ncols
  INTEGER(KIND=8), intent(in)            :: nstart_pos
  INTEGER(KIND=8), intent(in)            :: poolno
  INTEGER(KIND=8), intent(out), OPTIONAL :: indices(:,:)

  INTEGER(KIND=JPIM)              :: handle4
  INTEGER(KIND=JPIM)              :: nrows4, ncols4
  INTEGER(KIND=JPIM)              :: nstart_pos4
  INTEGER(KIND=JPIM)              :: poolno4
  INTEGER(KIND=JPIM) ,allocatable :: indices4(:,:)

  integer :: o=0,o1=0

  handle4 = handle4
  nstart_pos4 = nstart_pos
  poolno4 = poolno
  if (present(indices)) allocate(indices4(size(indices,1),size(indices,2)))

  if (present(indices)) o1=1
  o=o1

  select case(o)
  case(0)
     call ODB_getindices4(handle4, view, table, nrows4, ncols4, nstart_pos4, poolno4)
  case(1)
     call ODB_getindices4(handle4, view, table, nrows4, ncols4, nstart_pos4, poolno4, indices=indices4)
  end select

  nrows = nrows4
  ncols = ncols4
  if (present(indices)) then
    indices = indices4
    deallocate(indices4)
  endif

End Subroutine ODB_getindices

Subroutine ODB_putindices(handle, view, table, nrows, ncols, nstart_pos, poolno, indices)

  Use odb_module, only : ODB_putindices4 => ODB_putindices

  implicit none

  INTEGER(KIND=8), intent(in)            :: handle
  character(len=*), intent(in)           :: view, table
  INTEGER(KIND=8), intent(in)            :: nrows, ncols
  INTEGER(KIND=8), intent(in)            :: nstart_pos
  INTEGER(KIND=8), intent(in)            :: poolno
  INTEGER(KIND=8), intent(in)            :: indices(:,:)

  INTEGER(KIND=JPIM)              :: handle4
  INTEGER(KIND=JPIM)              :: nrows4, ncols4
  INTEGER(KIND=JPIM)              :: nstart_pos4
  INTEGER(KIND=JPIM)              :: poolno4
  INTEGER(KIND=JPIM) ,allocatable :: indices4(:,:)

  handle4 = handle4
  nrows4 = nrows
  ncols4 = ncols4
  nstart_pos4 = nstart_pos
  poolno4 = poolno
  allocate(indices4(size(indices,1),size(indices,2)))
  indices4 = indices

  call ODB_putindices4(handle4, view, table, nrows4, ncols4, nstart_pos4, poolno4, indices=indices4)

  deallocate(indices4)

End Subroutine ODB_putindices

!
! ODB_close ===========================================================
!
Integer(Kind=8) Function ODB_close(HANDLE, SAVE)

  Use odb_module, only : ODB_close4 => ODB_close

  Integer(Kind=8), Intent(IN) :: HANDLE
  Logical(Kind=8), Intent(IN), Optional :: SAVE

  Integer(Kind=JPIM) :: HANDLE4 = 0, RC4
  Logical(Kind=JPIM) :: SAVE4

  integer :: o=0,o1=0

  if (present(save)) o1=1
  o=o1

  HANDLE4 = HANDLE
  If (PRESENT(SAVE)) Then
    SAVE4 = SAVE
  Else
    SAVE4 = .FALSE.
  End If

  select case(o)
  case(0)
     RC4=ODB_close4(handle4)
  case(1)
     RC4=ODB_close4(handle4, save=save4)
  end select

  ODB_close = RC4

End Function ODB_close

! =====================================================================
! wrapper functions for odbgetput.F90
! =====================================================================

!
! ODB_get =============================================================
!
Integer(Kind=8) Function ODB_get(HANDLE, DTNAME, D, NROWS, NCOLS,      &
                                 POOLNO, COLGET, COLPACK, COLFREE,     &
                                 INDEX, SORTED, OFFSET, START, LIMIT)

  Use odb_module, only : ODB_get4 => ODB_get

  Integer(Kind=8), Intent(IN) :: HANDLE
  Character(Len=*), Intent(IN) :: DTNAME
  Real(Kind=8), Intent(OUT) :: D(:,0:)
  Integer(Kind=8), Intent(INOUT) :: NROWS
  Integer(Kind=8), Intent(INOUT), Optional :: NCOLS
  Integer(Kind=8), Intent(IN), Optional :: POOLNO, OFFSET, START, LIMIT
  Logical(Kind=8), Dimension(:), Intent(IN), Optional ::               &
    COLGET, COLPACK, COLFREE
  Integer(Kind=8), Dimension(:), Intent(OUT), Optional :: INDEX
  Logical(Kind=8), Intent(IN), Optional :: SORTED

  Integer(Kind=JPIM) :: HANDLE4, RC4 = 0, NROWS4, NCOLS4 = -1,         &
                        POOLNO4 = -1, OFFSET4 = -1, START4 = -1,       &
                        LIMIT4 = -1
  Logical(Kind=JPIM), Dimension(:), Allocatable ::                     &
    COLGET4, COLPACK4, COLFREE4
  Integer(Kind=JPIM), Dimension(:), Allocatable :: INDEX4
  Logical(Kind=JPIM) :: SORTED4 = .FALSE.

  integer :: o=0,o1=0,o2=0,o3=0,o4=0,o5=0,o6=0,o7=0,o8=0,o9=0

  if (present(poolno)) o1=1
  if (present(colget)) o2=2
  if (present(colpack)) o3=4
  if (present(colfree)) o4=8
  if (present(index)) o5=16
  if (present(sorted)) o6=32
  if (present(offset)) o7=64
  if (present(start)) o8=128
  if (present(limit)) o9=256
  o=o1+o2+o3+o4+o5+o6+o7+o8+o9

  HANDLE4 = HANDLE
  NROWS4 = NROWS
  If (PRESENT(NCOLS))  NCOLS4  = NCOLS
  If (PRESENT(POOLNO)) POOLNO4 = POOLNO
  If (PRESENT(OFFSET)) OFFSET4 = OFFSET
  If (PRESENT(START))  START4  = START
  If (PRESENT(LIMIT))  LIMIT4  = LIMIT
  If (PRESENT(SORTED)) SORTED4 = SORTED
  If (PRESENT(COLGET)) Then
    Allocate(COLGET4(SIZE(COLGET)))
    COLGET4 = COLGET
  End If
  If (PRESENT(COLPACK)) Then
    Allocate(COLPACK4(SIZE(COLPACK)))
    COLPACK4 = COLPACK
  End If
  If (PRESENT(COLFREE)) Then
    Allocate(COLFREE4(SIZE(COLFREE)))
    COLFREE4 = COLFREE
  End If
  If (PRESENT(INDEX)) Then
    Allocate(INDEX4(SIZE(INDEX)))
  End If

  select case(o)
  case(0)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4)
  case(1)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4)
  case(2)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4)
  case(3)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4)
  case(4)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4)
  case(5)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4)
  case(6)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4)
  case(7)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4)
  case(8)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4)
  case(9)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4)
  case(10)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4)
  case(11)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4)
  case(12)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4)
  case(13)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4)
  case(14)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4)
  case(15)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4)
  case(16)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4)
  case(17)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4)
  case(18)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4)
  case(19)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4)
  case(20)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4)
  case(21)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4)
  case(22)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4)
  case(23)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4)
  case(24)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4)
  case(25)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4)
  case(26)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4)
  case(27)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4)
  case(28)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4)
  case(29)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, index=index4)
  case(30)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, index=index4)
  case(31)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, index=index4)
  case(32)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4)
  case(33)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4)
  case(34)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, sorted=sorted4)
  case(35)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, sorted=sorted4)
  case(36)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4)
  case(37)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4)
  case(38)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, sorted=sorted4)
  case(39)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, sorted=sorted4)
  case(40)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, sorted=sorted4)
  case(41)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, sorted=sorted4)
  case(42)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, sorted=sorted4)
  case(43)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, sorted=sorted4)
  case(44)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, sorted=sorted4)
  case(45)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, sorted=sorted4)
  case(46)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, sorted=sorted4)
  case(47)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, sorted=sorted4)
  case(48)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, sorted=sorted4)
  case(49)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, sorted=sorted4)
  case(50)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, sorted=sorted4)
  case(51)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, sorted=sorted4)
  case(52)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, sorted=sorted4)
  case(53)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, sorted=sorted4)
  case(54)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, sorted=sorted4)
  case(55)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  index=index4, sorted=sorted4)
  case(56)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, sorted=sorted4)
  case(57)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, sorted=sorted4)
  case(58)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, sorted=sorted4)
  case(59)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, &
                  index=index4, sorted=sorted4)
  case(60)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, sorted=sorted4)
  case(61)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4)
  case(62)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4)
  case(63)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, index=index4, sorted=sorted4)
  case(64)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, offset=offset4)
  case(65)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, offset=offset4)
  case(66)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, offset=offset4)
  case(67)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, offset=offset4)
  case(68)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, offset=offset4)
  case(69)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, offset=offset4)
  case(70)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, offset=offset4)
  case(71)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, offset=offset4)
  case(72)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, offset=offset4)
  case(73)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, offset=offset4)
  case(74)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, offset=offset4)
  case(75)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, offset=offset4)
  case(76)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, offset=offset4)
  case(77)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, offset=offset4)
  case(78)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, offset=offset4)
  case(79)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, offset=offset4)
  case(80)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, offset=offset4)
  case(81)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, offset=offset4)
  case(82)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, offset=offset4)
  case(83)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, offset=offset4)
  case(84)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, offset=offset4)
  case(85)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, offset=offset4)
  case(86)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, offset=offset4)
  case(87)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  index=index4, offset=offset4)
  case(88)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, offset=offset4)
  case(89)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, offset=offset4)
  case(90)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, offset=offset4)
  case(91)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, &
                  index=index4, offset=offset4)
  case(92)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, offset=offset4)
  case(93)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  index=index4, offset=offset4)
  case(94)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, offset=offset4)
  case(95)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, index=index4, offset=offset4)
  case(96)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4, offset=offset4)
  case(97)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4, offset=offset4)
  case(98)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, sorted=sorted4, offset=offset4)
  case(99)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, sorted=sorted4, offset=offset4)
  case(100)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4, offset=offset4)
  case(101)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4, offset=offset4)
  case(102)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, sorted=sorted4, offset=offset4)
  case(103)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  sorted=sorted4, offset=offset4)
  case(104)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, sorted=sorted4, offset=offset4)
  case(105)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, sorted=sorted4, offset=offset4)
  case(106)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, sorted=sorted4, offset=offset4)
  case(107)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, &
                  sorted=sorted4, offset=offset4)
  case(108)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, sorted=sorted4, offset=offset4)
  case(109)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, offset=offset4)
  case(110)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, offset=offset4)
  case(111)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, sorted=sorted4, offset=offset4)
  case(112)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, sorted=sorted4, offset=offset4)
  case(113)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, sorted=sorted4, offset=offset4)
  case(114)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, sorted=sorted4, offset=offset4)
  case(115)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, sorted=sorted4, &
                  offset=offset4)
  case(116)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, sorted=sorted4, offset=offset4)
  case(117)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, sorted=sorted4, &
                  offset=offset4)
  case(118)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, sorted=sorted4, &
                  offset=offset4)
  case(119)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  sorted=sorted4, offset=offset4)
  case(120)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, sorted=sorted4, offset=offset4)
  case(121)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4)
  case(122)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4)
  case(123)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4)
  case(124)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4)
  case(125)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, offset=offset4)
  case(126)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, offset=offset4)
  case(127)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, index=index4, sorted=sorted4, offset=offset4)
  case(128)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, start=start4)
  case(129)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, start=start4)
  case(130)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, start=start4)
  case(131)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, start=start4)
  case(132)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, start=start4)
  case(133)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, start=start4)
  case(134)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, start=start4)
  case(135)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, start=start4)
  case(136)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, start=start4)
  case(137)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, start=start4)
  case(138)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, start=start4)
  case(139)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, start=start4)
  case(140)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, start=start4)
  case(141)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, start=start4)
  case(142)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, start=start4)
  case(143)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, start=start4)
  case(144)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, start=start4)
  case(145)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, start=start4)
  case(146)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, start=start4)
  case(147)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, start=start4)
  case(148)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, start=start4)
  case(149)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, start=start4)
  case(150)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, start=start4)
  case(151)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  start=start4)
  case(152)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, start=start4)
  case(153)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, start=start4)
  case(154)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, start=start4)
  case(155)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  start=start4)
  case(156)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, start=start4)
  case(157)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  index=index4, start=start4)
  case(158)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, start=start4)
  case(159)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, index=index4, start=start4)
  case(160)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4, start=start4)
  case(161)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4, start=start4)
  case(162)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, sorted=sorted4, start=start4)
  case(163)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, sorted=sorted4, start=start4)
  case(164)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4, start=start4)
  case(165)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4, start=start4)
  case(166)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, sorted=sorted4, start=start4)
  case(167)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  sorted=sorted4, start=start4)
  case(168)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, sorted=sorted4, start=start4)
  case(169)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, sorted=sorted4, start=start4)
  case(170)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, sorted=sorted4, start=start4)
  case(171)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, &
                  sorted=sorted4, start=start4)
  case(172)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, sorted=sorted4, start=start4)
  case(173)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, start=start4)
  case(174)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, start=start4)
  case(175)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, sorted=sorted4, start=start4)
  case(176)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, sorted=sorted4, start=start4)
  case(177)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, sorted=sorted4, start=start4)
  case(178)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, sorted=sorted4, start=start4)
  case(179)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, sorted=sorted4, &
                  start=start4)
  case(180)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, sorted=sorted4, start=start4)
  case(181)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, sorted=sorted4, &
                  start=start4)
  case(182)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, sorted=sorted4, &
                  start=start4)
  case(183)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  sorted=sorted4, start=start4)
  case(184)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, sorted=sorted4, start=start4)
  case(185)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, sorted=sorted4, &
                  start=start4)
  case(186)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, sorted=sorted4, &
                  start=start4)
  case(187)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  sorted=sorted4, start=start4)
  case(188)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, start=start4)
  case(189)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, start=start4)
  case(190)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, start=start4)
  case(191)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, index=index4, sorted=sorted4, start=start4)
  case(192)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, offset=offset4, start=start4)
  case(193)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, offset=offset4, start=start4)
  case(194)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, offset=offset4, start=start4)
  case(195)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, offset=offset4, start=start4)
  case(196)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, offset=offset4, start=start4)
  case(197)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, offset=offset4, start=start4)
  case(198)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, offset=offset4, start=start4)
  case(199)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  offset=offset4, start=start4)
  case(200)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, offset=offset4, start=start4)
  case(201)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, offset=offset4, start=start4)
  case(202)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, offset=offset4, start=start4)
  case(203)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, &
                  offset=offset4, start=start4)
  case(204)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, offset=offset4, start=start4)
  case(205)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  offset=offset4, start=start4)
  case(206)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  offset=offset4, start=start4)
  case(207)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, offset=offset4, start=start4)
  case(208)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, offset=offset4, start=start4)
  case(209)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, offset=offset4, start=start4)
  case(210)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, offset=offset4, start=start4)
  case(211)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, offset=offset4, &
                  start=start4)
  case(212)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, offset=offset4, start=start4)
  case(213)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, offset=offset4, &
                  start=start4)
  case(214)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, offset=offset4, &
                  start=start4)
  case(215)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  offset=offset4, start=start4)
  case(216)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, offset=offset4, start=start4)
  case(217)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, offset=offset4, &
                  start=start4)
  case(218)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, offset=offset4, &
                  start=start4)
  case(219)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  offset=offset4, start=start4)
  case(220)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, &
                  offset=offset4, start=start4)
  case(221)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  index=index4, offset=offset4, start=start4)
  case(222)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, offset=offset4, start=start4)
  case(223)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, index=index4, offset=offset4, start=start4)
  case(224)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4, offset=offset4, start=start4)
  case(225)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4, offset=offset4, start=start4)
  case(226)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, sorted=sorted4, offset=offset4, start=start4)
  case(227)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, sorted=sorted4, offset=offset4, &
                  start=start4)
  case(228)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4, offset=offset4, start=start4)
  case(229)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4, offset=offset4, &
                  start=start4)
  case(230)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, sorted=sorted4, offset=offset4, &
                  start=start4)
  case(231)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, sorted=sorted4, &
                  offset=offset4, start=start4)
  case(232)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, sorted=sorted4, offset=offset4, start=start4)
  case(233)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, sorted=sorted4, offset=offset4, &
                  start=start4)
  case(234)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, sorted=sorted4, offset=offset4, &
                  start=start4)
  case(235)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, sorted=sorted4, &
                  offset=offset4, start=start4)
  case(236)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, sorted=sorted4, &
                  offset=offset4, start=start4)
  case(237)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, offset=offset4, start=start4)
  case(238)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, offset=offset4, start=start4)
  case(239)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, sorted=sorted4, offset=offset4, start=start4)
  case(240)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, sorted=sorted4, offset=offset4, start=start4)
  case(241)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, sorted=sorted4, offset=offset4, &
                  start=start4)
  case(242)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, sorted=sorted4, offset=offset4, &
                  start=start4)
  case(243)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4)
  case(244)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, sorted=sorted4, offset=offset4, &
                  start=start4)
  case(245)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4)
  case(246)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4)
  case(247)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  sorted=sorted4, offset=offset4, start=start4)
  case(248)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, sorted=sorted4, offset=offset4, &
                  start=start4)
  case(249)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4)
  case(250)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4)
  case(251)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4, start=start4)
  case(252)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4, start=start4)
  case(253)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, offset=offset4, start=start4)
  case(254)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, offset=offset4, start=start4)
  case(255)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
                  colfree=colfree4, index=index4, sorted=sorted4, offset=offset4, start=start4)
  case(256)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, limit=limit4)
  case(257)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, limit=limit4)
  case(258)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, limit=limit4)
  case(259)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, limit=limit4)
  case(260)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, limit=limit4)
  case(261)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, limit=limit4)
  case(262)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, limit=limit4)
  case(263)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, limit=limit4)
  case(264)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, limit=limit4)
  case(265)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, limit=limit4)
  case(266)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, limit=limit4)
  case(267)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, limit=limit4)
  case(268)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, limit=limit4)
  case(269)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, limit=limit4)
  case(270)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, limit=limit4)
  case(271)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, &
             colfree=colfree4, limit=limit4)
  case(272)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, limit=limit4)
  case(273)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, limit=limit4)
  case(274)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, limit=limit4)
  case(275)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, limit=limit4)
  case(276)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, limit=limit4)
  case(277)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, limit=limit4)
  case(278)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, limit=limit4)
  case(279)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, limit=limit4)
  case(280)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, limit=limit4)
  case(281)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, limit=limit4)
  case(282)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, limit=limit4)
  case(283)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, limit=limit4)
  case(284)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, limit=limit4)
  case(285)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, index=index4, &
                  limit=limit4)
  case(286)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, index=index4, &
                  limit=limit4)
  case(287)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, limit=limit4)
  case(288)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4, limit=limit4)
  case(289)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4, limit=limit4)
  case(290)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, sorted=sorted4, limit=limit4)
  case(291)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, sorted=sorted4, limit=limit4)
  case(292)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4, limit=limit4)
  case(293)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4, limit=limit4)
  case(294)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, sorted=sorted4, limit=limit4)
  case(295)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, sorted=sorted4, &
                  limit=limit4)
  case(296)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, sorted=sorted4, limit=limit4)
  case(297)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, sorted=sorted4, limit=limit4)
  case(298)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, sorted=sorted4, limit=limit4)
  case(299)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, sorted=sorted4, &
                  limit=limit4)
  case(300)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, sorted=sorted4, limit=limit4)
  case(301)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, sorted=sorted4, &
                  limit=limit4)
  case(302)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, sorted=sorted4, &
                  limit=limit4)
  case(303)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, limit=limit4)
  case(304)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, sorted=sorted4, limit=limit4)
  case(305)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, sorted=sorted4, limit=limit4)
  case(306)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, sorted=sorted4, limit=limit4)
  case(307)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, sorted=sorted4, limit=limit4)
  case(308)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, sorted=sorted4, limit=limit4)
  case(309)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, sorted=sorted4, limit=limit4)
  case(310)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, sorted=sorted4, limit=limit4)
  case(311)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  sorted=sorted4, limit=limit4)
  case(312)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, sorted=sorted4, limit=limit4)
  case(313)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, sorted=sorted4, limit=limit4)
  case(314)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, sorted=sorted4, limit=limit4)
  case(315)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  sorted=sorted4, limit=limit4)
  case(316)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, sorted=sorted4, &
                  limit=limit4)
  case(317)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, limit=limit4)
  case(318)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, limit=limit4)
  case(319)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, limit=limit4)
  case(320)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, offset=offset4, limit=limit4)
  case(321)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, offset=offset4, limit=limit4)
  case(322)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, offset=offset4, limit=limit4)
  case(323)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, offset=offset4, limit=limit4)
  case(324)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, offset=offset4, limit=limit4)
  case(325)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, offset=offset4, limit=limit4)
  case(326)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, offset=offset4, limit=limit4)
  case(327)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, offset=offset4, &
                  limit=limit4)
  case(328)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, offset=offset4, limit=limit4)
  case(329)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, offset=offset4, limit=limit4)
  case(330)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, offset=offset4, limit=limit4)
  case(331)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, offset=offset4, &
                  limit=limit4)
  case(332)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, offset=offset4, limit=limit4)
  case(333)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, offset=offset4, &
                  limit=limit4)
  case(334)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, offset=offset4, &
                  limit=limit4)
  case(335)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  offset=offset4, limit=limit4)
  case(336)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, offset=offset4, limit=limit4)
  case(337)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, offset=offset4, limit=limit4)
  case(338)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, offset=offset4, limit=limit4)
  case(339)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, offset=offset4, limit=limit4)
  case(340)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, offset=offset4, limit=limit4)
  case(341)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, offset=offset4, limit=limit4)
  case(342)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, offset=offset4, limit=limit4)
  case(343)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  offset=offset4, limit=limit4)
  case(344)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, offset=offset4, limit=limit4)
  case(345)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, offset=offset4, limit=limit4)
  case(346)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, offset=offset4, limit=limit4)
  case(347)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  offset=offset4, limit=limit4)
  case(348)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, offset=offset4, &
                  limit=limit4)
  case(349)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, index=index4, &
                  offset=offset4, limit=limit4)
  case(350)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, index=index4, &
                  offset=offset4, limit=limit4)
  case(351)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, offset=offset4, limit=limit4)
  case(352)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4, offset=offset4, limit=limit4)
  case(353)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4, offset=offset4, limit=limit4)
  case(354)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, sorted=sorted4, offset=offset4, limit=limit4)
  case(355)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, sorted=sorted4, offset=offset4, limit=limit4)
  case(356)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4, offset=offset4, limit=limit4)
  case(357)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4, offset=offset4, &
                  limit=limit4)
  case(358)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, sorted=sorted4, offset=offset4, &
                  limit=limit4)
  case(359)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(360)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, sorted=sorted4, offset=offset4, limit=limit4)
  case(361)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, sorted=sorted4, offset=offset4, &
                  limit=limit4)
  case(362)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, sorted=sorted4, offset=offset4, &
                  limit=limit4)
  case(363)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(364)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, sorted=sorted4, offset=offset4, &
                  limit=limit4)
  case(365)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(366)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(367)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, offset=offset4, limit=limit4)
  case(368)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, sorted=sorted4, offset=offset4, limit=limit4)
  case(369)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, sorted=sorted4, offset=offset4, limit=limit4)
  case(370)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, sorted=sorted4, offset=offset4, limit=limit4)
  case(371)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(372)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, sorted=sorted4, offset=offset4, limit=limit4)
  case(373)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(374)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(375)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  sorted=sorted4, offset=offset4, limit=limit4)
  case(376)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, sorted=sorted4, offset=offset4, &
                  limit=limit4)
  case(377)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(378)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(379)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4, limit=limit4)
  case(380)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4, limit=limit4)
  case(381)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4, limit=limit4)
  case(382)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4, limit=limit4)
  case(383)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, offset=offset4, limit=limit4)
  case(384)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, start=start4, limit=limit4)
  case(385)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, start=start4, limit=limit4)
  case(386)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, start=start4, limit=limit4)
  case(387)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, start=start4, limit=limit4)
  case(388)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, start=start4, limit=limit4)
  case(389)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, start=start4, limit=limit4)
  case(390)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, start=start4, limit=limit4)
  case(391)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, start=start4, limit=limit4)
  case(392)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, start=start4, limit=limit4)
  case(393)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, start=start4, limit=limit4)
  case(394)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, start=start4, limit=limit4)
  case(395)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, start=start4, limit=limit4)
  case(396)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, start=start4, limit=limit4)
  case(397)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, start=start4, &
                  limit=limit4)
  case(398)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, start=start4, &
                  limit=limit4)
  case(399)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  start=start4, limit=limit4)
  case(400)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, start=start4, limit=limit4)
  case(401)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, start=start4, limit=limit4)
  case(402)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, start=start4, limit=limit4)
  case(403)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, start=start4, limit=limit4)
  case(404)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, start=start4, limit=limit4)
  case(405)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, start=start4, limit=limit4)
  case(406)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, start=start4, limit=limit4)
  case(407)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  start=start4, limit=limit4)
  case(408)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, start=start4, limit=limit4)
  case(409)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, start=start4, limit=limit4)
  case(410)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, start=start4, limit=limit4)
  case(411)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  start=start4, limit=limit4)
  case(412)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, start=start4, limit=limit4)
  case(413)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, index=index4, &
                  start=start4, limit=limit4)
  case(414)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, index=index4, &
                  start=start4, limit=limit4)
  case(415)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, start=start4, limit=limit4)
  case(416)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4, start=start4, limit=limit4)
  case(417)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4, start=start4, limit=limit4)
  case(418)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, sorted=sorted4, start=start4, limit=limit4)
  case(419)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, sorted=sorted4, start=start4, limit=limit4)
  case(420)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4, start=start4, limit=limit4)
  case(421)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4, start=start4, limit=limit4)
  case(422)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, sorted=sorted4, start=start4, limit=limit4)
  case(423)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(424)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, sorted=sorted4, start=start4, limit=limit4)
  case(425)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, sorted=sorted4, start=start4, limit=limit4)
  case(426)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, sorted=sorted4, start=start4, limit=limit4)
  case(427)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(428)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, sorted=sorted4, start=start4, &
                  limit=limit4)
  case(429)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(430)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(431)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, start=start4, limit=limit4)
  case(432)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, sorted=sorted4, start=start4, limit=limit4)
  case(433)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, sorted=sorted4, start=start4, limit=limit4)
  case(434)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, sorted=sorted4, start=start4, limit=limit4)
  case(435)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(436)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, sorted=sorted4, start=start4, limit=limit4)
  case(437)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(438)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(439)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  sorted=sorted4, start=start4, limit=limit4)
  case(440)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, sorted=sorted4, start=start4, limit=limit4)
  case(441)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(442)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(443)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  sorted=sorted4, start=start4, limit=limit4)
  case(444)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, sorted=sorted4, &
                  start=start4, limit=limit4)
  case(445)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, start=start4, limit=limit4)
  case(446)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, start=start4, limit=limit4)
  case(447)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, start=start4, limit=limit4)
  case(448)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, offset=offset4, start=start4, limit=limit4)
  case(449)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, offset=offset4, start=start4, limit=limit4)
  case(450)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, offset=offset4, start=start4, limit=limit4)
  case(451)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, offset=offset4, start=start4, limit=limit4)
  case(452)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, offset=offset4, start=start4, limit=limit4)
  case(453)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, offset=offset4, start=start4, limit=limit4)
  case(454)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, offset=offset4, start=start4, limit=limit4)
  case(455)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, offset=offset4, &
                  start=start4, limit=limit4)
  case(456)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, offset=offset4, start=start4, limit=limit4)
  case(457)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, offset=offset4, start=start4, limit=limit4)
  case(458)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, offset=offset4, start=start4, limit=limit4)
  case(459)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, offset=offset4, &
                  start=start4, limit=limit4)
  case(460)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, offset=offset4, start=start4, &
                  limit=limit4)
  case(461)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, offset=offset4, &
                  start=start4, limit=limit4)
  case(462)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, offset=offset4, &
                  start=start4, limit=limit4)
  case(463)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  offset=offset4, start=start4, limit=limit4)
  case(464)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, offset=offset4, start=start4, limit=limit4)
  case(465)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, offset=offset4, start=start4, limit=limit4)
  case(466)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, offset=offset4, start=start4, limit=limit4)
  case(467)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, offset=offset4, &
                  start=start4, limit=limit4)
  case(468)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, offset=offset4, start=start4, limit=limit4)
  case(469)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, offset=offset4, &
                  start=start4, limit=limit4)
  case(470)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, offset=offset4, &
                  start=start4, limit=limit4)
  case(471)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  offset=offset4, start=start4, limit=limit4)
  case(472)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, offset=offset4, start=start4, limit=limit4)
  case(473)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, offset=offset4, &
                  start=start4, limit=limit4)
  case(474)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, offset=offset4, &
                  start=start4, limit=limit4)
  case(475)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  offset=offset4, start=start4, limit=limit4)
  case(476)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, offset=offset4, &
                  start=start4, limit=limit4)
  case(477)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, index=index4, &
                  offset=offset4, start=start4, limit=limit4)
  case(478)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, index=index4, &
                  offset=offset4, start=start4, limit=limit4)
  case(479)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, offset=offset4, start=start4, limit=limit4)
  case(480)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(481)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(482)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(483)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(484)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(485)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(486)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(487)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(488)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, sorted=sorted4, offset=offset4, start=start4, &
                  limit=limit4)
  case(489)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(490)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(491)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(492)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(493)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(494)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(495)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(496)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, index=index4, sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(497)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, index=index4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(498)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, index=index4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(499)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(500)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, index=index4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(501)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(502)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(503)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, index=index4, &
                  sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(504)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colfree=colfree4, index=index4, sorted=sorted4, offset=offset4, &
                  start=start4, limit=limit4)
  case(505)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(506)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(507)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(508)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, colfree=colfree4, index=index4, sorted=sorted4, &
                  offset=offset4, start=start4, limit=limit4)
  case(509)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(510)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, colget=colget4, colpack=colpack4, colfree=colfree4, index=index4, &
                  sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  case(511)
     RC4=ODB_get4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colget=colget4, colpack=colpack4, colfree=colfree4, &
                  index=index4, sorted=sorted4, offset=offset4, start=start4, limit=limit4)
  end select

  NROWS = NROWS4
  If (PRESENT(NCOLS)) NCOLS = NCOLS4
  If (PRESENT(INDEX)) Then
    INDEX = INDEX4
    Deallocate(INDEX4)
  End If
  If (Allocated(COLGET4))  Deallocate(COLGET4)
  If (Allocated(COLPACK4)) Deallocate(COLPACK4)
  If (Allocated(COLFREE4)) Deallocate(COLFREE4)
  ODB_get = RC4

End Function ODB_get

!
! ODB_put =============================================================
!
Integer(Kind=8) Function ODB_put(HANDLE, DTNAME, D, NROWS, NCOLS,      &
                                 POOLNO, COLPUT, COLPACK, SORTED,      &
                                 OFFSET)

  Use odb_module, only : ODB_put4 => ODB_put

  Integer(Kind=8), Intent(IN) :: HANDLE
  Character(Len=*), Intent(IN) :: DTNAME
  Real(Kind=8), Intent(INOUT) :: D(:,0:)
  Integer(Kind=8), Intent(INOUT) :: NROWS
  Integer(Kind=8), Intent(INOUT), Optional :: NCOLS
  Integer(Kind=8), Intent(IN), Optional :: POOLNO, OFFSET
  Logical(Kind=8), Dimension(:), Intent(IN), Optional :: COLPUT, COLPACK
  Logical(Kind=8), Intent(IN), Optional :: SORTED

  Integer(Kind=JPIM) :: HANDLE4, RC4 = 0, NROWS4, NCOLS4 = -1,         &
                        POOLNO4 = -1, OFFSET4 = -1
  Logical(Kind=JPIM), Dimension(:), Allocatable :: COLPUT4, COLPACK4
  Logical(Kind=JPIM) :: SORTED4 = .FALSE.

  integer :: o=0,o1=0,o2=0,o3=0,o4=0,o5=0

  if (present(poolno)) o1=1
  if (present(colput)) o2=2
  if (present(colpack)) o3=4
  if (present(sorted)) o4=8
  if (present(offset)) o5=16
  o=o1+o2+o3+o4+o5


  HANDLE4 = HANDLE
  NROWS4 = NROWS
  If (PRESENT(NCOLS))  NCOLS4  = NCOLS
  If (PRESENT(POOLNO)) POOLNO4 = POOLNO
  If (PRESENT(OFFSET)) OFFSET4 = OFFSET
  If (PRESENT(SORTED)) SORTED4 = SORTED
  If (PRESENT(COLPUT)) Then
    Allocate(COLPUT4(SIZE(COLPUT)))
    COLPUT4 = COLPUT
  End If
  If (PRESENT(COLPACK)) Then
    Allocate(COLPACK4(SIZE(COLPACK)))
    COLPACK4 = COLPACK
  End If

  select case(o)
  case(0)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4)
  case(1)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4)
  case(2)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colput=colput4)
  case(3)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colput=colput4)
  case(4)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4)
  case(5)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4)
  case(6)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colput=colput4, colpack=colpack4)
  case(7)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colput=colput4, colpack=colpack4)
  case(8)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4)
  case(9)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4)
  case(10)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colput=colput4, sorted=sorted4)
  case(11)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colput=colput4, sorted=sorted4)
  case(12)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4)
  case(13)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4)
  case(14)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colput=colput4, colpack=colpack4, sorted=sorted4)
  case(15)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colput=colput4, colpack=colpack4, sorted=sorted4)
  case(16)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, offset=offset4)
  case(17)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, offset=offset4)
  case(18)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colput=colput4, offset=offset4)
  case(20)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, offset=offset4)
  case(21)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, offset=offset4)
  case(22)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colput=colput4, colpack=colpack4, offset=offset4)
  case(23)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colput=colput4, colpack=colpack4, offset=offset4)
  case(24)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, sorted=sorted4, offset=offset4)
  case(25)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, sorted=sorted4, offset=offset4)
  case(26)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colput=colput4, sorted=sorted4, offset=offset4)
  case(27)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colput=colput4, sorted=sorted4, offset=offset4)
  case(28)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colpack=colpack4, sorted=sorted4, offset=offset4)
  case(29)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colpack=colpack4, sorted=sorted4, offset=offset4)
  case(30)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, colput=colput4, colpack=colpack4, sorted=sorted4, offset=offset4)
  case(31)
     RC4=ODB_put4(handle4, dtname, d, nrows4, ncols4, poolno=poolno4, colput=colput4, colpack=colpack4, sorted=sorted4, &
                  offset=offset4)
  end select

  NROWS = NROWS4
  If (PRESENT(NCOLS)) NCOLS = NCOLS4
  If (Allocated(COLPUT4))  Deallocate(COLPUT4)
  If (Allocated(COLPACK4)) Deallocate(COLPACK4)
  ODB_put = RC4

End Function ODB_put

! =====================================================================
! wrapper functions for odbutil.F90
! =====================================================================

!
! ODB_getnames ========================================================
!
Integer(Kind=8) Function ODB_getnames(HANDLE, DTNAME, MODE, OUTNAMES)

  Use odb_module, only : ODB_getnames4 => ODB_getnames

  Integer(Kind=8), Intent(IN) :: HANDLE
  Character(Len=*), Intent(IN) :: DTNAME, MODE
  Character(Len=*), Dimension(:), Intent(OUT), Optional :: OUTNAMES

  Integer(Kind=JPIM) :: HANDLE4, RC4 = 0

  HANDLE4 = HANDLE
  RC4 = ODB_getnames4(HANDLE4, DTNAME, MODE, OUTNAMES)
  ODB_getnames = RC4

End Function ODB_getnames

!
! ODB_varindex ========================================================
!
Integer(Kind=8) Function ODB_varindex(HANDLE, DTNAME, NAMES, IDX)

  Use odb_module, only : ODB_varindex4 => ODB_varindex

  Integer(Kind=8), Intent(IN) :: HANDLE
  Character(Len=*), Intent(IN) :: DTNAME
  Character(Len=*), Dimension(:), Intent(IN) :: NAMES
  Integer(Kind=8), Dimension(:), Intent(OUT) :: IDX

  Integer(Kind=JPIM) :: HANDLE4, RC4 = 0
  Integer(Kind=JPIM), Dimension(:), Allocatable :: IDX4

  HANDLE4 = HANDLE
  Allocate(IDX4(SIZE(IDX)))

  RC4=ODB_varindex4(HANDLE4, DTNAME, NAMES, IDX4)

  IDX = IDX4
  ODB_varindex = RC4

End Function ODB_varindex

Integer(Kind=8) FUNCTION ODB_cancel(handle, dtname, poolno)

  Use odb_module, only : ODB_cancel4 => ODB_cancel

  INTEGER(KIND=8), intent(in)           :: handle
  character(len=*), intent(in)  :: dtname
  INTEGER(KIND=8), intent(in), optional :: poolno

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: poolno4
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  if (present(poolno)) o1=1
  o=o1

  HANDLE4 = handle
  If (PRESENT(poolno)) poolno4 = poolno
  

  select case(o)
  case(0)
     RC4=ODB_cancel4(handle4, dtname)
  case(1)
     RC4=ODB_cancel4(handle4, dtname, poolno=poolno4)
  end select

  ODB_cancel = RC4

End Function ODB_cancel

Integer(Kind=8) FUNCTION ODB_end()

  Use odb_module, only : ODB_end4 => ODB_end

  implicit none
  INTEGER(KIND=JPIM) :: RC4

  RC4 = ODB_end4()

  ODB_end = RC4

End Function ODB_end

Integer(Kind=8) Function ODB_load(handle, poolno, sync)

  Use odb_module, only : ODB_load4 => ODB_load

  implicit none

  INTEGER(KIND=8), intent(in)           :: handle
  INTEGER(KIND=8), intent(in), optional :: poolno
  logical(KIND=8), intent(in), optional :: sync

  INTEGER(KIND=JPIM) :: RC4

  INTEGER(KIND=JPIM) :: handle4, poolno4
  logical(KIND=JPIM) :: sync4

  integer :: o=0,o1=0,o2=0

  handle4 = handle
  if (present(poolno)) poolno4 = poolno
  if (present(sync))   sync4 = sync

  if (present(poolno)) o1=1
  if (present(sync)) o2=2
  o=o1+o2

  select case(o)
  case(0)
     RC4=ODB_load4(handle4)
  case(1)
     RC4=ODB_load4(handle4, poolno=poolno4)
  case(2)
     RC4=ODB_load4(handle4, sync=sync4)
  case(3)
     RC4=ODB_load4(handle4, poolno=poolno4, sync=sync4)
  end select

  ODB_load = RC4

End Function ODB_load

Integer(Kind=8) Function ODB_store(handle, poolno, sync)

  Use odb_module, only : ODB_store4 => ODB_store

  implicit none

  INTEGER(KIND=8), intent(in)           :: handle
  INTEGER(KIND=8), intent(in), optional :: poolno
  logical(KIND=8), intent(in), optional :: sync

  INTEGER(KIND=JPIM) :: RC4

  INTEGER(KIND=JPIM) :: handle4, poolno4
  logical(KIND=JPIM) :: sync4

  integer :: o=0,o1=0,o2=0

  handle4 = handle
  if (present(poolno)) poolno4 = poolno
  if (present(sync))   sync4 = sync

  if (present(poolno)) o1=1
  if (present(sync)) o2=2
  o=o1+o2

  select case(o)
  case(0)
     RC4=ODB_store4(handle4)
  case(1)
     RC4=ODB_store4(handle4, poolno=poolno4)
  case(2)
     RC4=ODB_store4(handle4, sync=sync4)
  case(3)
     RC4=ODB_store4(handle4, poolno=poolno4, sync=sync4)
  end select

  ODB_store = RC4

End Function ODB_store

Integer(Kind=8) Function ODB_getsize(handle, dtname, nrows, ncols, nra, poolno)

  Use odb_module, only : ODB_getsize4 => ODB_getsize

  implicit none

  INTEGER(KIND=8), intent(in)           :: handle
  INTEGER(KIND=8), intent(out)          :: nrows, ncols
  character(len=*), intent(in)          :: dtname
  INTEGER(KIND=8), intent(out), optional:: nra
  INTEGER(KIND=8), intent(in), optional :: poolno

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: nrows4, ncols4
  INTEGER(KIND=JPIM) :: nra4
  INTEGER(KIND=JPIM) :: poolno4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0

  handle4 = handle
  if (present(poolno)) poolno4 = poolno

  if (present(nra)) o1=1
  if (present(poolno)) o2=2
  o=o1+o2

  select case(o)
  case(0)
     RC4=ODB_getsize4(handle4,dtname,nrows4,ncols4)
  case(1)
     RC4=ODB_getsize4(handle4,dtname,nrows4,ncols4, nra=nra4)
  case(2)
     RC4=ODB_getsize4(handle4,dtname,nrows4,ncols4, poolno=poolno4)
  case(3)
     RC4=ODB_getsize4(handle4,dtname,nrows4,ncols4, nra=nra4, poolno=poolno4)
  end select

  nrows = nrows4
  ncols = ncols4
  if (present(nra)) nra = nra4

  ODB_getsize = RC4

End Function ODB_getsize

Integer(Kind=8) Function ODB_swapout(handle, dtname, poolno, save, repack)

  Use odb_module, only : ODB_swapout4 => ODB_swapout

  implicit none

  INTEGER(KIND=8) , intent(in)           :: handle
  character(len=*), intent(in)           :: dtname
  INTEGER(KIND=8) , intent(in), optional :: poolno
  logical(Kind=8) , intent(in), optional :: save, repack

  INTEGER(KIND=JPIM) :: RC4

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: poolno4
  logical(Kind=JPIM) :: save4, repack4

  integer :: o=0,o1=0,o2=0,o3=0

  handle4 = handle
  if (present(poolno)) poolno4 = poolno
  if (present(save)) save4 = save
  if (present(repack)) repack4 = repack

  if (present(poolno)) o1=1
  if (present(save)) o2=2
  if (present(repack)) o3=4
  o=o1+o2+o3

  select case(o)
  case(0)
     RC4=ODB_swapout4(handle4,dtname)
  case(1)
     RC4=ODB_swapout4(handle4,dtname, poolno=poolno4)
  case(2)
     RC4=ODB_swapout4(handle4,dtname, save=save4)
  case(3)
     RC4=ODB_swapout4(handle4,dtname, poolno=poolno4, save=save4)
  case(4)
     RC4=ODB_swapout4(handle4,dtname, repack=repack4)
  case(5)
     RC4=ODB_swapout4(handle4,dtname, poolno=poolno4, repack=repack4)
  case(6)
     RC4=ODB_swapout4(handle4,dtname, save=save4, repack=repack4)
  case(7)
     RC4=ODB_swapout4(handle4,dtname, poolno=poolno4, save=save4, repack=repack4)
  end select

  ODB_swapout = RC4

End Function ODB_swapout

Integer(Kind=8) Function ODB_pack(handle, dtname, poolno)

  Use odb_module, only : ODB_pack4 => ODB_pack

  implicit none

  INTEGER(KIND=8), intent(in)           :: handle
  character(len=*), intent(in)    :: dtname
  INTEGER(KIND=8), intent(in), optional :: poolno

  INTEGER(KIND=JPIM) :: RC4

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: poolno4

  integer :: o=0,o1=0

  handle4 = handle
  if (present(poolno)) poolno4 = poolno

  if (present(poolno)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_pack4(handle4, dtname)
  case(1)
     RC4=ODB_pack4(handle4, dtname, poolno=poolno4)
  end select

  ODB_pack = RC4

End Function ODB_pack

Integer(Kind=8) Function ODB_unpack(handle, dtname, poolno)

  Use odb_module, only : ODB_unpack4 => ODB_unpack

  implicit none

  INTEGER(KIND=8), intent(in)           :: handle
  character(len=*), intent(in)    :: dtname
  INTEGER(KIND=8), intent(in), optional :: poolno

  INTEGER(KIND=JPIM) :: RC4

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: poolno4

  integer :: o=0,o1=0

  handle4 = handle
  if (present(poolno)) poolno4 = poolno

  if (present(poolno)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_unpack4(handle4, dtname)
  case(1)
     RC4=ODB_unpack4(handle4, dtname, poolno=poolno4)
  end select

  ODB_unpack = RC4

End Function ODB_unpack

Integer(Kind=8) Function ODB_gethandle(handle, dtname, addview)

  Use odb_module, only : ODB_gethandle4 => ODB_gethandle

  implicit none

  INTEGER(KIND=8) , intent(in)             :: handle
  character(len=*), intent(in)             :: dtname
  logical(Kind=8) , intent(in), optional   :: addview

  INTEGER(KIND=JPIM) :: RC4

  INTEGER(KIND=JPIM) :: handle4
  logical(Kind=4)    :: addview4

  integer :: o=0,o1=0

  handle4 = handle
  if (present(addview)) addview4 = addview

  if (present(addview)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_gethandle4(handle4, dtname)
  case(1)
     RC4=ODB_gethandle4(handle4, dtname, addview=addview4)
  end select

  ODB_gethandle = RC4

End Function ODB_gethandle

Integer(Kind=8) Function ODB_addpools(handle, pool_excess,old_npools,maxpoolno)

  Use odb_module, only : ODB_addpools4 => ODB_addpools

  implicit none

  INTEGER(KIND=8), intent(in) :: handle, pool_excess
  INTEGER(KIND=8), intent(out), OPTIONAL :: old_npools,maxpoolno

  INTEGER(KIND=JPIM) :: RC4

  INTEGER(KIND=JPIM) :: handle4, pool_excess4
  INTEGER(KIND=JPIM) :: old_npools4,maxpoolno4

  integer :: o=0,o1=0,o2=0

  handle4 = handle
  pool_excess4 = pool_excess

  if (present(old_npools)) o1=1
  if (present(maxpoolno)) o2=2
  o=o1+o2

  select case(o)
  case(0)
     RC4=ODB_addpools4(handle4, pool_excess4)
  case(1)
     RC4=ODB_addpools4(handle4, pool_excess4, old_npools=old_npools4)
  case(2)
     RC4=ODB_addpools4(handle4, pool_excess4, maxpoolno=maxpoolno4)
  case(3)
     RC4=ODB_addpools4(handle4, pool_excess4, old_npools=old_npools4, maxpoolno=maxpoolno4)
  end select

  if (present(old_npools)) old_npools = old_npools4
  if (present(maxpoolno))  maxpoolno  = maxpoolno4
  ODB_addpools = RC4

End Function ODB_addpools

Integer(Kind=8) Function ODB_remove(handle, dtname, poolno)

  use odb_module, only : ODB_remove4 => ODB_remove

  implicit none

  INTEGER(KIND=8), intent(in)           :: handle
  character(len=*), intent(in)             :: dtname
  INTEGER(KIND=8), intent(in), optional :: poolno

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: poolno4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  handle4 = handle
  if (present(poolno)) poolno4 = poolno

  if (present(poolno)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_remove4(handle4, dtname)
  case(1)
     RC4=ODB_remove4(handle4, dtname, poolno=poolno4)
  end select

  ODB_remove = RC4

End Function ODB_remove

Integer(Kind=4) Function ODB_getprecision(handle, dtname, maxbits, anyflp)

  Use odb_module, only : ODB_getprecision4 => ODB_getprecision

  Implicit none

  INTEGER(KIND=8 ), intent(in)  :: handle
  INTEGER(KIND=8) , intent(out) :: maxbits
  logical(Kind=8) , intent(out) :: anyflp
  character(len=*), intent(in)  :: dtname

  INTEGER(KIND=JPIM) :: RC4

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: maxbits4
  logical(Kind=JPIM) :: anyflp4

  handle4 = handle

  RC4 = ODB_getprecision4(handle4, dtname, maxbits4, anyflp4)

  maxbits = maxbits4
  anyflp = anyflp4
  ODB_getprecision = RC4

End Function ODB_getprecision

Integer(Kind=8) Function ODB_getval(handle, varname, viewname)

  Use odb_module, only : ODB_getval4 => ODB_getval

  implicit none

  INTEGER(KIND=8)   , intent(in)           :: handle
  character(len=*)  , intent(in)           :: varname
  character(len=*)  , intent(in), optional :: viewname

  INTEGER(KIND=JPIM) :: handle4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  handle4 = handle

  if (present(viewname)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_getval4(handle4, varname)
  case(1)
     RC4=ODB_getval4(handle4, varname, viewname=viewname)
  end select

  ODB_getval = RC4

End Function ODB_getval

Logical(Kind=8) Function ODB_varexist(handle, varname, viewname)

  Use odb_module, only : ODB_varexist4 => ODB_varexist

  implicit none

  INTEGER(KIND=8) , intent(in)           :: handle
  character(len=*), intent(in)           :: varname
  character(len=*), intent(in), optional :: viewname

  INTEGER(KIND=JPIM) :: handle4

  logical(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  handle4 = handle

  if (present(viewname)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_varexist4(handle4, varname)
  case(1)
     RC4=ODB_varexist4(handle4, varname, viewname=viewname)
  end select

  ODB_varexist = RC4

End Function ODB_varexist

Integer (Kind=8) Function ODB_poolinfo(handle, poolids, with_poolmask)

  Use odb_module, only : ODB_poolinfo4 => ODB_poolinfo

  implicit none

  INTEGER(KIND=8), intent(in)            :: handle
  INTEGER(KIND=8), intent(out), optional :: poolids(:)
  logical(Kind=8), intent(in) , optional :: with_poolmask

  INTEGER(KIND=JPIM)              :: handle4
  INTEGER(KIND=JPIM), allocatable :: poolids4(:)
  logical                         :: with_poolmask4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0

  allocate(poolids4(size(poolids)))
  handle4 = handle
  if (present(with_poolmask)) with_poolmask4 = with_poolmask

  if (present(poolids)) o1=1
  if (present(with_poolmask)) o2=2
  o=o1+o2

  select case(o)
  case(0)
     RC4=ODB_poolinfo4(handle4)
  case(1)
     RC4=ODB_poolinfo4(handle4, poolids=poolids4)
  case(2)
     RC4=ODB_poolinfo4(handle4, with_poolmask=with_poolmask4)
  case(3)
     RC4=ODB_poolinfo4(handle4, poolids=poolids4, with_poolmask=with_poolmask4)
  end select
  
  if (present(poolids)) poolids4 = poolids

  deallocate(poolids4)
  ODB_poolinfo = RC4

End Function ODB_poolinfo

Integer(Kind=8) FUNCTION ODB_twindow(target_date, target_time, analysis_date, analysis_time, left_margin, right_margin)

  Use odb_module, only : ODB_twindow4 => ODB_twindow

  implicit none

  INTEGER(KIND=8), intent(in) :: target_date, target_time
  INTEGER(KIND=8), intent(in) :: analysis_date, analysis_time
  INTEGER(KIND=8), intent(in) :: left_margin, right_margin

  INTEGER(KIND=JPIM) :: target_date4, target_time4
  INTEGER(KIND=JPIM) :: analysis_date4, analysis_time4
  INTEGER(KIND=JPIM) :: left_margin4, right_margin4
  
  INTEGER(KIND=JPIM) :: RC4

  target_date4 = target_date
  target_time4 = target_time
  analysis_date4 = analysis_date
  analysis_time4 = analysis_time
  left_margin4 = left_margin
  right_margin4 = right_margin

  RC4 = ODB_twindow4(target_date4, target_time4, analysis_date4, analysis_time4, left_margin4, right_margin4)

  ODB_twindow = RC4

End Function ODB_twindow

Integer(Kind=8) FUNCTION ODB_tdiff(target_date, target_time, analysis_date, analysis_time)

  Use odb_module, only : ODB_tdiff4 => ODB_tdiff

  implicit none

  INTEGER(KIND=8), intent(in) :: target_date, target_time
  INTEGER(KIND=8), intent(in) :: analysis_date, analysis_time

  INTEGER(KIND=JPIM) :: target_date4, target_time4
  INTEGER(KIND=JPIM) :: analysis_date4, analysis_time4
  
  INTEGER(KIND=JPIM) :: RC4

  target_date4 = target_date
  target_time4 = target_time
  analysis_date4 = analysis_date
  analysis_time4 = analysis_time

  RC4 = ODB_tdiff4(target_date4, target_time4, analysis_date4, analysis_time4)

  ODB_tdiff = RC4

End Function ODB_tdiff

SUBROUTINE ODB_analysis_datetime(handle, andate, antime)

  use odb_module, only : ODB_analysis_datetime4 => ODB_analysis_datetime

  implicit none

  INTEGER(KIND=8), intent(in) :: handle, andate, antime

  INTEGER(KIND=JPIM) :: handle4, andate4, antime4

  handle4 = handle
  andate4 = andate
  antime4 = antime

  call ODB_analysis_datetime4(handle4, andate4, antime4)

End Subroutine ODB_analysis_datetime

Integer(Kind=8) Function ODB_get_update_info(handle, dtname, colput)

  Use odb_module, only : ODB_get_update_info4 => ODB_get_update_info

  implicit none

  INTEGER(KIND=8)  , intent(in)  :: handle
  character(len=*) , intent(in)  :: dtname
  logical(Kind=8)  , intent(out) :: colput(:)

  INTEGER(KIND=JPIM)   :: handle4
  logical, allocatable :: colput4(:)
  
  INTEGER(KIND=JPIM)   :: RC4

  handle4 = handle
  allocate(colput4(size(colput)))
  colput4 = colput

  RC4 = ODB_get_update_info4(handle4, dtname, colput4)

  deallocate(colput4)

  ODB_get_update_info = RC4

End Function ODB_get_update_info

Integer(Kind=8) Function ODB_sortkeys(handle, dtname, keys)

  Use odb_module, only : ODB_sortkeys4 => ODB_sortkeys

  implicit none

  INTEGER(KIND=8), intent(in)            :: handle
  character(len=*), intent(in)           :: dtname
  INTEGER(KIND=8),intent(out) , optional :: keys(:)

  INTEGER(KIND=JPIM)              :: handle4
  INTEGER(KIND=JPIM), allocatable :: keys4(:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  handle4 = handle
  if (present(keys)) allocate(keys4(size(keys)))

  if (present(keys)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_sortkeys4(handle4, dtname)
  case(1)
     RC4=ODB_sortkeys4(handle4, dtname, keys=keys4)
  end select

  if (present(keys)) then
     keys = keys4
     deallocate(keys4)
  endif

  ODB_sortkeys = RC4

End Function ODB_sortkeys

Integer(Kind=8) Function ODB_io_method(handle)

  Use odb_module, only : ODB_io_method4 => ODB_io_method

  implicit none

  INTEGER(KIND=8), intent(in)  :: handle

  INTEGER(KIND=JPIM) :: handle4

  INTEGER(KIND=JPIM) :: RC4

  handle4 = handle

  RC4 = ODB_io_method4(handle4)

  ODB_io_method = RC4

End Function ODB_io_method

Logical(Kind=8) Function ODB_has_select_distinct(handle, dtname, ncols)

  Use odb_module, only : ODB_has_select_distinct4 => ODB_has_select_distinct

  implicit none

  INTEGER(KIND=8) , intent(in)            :: handle
  character(len=*), intent(in)            :: dtname
  INTEGER(KIND=8) , intent(out), optional :: ncols

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: ncols4

  Logical :: RC4

  integer :: o=0,o1=0

  handle4 = handle

  if (present(ncols)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_has_select_distinct4(handle4, dtname)
  case(1)
     RC4=ODB_has_select_distinct4(handle4, dtname, ncols=ncols4)
  end select

  if (present(ncols)) ncols4 = ncols

  ODB_has_select_distinct = RC4

End Function ODB_has_select_distinct

Logical(Kind=8) Function ODB_has_uniqueby(handle, dtname, ncols)

  Use odb_module, only : ODB_has_uniqueby4 => ODB_has_uniqueby

  implicit none

  INTEGER(KIND=8) , intent(in)            :: handle
  character(len=*), intent(in)            :: dtname
  INTEGER(KIND=8) , intent(out), optional :: ncols

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: ncols4

  Logical :: RC4

  integer :: o=0,o1=0

  handle4 = handle

  if (present(ncols)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_has_uniqueby4(handle4, dtname)
  case(1)
     RC4=ODB_has_uniqueby4(handle4, dtname, ncols=ncols4)
  end select

  if (present(ncols)) ncols4 = ncols

  ODB_has_uniqueby = RC4

End Function ODB_has_uniqueby

Logical(Kind=8) Function ODB_has_orderby(handle, dtname, ncols)

  Use odb_module, only : ODB_has_orderby4 => ODB_has_orderby

  implicit none

  INTEGER(KIND=8) , intent(in)            :: handle
  character(len=*), intent(in)            :: dtname
  INTEGER(KIND=8) , intent(out), optional :: ncols

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: ncols4

  Logical :: RC4

  integer :: o=0,o1=0

  handle4 = handle

  if (present(ncols)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_has_orderby4(handle4, dtname)
  case(1)
     RC4=ODB_has_orderby4(handle4, dtname, ncols=ncols4)
  end select

  if (present(ncols)) ncols4 = ncols

  ODB_has_orderby = RC4

End Function ODB_has_orderby

! odbshared

Subroutine ODB_abort(routine, s, code, really_abort)

  Use odb_module, only : ODB_abort4 => ODB_abort

  implicit none

  character(len=*), intent(in)           :: routine
  character(len=*), intent(in), optional :: s
  INTEGER(KIND=8) , intent(in), optional :: code
  logical(Kind=8) , intent(in), optional :: really_abort

  INTEGER(KIND=JPIM) :: code4
  logical(Kind=JPIM) :: really_abort4

  integer :: o=0,o1=0,o2=0,o3=0

  if (present(code)) code4 = code
  if (present(really_abort)) really_abort4 = really_abort

  if (present(s)) o1=1
  if (present(code)) o2=2
  if (present(really_abort)) o3=4
  o=o1+o2+o3

  select case(o)
  case(0)
     call ODB_abort4(routine)
  case(1)
     call ODB_abort4(routine, s=s)
  case(2)
     call ODB_abort4(routine, code=code4)
  case(3)
     call ODB_abort4(routine, s=s, code=code4)
  case(4)
     call ODB_abort4(routine, really_abort=really_abort4)
  case(5)
     call ODB_abort4(routine, s=s, really_abort=really_abort4)
  case(6)
     call ODB_abort4(routine, code=code4, really_abort=really_abort4)
  case(7)
     call ODB_abort4(routine, s=s, code=code4, really_abort=really_abort4)
  end select

End Subroutine ODB_abort

Integer(Kind=8) Function ODB_lda(n, method)

  use odb_module, only : ODB_lda4 => ODB_lda

  implicit none

  INTEGER(KIND=8), intent(in) :: n
  INTEGER(KIND=8), intent(in), OPTIONAL :: method

  INTEGER(KIND=JPIM) :: n4
  INTEGER(KIND=JPIM) :: method4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  n4 = n
  if (present(method)) method4 = method

  if (present(method)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_lda4(n4)
  case(1)
     RC4=ODB_lda4(n4, method=method4)
  end select

  ODB_lda = RC4

End Function ODB_lda

Integer(Kind=8) Function ODB_get_vpools(handle, poolnos)

  Use odb_module, only : ODB_get_vpools4 => ODB_get_vpools

  implicit none

  INTEGER(KIND=8), intent(in) :: handle
  INTEGER(KIND=8), intent(out), optional :: poolnos(:)

  INTEGER(KIND=JPIM)              :: handle4
  INTEGER(KIND=JPIM), allocatable :: poolnos4(:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  handle4 = handle
  if (present(poolnos)) allocate(poolnos4(size(poolnos)))

  if (present(poolnos)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_get_vpools4(handle4)
  case(1)
     RC4=ODB_get_vpools4(handle4, poolnos=poolnos4)
  end select

  if (present(poolnos)) poolnos = poolnos4
  ODB_get_vpools = RC4

End Function ODB_get_vpools

Integer(Kind=8) Function ODB_unmap_vpools(handle)

  Use odb_module, only : ODB_unmap_vpools4 => ODB_unmap_vpools

  implicit none

  INTEGER(KIND=8), intent(in) :: handle

  INTEGER(KIND=JPIM) :: handle4

  INTEGER(KIND=JPIM) :: RC4

  handle4 = handle

  RC4=ODB_unmap_vpools4(handle4)

  ODB_unmap_vpools = RC4

End Function ODB_unmap_vpools

Integer(Kind=8) Function ODB_map_vpools_direct(handle, poolnos, verbose)

  Use odb_module, only : ODB_map_vpools_direct4 => ODB_map_vpools_direct

  implicit none

  INTEGER(KIND=8), intent(in)           :: handle
  INTEGER(KIND=8), intent(in)           :: poolnos(:)
  logical(Kind=8), intent(in), optional :: verbose

  INTEGER(KIND=JPIM)              :: handle4
  INTEGER(KIND=JPIM), allocatable :: poolnos4(:)
  logical                         :: verbose4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  handle4 = handle
  allocate(poolnos4(size(poolnos)))
  poolnos4 = poolnos
  if (present(verbose)) verbose4 = verbose

  if (present(verbose)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_map_vpools_direct4(handle4, poolnos4)
  case(1)
     RC4=ODB_map_vpools_direct4(handle4, poolnos4, verbose=verbose4)
  end select

  ODB_map_vpools_direct = RC4

End Function ODB_map_vpools_direct

Integer(Kind=8) Function ODB_map_vpools_fromfile(handle, file, verbose)

  Use odb_module, only : ODB_map_vpools_fromfile4 => ODB_map_vpools_fromfile

  implicit none

  INTEGER(KIND=8), intent(in)            :: handle
  character(len=*), intent(in)           :: file
  logical(Kind=8), intent(in), optional  :: verbose

  INTEGER(KIND=JPIM) :: handle4
  logical            :: verbose4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  handle4 = handle
  if (present(verbose)) verbose4 =verbose

  if (present(verbose)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_map_vpools_fromfile4(handle4, file)
  case(1)
     RC4=ODB_map_vpools_fromfile4(handle4, file, verbose=verbose4)
  end select

  ODB_map_vpools_fromfile = RC4

End Function ODB_map_vpools_fromfile

Integer(Kind=8) Function ODB_toggle_vpools(handle, toggle)
 
  Use odb_module, only : ODB_toggle_vpools4 => ODB_toggle_vpools

  implicit none

  INTEGER(KIND=8), intent(in) :: handle
  INTEGER(KIND=8), intent(in), optional :: toggle

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: toggle4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  handle4 = handle
  if (present(toggle)) toggle4 = toggle

  if (present(toggle)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_toggle_vpools4(handle4)
  case(1)
     RC4=ODB_toggle_vpools4(handle4, toggle=toggle4)
  end select

  ODB_toggle_vpools = RC4

End Function ODB_toggle_vpools

Integer(Kind=8) Function ODB_pool2vpool(handle, poolno)

  Use odb_module, only : ODB_pool2vpool4 => ODB_pool2vpool
  
  implicit none

  INTEGER(KIND=8), intent(in) :: handle
  INTEGER(KIND=8), intent(in) :: poolno

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: poolno4

  INTEGER(KIND=JPIM) :: RC4

  handle4 = handle
  poolno4 = poolno

  RC4=ODB_pool2vpool4(handle4, poolno4)

  ODB_pool2vpool=RC4

End Function ODB_pool2vpool

Integer(Kind=8) Function ODB_vpool2pool(handle, poolno)

  Use odb_module, only : ODB_vpool2pool4 => ODB_vpool2pool
  
  implicit none

  INTEGER(KIND=8), intent(in) :: handle
  INTEGER(KIND=8), intent(in) :: poolno

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: poolno4

  INTEGER(KIND=JPIM) :: RC4

  handle4 = handle
  poolno4 = poolno

  RC4=ODB_vpool2pool4(handle4, poolno4)

  ODB_vpool2pool=RC4

End Function ODB_vpool2pool

Logical(Kind=8) Function ODB_valid_poolno(poolno)

  Use odb_module, only : ODB_valid_poolno4 => ODB_valid_poolno

  implicit none

  INTEGER(KIND=8), intent(in) :: poolno

  INTEGER(KIND=JPIM) :: poolno4

  Logical(KIND=JPIM) :: RC4

  poolno4 = poolno

  RC4=ODB_valid_poolno4(poolno4)

  ODB_valid_poolno = RC4

End Function ODB_valid_poolno

! odbmp

Subroutine ODBMP_abort(routine, s, code, really_abort)

  Use odb_module, only : ODBMP_abort4 => ODBMP_abort

  implicit none

  character(len=*), intent(in)           :: routine
  character(len=*), intent(in), optional :: s
  INTEGER(KIND=8) , intent(in), optional :: code
  logical(Kind=8) , intent(in), optional :: really_abort

  INTEGER(KIND=JPIM) :: code4
  logical            :: really_abort4

  integer :: o=0,o1=0,o2=0,o3=0

  if (present(code)) code4 = code
  if (present(really_abort)) really_abort4 = really_abort

  if (present(s)) o1=1
  if (present(code)) o2=2
  if (present(really_abort)) o3=4
  o=o1+o2+o3

  select case(o)
  case(0)
     call ODBMP_abort4(routine)
  case(1)
     call ODBMP_abort4(routine, s=s)
  case(2)
     call ODBMP_abort4(routine, code=code4)
  case(3)
     call ODBMP_abort4(routine, s=s, code=code4)
  case(4)
     call ODBMP_abort4(routine, really_abort=really_abort4)
  case(5)
     call ODBMP_abort4(routine, s=s, really_abort=really_abort4)
  case(6)
     call ODBMP_abort4(routine, code=code4, really_abort=really_abort4)
  case(7)
     call ODBMP_abort4(routine, s=s, code=code4, really_abort=really_abort4)
  end select

End Subroutine ODBMP_abort

Subroutine ODBMP_init(LDsync)
  
  Use odb_module, only : ODBMP_init4 => ODBMP_init

  implicit none

  logical(Kind=8), intent(in), optional :: LDsync
  logical :: LDsync4

  integer :: o=0,o1=0

  if (present(LDsync)) LDsync4 = LDsync

  if (present(LDsync)) o1=1
  o=o1

  select case(o)
  case(0)
     call ODBMP_init4()
  case(1)
     call ODBMP_init4(LDsync=LDsync4)
  end select

End Subroutine ODBMP_init

Integer(Kind=8) Function ODBMP_end()

  Use odb_module, only : ODBMP_end4 => ODBMP_end

  implicit none

  Integer(Kind=JPIM) :: RC4

  RC4 = ODBMP_end4()

  ODBMP_end = RC4

End Function ODBMP_end

Subroutine ODBMP_sync(where)
  
  Use odb_module, only : ODBMP_sync4 => ODBMP_sync

  implicit none

  Integer(Kind=8), intent(in), optional :: where
  Integer(Kind=JPIM) :: where4

  integer :: o=0,o1=0

  if (present(where)) where4 = where

  if (present(where)) o1=1
  o=o1

  select case(o)
  case(0)
     call ODBMP_sync4()
  case(1)
     call ODBMP_sync4(where=where4)
  end select

End Subroutine ODBMP_sync

Logical(Kind=8) Function ODBMP_testready(with)
  
  Use odb_module, only : ODBMP_testready4 => ODBMP_testready

  implicit none

  Integer(Kind=8), intent(in) :: with
  Integer(Kind=JPIM) :: with4
  Logical(Kind=JPIM) :: RC4

  with4 = with

  RC4=ODBMP_testready4(with4)

  ODBMP_testready = RC4

End Function ODBMP_testready

Integer(Kind=8) Function ODBMP_locking(lockid, onoff)

  Use odb_module, only : ODBMP_locking4 => ODBMP_locking

  implicit none

  INTEGER(KIND=8), intent(in) :: lockid, onoff
  INTEGER(KIND=JPIM) :: lockid4, onoff4
  Integer(Kind=JPIM) :: RC4

  lockid4 = lockid
  onoff4 = onoff

  RC4=ODBMP_locking4(lockid4, onoff4)

  ODBMP_locking = RC4

End Function ODBMP_locking

Subroutine ODBMP_trace(trace_on)

  Use odb_module, only : ODBMP_trace4 => ODBMP_trace

  implicit none

  logical(Kind=8), intent(in) :: trace_on
  logical(Kind=JPIM) :: trace_on4

  trace_on4 = trace_on

  call ODBMP_trace4(trace_on4)

End Subroutine ODBMP_trace

Integer(Kind=8) Function ODBMP_physproc(x)

  Use odb_module, only : ODBMP_physproc4 => ODBMP_physproc

  implicit none

  Integer(Kind=8), intent(in) :: x
  Integer(Kind=JPIM) :: x4
  Integer(Kind=JPIM) :: RC4

  x4 = x

  RC4=ODBMP_physproc4(x4)

  ODBMP_physproc = RC4

End Function ODBMP_physproc

Integer(Kind=8) Function ODBMP_setup_exchange(opponent, start_proc)

  Use odb_module, only : ODBMP_setup_exchange4 => ODBMP_setup_exchange

  implicit none

  INTEGER(KIND=8), intent(out) :: opponent(:)
  INTEGER(KIND=8), intent(in), OPTIONAL :: start_proc

  INTEGER(KIND=JPIM), allocatable :: opponent4(:)
  INTEGER(KIND=JPIM) :: start_proc4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  allocate(opponent4(size(opponent)))
  opponent4 = opponent
  if (present(start_proc)) start_proc4 = start_proc

  if (present(start_proc)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODBMP_setup_exchange4(opponent4)
  case(1)
     RC4=ODBMP_setup_exchange4(opponent4, start_proc=start_proc4)
  end select

  deallocate(opponent4)

  ODBMP_setup_exchange = RC4

End Function ODBMP_setup_exchange

Integer(Kind=8) Function ODB_to_netcdf(handle, dtname, file, poolno, setvars, values, mdi)

  Use odb_module, only : ODB_to_netcdf4 => ODB_to_netcdf

  implicit none

  INTEGER(KIND=8)   , intent(in)           :: handle
  character(len=*)  , intent(in)           :: dtname
  character(len=*)  , intent(in), optional :: file
  INTEGER(KIND=8)   , intent(in), optional :: poolno
  character(len=*)  , intent(in), optional :: setvars(:)
  REAL(KIND=JPRB)   , intent(in), optional :: values(:)
  REAL(KIND=JPRB)   , intent(in), optional :: mdi

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: poolno4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0,o3=0,o4=0,o5=0

  handle4 = handle
  if (present(poolno)) poolno4 = poolno

  if (present(file)) o1=1
  if (present(poolno)) o2=2
  if (present(setvars)) o3=4
  if (present(values)) o4=8
  if (present(mdi)) o5=16
  o=o1+o2+o3+o4+o5

  select case(o)
  case(0)
     RC4=ODB_to_netcdf4(handle4, dtname)
  case(1)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file)
  case(2)
     RC4=ODB_to_netcdf4(handle4, dtname, poolno=poolno4)
  case(3)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, poolno=poolno4)
  case(4)
     RC4=ODB_to_netcdf4(handle4, dtname, setvars=setvars)
  case(5)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, setvars=setvars)
  case(6)
     RC4=ODB_to_netcdf4(handle4, dtname, poolno=poolno4, setvars=setvars)
  case(7)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, poolno=poolno4, setvars=setvars)
  case(8)
     RC4=ODB_to_netcdf4(handle4, dtname, values=values)
  case(9)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, values=values)
  case(10)
     RC4=ODB_to_netcdf4(handle4, dtname, poolno=poolno4, values=values)
  case(11)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, poolno=poolno4, values=values)
  case(12)
     RC4=ODB_to_netcdf4(handle4, dtname, setvars=setvars, values=values)
  case(13)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, setvars=setvars, values=values)
  case(14)
     RC4=ODB_to_netcdf4(handle4, dtname, poolno=poolno4, setvars=setvars, values=values)
  case(15)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, poolno=poolno4, setvars=setvars, values=values)
  case(16)
     RC4=ODB_to_netcdf4(handle4, dtname, mdi=mdi)
  case(17)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, mdi=mdi)
  case(18)
     RC4=ODB_to_netcdf4(handle4, dtname, poolno=poolno4, mdi=mdi)
  case(19)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, poolno=poolno4, mdi=mdi)
  case(20)
     RC4=ODB_to_netcdf4(handle4, dtname, setvars=setvars, mdi=mdi)
  case(21)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, setvars=setvars, mdi=mdi)
  case(22)
     RC4=ODB_to_netcdf4(handle4, dtname, poolno=poolno4, setvars=setvars, mdi=mdi)
  case(23)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, poolno=poolno4, setvars=setvars, mdi=mdi)
  case(24)
     RC4=ODB_to_netcdf4(handle4, dtname, values=values, mdi=mdi)
  case(25)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, values=values, mdi=mdi)
  case(26)
     RC4=ODB_to_netcdf4(handle4, dtname, poolno=poolno4, values=values, mdi=mdi)
  case(27)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, poolno=poolno4, values=values, mdi=mdi)
  case(28)
     RC4=ODB_to_netcdf4(handle4, dtname, setvars=setvars, values=values, mdi=mdi)
  case(29)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, setvars=setvars, values=values, mdi=mdi)
  case(30)
     RC4=ODB_to_netcdf4(handle4, dtname, poolno=poolno4, setvars=setvars, values=values, mdi=mdi)
  case(31)
     RC4=ODB_to_netcdf4(handle4, dtname, file=file, poolno=poolno4, setvars=setvars, values=values, mdi=mdi)
  end select

  ODB_to_netcdf = RC4

End Function ODB_to_netcdf

Integer(Kind=8) Function ODB_distribute_str(s, target)

  Use odb_module, only : ODB_distribute4 => ODB_distribute

  implicit none

  character(len=*), intent(inout)        :: s
  INTEGER(KIND=8) , intent(in), optional :: target

  INTEGER(KIND=JPIM) :: target4
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  if (present(target)) target4 = target

  if (present(target)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_distribute4(s)
  case(1)
     RC4=ODB_distribute4(s, target=target4)
  end select

  ODB_distribute_str = RC4

End Function ODB_distribute_str
  
Integer(Kind=8) Function ODB_distribute_vecstr(s, target)

  Use odb_module, only : ODB_distribute4 => ODB_distribute

  implicit none

  character(len=*), intent(inout)        :: s(:)
  INTEGER(KIND=8) , intent(in), optional :: target

  INTEGER(KIND=JPIM) :: target4
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  if (present(target)) target4 = target

  if (present(target)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_distribute4(s)
  case(1)
     RC4=ODB_distribute4(s, target=target4)
  end select

  ODB_distribute_vecstr = RC4

End Function ODB_distribute_vecstr
  
Integer(Kind=8) Function ODB_distribute_int(s, target)

  Use odb_module, only : ODB_distribute4 => ODB_distribute

  implicit none

  INTEGER(KIND=8), intent(inout)        :: s
  INTEGER(KIND=8), intent(in), optional :: target

  INTEGER(KIND=JPIM) :: s4
  INTEGER(KIND=JPIM) :: target4
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  s4 = s
  if (present(target)) target4 = target

  if (present(target)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_distribute4(s4)
  case(1)
     RC4=ODB_distribute4(s4, target=target4)
  end select
  
  s = s4
  ODB_distribute_int = RC4

End Function ODB_distribute_int
  
Integer(Kind=8) Function ODB_distribute_vecint(s, target)

  Use odb_module, only : ODB_distribute4 => ODB_distribute

  implicit none

  INTEGER(KIND=8), intent(inout)        :: s(:)
  INTEGER(KIND=8), intent(in), optional :: target

  INTEGER(KIND=JPIM), allocatable :: s4(:)
  INTEGER(KIND=JPIM) :: target4
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  allocate(s4(size(s)))
  s4 = s
  if (present(target)) target4 = target

  if (present(target)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_distribute4(s4)
  case(1)
     RC4=ODB_distribute4(s4, target=target4)
  end select
  
  s = s4
  deallocate(s4)
  ODB_distribute_vecint = RC4

End Function ODB_distribute_vecint
  
Integer(Kind=8) Function ODB_distribute_real8(s, target)

  Use odb_module, only : ODB_distribute4 => ODB_distribute

  implicit none

  REAL(KIND=JPRB), intent(inout)        :: s
  INTEGER(KIND=8), intent(in), optional :: target

  INTEGER(KIND=JPIM) :: target4
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  if (present(target)) target4 = target

  if (present(target)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_distribute4(s)
  case(1)
     RC4=ODB_distribute4(s, target=target4)
  end select
  
  ODB_distribute_real8 = RC4

End Function ODB_distribute_real8

Integer(Kind=8) Function ODB_distribute_vecreal8(s, target)

  Use odb_module, only : ODB_distribute4 => ODB_distribute

  implicit none

  REAL(KIND=JPRB), intent(inout)        :: s(:)
  INTEGER(KIND=8), intent(in), optional :: target

  INTEGER(KIND=JPIM) :: target4
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  if (present(target)) target4 = target

  if (present(target)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_distribute4(s)
  case(1)
     RC4=ODB_distribute4(s, target=target4)
  end select
  
  ODB_distribute_vecreal8 = RC4

End Function ODB_distribute_vecreal8

Real(Kind=8) Function ODB_dsetval(handle, varname, newvalue, viewname)

  Use odb_module, only : ODB_setval4 => ODB_setval

  implicit none

  INTEGER(KIND=8), intent(in)            :: handle
  character(len=*), intent(in)           :: varname
  REAL(KIND=JPRB), intent(in)            :: newvalue
  character(len=*), intent(in), optional :: viewname 

  INTEGER(KIND=JPIM) :: handle4
  Real(KIND=JPRM)    :: RC4

  integer :: o=0,o1=0

  handle4 = handle

  if (present(viewname)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_setval4(handle4, varname, newvalue)
  case(1)
     RC4=ODB_setval4(handle4, varname, newvalue, viewname=viewname)
  end select

  ODB_dsetval = RC4

End Function ODB_dsetval

Integer(Kind=8) Function ODB_isetval(handle, varname, newvalue_in, viewname)

  Use odb_module, only : ODB_setval4 => ODB_setval

  implicit none

  INTEGER(KIND=8), intent(in)            :: handle
  character(len=*), intent(in)           :: varname
  INTEGER(KIND=8), intent(in)            :: newvalue_in
  character(len=*), intent(in), optional :: viewname 

  INTEGER(KIND=JPIM) :: handle4
  INTEGER(KIND=JPIM) :: newvalue_in4
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  handle4 = handle
  newvalue_in4 = newvalue_in

  if (present(viewname)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_setval4(handle4, varname, newvalue_in4)
  case(1)
     RC4=ODB_setval4(handle4, varname, newvalue_in4, viewname=viewname)
  end select

  ODB_isetval = RC4

End Function ODB_isetval

Integer(Kind=8) Function ODB_control_word_info_vector(v,nrows,poolnos,rownums,noffset)

  Use odb_module, only : ODB_control_word_info4 => ODB_control_word_info

  implicit none

  REAL(KIND=JPRB), intent(in)            :: v(:)
  INTEGER(KIND=8), intent(in), optional  :: nrows, noffset
  INTEGER(KIND=8), intent(out), optional :: poolnos(:), rownums(:)

  INTEGER(KIND=JPIM)              :: nrows4, noffset4
  INTEGER(KIND=JPIM), allocatable :: poolnos4(:), rownums4(:)
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0,o3=0

  nrows4 = nrows
  noffset4 = noffset
  if (present(poolnos)) allocate(poolnos4(size(poolnos)))
  if (present(rownums)) allocate(rownums4(size(rownums)))

  if (present(nrows)) o1=1
  if (present(poolnos)) o2=2
  if (present(noffset)) o3=4
  o=o1+o2+o3

  select case(o)
  case(0)
     RC4=ODB_control_word_info4(v)
  case(1)
     RC4=ODB_control_word_info4(v, nrows=nrows4)
  case(2)
     RC4=ODB_control_word_info4(v, poolnos=poolnos4)
  case(3)
     RC4=ODB_control_word_info4(v, nrows=nrows4, poolnos=poolnos4)
  case(4)
     RC4=ODB_control_word_info4(v, noffset=noffset4)
  case(5)
     RC4=ODB_control_word_info4(v, nrows=nrows4, noffset=noffset4)
  case(6)
     RC4=ODB_control_word_info4(v, poolnos=poolnos4, noffset=noffset4)
  case(7)
     RC4=ODB_control_word_info4(v, nrows=nrows4, poolnos=poolnos4, noffset=noffset4)
  end select
  
  poolnos = poolnos4
  rownums = rownums4
  if (present(poolnos)) deallocate(poolnos4)
  if (present(rownums)) deallocate(rownums4)

  ODB_control_word_info_vector = RC4

End Function ODB_control_word_info_vector

Integer(Kind=8) Function ODB_control_word_info_matrix(d,nrows,poolnos,rownums,noffset)

  Use odb_module, only : ODB_control_word_info4 => ODB_control_word_info

  implicit none

  REAL(KIND=JPRB), intent(in)            :: d(:,0:)
  INTEGER(KIND=8), intent(in), optional  :: nrows, noffset
  INTEGER(KIND=8), intent(out), optional :: poolnos(:), rownums(:)

  INTEGER(KIND=JPIM)              :: nrows4, noffset4
  INTEGER(KIND=JPIM), allocatable :: poolnos4(:), rownums4(:)
  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0,o3=0

  nrows4 = nrows
  noffset4 = noffset
  if (present(poolnos)) allocate(poolnos4(size(poolnos)))
  if (present(rownums)) allocate(rownums4(size(rownums)))

  if (present(nrows)) o1=1
  if (present(poolnos)) o2=2
  if (present(noffset)) o3=4
  o=o1+o2+o3

  select case(o)
  case(0)
     RC4=ODB_control_word_info4(d)
  case(1)
     RC4=ODB_control_word_info4(d, nrows=nrows4)
  case(2)
     RC4=ODB_control_word_info4(d, poolnos=poolnos4)
  case(3)
     RC4=ODB_control_word_info4(d, nrows=nrows4, poolnos=poolnos4)
  case(4)
     RC4=ODB_control_word_info4(d, noffset=noffset4)
  case(5)
     RC4=ODB_control_word_info4(d, nrows=nrows4, noffset=noffset4)
  case(6)
     RC4=ODB_control_word_info4(d, poolnos=poolnos4, noffset=noffset4)
  case(7)
     RC4=ODB_control_word_info4(d, nrows=nrows4, poolnos=poolnos4, noffset=noffset4)
  end select
  
  poolnos = poolnos4
  rownums = rownums4
  if (present(poolnos)) deallocate(poolnos4)
  if (present(rownums)) deallocate(rownums4)

  ODB_control_word_info_matrix = RC4

End Function ODB_control_word_info_matrix

Integer(Kind=8) Function ODB_iduplchk(a, nrows, ncols, npkcols, colidx, tol, dupl_with, idx)

  Use odb_module, only : ODB_duplchk4 => ODB_duplchk

  implicit none

  INTEGER(KIND=8), intent(in)         :: nrows, ncols, npkcols
  INTEGER(KIND=8), intent(out)        :: dupl_with(:)
  INTEGER(KIND=8), intent(inout)      :: a(:,:)
  INTEGER(KIND=8),  intent(in)        :: colidx(npkcols)
  INTEGER(KIND=JPIM), intent(in)      :: tol(npkcols)
  INTEGER(KIND=8),intent(in),optional :: idx(:)

  INTEGER(KIND=JPIM) :: nrows4, ncols4, npkcols4
  INTEGER(KIND=JPIM), allocatable :: dupl_with4(:)
  INTEGER(KIND=JPIM), allocatable :: a4(:,:)
  INTEGER(KIND=JPIM) :: colidx4(npkcols)
  INTEGER(KIND=JPIM) :: tol4(npkcols)
  INTEGER(KIND=JPIM), allocatable :: idx4(:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  nrows4 = nrows
  ncols4 = ncols
  npkcols4 = npkcols
  a4 = a
  colidx4 = colidx
  tol4 = tol
  idx4 = idx
  allocate(dupl_with4(size(dupl_with)))
  if (present(idx)) allocate(idx4(size(idx)))
  allocate(a4(size(a,1),size(a,2)))

  if (present(idx)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_duplchk4(a4, nrows4, ncols4, npkcols4, colidx4, tol4, dupl_with4)
  case(1)
     RC4=ODB_duplchk4(a4, nrows4, ncols4, npkcols4, colidx4, tol4, dupl_with4, idx=idx4)
  end select

  a = a4
  dupl_with = dupl_with4

  deallocate(dupl_with4)
  if (present(idx)) deallocate(idx4)
  deallocate(a4)

  ODB_iduplchk = RC4

End Function ODB_iduplchk

Integer(Kind=8) Function ODB_dduplchk(a, nrows, ncols, npkcols, colidx, tol, dupl_with, idx)

  Use odb_module, only : ODB_duplchk4 => ODB_duplchk

  implicit none

  INTEGER(KIND=8), intent(in)         :: nrows, ncols, npkcols
  INTEGER(KIND=8), intent(out)        :: dupl_with(:)
  REAL(KIND=JPRB), intent(inout)      :: a(:,:)
  INTEGER(KIND=8),  intent(in)        :: colidx(npkcols)
  REAL(KIND=JPRB), intent(in)      :: tol(npkcols)
  INTEGER(KIND=8),intent(in),optional :: idx(:)

  INTEGER(KIND=JPIM) :: nrows4, ncols4, npkcols4
  INTEGER(KIND=JPIM), allocatable :: dupl_with4(:)
  INTEGER(KIND=JPIM) :: colidx4(npkcols)
  INTEGER(KIND=JPIM), allocatable :: idx4(:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  nrows4 = nrows
  ncols4 = ncols
  npkcols4 = npkcols
  colidx4 = colidx
  if (present(idx)) then
    allocate(idx4(size(idx)))
    idx4 = idx
  endif
  allocate(dupl_with4(size(dupl_with)))

  if (present(idx)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_duplchk4(a, nrows4, ncols4, npkcols4, colidx4, tol, dupl_with4)
  case(1)
     RC4=ODB_duplchk4(a, nrows4, ncols4, npkcols4, colidx4, tol, dupl_with4, idx=idx4)
  end select

  if (present(idx)) deallocate(idx4)
  deallocate(dupl_with4)

  ODB_dduplchk = RC4

End Function ODB_dduplchk

Integer(Kind=8) Function ODB_igroupify(a, idx, off, cnt, grplist)

  Use odb_module, only : ODB_groupify4 => ODB_groupify

  implicit none

  INTEGER(KIND=8), intent(in)            :: a(:)
  INTEGER(KIND=8), intent(in) , optional :: idx(:)
  INTEGER(KIND=8), intent(out), optional :: off(:), cnt(:)
  INTEGER(KIND=8), intent(out), optional :: grplist(:)

  INTEGER(KIND=JPIM), allocatable :: a4(:)
  INTEGER(KIND=JPIM), allocatable :: idx4(:)
  INTEGER(KIND=JPIM), allocatable :: off4(:), cnt4(:)
  INTEGER(KIND=JPIM), allocatable :: grplist4(:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0,o3=0,o4=0

  allocate(a4(size(a)))
  a4 = a
  if (present(idx)) then
    allocate(idx4(size(idx)))
    idx4 = idx
  endif
  if (present(off)) allocate(off4(size(off)))
  if (present(cnt)) allocate(cnt4(size(cnt)))
  if (present(grplist)) allocate(grplist4(size(grplist)))
  
  if (present(idx)) o1=1
  if (present(off)) o2=2
  if (present(cnt)) o3=4
  if (present(grplist)) o4=8
  o=o1+o2+o3+o4

  select case(o)
  case(0)
     RC4=ODB_groupify4(a4)
  case(1)
     RC4=ODB_groupify4(a4, idx=idx4)
  case(2)
     RC4=ODB_groupify4(a4, off=off4)
  case(3)
     RC4=ODB_groupify4(a4, idx=idx4, off=off4)
  case(4)
     RC4=ODB_groupify4(a4, cnt=cnt4)
  case(5)
     RC4=ODB_groupify4(a4, idx=idx4, cnt=cnt4)
  case(6)
     RC4=ODB_groupify4(a4, off=off4, cnt=cnt4)
  case(7)
     RC4=ODB_groupify4(a4, idx=idx4, off=off4, cnt=cnt4)
  case(8)
     RC4=ODB_groupify4(a4, grplist=grplist4)
  case(9)
     RC4=ODB_groupify4(a4, idx=idx4, grplist=grplist4)
  case(10)
     RC4=ODB_groupify4(a4, off=off4, grplist=grplist4)
  case(11)
     RC4=ODB_groupify4(a4, idx=idx4, off=off4, grplist=grplist4)
  case(12)
     RC4=ODB_groupify4(a4, cnt=cnt4, grplist=grplist4)
  case(13)
     RC4=ODB_groupify4(a4, idx=idx4, cnt=cnt4, grplist=grplist4)
  case(14)
     RC4=ODB_groupify4(a4, off=off4, cnt=cnt4, grplist=grplist4)
  case(15)
     RC4=ODB_groupify4(a4, idx=idx4, off=off4, cnt=cnt4, grplist=grplist4)
  end select
  
  off = off4
  cnt = cnt4
  grplist = grplist4
  deallocate(a4)
  if (present(idx)) deallocate(idx4)
  if (present(off)) deallocate(off4)
  if (present(cnt)) deallocate(cnt4)
  if (present(grplist)) deallocate(grplist4)

  ODB_igroupify = RC4

End Function ODB_igroupify
  
Integer(Kind=8) Function ODB_dgroupify(a, idx, off, cnt, grplist)

  Use odb_module, only : ODB_groupify4 => ODB_groupify

  implicit none

  REAL(KIND=JPRB), intent(in)            :: a(:)
  INTEGER(KIND=8), intent(in) , optional :: idx(:)
  INTEGER(KIND=8), intent(out), optional :: off(:), cnt(:)
  REAL(KIND=JPRB), intent(out), optional :: grplist(:)

  INTEGER(KIND=JPIM), allocatable :: idx4(:)
  INTEGER(KIND=JPIM), allocatable :: off4(:), cnt4(:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0,o3=0,o4=0

  if (present(idx)) then
    allocate(idx4(size(idx)))
    idx4 = idx
  endif
  if (present(off)) allocate(off4(size(off)))
  if (present(cnt)) allocate(cnt4(size(cnt)))
  
  if (present(idx)) o1=1
  if (present(off)) o2=2
  if (present(cnt)) o3=4
  if (present(grplist)) o4=8
  o=o1+o2+o3+o4

  select case(o)
  case(0)
     RC4=ODB_groupify4(a)
  case(1)
     RC4=ODB_groupify4(a, idx=idx4)
  case(2)
     RC4=ODB_groupify4(a, off=off4)
  case(3)
     RC4=ODB_groupify4(a, idx=idx4, off=off4)
  case(4)
     RC4=ODB_groupify4(a, cnt=cnt4)
  case(5)
     RC4=ODB_groupify4(a, idx=idx4, cnt=cnt4)
  case(6)
     RC4=ODB_groupify4(a, off=off4, cnt=cnt4)
  case(7)
     RC4=ODB_groupify4(a, idx=idx4, off=off4, cnt=cnt4)
  case(8)
     RC4=ODB_groupify4(a, grplist=grplist)
  case(9)
     RC4=ODB_groupify4(a, idx=idx4, grplist=grplist)
  case(10)
     RC4=ODB_groupify4(a, off=off4, grplist=grplist)
  case(11)
     RC4=ODB_groupify4(a, idx=idx4, off=off4, grplist=grplist)
  case(12)
     RC4=ODB_groupify4(a, cnt=cnt4, grplist=grplist)
  case(13)
     RC4=ODB_groupify4(a, idx=idx4, cnt=cnt4, grplist=grplist)
  case(14)
     RC4=ODB_groupify4(a, off=off4, cnt=cnt4, grplist=grplist)
  case(15)
     RC4=ODB_groupify4(a, idx=idx4, off=off4, cnt=cnt4, grplist=grplist)
  end select
  
  off = off4
  cnt = cnt4
  if (present(idx)) deallocate(idx4)
  if (present(off)) deallocate(off4)
  if (present(cnt)) deallocate(cnt4)

  ODB_dgroupify = RC4

End Function ODB_dgroupify

Integer(Kind=8) Function ODB_itolsearch(tol, v, ksta, kend, index)

  Use odb_module, only : ODB_tolsearch4 => ODB_tolsearch

  implicit none

  INTEGER(KIND=8), intent(in)           :: ksta, kend
  INTEGER(KIND=8), intent(in)           :: tol, v(:)
  INTEGER(KIND=8), intent(in), OPTIONAL :: index(:)

  INTEGER(KIND=JPIM)              :: ksta4, kend4, tol4
  INTEGER(KIND=JPIM), allocatable :: v4(:)
  INTEGER(KIND=JPIM), allocatable :: index4(:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  ksta4 = ksta
  kend4 = kend
  tol4 = tol
  allocate(v4(size(v)))
  v4 = v
  if (present(index)) then
    allocate(index4(size(index)))
    index4 = index
  endif

  if (present(index)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_tolsearch4(tol4, v4, ksta4, kend4)
  case(1)
     RC4=ODB_tolsearch4(tol4, v4, ksta4, kend4, index=index4)
  end select
  
  deallocate(v4)
  if (present(index)) deallocate(index4)

  ODB_itolsearch = RC4

End Function ODB_itolsearch
  
Integer(Kind=8) Function ODB_dtolsearch(tol, v, ksta, kend, index)

  Use odb_module, only : ODB_tolsearch4 => ODB_tolsearch

  implicit none

  INTEGER(KIND=8), intent(in)           :: ksta, kend
  REAL(KIND=JPRB), intent(in)           :: tol, v(:)
  INTEGER(KIND=8), intent(in), OPTIONAL :: index(:)

  INTEGER(KIND=JPIM)              :: ksta4, kend4
  INTEGER(KIND=JPIM), allocatable :: index4(:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0

  ksta4 = ksta
  kend4 = kend
  if (present(index)) then
    allocate(index4(size(index)))
    index4 = index
  endif

  if (present(index)) o1=1
  o=o1

  select case(o)
  case(0)
     RC4=ODB_tolsearch4(tol, v, ksta4, kend4)
  case(1)
     RC4=ODB_tolsearch4(tol, v, ksta4, kend4, index=index4)
  end select
  
  if (present(index)) deallocate(index4)

  ODB_dtolsearch = RC4

End Function ODB_dtolsearch

Integer(Kind=8) Function ODB_ibinsearch(refval, v, ksta, kend, index, cluster_start, cluster_end)
  
  Use odb_module, only : ODB_binsearch4 => ODB_binsearch

  implicit none

  INTEGER(KIND=8), intent(in)            :: ksta, kend
  INTEGER(KIND=8), intent(in)            :: v(:), refval
  INTEGER(KIND=8), intent(in) , OPTIONAL :: index(:)
  INTEGER(KIND=8), intent(out), OPTIONAL :: cluster_start, cluster_end

  INTEGER(KIND=JPIM)              :: ksta4, kend4, refval4
  INTEGER(KIND=JPIM), allocatable :: v4(:)
  INTEGER(KIND=JPIM), allocatable :: index4(:)
  INTEGER(KIND=JPIM)              :: cluster_start4, cluster_end4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0,o3=0

  ksta4 = ksta
  kend4 = kend
  allocate(v4(size(v)))
  v4 = v
  refval4 = refval
  if (present(index)) then
    allocate(index4(size(index)))
    index4 = index
  endif

  if (present(index)) o1=1
  if (present(cluster_start)) o2=2
  if (present(cluster_end)) o3=4
  o=o1+o2+o3

  select case(o)
  case(0)
     RC4=ODB_binsearch4(refval4, v4, ksta4, kend4)
  case(1)
     RC4=ODB_binsearch4(refval4, v4, ksta4, kend4, index=index4)
  case(2)
     RC4=ODB_binsearch4(refval4, v4, ksta4, kend4, cluster_start=cluster_start4)
  case(3)
     RC4=ODB_binsearch4(refval4, v4, ksta4, kend4, index=index4, cluster_start=cluster_start4)
  case(4)
     RC4=ODB_binsearch4(refval4, v4, ksta4, kend4, cluster_end=cluster_end4)
  case(5)
     RC4=ODB_binsearch4(refval4, v4, ksta4, kend4, index=index4, cluster_end=cluster_end4)
  case(6)
     RC4=ODB_binsearch4(refval4, v4, ksta4, kend4, cluster_start=cluster_start4, cluster_end=cluster_end4)
  case(7)
     RC4=ODB_binsearch4(refval4, v4, ksta4, kend4, index=index4, cluster_start=cluster_start4, cluster_end=cluster_end4)
  end select

  if (present(cluster_start)) cluster_start4 = cluster_start
  if (present(cluster_end)) cluster_end4 = cluster_end
  if (present(index)) deallocate(index4)

  ODB_ibinsearch = RC4

End Function ODB_ibinsearch

Integer(Kind=8) Function ODB_dbinsearch(refval, v, ksta, kend, index, cluster_start, cluster_end)
  
  Use odb_module, only : ODB_binsearch4 => ODB_binsearch

  implicit none

  INTEGER(KIND=8), intent(in)            :: ksta, kend
  REAL(KIND=JPRB), intent(in)            :: v(:), refval
  INTEGER(KIND=8), intent(in) , OPTIONAL :: index(:)
  INTEGER(KIND=8), intent(out), OPTIONAL :: cluster_start, cluster_end

  INTEGER(KIND=JPIM)              :: ksta4, kend4
  INTEGER(KIND=JPIM), allocatable :: index4(:)
  INTEGER(KIND=JPIM)              :: cluster_start4, cluster_end4

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0,o3=0

  ksta4 = ksta
  kend4 = kend
  if (present(index)) then
    allocate(index4(size(index)))
    index4 = index
  endif

  if (present(index)) o1=1
  if (present(cluster_start)) o2=2
  if (present(cluster_end)) o3=4
  o=o1+o2+o3

  select case(o)
  case(0)
     RC4=ODB_binsearch4(refval, v, ksta4, kend4)
  case(1)
     RC4=ODB_binsearch4(refval, v, ksta4, kend4, index=index4)
  case(2)
     RC4=ODB_binsearch4(refval, v, ksta4, kend4, cluster_start=cluster_start4)
  case(3)
     RC4=ODB_binsearch4(refval, v, ksta4, kend4, index=index4, cluster_start=cluster_start4)
  case(4)
     RC4=ODB_binsearch4(refval, v, ksta4, kend4, cluster_end=cluster_end4)
  case(5)
     RC4=ODB_binsearch4(refval, v, ksta4, kend4, index=index4, cluster_end=cluster_end4)
  case(6)
     RC4=ODB_binsearch4(refval, v, ksta4, kend4, cluster_start=cluster_start4, cluster_end=cluster_end4)
  case(7)
     RC4=ODB_binsearch4(refval, v, ksta4, kend4, index=index4, cluster_start=cluster_start4, cluster_end=cluster_end4)
  end select

  if (present(cluster_start)) cluster_start4 = cluster_start
  if (present(cluster_end)) cluster_end4 = cluster_end

  ODB_dbinsearch = RC4

End Function ODB_dbinsearch

Integer(Kind=8) Function ODB_iaggregate(oper, a, nrows, target, idx, group_by, grplist, result)

  Use odb_module, only : ODB_aggregate4 => ODB_aggregate

  implicit none

  CHARACTER(len=*), intent(in)            :: oper
  INTEGER(KIND=8) , intent(in)            :: nrows, target
  INTEGER(KIND=8) , intent(inout)         :: a(:,:)
  INTEGER(KIND=8) , intent(in) , optional :: group_by(:), idx(:)
  INTEGER(KIND=8) , intent(out), optional :: grplist(:,:)
  REAL(KIND=JPRB) , intent(out), optional :: result(:)

  INTEGER(KIND=JPIM)               :: nrows4, target4
  INTEGER(KIND=JPIM), allocatable  :: a4(:,:)
  INTEGER(KIND=JPIM), allocatable  :: group_by4(:), idx4(:)
  INTEGER(KIND=JPIM), allocatable  :: grplist4(:,:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0,o3=0,o4=0

  nrows4 = nrows
  target4 = target
  allocate(a4(size(a,1),size(a,2)))
  a4 = a

  if (present(group_by)) then
    allocate(group_by4(size(group_by)))
    group_by4 = group_by
  endif
  if (present(idx)) then
    allocate(idx4(size(idx)))
    idx4 = idx
  endif
  if (present(grplist)) then
    allocate(grplist4(size(grplist,1),size(grplist,2)))
  endif

  if (present(idx)) o1=1
  if (present(group_by)) o2=2
  if (present(grplist)) o3=4
  if (present(result)) o4=8
  o=o1+o2+o3+o4

  select case(o)
  case(0)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4)
  case(1)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, idx=idx4)
  case(2)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, group_by=group_by4)
  case(3)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, idx=idx4, group_by=group_by4)
  case(4)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, grplist=grplist4)
  case(5)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, idx=idx4, grplist=grplist4)
  case(6)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, group_by=group_by4, grplist=grplist4)
  case(7)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, idx=idx4, group_by=group_by4, grplist=grplist4)
  case(8)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, result=result)
  case(9)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, idx=idx4, result=result)
  case(10)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, group_by=group_by4, result=result)
  case(11)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, idx=idx4, group_by=group_by4, result=result)
  case(12)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, grplist=grplist4, result=result)
  case(13)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, idx=idx4, grplist=grplist4, result=result)
  case(14)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, group_by=group_by4, grplist=grplist4, result=result)
  case(15)
     RC4=ODB_aggregate4(oper, a4, nrows4, target4, idx=idx4, group_by=group_by4, grplist=grplist4, result=result)
  end select

  a = a4
  deallocate(a4)
  deallocate(idx4)
  deallocate(group_by4)
  if (present(grplist)) then
    grplist4 = grplist
    deallocate(grplist4)
  endif

  ODB_iaggregate = RC4

End Function ODB_iaggregate

Integer(Kind=8) Function ODB_daggregate(oper, a, nrows, target, idx, group_by, grplist, result)

  Use odb_module, only : ODB_aggregate4 => ODB_aggregate

  implicit none

  CHARACTER(len=*), intent(in)            :: oper
  INTEGER(KIND=8) , intent(in)            :: nrows, target
  REAL(KIND=JPRB) , intent(inout)         :: a(:,:)
  INTEGER(KIND=8) , intent(in) , optional :: group_by(:), idx(:)
  REAL(KIND=JPRB) , intent(out), optional :: grplist(:,:)
  REAL(KIND=JPRB) , intent(out), optional :: result(:)

  INTEGER(KIND=JPIM)               :: nrows4, target4
  INTEGER(KIND=JPIM), allocatable  :: group_by4(:), idx4(:)

  INTEGER(KIND=JPIM) :: RC4

  integer :: o=0,o1=0,o2=0,o3=0,o4=0

  nrows4 = nrows
  target4 = target

  if (present(group_by)) then
    allocate(group_by4(size(group_by)))
    group_by4 = group_by
  endif
  if (present(idx)) then
    allocate(idx4(size(idx)))
    idx4 = idx
  endif

  if (present(idx)) o1=1
  if (present(group_by)) o2=2
  if (present(grplist)) o3=4
  if (present(result)) o4=8
  o=o1+o2+o3+o4

  select case(o)
  case(0)
     RC4=ODB_aggregate4(oper, a, nrows4, target4)
  case(1)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, idx=idx4)
  case(2)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, group_by=group_by4)
  case(3)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, idx=idx4, group_by=group_by4)
  case(4)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, grplist=grplist)
  case(5)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, idx=idx4, grplist=grplist)
  case(6)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, group_by=group_by4, grplist=grplist)
  case(7)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, idx=idx4, group_by=group_by4, grplist=grplist)
  case(8)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, result=result)
  case(9)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, idx=idx4, result=result)
  case(10)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, group_by=group_by4, result=result)
  case(11)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, idx=idx4, group_by=group_by4, result=result)
  case(12)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, grplist=grplist, result=result)
  case(13)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, idx=idx4, grplist=grplist, result=result)
  case(14)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, group_by=group_by4, grplist=grplist, result=result)
  case(15)
     RC4=ODB_aggregate4(oper, a, nrows4, target4, idx=idx4, group_by=group_by4, grplist=grplist, result=result)
  end select

  deallocate(idx4)
  deallocate(group_by4)

  ODB_daggregate = RC4

End Function ODB_daggregate

#endif
End Module ODB_MODULE8
