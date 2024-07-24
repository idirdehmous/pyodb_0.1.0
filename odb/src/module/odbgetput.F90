MODULE odbgetput

USE PARKIND1  ,ONLY : JPIM,JPIB,JPRB
USE YOMHOOK   ,ONLY : LHOOK, DR_HOOK
USE OML_MOD    ,ONLY : OML_IN_PARALLEL

USE odbshared ! The shared (mainly internal) stuff
USE odbmp ! The message passing module
USE odbutil ! Misc utilities 
USE str, only : toupper, sadjustl, sadjustr
USE odbsort, only : keysort
#ifdef NAG
USE F90_UNIX_IO, ONLY: FLUSH
#endif

IMPLICIT NONE
SAVE
PRIVATE

#ifndef USE_CTRIM
#define ctrim(x) x
#define CTRIM(x) x
#endif

#define trimadjL(x) trim(sadjustl(x))
#define trimadjR(x) trim(sadjustr(x))

#include "fodb_checkviewreg.h"

INTERFACE ODB_put
MODULE PROCEDURE &
  & ODB_dput
END INTERFACE

INTERFACE ODB_get
MODULE PROCEDURE &
  & ODB_dget
END INTERFACE

INTERFACE ODB_ctrlw_sort
MODULE PROCEDURE &
  & ODB_dctrlw_sort
END INTERFACE

public :: ODB_gethandle
public :: ODB_addview
public :: ODB_get
public :: ODB_put
public :: ODB_ctrlw_sort
public :: ODB_getsize

CONTAINS

FUNCTION ODB_getsize(handle, dtname, nrows, ncols, nra, poolno, using, &
     & ncols_aux, colaux) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)           :: handle
INTEGER(KIND=JPIM), intent(out)          :: nrows, ncols
character(len=*), intent(in)  :: dtname
INTEGER(KIND=JPIM), intent(out), optional:: nra
INTEGER(KIND=JPIM), intent(in), optional :: poolno, using
INTEGER(KIND=JPIM), intent(out), optional:: ncols_aux, colaux(:)
INTEGER(KIND=JPIM) :: rc, ipoolno, info(6), irecur, using_it, idummy(0), icols_aux, iret, icols
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_GETSIZE',0,ZHOOK_HANDLE)

rc = 0
nrows = 0
ncols = 0
if (odbHcheck(handle, 'ODB_getsize')) then
  ipoolno = get_poolno(handle, poolno)
  CALL fODB_checkviewreg(handle, ctrim(dtname), rc, using=using)
  using_it = 0
  if (present(using)) using_it = using
  irecur = 0
  CALL cODB_getsize(handle, ipoolno, ctrim(dtname), nrows, icols, irecur, rc, using_it)
  ncols = icols
  if (db_trace) then
    info(1) = ipoolno
    info(2) = rc
    info(3) = nrows
    info(4) = ncols
    info(5) = ODB_lda(nrows)
    info(6) = using_it
    CALL cODB_trace(handle, -1,'ODB_getsize:'//dtname, info, 5)
  endif
  if (rc < 0) then
    CALL ODB_abort('ODB_getsize',&
     &'Cannot get sizes of VIEW/TABLE="'//&
     &trim(dtname)//'"',&
     &rc)
    goto 99999
  endif
  iret = 0
  if (present(colaux)) then
    colaux(:) = 0
    CALL cODB_getsize_aux(handle, ipoolno, ctrim(dtname), icols, icols_aux, colaux, size(colaux), iret, using_it)
    if (present(ncols_aux)) ncols_aux = icols_aux
  else if (present(ncols_aux)) then
    CALL cODB_getsize_aux(handle, ipoolno, ctrim(dtname), icols, ncols_aux, idummy,            0, iret, using_it)
  endif
  if (iret < 0) then
    CALL ODB_abort('ODB_getsize[aux-data]',&
     &'Cannot obtain auxiliary sizeinfo for VIEW/TABLE="'//&
     &trim(dtname)//'"',&
     &iret)
    goto 99999
  endif
endif
if (present(nra)) then
  nra = ODB_lda(nrows)
endif
99999 continue
IF (LHOOK) CALL DR_HOOK('ODB:ODB_GETSIZE',1,ZHOOK_HANDLE)
END FUNCTION ODB_getsize

RECURSIVE FUNCTION ODB_gethandle(handle, dtname, addview, abort, using) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)    :: dtname
logical, intent(in), optional   :: addview, abort
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: using
INTEGER(KIND=JPIM) :: rc, using_it
logical LLaddview, LLabort
!-- can't have Dr.Hook yet in (potentially) recursive routines
!REAL(KIND=JPRB) ZHOOK_HANDLE
!IF (LHOOK) CALL DR_HOOK('ODB:ODB_GETHANDLE',0,ZHOOK_HANDLE)
rc = 0
if (odbHcheck(handle, 'ODB_gethandle')) then
  LLaddview = .FALSE.
  if (present(addview)) LLaddview = addview
  LLabort = .TRUE.
  if (present(abort)) LLabort = abort
  using_it = 0
  if (present(using)) using_it = using
  CALL cODB_gethandle(handle, ctrim(dtname), rc, using_it)
  if (LLaddview .and. rc <= 0) then
    rc = ODB_addview(handle, dtname, abort=.FALSE.)
    CALL cODB_gethandle(handle, ctrim(dtname), rc, using_it)
  endif
  if (rc <= 0 .and. LLabort) then
    CALL ODB_abort('ODB_gethandle',&
     &'Invalid VIEW/TABLE-handle for "'//trim(dtname)//'"',&
     &rc)
    goto 99999
  endif
endif
99999 continue
!-- can't have Dr.Hook yet in (potentially) recursive routines
!IF (LHOOK) CALL DR_HOOK('ODB:ODB_GETHANDLE',1,ZHOOK_HANDLE)
END FUNCTION ODB_gethandle

RECURSIVE FUNCTION ODB_addview(handle, dtname,&
     &viewfile,&
     &select, uniqueby, from, where, orderby, sortby,&
     &query, &
     &set, abort) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)          :: handle
character(len=*), intent(in) :: dtname
character(len=*), intent(in),optional :: viewfile
character(len=*), intent(in),optional :: select, uniqueby
character(len=*), intent(in),optional :: from, where
character(len=*), intent(in),optional :: orderby, sortby
character(len=*), intent(in),optional :: query
character(len=*), intent(in),optional :: set(:)
logical, intent(in), optional :: abort
INTEGER(KIND=JPIM) :: idummy, idummy_arr(1)
INTEGER(KIND=JPIM) :: rc, nset, j
logical on_error, recreate, is_table, fromfile, LLabort
character(len=maxstrlen) CL_where, CL_orderby
character(len=maxstrlen) CL_uniqueby, CL_set
character(len=maxstrlen) CL_query, CL_from
character(len=maxvarlen) dbname
!-- can't have Dr.Hook yet in (potentially) recursive routines
!REAL(KIND=JPRB) ZHOOK_HANDLE
!IF (LHOOK) CALL DR_HOOK('ODB:ODB_ADDVIEW',0,ZHOOK_HANDLE)

rc = 0

is_table = .FALSE.
if (len(dtname) >= 1) is_table = (dtname(1:1) == '@')

if (is_table) goto 99999

CALL cODB_trace(handle, 1,'ODB_addview:'//dtname, idummy_arr, 0)

LLabort = .TRUE.
if (present(abort)) LLabort = abort

fromfile = present(viewfile)

on_error = .not. odbHcheck(handle, 'ODB_addview', abort=.FALSE.)
on_error = on_error .or.&
     &(fromfile .and.&
     &(present(select) .or. present(from) .or. present(where)))
!!on_error = on_error .or. (present(select) .and. .not. present(from))
on_error = on_error .or. (.not. present(select) .and. present(from))
on_error = on_error .or.&
     &(present(where) .and.&
     &(.not. present(select) .and. .not. present(from)))
on_error = on_error .or.(present(orderby) .and. present(sortby))
on_error = on_error .or.(fromfile .and. present(query))
on_error = on_error .or. &
     &((present(select) .or. present(from) .or. present(where)).and.&
     &present(query))

if (on_error) then
  CALL ODB_abort('ODB_addview',&
   &'Conflicting and/or ambiguous arguments in VIEW="'//&
   &trim(dtname)//'"')
  rc = -2
  goto 99999
else
  dbname = db(handle)%name
  recreate = (present(select) .and. present(from))
  recreate = recreate .or. present(query)
  if (ODBMP_myproc == 1) then
    if (recreate) then

      if (present(uniqueby)) then
        CL_uniqueby = trim(sadjustl(uniqueby))
      else
        CL_uniqueby = ' '
      endif

      if (present(from)) then
        CL_from = trim(sadjustl(from))
      else
        CL_from = '*' 
      endif

      if (present(where)) then
        CL_where = trim(sadjustl(where))
      else
        CL_where = ' '
      endif

      if (present(orderby)) then
        CL_orderby = trim(sadjustl(orderby))
      else if (present(sortby)) then
        CL_orderby = trim(sadjustl(sortby))
      else
        CL_orderby = ' '
      endif

      nset = 0
      if (present(set)) nset = size(set)
      CL_set = ' '
      do j=1,nset
        CL_set = trim(CL_set)//'SET '//trimadjL(set(j))//';'//char(10)
      enddo

      if (present(query)) then
        CL_query = trim(sadjustl(query))
      else
        CL_query = ' '
      endif

      CALL cODB_makeview(&
       &handle,&
       &dtname,&
       &'',&
       &trimadjL(select),&
       &trim(CL_uniqueby),&
       &trimadjL(CL_from),&
       &trim(CL_where), &
       &trim(CL_orderby), &
       &trim(CL_set),&
       &trim(CL_query),&
       &rc)  

      if (rc < 0 .and. LLabort) then
        CALL ODB_abort('ODB_addview',&
         &'Compilation failed for VIEW="'//&
         &trim(dtname)//'", database="'//trim(dbname)//'"')
      endif
      if (rc < 0) goto 99999

    else if (fromfile) then

      CALL cODB_makeview(&
       &handle, &
       &dtname,&
       &trimadjL(viewfile),&
       &'',&
       &'',&
       &'',&
       &'', &
       &'', &
       &'',&
       &'',&
       &rc)

      if (rc < 0 .and. LLabort) then
        CALL ODB_abort('ODB_addview',&
         &'Compilation failed for VIEW="'//&
         &trim(dtname)//'", viewfile="'//&
         &trim(viewfile)//'", database="'//trim(dbname)//'"')
      endif
      if (rc < 0) goto 99999

    endif
  endif

  if (recreate .or. fromfile) then
    CALL ODBMP_sync()
    rc = 0
  else ! Check if already registered
    rc = ODB_gethandle(handle, dtname, abort=.FALSE.)    
  endif

  ! Add only if not already added (or recreate/fromfile)
  if (rc <= 0) CALL cODB_linkview(handle, dtname, rc)

  if (rc < 0 .and. LLabort) then
    CALL ODB_abort('ODB_addview',&
     &'Linking failed for VIEW="'//&
     &trim(dtname)//'", database="'//trim(dbname)//'"')
  endif
  if (rc < 0) goto 99999

  rc = ODB_gethandle(handle, dtname)
endif

idummy_arr = rc
CALL cODB_trace(handle, 0,'ODB_addview:'//dtname, idummy_arr, 1)

99999 continue
!-- can't have Dr.Hook yet in (potentially) recursive routines
!IF (LHOOK) CALL DR_HOOK('ODB:ODB_ADDVIEW',1,ZHOOK_HANDLE)
END FUNCTION ODB_addview

#undef REAL_VERSION

#define REAL_VERSION 8
#include "fodb.h"
#undef REAL_VERSION

#ifndef USE_CTRIM
#undef ctrim
#undef CTRIM
#endif

#undef trimadjL
#undef trimadjR

END MODULE odbgetput
