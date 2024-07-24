#ifdef RS6K
@PROCESS NOEXTCHK
#endif
MODULE odbutil

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK, DR_HOOK

USE odbshared ! The shared (internal) stuff
USE odbmp ! The message passing module
USE str, only : toupper, tolower, sadjustl, sadjustr
USE odbsort, only : keysort

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

INTERFACE ODB_setval
MODULE PROCEDURE & 
  & ODB_dsetval, ODB_isetval
END INTERFACE

INTERFACE ODB_duplchk
MODULE PROCEDURE & 
  & ODB_iduplchk, ODB_dduplchk
END INTERFACE

INTERFACE duplchk0
MODULE PROCEDURE & 
  & iduplchk0, dduplchk0
END INTERFACE

INTERFACE ODB_groupify
MODULE PROCEDURE & 
  & ODB_igroupify, ODB_dgroupify
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

public :: ODB_getnames
public :: ODB_varindex
public :: ODB_getprecision
public :: ODB_setval
public :: ODB_getval
public :: ODB_varexist
public :: ODB_poolinfo
public :: ODB_duplchk
public :: ODB_groupify
public :: ODB_tolsearch
public :: ODB_binsearch
public :: ODB_aggregate
public :: ODB_twindow
public :: ODB_tdiff
public :: ODB_analysis_datetime
public :: ODB_get_update_info
public :: ODB_get_aggr_info
public :: ODB_sortkeys
public :: ODB_io_method
public :: ODB_has_select_distinct
public :: ODB_has_uniqueby
public :: ODB_has_orderby
public :: ODB_has_aggrfuncs
public :: ODB_control_word_info

CONTAINS

FUNCTION ODB_poolinfo(handle, poolids, with_poolmask) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)            :: handle
INTEGER(KIND=JPIM), intent(out), optional :: poolids(:)
logical, intent(in), optional :: with_poolmask
INTEGER(KIND=JPIM) :: rc, j, jj, jlocal, npools, poolmask_set
INTEGER(KIND=JPIM), allocatable :: ids(:), pool_list(:)
logical LL_with_poolmask
logical, allocatable :: LL_poolmask(:)
rc = 0
if (odbHcheck(handle, 'ODB_poolinfo')) then
  npools = db(handle)%glbNpools ! Global no. of pools

  if (present(poolids)) then
    allocate(LL_poolmask(npools))
    LL_with_poolmask = .FALSE.
    if (present(with_poolmask)) LL_with_poolmask = with_poolmask

    if (LL_with_poolmask) then
      allocate(pool_list(npools))
      pool_list(:) = 0
      !-- Note: The current poolmask only reliable/applicable for local pools !!
      CALL cODB_get_poolmask(handle, npools, pool_list, poolmask_set, rc)
      if (rc == 0 .or. poolmask_set == 0) then
        !-- poolmask empty or not set at all --> process all pools
        LL_poolmask(:) = .TRUE.
      else
        do j=1,npools
          if (pool_list(j) == 1) then
            LL_poolmask(j) = .TRUE.
          else
            LL_poolmask(j) = .FALSE.
          endif
        enddo
      endif
      deallocate(pool_list)
    else ! disregard poolmask i.e. consider all pools included
      LL_poolmask(:) = .TRUE.
    endif

    allocate(ids(npools))
    ids(:) = 0 ! Initialize to non-existent pool number <= 0
    jj = 0
    do j=1,npools
      if (ODBMP_physproc(j) == ODBMP_myproc) then ! locally owned pools
        if (LL_poolmask(j)) then ! poolmask is guaranteed been set for local pools only
          jj = jj + 1
          ids(jj) = j
        endif
      endif
    enddo
    jlocal = jj
    do j=1,npools
      if (ODBMP_physproc(j) /= ODBMP_myproc) then ! non-local pools
      ! we do not test for
        jj = jj + 1
        ids(jj) = j
      endif
    enddo

    do j=1,min(npools,size(poolids))
      poolids(j) = ids(j)
    enddo
    deallocate(ids)
    deallocate(LL_poolmask)

!---  Note: Return code is now local no. of pools, i.e. the first entries put into poolids(:)
    rc = jlocal   ! i.e. db(handle)%locNpools if no poolmasking taken into account
  else
!---  Note: Return code without presence of poolids(:) arg is global no. of pools
!           (poolmask was not taken into account)
    rc = npools
  endif
endif
END FUNCTION ODB_poolinfo



FUNCTION ODB_twindow(&
     &target_date,&
     &target_time,&
     &analysis_date,&
     &analysis_time,&
     &left_margin,&
     &right_margin)&
     &RESULT(rc)
INTEGER(KIND=JPIM), intent(in) :: target_date, target_time
INTEGER(KIND=JPIM), intent(in) :: analysis_date, analysis_time
INTEGER(KIND=JPIM), intent(in) :: left_margin, right_margin
INTEGER(KIND=JPIM) :: rc
CALL cODB_twindow(&
     &target_date, target_time,&
     &analysis_date, analysis_time,&
     &left_margin, right_margin,&
     &rc)
END FUNCTION ODB_twindow



FUNCTION ODB_tdiff(&
     &target_date,&
     &target_time,&
     &analysis_date,&
     &analysis_time)&
     &RESULT(rc)
INTEGER(KIND=JPIM), intent(in) :: target_date, target_time
INTEGER(KIND=JPIM), intent(in) :: analysis_date, analysis_time
INTEGER(KIND=JPIM) :: rc
CALL cODB_tdiff(target_date, target_time,analysis_date, analysis_time,rc)
END FUNCTION ODB_tdiff



SUBROUTINE ODB_analysis_datetime(handle, andate, antime)
INTEGER(KIND=JPIM), intent(in) :: handle, andate, antime
INTEGER(KIND=JPIM) :: rc
if (odbHcheck(handle, 'ODB_analysis_datetime')) then
  rc = ODB_twindow(andate, antime, andate, antime, 0, 0)
  if (rc /= 1) then
    write(0,*)'*** Warning: Attempt to change analysis date/time failed'
    write(0,*) 'Wrong analysis date: ',andate,' and/or time ',antime
  else
    db(handle)%analysisDT(0) = andate
    db(handle)%analysisDT(1) = antime
  endif
endif
END SUBROUTINE ODB_analysis_datetime



FUNCTION ODB_getnames(handle, dtname, mode, outnames) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)         :: handle
character(len=*), intent(in)  :: dtname, mode
character(len=*), intent(out), optional :: outnames(:)
INTEGER(KIND=JPIM) :: rc
character(len=maxstrlen) CL_str
character(len=1) ODB_tag_delim
INTEGER(KIND=JPIM) :: what, j, istart, iend, ilen
logical LL_alltables, LL_cached
rc = 0
if ( mode == 'type'      .or. &
     mode == 'ctype'     .or. &
     mode == 'datatype' ) then
  what = 1
else if (mode == 'name' .or. mode == 'varname' .or.  mode == 'colname') then
  what = 2
else if (mode == 'table' .or. mode == 'tablename') then
  what = 3
else if (mode == 'view' .or. mode == 'viewname') then
  what = 4
else if (mode == 'sql' .or. mode == 'sqlname') then
  what = 4
else if (mode == 'query' .or. mode == 'queryname') then
  what = 4
else if (mode == 'ftntype' .or. mode == 'ftndatatype') then
  what = 11
else if (mode == 'exttype'      .or. &
         mode == 'extctype'     .or. &
         mode == 'extdatatype' ) then
  what = 101
else if (mode == 'extname' .or. mode == 'extvarname' .or. mode == 'colvarname') then
  what = 102
else if (mode == 'extftntype' .or. mode == 'extftndatatype') then
  what = 111
else
  what = 0
endif

if (odbHcheck(handle, 'ODB_getnames')) then
  LL_alltables = .FALSE.
  LL_cached = .FALSE.
  if (len(dtname) >= 1) then
    LL_alltables = (dtname(1:1) == '*' .AND. what == 3)
    LL_cached = LL_alltables .AND. associated(db(handle)%ctables)
  endif
  if (present(outnames)) then
!    CL_str = ' '
    if (LL_cached) then
      rc = db(handle)%ntables
    else
      CALL codb_strblank(CL_str) ! faster initialization
      CALL fODB_checkviewreg(handle, ctrim(dtname), rc)
      CALL cODB_getnames(handle, ctrim(dtname), what, CL_str, ilen, rc)
    endif
    if (rc < 0) then
      CALL ODB_abort('ODB_getnames',&
       &'Internal error: Cannot fit names(mode='//&
       &trim(mode)//&
       &') of VIEW/TABLE="'//trim(dtname)//&
       &'" into the array',&
       &rc)
      return
    endif
    if (rc > size(outnames)) then
      CALL ODB_abort('ODB_getnames',&
       &'Require more space for output names(mode='//&
       &trim(mode)//&
       &') of VIEW/TABLE="'//trim(dtname)//'"',&
       &rc)
      return
    endif
  else ! Just get the dimension/size
    if (LL_cached) then
      rc = db(handle)%ntables
    else
      CALL fODB_checkviewreg(handle, ctrim(dtname), rc)
      CALL cODB_getnames(handle, ctrim(dtname), what, '', ilen, rc)
    endif
    if (rc < 0) then
      CALL ODB_abort('ODB_getnames',&
       &'Internal error: with mode='//&
       &trim(mode)//&
       &') of VIEW/TABLE="'//trim(dtname)//&
       &'"',&
       &rc)
      return
    endif
!--   Yes, this is indeed correct: 
!     -- Just return the no. elems needed for outnames(:)
    return
  endif
  if (LL_cached) then
    do j=1,min(rc,size(outnames))
      outnames(j) = db(handle)%ctables(j)
    enddo
  else
    call cODB_tag_delim(ODB_tag_delim)
    iend = 1
    do j=1,min(rc,size(outnames))
      istart = iend + 1
      iend = iend + index(CL_str(istart:),ODB_tag_delim)
      outnames(j) = CL_str(istart:iend-1)
    enddo
    if (LL_alltables .AND. rc == db(handle)%ntables) then
      allocate(db(handle)%ctables(rc))
      iend = 1
      do j=1,rc
        istart = iend + 1
        iend = iend + index(CL_str(istart:),ODB_tag_delim)
        db(handle)%ctables(j) = CL_str(istart:iend-1)
      enddo
    endif
  endif
endif
END FUNCTION ODB_getnames



FUNCTION ODB_varindex(handle, dtname, names, idx) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)         :: handle
character(len=*), intent(in)  :: dtname
character(len=*), intent(in)  :: names(:)
INTEGER(KIND=JPIM), intent(out)        :: idx(:)
INTEGER(KIND=JPIM), parameter :: what = 2
INTEGER(KIND=JPIM) :: rc
character(len=len(names)) CL_names
character(len=maxstrlen) CL_str
character(len=1) ODB_tag_delim
character(len=30) CL_tmp
INTEGER(KIND=JPIM) :: i, j, ncols, ncols_idx
INTEGER(KIND=JPIM) :: i1
INTEGER(KIND=JPIM) :: jleft, jright, number, ilen
INTEGER(KIND=JPIM), allocatable :: istart(:), iend(:)
logical is_table, is_formula, takethis

is_table = .FALSE.
if (len(dtname) >= 1) then
  is_table = (dtname(1:1) == '@')
endif

rc = 0
if (odbHcheck(handle, 'ODB_varindex')) then
!  CL_str = ' '
  CALL codb_strblank(CL_str) ! faster initialization
  CALL fODB_checkviewreg(handle, ctrim(dtname), rc)
  CALL cODB_getnames(handle, ctrim(dtname), what, CL_str, ilen, rc)
  if (rc <= 0) then
    CALL ODB_abort('ODB_varindex',&
     &'Internal error: Cannot fit names'//&
     &' of VIEW/TABLE="'//trim(dtname)//&
     &'" into the array',&
     &rc)
    return
  endif
  ncols = rc
  allocate(istart(ncols))
  allocate(iend(0:ncols))
  iend(0) = 0
  call cODB_tag_delim(ODB_tag_delim)
  do j=1,ncols
    istart(j) = iend(j-1) + 2
    iend(j) = istart(j) + index(CL_str(istart(j):),ODB_tag_delim) - 2
  enddo

  idx(:) = 0
  ncols_idx = min(size(names), size(idx))
  rc = 0

  do i=1,ncols_idx
    CL_names = trimadjL(names(i))

! detect whether te current CL_names is in fact a formula or nickname 
    if (is_table) then
      is_formula = .FALSE.
    else
      i1 = scan(trim(CL_names), '@Formula', back=.TRUE.)
      is_formula = (i1 > 0) ! Formula
      if (.not.is_formula) then
        i1 = index(trim(CL_names), '@')
        if (i1 <= 0) then
          is_formula = .TRUE.
          CL_names = trim(CL_names)//'@Formula'
        endif
      endif
      if (is_formula) goto 988
    endif

! check for '[<number>]' and convert it to '_<number>'
    jleft = index(CL_names,'[')
    if (jleft > 1) then
       jright = index(CL_names,']', back=.TRUE.)
       if (jright > jleft + 1) then
          read(CL_names(jleft+1:jright-1),*,err=987,end=987) number
       else if (jright == jleft + 1) then
          number = 0
       else
          goto 987
       endif
       if (number < 0) then
          write(CL_tmp,*) -number
          CL_names = CL_names(1:jleft-1)//'__'//&
            trimadjL(CL_tmp) &
            //CL_names(jright+1:)
       else
          write(CL_tmp,*) number
         CL_names = CL_names(1:jleft-1)//'_'//&
            trimadjL(CL_tmp) &
            //CL_names(jright+1:)
       endif
    endif

987 continue
    i1 = index(CL_names,'(')
    if (i1 == 0) then
      i1 = 1
    else
      CALL toupper(CL_names(1:i1-1))
    endif
    CALL tolower(CL_names(i1:))

988 continue

    LOOP: do j=1,ncols
      takethis = .FALSE.
      if (is_table) then
        if (trim(CL_names)//'@'//trim(dtname(2:)) == CL_str(istart(j):iend(j))) takethis = .TRUE.
      else
        if (CL_names == CL_str(istart(j):iend(j))) takethis = .TRUE.
      endif
      if (takethis) then
        rc = rc + 1
        idx(i) = j
        exit LOOP
      endif
    enddo LOOP
  enddo

  deallocate(istart)
  deallocate(iend)
endif
END FUNCTION ODB_varindex



FUNCTION ODB_getprecision(handle, dtname, maxbits, anyflp)RESULT(rc)
INTEGER(KIND=JPIM), intent(in)         :: handle
INTEGER(KIND=JPIM), intent(out)        :: maxbits
logical, intent(out)          :: anyflp
character(len=*), intent(in)  :: dtname
INTEGER(KIND=JPIM) :: rc, ianyflp
rc = 0
anyflp = .FALSE.
maxbits = 0
if (odbHcheck(handle, 'ODB_getprecision')) then
  CALL cODB_getprecision(handle, ctrim(dtname), maxbits, ianyflp, rc)
  if (rc <= 0) then
    CALL ODB_abort('ODB_getprecision',&
     &'Internal error: Cannot get precision'//&
     &' of VIEW/TABLE="'//trim(dtname)//&
     &'" into the array',&
     &rc)
    return
  endif
  anyflp = (ianyflp > 0)
endif
END FUNCTION ODB_getprecision



FUNCTION ODB_dsetval(handle, varname, newvalue, viewname, using)RESULT(oldvalue)
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)  :: varname
REAL(KIND=JPRB), intent(in)              :: newvalue
character(len=*), intent(in), optional :: viewname
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: using
REAL(KIND=JPRB) :: oldvalue
INTEGER(KIND=JPIM) :: rc, using_it
oldvalue = 0
if (odbHcheck(handle, 'ODB_dsetval')) then
  using_it = 0
  if (present(using)) using_it = using
  if (present(viewname)) then
    CALL fODB_checkviewreg(handle, ctrim(viewname), rc)
    CALL cODB_setval(&
     &ctrim(db(handle)%name), ctrim(varname), ctrim(viewname),&
     &newvalue, oldvalue, using_it)
  else
    CALL cODB_setval(&
     &ctrim(db(handle)%name), ctrim(varname), ctrim(''),&
     &newvalue, oldvalue, using_it)
  endif
endif
END FUNCTION ODB_dsetval



FUNCTION ODB_isetval(handle, varname, newvalue_in, viewname, using)RESULT(oldvalue)
INTEGER(KIND=JPIM), intent(in)            :: handle
character(len=*), intent(in)     :: varname
INTEGER(KIND=JPIM), intent(in)            :: newvalue_in
character(len=*), intent(in), optional :: viewname
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: using
REAL(KIND=JPRB) :: newvalue
INTEGER(KIND=JPIM) :: oldvalue
oldvalue = 0
if (odbHcheck(handle, 'ODB_isetval')) then
  newvalue = newvalue_in
  oldvalue = ODB_setval(handle, varname, newvalue, viewname, using)
endif
END FUNCTION ODB_isetval



FUNCTION ODB_getval(handle, varname, viewname, using) RESULT(value)
INTEGER(KIND=JPIM), intent(in)         :: handle
character(len=*), intent(in)  :: varname
character(len=*), intent(in), optional :: viewname
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: using
REAL(KIND=JPRB) :: value
INTEGER(KIND=JPIM) :: rc, using_it
value = 0
if (odbHcheck(handle, 'ODB_getval')) then
  using_it = 0
  if (present(using)) using_it = using
  if (present(viewname)) then
    CALL fODB_checkviewreg(handle, ctrim(viewname), rc)
    CALL cODB_getval(ctrim(db(handle)%name), ctrim(varname), ctrim(viewname),value, using_it)
  else
    CALL cODB_getval(ctrim(db(handle)%name), ctrim(varname), ctrim('')      ,value, using_it)
  endif
endif
END FUNCTION ODB_getval



FUNCTION ODB_varexist(handle, varname, viewname) RESULT(exist)
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)  :: varname
character(len=*), intent(in), optional :: viewname
REAL(KIND=JPRB) :: oldvalue, tmpvalue, testvalue
logical exist
exist = .FALSE.
if (odbHcheck(handle, 'ODB_varexist')) then
  tmpvalue = 1998
  oldvalue = ODB_setval(handle, varname, tmpvalue, viewname)
  testvalue = ODB_getval(handle, varname, viewname)
  exist = (tmpvalue == testvalue)
  tmpvalue = ODB_setval(handle, varname, oldvalue, viewname)
endif
END FUNCTION ODB_varexist



FUNCTION ODB_get_update_info(handle, dtname, colput) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)         :: handle
character(len=*), intent(in)  :: dtname
logical, intent(out)          :: colput(:)
INTEGER(KIND=JPIM) :: can_update(size(colput))
INTEGER(KIND=JPIM) :: rc, ncols
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_GET_UPDATE_INFO',0,ZHOOK_HANDLE)
rc = 0
colput(:) = .FALSE. ! be conservative on this
if (odbHcheck(handle, 'ODB_get_update_info')) then
  ncols = size(colput)
  can_update(:) = 0
  CALL cODB_update_info(handle, ctrim(dtname), ncols, can_update, rc)
  if (ncols >=1 .and. ncols <= size(colput)) then
    colput(1:ncols) = (can_update(1:ncols) == 1)
  endif
endif
IF (LHOOK) CALL DR_HOOK('ODB:ODB_GET_UPDATE_INFO',1,ZHOOK_HANDLE)
END FUNCTION ODB_get_update_info


FUNCTION ODB_get_aggr_info(handle, dtname, aggr_func_flag, phase_id, poolno, using) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)         :: handle
character(len=*), intent(in)  :: dtname
INTEGER(KIND=JPIM), intent(out) :: aggr_func_flag(:), phase_id
INTEGER(KIND=JPIM), intent(in), optional :: poolno, using
INTEGER(KIND=JPIM) :: rc, ncols, ipoolno, using_it
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_GET_AGGR_INFO',0,ZHOOK_HANDLE)
rc = 0
aggr_func_flag(:) = 0
if (odbHcheck(handle, 'ODB_get_aggr_info')) then
  ncols = size(aggr_func_flag)
  ipoolno = -1
  if (present(poolno)) ipoolno = poolno
  using_it = 0
  if (present(using)) using_it = using
#if 0
  write(0,*) 'ODB_get_aggr_info< ipoolno, ncols, using_it=',ipoolno, ncols, using_it
#endif
  CALL cODB_aggr_info(handle, ipoolno, ctrim(dtname), ncols, aggr_func_flag, phase_id, using_it, rc)
#if 0
  write(0,*) 'ODB_get_aggr_info> rc, phase_id=',rc,phase_id
  write(0,*) 'ODB_get_aggr_info> aggr_func_flag(:)=',aggr_func_flag
#endif
  if (ODBMP_nproc > 1) phase_id = -1 ! Effectively disables the use of pf->tmp in codb.c/aggr.c
#if 0
  write(0,*) 'ODB_get_aggr_info>> ODBMP_nproc, phase_id=',ODBMP_nproc, phase_id
#endif
endif
IF (LHOOK) CALL DR_HOOK('ODB:ODB_GET_AGGR_INFO',1,ZHOOK_HANDLE)
END FUNCTION ODB_get_aggr_info


FUNCTION ODB_sortkeys(handle, dtname, keys) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)  :: handle
character(len=*), intent(in)    :: dtname
INTEGER(KIND=JPIM),intent(out) , optional :: keys(:)
INTEGER(KIND=JPIM) rc, idummy_arr(0), vhandle
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_SORTKEYS',0,ZHOOK_HANDLE)
rc = 0
if (odbHcheck(handle, 'ODB_sortkeys',abort=.FALSE.)) then
  CALL fODB_checkviewreg(handle, ctrim(dtname), vhandle)
  if (present(keys)) then
    if (size(keys) > 0) then
      CALL cODB_sortkeys(handle, dtname, size(keys), keys(1), rc)
    endif
  else
    CALL cODB_sortkeys(handle, dtname, 0, idummy_arr, rc)
  endif
endif
IF (LHOOK) CALL DR_HOOK('ODB:ODB_SORTKEYS',1,ZHOOK_HANDLE)
END FUNCTION ODB_sortkeys


FUNCTION ODB_io_method(handle) RESULT(rc)
INTEGER(KIND=JPIM), intent(in)  :: handle
INTEGER(KIND=JPIM) rc
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_IO_METHOD',0,ZHOOK_HANDLE)
rc = 1
if (odbHcheck(handle, 'ODB_io_method',abort=.FALSE.)) then
  rc = db(handle)%io_method
endif
IF (LHOOK) CALL DR_HOOK('ODB:ODB_IO_METHOD',1,ZHOOK_HANDLE)
END FUNCTION ODB_io_method


FUNCTION ODB_has_select_distinct(handle, dtname, ncols) RESULT(has_select_distinct)
INTEGER(KIND=JPIM), intent(in)  :: handle
character(len=*), intent(in)    :: dtname
INTEGER(KIND=JPIM), intent(out), optional :: ncols
logical has_select_distinct, is_table
INTEGER(KIND=JPIM) view_info(1), nview_info, vhandle, icols
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_HAS_SELECT_DISTINCT',0,ZHOOK_HANDLE)
has_select_distinct = .FALSE.
icols = 0
if (odbHcheck(handle, 'ODB_has_select_distinct',abort=.FALSE.)) then
  CALL fODB_checkviewreg(handle, ctrim(dtname), vhandle)
  is_table = .FALSE.
  if (len(dtname) >= 1) is_table = (dtname(1:1) == '@')
  if (is_table) then
    has_select_distinct = .FALSE.
  else
    nview_info = 0
    view_info(:) = 0
    CALL cODB_get_view_info(handle, ctrim(dtname), view_info(1), size(view_info), nview_info)
    ! A negative view_info(1) should contain ABS() of no. of columns in SELECT DISTINCT
    has_select_distinct = (nview_info >= 1 .AND. view_info(1) < 0)
    icols = abs(view_info(1))
  endif
endif
if (present(ncols)) ncols = icols
IF (LHOOK) CALL DR_HOOK('ODB:ODB_HAS_SELECT_DISTINCT',1,ZHOOK_HANDLE)
END FUNCTION ODB_has_select_distinct


FUNCTION ODB_has_uniqueby(handle, dtname, ncols) RESULT(has_uniqueby)
INTEGER(KIND=JPIM), intent(in)  :: handle
character(len=*), intent(in)    :: dtname
INTEGER(KIND=JPIM), intent(out), optional :: ncols
logical has_uniqueby, is_table
INTEGER(KIND=JPIM) view_info(1), nview_info, vhandle, icols
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_HAS_UNIQUEBY',0,ZHOOK_HANDLE)
has_uniqueby = .FALSE.
icols = 0
if (odbHcheck(handle, 'ODB_has_uniqueby',abort=.FALSE.)) then
  CALL fODB_checkviewreg(handle, ctrim(dtname), vhandle)
  is_table = .FALSE.
  if (len(dtname) >= 1) is_table = (dtname(1:1) == '@')
  if (is_table) then
    has_uniqueby = .FALSE.
  else
    nview_info = 0
    view_info(:) = 0
    CALL cODB_get_view_info(handle, ctrim(dtname), view_info(1), size(view_info), nview_info)
    ! A positive view_info(1) should contain no. of columns in UNIQUEBY-clause
    has_uniqueby = (nview_info >= 1 .AND. view_info(1) > 0)
    icols = abs(view_info(1))
  endif
endif
if (present(ncols)) ncols = icols
IF (LHOOK) CALL DR_HOOK('ODB:ODB_HAS_UNIQUEBY',1,ZHOOK_HANDLE)
END FUNCTION ODB_has_uniqueby


FUNCTION ODB_has_orderby(handle, dtname, ncols) RESULT(has_orderby)
INTEGER(KIND=JPIM), intent(in)  :: handle
character(len=*), intent(in)    :: dtname
INTEGER(KIND=JPIM), intent(out), optional :: ncols
logical has_orderby, is_table
INTEGER(KIND=JPIM) nkeys, vhandle, icols
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_HAS_ORDERBY',0,ZHOOK_HANDLE)
has_orderby = .FALSE.
icols = 0
if (odbHcheck(handle, 'ODB_has_orderby',abort=.FALSE.)) then
  CALL fODB_checkviewreg(handle, ctrim(dtname), vhandle)
  is_table = .FALSE.
  if (len(dtname) >= 1) is_table = (dtname(1:1) == '@')
  if (is_table) then
    has_orderby = .FALSE.
  else
    nkeys = ODB_sortkeys(handle, dtname)
    has_orderby = (nkeys > 0)
    icols = nkeys
  endif
endif
if (present(ncols)) ncols = icols
IF (LHOOK) CALL DR_HOOK('ODB:ODB_HAS_ORDERBY',1,ZHOOK_HANDLE)
END FUNCTION ODB_has_orderby


FUNCTION ODB_has_aggrfuncs(handle, dtname, ncols, phase_id, poolno, using) RESULT(has_aggrfuncs)
INTEGER(KIND=JPIM), intent(in)  :: handle
character(len=*), intent(in)    :: dtname
INTEGER(KIND=JPIM), intent(out), optional :: ncols ! No. of cols having aggregate funcs
INTEGER(KIND=JPIM), intent(out), optional :: phase_id
INTEGER(KIND=JPIM), intent(in), optional :: poolno, using
logical has_aggrfuncs, is_table
INTEGER(KIND=JPIM) vhandle, icols, idummy(0), iphase_id
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_HAS_AGGRFUNCS',0,ZHOOK_HANDLE)
has_aggrfuncs = .FALSE.
icols = 0
iphase_id = -1
if (odbHcheck(handle, 'ODB_has_aggrfuncs',abort=.FALSE.)) then
  CALL fODB_checkviewreg(handle, ctrim(dtname), vhandle)
  is_table = .FALSE.
  if (len(dtname) >= 1) is_table = (dtname(1:1) == '@')
  if (is_table) then
    has_aggrfuncs = .FALSE.
  else
    icols = ODB_get_aggr_info(handle, dtname, idummy, iphase_id, poolno=poolno, using=using)
    has_aggrfuncs = (icols > 0)
  endif
endif
if (present(ncols)) ncols = icols
if (present(phase_id)) phase_id = iphase_id
IF (LHOOK) CALL DR_HOOK('ODB:ODB_HAS_AGGRFUNCS',1,ZHOOK_HANDLE)
END FUNCTION ODB_has_aggrfuncs


FUNCTION ODB_control_word_info_vector(v,nrows,poolnos,rownums,noffset) RESULT(rc)
REAL(KIND=JPRB), intent(in)               :: v(:)
INTEGER(KIND=JPIM), intent(in), optional  :: nrows, noffset
INTEGER(KIND=JPIM), intent(out), optional :: poolnos(:), rownums(:)
INTEGER(KIND=JPIM) rc, nrows_out, j, noffset_out
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_CONTROL_WORD_INFO_VECTOR',0,ZHOOK_HANDLE)
nrows_out = size(v)
if (present(nrows))   nrows_out = min(nrows_out, nrows)
if (present(poolnos)) nrows_out = min(nrows_out, size(poolnos))
if (present(rownums)) nrows_out = min(nrows_out, size(rownums))
if (.not.present(poolnos) .and. .not.present(rownums)) nrows_out = 0
if (nrows_out > 0) then
  if (present(poolnos)) then
    CALL cODB_get_poolnos(v(1), 0, nrows_out, 0, poolnos(1))
  endif
  if (present(rownums)) then
     noffset_out = 0
    if (present(noffset)) noffset_out = noffset
    CALL cODB_get_rownum(v(1), 0, nrows_out, 0, noffset_out, rownums(1))
  endif
endif ! if (nrows_out > 0)
rc = nrows_out
IF (LHOOK) CALL DR_HOOK('ODB:ODB_CONTROL_WORD_INFO_VECTOR',1,ZHOOK_HANDLE)
END FUNCTION ODB_control_word_info_vector


FUNCTION ODB_control_word_info_matrix(d,nrows,poolnos,rownums,noffset) RESULT(rc)
REAL(KIND=JPRB), intent(in)               :: d(:,0:)
INTEGER(KIND=JPIM), intent(in), optional  :: nrows, noffset
INTEGER(KIND=JPIM), intent(out), optional :: poolnos(:), rownums(:)
INTEGER(KIND=JPIM) rc
REAL(KIND=JPRB) ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK('ODB:ODB_CONTROL_WORD_INFO_MATRIX',0,ZHOOK_HANDLE)
rc = ODB_control_word_info_vector(d(:,0),nrows,poolnos,rownums,noffset)
IF (LHOOK) CALL DR_HOOK('ODB:ODB_CONTROL_WORD_INFO_MATRIX',1,ZHOOK_HANDLE)
END FUNCTION ODB_control_word_info_matrix


#undef INT_VERSION
#undef REAL_VERSION

#define INT_VERSION 4
#include "fodbutil.h"
#undef INT_VERSION

#define REAL_VERSION 8
#include "fodbutil.h"
#undef REAL_VERSION

#ifndef USE_CTRIM
#undef ctrim
#undef CTRIM
#endif

#undef trimadjL
#undef trimadjR

END MODULE odbutil
