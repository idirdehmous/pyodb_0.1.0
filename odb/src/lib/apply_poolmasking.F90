SUBROUTINE apply_poolmasking(khandle, kversion, &
 & cdlabel, kvlabel, &
 & ktslot, kobstype, kcodetype, ksensor, &
 & ksubtype, kbufrtype, ksatinst) 

!-- Please make sure that this routine is called under OpenMP CRITICAL-section

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK

USE odb_module

#ifdef NAG
use f90_unix_io, only: flush
#endif

implicit none

INTEGER(KIND=JPIM), intent(in) :: khandle, kversion, kvlabel
character(len=*), intent(in) :: cdlabel
INTEGER(KIND=JPIM), intent(in) :: ktslot, kobstype, kcodetype, ksensor
INTEGER(KIND=JPIM), intent(in) :: ksubtype, kbufrtype, ksatinst

INTEGER(KIND=JPIM), parameter :: OFF = 0
INTEGER(KIND=JPIM) :: oldvalue, rc, poolno, nrows, ncols, nra, onoff, npools
INTEGER(KIND=JPIM), allocatable :: pool_list(:,:)
REAL(KIND=JPRB), allocatable :: zpool_list(:,:)
INTEGER(KIND=JPIM) :: j, j1, j2
character(len=9), save :: cvar(7) = (/'$obstype ',  &! 1
 & '$codetype',  &! 2
 & '$sensor  ',  &! 3
 & '$tslot   ',  &! 4
 & '$subtype ',  &! 5
 & '$bufrtype',  &! 6
 & '$satinst '/)  ! 7 
REAL(KIND=JPRB) :: z(size(cvar))
INTEGER(KIND=JPIM), parameter :: min_version = 1
INTEGER(KIND=JPIM), parameter :: max_version = 3
logical, save :: view_added(min_version:max_version)
logical, save :: first_time = .TRUE.
character(len=20) ::  viewname

! Saved poolmask "image" (pimg) ; save NPMCACHE distinct calls in a circular buffer
TYPE pimg_t 
!!SEQUENCE -- commented out to eliminate a potential misalignment (SGI/Altix ifort/05-Oct-2005/SS)
logical   :: in_use
INTEGER(KIND=JPIM) :: handle, version
INTEGER(KIND=JPIM) :: tslot, obstype, codetype, sensor
INTEGER(KIND=JPIM) :: subtype, bufrtype, satinst
INTEGER(KIND=JPIM) :: nrows, ncols
INTEGER(KIND=JPIM) :: onoff
INTEGER(KIND=JPIM), POINTER :: pool_list(:)
character(len=80) ::  clabel
END TYPE pimg_t
INTEGER(KIND=JPIM), parameter :: NPMCACHE_MIN = 10
INTEGER(KIND=JPIM), parameter :: NPMCACHE_DEFAULT = 100
INTEGER(KIND=JPIM), save      :: NPMCACHE = NPMCACHE_DEFAULT
INTEGER(KIND=JPIM), save      :: inext   = 1
INTEGER(KIND=JPIM), save      :: maxever = 0
TYPE(pimg_t), allocatable, save :: pimg(:)
logical, allocatable, save :: poolmask_table_exist(:) ! maxhandle from odbshared.F90
logical, allocatable, save :: existence_tested(:) ! maxhandle from odbshared.F90
INTEGER(KIND=JPIM) :: it, inumt
INTEGER(KIND=JPIM), external :: get_max_threads, get_thread_id
REAL(KIND=JPRB) :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('APPLY_POOLMASKING',0,ZHOOK_HANDLE)
if (khandle < 0 .OR. khandle > maxhandle .AND. LHOOK) CALL DR_HOOK('APPLY_POOLMASKING',1,ZHOOK_HANDLE)
if (khandle < 0 .OR. khandle > maxhandle) return ! Security

it = get_thread_id()

if (first_time) then
  CALL util_igetenv('ODB_POOLMASK_CACHE', NPMCACHE_DEFAULT, NPMCACHE)
  if (NPMCACHE < NPMCACHE_MIN) NPMCACHE = NPMCACHE_MIN
  allocate(pimg(NPMCACHE))
  do j=1,NPMCACHE
    pimg(j)%in_use   = .FALSE.
    pimg(j)%handle   = -1
    pimg(j)%version  =  0
    pimg(j)%tslot    = -1
    pimg(j)%obstype  = -1
    pimg(j)%codetype = -1
    pimg(j)%sensor   = -1
    pimg(j)%subtype  = -1
    pimg(j)%bufrtype = -1
    pimg(j)%satinst  = -1
    pimg(j)%nrows    =  0
    pimg(j)%ncols    =  0
    pimg(j)%onoff    = -1
    nullify(pimg(j)%pool_list)
    pimg(j)%clabel    = '<undefined>'
  enddo
  allocate(poolmask_table_exist(maxhandle))
  poolmask_table_exist(:) = .FALSE.
  allocate(existence_tested(maxhandle))
  existence_tested(:) = .FALSE.
  view_added(:) = .FALSE.
  first_time = .FALSE.
endif

if (kversion ==  0) then
!-- Release memories populated by khandle
  do j=1,maxever
    if (pimg(j)%in_use .AND. pimg(j)%handle == khandle) then
      pimg(j)%in_use = .FALSE.
      pimg(j)%handle = -1
      if (associated(pimg(j)%pool_list)) then
        deallocate(pimg(j)%pool_list)
        nullify(pimg(j)%pool_list)
      endif
    endif
  enddo

  poolmask_table_exist(khandle) = .FALSE.
  existence_tested(khandle) = .FALSE.

  if (it == 1) then ! Only master thread does this
    inumt = get_max_threads()
    do j=1,inumt
      CALL cODB_reset_poolmask(khandle, j)
    enddo
  endif

  IF (LHOOK) CALL DR_HOOK('APPLY_POOLMASKING',1,ZHOOK_HANDLE)
  return
endif

if (kversion < 0) then
!-- Reset whatever was saved from the previous call
  CALL cODB_reset_poolmask(khandle, it)
  IF (LHOOK) CALL DR_HOOK('APPLY_POOLMASKING',1,ZHOOK_HANDLE)
  return
endif

if (kversion < min_version .or. kversion > max_version .AND. LHOOK) CALL DR_HOOK('APPLY_POOLMASKING',1,ZHOOK_HANDLE)
if (kversion < min_version .or. kversion > max_version) return

!-- Check if we can re-use anything from the most recent call(s) ?

do j=1,maxever
  if (pimg(j)%in_use .AND. &
     & pimg(j)%handle   == khandle   .AND. &
     & pimg(j)%version  == kversion  .AND. &
     & pimg(j)%tslot    == ktslot    .AND. &
     & pimg(j)%obstype  == kobstype  .AND. &
     & pimg(j)%codetype == kcodetype .AND. &
     & pimg(j)%sensor   == ksensor   .AND. &
     & pimg(j)%subtype  == ksubtype  .AND. &
     & pimg(j)%bufrtype == kbufrtype .AND. &
     & pimg(j)%satinst  == ksatinst  .AND. &
     & pimg(j)%nrows > 0 .AND. pimg(j)%ncols > 0 .AND. &
     & associated(pimg(j)%pool_list)) then 
!!    pimg(j)%clabel = cdlabel
    write(pimg(j)%clabel,'(a,"-",i12)') trim(cdlabel), kvlabel
    CALL cODB_set_poolmask(khandle, pimg(j)%nrows, pimg(j)%pool_list(1), pimg(j)%onoff)
    CALL pm_debug_out()
    IF (LHOOK) CALL DR_HOOK('APPLY_POOLMASKING',1,ZHOOK_HANDLE)
    return
  endif
enddo

z(:) = -1

if (kversion == 1) then      ! (obstype,codetype,sensor) present

  j1 = 1
  j2 = 3
  z(1) = kobstype
  if (abs(kobstype ) == abs(ODB_NMDI) .or. kobstype  == -1) j1 = 2
  z(2) = kcodetype
  if (abs(kcodetype) == abs(ODB_NMDI) .or. kcodetype == -1) z(2) = -1
  z(3) = ksensor
  if (abs(ksensor  ) == abs(ODB_NMDI) .or. ksensor   == -1) j2 = 2

ELSEIF (kversion == 2) then ! (obstype,codetype,sensor,tslot) present

  j1 = 1
  j2 = 4
  z(1) = kobstype
  if (abs(kobstype ) == abs(ODB_NMDI) .or. kobstype  == -1) j1 = 2
  z(2) = kcodetype
  if (abs(kcodetype) == abs(ODB_NMDI) .or. kcodetype == -1) z(2) = -1
  z(3) = ksensor
  if (abs(ksensor  ) == abs(ODB_NMDI) .or. ksensor   == -1) z(3) = -1
  z(4) = ktslot
  if (abs(ktslot   ) == abs(ODB_NMDI) .or. ktslot    == -1) j2 = 3

ELSEIF (kversion == 3) then ! (subtype,bufrtype,satinst) present

  j1 = 5
  j2 = 7
  z(5) = ksubtype
  if (abs(ksubtype ) == abs(ODB_NMDI) .or. ksubtype  == -1) j1 = 6
  z(6) = kbufrtype
  if (abs(kbufrtype) == abs(ODB_NMDI) .or. kbufrtype == -1) z(2) = -1
  z(7) = ksatinst
  if (abs(ksatinst)  == abs(ODB_NMDI) .or. ksatinst  == -1) j2 = 6

endif

!-- Poolmask is available on EVERY pool; so apply on first mypool i.e. ODBMP_myproc !
poolno = ODBMP_myproc

!-- Make sure that "poolno" is masked-in i.e. disable any existing poolmasks for the moment
CALL cODB_toggle_poolmask(khandle, OFF, oldvalue)

!-- Check that @poolmask-table has been created and has > 0 entries
if (.NOT.existence_tested(khandle)) then
  CALL cODB_poolmasking_status(khandle, rc)
  if (rc == 1) then
    rc = ODB_getsize(khandle, '@poolmask', nrows, ncols, poolno=poolno)
    poolmask_table_exist(khandle) = (nrows > 0)
  else
    poolmask_table_exist(khandle) = .FALSE.
  endif
  existence_tested(khandle) = .TRUE.
endif

if (.NOT.poolmask_table_exist(khandle)) then
  CALL cODB_toggle_poolmask(khandle, oldvalue, rc)
  IF (LHOOK) CALL DR_HOOK('APPLY_POOLMASKING',1,ZHOOK_HANDLE)
  return
endif

!-- Construct view name 
CALL append_num(viewname, 'poolmask_', kversion)

!-- Add view name to the list of known views
if (.not.view_added(kversion)) then
  rc = ODB_addview(khandle, viewname)
  view_added(kversion) = .TRUE.
endif

!-- Select data : get poolmasks that satisfy SQL to be executed
rc = ODB_select(khandle, viewname, nrows, ncols, nra=nra, poolno=poolno, &
 & setvars=cvar(j1:j2), values=z(j1:j2)) 

if (nrows > 0) then
!-- ODB_select() found something ==> found pools must be turned ON
  onoff = 1
  allocate(zpool_list(nra, 0:ncols))
  rc = ODB_get(khandle, viewname, zpool_list, nrows, ncols, poolno=poolno)
  allocate(pool_list(nra, ncols))
  pool_list(1:nrows,1:ncols) = zpool_list(1:nrows,1:ncols)
  deallocate(zpool_list)
  CALL keysort(rc, pool_list(1:nrows,1), nrows)
else
!-- ODB_select() found nothing ==> ALL pools must be turned OFF
  onoff = 0
  npools = db(khandle)%glbNpools
  nrows = npools
  ncols = 1
  allocate(pool_list(nrows,ncols))
  do j=1,nrows
    pool_list(j,1) = j
  enddo
endif

rc = ODB_cancel(khandle, viewname, poolno=poolno)

!-- Enable (possible) old poolmask
CALL cODB_toggle_poolmask(khandle, oldvalue, rc)

!-- Change poolmask (internally per OpenMP thread!) & release space occupied by pool_list
if (allocated(pool_list)) then
  CALL cODB_set_poolmask(khandle, nrows, pool_list(1,1), onoff)
  j = inext
  pimg(j)%in_use   = .TRUE.
  pimg(j)%handle   = khandle
  pimg(j)%version  = kversion
  pimg(j)%tslot    = ktslot
  pimg(j)%obstype  = kobstype
  pimg(j)%codetype = kcodetype
  pimg(j)%sensor   = ksensor
  pimg(j)%subtype  = ksubtype
  pimg(j)%bufrtype = kbufrtype
  pimg(j)%satinst  = ksatinst
  pimg(j)%nrows    = nrows
  pimg(j)%ncols    = ncols
  pimg(j)%onoff    = onoff
  if (associated(pimg(j)%pool_list)) deallocate(pimg(j)%pool_list)
  allocate(pimg(j)%pool_list(nrows))
  pimg(j)%pool_list(1:nrows) = pool_list(1:nrows,1)
!!  pimg(j)%clabel = cdlabel
  write(pimg(j)%clabel,'(a,"-",i12)') trim(cdlabel), kvlabel

  maxever = max(maxever, j)
  inext = mod(inext, NPMCACHE) + 1 ! circularity
  deallocate(pool_list)

  CALL pm_debug_out()
endif
IF (LHOOK) CALL DR_HOOK('APPLY_POOLMASKING',1,ZHOOK_HANDLE)

CONTAINS

subroutine pm_debug_out()
implicit none
logical, save :: first_time_here = .TRUE.
INTEGER(KIND=JPIM), parameter :: ipm_debug_def = 0
INTEGER(KIND=JPIM), save      :: ipm_debug = 0
REAL(KIND=JPRB) :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('PM_DEBUG_OUT',0,ZHOOK_HANDLE)
if (first_time_here) then
  CALL util_igetenv('ODB_POOLMASK_DEBUG', ipm_debug_def, ipm_debug)
  if (ipm_debug == -1 .OR. ipm_debug == ODBMP_myproc) then
    ipm_debug = ODBMP_myproc
  endif
  first_time_here = .FALSE.
endif

if (ipm_debug == ODBMP_myproc) then
  write(0,*) ODBMP_myproc,': it=',it,' Using pool_list#',j,&
   & '; poolmask-onoff =',pimg(j)%onoff,&
   & ' for h,ver,t,ot,cdt,sen,sub,bfr,sat=',&
   & pimg(j)%handle, pimg(j)%version, &
   & pimg(j)%tslot, pimg(j)%obstype, pimg(j)%codetype, pimg(j)%sensor, &
   & pimg(j)%subtype, pimg(j)%bufrtype, pimg(j)%satinst 
  write(0,*) ODBMP_myproc,': pool_list#, list_length, label=',j,pimg(j)%nrows,&
   & ' "'//trim(pimg(j)%clabel)//'"' 
  write(0,'(16i5)') pimg(j)%pool_list(1:pimg(j)%nrows)
  call flush(0)
  CALL cODB_print_poolmask(pimg(j)%handle, it)
endif
IF (LHOOK) CALL DR_HOOK('PM_DEBUG_OUT',1,ZHOOK_HANDLE)
end subroutine pm_debug_out

END SUBROUTINE apply_poolmasking
