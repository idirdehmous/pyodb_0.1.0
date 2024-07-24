MODULE odbnetcdf
USE PARKIND1  ,ONLY : JPIM     ,JPRB

USE odbshared
USE odbmp
USE odbutil
USE odb
USE odbgetput
USE str, only : tolower, sadjustl

!RJ #define trimadjL(x) trim(sadjustl(x))

IMPLICIT NONE
SAVE
PRIVATE

INTERFACE ODB_to_netcdf
MODULE PROCEDURE & 
  & ODB_to_netcdf_dtname
END INTERFACE

INTEGER(KIND=JPIM) :: icdf_char    = -1
INTEGER(KIND=JPIM) :: icdf_byte    = -1
INTEGER(KIND=JPIM) :: icdf_short   = -1 
INTEGER(KIND=JPIM) :: icdf_int     = -1
INTEGER(KIND=JPIM) :: icdf_float   = -1
INTEGER(KIND=JPIM) :: icdf_double  = -1
INTEGER(KIND=JPIM) :: icdf_unknown = -1
REAL(KIND=JPRB), parameter :: too_big4float = 1.7E+38_JPRB
REAL(KIND=JPRB), parameter :: too_tiny = 1E-15_JPRB

PUBLIC :: ODB_to_netcdf
PUBLIC :: to_netcdf
PUBLIC :: init_cdf

CONTAINS

SUBROUTINE init_cdf()
implicit none
logical, save :: LLfirst_time = .TRUE.
if (LLfirst_time) then
  CALL codb_gettype_netcdf('char', icdf_char)
  CALL codb_gettype_netcdf('byte', icdf_byte)
  CALL codb_gettype_netcdf('short', icdf_short)
  CALL codb_gettype_netcdf('int', icdf_int)
  CALL codb_gettype_netcdf('float', icdf_float)
  CALL codb_gettype_netcdf('double', icdf_double)
  CALL codb_gettype_netcdf('unknown', icdf_unknown)
  LLfirst_time = .FALSE.
endif
END SUBROUTINE init_cdf


SUBROUTINE modify_type(pd, mdi, itype, ipack, scale_factor, add_offset, pref_out, LDnopacking)
implicit none
REAL(KIND=JPRB), intent(in) :: pd(:)
REAL(KIND=JPRB), intent(in) :: mdi
INTEGER(KIND=JPIM), intent(inout) :: itype
INTEGER(KIND=JPIM), intent(out) :: ipack
REAL(KIND=JPRB), intent(out) :: scale_factor, add_offset, pref_out
logical, intent(in) :: LDnopacking
INTEGER(KIND=JPIM) j, nrows, count_mdis, count_zeros, count_thesame, count_ints, iref
REAL(KIND=JPRB)    :: zmin, zmax, zref
REAL(KIND=JPRB), parameter :: rmax_byte_dist  =   127_JPRB
REAL(KIND=JPRB), parameter :: rmax_short_dist = 32767_JPRB
ipack = 0
scale_factor = 1
add_offset = 0
pref_out = abs(mdi)
nrows = size(pd)
if (nrows == 0) return
if (LDnopacking) then
  if (itype /= icdf_char) itype = icdf_double ! full precision preserved; and on strings by definition
  return
endif

if (itype /= icdf_char) then
  count_mdis = count(abs(pd(1:nrows)) == abs(mdi))
  count_zeros = count(pd(1:nrows) == 0)

  if (count_zeros + count_mdis == nrows) then
    itype = icdf_byte
  endif

  if (itype == icdf_float) then
    count_ints = 0
    do j=1,nrows
      if (abs(pd(j)) /= abs(mdi)) then
        if (abs(pd(j)) > 2147483647) exit
        iref = int(pd(j))
        zref = iref
        if (zref /= pd(j)) exit
        count_ints = count_ints + 1
      endif
    enddo
    if (count_ints == nrows - count_mdis) itype = icdf_int
  endif

  if (itype == icdf_int) then
    if (count(abs(pd(1:nrows)) < 128 .AND. abs(pd(1:nrows)) /= abs(mdi)) == &
        nrows - count_mdis) then
      itype = icdf_byte
    else if (count(abs(pd(1:nrows)) < 32768 .AND. abs(pd(1:nrows)) /= abs(mdi)) == &
             nrows - count_mdis) then
      itype = icdf_short
    endif
  else if (itype == icdf_float) then
    if (count(abs(pd(1:nrows)) > too_big4float .AND. abs(pd(1:nrows)) /= abs(mdi)) > 0) &
      itype = icdf_double
  endif
endif
zref = pd(1)
count_thesame = 1
do j=2,nrows
  if (pd(j) /= zref) exit
  count_thesame = count_thesame + 1
enddo
count_thesame = -1 ! ncview doesn't appreciate these and no actual gain in filesize
if (count_thesame == nrows) then ! all values the same as pd(1)
  ipack = 1
  pref_out = zref
else if (count_mdis < nrows .AND. &
  (itype == icdf_float .OR. itype == icdf_double .OR. itype == icdf_int)) then
  zmin = minval(pd(1:nrows),MASK=abs(pd(1:nrows)) /= abs(mdi))
  zmax = maxval(pd(1:nrows),MASK=abs(pd(1:nrows)) /= abs(mdi))
  if (zmax == zmin) then
    scale_factor = 1
    add_offset = zmin
    itype = icdf_byte
    ipack = 3
  else if (itype == icdf_int .AND. zmax - zmin < rmax_byte_dist) then
    scale_factor = 1
    add_offset = zmin
    itype = icdf_byte
    ipack = 3
  else if (itype == icdf_int .AND. zmax - zmin < rmax_short_dist) then
    scale_factor = 1
    add_offset = zmin
    itype = icdf_short
    ipack = 2
  else if (itype /= icdf_int) then
    scale_factor = (zmax - zmin)/(rmax_short_dist - 1.0_JPRB)
    add_offset = zmin
    itype = icdf_short
    ipack = 2
  endif
endif
END SUBROUTINE modify_type

SUBROUTINE to_netcdf(title, ncfile, namecfg, sql_query, &
     & d, &
     & itype, &
     & odb_type, odb_name, odb_nickname, &
     & mdi, ipoolno, LDnopacking, LDdebug_mode)
implicit none
character(len=*), intent(in) :: title, ncfile, namecfg, sql_query
character(len=*), intent(in) :: odb_type(:), odb_name(:), odb_nickname(:)
REAL(KIND=JPRB), intent(in) :: d(:,:)
INTEGER(KIND=JPIM), intent(inout) :: itype(:)
REAL(KIND=JPRB), intent(in) :: mdi
INTEGER(KIND=JPIM), intent(in) :: ipoolno
logical, intent(in) :: LDnopacking, LDdebug_mode
INTEGER(KIND=JPIM) i, j, nrows, ncols, iret, ncid, icnt, ios, ii_cnt
INTEGER(KIND=JPIM) icolid(size(itype)), ipack(size(itype)), idx(size(itype))
REAL(KIND=JPRB) :: scale_factor(size(itype)), add_offset(size(itype))
REAL(KIND=JPRB) :: mdi_out(size(itype)), itype_orig(size(itype))
REAL(KIND=JPRB), allocatable :: zbuf(:)
character(len=256), allocatable, save :: refname(:), mapname(:), long_name(:), units(:)
REAL(KIND=JPRB), allocatable, save :: a(:), b(:)  ! for linear conversions : A * x + B
character(len=4096) cline
character(len=4096) CL_namecfg
character(len=4096), save :: CL_namecfg_last = ' '

nrows = size(d, dim=1)
ncols = min(size(d, dim=2), size(itype), size(odb_type), size(odb_name), size(odb_nickname))

icnt = 0

CALL codb_truename(trim(namecfg), CL_namecfg, iret)
if (CL_namecfg == CL_namecfg_last) then
  if (allocated(refname)) icnt = size(refname)
  goto 999
endif

if (LDdebug_mode) &
  write(0,*) ODBMP_myproc,&
             ': Opening name-configuration file "'//trim(CL_namecfg(1:iret))//'" (if available)'
open(1,file=CL_namecfg(1:iret),status='old',err=888)
CL_namecfg_last = CL_namecfg(1:iret)

do
  read(1,'(a)',err=98,end=98) cline
  if (len_trim(cline) == 0) cycle
  if (cline(1:1) == '!' .OR. cline(1:1) == '#') cycle
  icnt = icnt + 1
enddo
 98   continue

if (icnt > 0) then
  rewind(1)

  if (allocated(refname))    deallocate(refname)
  if (allocated(mapname))    deallocate(mapname)
  if (allocated(long_name))  deallocate(long_name)
  if (allocated(units))      deallocate(units)
  if (allocated(a)) deallocate(a)
  if (allocated(b)) deallocate(b)

  allocate(refname(icnt))
  allocate(mapname(0:icnt))    ; mapname(0)    = '-'
  allocate(long_name(0:icnt))  ; long_name(0)  = '-'
  allocate(units(0:icnt))      ; units(0)      = '-'
  allocate(a(0:icnt)) ; a(0) = 1
  allocate(b(0:icnt)) ; b(0) = 0

  i = 0
  do
    read(1,'(a)',err=99,end=99) cline
    if (len_trim(cline) == 0) cycle
    cline = sadjustl(cline)
    if (cline(1:1) == '!' .OR. cline(1:1) == '#') cycle
    i = i + 1
    refname(i) = ' '
    mapname(i) = '-'
    long_name(i) = '-'
    units(i) = '-'
    a(i) = 1
    b(i) = 0
    ios = 0
!!    call tolower(cline)
    read(cline,*,iostat=ios,err=97,end=97) refname(i),mapname(i),long_name(i),units(i),a(i),b(i)
 97 continue
    if (mapname(i) /= '-' .AND. long_name(i) == '-') long_name(i) = mapname(i)
    if (LDdebug_mode .AND. ODBMP_myproc == 1) then
      write(0,*)'>> i,a(i),b(i) & names=',&
             i, a(i), b(i), &
             ',refname="'//trim(refname(i))//'"',&
             ',mapname="'//trim(mapname(i))//'"',&
             ',long_name="'//trim(long_name(i))//'"',&
             ',units="'//trim(units(i))//'"'
    endif
  enddo
 99 continue
endif
close(1)
goto 999
 888  continue
 if (LDdebug_mode) &
    write(0,*) ODBMP_myproc,': Name-configuration file could not be opened'
 999  continue

if (icnt == 0) then
  if (allocated(refname))    deallocate(refname)
  if (allocated(mapname))    deallocate(mapname)
  if (allocated(long_name))  deallocate(long_name)
  if (allocated(units))      deallocate(units)
  if (allocated(a)) deallocate(a)
  if (allocated(b)) deallocate(b)

  allocate(refname(icnt))
  allocate(mapname(0:icnt))    ; mapname(0)    = '-'
  allocate(long_name(0:icnt))  ; long_name(0)  = '-'
  allocate(units(0:icnt))      ; units(0)      = '-'
  allocate(a(0:icnt)) ; a(0) = 1
  allocate(b(0:icnt)) ; b(0) = 0
endif

idx(:) = 0
ii_cnt = 0
LOOP_i: do i=icnt,1,-1
  LOOP_j: do j=1,ncols
    if (idx(j) == 0 .AND. odb_name(j) == refname(i)) then
      idx(j) = i
      ii_cnt = ii_cnt + 1
      exit LOOP_j
    endif
  enddo LOOP_j
  if (ii_cnt >= ncols) exit LOOP_i
enddo LOOP_i

write(0,'(a,i10,1x,i5)') '*** Writing to NetCDF-file "'//trim(ncfile)//'" : nrows, ncols =',nrows,ncols

CALL codb_open_netcdf(ncid, trim(ncfile), 'w', iret)

CALL codb_begindef_netcdf(ncid, trim(title), trim(sql_query), nrows, ncols, mdi, ipoolno, iret)

allocate(zbuf(1:nrows))

itype_orig(:) = itype(:)

do j=1,ncols
  zbuf(1:nrows) = d(1:nrows,j)

  i = idx(j)

  if (a(i) /= 1 .AND. b(i) /= 0) then
    WHERE (abs(zbuf(1:nrows)) /= abs(mdi)) zbuf(1:nrows) = a(i) * zbuf(1:nrows) + b(i)
  else if (a(i) /= 1 .AND. b(i) == 0) then
    WHERE (abs(zbuf(1:nrows)) /= abs(mdi)) zbuf(1:nrows) = a(i) * zbuf(1:nrows)
  else if (a(i) == 1 .AND. b(i) /= 0) then
    WHERE (abs(zbuf(1:nrows)) /= abs(mdi)) zbuf(1:nrows) = zbuf(1:nrows) + b(i)
  endif

  if (itype_orig(j) == icdf_float) then
    WHERE (abs(zbuf(1:nrows)) /= abs(mdi) .AND. abs(zbuf(1:nrows)) < too_tiny) zbuf(1:nrows) = 0
  endif

  CALL modify_type(zbuf(1:nrows), &
                   mdi, itype(j), ipack(j), &
                   scale_factor(j), add_offset(j), mdi_out(j), Ldnopacking)

  CALL codb_putheader_netcdf(ncid, j, &
                   odb_type(j), odb_name(j), odb_nickname(j), &
                   mapname(i), long_name(i), units(i), &
                   mdi_out(j), itype(j), ipack(j), &
                   scale_factor(j), add_offset(j), iret)
  icolid(j) = iret
enddo

CALL codb_enddef_netcdf(ncid, iret)

do j=1,ncols
  zbuf(1:nrows) = d(1:nrows,j)

  i = idx(j)

  if (a(i) /= 1 .AND. b(i) /= 0) then
    WHERE (abs(zbuf(1:nrows)) /= abs(mdi)) zbuf(1:nrows) = a(i) * zbuf(1:nrows) + b(i)
  else if (a(i) /= 1 .AND. b(i) == 0) then
    WHERE (abs(zbuf(1:nrows)) /= abs(mdi)) zbuf(1:nrows) = a(i) * zbuf(1:nrows)
  else if (a(i) == 1 .AND. b(i) /= 0) then
    WHERE (abs(zbuf(1:nrows)) /= abs(mdi)) zbuf(1:nrows) = zbuf(1:nrows) + b(i)
  endif

  if (itype_orig(j) == icdf_float) then
    WHERE (abs(zbuf(1:nrows)) /= abs(mdi) .AND. abs(zbuf(1:nrows)) < too_tiny) zbuf(1:nrows) = 0
  endif

  CALL codb_putdata_netcdf(ncid, icolid(j), ipack(j), zbuf(1), 1, nrows, iret)
enddo

deallocate(zbuf)

CALL codb_close_netcdf(ncid, iret)

END SUBROUTINE to_netcdf


FUNCTION ODB_to_netcdf_dtname(handle, dtname,&
                             &file, poolno,  &
                             &setvars, values, mdi) RESULT(rc)
implicit none

! handle = database handle
INTEGER(KIND=JPIM), intent(in)         :: handle
! dtname = view or table name to be executed and converted into NETCDF format
character(len=*), intent(in)  :: dtname
! file = NETCDF output file
!        if not given, defaults to trim(dtname)//'.nc.%d', where %d is turned into a poolno
!        if poolno not given, then %d indicates my MPL-task id , i.e. one of 1..NPES
character(len=*), intent(in), optional :: file
! poolno = Pool number in concern (default = -1 i.e. all *local* pools)
INTEGER(KIND=JPIM), intent(in), optional :: poolno
! setvars = list of $-variables to be altered when SQL is executed
character(len=*), intent(in), optional :: setvars(:)
! values = changed values for each $-variable in setvars-list
REAL(KIND=JPRB)            , intent(in), optional :: values(:)
! mdi = missing data indicator
!       if not supplied, abs(ODB_NMDI) becomes the default
REAL(KIND=JPRB), intent(in), optional :: mdi
! rc = return code; if  >= 0, then indicates no. of rows processed on this call
INTEGER(KIND=JPIM) :: rc

INTEGER(KIND=JPIM) :: nrows, ncols, nra
INTEGER(KIND=JPIM) :: ipoolno, percent_d, iret, i, j
REAL(KIND=JPRB), allocatable :: d(:,:)
INTEGER(KIND=JPIM), allocatable :: itype(:)
character(len=maxvarlen), allocatable :: names(:), types(:), ftntypes(:)
character(len=4096)  CL_file, CL_title, CL_namecfg
character(len=maxvarlen)  dbname, env
INTEGER(KIND=JPIM) :: idummy, idummy_arr(0), idx
REAL(KIND=JPRB) mdi_out
logical is_table
logical LLnopacking, LLdebug_mode

CALL codb_has_netcdf(rc)
if (rc == 0) then
  if (ODBMP_myproc == 1) write(0,*)'***Warning: Not compiled with -DHAS_NETCDF => no NetCDF support'
  return ! no NetCDF support
endif

is_table = .FALSE.
if (len(dtname) >= 1) is_table = (dtname(1:1) == '@')

CALL codb_getenv('ODB_NETCDF_NOPACKING', env) ! set ODB_NETCDF_NOPACKING=1 to see all in double or string
LLnopacking = (env == '1')

CALL codb_getenv('ODB_NETCDF_DEBUG', env)
LLdebug_mode = (env == '1')

CALL cODB_trace(handle, 1, 'ODB_to_netcdf:'//dtname,idummy_arr, 0)

rc = 0

if (odbHcheck(handle, 'ODB_to_netcdf')) then
  ipoolno = get_poolno(handle, poolno)
  if (ipoolno == -1) then
    percent_d = ODBMP_myproc
  else
    percent_d = ipoolno
  endif

  if (present(file)) then
    CL_file = file
  else
    CL_file = trim(dtname)//'.nc.%d'
  endif

  idx = index(CL_file, '%') 
  if (idx > 0) CALL codb_pc_filter(trim(CL_file), CL_file, percent_d, iret)

  iret = ODB_select(handle, dtname, nrows, ncols, &
                   &poolno=ipoolno, nra=nra,   &
                   &setvars=setvars, values=values)

  if (LLdebug_mode) write(0,*) ODBMP_myproc, ': ODB_to_netcdf > dtname="'//trim(dtname)// &
                               '", poolno,nrows,ncols=',ipoolno,nrows,ncols

  if (nrows > 0) then
    allocate(names(1:ncols))
    iret = ODB_getnames(handle, dtname, 'name', names(1:ncols))

    allocate(d(nra,0:ncols))
    allocate(itype(1:ncols))
    allocate(types(1:ncols))
    allocate(ftntypes(1:ncols))

    iret = ODB_getnames(handle, dtname,'type', types(1:ncols))
    iret = ODB_getnames(handle, dtname,'ftntype', ftntypes(1:ncols))

    CALL init_cdf()

    iret = ODB_get(handle, dtname, d, nrows, ncols, poolno=ipoolno)

    do j=1,ncols
      if ( ftntypes(j)(1:7) == 'REAL(8)' .and.&
         &    types(j)(1:7) == 'string ' ) then
        itype(j) = icdf_char
      else if ( ftntypes(j)(1:7) == 'CHAR(8)') then ! a future extension
        itype(j) = icdf_char
      else if (ftntypes(j)(1:5) == 'REAL(') then
        itype(j) = icdf_float
      else if (ftntypes(j)(1:8) == 'INTEGER(') then
        itype(j) = icdf_int
      else
        itype(j) = icdf_unknown
      endif
    enddo

    if (present(mdi)) then
      mdi_out = mdi
    else
      mdi_out = ODB_getval(handle, '$mdi')
      if (mdi_out == 0) mdi_out = abs(ODB_NMDI) ! i.e. $mdi was most likely not defined
    endif

    iret = ODB_cancel(handle, dtname, poolno=ipoolno)

    dbname = db(handle)%name    
    if (is_table) then
      CL_title = 'Database '//trim(dbname)//', table '//trim(dtname(2:))//', pool#%d'
    else
      CL_title = 'Database '//trim(dbname)//', data query '//trim(dtname(1:))//', pool#%d'
    endif
    CALL codb_pc_filter(trim(CL_title), CL_title, ipoolno, iret)

    CALL codb_getenv('ODB_NAMECFG_'//trim(dbname),CL_namecfg)
    if (CL_namecfg == ' ') CALL codb_getenv('ODB_NAMECFG',CL_namecfg)
    if (CL_namecfg == ' ') CL_namecfg = trim(dbname)//'.namecfg'

    CALL to_netcdf(trim(CL_title), trim(CL_file), trim(CL_namecfg), 'N/A', &
                  & d(1:nrows, 1:ncols), &
                  & itype(1:ncols), &
                  & types(1:ncols), names(1:ncols), names(1:ncols), &
                  & mdi_out, ipoolno, LLnopacking, LLdebug_mode)

    deallocate(d)
    deallocate(itype)
    deallocate(names)
    deallocate(types)
    deallocate(ftntypes)
  endif ! if (nrows > 0) then

  rc = nrows
endif

CALL cODB_trace(handle, 0, 'ODB_to_netcdf:'//dtname,idummy_arr, 0)
END FUNCTION ODB_to_netcdf_dtname

END MODULE odbnetcdf
