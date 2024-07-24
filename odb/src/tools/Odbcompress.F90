PROGRAM odbcompress

!
! odbcompress.x -i INPUT_DATABASE_NAME -o OUTPUT_DATABASE_NAME [-a] [-k] [-v] [-m membytes]
!
! Copies all matching tables/column entries found in OUTPUT_DATABASE_NAME
! from INPUT_DATABASE_NAME to OUTPUT_DATABASE_NAME
!
! The idea is in the script level that you 
! - copy <INPUT_DATABASE_NAME>.sch into <OUTPUT_DATABASE_NAME>.ddl
! - copy <INPUT_DATABASE_NAME>.flags into <OUTPUT_DATABASE_NAME>.flags
! - edit <OUTPUT_DATABASE_NAME>.ddl to remove undesired tables/col.entries
! - compile <OUTPUT_DATABASE_NAME>.ddl into lib<OUTPUT_DATABASE_NAME>.a
! - link Odbcompress.o with libs lib<INPUT_DATABASE_NAME>.a &  lib<OUTPUT_DATABASE_NAME>.a
! - and run !!
!
! Does not filter out any rows (i.e. no SQL will be executed)
! Also, keeps the same number of pools as in the originating database,
! unless -a option is used, which enables append-mode i.e.
! as many new pools will be added as there are in the INPUT_DATABASE_NAME
! (= enables incremental creation timeseries database)
!
! -k option enables to convert $ODB_LAT (def=lat@hdr) and $ODB_LON (def=lon@hdr) to degrees
! before writing to output database
!
! -v option provides more verbose output
!
! -m membytes option limits number of bytes per each ODB_get (default=2147483647)
!
! Poolmask (for input database) can be used to define more fine grained input/output of pools
!
! 02-Dec-2005 : Initial version by Sami Saarinen
!

USE PARKIND1  ,ONLY : JPIM,JPRB
USE odb_module
USE mpl_module
USE YOMHOOK, ONLY : LHOOK, DR_HOOK

IMPLICIT NONE

REAL(KIND=JPRB) :: ZHOOK_HANDLE
INTEGER(KIND=JPIM) :: myproc, nproc, jtbl, errflg, ntbl, nra, jp, jcol, ntbl_in
INTEGER(KIND=JPIM) :: j, numargs, rc, nrows, old_npools_out, jp_out
INTEGER(KIND=JPIM) :: h_in , ncols_in , npools_in , npools_more, poolmask_set
INTEGER(KIND=JPIM) :: h_out, ncols_out, npools_out, jj, iloop, ilatlon_rad
INTEGER(KIND=JPIM) :: istart, ilimit, igot, membytes, ncols_max, itotal, itmp
REAL(KIND=JPRB), allocatable, TARGET :: x(:,:)
REAL(KIND=JPRB), POINTER :: z(:,:)
character(len=8)   :: open_mode
character(len=256) :: a_out, arg, input_db, output_db
character(len=256), allocatable :: in_names(:), out_names(:), table(:), table_in(:)
character(len=256) :: ODB_LAT, ODB_LON, ODB_LATLON_RAD
logical :: LLappend, LLkonvert, LLidentical, LLverbose
logical, allocatable :: LLtaken(:), LL_poolmask(:), LLcommon(:)
INTEGER(KIND=JPIM), allocatable :: take_this(:), pool_list(:)
REAL(KIND=JPRB) :: RAD2DEG, xx
REAL(KIND=JPRB) :: RPI, RADIANS, RDEGREE
RAD2DEG(xx) = RDEGREE * xx

IF (LHOOK) CALL DR_HOOK('ODBCOMPRESS',0,ZHOOK_HANDLE)

!-- Consistent with odb/cma2odb/sunumc1.F90
RPI    =2.0_JPRB*ASIN(1.0_JPRB)
RADIANS=RPI/180._JPRB
RDEGREE=180._JPRB/RPI

rc = ODB_init(myproc=myproc,nproc=nproc)

! -i input_db
! -o output_db
! -a            output database will be opened in append_mode
! -k            apply rad2deg() on $ODB_LAT & $ODB_LON and set $ODB_LATLON_RAD values to zero
! -v            be more verbose

errflg = 0
numargs = MPL_iargc()
call MPL_getarg(0,a_out)

input_db = ' '
output_db = ' '
LLappend = .FALSE.
LLkonvert = .FALSE.
LLverbose = .FALSE.
membytes = 2147483647

j = 0
do while (j < numargs)
  j = j + 1
  call MPL_getarg(j,arg)
  if (arg == '-a') then
    LLappend = .TRUE.
  else if (arg == '-k') then
    LLkonvert = .TRUE.
  else if (arg == '-v') then
    LLverbose = .TRUE.
  else if (arg == '-i' .and. j < numargs) then
    j = j + 1
    call MPL_getarg(j,arg)
    input_db = arg
  else if (arg == '-o' .and. j < numargs) then
    j = j + 1
    call MPL_getarg(j,arg)
    output_db = arg
  else if (arg == '-m' .and. j < numargs) then
    j = j + 1
    call MPL_getarg(j,arg)
    read(arg,'(i20)') membytes
    membytes = max(1048576,membytes)
  else
    write(0,*)'***Error: Unrecognized option "'//trim(arg)//'"'
    errflg = errflg + 1
  endif
enddo

if (input_db == ' ')  then
  write(0,*)'***Error: No input database (-i option) given'
  errflg = errflg + 1
endif

if (output_db == ' ') then
  write(0,*)'***Error: No output database (-o option) given'
  errflg = errflg + 1
endif

if (errflg > 0) then ! Abort
  write(0,*)'***Error(s) in argument list : rc=',errflg
  CALL ODB_abort(trim(a_out),'Error(s) in argument list', errflg)
endif

1000 format(1x,a)
1001 format(1x,a,i5,a,i5)
1002 format(1x,a,3i13)
1003 format(1x,a,i5,a,4i12)
1004 format(1x,a,i12,a)

if (LLkonvert) then
  CALL codb_getenv('ODB_LAT',ODB_LAT)
  if (ODB_LAT == ' ') ODB_LAT = 'lat@hdr'
  CALL codb_getenv('ODB_LON',ODB_LON)
  if (ODB_LON == ' ') ODB_LON = 'lon@hdr'
  write(0,1000)'odbcompress: Converting ('//trim(ODB_LAT)//','//trim(ODB_LON)//') to degrees'
  CALL codb_getenv('ODB_LATLON_RAD',ODB_LATLON_RAD)
  if (ODB_LATLON_RAD == ' ') ODB_LATLON_RAD = 'latlon_rad@desc'
  write(0,1000)'odbcompress: Setting '//trim(ODB_LATLON_RAD)//' entries to zero'
else
  CALL codb_getenv('ODB_LATLON_RAD',ODB_LATLON_RAD)
  if (ODB_LATLON_RAD == ' ') ODB_LATLON_RAD = 'latlon_rad@desc'
  write(0,1000)'odbcompress: Setting '//trim(ODB_LATLON_RAD)//' entries to one'
endif

h_in = ODB_open(input_db, 'READONLY', npools_in)

!-- Input pool selection
allocate(pool_list(npools_in))
pool_list(:) = 0
CALL cODB_get_poolmask(h_in, npools_in, pool_list, poolmask_set, rc)

npools_more = 0
allocate(LL_poolmask(npools_in))
if (rc == 0 .or. poolmask_set == 0) then
!-- by default all pools
  LL_poolmask(:) = .TRUE. ! i.e. do process ALL pools
  npools_more = npools_in
else
!-- be more selective
  LL_poolmask(:) = .FALSE. ! i.e. do NOT process any pools by default
  do jp=1,npools_in
    if (pool_list(jp) == 1) then
      LL_poolmask(jp) = .TRUE.
      npools_more = npools_more + 1
    endif
  enddo
endif
deallocate(pool_list)

npools_out = npools_more
open_mode = 'NEW'
if (LLappend) open_mode = 'APPEND'

old_npools_out = 0
h_out = ODB_open(output_db, open_mode, npools_out, old_npools=old_npools_out)

if (old_npools_out > 0) then
  npools_out = ODB_addpools(h_out, npools_more)
endif

ntbl = ODB_getnames(h_out, '*', 'table')
allocate(table(ntbl))
ntbl = ODB_getnames(h_out, '*', 'table', table)

ntbl_in = ODB_getnames(h_in, '*', 'table')
allocate(table_in(ntbl_in))
ntbl_in = ODB_getnames(h_in, '*', 'table', table_in)

allocate(LLcommon(ntbl_in))
LLcommon(:) = .FALSE.
do j=1,ntbl
  do jtbl=1,ntbl_in
    if (LLcommon(jtbl)) cycle
    if (table_in(jtbl) == table(j)) then
      LLcommon(jtbl) = .TRUE.
      exit
    endif
  enddo
enddo

deallocate(table)
nullify(z)

if (LLverbose) write(0,1004)'odbcompress: Amount of memory given to ODB_get() = ',membytes,' bytes'

jp_out = old_npools_out
do jp=1,npools_in ! Loop over input pools
  if (.not.LL_poolmask(jp)) cycle
  jp_out = jp_out + 1
  if (LLverbose) write(0,*)
  write(0,1001)'odbcompress: input pool#',jp,' goes to output pool#',jp_out

  TABLE_LOOP: do jtbl=1,ntbl_in 
    ! Note: We loop over the table-names that are common for INPUT *and* OUTPUT databases.
    !    Otherwise (if only OUTPUT/INPUT-table names were used) we could get error messages, like:
    !       get_forfunc: Unregistered (handle,dbname,poolno,it,dataname)=(1,'DBNAME',1,0,'@poolmask')

    if (.not.LLcommon(jtbl)) cycle TABLE_LOOP

    rc = ODB_select(h_in, table_in(jtbl), nrows, ncols_in, nra=nra, poolno=jp)

    if (LLverbose) write(0,1002) 'odbcompress: Input table "'//trim(table_in(jtbl))//'" : nrows,ncols_in,nra=',&
         &                                                                                 nrows,ncols_in,nra

    if (nrows <= 0) cycle TABLE_LOOP

    allocate(in_names(ncols_in))
    rc = ODB_getnames(h_in, table_in(jtbl), 'name', in_names)

    ncols_out = ODB_getnames(h_out, table_in(jtbl), 'name')
    allocate(out_names(ncols_out))
    rc = ODB_getnames(h_out, table_in(jtbl), 'name', out_names)

    LLidentical = (ncols_in == ncols_out)
    if (LLidentical) LLidentical = ALL(in_names(:) == out_names(:))

    if (LLverbose .and. LLidentical) write(0,*) 'odbcompress: input & output tables are identical'

    if (LLverbose) write(0,1002) 'odbcompress: Output table "'//trim(table_in(jtbl))//'" : nrows,ncols_out,nra=',&
         &                                                                                  nrows,ncols_out,nra

    allocate(take_this(ncols_out))
    take_this(:) = 0

    allocate(LLtaken(ncols_in))
    LLtaken(:) = .FALSE.

    OUTER: do jcol=1,ncols_out
      INNER: do j=1,ncols_in
        if (LLtaken(j)) cycle INNER
        if (out_names(jcol) == in_names(j)) then
          take_this(jcol) = j
          LLtaken(j) = .TRUE.
          exit INNER
        endif
      enddo INNER
    enddo OUTER

    if (LLkonvert) then
      do jcol=1,ncols_out
        if (take_this(jcol) > 0) then
          if (out_names(jcol) == ODB_LAT .or. out_names(jcol) == ODB_LON) then
            ! reversing the sign triggers rad2deg() conversion
            take_this(jcol) = -take_this(jcol)
          endif
        endif
      enddo
    endif
    
    ! In the next we assume that ODB_LATLON_RAD never sits in the same table as ODB_LAT or ODB_LON
    ilatlon_rad = -1
    if (ALL(take_this(:) >= 0)) then
       do jcol=1,ncols_out
          if (take_this(jcol) > 0) then
             if (out_names(jcol) == ODB_LATLON_RAD) then
                ! reversing the sign enforces values at latlon_rad@desc to be 0
                take_this(jcol) = -take_this(jcol)
                if (LLkonvert) then
                   ilatlon_rad = 0
                else
                   ilatlon_rad = 1
                endif
                exit
             endif
          endif
       enddo
    endif
  
    if (ilatlon_rad /= -1) then
       rc = ODB_setval(h_out,'$latlon_rad',ilatlon_rad)
    endif

    deallocate(in_names)
    deallocate(out_names)

    ncols_max = max(ncols_in, ncols_out) + 1
!   ilimit * ncols_max * 8 <= membytes  ==> ilimit = max(1,membytes / (ncols_max * 8))
    ilimit = max(1,membytes / (ncols_max * 8))
    ilimit = min(ilimit, nrows)
    nra = ODB_lda(ilimit) ! nra re-defined

    if (LLverbose) write(0,*) 'odbcompress: Allocating space for input-array x'
    allocate(x(nra,0:ncols_in))

    if (LLidentical) then
      if (LLverbose) write(0,*) 'odbcompress: Input-array x shares space with output-array z'
      z => x
    else
      if (LLverbose) write(0,*) 'odbcompress: Allocating space for output-array z'
      allocate(z(nra,0:ncols_out))
    endif

    istart = 1
    itotal = 0
    iloop = 0
    GET_LOOP: do while (ilimit > 0)
       igot = 0
       iloop = iloop + 1
       if (LLverbose) write(0,1003) 'odbcompress: GET_LOOP#',iloop,'> istart,ilimit,itotal,nrows=',&
            &                                                          istart,ilimit,itotal,nrows

       itmp = ilimit
       igot = ODB_get(h_in, table_in(jtbl), x, itmp, ncols_in, &
            &         colget=LLtaken, colfree=LLtaken, poolno=jp, &
            &         start=istart, limit=ilimit)

       itotal = itotal + igot
       ilimit = min(ilimit, nrows - itotal)

       if (LLverbose) write(0,1003) 'odbcompress: GET_LOOP#',iloop,'<   igot,ilimit,itotal,nrows=',&
            &                                                            igot,ilimit,itotal,nrows

       if (igot == 0) exit GET_LOOP

       do jcol=1,ncols_out
          if (take_this(jcol) > 0) then
             j = take_this(jcol)
             if (.not.LLidentical) z(1:igot,jcol) = x(1:igot,j)
          else if (take_this(jcol) < 0) then ! i.e. apply "konvert" for this column !!
             j = -take_this(jcol)
             if (ilatlon_rad /= -1) then
                z(1:igot,jcol) = 0 ! Values (usually) at 'latlon_rad@desc'
             else
                do jj=1,igot
                   z(jj,jcol) = rad2deg(x(jj,j)) ! Usually 'lat@hdr' and/or 'lon@hdr'
                enddo
             endif
          else
             z(1:igot,jcol) = 0 ! usual default; could be NMDI or RMDI ! need to know the type, anyway
          endif
       enddo

       itmp = igot
       rc = ODB_put(h_out, table_in(jtbl), z, itmp, ncols_out, poolno=jp_out)

       istart = istart + igot
    enddo GET_LOOP ! do while (ilimit > 0)

    rc = ODB_swapout(h_in, table_in(jtbl), poolno=jp)

    if (.not.LLidentical) then
      deallocate(x)
      if (LLverbose) write(0,*) 'odbcompress: Input-array x de-allocated'
      deallocate(z)
      if (LLverbose) write(0,*) 'odbcompress: Output-array z de-allocated'
    else
      deallocate(x)
      if (LLverbose) write(0,*) 'odbcompress: Input-array x de-allocated'
    endif
    nullify(z)
    
    deallocate(LLtaken)
    deallocate(take_this)

    if (ODB_io_method(h_out) /= 4) then
      rc = ODB_swapout(h_out, table_in(jtbl), save = .TRUE., poolno=jp_out)
    else
      rc = ODB_pack(h_out, table_in(jtbl), poolno=jp_out)
    endif

    if (LLverbose) write(0,1002) 'odbcompress: table "'//trim(table_in(jtbl))//'" done.'
  enddo TABLE_LOOP ! do jtbl=1,ntbl_in

  rc = ODB_release(h_in, poolno=jp)
enddo

deallocate(table_in)
deallocate(LLcommon)
deallocate(LL_poolmask)

write(0,1000)'odbcompress: closing databases ...'

rc = ODB_close(h_out, save = .TRUE.)
rc = ODB_close(h_in)

write(0,1000)'odbcompress: all processing done'

rc = ODB_end()

IF (LHOOK) CALL DR_HOOK('ODBCOMPRESS',1,ZHOOK_HANDLE)
END PROGRAM odbcompress
