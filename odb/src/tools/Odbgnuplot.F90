program odbgnuplot
!
!-- To browse particular ODB-table and/or SQL and pass output to "stdout" for use by gnuplot
!   Based on odbless-program
!
!   Usage: 
!
! ./odbgnuplot.x info dbname view/tablename starting_row numrows row_buffer_size konvert2degrees every debug
!
!  Parameters:
!
!  info   = 1 produce info only about column names and number of columns; Quick, no ODB_select() executed
!         = 0 skip info and do the real thing i.e. execute ODB_select()
!  dbname = ODB database name e.g. ECMA
!  view   = view/table name e.g. myview
!
!  starting_row = row number to start the output (usually = 1 = default)
!  numrows = number of rows to display (all = -1 --> 2147483647 will become the limit; default)
!
!  row_buffer_size = how many rows max to allocate per each ODB_get() (set to -1 to let the program to select = default)
!
!  konvert2degrees = 1 convert radians to degrees for $ODB_LAT (usually "lat@hdr"), $ODB_LON (usually "lon@hdr")
!                    0 do NOT convert radians to degrees for $ODB_LAT, $ODB_LON (default)
!
!  every = 1 to print at most numrows of every pool (controlled via ODB_PERMANENT_POOLMASK_<dbname>; see below)
!          0 to scan all pools until total numrows has been reached (default)
!
!  debug = 1 debug on
!          0 debug off (default)
!
!
! For example: 
! 1) info records only:        ./odbgnuplot.x 1 ECMA myview
! 2) plot first 100 rows only: ./odbgnuplot.x 0 ECMA myview 1 100 100
! 3) plot all data available and convert possible lat/lon @hdr to degrees; max bufsize 100,000 rows:
!                              ./odbgnuplot.x 0 ECMA myview 1  -1 100000 1
! 4) plot a little bit (50 rows) from every pool speficied via ODB_PERMANENT_POOLMASK_ECMA:
!                              ./odbgnuplot.x 0 ECMA myview 1  50     50 0 1
!
!  Pools are controlled via ODB_PERMANENT_POOLMASK_<dbname> or ODB_PERMANENT_POOLMASK
!

USE PARKIND1  ,ONLY : JPIM,JPRB
use odb_module
use mpl_module
implicit none

CHARACTER(LEN=*), PARAMETER :: progname = 'odbgnuplot'

INTEGER(KIND=JPIM) :: h, rc, npools, nproc, myproc, j, jp, rowbuf, konvert
INTEGER(KIND=JPIM) :: nrows, ncols, istart, numrows, n, npolled, igot
INTEGER(KIND=JPIM) :: numargs, ntot, debug, every, info
INTEGER(KIND=JPIM) :: saved(2)
INTEGER(KIND=JPIM) :: jp1, jp2, jinc, llc_idx(2)
INTEGER(KIND=JPIM) :: nchunk, j1, j2, nleft, nmypools, jj
character(len=256) arg
character(len=64)  dbname, dtname, latlon_names(size(llc_idx)), env
logical LLopen, LLclose, LLselect, LLcancel, LLinside, LLkonvert, LLspecial
logical LLprthdr, LLprttimingstat, LLalloc, LLdebug, LLevery, LLinfo
INTEGER(KIND=JPIM), allocatable :: mypools(:)
character(len=maxvarlen), allocatable :: colname(:)

rc = ODB_init(myproc=myproc, nproc=nproc)

info    =    0
istart  =    1
numrows =   20
rowbuf  = 1000
konvert =    0
debug   =    0
every   =    0

numargs = MPL_iargc()

if (numargs >= 3) then
  if (numargs >= 1) then
    call MPL_getarg(1, arg)
    read(arg,'(i12)',err=11,end=11) info
  endif
  info = max(0,min(1,info))
  
11 continue

  call MPL_getarg(2, dbname)
  call MPL_getarg(3, dtname)
  
  if (numargs >= 4) then
    call MPL_getarg(4, arg)
    read(arg,'(i12)',err=44,end=44) istart
  endif
  istart = max(1,istart)
44 continue

  if (numargs >= 5) then
    call MPL_getarg(5, arg)
    read(arg,'(i12)',err=55,end=55) numrows
  endif
  if (numrows <= 0) numrows = 2147483647
55 continue

  if (numargs >= 6) then
    call MPL_getarg(6, arg)
    read(arg,'(i12)',err=66,end=66) rowbuf
  endif
  if (rowbuf <= 0) rowbuf = -1
66 continue

  if (numargs >= 7) then
    call MPL_getarg(7, arg)
    read(arg,'(i12)',err=77,end=77) konvert
  endif
77 continue

  if (numargs >= 8) then
    call MPL_getarg(8, arg)
    read(arg,'(i12)',err=88,end=88) every
  endif
88 continue

  if (numargs >= 9) then
    call MPL_getarg(9, arg)
    read(arg,'(i12)',err=99,end=99) debug
  endif
99 continue

else
  call MPL_getarg(0, arg)
  CALL ODB_abort(progname,&
  & 'Usage: '//trim(arg)//&
  & ' info dbname view/tablename starting_row numrows'//&
  & ' row_buffer_size konvert2degrees summary every debug')
endif

LLinfo = (info == 1)

if (LLinfo) then
! Info only: to prevent reading data from any pools or tables
  call codb_putenv('ODB_CONSIDER_TABLES=//')
  call codb_putenv('ODB_PERMANENT_POOLMASK_'//trim(dbname)//'=0')
endif

h = ODB_open(dbname, 'READONLY', npools)

if (LLinfo) then
! Info only: print ncols followed by column names; then exit the application; costs nothing!
  ncols = ODB_getnames(h, dtname, 'colname')
  allocate(colname(1:ncols))
  ncols = ODB_getnames(h, dtname, 'colname', outnames=colname)
  write(6,*) ncols
  do j=1,ncols
    write(6,'(1x,a)') trim(colname(j))
  enddo
  deallocate(colname)
  rc = ODB_close(h)
  rc = ODB_end()
  call exit(0) ! should never end up here
endif

!-- Fully tolerate errors in time-records i.e. fix records when calling twindow/tdiff funcs
call codb_allow_time_error(235959)

call codb_putenv('ODB_REPORTER=stdout')
call codb_putenv('ODB_PRINT_COLON=#')

LLopen = .TRUE.
LLclose = .FALSE.
LLselect = .FALSE.
LLcancel = .FALSE.
LLkonvert = (konvert == 1)
if (.not.LLkonvert) konvert = 0
LLdebug = (debug == 1)
if (.not.LLdebug) debug = 0
LLevery = (every == 1)
if (.not.LLevery) every = 0

if (ODB_has_orderby(h, dtname) .or. &
!! not implemented yet  & ODB_has_uniqueby(h, dtname) .or. & ! Don't know yet if this is ok
  & ODB_has_select_distinct(h, dtname)) then
!-- need all pools in one go, since
!   either contains ORDERBY
!   or     SELECT DISTINCT (or UNIQUEBY)
!   or     combination of ORDERBY with SELECT DISTINCT (or UNIQUEBY)
  nmypools = 1
  allocate(mypools(nmypools))
  mypools(1) = -1
  jp1 = 1
  jp2 = 1
  jinc = 1
!  jp1 = -1
!  jp2 = -1
!  jinc = 1
  istart = 1
  numrows = 2147483647
  rowbuf = -1
  LLspecial = .TRUE.
else
  allocate(mypools(npools))
  nmypools = ODB_poolinfo(h, mypools, with_poolmask=.TRUE.)
  jp1 = 1
  jp2 = nmypools
  jinc = 1
!  jp1 = myproc
!  jp2 = npools
!  jinc = nproc
  LLspecial = .FALSE.
endif

if (LLdebug) then
 1001 format(a,/,(6i12))
  write(0,1001) '$ npools, nmypools, size(mypools), jp1, jp2, jinc, mypools(:)=',&
      &            npools, nmypools, size(mypools), jp1, jp2, jinc, mypools(:)
endif

istart = istart - 1 ! "C-indexing" enables more convenient offsetting
npolled = 0
ntot = 0
LLprthdr = .TRUE.
LLprttimingstat = .FALSE.
LLalloc = .FALSE.

if (LLevery) then
  saved(1) = istart
  saved(2) = numrows
else
  saved(:) = 0
endif

if (LLdebug) then
  write(0,*) '## every, saved(:)=',every, saved(:)
endif

OUTER_LOOP: do jj=jp1,jp2,jinc
  jp = mypools(jj)
  rc = ODB_select(h, dtname, nrows, ncols, poolno=jp)

  if (LLdebug) then
     write(0,1000) '>> jp, jp1, jp2, jinc, nrows, ncols=', &
          &            jp, jp1, jp2, jinc, nrows, ncols
  endif

  if (nrows > 0) then

    if (LLevery) then
    !-- This "reset" enables us to fetch at most "numrows" per each pool (in poolmask)
      istart = npolled + saved(1)
      numrows = saved(2)
      if (LLdebug) then
        write(0,*) '#> npolled, istart, numrows=',npolled, istart, numrows
      endif
    endif    

    LLinside = (istart >= npolled .and. istart < npolled + nrows)

    if (LLinside) then
      nleft = min(nrows, numrows)
      if (rowbuf == -1) then
        n = min(nrows, numrows)
      else if (nrows > rowbuf) then
        n = rowbuf
      else
        n = nrows
      endif
      n = min(n,nleft)

      if (.not.LLalloc) then
        LLalloc = .TRUE.

        if (LLkonvert) then
          call codb_getenv('ODB_LAT',env)
          if (env == ' ') env = 'lat@hdr'
          latlon_names(1) = env
          call codb_getenv('ODB_LON',env)
          if (env == ' ') env = 'lon@hdr'
          latlon_names(2) = env
          ! llc stands for latitude,longitude,color !
          rc = ODB_varindex(h, dtname, latlon_names, llc_idx)
          ! lat and/or lon will be converted to degrees, if corresponding llc_idx(:) was positive
          if (llc_idx(1) > 0) llc_idx(1) = -llc_idx(1) ! due to the stupid logic in ODB_print() ;-(
          if (llc_idx(2) > 0) llc_idx(2) = -llc_idx(2) ! due to the stupid logic in ODB_print() ;-(
        else
          llc_idx(:) = 0 ! (lat,lon) will not be converted in ODB_print
        endif
      endif

      j1 = (istart - npolled) + 1 ! (istart - npolled) relative to the beginning of this pool
      j2 = (istart - npolled) + nleft

      if (LLdebug) then
1000     format(1x,a,/,10i12)
         write(0,1000) '# nleft, n, j1, j2, istart, npolled, nrows, numrows, rowbuf, ntot=', &
              &           nleft, n, j1, j2, istart, npolled, nrows, numrows, rowbuf, ntot
      endif

      INNER_LOOP: do j=j1,j2,n
        nchunk = min(n, nleft)
        if (LLdebug) then
           write(0,1000) '> ntot,nleft,j,n,nchunk,numrows,nrows,istart,npolled=',&
                &           ntot,nleft,j,n,nchunk,numrows,nrows,istart,npolled
        endif

        rc = ODB_print(h, dtname, file='stdout', poolno=jp, &
           & inform_progress=.FALSE., &
           & open_file=LLopen, close_file=LLclose, &
           & select_query=LLselect, cancel_query=LLcancel, &
           & print_title=LLprthdr, print_timingstat=LLprttimingstat, &
           & start=j, limit=nchunk, got=igot, llc_idx=llc_idx)

        LLopen = .FALSE.
        LLprthdr= .FALSE.
        ntot = ntot + igot
        istart = istart + igot
        numrows = numrows - igot
        nleft = nleft - igot
        if (LLdebug) then
           write(0,1000) '< ntot,nleft,j,n,nchunk,numrows,nrows,istart,npolled,igot=',&
                &           ntot,nleft,j,n,nchunk,numrows,nrows,istart,npolled,igot
        endif
        if (LLspecial) goto 9999
        if (igot == 0) exit INNER_LOOP
      enddo INNER_LOOP
    endif ! if (LLinside) then ...

    npolled = npolled + nrows
  endif ! if (nrows > 0) then ...

 9999 continue
  rc = ODB_cancel(h, dtname, poolno=jp)
  rc = ODB_release(h, poolno=jp)

  if (LLdebug) then
     write(0,1000) '<< jp, jp1, jp2, jinc, nrows, ncols, numrows, istart=', &
          &            jp, jp1, jp2, jinc, nrows, ncols, numrows, istart
     write(0,*) 'LLspecial=',LLspecial
     call flush(0)
  endif

  if (LLspecial) exit OUTER_LOOP
  if (.not.LLevery .and. numrows <= 0) exit OUTER_LOOP
enddo OUTER_LOOP

deallocate(mypools)

rc = ODB_end()

end program odbgnuplot
