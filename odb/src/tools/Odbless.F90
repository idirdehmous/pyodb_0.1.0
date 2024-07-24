program odbless
!
!-- To browse particular ODB-table and/or SQL
!
!   Usage: 
!
! odbless.x dbname view/tablename starting_row numrows row_buffer_size konvert2degrees summary debug every
!
!   Pools controlled via ODB_PERMANENT_POOLMASK_<dbname> or ODB_PERMANENT_POOLMASK
!

USE PARKIND1  ,ONLY : JPIM,JPRB,JPIB
use odb_module
use mpl_module
implicit none

INTEGER(KIND=JPIM) :: h, rc, npools, nproc, myproc, jp, rowbuf, konvert
INTEGER(KIND=JPIM) :: nrows, ncols, istart, numrows, npolled, igot
INTEGER(KIND=JPIM) :: numargs, ntot, summary, debug, every
INTEGER(KIND=JPIM) :: saved(2), iret
INTEGER(KIND=JPIM) :: jp1, jp2, jinc, llc_idx(2)
INTEGER(KIND=JPIM) :: nchunk, nleft, nmypools, jj
INTEGER(KIND=JPIM) :: j1, j2, n
INTEGER(KIND=JPIB) :: j
character(len=256) arg
character(len=64)  dbname, dtname, latlon_names(size(llc_idx)), env
logical LLopen, LLclose, LLselect, LLcancel, LLinside, LLkonvert, LLspecial
logical LLprthdr, LLprttimingstat, LLalloc, LLsummary, LLdebug, LLevery, LLaggr
!-- stat: see module file odb/module/odbstat.F90 for more; currently:
!   1=counts (non-MDIs), 2=counts (MDIs), 3=min, 4=max, 5=sum->avg, 6=sumsqr->stdev, 7=sumsqr->rms
INTEGER(KIND=JPIM), parameter :: nstat = odb_statlen
REAL(KIND=JPRB), allocatable :: stat(:,:)
INTEGER(KIND=JPIM), allocatable :: mypools(:)
INTEGER(KIND=JPIM) :: my_first_pool(1)

rc = ODB_init(myproc=myproc, nproc=nproc)

istart  =    1
numrows =   20
rowbuf  = 1000
konvert =    0
summary =    1
debug   =    0

numargs = MPL_iargc()

if (numargs >= 2) then
  call MPL_getarg(1, dbname)
  call MPL_getarg(2, dtname)
  
  if (numargs >= 3) then
    call MPL_getarg(3, arg)
    read(arg,'(i12)',err=33,end=33) istart
  endif
  istart = max(1,istart)
33 continue

  if (numargs >= 4) then
    call MPL_getarg(4, arg)
    read(arg,'(i12)',err=44,end=44) numrows
  endif
  if (numrows <= 0) numrows = 2147483647
44 continue

  if (numargs >= 5) then
    call MPL_getarg(5, arg)
    read(arg,'(i12)',err=55,end=55) rowbuf
  endif
  if (rowbuf <= 0) rowbuf = -1
55 continue

  if (numargs >= 6) then
    call MPL_getarg(6, arg)
    read(arg,'(i12)',err=66,end=66) konvert
  endif
66 continue

  if (numargs >= 7) then
    call MPL_getarg(7, arg)
    read(arg,'(i12)',err=77,end=77) summary
  endif
77 continue

  if (numargs >= 8) then
    call MPL_getarg(8, arg)
    read(arg,'(i12)',err=88,end=88) debug
  endif
88 continue

  if (numargs >= 9) then
    call MPL_getarg(9, arg)
    read(arg,'(i12)',err=99,end=99) every
  endif
99 continue

else
  call MPL_getarg(0, arg)
  CALL ODB_abort('odbless',&
  & 'Usage: '//trim(arg)//&
  & ' dbname view/tablename starting_row numrows'//&
  & ' row_buffer_size konvert2degrees summary debug every')
endif

h = ODB_open(dbname, 'READONLY', npools)

!-- Fully tolerate errors in time-records i.e. fix records when calling twindow/tdiff funcs
call codb_allow_time_error(235959)

call codb_putenv('ODB_REPORTER=stdout')

LLopen = .TRUE.
LLclose = .FALSE.
LLselect = .FALSE.
LLcancel = .FALSE.
LLkonvert = (konvert == 1)
if (.not.LLkonvert) konvert = 0
LLsummary = (summary >= 1)
if (.not.LLsummary) summary = 0
LLdebug = (debug == 1)
if (.not.LLdebug) debug = 0
LLevery = (every == 1)
if (.not.LLevery) every = 0
LLaggr = ODB_has_aggrfuncs(h, dtname)

if (ODB_has_orderby(h, dtname) .or. &
  & LLaggr .or. &
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
  if (LLaggr) LLselect = .TRUE.
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

if (nmypools >= 1) then
  my_first_pool(1) = mypools(1)
else
  my_first_pool(1) = -1
endif

if (LLaggr) then
  iret = ODB_poolinfo(h, my_first_pool, with_poolmask=.TRUE.)
  if (iret <= 0) my_first_pool(1) = -1
endif

20061 format(a)
20062 format(3(a,i11))
20063 format(4(a,i1))
write(6,20061) ':odbless: database='//trim(dbname)//', view/table='//trim(dtname)
write(6,20062) ':odbless: row-start=',istart,', row-limit=',numrows,', row-bufsize=',rowbuf
write(6,20063) ':odbless: convert (lat,lon) to degrees=',konvert,&
     & ', print summary statistics=',summary,&
     & ', debug mode=',debug,', every=',every
call flush(6)

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
  if (.not.LLaggr) then
    rc = ODB_select(h, dtname, nrows, ncols, poolno=jp)
  else
    rc = ODB_getsize(h, dtname, nrows, ncols, poolno=my_first_pool(1))
    nrows = 2147483647
  endif

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
        if (LLsummary) then
          allocate(stat(nstat,ncols))
          stat(:,:) = 0
        else
          allocate(stat(0,ncols))
        endif

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

      j = j1
      INNER_LOOP: do while (j <= j2) 
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
           & stat=stat, &
           & start=int(j), limit=nchunk, got=igot, llc_idx=llc_idx)

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
        j = j + n ! Note: j is 64-bit/8-byte integer and doesn't overflow here, when n is big (e.g. when LLaggr is true)
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

if (LLsummary .and. LLalloc .and. ntot > 0) then
  if (LLdebug) write(0,*)'ntot=',ntot
  rc = ODB_print(h, dtname, file='stdout', poolno=1, &
       & inform_progress=.FALSE., show_DB_index=.FALSE.,&
       & open_file=.TRUE., close_file=.TRUE., &
       & select_query=.FALSE., cancel_query=.FALSE., &
       & print_title=.FALSE., print_timingstat=.FALSE., print_summary=.TRUE., &
       & stat=stat)
endif

if (allocated(stat)) deallocate(stat)
deallocate(mypools)

rc = ODB_end()

end program odbless
