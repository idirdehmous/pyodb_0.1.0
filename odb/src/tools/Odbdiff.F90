program odbdiff
! This program opens two ODB databases DB1 & DB2
! and apply a query on both to find out data differences
USE PARKIND1  ,ONLY : JPIM,JPIB,JPRB
use odb_module
use mpl_module
USE str, only : toupper, sadjustr
USE yomhook, only : LHOOK, DR_HOOK
implicit none
REAL(KIND=JPRB), parameter :: rscale = 1000000._JPRB
REAL(KIND=JPRB), parameter :: eps = tiny(rscale)
INTEGER(KIND=JPIM) :: h(2), npools(2), nrows(2), ncols(2), nra
INTEGER(KIND=JPIM) :: j, jj, jp, rc, nkey, ilen, mkey(2), idx, nr, seqno, icolor
INTEGER(KIND=JPIM) :: istart, iend, ntotdiffs, numargs, maxdiffnum, ndiffs
INTEGER(KIND=JPIM) :: icol_lat(2), icol_lon(2), irad2deg(2), nbits, itmp
INTEGER(KIND=JPIM) :: adjust_right, jjj, doffset, memopt, jstart, jend, inc
INTEGER(KIND=JPIM) :: k, j1, j2, kk
character(len=1024) :: cmd, simulobs_file
character(len=64)   :: DB1, DB2, query, color
character(len=30)   :: cnum
INTEGER(KIND=JPIM)  :: irefval
character(len=8) :: ztr
REAL(KIND=JPRB)   , allocatable :: x1(:,:), x2(:,:), z(:)
INTEGER(KIND=JPIM), allocatable :: sortidx1(:), isort(:,:)
logical, allocatable :: LLtakethis1(:), LLtakethis2(:)
INTEGER(KIND=JPIM), allocatable :: pick1(:), pick2(:), xrossref(:)
character(len=64) , allocatable :: names1(:), names2(:), types(:)
logical :: LLmatch, LLddl, LLrad2deg(2), LLmemopt, LLfirst, LLlast
logical, allocatable :: LLstring(:), LLint(:), LLmask(:)
REAL(KIND=JPRB) :: ZHOOK_HANDLE, ZH1(3), ZH2(5)
REAL(KIND=JPRB) :: RAD2DEG, x, rlatinc, lat0, lat1, lat2
REAL(KIND=JPRB) :: RPI, RADIANS, RDEGREE
RAD2DEG(x) = RDEGREE * x

IF (LHOOK) CALL DR_HOOK('ODBDIFF',0,ZHOOK_HANDLE)
rc = ODB_init()

RPI    =2.0_JPRB*ASIN(1.0_JPRB)
RADIANS=RPI/180._JPRB
RDEGREE=180._JPRB/RPI

numargs = MPL_iargc()
maxdiffnum = 2147483647
simulobs_file = '/dev/null'
color = '_color'
icolor = 1
irad2deg(:) = 0
nbits = 0
adjust_right = 0
memopt=0
rlatinc = 0
if (numargs /= 13) then
  call MPL_getarg(0,cmd)
  write(0,*) 'Usage: '//trim(cmd)//' DB1 DB2 query_name maxdiffnum simulobs_file '//&
           & 'color 1_or_2 rad2deg1 rad2deg2 nbits adjust_right memopt rlatinc'
  CALL ODB_abort('MAIN','No. of args must be 13',numargs)
else
  if (numargs >= 1) then
    call MPL_getarg(1,DB1)
    write(0,*)'===== DB1='//trim(DB1)//' ====='
  endif
  if (numargs >= 2) then
    call MPL_getarg(2,DB2)
    write(0,*)'===== DB2='//trim(DB2)//' ====='
  endif
  if (numargs >= 3) then
    call MPL_getarg(3,query)
    write(0,*)'===== Query='//trim(query)//' ====='
  endif
  if (numargs >= 4) then
    call MPL_getarg(4,cnum)
    read(cnum,'(i30)') maxdiffnum
    if (maxdiffnum <= 0) maxdiffnum = 2147483647
  endif
  write(0,*)'===== Max no. of diffs=',maxdiffnum,' before stopping ====='
  if (numargs >= 5) then
    call MPL_getarg(5,simulobs_file)
  endif
  write(0,*)'===== Simulobs input on diffs goes to file='//trim(simulobs_file)//' ===='
  if (numargs >= 6) then
    call MPL_getarg(6,color)
    if (color == ' ') color = '_color'
  endif
  write(0,*)'===== Simulobs color column='//trim(color)//' ===='
  if (numargs >= 7) then
    call MPL_getarg(7,cnum)
    read(cnum,'(i30)') icolor
    if (icolor <= 0) icolor = 1
  endif
  write(0,*)'===== Poolno for database REFCMP=',icolor,' ===='
  if (numargs >= 8) then
    call MPL_getarg(8,cnum)
    read(cnum,'(i30)') irad2deg(1)
  endif
  write(0,*)'===== Radians to degree conversion to be performed for DB='//trim(DB1)//' : ',irad2deg(1),' ===='
  if (numargs >= 9) then
    call MPL_getarg(9,cnum)
    read(cnum,'(i30)') irad2deg(2)
  endif
  write(0,*)'===== Radians to degree conversion to be performed for DB='//trim(DB2)//' : ',irad2deg(2),' ===='
  if (numargs >= 10) then
    call MPL_getarg(10,cnum)
    read(cnum,'(i30)') nbits
    if (nbits <  0) nbits =  0
    if (nbits > 64) nbits = 64
  endif
  write(0,*)'===== Number of last bits to be masked out from converted (lat,lon) = ',nbits,' ===='
  if (numargs >= 11) then
    call MPL_getarg(11,cnum)
    read(cnum,'(i30)') adjust_right
  endif
  write(0,*)'===== Right-adjust all strings = ',adjust_right,' ===='
  if (numargs >= 12) then
    call MPL_getarg(12,cnum)
    read(cnum,'(i30)') memopt
  endif
  write(0,*)'===== Memory optimization for reference database (1=on/0=off) = ',memopt,' ===='
  if (numargs >= 13) then
    call MPL_getarg(13,cnum)
    read(cnum,'(f30.0)') rlatinc
    rlatinc = abs(rlatinc)
    rlatinc = max(rlatinc,0.1_JPRB) ! Minimum lat.band slice at least 0.1 degrees
  endif
  write(0,*)'===== Latitude increment =',rlatinc,' ===='
endif
LLmemopt = (memopt == 1)
LLrad2deg(1) = (irad2deg(1) == 1)
LLrad2deg(2) = (irad2deg(2) == 1)
icol_lat(:) = 0
icol_lon(:) = 0

ntotdiffs = 0
LLfirst = .TRUE.
lat0 = -90
inc = 0
nkey = 0

 100  continue
!-----------------------------------------------------------------------------------

lat1 = lat0 + inc * rlatinc
inc = inc + 1
lat2 = lat0 + inc * rlatinc
LLlast = (lat2 >= 90)

write(0,*)'==> Running with lat1=',lat1,', lat2=',lat2,' : inc=',inc

!-----------------------------------------------------------------------------------
!-- Process database#1 : Read query result into memory & release database
!-----------------------------------------------------------------------------------

h(1) = ODB_open(DB1,'READONLY',npools=npools(1))

if (LLfirst) then
  rc = ODB_getnames(h(1),query,'name')
  ncols(1) = rc
  allocate(names1(rc), types(rc), LLstring(rc), LLint(rc))
  LLstring(:) = .FALSE.
  LLint(:) = .FALSE.
  allocate(LLtakethis1(rc), pick1(rc))
  rc = ODB_getnames(h(1),query,'name',names1)
  rc = ODB_getnames(h(1),query,'type',types)
  do j=1,rc
    if (names1(j)(1:8) == 'LINKLEN(') then
      idx = scan(names1(j),')')
      if (idx > 0) then
        names1(j) = names1(j)(9:idx-1)//'.len'//names1(j)(idx+1:)
      endif
    else if (names1(j)(1:11) == 'LINKOFFSET(') then
      idx = scan(names1(j),')')
      if (idx > 0) then
        names1(j) = names1(j)(12:idx-1)//'.offset'//names1(j)(idx+1:)
      endif
    endif
    idx = scan(names1(j),'@',back=.TRUE.)
    if (idx > 0) names1(j)(idx:idx) = '_'
    idx = scan(names1(j),'.')
    if (idx > 0) then
      names1(j) = names1(j)(1:idx-1)//'_dot_'//names1(j)(idx+1:)
    endif
  enddo
endif ! if (LLfirst)

!-----------------------------------------------------------------------------------
!-- Open database#2 just to obtain ncols(2) and desired names2(:)
!-----------------------------------------------------------------------------------

if (LLfirst) then
  IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_QUICKOPEN',0,ZH2(1))
  h(2) = ODB_open(DB2,'READONLY',npools=npools(2))
  rc = ODB_getnames(h(2),query,'name')
  ncols(2) = rc
  allocate(names2(rc))
  allocate(LLtakethis2(rc), pick2(rc), xrossref(rc))
  rc = ODB_getnames(h(2),query,'name',names2)
  do j=1,rc
    if (names2(j)(1:8) == 'LINKLEN(') then
      idx = scan(names2(j),')')
      if (idx > 0) then
        names2(j) = names2(j)(9:idx-1)//'.len'//names2(j)(idx+1:)
      endif
    else if (names2(j)(1:11) == 'LINKOFFSET(') then
      idx = scan(names2(j),')')
      if (idx > 0) then
        names2(j) = names2(j)(12:idx-1)//'.offset'//names2(j)(idx+1:)
      endif
    endif
    idx = scan(names2(j),'@')
    if (idx > 0) names2(j)(idx:idx) = '_'
    idx = scan(names2(j),'.')
    if (idx > 0) then
      names2(j) = names2(j)(1:idx-1)//'_dot_'//names2(j)(idx+1:)
    endif
  enddo
  rc = ODB_close(h(2))
  IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_QUICKOPEN',1,ZH2(1))
endif

!-----------------------------------------------------------------------------------
!-- Find common column names between query@DB1 & query@DB2
!-----------------------------------------------------------------------------------

if (LLfirst) then
  LLtakethis1(:) = .FALSE.
  pick1(:) = 0
  LLtakethis2(:) = .FALSE.
  pick2(:) = 0
  xrossref(:) = 0

  k = 0
  do j1=1,ncols(1)
    do j2=1,ncols(2)
      if (LLtakethis2(j2)) then
        cycle
      else if (names1(j1) == names2(j2)) then
        k = k + 1
        LLtakethis1(j1) = .TRUE.
        pick1(k) = j1
        LLtakethis2(j2) = .TRUE.
        pick2(k) = j2
        xrossref(j2) = j1
        exit
      endif
    enddo
  enddo

  nkey = k
  if (nkey == 0) then
    write(0,*)'***Error: No common columns between DB1 & DB2 queries: nkey=',nkey
  endif

    write(0,*)'Query info for DB1="'//trim(DB1)//'" : ncols=',ncols(1)
    do j1=1,ncols(1)
      write(0,'(1x,2i10,1x,L1,1x,1h",a,1h")') j1,pick1(j1),LLtakethis1(j1),names1(j1)
    enddo
    write(0,*)'Query info for DB2="'//trim(DB2)//'" : ncols=',ncols(2)
    do j2=1,ncols(2)
      write(0,'(1x,2i10,1x,L1,1x,1h",a,1h")') j2,pick2(j2),LLtakethis2(j2),names2(j2)
    enddo
    write(0,*)'xrossref: size=',size(xrossref),', values follow:'
    write(0,*) xrossref(:)

  if (nkey == 0) then
    CALL ODB_abort('MAIN','No common columns between DB1 & DB2 queries',-2)
  endif

  do j=1,ncols(1)
    if (LLtakethis1(j)) then
      if (types(j) == 'Bitfield') types(j) = 'pk1int'
      LLstring(j) = (types(j) == 'string')
      LLint(j) = (scan(types(j),'int') > 0 .or. types(j)(1:4) == 'link')
      if (LLrad2deg(1) .and. names1(j)(1:7) == 'lat_hdr') icol_lat(1) = j ! hardcoding : 'lat_hdr'
      if (LLrad2deg(1) .and. names1(j)(1:7) == 'lon_hdr') icol_lon(1) = j ! hardcoding : 'lon_hdr'
    endif
  enddo

  do j=1,ncols(2)
    if (LLtakethis2(j)) then
      if (LLrad2deg(2) .and. names2(j)(1:7) == 'lat_hdr') icol_lat(2) = j ! hardcoding : 'lat_hdr'
      if (LLrad2deg(2) .and. names2(j)(1:7) == 'lon_hdr') icol_lon(2) = j ! hardcoding : 'lon_hdr'
    endif
  enddo

  do j=1,2
    if (icol_lat(j) == 0 .or. icol_lon(j) == 0) LLrad2deg(j) = .FALSE.
    if (.not. LLrad2deg(j)) then
      icol_lat(j) = 0
      icol_lon(j) = 0
    endif
  enddo

  if (trim(simulobs_file) /= '/dev/null' .and. trim(simulobs_file) /= ' ') then
    open(1,file='REFCMP.ddl',status='unknown',position='rewind')
    write(1,'(a)') 'CREATE TABLE refcmp AS ('
    do j=1,ncols(1)
      if (LLtakethis1(j)) then
        write(1,'(2x,a)') trim(names1(j))//' '//trim(types(j))//','
      endif
    enddo
    write(1,'(2x,a)') trim(color)//' pk1int,'
    write(1,'(a)') ');'
    close(1)
    LLddl = .true.
  else
    LLddl = .false.
  endif

  deallocate(names2)
endif

!-----------------------------------------------------------------------------------
!-- Continue processing with database#1 
!-----------------------------------------------------------------------------------

jp = -1 ! All pools in one go for database#1 (you may not have enough memory for this
        ! ==> to be fixed later; meanwhile: ask less data in your query ;-)
        !     or use latitude-band approach

IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB1_OPENGETCLOSE',0,ZH1(1))

if (LLfirst) then
  rc = ODB_setval(h(1), '$lat1', lat1-eps, query)
else
  rc = ODB_setval(h(1), '$lat1', lat1, query)
endif

if (LLlast) then
  rc = ODB_setval(h(1), '$lat2', lat2+eps, query)
else
  rc = ODB_setval(h(1), '$lat2', lat2, query)
endif

rc = ODB_select(h(1), query, nrows(1), ncols(1), nra=nra, poolno=jp)
write(0,*) '<1>poolno=',jp,' : nrows(1) = ',nrows(1)
allocate(x1(nra,0:ncols(1)))

if (LLmemopt) then
! Load incrementally to fit more into memory (can be much slower)
  jstart=1
  jend=npools(1)
else
! Load all in one go (may not fit into memory)
  jstart=-1
  jend=-1
endif

doffset = 0
do jp=jstart,jend
  rc = ODB_get(h(1), query, x1, nrows(1), ncols(1), offset=doffset, poolno=jp, &
       &       colget=LLtakethis1)
  write(0,*) '<1>poolno=',jp,' : doffset, rc =',doffset, rc
  doffset = doffset + rc
  rc = ODB_cancel(h(1), query, poolno=jp)
  rc = ODB_release(h(1), poolno=jp)
enddo

rc = ODB_close(h(1))
IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB1_OPENGETCLOSE',1,ZH1(1))

write(0,*)'==> nkey =',nkey
ilen = nkey * 8 ! nkey times 8 bytes

allocate(z(nkey))

!..... Perform radians to degrees conversion for (lat,lon), if requested/applicable
if (LLrad2deg(1)) then
  do j=1,nrows(1)
    x1(j,icol_lat(1)) = rad2deg(x1(j,icol_lat(1)))
    x1(j,icol_lon(1)) = rad2deg(x1(j,icol_lon(1)))
  enddo
endif

!..... Mask out (zero) <nbits> last bits that are due to rounding of error on different machines

if (LLrad2deg(1)) then
  write(0,*)'==> nbits=',nbits

  if (LLrad2deg(1)) then
    call cmask64bits(x1(1,icol_lat(1)),nrows(1),nbits)
    call cmask64bits(x1(1,icol_lon(1)),nrows(1),nbits)
  endif

  if (LLrad2deg(1)) then
    do j=1,nrows(1)
      itmp = nint(x1(j,icol_lat(1)) * rscale)
      x1(j,icol_lat(1)) = itmp
      itmp = nint(x1(j,icol_lon(1)) * rscale)
      x1(j,icol_lon(1)) = itmp
    enddo
  endif
endif

!..... Right-adjust strings, if any
if (adjust_right == 1 .and. any(LLstring(:))) then
  do jj=1,ncols(1)
    if (LLtakethis1(jj) .and. LLstring(jj)) then
      do j=1,nrows(1)
        x = x1(j,jj)
        write(ztr,'(a8)') x
        ztr = sadjustr(ztr)
        read(ztr,'(a8)') x
        x1(j,jj) = x
      enddo
    endif
  enddo
endif

!..... Generate adjusted 32-bit cyclic redundancy check -key (a pseudo-seqno)
allocate(isort(nrows(1),2))
IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB1_CRC32',0,ZH1(2))
do j=1,nrows(1)
  isort(j,1) = 0
  z(1:nkey) = x1(j,pick1(1:nkey))
  call crc32(z(1),ilen,isort(j,1))  ! Generate near-unique key
enddo
do j=1,nrows(1)
  isort(j,2) = j
enddo
IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB1_CRC32',1,ZH1(2))

allocate(sortidx1(nrows(1)))
mkey(1) = 1
mkey(2) = 2
IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB1_KEYSORT',0,ZH1(3))
call keysort(rc,isort,nrows(1),multikey=mkey,index=sortidx1,init=.true.)
IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB1_KEYSORT',1,ZH1(3))

allocate(LLmask(nrows(1)))
LLmask(:) = .TRUE.

if (LLfirst .and. LLddl) then
  open(1,file=trim(simulobs_file),status='unknown',position='append')
  do j=1,ncols(1)
    if (LLtakethis1(j)) then
      write(1,'(1x,a)',advance='no') trim(names1(j))
    endif
  enddo    
  write(1,'(1x,a)',advance='no') trim(color)
  write(1,*)
endif

if (LLfirst) then
  deallocate(names1)
  deallocate(types)
endif

!-----------------------------------------------------------------------------------
!-- Process database#2
!-----------------------------------------------------------------------------------

IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_OPENGETCLOSE',0,ZH2(1))
h(2) = ODB_open(DB2,'READONLY',npools=npools(2))

if (LLfirst) then
  rc = ODB_setval(h(2), '$lat1', lat1-eps, query)
else
  rc = ODB_setval(h(2), '$lat1', lat1, query)
endif

if (LLlast) then
  rc = ODB_setval(h(2), '$lat2', lat2+eps, query)
else
  rc = ODB_setval(h(2), '$lat2', lat2, query)
endif

nr = 0
DB2_LOOP: do jp=1,npools(2) ! Pool-by-pool to save memory 
  ndiffs = 0

  IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_SELECTGETRELEASE',0,ZH2(2))
  rc = ODB_select(h(2), query, nrows(2), ncols(2), nra=nra, poolno=jp)
  write(0,1000,advance='no') '<2>poolno=',jp,' : nrows(2) = ',nrows(2),char(13)
 1000 format(1x,a,i5,a,i12,a)

  if (nrows(2) == 0) then
    rc = ODB_release(h(2), poolno=jp)
    IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_SELECTGETRELEASE',1,ZH2(2))
    cycle DB2_LOOP
  endif

  nr = nr + nrows(2)

  allocate(x2(nra,0:ncols(2)))
  rc = ODB_get(h(2), query, x2, nrows(2), ncols(2), poolno=jp, &
       &       colget=LLtakethis2)
  rc = ODB_release(h(2), poolno=jp)
  IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_SELECTGETRELEASE',1,ZH2(2))

  if (LLrad2deg(2)) then
    do j=1,nrows(2)
      x2(j,icol_lat(2)) = rad2deg(x2(j,icol_lat(2)))
      x2(j,icol_lon(2)) = rad2deg(x2(j,icol_lon(2)))
    enddo
  endif

  if (LLrad2deg(2)) then
    if (LLrad2deg(2)) then
      call cmask64bits(x2(1,icol_lat(2)),nrows(2),nbits)
      call cmask64bits(x2(1,icol_lon(2)),nrows(2),nbits)
    endif

    if (LLrad2deg(2)) then
      do j=1,nrows(2)
        itmp = nint(x2(j,icol_lat(2)) * rscale)
        x2(j,icol_lat(2)) = itmp
        itmp = nint(x2(j,icol_lon(2)) * rscale)
        x2(j,icol_lon(2)) = itmp
      enddo
    endif
  endif

  if (adjust_right == 1 .and. any(LLstring(:))) then
    do jj=1,ncols(2)
      if (.not.LLtakethis2(jj)) cycle
      kk = xrossref(jj)
      if (LLstring(kk)) then
        do j=1,nrows(2)
          x = x2(j,jj)
          write(ztr,'(a8)') x
          ztr = sadjustr(ztr)
          read(ztr,'(a8)') x
          x2(j,jj) = x
        enddo
      endif
    enddo
  endif

  IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_CRC32',0,ZH2(4))
  do j=1,nrows(2)
    seqno = 0
    z(1:nkey) = x2(j,pick2(1:nkey))
    call crc32(z(1),ilen,seqno) ! Generate near-unique key
    x2(j,0) = seqno
  enddo
  IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_CRC32',1,ZH2(4))

  IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_MATCHING',0,ZH2(3))
  A_LOOP: do j=1,nrows(2)
    irefval = x2(j,0)
    idx = ODB_binsearch(irefval, isort(1:nrows(1),1), 1, nrows(1), &
                       & index=sortidx1, &
                       & cluster_start=istart, cluster_end=iend)

    LLmatch = (idx >= 1 .and. idx <= nrows(1))

    if (LLmatch) then
      z(1:nkey) = x2(j,pick2(1:nkey))
      LLmatch = .FALSE.
      IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_LINEAR_SEARCH_LOOP',0,ZH2(5))
      LOOP: do jj=istart,iend
        if (.not.LLmask(jj)) cycle LOOP
        idx = sortidx1(jj)
        LLmatch = ALL(z(1:nkey) == x1(idx,pick1(1:nkey)))
        if (LLmatch) then ! An EXACT match found
          LLmask(jj) = .FALSE. ! Do not match this location twice
          exit LOOP
        endif
      enddo LOOP
      IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_LINEAR_SEARCH_LOOP',1,ZH2(5))
    endif

    if (.not.LLmatch) then
      ndiffs = ndiffs + 1
      ntotdiffs = ntotdiffs + 1
      if (LLddl) then
        do jj=1,ncols(2)
           if (.not.LLtakethis2(jj)) cycle
           kk = xrossref(jj)
           if (LLstring(kk)) then
             write(ztr,'(a8)') x2(j,jj)
             do jjj=1,len(ztr)
               itmp = ichar(ztr(jjj:jjj))
               if (itmp < 32 .or. itmp > 126) ztr(jjj:jjj) = '?'
             enddo
             write(1,'(1x,a10)',advance='no') "'"//ztr//"'"
           else if (LLint(kk)) then
             write(1,'(1x,i11)',advance='no') int(x2(j,jj))
           else
             if (jj == icol_lat(2) .or. jj == icol_lon(2)) then
               write(1,'(1x,-6p,f20.6)',advance='no') x2(j,jj)
             else
               write(1,'(1x,1p,g20.12)',advance='no') x2(j,jj)
             endif
           endif
        enddo
        write(1,'(1x,i2)',advance='no') icolor
        write(1,*)
!-start debug
!        if (LLrad2deg(1).or.LLrad2deg(2)) then
!          write(1,'(a)',advance='no') '# 0x:'
!          do jj=1,ncols(2)
!            if (jj == icol_lat(1) .or. jj == icol_lon(1) .or. &
!                jj == icol_lat(2) .or. jj == icol_lon(2)) then
!              x = x2(j,jj) ! No back-scaling here
!              write(1,'(1x,g30.20)',advance='no') x
!              write(1,'(1x,z16.16)',advance='no') x
!              write(1,'(1x,b64.64)',advance='no') x
!            endif
!          enddo
!          write(1,*)
!        endif
!-end debug
      endif
      if (ntotdiffs >= maxdiffnum) exit A_LOOP
    endif
  enddo A_LOOP
  IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_MATCHING',1,ZH2(3))

  deallocate(x2)
  write(0,1100) '<2>poolno=',jp,' : nrows(2) = ',nrows(2), ' > ',ndiffs,' diffs'
 1100 format(1x,a,i5,a,i12,a,i12,a)
  if (ntotdiffs >= maxdiffnum) exit DB2_LOOP
enddo DB2_LOOP

deallocate(LLmask)
deallocate(x1)
deallocate(sortidx1)
deallocate(isort)
deallocate(z)

write(0,*)'>>> Number of diffs found =',ntotdiffs,', limit =',maxdiffnum

rc = ODB_close(h(2))
IF (LHOOK) CALL DR_HOOK('ODBDIFF:DB2_OPENGETCLOSE',1,ZH2(1))

LLfirst = .FALSE.

if (.not.LLlast .and. ntotdiffs < maxdiffnum) goto 100

deallocate(LLstring)
deallocate(LLint)

deallocate(LLtakethis1)
deallocate(pick1)
deallocate(LLtakethis2)
deallocate(pick2)
deallocate(xrossref)

if (LLddl) then
  write(1,'(a,i20)') '# Number of diffs found = ',ntotdiffs
  close(1)
endif

rc = ODB_end()
IF (LHOOK) CALL DR_HOOK('ODBDIFF',1,ZHOOK_HANDLE)

end program odbdiff
