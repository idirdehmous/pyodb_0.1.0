#if INT_VERSION == 4

#define GEN_TYPE             INTEGER(KIND=JPIM)
#define GEN_TOLSEARCH        ODB_itolsearch
#define GEN_TOLSEARCH_NAME  'ODB_itolsearch'
#define GEN_BINSEARCH        ODB_ibinsearch
#define GEN_BINSEARCH_NAME  'ODB_ibinsearch'
#define GEN_DUPL0            iduplchk0
#define GEN_DUPL0_NAME      'iduplchk0'
#define GEN_UI_RATIO         1
#define GEN_DUPL             ODB_iduplchk
#define GEN_DUPL_NAME       'ODB_iduplchk'
#define GEN_GROUP            ODB_igroupify
#define GEN_GROUP_NAME      'ODB_igroupify'
#define GEN_UNIQUE_FUNC      cODB_ui_unique
#define GEN_AGGR             ODB_iaggregate
#define GEN_AGGR_NAME       'ODB_iaggregate'

#elif REAL_VERSION == 8

#define GEN_TYPE             REAL(KIND=JPRB)
#define GEN_TOLSEARCH        ODB_dtolsearch
#define GEN_TOLSEARCH_NAME  'ODB_dtolsearch'
#define GEN_BINSEARCH        ODB_dbinsearch
#define GEN_BINSEARCH_NAME  'ODB_dbinsearch'
#define GEN_DUPL0            dduplchk0
#define GEN_DUPL0_NAME      'dduplchk0'
#define GEN_UI_RATIO         2
#define GEN_DUPL             ODB_dduplchk
#define GEN_DUPL_NAME       'ODB_dduplchk'
#define GEN_GROUP            ODB_dgroupify
#define GEN_GROUP_NAME      'ODB_dgroupify'
#define GEN_UNIQUE_FUNC      cODB_d_unique
#define GEN_AGGR             ODB_daggregate
#define GEN_AGGR_NAME       'ODB_daggregate'

#else

  ERROR in programming : No datatype given (should never have ended up here)

#endif

#if defined(INT_VERSION) || defined(REAL_VERSION)

FUNCTION GEN_DUPL0(&
    &a, nrows, ncols, npkcols,&
    &colidx, dupl_with,idx)&
    &RESULT(ndupl)

implicit none
INTEGER(KIND=JPIM), intent(in)     :: nrows, ncols, npkcols
INTEGER(KIND=JPIM), intent(out)    :: dupl_with(:)
GEN_TYPE, intent(in)      :: a(:,:)
INTEGER(KIND=JPIM),  intent(in)    :: colidx(npkcols)
INTEGER(KIND=JPIM),intent(in),optional :: idx(:)

INTEGER(KIND=JPIM) :: cnt(nrows), info(5 + npkcols)
INTEGER(KIND=JPIM) :: ndupl, kpkcols
INTEGER(KIND=JPIM) :: i, j, jj, kk, nldx, itag, ihash
INTEGER(KIND=JPIM) :: is_unique, n
GEN_TYPE, allocatable :: x(:,:)
INTEGER(KIND=JPIM) , allocatable :: hash(:)

ndupl = 0
dupl_with(:) = 0

kpkcols = npkcols
do jj=1,npkcols
  j = colidx(jj)
  if (j < 1 .or. j > ncols) kpkcols = kpkcols - 1
enddo

n = min(size(a,dim=1),nrows)
n = min(n,size(dupl_with))
if (present(idx)) n = min(n,size(idx))

if (db_trace) then
  info(1) = n
  info(2) = nrows
  info(3) = ncols
  info(4) = npkcols
  info(5) = kpkcols
  info(5 + 1:5 + npkcols) = colidx(npkcols)
  CALL cODB_trace(-1, 1,&
         GEN_DUPL0_NAME,&
         info, size(info))
endif

if (kpkcols == 0) goto 9999
if (n < nrows) goto 9999

nldx = ODB_lda(kpkcols)
allocate(x(nldx,n))

kk = 0
do jj=1,npkcols
  j = colidx(jj)
  if (j < 1 .or. j > ncols) cycle
  kk = kk + 1
  if (present(idx)) then
    do i=1,n
      x(kk,i) = a(idx(i),j)
    enddo
  else
    do i=1,n
      x(kk,i) = a(i,j)
    enddo
  endif
enddo

CALL cODB_hash_set_lock()
CALL cODB_hash_init()

allocate(hash(n))
CALL cODB_vechash(&
     kpkcols * GEN_UI_RATIO, &
     nldx * GEN_UI_RATIO,&
     n, &
     x(1,1), hash(1))

do i=1,n
  itag = i
  CALL &
       GEN_UNIQUE_FUNC(&
       kpkcols, x(1,i), hash(i), &
       is_unique, itag, ihash)
  if (is_unique == 0) then ! i.e. is duplicate
    if (present(idx)) then
      dupl_with(i) = idx(itag)
    else
      dupl_with(i) = itag
    endif
  endif
enddo

CALL cODB_hash_init()

deallocate(hash)
deallocate(x)

CALL cODB_hash_unset_lock()

ndupl = count(dupl_with(:) > 0)

9999 continue
!write(0,*) trim(GEN_DUPL0_NAME)//&
!    &': No. of duplicates found =',ndupl

if (db_trace) then
  info(1) = n
  info(2) = n - ndupl
  info(3) = ndupl
  CALL cODB_trace(-1, 0, &
         GEN_DUPL0_NAME,&
         info, 3)
endif

END FUNCTION


FUNCTION GEN_TOLSEARCH(&
    &tol, v, &
    &ksta, kend, index)&
    &RESULT(loc)
implicit none

INTEGER(KIND=JPIM), intent(in)  :: ksta, kend
GEN_TYPE, intent(in) :: tol, v(:)
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: index(:)
INTEGER(KIND=JPIM) :: n, k, low, high, loc
GEN_TYPE refval, value, delta, ztol

n = size(v)
if (present(index)) n = min(n,size(index))

loc = ksta

if (n <= 0) return
if (ksta > kend) return
if (ksta < 1 .or. ksta > n) return
if (kend < 1 .or. kend > n) return
if (tol < 0) return

low  = ksta
high = kend
ztol = tol

if (present(index)) then
  refval = v(index(low))
  do k=low+1,high
    delta = abs(refval-v(index(k)))
    if (delta > ztol) then
      loc = k-1
      return
    endif
  enddo
else
  refval = v(low)
  do k=low+1,high
    delta = abs(refval-v(k))
    if (delta > ztol) then
      loc = k-1
      return
    endif
  enddo
endif

loc = high

END FUNCTION

FUNCTION GEN_BINSEARCH(&
    &refval, v, &
    &ksta, kend, &
    &index, &
    &cluster_start, cluster_end) &
    &RESULT(loc)
implicit none

INTEGER(KIND=JPIM), intent(in)  :: ksta, kend
GEN_TYPE, intent(in) :: v(:), refval
INTEGER(KIND=JPIM), intent(in) , OPTIONAL :: index(:)
INTEGER(KIND=JPIM), intent(out), OPTIONAL :: cluster_start, cluster_end
INTEGER(KIND=JPIM) :: n, k, low, high, loc, mid

n = size(v)
if (present(index)) n = min(n,size(index))

loc = 0

if (present(cluster_start)) cluster_start = 0
if (present(cluster_end)) cluster_end = 0

if (n <= 0) goto 9999
if (ksta > kend) goto 9999
if (ksta < 1 .or. ksta > n) goto 9999
if (kend < 1 .or. kend > n) goto 9999

low  = ksta
high = kend

if (present(index)) then
  do while (low <= high)
    mid = (low+high)/2
    k = mid
    if (refval < v(index(k))) then
      high = mid - 1
    else if (refval > v(index(k))) then
      low = mid + 1
    else ! Match found
      loc = k
      if (present(cluster_start)) then
        if (refval > v(index(ksta))) then
          cluster_start = loc
          do k=loc-1,ksta,-1
            if (refval > v(index(k))) exit
            cluster_start = k
          enddo
        else
          cluster_start = ksta
        endif
      endif
      if (present(cluster_end)) then
        if (refval < v(index(kend))) then
          cluster_end = loc
          do k=loc+1,kend
            if (refval < v(index(k))) exit
            cluster_end = k
          enddo
        else
          cluster_end = kend
        endif
      endif
      goto 9999
    endif
  enddo ! do while (low <= high)
else
  do while (low <= high)
    mid = (low+high)/2
    k = mid
    if (refval < v(k)) then
      high = mid - 1
    else if (refval > v(k)) then
      low = mid + 1
    else ! Match found
      loc = k
      if (present(cluster_start)) then
        if (refval > v(ksta)) then
          cluster_start = loc
          do k=loc-1,ksta,-1
            if (refval > v(k)) exit
            cluster_start = k
          enddo
        else
          cluster_start = ksta
        endif
      endif
      if (present(cluster_end)) then
        if (refval < v(kend)) then
          cluster_end = loc
          do k=loc+1,kend
            if (refval < v(k)) exit
            cluster_end = k
          enddo
        else
          cluster_end = kend
        endif
      endif
      goto 9999
    endif
  enddo ! do while (low <= high)
endif

loc = 0

9999 continue

END FUNCTION

!---  Public routines
FUNCTION GEN_DUPL(&
    &a, nrows, ncols, npkcols,&
    &colidx, tol, dupl_with, idx)&
    &RESULT(ndupl)
USE odbshared, only : db_trace
implicit none
INTEGER(KIND=JPIM), intent(in)     :: nrows, ncols, npkcols
INTEGER(KIND=JPIM), intent(out)    :: dupl_with(:)
GEN_TYPE, intent(inout) :: a(:,:)  ! Not actually updated (indexed sort)
INTEGER(KIND=JPIM),  intent(in)    :: colidx(npkcols)
GEN_TYPE, intent(in)    :: tol(npkcols)
INTEGER(KIND=JPIM),intent(in),optional :: idx(:)

INTEGER(KIND=JPIM) :: cnt(nrows), info(5 + npkcols), index(nrows)
INTEGER(KIND=JPIM) :: icnt, iloc, inc, maxsweep, n, nra
INTEGER(KIND=JPIM) :: ndupl, kpkcols, isweep, rc, nhigh
INTEGER(KIND=JPIM) :: i,j,k,ii,jj,kk,iend,iii,jjarr(1)
GEN_TYPE, allocatable :: atemp(:,:)
INTEGER(KIND=JPIM), allocatable :: mkeys(:)
GEN_TYPE toler
logical zerotol

zerotol = ALL(tol(:)<=0)
if (zerotol) then
  ndupl = GEN_DUPL0(&
         a, nrows, ncols, npkcols,&
         colidx, dupl_with, idx=idx)
  return
endif

ndupl = 0
dupl_with(:) = 0

kpkcols = npkcols
do jj=1,npkcols
  j = colidx(jj)
  if (j < 1 .or. j > ncols) kpkcols = kpkcols - 1
enddo

n = min(size(a,dim=1),nrows)
n = min(n,size(dupl_with))
if (present(idx)) n = min(n,size(idx))

if (db_trace) then
  info(1) = n
  info(2) = nrows
  info(3) = ncols
  info(4) = npkcols
  info(5) = kpkcols
  info(5 + 1:5 + npkcols) = colidx(npkcols)
  CALL cODB_trace(-1, 1,&
       GEN_DUPL_NAME,&
       info, size(info))
endif

if (kpkcols == 0) goto 9999
if (n < nrows) goto 9999

allocate(mkeys(kpkcols))
kk = 0
do jj=1,npkcols
  j = colidx(jj)
  if (j < 1 .or. j > ncols) cycle
  kk = kk + 1
  mkeys(kk) = j
enddo

if (kk > 0) then
  if (present(idx)) then
    do j=1,n
      index(j) = idx(j)
    enddo
  else
    do j=1,n
      index(j) = j
    enddo
  endif
  CALL keysort(rc, a, nrows, &
       key=mkeys(1),&
!       multikey=mkeys(1:kk),&
       index=index, init=.FALSE.)
endif

deallocate(mkeys)

if (kk == 0) goto 9999
maxsweep = kk

!write(0,*) trim(GEN_DUPL_NAME)//': maxsweep=',maxsweep

cnt(:) = 0
isweep = 1
do jj=1,npkcols
  j = colidx(jj)
  if (j < 1 .or. j > ncols) cycle
!!  write(0,*)'atemp: start of sweep ',isweep,'; jj,j=',jj,j

  jjarr(1) = jj
  CALL cODB_trace(-1, 2,&
       trim(GEN_DUPL_NAME)//': jj=', &
       jjarr, 1)

  toler = tol(jj)

!  write(0,*) trim(GEN_DUPL_NAME)//' : jj, j, tol(jj)=',jj, j, tol(jj)

  ii = 1
  do while ( ii < nrows )
    iloc = -1
    if (isweep > 1) then
      icnt = cnt(ii)
      nhigh = min(ii+icnt-1,nrows)
      if (nhigh - ii + 1 > 2) then
        if (.not.allocated(atemp)) then
          nra = ODB_lda(nrows)
          allocate(atemp(nra,2))
!!          write(0,*)'atemp: allocated; ',nrows,nra,2,shape(atemp)
        endif
!!        write(0,*)'atemp: ii,nhigh=',ii,nhigh
        do iii=ii,nhigh
          atemp(iii,1) = a(index(iii),j)
          atemp(iii,2) = index(iii)
        enddo
!!        write(0,*)'atemp:  iii,atemp(ii:nhigh,1:2) in>'
!!        do iii=ii,nhigh
!!           write(0,*) iii,atemp(iii,1:2),index(iii),a(index(iii),j)
!!        enddo
        CALL keysort(rc, atemp(ii:nhigh,1:2), nhigh - ii + 1, key=1)
        do iii=ii,nhigh
          index(iii) = atemp(iii,2)
        enddo
!!        write(0,*)'atemp:  iii,atemp(ii:nhigh,1:2) out<'
!!        do iii=ii,nhigh
!!           write(0,*) iii,atemp(iii,1:2),index(iii),a(index(iii),j)
!!        enddo
      endif
    else
      icnt = 0
      nhigh = nrows
    endif

    if (ii < nhigh) then
      iloc = GEN_TOLSEARCH(&
             toler, a(1:nrows,j),&
             ii, nhigh, index=index)
    else
      iloc = ii
    endif

    icnt = iloc - ii + 1

!!    write(0,*) 'atemp: ii,nhigh,iloc,icnt=',ii,nhigh,iloc,icnt

!    if (isweep > 1 .and. icnt > 0) &
!      write(0,'(6i8)') isweep,ii,nhigh,iloc,cnt(ii),icnt

    cnt(ii) = icnt

    if (icnt > 1) then
      inc = 0
      iend = min(ii+icnt-1,nrows)
      do kk=ii+1,iend
         cnt(kk) = cnt(kk-1) - 1
         inc = inc + 1
      enddo
    else
      inc = 1
    endif
    inc = max(inc,1)

    ii = ii + inc
  enddo

!  write(0,*)'After sweep#',isweep,' : column#',j,toler
!  do kk=1,nrows
!    if (cnt(kk) > 0) then
!      write(0,'(2i10,2x,g13.7,2x,i10)') &
!       &       kk, index(kk), a(index(kk),j), cnt(kk)
!      if (cnt(kk) == 1 .and. kk < nrows) then
!        write(0,'(2i10,2x,g13.7,2x,i10," ***")') &
!         &       kk+1, index(kk+1), a(index(kk+1),j), cnt(kk+1)
!      endif
!    endif
!  enddo

  isweep = isweep + 1
enddo

do ii=1,nrows
  i = index(ii)
  icnt = cnt(ii)
  if (dupl_with(i) == 0 .and. icnt > 1) then
    nhigh = min(ii+icnt-1,nrows)
    do kk=ii+1,nhigh
      k = index(kk)
      dupl_with(k) = i
    enddo
  endif
enddo

ndupl = count(dupl_with(:) > 0)

!      write(0,*) trim(GEN_DUPL_NAME)//': No. of duplicates found =',ndupl
!      write(0,'((4i10))') &
!     &     (kk, index(kk), cnt(kk), dupl_with(index(kk)), &
!     &     kk=1,min(nrows,50))


9999 continue
!write(0,*) trim(GEN_DUPL_NAME)//': No. of duplicates found =',ndupl

if (allocated(atemp)) then
  deallocate(atemp)
!!  write(0,*)'atemp: deallocated'
endif

if (db_trace) then
  info(1) = nrows
  info(2) = nrows - ndupl
  info(3) = ndupl
  CALL cODB_trace(-1, 0, &
       GEN_DUPL_NAME,&
       info, 3)
endif

END FUNCTION



FUNCTION GEN_GROUP(&
    &a,&
    &idx, off, cnt, grplist) RESULT(ngrp)
implicit none
GEN_TYPE, intent(in) :: a(:)
INTEGER(KIND=JPIM), intent(in) , optional :: idx(:)
INTEGER(KIND=JPIM), intent(out), optional :: off(:), cnt(:)
GEN_TYPE,intent(out), optional :: grplist(:)
INTEGER(KIND=JPIM) :: j, jj, n, k, nminsz, iret
INTEGER(KIND=JPIM), allocatable :: nidx(:)
GEN_TYPE refval
INTEGER(KIND=JPIM) ngrp

ngrp = 0
n = size(a)
if (present(idx)) n = min(n,size(idx))
k = min(1,n)

if (k <= 0) return

allocate(nidx(n))
if (present(idx)) then
  nidx(1:n) = idx(1:n)
else
  do j=1,n
    nidx(j) = j
  enddo
endif

k = 1
jj = 1
j = nidx(jj)
refval = a(j)
do jj=2,n
  j = nidx(jj)
  if (a(j) /= refval) then
    refval = a(j)
    k = k + 1
  endif
enddo

ngrp = k

if (present(off) .and. present(cnt)) then
  if (present(grplist)) then
    nminsz = min(size(off), size(cnt), size(grplist))
  else
    nminsz = min(size(off), size(cnt))
  endif
  if (nminsz < ngrp) then
!--   An error
    ngrp = -ngrp 
    goto 99
  endif

  k = 1
  jj = 1
  j = nidx(jj)
  refval = a(j)
  off(k) = 0
  do jj=2,n
    j = nidx(jj)
    if (a(j) /= refval) then
      refval = a(j)
      k = k + 1
      off(k) = jj-1
    endif
  enddo

  do k=1,ngrp-1
    cnt(k) = off(k+1) - off(k)
  enddo
  cnt(ngrp) = n - off(ngrp)

  if (present(grplist)) then
    do k=1,ngrp
      grplist(k) = a(nidx(off(k)+1))
    enddo
  endif
endif

99   continue
if (allocated(nidx)) deallocate(nidx)
END FUNCTION


FUNCTION GEN_AGGR(&
                  oper, a, nrows, target, &
                  idx, &
                  group_by, &
                  grplist, result) RESULT(ngrp)

implicit none
CHARACTER(len=*), intent(in) :: oper
INTEGER(KIND=JPIM), intent(in) :: nrows, target
GEN_TYPE, intent(inout) :: a(:,:)  ! Not actually updated
INTEGER(KIND=JPIM), intent(in) , optional :: group_by(:), idx(:)
GEN_TYPE,intent(out), optional :: grplist(:,:)
REAL(KIND=JPRB),  intent(out), optional :: result(:)
INTEGER(KIND=JPIM), allocatable :: nidx(:)
INTEGER(KIND=JPIM) n, ngrp, ncols, rc, j, nkeys, i, i1, ndupl
logical LL_count, LL_sum, LL_avg, LL_max, LL_min
logical LL_absmax, LL_absmin
INTEGER(KIND=JPIM), allocatable :: off(:), cnt(:), mkeys(:), dupl_with(:)


ngrp = 0
n = min(nrows,size(a,dim=1))
if (n <= 0) goto 99

LL_count  = (oper == 'COUNT'  .or. oper == 'count'  )
LL_sum    = (oper == 'SUM'    .or. oper == 'sum'    )
LL_avg    = (oper == 'AVG'    .or. oper == 'avg'    )
LL_max    = (oper == 'MAX'    .or. oper == 'max'    )
LL_min    = (oper == 'MIN'    .or. oper == 'min'    )
LL_absmax = (oper == 'ABSMAX' .or. oper == 'absmax' )
LL_absmin = (oper == 'ABSMIN' .or. oper == 'absmin' )

if (.not. LL_count   .and. &
    .not. LL_sum     .and. .not. LL_avg    .and. &
    .not. LL_max     .and. .not. LL_min    .and. &
    .not. LL_absmax  .and. .not. LL_absmin &
   ) goto 99

ncols = size(a,dim=2)
if (target <= 0 .or. target > ncols) goto 99

if (present(group_by)) then
  if (any(abs(group_by(:)) > ncols)) goto 99
  if (any(abs(group_by(:)) ==    0)) goto 99
  nkeys = size(group_by)
  if (present(grplist)) then
    if (size(grplist,dim=2) > nkeys) goto 99
  endif
  allocate(mkeys(group_by(nkeys)))
  mkeys(:) = group_by(:)
else
  nkeys=0
endif

if (nkeys > 0) then
  allocate(nidx(n))
  if (present(idx)) then
    nidx(1:n) = idx(1:n)
  else
    do j=1,n
      nidx(j) = j
    enddo
  endif
  CALL keysort(rc, a, n, &
       multikey=mkeys, &
       index=nidx, init=.FALSE.)
!  ngrp = ODB_groupify(a(1:n,group_by), nidx)
  mkeys(:) = abs(group_by(:))
  allocate(dupl_with(n))
  ndupl = GEN_DUPL0(&
    &a, n, ncols, nkeys,&
    &mkeys, dupl_with, idx=nidx)
  ngrp = n - ndupl
else
  ngrp = 1
endif

if (ngrp <= 0) goto 99

if (present(result)) then
  if (size(result) < ngrp) then
!-- An error
    ngrp = -ngrp
    goto 99
  endif
endif

if (nkeys > 0) then
  allocate(off(ngrp))
  allocate(cnt(ngrp))
!  ngrp = ODB_groupify(a(1:n,group_by), nidx, off, cnt, grplist)
!  if (ngrp <= 0) goto 99
  i1 = 1 ! The first one is by definition not a duplicate
  do j=1,ngrp
    if (present(grplist)) then
      grplist(j,1:nkeys) = a(nidx(i1),mkeys(1:nkeys))
    endif
    off(j) = i1-1
    cnt(j) = 1
    do i=i1+1,n
      if (dupl_with(i) == 0) then
        i1 = i
        exit
      endif
      cnt(j) = cnt(j) + 1
    enddo
  enddo
  if (present(result)) then
    do j=1,ngrp
      if (LL_count) then
        result(j) = cnt(j)
      else if (LL_sum .or. LL_avg) then
        result(j) = sum(a(nidx(off(j)+1:off(j)+cnt(j)),target))
        if (LL_avg) result(j) = result(j)/cnt(j)
      else if (LL_max) then
        result(j) = maxval(a(nidx(off(j)+1:off(j)+cnt(j)),target))
      else if (LL_min) then
        result(j) = minval(a(nidx(off(j)+1:off(j)+cnt(j)),target))
      else if (LL_absmax) then
        result(j) = maxval(abs(a(nidx(off(j)+1:off(j)+cnt(j)),target)))
      else if (LL_absmin) then
        result(j) = minval(abs(a(nidx(off(j)+1:off(j)+cnt(j)),target)))
      endif
    enddo
  endif
else
  if (present(result)) then
    ngrp = 1
    j = 1
    if (LL_count) then
      result(j) = n
    else if (LL_sum .or. LL_avg) then
      result(j) = sum(a(1:n,target))
      if (LL_avg) result(j) = result(j)/n
    else if (LL_max) then
      result(j) = maxval(a(1:n,target))
    else if (LL_min) then
      result(j) = minval(a(1:n,target))
    else if (LL_absmax) then
      result(j) = maxval(abs(a(1:n,target)))
    else if (LL_absmin) then
      result(j) = minval(abs(a(1:n,target)))
    endif
  endif
endif

99   continue
if (allocated(nidx))  deallocate(nidx)
if (allocated(off))   deallocate(off)
if (allocated(cnt))   deallocate(cnt)
if (allocated(mkeys)) deallocate(mkeys)
if (allocated(dupl_with)) deallocate(dupl_with)

END FUNCTION

#ifndef NO_UNDEF
#undef GEN_TYPE
#undef GEN_TOLSEARCH
#undef GEN_TOLSEARCH_NAME
#undef GEN_BINSEARCH
#undef GEN_BINSEARCH_NAME
#undef GEN_DUPL0
#undef GEN_DUPL0_NAME
#undef GEN_UI_RATIO
#undef GEN_DUPL
#undef GEN_DUPL_NAME
#undef GEN_GROUP
#undef GEN_GROUP_NAME
#undef GEN_UNIQUE_FUNC
#undef GEN_AGGR
#undef GEN_AGGR_NAME
#endif

#endif
