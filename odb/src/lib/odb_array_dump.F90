SUBROUTINE odb_darray_dump(khandle, dump_name, viewname, colname, colflag, mdbname, ncolname, &
 & iu, d, nra, nrows, ncols, ncol_start) 

USE PARKIND1  ,ONLY : JPIM     ,JPRB     ,JPIB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK

#ifdef NAG
use f90_unix_io, only: flush
#endif

use odbshared

implicit none

INTEGER(KIND=JPIM), intent(in) :: khandle, ncolname, iu, nra, nrows, ncols, ncol_start
character(len=*), intent(in) :: dump_name, viewname, colname(ncolname), mdbname(ncolname)
logical, intent(in)   :: colflag(ncolname)
REAL(KIND=JPRB), intent(in)   :: d(nra, ncol_start:ncols)
INTEGER(KIND=JPIM), parameter :: jinc = 100
logical :: LLoutput, LLstatid, LLsame
INTEGER(KIND=JPIM) :: jrow, jcol, maxrows, j1, j2
INTEGER(KIND=JPIM), allocatable :: rownum(:), poolno(:)
INTEGER(KIND=JPIB), external :: loc_addr
REAL(KIND=JPRB) :: refval
REAL(KIND=JPRB) ::    dbuf(jinc)
INTEGER(KIND=JPIM) :: ibuf(jinc * 2)
REAL(KIND=JPRB) :: ZHOOK_HANDLE
EQUIVALENCE (dbuf,ibuf)  ! Yeah, I know this is bad, Mats, but it is just for STATID diagnostix !

IF (LHOOK) CALL DR_HOOK('ODB_DARRAY_DUMP',0,ZHOOK_HANDLE)
if (dump_count > max_dump_count .AND. LHOOK) CALL DR_HOOK('ODB_DARRAY_DUMP',1,ZHOOK_HANDLE)
if (dump_count > max_dump_count) return
dump_count = dump_count + 1

maxrows = min(nrows,nra)

write(iu,'(1x,a,z16.16)')&
 & 'ODB_DARRAY_DUMP of '//trim(dump_name)//' for view='//trim(viewname)//' at addr=0x',loc_addr(d) 
write(iu,*)'nra, nrows, ncols, ncol_start, maxrows, ncolname=',&
 & nra, nrows, ncols, ncol_start, maxrows, ncolname

if (maxrows > 0) then
  do jcol=ncol_start,ncols
    if (jcol == 0) then ! hexdump only
      allocate(rownum(maxrows))
      allocate(poolno(maxrows))
      write(iu,*)'column#',jcol,' (control column) ; values are : the col#0-value, poolwise-rownum & poolno'
      CALL cODB_get_rownum(d(1,0), 0, maxrows, 0, 0, rownum)
      CALL cODB_get_poolnos(d(1,0), 0, maxrows, 0, poolno)
      j2 = 0
      LOOPZ: do
        j1 = j2+1
        j2 = min(j2+jinc,maxrows)
        if (j2 < j1) exit LOOPZ
        write(iu,&
         & '(1x,i7," - ",i7,5(:,1x,i12,1x,i7,1x,i3),:,/,(:,18x,5(:,1x,i12,1x,i7,1x,i3)))') &
         & j1,j2, (int(d(jrow,jcol)), rownum(jrow), poolno(jrow), jrow=j1,j2) 
        if (j2 >= maxrows) exit LOOPZ
      enddo LOOPZ
    else
      LLoutput = .TRUE.
      LLstatid = .FALSE.
      if (jcol <= ncolname) then
        write(iu,*)'column#',jcol, ' : '//trim(colname(jcol))//' alias '//trim(mdbname(jcol))//&
         & ' for view='//trim(viewname) 
        LLstatid = (colname(jcol) == 'statid@hdr')
        LLoutput = colflag(jcol)
      else
        write(iu,*)'column#',jcol, ' :  for view='//trim(viewname)
      endif
      if (LLoutput) then
        j2 = 0
        LOOP: do
          j1 = j2+1
          j2 = min(j2+jinc,maxrows)
          if (j2 < j1) exit LOOP
          refval = d(j1,jcol)
          LLsame = ALL(d(j1:j2,jcol) == refval)
          if (LLstatid) then
            if (LLsame) then
              dbuf(1) = refval
              write(iu,'(1x,i7," - ",i7,1x,a8,2(1x,i11),1x,a)') &
               & j1,j2, refval, ibuf(1), ibuf(2), '(all values are the same)' 
            else
              do jrow=j1,j2
                dbuf(jrow-j1+1) = d(jrow,jcol)
              enddo
              write(iu,'(1x,i7," - ",i7,5(:,1x,a8,2(1x,i11)),:,/,(:,18x,5(:,1x,a8,2(1x,i11))))') &
               & j1,j2, (d(jrow,jcol), ibuf(2*(jrow-j1+1)-1), ibuf(2*(jrow-j1+1)), jrow=j1,j2) 
            endif
          else
            if (LLsame) then
              write(iu,'(1x,i7," - ",i7,1p,1x,g19.10,1x,a)') &
               & j1,j2, refval, '(all values are the same)' 
            else
              write(iu,'(1x,i7," - ",i7,1p,5(:,1x,g19.10),:,/,(:,18x,5(:,1x,g19.10)))') &
               & j1,j2, (d(jrow,jcol), jrow=j1,j2) 
            endif
          endif
          if (j2 >= maxrows) exit LOOP 
        enddo LOOP
      endif ! if (LLoutput) then
    endif
    call flush(iu)
  enddo
endif

if (allocated(rownum)) deallocate(rownum)
if (allocated(poolno)) deallocate(poolno)

write(iu,*)'End of ODB_DARRAY_DUMP of '//trim(dump_name)//' for view='//trim(viewname)
call flush(iu)
IF (LHOOK) CALL DR_HOOK('ODB_DARRAY_DUMP',1,ZHOOK_HANDLE)

END SUBROUTINE odb_darray_dump
