program create_index
USE PARKIND1  ,ONLY : JPIM     ,JPRB
use odb_module
use mpl_module
implicit none
INTEGER(KIND=JPIM) :: h, npools, j, jp, rc, idxtype, nsets, len_bitmap
INTEGER(KIND=JPIM) :: nra, nrows, ncols, nc, numargs, jj, k, ktot
INTEGER(KIND=JPIM) :: nproc, myproc, bailout
INTEGER(KIND=JPIM), parameter :: iparam_end = 5
character(len=64) dbname, idxname, table, arg, clratio
character(len=1) cactive(2)
character(len=64), allocatable :: cols(:)
REAL(KIND=JPRB), allocatable :: a(:,:), aa(:,:), colbuf(:)
REAL(KIND=JPRB) ratio, ratio_limit
INTEGER(KIND=JPIM), allocatable :: colidx(:), unique_idx(:), idx(:), bitmap(:), sortidx(:)
logical, allocatable :: colget(:), idxfound(:)
character(len=maxstrlen) colnames

!# Usage for now
!./create_index.x $dbname $idxtype $idxname $table $cols

rc = ODB_init(nproc=nproc,myproc=myproc)

numargs = MPL_iargc()

if (numargs <= iparam_end) then
  call odb_abort('create_index(MAIN)',&
               & 'Usage: ./create_index.x $dbname $idxtype $idxname $table $cols')
  call exit(1)
endif

call MPL_getarg(1,dbname)
write(0,*)'dbname='//trim(dbname)
call MPL_getarg(2,arg) ; read(arg,'(i20)') idxtype
write(0,*)'idxtype=',idxtype
cactive(:) = ' '
if (idxtype >= 1 .and. idxtype <= 2) cactive(idxtype) = '*'
call MPL_getarg(3,idxname)
write(0,*)'idxname='//trim(idxname)
call MPL_getarg(4,table)
if (table(1:1) /= '@') table = '@'//trim(table)
write(0,*)'table='//trim(table)
call MPL_getarg(5,clratio)
read(clratio,'(f20.0)') ratio_limit
write(0,'(1x,a,f8.2,a)') 'ratio=',ratio_limit,'%'

nc = numargs - iparam_end
allocate(cols(nc))

CALL codb_strblank(colnames)
do j=1,nc
  call MPL_getarg(iparam_end+j,cols(j))
  if (j == 1) then
    colnames = trim(cols(j))
  else
    colnames = trim(colnames)//','//trim(cols(j))
  endif
enddo

npools = 0
h = ODB_open(dbname,'READONLY',npools)

!-- check that specified columns really exist on the given table
allocate(colidx(nc))
colidx(:) = 0
rc = ODB_varindex(h, table, cols, colidx)
if (any(colidx(:) == 0)) then
  call odb_abort('create_index(MAIN)',&
                &'One or more columns in "'//&
                &trim(colnames)//'" does not belong to table "'//trim(table(2:))//'"')
  call exit(2)
endif

if (nc > 1) then
!-- Reorder columns w.r.t. colidx(:)
  allocate(sortidx(nc))
  call keysort(rc, colidx, nc, index=sortidx, init=.TRUE.)
  write(0,*) '         colidx(:)=',colidx(:)
  write(0,*) '        sortidx(:)=',sortidx(:)
  write(0,*) 'colidx(sortidx(:))=',colidx(sortidx(:))
  cols(:) = cols(sortidx(:))
  colidx(:) = colidx(sortidx(:))
  deallocate(sortidx)

  CALL codb_strblank(colnames)
  do j=1,nc
    if (j == 1) then
      colnames = trim(cols(j))
    else
      colnames = trim(colnames)//','//trim(cols(j))
    endif
  enddo
endif

write(0,*)'> colnames="'//trim(colnames)//'"'
do j=1,nc
  write(0,'(a,i5,a,i5)')' col#',j,' = '//trim(cols(j))//', colidx#',colidx(j)
enddo

ncols = ODB_getnames(h, table, 'colname')
allocate(colget(ncols))
colget(:) = .FALSE.
colget(colidx(:)) = .TRUE.

allocate(colbuf(nc))
do jp=myproc,npools,nproc
  rc = ODB_select(h, table, nrows, ncols, nra=nra, poolno=jp)
  if (nrows > 0) then
    write(0,*)'*** Poolno=',jp,', nrows=',nrows
    allocate(a(nra,0:ncols))
    rc = ODB_get(h, table, a, nrows, ncols, colget=colget, poolno=jp)
    !do j=1,min(5,nrows)
    !  write(0,'(i10,1p,(4(g20.10)))') j,a(j,colidx(:))
    !enddo
    !write(0,*)'etc...'
    allocate(aa(nra,1:nc))
    do j=1,nc
      aa(1:nrows,j) = a(1:nrows,colidx(j))
    enddo
    deallocate(a)
    allocate(unique_idx(nrows))
    unique_idx(:) = -1
    bailout = nint((ratio_limit * nrows)/100.0d0) + 10
    call codb_cardinality(nc, nrows, nra, aa, nsets, unique_idx, nrows, bailout)
    ratio = (nsets * 100.0d0)/nrows
    write(0,'(a,i10,a,i10,a,i10,a,f8.2,a)') &
         & ' > Cardinality: nsets=',nsets,' of nrows=',nrows,', bailout=',bailout,' : ratio=',ratio,'%'
    write(0,*)'> Space estimates: poolno=',jp
    write(0,*) cactive(1)//'idxtype=1 (unique index): ',nsets+nrows,' [32-bit words]'
    len_bitmap = (nrows+32-1)/32
    write(0,*) cactive(2)//'idxtype=2 (bitmap index): ',nsets*len_bitmap,' [32-bit words]'
    if (ratio > ratio_limit) then
      write(0,*)'***Warning: Abandoning index creation; Ratio too high (> '//trim(clratio)//'%)'
      goto 100
    else if (nrows == 0) then
      write(0,*)'***Warning: Abandoning index creation; Bailout reached'
      goto 100
    endif
    do j=1,nsets
      write(0,'(i10,1p,(4(g20.10)))') unique_idx(j),aa(unique_idx(j),:)
    enddo
    if (idxtype == 2) then ! type=BITMAP
      allocate(bitmap(len_bitmap))
    endif
    allocate(idx(nrows))
    allocate(idxfound(nrows))
    idxfound(:) = .FALSE.
    write(0,*) 'indices ---> idxtype=',idxtype
    ktot = 0
    do j=1,nsets
      colbuf(:) = aa(unique_idx(j),:)
      k = 0
      do jj=1,nrows
        if (idxfound(jj)) cycle
        if (all(aa(jj,:) == colbuf(:))) then
          k = k + 1
          idx(k) = jj-1 ! C-indexing
          idxfound(jj) = .TRUE.
        endif
      enddo
      ktot = ktot + k
      write(0,'(i10,a)') -k,' WHERE'
      write(0,'(1p,(3((a," == ",g20.10,:," AND "))))') (trim(cols(jj)), colbuf(jj), jj=1,nc)
      write(0,'(8i10)',advance='no') -ktot, idx(1:min(7,k))
      if (k > 7) then
        write(0,*)'etc...'
      else
        write(0,*)' '
      endif
      if (idxtype == 2) then ! type=BITMAP
        bitmap(:) = 0
        call codbit_setidx(bitmap,nrows,32,idx,k)
        write(0,'(3b32.32)',advance='no') bitmap(1:min(3,len_bitmap))
        if (len_bitmap > 3) then
          write(0,*)'etc...'
        else
          write(0,*)' '
        endif
      endif
    enddo ! do j=1,nsets
    write(0,*) '> nsets, nrows, ktot=',nsets, nrows, ktot
    if (nrows /= ktot) then
      call odb_abort('create_index(MAIN)',&
                    & 'nrows /= ktot')
      call exit(3)
    endif
    if (idxtype == 2) then ! type=BITMAP
      deallocate(bitmap)
    endif
    deallocate(idx)
    deallocate(idxfound)
 100  continue
    deallocate(aa)
    deallocate(unique_idx)
  endif
  rc = ODB_release(h, poolno=jp)
enddo

deallocate(cols)
deallocate(colbuf)
deallocate(colidx)
deallocate(colget)

rc = ODB_close(h)

rc = ODB_end()

end program create_index
