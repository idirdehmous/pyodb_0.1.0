#if REAL_VERSION == 8

#define GEN_TYPE       REAL(KIND=JPRB)
#define GEN_GET        ODB_dget
#define GEN_PUT        ODB_dput
#define GEN_GETNAME   'ODB:ODB_DGET'
#define GEN_PUTNAME   'ODB:ODB_DPUT'
#define GEN_CODB_GET  cODB_dget
#define GEN_CODB_PUT  cODB_dput
#define GEN_INC        2
#define GEN_CTRLW_SORT ODB_dctrlw_sort
#define GEN_CTRLW_SORTNAME     'ODB:ODB_DCTRLW_SORT'
#define GEN_CTRLW_SORTNAME_OMP 'ODB:ODB_DCTRLW_SORT>OMPCOPY'
#define GEN_AE_DUMP_DATATYPE (REAL_VERSION * 100)

#else

  ERROR in programming : No datatype given (should never have ended up here)

#endif

#if defined(REAL_VERSION)

FUNCTION GEN_GET(handle, dtname, d, nrows, ncols,&
    &poolno, colget, colpack, colfree, index, sorted, offset,&
    &start, limit, process_select_distinct, inform_progress, using)&
    &RESULT(rc)

INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)  :: dtname
GEN_TYPE, intent(inout)       :: d(:,0:)
INTEGER(KIND=JPIM), intent(inout)        :: nrows
INTEGER(KIND=JPIM), intent(inout), optional :: ncols
INTEGER(KIND=JPIM), intent(in), optional :: poolno, offset, start, limit, using
logical, intent(in), optional :: colget(:), colpack(:), colfree(:)
INTEGER(KIND=JPIM), intent(out),optional :: index(:)
     logical, intent(in), optional :: sorted, process_select_distinct, inform_progress
INTEGER(KIND=JPIM) :: rc, ipoolno
INTEGER(KIND=JPIM) :: i, j, nra, jcol
INTEGER(KIND=JPIM) :: nrows_in, ncols_out, doffset, info(4)
INTEGER(KIND=JPIM) :: nrows_out, ireplicate_PE
INTEGER(KIND=JPIM) :: ldimd, nlastrow, targetPE
INTEGER(KIND=JPIM) :: ndupl
INTEGER(KIND=JPIM) :: jj, ngrp, maxlen, iphase_id, using_it, ii
INTEGER(KIND=JPIM), allocatable :: iflag(:)
logical  , allocatable :: takethis(:)
INTEGER(KIND=JPIM) :: npes, nrows_in_arr(1), i_inform_progress
INTEGER(KIND=JPIM) :: idummy, idummy_arr(0), inumabs, icnt
logical is_table, has_index, has_index_abs, want_sorted, has_anyflp, is_replicated
logical has_select_distinct, LLsync, LLmpex1debug
logical has_aggrfuncs, LLall_aggr, LLchanged, LLinform_progress
character(len=1) env
INTEGER(KIND=JPIM) :: nkey, maxbits, vhandle, imaxcol, maxcols
INTEGER(KIND=JPIM), allocatable :: mkeys(:)
INTEGER(KIND=JPIM), allocatable :: index_abs(:), absmode(:)
INTEGER(KIND=JPIM), allocatable :: nrowvec(:)
INTEGER(KIND=JPIM), allocatable :: index_aggr(:), grpsta(:)
GEN_TYPE, allocatable :: dtmp(:,:), dtmpabs(:,:), zbuf(:)
GEN_TYPE, allocatable :: tol(:)
GEN_TYPE, allocatable :: dres(:,:)
GEN_TYPE tmpval(2)
INTEGER(KIND=JPIM), allocatable :: colidx(:), dupl_with(:), dupl_index(:), k(:)
INTEGER(KIND=JPIM), allocatable :: iaggrfuncflag(:)
INTEGER(KIND=JPIM) :: opponent(ODBMP_NPROC+1)
INTEGER(KIND=JPIM) :: joppo, noppo, loopcnt, jloop, ndata
INTEGER(KIND=JPIM) :: ikey, ndtypes, istart, ilimit, iaggrcols, icols_aux, i2nd, icols
logical, allocatable :: LLswap(:), LLmask(:)
character(len=maxvarlen), allocatable :: CLdtypes(:)
INTEGER(KIND=JPIM) :: fast_physproc, xfast
REAL(KIND=JPRB) :: ZHOOK_HANDLE
fast_physproc(xfast) = mod(xfast-1,ODBMP_nproc)+1
IF (LHOOK) CALL DR_HOOK(GEN_GETNAME,0,ZHOOK_HANDLE)

has_aggrfuncs = .FALSE.
iaggrcols = 0
has_select_distinct = .FALSE.
has_index_abs = .FALSE.
has_index = present(index)

is_table = .FALSE.
if (len(dtname) >= 1) then
  is_table = (dtname(1:1) == '@')
endif

rc = 0
ldimd = size(d, dim=1)
ncols_out = size(d, dim=2) - 1
if (present(ncols)) then
  ncols_out = min(ncols,ncols_out)
endif
icols_aux = 0
icols = 0
nrows_in = nrows
nrows_out = 0
ireplicate_PE = 0
if (present(offset)) then
  doffset = offset ! No checking by the way
else
  doffset = 0
endif

!-- starting row (from database query/table point of view)
if (present(start)) then
  istart = max(1,start)
else
  istart = 1
endif
istart = istart - 1 ! In terms of C-indexing

!-- how many rows to return (to the array d(:,0:) itself)
if (present(limit)) then
  ilimit = max(0,min(limit, nrows_in))
else
  ilimit = nrows_in
endif

if (ldimd < nrows_in) then
  rc = ldimd - nrows_in
  CALL ODB_abort(&
       &GEN_GETNAME,&
       &'The actual no. of rows is less than specified in TABLE/VIEW="'//&
       &trim(dtname)//'"',&
       &rc)
  goto 99999
endif

want_sorted = .TRUE.
if (present(sorted)) want_sorted = sorted

LLinform_progress=.FALSE.
i_inform_progress=0
if (present(inform_progress)) then
  LLinform_progress=inform_progress
  if (LLinform_progress) i_inform_progress=1
endif
if (ODBMP_nproc > 1) then
  LLinform_progress=.FALSE. ! Otherwise messy output
  i_inform_progress=0
endif

using_it = 0
if (present(using)) using_it = using

if (db_trace) then
  info(1) = ldimd
  info(2) = nrows_in
  info(3) = ncols_out
  info(4) = using_it
  CALL cODB_trace(handle, 1,&
       & GEN_GETNAME//':'//dtname, info, size(info))
endif

if (odbHcheck(handle, GEN_GETNAME)) then
  allocate(iflag(ncols_out))

  if (present(colget) .or. present(colpack) .or. present(colfree)) then
    iflag(:) = 0
  else
    iflag(:) = def_flag
  endif

  if (present(colget)) then
    do i=1,min(ncols_out,size(colget))
      if (colget(i)) iflag(i) = iflag(i) + flag_get
    enddo
  endif

  if (present(colpack)) then
    do i=1,min(ncols_out,size(colpack))
      if (colpack(i)) iflag(i) = iflag(i) + flag_pack
    enddo
  endif

  if (ODB_io_method(handle) == 5) then
    !-- With I/O-method#5 meant for read/only we prefer deallocating column space
    !   *IMMEDIATELY* a column in concern has been fetch'ed
    !   The drawback is that subsequent references imply re-read of data i.e. seek&read I/O
    if (present(colfree)) then
      do i=1,min(ncols_out,size(colfree))
        if (colfree(i)) iflag(i) = iflag(i) + flag_free
      enddo
    else
      do i=1,ncols_out
        iflag(i) = iflag(i) + flag_free
      enddo
    endif
  endif

  allocate(takethis(0:ncols_out))
  takethis(0) = .TRUE.
  do i=1,ncols_out
    takethis(i) = (IAND(iflag(i),flag_get) == flag_get)
  enddo

  ipoolno = get_poolno(handle, poolno)

  if (is_table) then
    has_aggrfuncs = .FALSE.
    has_select_distinct = .FALSE.
    npes = 0
  else
    has_aggrfuncs = ODB_has_aggrfuncs(handle, dtname, ncols=iaggrcols, using=using)
    has_select_distinct = ODB_has_select_distinct(handle, dtname)
    vhandle = ODB_gethandle(handle, dtname, using=using)
    CALL cODB_restore_peinfo(vhandle, ireplicate_PE, npes, using_it)
  endif

  if (has_select_distinct .OR. has_aggrfuncs) then 
    ! do NOT allow the use of (start,limit) -feature with 'SELECT DISTINCT'
    ! do NOT allow the use of (start,limit) -feature when aggregate functions are involved
    istart = 0
    ilimit = nrows_in
  endif

  is_replicated = (ireplicate_PE /= 0)

  if (npes == 0) then
    if (nrows_in > 0 .and. ilimit > 0) then
      if (doffset == 0) CALL init_region(d, size(d) * ODB_SIZEOF_INT * GEN_INC, UNDEFDB_actual)
      CALL & 
        GEN_CODB_GET &
            &(handle, ipoolno,&
            &ctrim(dtname), d(1+doffset,0), 0, ldimd,&
            &ilimit, ncols_out, &
            &iflag, -1,&
            &istart, ilimit,&
            &i_inform_progress, using_it, rc)
#if 0
      CALL cODB_ae_dump(handle, 1, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                        ldimd, nrows_in, ncols_out, d(1,0))
#endif
    endif

    if (rc < 0) then
      CALL ODB_abort(&
             &GEN_GETNAME,&
             &'Cannot get data for VIEW/TABLE="'//&
             &trim(dtname)//'"',&
             &rc)
      goto 99999
    endif

    nrows_out = rc
    nrows = rc
  else

    if (is_replicated) then
      nrows_in_arr(1) = 0
      CALL cODB_get_rowvec(handle, ipoolno,&
            &ctrim(dtname),&
            &nrows_in_arr, 1,&
            &rc, using_it)
      nrows_in = nrows_in_arr(1)
      npes = ODBMP_nproc
      allocate(nrowvec(npes))
      nrowvec(:) = 0
      nrowvec(ODBMP_myproc) = nrows_in
      CALL ODBMP_global('SUM', nrowvec)
    else
      allocate(nrowvec(npes))
      CALL cODB_get_rowvec(handle, ipoolno,&
            &ctrim(dtname),&
            &nrowvec, npes,&
            &rc, using_it)
    endif

    nrows_in = maxval(nrowvec(:))
    nra = ODB_lda(nrows_in)
    allocate(dtmp(nra,0:ncols_out))

    !-- do only the necessary initializations
    dtmp(:,0) = 0 ! control-word

    nlastrow = 0

    if (is_replicated) then
      istart = 0
      ilimit = nrowvec(ODBMP_myproc)
      if (ilimit > 0) then
        CALL &
         GEN_CODB_GET &
             &(handle, ipoolno,&
             &ctrim(dtname), dtmp, 0, nra,&
             &nrowvec(ODBMP_myproc), ncols_out, &
             &iflag, 1,&
             &istart, ilimit,&
             &i_inform_progress, using_it, rc)

#if 0
        CALL cODB_ae_dump(handle, 2, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                          nra, nrowvec(ODBMP_myproc), ncols_out, dtmp)
#endif
      else
        rc = ilimit
      endif

      if (rc /= ilimit) then
        CALL ODB_abort(&
                &GEN_GETNAME,&
                &'Cannot get local replicated data for VIEW="'//&
                &trim(dtname)//'"',&
                &ipoolno)
        rc = -2
        goto 99999
      endif

      if (nrows_in > 0) CALL ODBMP_exchange2(d,nlastrow,dtmp,nrowvec,ncols_out,takethis)
    else
      !*** Note: npes is a kind of no. of pools over all procs

      CALL cODB_getenv('ODB_MPEXCHANGE1_DEBUG', env)
      LLmpex1debug = (env == '1')

      loopcnt = (npes + ODBMP_nproc - 1)/ODBMP_nproc
      noppo = ODBMP_setup_exchange(opponent)

      LLsync = (npes < ODBMP_nproc   .OR. &
                noppo /= ODBMP_nproc .OR. &
                mod(npes,ODBMP_nproc) /= 0)

      if (LLmpex1debug) then
        write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': ODBMP_nproc, ODBMP_myproc, npes, loopcnt, noppo, LLsync=', &
                                                     ODBMP_nproc, ODBMP_myproc, npes, loopcnt, noppo, LLsync
        write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': view='//ctrim(dtname)
        write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': opponent(:)=',opponent(:)
        write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': size(nrowvec), nrowvec(:)=',size(nrowvec), nrowvec(:)
        write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': size & shape of d   =',size(d)   , shape(d)
        write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': size & shape of dtmp=',size(dtmp), shape(dtmp)
        call flush(6)
      endif

      CALL ODBMP_sync(where=0)
      do jloop=1,loopcnt
        if (LLmpex1debug) then
          write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': >jloop=',jloop
          call flush(6)
        endif
        do joppo=1,noppo
          targetPE = opponent(joppo)
          if (LLmpex1debug) then
            write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': >>>joppo, targetPE=',joppo,targetPE
            call flush(6)
          endif
          if (targetPE >= 1 .and. targetPE <= ODBMP_nproc) then
            j = targetPE + (jloop-1)*ODBMP_nproc !*** Pools j assigned in a round-robin fashion
            if (LLmpex1debug) then
              write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': j, npes=',j,npes
              call flush(6)
            endif
            if (j <= npes) then
              if (LLmpex1debug) then
                write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': j, nrowvec(j)=',j, nrowvec(j)
                call flush(6)
              endif
              istart = 0
              ilimit = nrowvec(j)
              if (ilimit > 0) then
                CALL &
                 GEN_CODB_GET &
                 &(handle, ipoolno,&
                 &ctrim(dtname), dtmp, 0, nra,&
                 &nrowvec(j), ncols_out, &
                 &iflag, j,&
                 &istart, ilimit,&
                 &i_inform_progress, using_it, rc)

#if 0
                CALL cODB_ae_dump(handle, 3, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                                  nra, nrowvec(j), ncols_out, dtmp)
#endif
              else
                rc = ilimit
              endif

              if (rc /= ilimit) then
                CALL ODB_abort(&
                     &GEN_GETNAME,&
                     &'Cannot get parallel data for VIEW="'//&
                     &trim(dtname)//'"',&
                     &j)
                rc = -2
                goto 99999
              endif
              ndata = nrowvec(j)
            else
              ndata = 0
            endif ! if (j <= npes) then ... else ..
            if (LLmpex1debug) then
              write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': ==> j, nlastrow, ndata=',j, nlastrow, ndata
              call flush(6)
            endif
            CALL ODBMP_exchange1(targetPE,&
                                 d,nlastrow,&
                                 dtmp,ndata,ncols_out,takethis,LLmpex1debug)
            if (LLmpex1debug) then
              write(6,*) ODBMP_myproc,';'//GEN_GETNAME//': <== j, nlastrow, ndata=',j, nlastrow, ndata
              call flush(6)
            endif
          endif ! if (targetPE >= 1 .and. targetPE <= ODBMP_nproc) then ...
          if (LLsync) CALL ODBMP_sync(where=joppo)
        enddo ! do joppo=1,noppo
        if (.not.LLsync) CALL ODBMP_sync(where=jloop)
      enddo ! do jloop=1,loopcnt
    endif

    deallocate(dtmp)
    deallocate(nrowvec)

    nrows_out = nlastrow

    rc = nlastrow

    if (nlastrow /= nrows) then
      rc = nrows - nlastrow
      CALL ODB_abort(&
             &GEN_GETNAME, &
             &'Unable to get ALL parallel data in VIEW="'//&
             &trim(dtname)//'"',&
             &rc)
      rc = -3
      goto 99999
    endif ! if (is_replicated) then ... else ...

#if 0
    CALL cODB_ae_dump(handle, 4, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                      ldimd, nrows_out, ncols_out, d)
#endif

    call ODB_ctrlw_sort(d, nrows_out, takethis)

#if 0
    CALL cODB_ae_dump(handle, 5, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                      ldimd, nrows_out, ncols_out, d)
#endif

  endif ! if (npes == 0) then ... else ...

  if (has_aggrfuncs .AND. nrows_out > 0 .AND. ncols_out > 0) then
    LLall_aggr = (iaggrcols == ncols_out) ! True if all (columns) are aggregate funcs
    allocate(iaggrfuncflag(ncols_out))
    iaggrfuncflag(:) = 0
    rc = ODB_get_aggr_info(handle, dtname, iaggrfuncflag, iphase_id, poolno=ipoolno, using=using)

    allocate(colidx(ncols_out))
    idummy = ODB_getsize(handle, dtname, idummy, icols, poolno=ipoolno, using=using, &
         & colaux=colidx, ncols_aux=icols_aux)

#if 0
    write(0,'(1x,a,7i12)') &
         & 'fodb.h: ipoolno, nrows_out, ncols_out, iaggrcols, icols, icols_aux, iphase_id = ',&
         &          ipoolno, nrows_out, ncols_out, iaggrcols, icols, icols_aux, iphase_id
#endif

    if (ncols_out < icols) then
      CALL ODB_abort(&
           &GEN_GETNAME,&
           &'Too few columns supplied to perform aggregate function calculations in VIEW="'//&
           &trim(dtname)//'"',&
           &ncols_out - icols)
      rc = -6
      goto 99999
    endif

    if (LLall_aggr) then ! Every column is an aggregate function
      do j=1,icols-icols_aux
        if (iphase_id == 1 .or. colidx(j) == j) then
          jj = j
          i2nd = 1
        else
          jj = colidx(j)
          i2nd = 2
        endif
#if 0
        write(0,*) 'fodb.h: j, jj, i2nd, iphase_id =',j, jj, i2nd, iphase_id
#endif
        CALL cODB_calc_aggr(iphase_id, iaggrfuncflag(j), i2nd, tmpval(1), nrows_out, d(1,j), d(1,jj))
        jj = j
        do ii=1,i2nd
          d(1,jj) = tmpval(ii)
          jj = colidx(jj)
        enddo
      enddo
      ngrp = 1
    else ! *not* all columns aggregate funcs --> f.ex. due to binning : SELECT obstype,codetype,avg(obsvalue)
      nkey = icols - icols_aux - iaggrcols
#if 0
      write(0,'(1x,a,5i12)') 'fodb.h: nkey, ncols_out, iaggrcols, icols, icols_aux =',&
           &                          nkey, ncols_out, iaggrcols, icols, icols_aux
#endif
      allocate(mkeys(nkey))
      mkeys(:) = 0
      jj = 0
      do j=1,icols - icols_aux
        if (iaggrfuncflag(j) == 0) then ! i.e. ODB_AGGR_NONE
          jj = jj + 1
          mkeys(jj) = j + 1 ! +1 since array d(:,0:), not d(:,1:) passed to the keysort
        endif
      enddo
#if 0
      write(0,*) 'fodb.h: jj, mkeys(:)=',jj, mkeys(:)
#endif
      allocate(index_aggr(nrows_out))
      CALL keysort(rc, d, nrows_out, &
           &multikey=mkeys(1:nkey), &
           &index=index_aggr(1:nrows_out), init=.TRUE.)
      mkeys(:) = mkeys(:) - 1 ! Now outside the keysort : normal column numbering
!      do i=1,min(100,nrows_out)
!         write(0,*) i,index_aggr(i),d(index_aggr(i),mkeys(1:nkey))
!      enddo
      allocate(grpsta(nrows_out+1))
      ngrp = 1
      jj = 1
      grpsta(ngrp) = jj
      do i=2,nrows_out
        LLchanged = .FALSE.
        do j=1,nkey
          if (d(index_aggr(i),mkeys(j)) /= d(index_aggr(jj),mkeys(j))) then
            LLchanged = .TRUE.
            exit
          endif
        enddo
        if (LLchanged) then
          jj = i
          ngrp = ngrp + 1
          grpsta(ngrp) = jj
        endif
      enddo
      grpsta(ngrp+1) = nrows_out + 1
      deallocate(mkeys)
      maxlen = maxval(grpsta(2:ngrp+1) - grpsta(1:ngrp))
#if 0
      write(0,*) 'fodb.h: ngrp, maxlen, nrows_out=',ngrp, maxlen,nrows_out
      write(0,*) 'fodb.h: grpsta(1:ngrp+1)=',grpsta(1:ngrp+1)
#endif
      maxlen = ODB_lda(maxlen)
      if (iphase_id == 1 .or. icols_aux == 0) then
        i2nd = 1
      else
        i2nd = 2
      endif
#if 0
      write(0,'(1x,a,5i12)') 'fodb.h> icols, icols_aux, i2nd, maxlen, iphase_id=',&
           &                          icols, icols_aux, i2nd, maxlen, iphase_id
#endif
      allocate(dtmp(maxlen,i2nd))
      allocate(dres(ODB_lda(ngrp),icols))
      do i=1,ngrp
        do j=1,icols-icols_aux
          if (iaggrfuncflag(j) > 0) then ! i.e. has aggregate functions
            jj = grpsta(i+1)-grpsta(i)
! bug fixed 21/10/08 AF            dtmp(1:jj,1) = d(index_aggr(grpsta(i):grpsta(i+1)),j)
            dtmp(1:jj,1) = d(index_aggr(grpsta(i):grpsta(i+1) -1),j)
            if (iphase_id == 1 .or. colidx(j) == j) then
              i2nd = 1
            else
              i2nd = 2
              dtmp(1:jj,i2nd) = d(index_aggr(grpsta(i):grpsta(i+1)),colidx(j))
            endif
#if 0
            write(0,'(1x,a,4i12)') 'fodb.h: j, jj, i2nd, iphase_id =',j, jj, i2nd, iphase_id
#endif
            CALL cODB_calc_aggr(iphase_id, iaggrfuncflag(j), i2nd, tmpval(1), jj, dtmp(1,1), dtmp(1,i2nd))
            jj = j
            do ii=1,i2nd
              dres(i,jj) = tmpval(ii)
              jj = colidx(jj)
            enddo
          else
            dres(i,j) = d(index_aggr(grpsta(i)),j)
          endif
        enddo
      enddo
      if (iphase_id == 0) then
        d(1:ngrp,1:icols) = dres(1:ngrp,1:icols)
      else
        d(1:ngrp,1:icols-icols_aux) = dres(1:ngrp,1:icols-icols_aux)
      endif
      deallocate(dtmp)
      deallocate(dres)
      deallocate(index_aggr)
      deallocate(grpsta)
    endif
    deallocate(colidx)
    if (iphase_id == 0) icols_aux = 0 ! To return correct "ncols", if (present(ncols)) [since still in preproc phase]
    rc = ngrp
    nrows_out = ngrp
    nrows = ngrp
    deallocate(iaggrfuncflag)
  endif

  if (present(process_select_distinct)) then
    if (has_select_distinct .and. .NOT.process_select_distinct) has_select_distinct = .FALSE.
  endif

  if (has_select_distinct .AND. &
!-wrong      (ireplicate_PE == ODBMP_myproc .OR. ireplicate_PE == -1) .AND. &
      nrows_out > 0 .AND. ncols_out > 0) then

     if (has_index) then
        if (size(index) < nrows_out) then
           CALL ODB_abort(&
                &GEN_GETNAME,&
                &'Index vector supplied is too short in VIEW="'//&
                &trim(dtname)//'"',&
                &size(index) - nrows_out)
           rc = -4
           goto 99999
        endif
     endif

     allocate(colidx(ncols_out))
     allocate(tol(ncols_out))
     allocate(dupl_with(nrows_out))
     allocate(dupl_index(nrows_out))

     do j=1,ncols_out
       colidx(j) = j
       tol(j) = 0
     enddo

     if (has_index) then
       do i=1,nrows_out
         dupl_index(i) = index(i)
       enddo
     else
       do i=1,nrows_out
         dupl_index(i) = i
       enddo
     endif

     ndupl = ODB_duplchk(d(:,1:), nrows_out, ncols_out, ncols_out, &
                         colidx, tol, dupl_with, idx=dupl_index)

!     write(0,*)'---> ndupl=',ndupl

     if (ndupl > 0) then
       nra = ODB_lda(nrows_out - ndupl)
       allocate(dtmp(nrows_out - ndupl, 0:ncols_out))
       allocate(k(ncols_out))

       do j=1,ncols_out
         k(j) = 0
         do i=1,nrows_out
           if (dupl_with(i) == 0) then
             k(j) = k(j) + 1
             dtmp(k(j),j) = d(i,j) 
           endif
         enddo
       enddo

       ! if (ANY(k(:) != nrows_out - ndupl)) then abort

       deallocate(k)
       nrows_out = nrows_out - ndupl

       do j=1,ncols_out
         do i=1,nrows_out
           d(i,j) = dtmp(i,j)
         enddo
       enddo

       !-- This not supposed to be updatable, so put control word to zero
       !! d(1:nrows_out,0) = 0 ! -- we may want to display the origin (= poolno & rownum) of dada!

       !-- And this is how much valid data we have left in the d-matrix
       nrows = nrows_out

#if 0
       CALL cODB_ae_dump(handle, 6, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                         ldimd, nrows_out, ncols_out, d)
#endif

       deallocate(dtmp)
     endif

     deallocate(colidx)
     deallocate(tol)
     deallocate(dupl_with)
     deallocate(dupl_index)
  endif

  if (want_sorted) then
    nkey = ODB_sortkeys(handle, ctrim(dtname))
  else
    nkey = -1
  endif

  if (nkey > 0 .and. nrows_out > 0 .and. .not. is_table) then
    allocate(mkeys(nkey))

    mkeys(:) = 0
    rc = ODB_sortkeys(handle, ctrim(dtname), mkeys)

    allocate(absmode(nkey))
    absmode(:) = 0
    inumabs = 0
    CALL cODB_maxcols(maxcols)

!    write(0,*)'--> maxcols, nkey   , mkeys(1:nkey)=',maxcols, nkey, mkeys(1:nkey)

!--   Plus one to account the col#0
!     Accumulate only active columns in the range of [1..ncols_out] or [-ncols_out..-1]
    nkey = 0
    do j=1,size(mkeys)
      ikey = abs(mkeys(j))
      if (ikey == 0) cycle
      if (ikey > ncols_out .and. ikey <= maxcols) cycle
      if (ikey > maxcols + ncols_out) cycle

      if (ikey > maxcols) then
        if (mkeys(j) > 0) then
          ikey = mkeys(j) - maxcols + 1 ! ABS-sorting, ascending
          absmode(nkey+1) = 1
        else
          ikey = -(abs(mkeys(j)) - maxcols + 1) ! ABS-sorting, descending
          absmode(nkey+1) = -1
        endif
      else if (mkeys(j) > 0) then
        ikey = mkeys(j) + 1 ! Ascending sort
      else if (mkeys(j) < 0) then
        ikey = mkeys(j) - 1 ! Descending sort
      else
        cycle
      endif

      if (takethis(abs(ikey)-1)) then
        mkeys(nkey+1) = ikey
        if (absmode(nkey+1) /= 0) inumabs = inumabs + 1
        nkey = nkey + 1
      else
        absmode(nkey+1) = 0
      endif
    enddo

!    write(0,*)'->> nkey   , mkeys(1:nkey)=',nkey, mkeys(1:nkey)
!    write(0,*)'->> inumabs, absmode(1:nkey)=',inumabs, absmode(1:nkey)
    
    if (nkey > 0) then

#if defined(LITTLE)
!-- Little endian machines need to byteswap strings i.e. 8-byte holleriths to obtain
!   correct sorting; After sorting bytes must be swapped back
      ndtypes = ODB_getnames(handle, ctrim(dtname), 'datatype') 
      allocate(CLdtypes(ndtypes))
      ndtypes = ODB_getnames(handle, ctrim(dtname), 'datatype', CLdtypes) 
      allocate(LLswap(nkey))
      LLswap(:) = .FALSE.
      do j=1,nkey
        ikey = abs(mkeys(j)) - 1
        if (CLdtypes(ikey) == 'string') then
          LLswap(j) = .TRUE.
          CALL swap8bytes(d(1,ikey), nrows_out)
        endif
      enddo
      deallocate(CLdtypes)
#endif

      if (inumabs > 0) then ! ABS-sorting
        allocate(dtmpabs(nrows_out,inumabs))
        if (has_index) then
           has_index_abs = .FALSE.
        else
           has_index_abs = .TRUE.
           allocate(index_abs(1:nrows_out))
        endif
        jcol = 0
        do j=1,nkey
          if (absmode(j) /= 0) then
            ikey = absmode(j) * mkeys(j) - 1
            jcol = jcol + 1
            do i=1,nrows_out
              dtmpabs(i,jcol) = d(i,ikey)
              if (d(i,ikey) < 0) d(i,ikey) = -d(i,ikey)
            enddo
          endif
        enddo
      endif

      if (has_index) then
        CALL keysort(rc, d, nrows_out, &
               &multikey=mkeys(1:nkey), &
               &index=index(1:nrows_out), init=.TRUE.)
      else if (has_index_abs) then
        CALL keysort(rc, d, nrows_out, &
               &multikey=mkeys(1:nkey), &
               &index=index_abs(1:nrows_out), init=.TRUE.)
      else
        CALL keysort(rc, d, nrows_out, &
               &multikey=mkeys(1:nkey))
      endif

      if (inumabs > 0) then ! ABS-sorting (guaranteed via index-sorting)
        jcol = 0
        do j=1,nkey
          if (absmode(j) /= 0) then
            ikey = absmode(j) * mkeys(j) - 1
            jcol = jcol + 1
            d(1:nrows_out,ikey) = dtmpabs(1:nrows_out,jcol) ! index-sort => order preserved
          endif
        enddo
        deallocate(dtmpabs)
      endif

      if (allocated(index_abs)) then ! need physical reordering
        allocate(zbuf(nrows_out))
        imaxcol = min(size(d,dim=2),size(takethis)) - 1
        do j=0,imaxcol
           if (j > 0 .and. .not. takethis(j)) cycle
           zbuf(1:nrows_out) = d(index_abs(1:nrows_out),j)
           d(1:nrows_out,j) = zbuf(1:nrows_out)
        enddo
        deallocate(index_abs)
        deallocate(zbuf)
      endif

#if defined(LITTLE)
!-- Little endian machines need to byteswap strings i.e. 8-byte holleriths to obtain
!   correct sorting; After sorting bytes must be swapped back
      do j=1,nkey
        ikey = abs(mkeys(j)) - 1
        if (LLswap(j)) CALL swap8bytes(d(1,ikey), nrows_out)
      enddo
      deallocate(LLswap)
#endif

      if (has_index) then
        CALL cODB_ae_dump(handle, 7, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                          ldimd, nrows_out, ncols_out, d)
      else
        CALL cODB_ae_dump(handle, 8, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                          ldimd, nrows_out, ncols_out, d)
      endif

    endif ! if (nkey > 0) then

    deallocate(mkeys)
    deallocate(absmode)
  endif ! if (nkey > 0 .and. nrows_out > 0 .and. .not. is_table) ...

  if (allocated(iflag))    deallocate(iflag)
  if (allocated(takethis)) deallocate(takethis)
endif ! if (odbHcheck(handle, GEN_GETNAME))

if (present(ncols)) then
  if (icols > 0) then
    ncols = icols - icols_aux
  else
    ncols = ncols_out
  endif
endif

if (rc >= 0) then
  rc = nrows_out
endif

CALL cODB_trace(handle, 0,&
    &GEN_GETNAME//':'//dtname, idummy_arr, 0)
99999 continue
IF (LHOOK) CALL DR_HOOK(GEN_GETNAME,1,ZHOOK_HANDLE)
END FUNCTION


FUNCTION GEN_PUT(handle, dtname, d, nrows, ncols,&
    &poolno, colput, colpack, sorted, offset, store_intermed, using) &
    &RESULT(rc)
INTEGER(KIND=JPIM), intent(in)           :: handle
character(len=*), intent(in)  :: dtname
GEN_TYPE, intent(inout)       :: d(:,0:)
INTEGER(KIND=JPIM), intent(in)              :: nrows
INTEGER(KIND=JPIM), intent(inout), optional :: ncols
INTEGER(KIND=JPIM), intent(in), optional :: poolno, offset, using
logical, intent(in), optional :: colput(:), colpack(:)
logical, intent(in), optional :: sorted, store_intermed
INTEGER(KIND=JPIM) :: rc, ipoolno
INTEGER(KIND=JPIM) :: i, j, jcol
INTEGER(KIND=JPIM) :: nrows_out, nra
INTEGER(KIND=JPIM) :: nrows_in, ncols_out, doffset
INTEGER(KIND=JPIM) :: ldimd, ireplicate_PE, info(5)
INTEGER(KIND=JPIM) :: icount, npes, nlastrow, ifill_intermed, using_it
logical is_table, is_replicated, do_sort, LLsync, LLmpex1debug
character(len=1) env
INTEGER(KIND=JPIM) :: glbNpools, targetPE
INTEGER(KIND=JPIM) :: idummy, idummy_arr(0), nkey
GEN_TYPE, allocatable :: dtmp(:,:)
INTEGER(KIND=JPIM), allocatable :: ipool(:), npoollen(:), npoolptr(:)
INTEGER(KIND=JPIM), allocatable :: nrowvec(:)
INTEGER(KIND=JPIM), allocatable :: iflag(:)
logical  , allocatable :: takethis(:)
INTEGER(KIND=JPIM) opponent(ODBMP_nproc+1)
INTEGER(KIND=JPIM) :: joppo, noppo, loopcnt, jloop, ipoolptr1, ipoolptr2, ndata
INTEGER(KIND=JPIM) :: fast_physproc, xfast
INTEGER(KIND=JPIM) :: ipoolno_save
fast_physproc(xfast) = mod(xfast-1,ODBMP_nproc)+1
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK(GEN_PUTNAME,0,ZHOOK_HANDLE)

is_table = .FALSE.
if (len(dtname) >= 1) then
  is_table = (dtname(1:1) == '@')
endif

rc = 0
ireplicate_PE = 0
if (present(offset)) then
  doffset = offset ! No checking by the way
else
  doffset = 0
endif

ipoolno = get_poolno(handle, poolno)

if ( is_table .and. (&
    &.not. ODB_valid_poolno(ipoolno) .or. ipoolno == -1) ) then
  CALL ODB_abort(&
       &GEN_PUTNAME,&
       &'A valid local pool is required for TABLE put : "'//&
       &trim(dtname)//'"',&
       &ipoolno,&
       &.FALSE.)
  rc = -1
  goto 99999
endif

ipoolno_save = ipoolno

ldimd = size(d, dim=1)
nrows_in = nrows
ncols_out = size(d, dim=2) - 1
if (present(ncols)) then
  ncols_out = min(ncols,ncols_out)
endif

if (ldimd < nrows_in) then
  rc = ldimd - nrows_in
  CALL ODB_abort(&
       &GEN_PUTNAME,&
       &'The actual no. of rows is less than specified in TABLE/VIEW="'//&
       &trim(dtname)//'"',&
       &rc)
  goto 99999
endif

do_sort = .FALSE.
if (present(sorted)) do_sort = sorted

ifill_intermed = 0
if (present(store_intermed)) then
  if (store_intermed) ifill_intermed = 1
endif

using_it = 0
if (present(using)) using_it = using

if (db_trace) then
  info(1) = ldimd
  info(2) = nrows_in
  info(3) = ncols_out
  info(4) = ifill_intermed
  info(5) = using_it
  CALL cODB_trace(handle, 1,&
       &GEN_PUTNAME//':'//dtname, info, size(info))
endif

if (odbHcheck(handle, GEN_PUTNAME)) then
  if (ODB_io_method(handle) == 5 .and. ifill_intermed /= 1) then
    CALL ODB_abort(&
      & GEN_PUTNAME, &
      & 'ODB_IO_METHOD=5 does not allow ODB_put()-operations. Found with "'//&
      & trim(dtname)//'"',&
      & 5)
    goto 99999
  endif

  allocate(iflag(ncols_out))

  if (present(colput) .or. present(colpack)) then
    iflag(:) = 0
  else
    iflag(:) = def_flag
  endif

  if (present(colput)) then
    do i=1,min(ncols_out,size(colput))
      if (colput(i)) iflag(i) = iflag(i) + flag_put
    enddo
!== Not putting back anything ==> skip extra processing
    if (ALL(iflag(:) == 0)) goto 9999
  endif

  if (present(colpack)) then
    do i=1,min(ncols_out,size(colpack))
      if (colpack(i)) iflag(i) = iflag(i) + flag_pack
    enddo
  endif

  allocate(takethis(0:ncols_out))
  takethis(0) = .TRUE.
  do i=1,ncols_out
    takethis(i) = (IAND(iflag(i),flag_put) == flag_put)
  enddo

  if (is_table) then
!--   A table

!..   Note: Control word (column#0) is overwritten in the case of a table

    if (nrows_in > 0) then
      CALL cODB_put_control_word(d(1,0), &
                                 0, nrows_in, 0, &
                                 ipoolno, 0)
    endif

    npes = 0
  else
    CALL cODB_get_npes(handle, ipoolno,&
         & ctrim(dtname), ireplicate_PE, rc, using_it)
    is_replicated = (ireplicate_PE /= 0)

    if (rc < 0) then
      CALL ODB_abort(&
             & GEN_PUTNAME, &
             &'Cannot get NPES for VIEW="'//&
             &trim(dtname)//'"',&
             &rc)
      goto 99999
    endif

    npes = rc

    if (is_replicated .or. npes == 0) then
!--   A replicated view; forget the "ipoolno"; it'll be determined 
!     from the control word, column#0
!     Note: Only local pools with local data mods will be affected
!           thus no message passing is involved

      icount = 0      
      if (nrows_in > 0) then
        allocate(ipool(nrows_in))
        CALL cODB_get_poolnos(d(1,0), &
                              0, nrows_in, 0, &
                              ipool)

        do j=1,nrows_in
          ipoolno = ipool(j)
          if (fast_physproc(ipoolno) /= ODBMP_myproc) then
            icount = icount + 1
            ipool(j) = -1
          endif
        enddo

        if (icount > 0) then
          CALL cODB_mask_control_word(d(1,0), &
                                      0, nrows_in, 0, &
                                      ipool)
        endif

        deallocate(ipool)
      endif

!-- Was sorted i.e. ORDERBY/SORTBY was present ? If yes ==> *must* sort
      nkey = ODB_sortkeys(handle, ctrim(dtname))

!-- Try to avoid unnecessary sorting and you will possibly save 
!   a considerable amount of memory !!

      if (do_sort .or. icount > 0 .or. is_replicated .or. nkey > 0) then
        CALL cODB_ae_dump(handle, -1, ipoolno_save, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                          ldimd, nrows_in, ncols_out, d)

        call ODB_ctrlw_sort(d, nrows_in, takethis)
        nrows_in = nrows_in - icount
      endif
    else
      CALL cODB_ae_dump(handle, -2, ipoolno_save, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                        ldimd, nrows_in, ncols_out, d)

      call ODB_ctrlw_sort(d, nrows_in, takethis)

      allocate(npoollen(npes))
      allocate(npoolptr(npes))

      if (nrows_in > 0) then
        allocate(ipool(nrows_in))
        CALL cODB_get_poolnos(d(1,0), &
            & 0, nrows_in, 0, &
            & ipool)
        do ipoolno=1,npes
          npoollen(ipoolno) = count(ipool(:) == ipoolno)
        enddo
        deallocate(ipool)
      else
        npoollen(:) = 0
      endif

      npoolptr(1) = 0
      do ipoolno=2,npes
        npoolptr(ipoolno) = npoolptr(ipoolno-1) + npoollen(ipoolno-1)
      enddo

      CALL cODB_getenv('ODB_MPEXCHANGE1_DEBUG', env)
      LLmpex1debug = (env == '1')

      allocate(nrowvec(npes))

      CALL cODB_get_rowvec(handle, -1,&
             &ctrim(dtname),&
             &nrowvec, npes,&
             &rc, using_it)

      nrows_in = sum(nrowvec(:))

      if (LLmpex1debug) then
        write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//':   nrows_in, npes, nrowvec(1:npes) [local]=',&
                   nrows_in, npes, nrowvec(:)
        call flush(6)
      endif

      CALL ODBMP_global('SUM', nrowvec)

      nrows_out = 0
      do j=1,npes
        if (fast_physproc(j) == ODBMP_myproc) then
          nrows_out = nrows_out + nrowvec(j)
        endif
      enddo

      if (LLmpex1debug) then
        write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': nrows_out, npes, nrowvec(1:npes) [global]=',&
                   nrows_out, npes, nrowvec(:)
        call flush(6)
      endif

      nra = ODB_lda(nrows_out)
      allocate(dtmp(nra,0:ncols_out))
      !-- do only the necessary initializations
      dtmp(:,0) = 0 ! control-word

      !*** Note: npes is a kind of no. of pools over all procs

      loopcnt = (npes + ODBMP_nproc - 1)/ODBMP_nproc
      noppo = ODBMP_setup_exchange(opponent)

      LLsync = (npes < ODBMP_nproc   .OR. &
                noppo /= ODBMP_nproc .OR. &
                mod(npes,ODBMP_nproc) /= 0)

      if (LLmpex1debug) then
        write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': ODBMP_nproc, ODBMP_myproc, npes, loopcnt, noppo, LLsync=', &
                                                     ODBMP_nproc, ODBMP_myproc, npes, loopcnt, noppo, LLsync
        write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': view='//ctrim(dtname)
        write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': opponent(:)=',opponent(:)
        write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': size(npoolptr), npoolptr(:)=',size(npoolptr), npoolptr(:)
        write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': size(npoollen), npoollen(:)=',size(npoollen), npoollen(:)
        write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': size & shape of d   =',size(d)   , shape(d)
        write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': size & shape of dtmp=',size(dtmp), shape(dtmp)
        call flush(6)
      endif

      CALL ODBMP_sync(where=0)
      nlastrow = 0
      do jloop=1,loopcnt
        if (LLmpex1debug) then
          write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': >jloop=',jloop
          call flush(6)
        endif
        do joppo=1,noppo
          targetPE = opponent(joppo)
          if (LLmpex1debug) then
            write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': >>> joppo, targetPE=',joppo,targetPE
            call flush(6)
          endif
          if (targetPE >= 1 .and. targetPE <= ODBMP_nproc) then
            j = targetPE + (jloop-1)*ODBMP_nproc !*** Pools j assigned in a round-robin fashion
            if (LLmpex1debug) then
              write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': j, npes=',j,npes
              call flush(6)
            endif
            if (j <= npes) then
              if (LLmpex1debug) then
                write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': j,nlastrow,npoolptr(j),npoollen(j)=',&
                                                             j,nlastrow,npoolptr(j),npoollen(j)
                call flush(6)
              endif
              if (npoollen(j) > 0) then
                ipoolptr1 = npoolptr(j)+1
                ipoolptr2 = npoolptr(j)+npoollen(j)
                ndata = npoollen(j)
              else
                ipoolptr1 = 0
                ipoolptr2 = 0
                ndata = 0
              endif
            else
              ipoolptr1 = 0
              ipoolptr2 = 0
              ndata = 0
            endif ! if (j <= npes) then ...
            if (LLmpex1debug) then
              write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': ==> j,nlastrow,ndata,ipoolptr1,ipoolptr2=',&
                                                               j,nlastrow,ndata,ipoolptr1,ipoolptr2
              call flush(6)
            endif
            CALL ODBMP_exchange1(targetPE,&
                              &dtmp,nlastrow,&
                              &d(ipoolptr1:ipoolptr2,:),&
                              &ndata,ncols_out,takethis,LLmpex1debug)
            if (LLmpex1debug) then
              write(6,*) ODBMP_myproc,';'//GEN_PUTNAME//': <== j,nlastrow,ndata,ipoolptr1,ipoolptr2=',&
                                                               j,nlastrow,ndata,ipoolptr1,ipoolptr2
              call flush(6)
            endif
          endif ! if (targetPE >= 1 .and. targetPE <= ODBMP_nproc) then ...
          if (LLsync) CALL ODBMP_sync(where=-joppo)
        enddo ! do joppo=1,noppo
        if (.not.LLsync) CALL ODBMP_sync(where=-jloop)
      enddo ! do jloop=1,loopcnt

      deallocate(npoollen)
      deallocate(npoolptr)

      if (nlastrow == nrows_out) then
        if (nlastrow > 0) then
          CALL cODB_ae_dump(handle, -3, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                            nra, nlastrow, ncols_out, dtmp)

          CALL ODB_ctrlw_sort(dtmp, nlastrow, takethis)

          CALL cODB_ae_dump(handle, -4, ipoolno, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                            nra, nlastrow, ncols_out, dtmp)

          ipoolno = -1
          CALL &
            GEN_CODB_PUT &
                &(handle, ipoolno,&
                &ctrim(dtname), dtmp, 0, nra,&
                &nlastrow, ncols_out, &
                &iflag, -1, ifill_intermed, &
                &using_it, rc)

          if (rc /= nlastrow) then
            CALL ODB_abort(&
                  &GEN_PUTNAME, &
                  &'Cannot put parallel data for VIEW="'//&
                  &trim(dtname)//'"',&
                  &rc)
            rc = -2
            goto 99999
          endif
        endif
      else
!        write(0,*)'nrows_out, nlastrow=',nrows_out, nlastrow
        CALL ODB_abort(&
                &GEN_PUTNAME, &
                &'Unable to put ALL parallel data in VIEW="'//&
                &trim(dtname)//'"',&
                &nrows_out-nlastrow)
        rc = -3
        goto 99999
      endif

      deallocate(dtmp)
      deallocate(nrowvec)

      rc = nrows_out

      nrows_in = 0 ! Put already performed
    endif ! if (is_replicated .or. npes == 0) then ... else
  endif ! if (is_table) then ... else

  if (nrows_in > 0) then

!--   This is for TABLEs or replicated VIEWs or non-shuffled VIEWs only

    if (nrows_in >= ODB_MAXROWS) then
!--   With 32-bit signed arithmetic should never enter here !!
      rc = ODB_MAXROWS - nrows_in
      CALL ODB_abort(&
             &GEN_PUTNAME, &
             &'Too many rows in VIEW/TABLE="'//&
             &trim(dtname)//'"',&
             &rc)
      goto 99999
    endif

    if (nrows_in > 0) then
      CALL cODB_ae_dump(handle, -5, ipoolno_save, ctrim(dtname), GEN_AE_DUMP_DATATYPE, &
                        ldimd, nrows_in, ncols_out, d(1,0))

      CALL &
        GEN_CODB_PUT &
          &(handle, ipoolno_save,&
          &ctrim(dtname), d(1+doffset,0), 0, ldimd,&
          &nrows_in, ncols_out, &
          &iflag, -1, ifill_intermed, &
          &using_it, rc)
    endif

    if (rc < 0) then
      CALL ODB_abort(&
             &GEN_PUTNAME, &
             &'Cannot put data for VIEW/TABLE="'//&
             &trim(dtname)//'"',&
             &rc)
      goto 99999
    endif
  endif

9999 continue
  if (allocated(iflag))    deallocate(iflag)
  if (allocated(takethis)) deallocate(takethis)
endif ! if (odbHcheck(handle, GEN_PUTNAME))

if (present(ncols)) ncols = ncols_out

CALL cODB_trace(handle, 0,&
    &GEN_PUTNAME//':'//dtname, idummy_arr, 0)
99999 continue
IF (LHOOK) CALL DR_HOOK(GEN_PUTNAME,1,ZHOOK_HANDLE)
END FUNCTION

SUBROUTINE GEN_CTRLW_SORT(d, nrows_in, takethis)
implicit none
GEN_TYPE, intent(inout) :: d(:,0:)
INTEGER(KIND=JPIM), intent(in)   :: nrows_in
logical, intent(in)     :: takethis(0:)
INTEGER(KIND=JPIM), allocatable :: ctrlw_index(:) ! can be large >> 100,000 elements
INTEGER(KIND=JPIB), allocatable :: ctrlw(:)       ! can be large >> 100,000 elements (note: 64-bit int)
INTEGER(KIND=JPIM), allocatable :: icols(:)
GEN_TYPE , allocatable :: zbuf(:)
INTEGER(KIND=JPIM) :: rc, j, imaxcol, inumcols, jj, inumt
INTEGER(KIND=JPIM), external :: get_max_threads
logical :: LLomp_okay
REAL(KIND=JPRB) :: ZHOOK_HANDLE, ZHOOK_HANDLE_OMP
IF (LHOOK) CALL DR_HOOK(GEN_CTRLW_SORTNAME,0,ZHOOK_HANDLE)
if (nrows_in > 0) then
  allocate(ctrlw(nrows_in), ctrlw_index(nrows_in))
  CALL cODB_get_control_word(d(1,0), 0, nrows_in, 0, ctrlw(1))
  CALL keysort(rc, ctrlw, nrows_in, index=ctrlw_index, init=.TRUE.)
  deallocate(ctrlw)
  imaxcol = min(size(d,dim=2),size(takethis)) - 1
  allocate(icols(imaxcol+1))
  jj = 0
  do j=0,imaxcol
    if (j > 0 .and. .not. takethis(j)) cycle
    jj = jj + 1
    icols(jj) = j
  enddo
  inumcols = jj
  inumt = get_max_threads()
!  LLomp_okay = ( &
!       & nrows_in > 10000 .and. &
!       & inumcols >= inumt .and. &
!       & .not. OML_IN_PARALLEL()) ! Prevents nested OpenMP & too short loops/too few columns
!!$OMP PARALLEL PRIVATE(j,jj,zbuf,ZHOOK_HANDLE_OMP) IF (LLomp_okay)
!  IF (LHOOK) CALL DR_HOOK(GEN_CTRLW_SORTNAME_OMP,0,ZHOOK_HANDLE_OMP)
  allocate(zbuf(nrows_in))
!!$OMP DO SCHEDULE(DYNAMIC,1)
  do jj=1,inumcols
    j = icols(jj)
    zbuf(1:nrows_in) = d(ctrlw_index(1:nrows_in),j)
    d(1:nrows_in,j) = zbuf(1:nrows_in)
  enddo
!!$OMP END DO
  deallocate(zbuf)
!  IF (LHOOK) CALL DR_HOOK(GEN_CTRLW_SORTNAME_OMP,1,ZHOOK_HANDLE_OMP)
!!$OMP END PARALLEL
  deallocate(ctrlw_index, icols)
endif
99999 continue
IF (LHOOK) CALL DR_HOOK(GEN_CTRLW_SORTNAME,1,ZHOOK_HANDLE)
END SUBROUTINE

#ifndef NO_UNDEF
#undef GEN_TYPE
#undef GEN_GET
#undef GEN_PUT
#undef GEN_GETNAME
#undef GEN_PUTNAME
#undef GEN_CODB_GET
#undef GEN_CODB_PUT
#undef GEN_INC
#undef GEN_CTRLW_SORT
#undef GEN_CTRLW_SORTNAME
#undef GEN_CTRLW_SORTNAME_OMP
#undef GEN_AE_DUMP_DATATYPE
#endif

#endif
