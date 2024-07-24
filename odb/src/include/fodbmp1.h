#if INT_VERSION == 4

#define GEN_TYPE          INTEGER(KIND=JPIM)
#define GEN_MSGTYPE       1
#define GEN_XCHANGE1       ODBMP_iexchange1
#define GEN_XCHANGE1_NAME 'ODBMP:ODBMP_IEXCHANGE1'

#elif REAL_VERSION == 8

#define GEN_TYPE          REAL(KIND=JPRB)
#define GEN_MSGTYPE       8
#define GEN_XCHANGE1       ODBMP_dexchange1
#define GEN_XCHANGE1_NAME 'ODBMP:ODBMP_DEXCHANGE1'

#else

  ERROR in programming : No datatype given (should never have ended up here)

#endif

#if defined(INT_VERSION) || defined(REAL_VERSION)

SUBROUTINE GEN_XCHANGE1 &
    &(targetPE, &
    &to, nlastrow, &
    &from, nrows, ncols, &
    &takethis, LDdebug)
USE MPL_MODULE
implicit none

INTEGER(KIND=JPIM), intent(in)    :: targetPE, nrows, ncols
INTEGER(KIND=JPIM), intent(inout) :: nlastrow
GEN_TYPE, intent(out)  :: to(:,0:)
GEN_TYPE, intent(in)   :: from(:,0:)
logical , intent(in)   :: takethis(0:), LDdebug

INTEGER(KIND=JPIM), parameter :: datatype = GEN_MSGTYPE
INTEGER(KIND=JPIM) :: msgtag_send, msgtag_recv, j, rc, info(8)
INTEGER(KIND=JPIM) :: recv_count, recv_from, recv_tag
INTEGER(KIND=JPIM) :: nlastrow_local, freerows
INTEGER(KIND=JPIM) :: ireq(1+ncols), nreq
INTEGER(KIND=JPIM) :: ireq_zerodata(1+ncols), nreq_zerodata
GEN_TYPE :: zerodata(0)
logical LLsuppress_send, LLsuppress_recv
REAL(KIND=JPRB) :: ZHOOK_HANDLE
IF (LHOOK) CALL DR_HOOK(GEN_XCHANGE1_NAME,0,ZHOOK_HANDLE)

nreq=0
nreq_zerodata=0
freerows = size(to,dim=1) - nlastrow

if (LDdebug) then
  write(6,'(a1,i2,a,10i8)') &
           '#',ODBMP_myproc, ': '// GEN_XCHANGE1_NAME &
           //' targetPE, nlastrow, nrows, ncols, freerows, size(to,dim=1), size(from,dim=1)=', &
               targetPE, nlastrow, nrows, ncols, freerows, size(to,dim=1), size(from,dim=1)
  call flush(6)
endif

if (&
    ((targetPE == ODBMP_myproc) .and. (nrows > 0 .and. freerows < nrows)) .OR. &
    ((targetPE /= ODBMP_myproc) .and. (freerows < 0))) then
    write(0,*) ODBMP_myproc, ': '// GEN_XCHANGE1_NAME &
           //' targetPE, nlastrow, nrows, ncols, freerows, size(to,dim=1), size(from,dim=1)=', &
               targetPE, nlastrow, nrows, ncols, freerows, size(to,dim=1), size(from,dim=1)
    CALL ODBMP_abort(&
                     & GEN_XCHANGE1_NAME, &
                     & 'Insufficient amount of free rows in destination matrix', &
                     & freerows-nrows)
endif

if (targetPE == ODBMP_myproc) then
!== Perform local copying only
  recv_count = 0
  if (nrows > 0) then
    COPY_MYSELF_LOOP: do j=0,ncols
      if (j > 0 .and. .not. takethis(j)) cycle COPY_MYSELF_LOOP ! Always take col#0, however!
      to(nlastrow+1:nlastrow+nrows,j) = from(1:nrows,j)
    enddo COPY_MYSELF_LOOP
    nlastrow = nlastrow + nrows
  endif
  goto 999
endif

LLsuppress_send=.FALSE.
LLsuppress_recv=.FALSE.
nlastrow_local = nlastrow
COMM_LOOP: do j=0,ncols
  if (LLsuppress_send .and. LLsuppress_recv) exit COMM_LOOP
  if (j > 0 .and. .not. takethis(j)) cycle COMM_LOOP ! Always take col#0, however!
  rc = 0
  if (LLsuppress_send) goto 2001

  msgtag_send = targetPE * 100 + j

  if (mp_trace) then
    info(1) = j
    info(2) = targetPE
    info(3) = nrows
    info(4) = msgtag_send
    info(5) = datatype
    CALL cODB_trace(-1, 2, &
        & GEN_XCHANGE1_NAME &
        & //':send column',info,5)
  endif

  if (LDdebug) then
    write(6,*) '#',ODBMP_myproc, ': send() to PE#',targetPE,&
               ' j, nrows, msgtag_send=',&
                 j, nrows, msgtag_send
    call flush(6)
  endif

  if (nrows > 0) then
    nreq=nreq+1
    CALL MPL_SEND(from(1:nrows,j),kdest=targetPE,ktag=msgtag_send,kmp_type=JP_NON_BLOCKING_STANDARD, &
                  krequest=ireq(nreq), kerror=rc)
  else
    nreq_zerodata=nreq_zerodata+1
    CALL MPL_SEND(zerodata,kdest=targetPE,ktag=msgtag_send,kmp_type=JP_NON_BLOCKING_STANDARD, &
                  krequest=ireq_zerodata(nreq_zerodata), kerror=rc)
    LLsuppress_send = .TRUE. ! Send zero length data once only
    if (LDdebug .and. LLsuppress_send) then
      write(6,*) '#',ODBMP_myproc, ": Further send()'s suppressed"
      call flush(6)
    endif
  endif

  if (rc /= 0) then
    write(0,*) ODBMP_myproc,': Failed to send() data to PE#',targetPE,&
                            ' : j, nrows, msgtag_send, rc=',&
                                j, nrows, msgtag_send, rc
    CALL ODBMP_abort(&
                     & GEN_XCHANGE1_NAME, &
                     & 'Failed to send() data', &
                     & rc)
  endif

2001 continue
  if (LLsuppress_recv) cycle COMM_LOOP

  msgtag_recv = ODBMP_myproc * 100 + j

  if (mp_trace) then
    info(1) = j
    info(2) = targetPE
    info(3) = nrows
    info(4) = msgtag_recv
    info(5) = datatype
    info(6) = rc
    info(7) = nlastrow
    info(8) = freerows
    CALL cODB_trace(-1, 1, &
        & GEN_XCHANGE1_NAME &
        & //':send rc/last; begin recv column',info,8)
  endif

  if (LDdebug) then
    write(6,*) '#',ODBMP_myproc, ': >>recv() from PE#',targetPE,&
               ' j, msgtag_recv, freerows, nlastrow=',&
                 j, msgtag_recv, freerows, nlastrow
    call flush(6)
  endif

  if (freerows > 0) then
    CALL MPL_RECV(to(nlastrow+1:nlastrow+freerows,j),ksource=targetPE,ktag=msgtag_recv,&
                 &kount=recv_count,kfrom=recv_from,krecvtag=recv_tag,kerror=rc)
  else
    CALL MPL_RECV(zerodata,ksource=targetPE,ktag=msgtag_recv,&
                 &kount=recv_count,kfrom=recv_from,krecvtag=recv_tag,kerror=rc)
  endif

  if (LDdebug) then
    write(6,*) '#',ODBMP_myproc, ': <<recv() from PE#',targetPE,&
               ' j, msgtag_recv, freerows, nlastrow, recv_count, recv_from, recv_tag, rc=',&
                 j, msgtag_recv, freerows, nlastrow, recv_count, recv_from, recv_tag, rc
    call flush(6)
  endif

  if (rc /= 0) then
    write(0,*) ODBMP_myproc,': Failed to recv() data from PE#',targetPE,&
               ' : j, msgtag_recv, freerows, nlastrow, recv_count, recv_from, recv_tag, rc=',&
                   j, msgtag_recv, freerows, nlastrow, recv_count, recv_from, recv_tag, rc
    CALL ODBMP_abort(&
                     & GEN_XCHANGE1_NAME, &
                     & 'Failed to recv() data', &
                     & rc)
  endif

  if (mp_trace) then
    info(1) = j
    info(2) = recv_from
    info(3) = recv_count
    info(4) = recv_tag
    info(5) = datatype
    info(6) = rc
    info(7) = nlastrow
    info(8) = freerows
    CALL cODB_trace(-1, 0, &
        & GEN_XCHANGE1_NAME &
        & //':              end recv column',info,8)
  endif

  if (j == 0) then
    nlastrow_local = nlastrow_local + recv_count
    LLsuppress_recv = (recv_count == 0)
    if (LDdebug .and. LLsuppress_recv) then
      write(6,*) '#',ODBMP_myproc, ": Further recv()'s suppressed"
      call flush(6)
    endif
  endif
enddo COMM_LOOP
nlastrow = nlastrow_local


999 continue
if (LDdebug) then
  write(6,'(a1,i2,a,10i8)') &
           '#',ODBMP_myproc, ': '// GEN_XCHANGE1_NAME &
           //' targetPE, nlastrow, recv_count=', &
               targetPE, nlastrow, recv_count
  call flush(6)
endif
if (nreq > 0) then
     call MPL_WAIT(from(1:nrows, 1), krequest=ireq(1:nreq), cdstring=GEN_XCHANGE1_NAME//' : wait for send data')
end if
if (nreq_zerodata > 0) then
    call MPL_WAIT(zerodata, krequest=ireq_zerodata(1:nreq_zerodata), cdstring=GEN_XCHANGE1_NAME//' : wait for send zerodata')
end if

IF (LHOOK) CALL DR_HOOK(GEN_XCHANGE1_NAME,1,ZHOOK_HANDLE)
END SUBROUTINE

#ifndef NO_UNDEF
#undef GEN_TYPE
#undef GEN_MSGTYPE
#undef GEN_XCHANGE1
#undef GEN_XCHANGE1_NAME
#endif

#endif
