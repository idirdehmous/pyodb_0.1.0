!
! Fscheduler.F90 : Runs Unix-commands read from file(s) in parallel
!
!! Usage: env ODB_PARAL=<ncpus> fscheduler.x maxcmds [cmdfile_1 [cmdfile_2 ...]]
!
! Special commands:
! ----------------
! wait                                -- will synchronize i.e. a barrier
! on_error: command [; command; ...]  -- command(s) to be run when error had detected
!
! Also, set export FSCHEDULER_PBAR=1 to get a "progress bar" to stdout


PROGRAM fscheduler
USE PARKIND1  ,ONLY : JPIM

#ifdef NAG
USE F90_UNIX_ENV, ONLY: GETARG, IARGC
USE F90_UNIX_IO, ONLY: FLUSH
#endif

implicit none
!-- Maximum number of characters in a Fortran90 string (i.e. character(len=maxstrlen))
!   (see also odb/module/odbshared.F90 definition)
CHARACTER(LEN=1),PARAMETER :: BACKSL='\\'
INTEGER(KIND=JPIM), parameter :: maxstrlen = 32767 ! 65536
INTEGER(KIND=JPIM) :: iu, nfiles, j, j1, j2, ncmd, ilen, ipos, k, kount, rc
INTEGER(KIND=JPIM) :: numproc_default, numproc
INTEGER(KIND=JPIM) :: pbar_default, pbar, pbar_cur, pbar_max
#ifndef NAG
INTEGER(KIND=JPIM) :: iargc
#endif
character(len=512) :: file
logical LLclose_file, LLwait, LLcont, LLpbar
logical LLhas_on_error, LLrun_on_error
INTEGER(KIND=JPIM) :: nmaxcmd, numargs, exit_code
INTEGER(KIND=JPIM) :: usec, usec_inc, usec_default
character(len=20) :: cmaxcmd
character(len=maxstrlen) :: acmd, on_error_cmd
character(len=maxstrlen), allocatable :: cmd(:)
INTEGER(KIND=JPIM), allocatable :: ilencmd(:)
INTEGER(KIND=JPIM), allocatable :: iecho(:)
INTEGER(KIND=JPIM), allocatable :: pid(:)

numproc_default = 1
CALL util_igetenv('ODB_PARAL', numproc_default, numproc)

pbar_default = 0
CALL util_igetenv('FSCHEDULER_PBAR', pbar_default, pbar)
LLpbar = (pbar == 1)

numargs = iargc()

if (numargs >= 1) then
  call getarg(1,cmaxcmd)
  read(cmaxcmd,'(i20)') nmaxcmd
  allocate(cmd(nmaxcmd))
  allocate(ilencmd(nmaxcmd))
  allocate(iecho(nmaxcmd))
else
  write(0,*) 'Usage: fscheduler.x maxcmds [cmdfile_1 [cmdfile_2 ...]]'
  call ec_exit(1)
endif

nfiles = numargs - 1
if (nfiles >= 1) then
  iu = 1
  LLclose_file = .TRUE.
else
  iu = 5
  LLclose_file = .FALSE.
endif

ncmd = 0
j2 = max(1,nfiles)
j1 = min(1,j2)
do j=j1,j2
  if (LLclose_file) then
    call getarg(j+1,file)
    open(iu,file=trim(file),status='old',err=99) ! ignore open errors
  endif
  LLcont = .FALSE.
  do while (ncmd < nmaxcmd)
    ilen = 1
    if (LLcont) ilen = len_trim(acmd)
    read(iu,'(a)',end=98,err=98) acmd(ilen:) ! ignore read & eof-errors
    acmd = adjustl(trim(acmd)) ! shift to left by skipping any leading blanks
    ilen = len_trim(acmd)
    if (acmd(ilen:ilen) == BACKSL) then
      ! This was a continuation line; wait for the rest the line
      LLcont = .TRUE.
    else if (ilen > 0 .and. acmd(1:1) /= '#' .and. acmd(1:1) /= '!') then
      ! Do not accept empty lines or lines beginning with '#' or '!'
      LLcont = .FALSE.
      ncmd = ncmd + 1
      cmd(ncmd) = acmd
      ilencmd(ncmd) = ilen ! cmd-length
      iecho(ncmd) = ilen ! how many chars to be echo'ed
      ! search for double-pipe-char '||' and truncate [echoing] just before it
      ipos = scan(cmd(ncmd)(1:ilen),'|',back=.TRUE.)
      if (ipos > 1) then
        if (cmd(ncmd)(ipos-1:ipos-1) == '|') iecho(ncmd) = ipos - 2
      endif
    else
      LLcont = .FALSE.
    endif
  enddo
 98   continue
  if (LLclose_file) then
    close(iu)
  endif
 99   continue
enddo

allocate(pid(ncmd))
pid(:) = -1
kount = 0 ! count of unfinished tasks
LLhas_on_error = .FALSE.
LLrun_on_error = .FALSE.
exit_code = 0
if (LLpbar) then
  pbar_max = ncmd
  pbar_cur = 0
endif

CMDLOOP: do j=1,ncmd
   LLwait = .FALSE.
   pid(j) = 0
#ifdef NECSX
   write(0,'(a)') cmd(j)(1:min(iecho(j),132))
#else
   write(0,'(a)') cmd(j)(1:iecho(j))
#endif
   if (ilencmd(j) >= 9 .and. cmd(j)(1:9) == 'on_error:') then
      on_error_cmd = adjustl(trim(cmd(j)(10:)))
      LLhas_on_error = .TRUE.
      if (LLpbar) CALL increment_pbar(.TRUE.)
   else if (cmd(j)(1:ilencmd(j)) == 'wait') then
      LLwait = .TRUE.
      if (LLpbar) CALL increment_pbar(.TRUE.)
   else
      call codb_subshell(cmd(j)(1:ilencmd(j)), pid(j)) ! fork() + system()
      if (pid(j) > 0) then
         kount = kount + 1
      else if (pid(j) == 0) then
         !-- subshell already finished with rc == 0
         if (LLpbar) CALL increment_pbar(.TRUE.)
         continue ! do nothing
      else ! pid(j) < 0
         !-- subshell already finished with rc > 0 == abs(pid(j)) 
         !   ==> run on_error-code (if available), when all other procs finished
         LLrun_on_error = .TRUE.
         exit_code = abs(pid(j))
         pid(j) = 0
         if (LLpbar) CALL increment_pbar(.TRUE.)
      endif
   endif

   ! The following arrangement allows new subprocess to be launched as soon as another one has finished !
   ! In other words: we do NOT wait for ALL the previous 'numproc'-tasks to finish first; just one

   usec_default = 500000
   usec_inc = 400000
   do while ((LLwait .and. kount > 0) .or. kount == numproc .or. (j == ncmd .and. kount > 0))
      call codb_wait(-1, 1, rc) ! wait for any (child-)process; do not block/hang; rc may contain finished pid
      if (rc > 0) then ! rc indeed contains a finished pid
         LOOP: do k=1,j
            if (rc == pid(k)) then
               pid(k) = -1
               kount = kount - 1
               if (LLpbar) CALL increment_pbar(.TRUE.)
               exit LOOP
            endif
         enddo LOOP
      else if (rc == 0) then ! Nobody exited this time
         usec = usec_default + kount * usec_inc
         call ec_usleep(usec) ! Take a "usec" microsecs nap!
         if (LLpbar) CALL increment_pbar(.FALSE.)
      else ! rc < 0
         LLrun_on_error = .TRUE.
         exit_code = abs(rc)
         kount = kount - 1
         if (LLpbar) CALL increment_pbar(.TRUE.)
      endif
   enddo

   if (LLrun_on_error) then
      if (LLpbar) CALL increment_pbar(.FALSE.)
      exit CMDLOOP
   endif
enddo CMDLOOP

deallocate(pid)

deallocate(cmd)
deallocate(ilencmd)
deallocate(iecho)

if (LLrun_on_error .and. LLhas_on_error) call codb_subshell(trim(on_error_cmd), rc)
if (LLrun_on_error .and. LLpbar) then
   call increment_pbar(.FALSE.,'FSCHEDULER has detected a fatal error !!!')
endif

call ec_exit(exit_code)

CONTAINS

SUBROUTINE increment_pbar(LDincr, cderrmsg)
USE PARKIND1  ,ONLY : JPIM, JPRB
implicit none
CHARACTER(LEN=1),PARAMETER :: BACKSL='\\'
logical, intent(in) :: LDincr
character(len=*), intent(in), optional :: cderrmsg
character(len=50), save :: cbar
INTEGER(KIND=JPIM), save :: npropel = 1
INTEGER(KIND=JPIM), parameter :: io = 6
character(len=1), parameter :: cr = char(13)
character(len=1) :: clast
character(len=1), parameter :: cpropel(5) = (/'-', BACKSL, '|', '/', '+'/)
REAL(KIND=JPRB) :: Telapsed, Tremains
character(len=10) :: nice(2)
INTEGER(KIND=JPIM) :: ipc, x, j, idiv
INTEGER(KIND=JPIM) :: percent_done
logical, save :: LLdone = .false.
REAL(KIND=JPRB) :: util_walltime
percent_done(x) = nint((x * 100.0_JPRB)/pbar_max)
if (present(cderrmsg)) then
   write(io,'(/a/)') cderrmsg
else
   if (LLdone) return
   if (pbar_cur == 0) then
      cbar = ' '
      Telapsed = util_walltime()
   endif
   if (LDincr) pbar_cur = pbar_cur + 1
   Telapsed = util_walltime()
   ipc = percent_done(pbar_cur)
   ipc = min(max(ipc, 0),100)
   idiv = 100/len(cbar)
   do j=1,ipc/idiv
      cbar(j:j) = '#'
   enddo
   do j=ipc/idiv+1,len(cbar)
      cbar(j:j) = '.'
   enddo
   clast = cr
   if (ipc == 100) then
      clast = ' '
      npropel = 5
      Tremains = 0
   else
      if (pbar_cur > 0) then
         Tremains = (Telapsed * (pbar_max - pbar_cur))/pbar_cur
      else
         Tremains = 2147483647.0_JPRB
      endif
   endif
   nice(1) = nicetime(Telapsed)
   nice(2) = nicetime(Tremains)
   write(io,&
        & '(3x,a1,a,2x,i3,a1,"  (time: ",a,", left: ",a,", procs:",i2,")      ",a1)',&
        & advance='no') &
        & cpropel(npropel),cbar, ipc, '%', &
        & trim(nice(1)), trim(nice(2)), &
        & kount, clast
   if (ipc == 100) then
      write(io,*) ' '
      LLdone = .TRUE.
   endif
   call flush(io)
   npropel = npropel + 1
   if (npropel > 4) npropel = 1
endif
END SUBROUTINE increment_pbar

FUNCTION nicetime(t) RESULT(c)
USE PARKIND1  ,ONLY : JPIM, JPRB
implicit none
character(len=10) c
REAL(KIND=JPRB), intent(in) :: t
INTEGER(KIND=JPIM) :: tim, hh, mm, ss
tim = int(t)
ss = mod(tim,60)
mm = mod(tim/60,60)
hh = tim/3600
if (hh > 0) then
  write(c,'(i4,":",i2.2,":",i2.2)') hh, mm, ss
else
  write(c,'(i7,":",i2.2)') mm, ss
endif
c = adjustl(c)
END FUNCTION nicetime

END PROGRAM fscheduler
