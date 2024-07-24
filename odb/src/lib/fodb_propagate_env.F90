subroutine fodb_propagate_env(kenforce, kverbose)
USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK
use odb_module ! , only : ODB_distribute, ODBMP_myproc, ODBMP_nproc, ODBMP_global
use mpl_module, only : MPL_BUFFER_METHOD, MPL_MBX_SIZE, MPL_METHOD
#ifdef NAG
use f90_unix_proc, only: system
#endif
implicit none
INTEGER(KIND=JPIM), intent(in) :: kenforce ! =0 don't enforce, <> 0 enforce to reset the env.vars
INTEGER(KIND=JPIM), intent(in) :: kverbose ! <> 0, do verbose output for proc == kverbose
INTEGER(KIND=JPIM) :: icount_env, iret, j, iloc, iprop, ipid, imbxsize
logical LLfirst_time, LLenforce, LLverbose
character(len=8192) env, oldenv
character(len=1024) pwd, home, file, deffile
character(len=80)   cpid
REAL(KIND=JPRB) :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('FODB_PROPAGATE_ENV',0,ZHOOK_HANDLE)

!write(0,*) ODBMP_myproc,': FODB_PROPAGATE_ENV : nproc, kenforce, kverbose',&
!         &                                ODBMP_nproc, kenforce, kverbose

if (ODBMP_nproc == 1) goto 9999
LLenforce = (kenforce /= 0)

iprop = 0                               ! Do not propagate by default
CALL codb_getpid(ipid)
write(cpid,*) ipid
cpid = adjustl(trim(cpid))
CALL codb_getenv('HOME',home)
if (home == ' ') home = '.'
deffile = trim(home)//'/odb_propagate_env.'//trim(cpid) ! If you do propagate, use this file by default
file = deffile

if (ODBMP_myproc == 1) then
  CALL codb_getenv('ODB_PROPAGATE_ENV',env)
!  write(0,*) ODBMP_myproc,': ODB_PROPAGATE_ENV="'//trim(env)//'"'
  if (env /= ' ') then
    iloc = scan(env,',')
!    write(0,*) ODBMP_myproc,': iloc=',iloc
    if (iloc > 1) then
      read(env(1:iloc-1),*,err=8888,end=8888) iprop
      read(env(iloc+1:),'(a)',err=8888,end=8888) file
      file = adjustl(trim(file))
    else
      read(env,*,err=8888,end=8888) iprop
    endif
  endif
  goto 300
 8888 continue
   iprop = 0
 300  continue
   if (file == ' ') file = deffile
!   write(0,*) ODBMP_myproc,': file,iprop : "'//trim(file)//'"',iprop
endif
CALL ODBMP_global('SUM',iprop)

if (iprop == 0) goto 9999

LLverbose = (kverbose == ODBMP_myproc .or. kverbose == -1 &
            & .or. iprop > 1 .or. iprop == -ODBMP_myproc)

!write(0,*) ODBMP_myproc,': iprop, kverbose, LLverbose, ODBMP_myproc=', &
!         &                 iprop, kverbose, LLverbose, ODBMP_myproc

if (ODBMP_myproc == 1) then
  CALL codb_remove_file(trim(file),iret)
  CALL codb_system('printenv | sort > '//trim(file))
  open(1,file=trim(file),status='old')
  LLfirst_time = .TRUE.
  icount_env = 0
 200  continue
  do
    if (LLfirst_time) then
      read(1,'(a)',err=100,end=100)
      icount_env = icount_env + 1
    else
      read(1,'(a)',err=100,end=100) env
      iret = ODB_distribute(env)
    endif
  enddo
 100  continue
  if (LLfirst_time) then
    rewind(1)
    if (LLverbose) write(0,*) ODBMP_myproc,': broadcasting icount_env=',icount_env
    iret = ODB_distribute(icount_env)
    LLfirst_time = .FALSE.
    if (icount_env > 0) goto 200
  endif
  close(1)
  CALL codb_remove_file(trim(file),iret)
else
  iret = ODB_distribute(icount_env)
  if (LLverbose) write(0,*) ODBMP_myproc,': received icount_env=',icount_env
  oldenv = ' '
  do j=1,icount_env
    iret = ODB_distribute(env)
    iloc = scan(env,'=')
    if (iloc > 0) then
      iloc = iloc - 1
      if (.not. LLenforce) CALL codb_getenv(env(1:iloc),oldenv)
      if (LLenforce .or. oldenv == ' ') then
        if (LLverbose) write(0,*) ODBMP_myproc,': Accepting '//env(1:iloc)
        CALL codb_putenv(trim(env))
      else
        if (LLverbose) write(0,*) ODBMP_myproc,': Rejecting '//env(1:iloc)//' since already defined'
      endif
    endif
  enddo
endif
! Finally, make sure the $PWD is set correctly
CALL codb_getcwd(pwd)
if (pwd == ' ') pwd = '.'
CALL codb_putenv('PWD='//trim(pwd))
if (LLverbose) write(0,*) ODBMP_myproc,': PWD was set to "'//trim(pwd)//'"'
call util_igetenv('MPL_MBX_SIZE',MPL_MBX_SIZE,imbxsize)
if (imbxsize /= MPL_MBX_SIZE) then
  if (LLverbose) write(0,*) ODBMP_myproc,': Re-setting MPL_MBX_SIZE to ',imbxsize
  call MPL_BUFFER_METHOD(MPL_METHOD,KMBX_SIZE=imbxsize)
endif
 9999 continue
IF (LHOOK) CALL DR_HOOK('FODB_PROPAGATE_ENV',1,ZHOOK_HANDLE)
end subroutine fodb_propagate_env
