SUBROUTINE cma_flperr( &
 & io, onlyinv, maxdump, mode, &
 & name, filename, &
 & d, nd, &
 & flag, nf, &
 & woff,  &
 & inf, itiny, inan) 

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK

#ifdef NAG
use f90_unix_io, only: flush
#endif

implicit none
INTEGER(KIND=JPIM), intent(in) :: io, nd, nf, onlyinv, maxdump
INTEGER(KIND=JPIM), intent(in) :: flag(nf)
REAL(KIND=JPRB),    intent(in) :: d(nd)
INTEGER(KIND=JPIM), intent(in) :: woff, inf, itiny, inan, mode
character(len=*), intent(in) :: name, filename
character(len=3) ::  star
character(len=20) ::  cmode
INTEGER(KIND=JPIM) :: j, jj, iflag
INTEGER(KIND=JPIM) :: itmp(3)
REAL(KIND=JPRB) :: ZHOOK_HANDLE

IF (LHOOK) CALL DR_HOOK('CMA_FLPERR',0,ZHOOK_HANDLE)
cmode = 'processing'
if (mode == 1) then
  cmode = 'reading'
else
  cmode = 'writing'
endif

write(io,'(1x,a)') &
 & 'CMA_FLPERR: Invalid floating point value(s)'// &
 & ' detected while '//trim(cmode) 
write(io,'(1x,a)')  &
 & '            file="'//trim(filename)//'" ;' 
write(io,'(1x,a)')  &
 & '            routine="'//trim(name)//'"' 
write(io,*)'Approximate word offset (unpacked) : ',woff

if (io /= 0) then
  write( 0,'(1x,a)') &
   & 'CMA_FLPERR: Invalid floating point value(s)'// &
   & ' detected while '//trim(cmode) 
  write( 0,'(1x,a)')  &
   & '            file="'//trim(filename)//'" ;' 
  write( 0,'(1x,a)')  &
   & '            routine="'//trim(name)//'"' 
  write( 0,*)'Approximate word offset (unpacked) : ',woff
endif

write(io,*)'There are ',inf,' infinite values (Inf)'
write(io,*)'          ',itiny,' too tiny numbers'
write(io,*)'          ',inan,' Not-a-Numbers (NaN)'

if (onlyinv > 0) then
  if (nf >= nd) then
    jj = 1
    j = 1
    do while (j <= nd .and. jj <= maxdump)
      if (flag(j) > 0) then
        write(star,'("<",i1,">")') flag(j)
        write(io,'(i4,1x,a3,2x,"0x",z16.16,2x,g20.12)') &
         & j,star,d(j),d(j) 
        jj = jj + 1
      endif
      j = j + 1
    enddo
  else
    jj = 1
    j = 1
    do while (j <= nd .and. jj <= maxdump)
      call cma_flpcheck(d(j),1,iflag,1, &
       & itmp(1),itmp(2),itmp(3)) 
      if (iflag > 0) then
        write(io,'(i4,6x,"0x",z16.16,2x,g20.12)') &
         & j,d(j),d(j) 
        jj = jj + 1
      endif
      j = j + 1
    enddo
  endif
else
  if (nf >= nd) then
    do j=1,maxdump
      star = ' '
      if (flag(j) > 0) write(star,'("<",i1,">")') flag(j)
      write(io,'(i4,1x,a3,2x,"0x",z16.16,2x,g20.12)') &
       & j,star,d(j),d(j) 
    enddo
  else
    do j=1,maxdump
      write(io,'(i4,6x,"0x",z16.16,2x,g20.12)') &
       & j,d(j),d(j) 
    enddo
  endif
endif

write(io,*)'CMA_FLPERR: End of dump.'

#if defined(VPP) || defined(FUJITSU) || defined(SGI) || defined(RS6K)
call errtra()
#endif

call flush(io)
IF (LHOOK) CALL DR_HOOK('CMA_FLPERR',1,ZHOOK_HANDLE)
END SUBROUTINE cma_flperr
