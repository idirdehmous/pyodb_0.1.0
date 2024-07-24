SUBROUTINE cma_prt_stat( &
 & ftn_unit, cma_unit, &
 & binno, numbins, &
 & fileno, &
 & str_open_time,  str_close_time, &
 & logical_name,   true_name, &
 & pipecmd,        cmd, &
 & read_only, packmethod, blocksize, &
 & numddrs,   lenddrs, &
 & numobs,    maxreplen,  cmalen, &
 & filesize,  filepos, blksize,  &
 & bytes,     num_trans, &
 & readbuf_len, readbuf_is_alloc, &
 & writebuf_len, writebuf_is_alloc, &
 & prealloc, extent,   &
 & mrfs_flag, &
 & walltime,  xfer_speed, &
 & usercpu,   syscpu) 

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE YOMHOOK   ,ONLY : LHOOK,   DR_HOOK

#ifdef NAG
use f90_unix_io, only: flush
#endif

implicit none
INTEGER(KIND=JPIM), intent(in)       :: ftn_unit, cma_unit, binno, fileno
CHARACTER (LEN = *), intent(in) :: str_open_time,  str_close_time 
CHARACTER (LEN = *), intent(in) :: logical_name,   true_name
CHARACTER (LEN = *), intent(in) :: pipecmd,        cmd
INTEGER(KIND=JPIM), intent(in)       :: read_only, numbins, packmethod
INTEGER(KIND=JPIM), intent(in)       :: blocksize
INTEGER(KIND=JPIM), intent(in)       :: numddrs,   lenddrs
INTEGER(KIND=JPIM), intent(in)       :: numobs,    maxreplen,  cmalen
INTEGER(KIND=JPIM), intent(in)       :: filesize,  filepos, blksize
INTEGER(KIND=JPIM), intent(in)       :: bytes,     num_trans
INTEGER(KIND=JPIM), intent(in)       :: readbuf_len,  writebuf_len
INTEGER(KIND=JPIM), intent(in)       :: readbuf_is_alloc, writebuf_is_alloc
INTEGER(KIND=JPIM), intent(in)       :: prealloc, extent, mrfs_flag
REAL(KIND=JPRB), intent(in)          :: walltime,  xfer_speed
REAL(KIND=JPRB), intent(in)          :: usercpu,   syscpu
! === END OF INTERFACE BLOCK ===

!-----------------------------------------------------------------------
!234567890c234567890c234567890c234567890c234567890c234567890c234567890--

INTEGER(KIND=JPIM) :: io
REAL(KIND=JPRB) :: saving
character(len=20), save :: alloc_msg(0:1)
REAL(KIND=JPRB) :: ZHOOK_HANDLE
data alloc_msg/'  (NOT allocated)', '  (allocated)'/

IF (LHOOK) CALL DR_HOOK('CMA_PRT_STAT',0,ZHOOK_HANDLE)
io = ftn_unit

if (io >= 0) then
  if (binno == 0) then
    write(io,*) "*** STATISTICS for CMA-file '"// &
     & logical_name//"'" 
  else
    write(io,*)
  endif

  write(io,*) "Physical filename     : '"//true_name//"'"
  if (read_only == 1) then
    write(io,*) "Open mode             :  READ-ONLY"
  else
    write(io,*) "Open mode             :  WRITE-ONLY"
  endif
  write(io,*) "CMA-index no.         : ",cma_unit
  write(io,*) "  --- bin no.         : ",binno
  write(io,*) "No. of bins           : ",numbins
  write(io,*) "UNIX-file no.         : ",fileno
  write(io,*) "Disk blocksize        : ",blksize
  if (mrfs_flag == 1) &
   & write(io,*) "Assigned to the memory resident file system" 
  write(io,*) "Opened at "//str_open_time
  write(io,*) "Closed at "//str_close_time
         
  if (read_only == 1 .and. binno == 0) then
    write(io,*) "No. of DDRs           : ",numddrs
    write(io,*) "Total DDR length      : ",lenddrs
    write(io,*) "No. of reports        : ",numobs
    write(io,*) "Longest report (words): ",maxreplen
    write(io,*) "Report data length    : ",cmalen
  endif
         
  write(io,*) "Pipe cmd (unfiltered) : "//pipecmd
  write(io,*) "Pipe cmd (filtered)   : "//cmd
  write(io,*) "Packing method        : ",packmethod
  write(io,*) "Blocksize             : ",blocksize
  write(io,*) "File size (in bytes)  : ",filesize
  write(io,*) "File position         : ",filepos
  if ( filesize >  0 .and.  &
     & filepos  >  0 .and.  &
     & filepos  >= filesize ) then 
    saving = 100.0D0*(1.0D0 - dble(filesize)/dble(filepos))
    write(io,1) "Space saving          : ",saving
    1          format(1x,a,f12.2,'%')
  endif
  write(io,*) "Bytes processed       : ",bytes
  write(io,*) "No. of transactions   : ",num_trans
  write(io,*) "I/O-buffer size: read : ",readbuf_len, &
   & alloc_msg(readbuf_is_alloc) 
  write(io,*) "            --- write : ",writebuf_len, &
   & alloc_msg(writebuf_is_alloc) 
  write(io,*) "Preallocation & extent: ",prealloc,extent
  write(io,1000) "I/O-times: Wall clock : ", &
   & walltime," sec", &
   & xfer_speed," MB/s" 
  write(io,1000) "        --- CPU-times : ", &
   & usercpu," sec (user)", &
   & syscpu," sec (sys)" 
  1000    format(1x,a,f10.3,a,f10.3,a)

  if (binno == numbins-1) then
    write(io,*) "*** END OF STATISTICS ***"
!--   Make sure it comes to the logfile (... before an abort ... ;-)
    call flush(io)
  endif

endif
IF (LHOOK) CALL DR_HOOK('CMA_PRT_STAT',1,ZHOOK_HANDLE)

END SUBROUTINE cma_prt_stat
