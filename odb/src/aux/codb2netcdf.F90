SUBROUTINE codb2netcdf(title, ncfile, namecfg, sql_query, &
     & d, nra, nrows, ncols, &
     & idtnum, ipoolno, &
     & odb_type_tag, odb_name_tag, odb_nickname_tag)

USE PARKIND1  ,ONLY : JPIM     ,JPRB
USE ODBNETCDF, ONLY : to_netcdf, init_cdf 

implicit none
character(len=*), intent(in) :: title, ncfile, namecfg, sql_query
INTEGER(KIND=JPIM), intent(in) :: nra, nrows, ncols
REAL(KIND=JPRB), intent(in) :: d(nra, ncols)
INTEGER(KIND=JPIM), intent(in) :: idtnum(ncols), ipoolno
character(len=*), intent(in) :: odb_type_tag, odb_name_tag, odb_nickname_tag

INTEGER(KIND=JPIM) :: itype(ncols)
character(len=4096) types(ncols), names(ncols), nicknames(ncols)
REAL(KIND=JPRB) :: nmdi, rmdi, mdi
INTEGER(KIND=JPIM) :: istart, iend, j
character(len=1) ODB_tag_delim, env
logical LLnopacking, LLdebug_mode

CALL codb_getenv('ODB_NETCDF_NOPACKING', env) ! set ODB_NETCDF_NOPACKING=1 to see all in double or string
LLnopacking = (env == '1')

CALL codb_getenv('ODB_NETCDF_DEBUG', env)
LLdebug_mode = (env == '1')

call pcma_get_mdis(nmdi, rmdi)
mdi = abs(nmdi)

call cODB_tag_delim(ODB_tag_delim)

call init_cdf()

do j=1,ncols
  CALL dtnum_to_netcdf_type(idtnum(j), itype(j))
enddo

iend = 1
do j=1,ncols
  istart = iend + 1
  iend = iend + index(odb_type_tag(istart:),ODB_tag_delim)
  types(j) = odb_type_tag(istart:iend-1)
enddo

iend = 1
do j=1,ncols
  istart = iend + 1
  iend = iend + index(odb_name_tag(istart:),ODB_tag_delim)
  names(j) = odb_name_tag(istart:iend-1)
enddo

iend = 1
do j=1,ncols
  istart = iend + 1
  iend = iend + index(odb_nickname_tag(istart:),ODB_tag_delim)
  nicknames(j) = odb_nickname_tag(istart:iend-1)
enddo

CALL to_netcdf(trim(title), trim(ncfile), trim(namecfg), trim(sql_query), &
     & d(1:nrows, 1:ncols), &
     & itype(1:ncols), &
     & types(1:ncols), names(1:ncols), nicknames(1:ncols), &
     & mdi, ipoolno, LLnopacking, LLdebug_mode)

END SUBROUTINE codb2netcdf
