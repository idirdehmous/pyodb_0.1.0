module odbmap_reportype

use yomhook, only : lhook, dr_hook

integer(kind=4), parameter :: nvind = 2147483647

! Describes a single mapping between different codes and a reportype
type Mapping
    integer(kind=4) :: groupid
    integer(kind=4) :: satid
    integer(kind=4) :: bufrtype
    integer(kind=4) :: subtype
    integer(kind=4) :: obstype
    integer(kind=4) :: codetype
    integer(kind=4) :: sensor
    integer(kind=4) :: reportype
    integer(kind=4) :: satinst
    integer(kind=4) :: accumulation_length
end type

! Stores all code mappings from an input config file
type AllMappings
    Type(Mapping), pointer :: Entries(:)
    integer :: mdi=-1
end type

Type(AllMappings), save :: CodeMappings

contains


subroutine load_config_if_required() 
    implicit none

    character(len=128) :: config_filename

    ! Check if CodeMappings data structure has already been filled with data
    ! if CodeMappings not yet filled, then load data from the config file
    if (.not.associated(CodeMappings%Entries)) then
        call get_environment_variable("ODB_CODE_MAPPINGS", config_filename)
        call load_report_type_mappings_from_config(config_filename)
    end if


end subroutine load_config_if_required


subroutine find_obstype_codetype(bufrtype, subtype, obstype, codetype, lallsky)
    implicit none

    integer(kind=4), intent(in)  :: bufrtype
    integer(kind=4), intent(in)  :: subtype
    integer(kind=4), intent(out) :: obstype
    integer(kind=4), intent(out) :: codetype
    integer(kind=4), parameter   :: allsky_obstype = 16
    integer(kind=4)              :: i
    integer(kind=4)              :: n_matches
    integer(kind=4)              :: n_mappings
    logical, intent(in)  :: lallsky
    real(kind=8) :: hook_handle
    character(len=128), parameter :: hook_label = &
        & "odbmap_reportype:find_obstype_codetype"

    if (lhook) call dr_hook(hook_label, 0, hook_handle)
    
    ! Load data from config file if not already in-memory
    call load_config_if_required()

    obstype = CodeMappings%mdi

    ! Loop over all mappings, counting number of matches as it goes.
    n_mappings = size(CodeMappings%Entries)
    n_matches = 0

    ! Main search for matching code combinations from config file mappings.
    ! If multiple matches, then the last match is used.
    MainLoop: do i = 1,n_mappings
        if (lallsky) then
            if ((CodeMappings%Entries(i)%bufrtype == bufrtype) &
                .and. (CodeMappings%Entries(i)%subtype == subtype) &
                .and. (CodeMappings%Entries(i)%obstype == allsky_obstype)) then
                
                codetype = CodeMappings%Entries(i)%codetype
                obstype = CodeMappings%Entries(i)%obstype

                n_matches = n_matches + 1
            end if
        else
            if ((CodeMappings%Entries(i)%bufrtype == bufrtype) &
                .and. (CodeMappings%Entries(i)%subtype == subtype) &
                .and. (CodeMappings%Entries(i)%obstype /= allsky_obstype)) then
                
                codetype = CodeMappings%Entries(i)%codetype
                obstype = CodeMappings%Entries(i)%obstype

                n_matches = n_matches + 1
            end if
        end if
    end do MainLoop

    ! Special case for bit-comparability with old code: Wind profilers.
    ! Explanation: Mappings from bufrtype,subtype to obstype,codetype are not unique
    !       -old code contained repeated mappings for same bufrtype,subtype
    !         -order of if statements determined which mapping was used
    !       -new config file also contains multiple mappings for the same bufrtype,subtype
    !         -order in config file is different to that in old code
    !         -leads to differences in obs rejection (depends on codetype)
    !       -for bit-comparability, special case inserted here to give same output as old code.
    if ((bufrtype == 2) .and. (subtype == 96)) then  
        obstype = 6
        codetype = 134
    end if

    ! Exit with error if no matches, or if more than one match
    if (n_matches == 0) then
        write(0, "(2(a,i0),a,l1)") 'Error: No obstype/codetype mapping for bufrtype=', &
            & bufrtype, ", subtype=", subtype, ", all-sky=", lallsky
        call abor1("find_obstype_codetype")
    end if 

    if (lhook) call dr_hook(hook_label, 1, hook_handle)

    return

end subroutine find_obstype_codetype

subroutine find_obskind(subtype, satemKind, satobKind, conventionalKind, accumulationKind)

    implicit none

    integer(kind=4), intent(in)   :: subtype
    integer(kind=4)        :: n_mappings
    integer(kind=4)        :: i
    logical, intent(inout) :: satemKind
    logical, intent(inout) :: satobKind
    logical, intent(inout) :: conventionalKind
    logical, intent(inout) :: accumulationKind
    real(kind=8) :: hook_handle
    character(len=128), parameter :: hook_label = &
        & "odbmap_reportype:find_obskind"

    if (lhook) call dr_hook(hook_label, 0, hook_handle)

    call load_config_if_required()
    n_mappings = size(CodeMappings%Entries)

    MappingLoop: do i = 1,n_mappings
        ! Find first entry with matching subtype
        if (CodeMappings%Entries(i)%subtype == subtype)then

            if ((CodeMappings%Entries(i)%sensor /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%satid /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%bufrtype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%subtype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%obstype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%codetype /= CodeMappings%mdi).and.&
                (CodeMappings%Entries(i)%accumulation_length == CodeMappings%mdi)) then
                  satemKind = .True.
                  exit MappingLoop
            end if
            if ((CodeMappings%Entries(i)%sensor == CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%satid /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%bufrtype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%subtype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%obstype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%codetype /= CodeMappings%mdi).and.&
                (CodeMappings%Entries(i)%accumulation_length == CodeMappings%mdi)) then
                  satobKind = .True.
                  exit MappingLoop
            end if
            if ((CodeMappings%Entries(i)%sensor == CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%satid == CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%bufrtype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%subtype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%obstype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%codetype /= CodeMappings%mdi).and.&
                (CodeMappings%Entries(i)%accumulation_length == CodeMappings%mdi)) then
                  conventionalKind = .True.
                  exit MappingLoop
            end if
            if ((CodeMappings%Entries(i)%sensor == CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%satid == CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%bufrtype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%subtype /= CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%obstype == CodeMappings%mdi).and. &
                (CodeMappings%Entries(i)%codetype == CodeMappings%mdi).and.&
                (CodeMappings%Entries(i)%accumulation_length /= CodeMappings%mdi)) then
                  accumulationKind = .True.
                  exit MappingLoop
            end if
        end if
    end do MappingLoop

    ! Check that a kind has been selected
    if ((satemKind .EQV. .false.).and.(satobKind .EQV. .false.).and.(conventionalKind .EQV. .false.).and.&
       (accumulationKind .EQV. .false.)) then
        write(0, "(a,i0)") 'Error : No Kind found for subtype: ',subtype
        call abor1("find_obskind")
    end if

    if (lhook) call dr_hook(hook_label, 1, hook_handle)

end subroutine find_obskind


subroutine find_reportype(groupid, satid, bufrtype,subtype,obstype,&
  & codetype, sensor, reportype, satinst, iproduct_type, iaccumulation_length)

    implicit none

    integer(kind=4), intent(out)   :: groupid
    integer(kind=4), value   :: sensor, satid
    integer(kind=4), value   :: bufrtype, subtype, obstype, codetype
    integer(kind=4), intent(out)  :: reportype, satinst
    integer(kind=4), intent(in), OPTIONAL  :: iproduct_type
    integer(kind=4), intent(in), OPTIONAL  :: iaccumulation_length
    integer(kind=4) :: n_mappings
    integer(kind=4) :: i 
    integer(kind=4) :: n_matches
    integer(kind=4) :: product_type
    integer(kind=4) :: accumulation_length
    logical :: satemKind = .False.
    logical :: satobKind = .False.
    logical :: conventionalKind = .False.
    logical :: accumulationKind = .False.
    real(kind=8) :: hook_handle
    character(len=128) :: reason
    character(len=128), parameter :: hook_label = &
        & "odbmap_reportype:find_reportype"

    if (lhook) call dr_hook(hook_label, 0, hook_handle)

    ! Set default values for incoming optional arguments
    if (present(iproduct_type)) then
        product_type = iproduct_type
    else
        product_type = CodeMappings%mdi
    endif

    if (present(iaccumulation_length)) then
        accumulation_length = iaccumulation_length
    else
        accumulation_length = CodeMappings%mdi
    endif

    ! Initialize output variables
    reportype = CodeMappings%mdi
    satinst = CodeMappings%mdi
    groupid = CodeMappings%mdi


    ! Check incoming values are sensible
    if (abs(sensor) == nvind) sensor = CodeMappings%mdi
    if (abs(satid) == nvind) satid = CodeMappings%mdi
    if (abs(bufrtype) == nvind) bufrtype = CodeMappings%mdi
    if (abs(subtype) == nvind) subtype = CodeMappings%mdi
    if (abs(obstype) == nvind) obstype = CodeMappings%mdi
    if (abs(codetype) == nvind) codetype = CodeMappings%mdi
    if (abs(product_type) == nvind) product_type = CodeMappings%mdi
    if (abs(accumulation_length) == nvind) accumulation_length = CodeMappings%mdi

    ! Load data from config file if not already in-memory
    call load_config_if_required()

    ! Loop over all mappings, counting number of matches as it goes.
    n_mappings = size(CodeMappings%Entries)
    n_matches = 0

    ! ReportTypes come in several different Kinds: SatobKind, SatemKind, ConventionalKind, AccumulationKind.
    ! Different codes are used in the mappings for each kind.
    ! Here, the kind must first be determined from the subtype using the mappings in the config file.
    ! Then, different checks and return values are set depending on the kind.
    call find_obskind(subtype, satemKind, satobKind, conventionalKind, accumulationKind)

    ! Special case. Reportype 7003 GRACE A GPSRO is of kind Satem, however when this routine is called it isn't passed sensor or satinst.
    !               So, we need to treat it as a satob kind which doesn't have the sensor checks.
    !               To deal with these situations more generally, we check if sensor is passed in or not.
    if ((satemKind).and.(sensor == CodeMappings%mdi))then
      satemKind = .false.
      satobkind = .true.
    end if

    ! Main search for matching code combinations from config file mappings
    EntryLoop: do i = 1,n_mappings
        if (conventionalKind) then
                if ((CodeMappings%Entries(i)%bufrtype == bufrtype) &
                    .and. (CodeMappings%Entries(i)%subtype == subtype) &
                    .and. (CodeMappings%Entries(i)%obstype == obstype) &
                    .and. (CodeMappings%Entries(i)%codetype == codetype)) then

                    groupid = CodeMappings%Entries(i)%groupid
                    reportype = CodeMappings%Entries(i)%reportype

                    n_matches = n_matches + 1
                end if
        else if (accumulationKind) then
                if ((CodeMappings%Entries(i)%bufrtype == bufrtype) &
                    .and. (CodeMappings%Entries(i)%subtype == subtype) &
                    .and. (CodeMappings%Entries(i)%accumulation_length == accumulation_length)) then
                    
                    groupid = CodeMappings%Entries(i)%groupid
                    reportype = CodeMappings%Entries(i)%reportype

                    n_matches = n_matches + 1
                end if
        else if (satemKind) then
            if ((CodeMappings%Entries(i)%sensor == sensor) &
                .and. (CodeMappings%Entries(i)%satid == satid) &
                .and. (CodeMappings%Entries(i)%bufrtype == bufrtype) &
                .and. (CodeMappings%Entries(i)%subtype == subtype) &
                .and. (CodeMappings%Entries(i)%obstype == obstype) &
                .and. (CodeMappings%Entries(i)%codetype == codetype)) then
                
                groupid = CodeMappings%Entries(i)%groupid
                reportype = CodeMappings%Entries(i)%reportype
                satinst = CodeMappings%Entries(i)%satinst

                n_matches = n_matches + 1
            end if
       else if (satobKind) then
            if ((CodeMappings%Entries(i)%satid == satid) &
                .and. (CodeMappings%Entries(i)%bufrtype == bufrtype) &
                .and. (CodeMappings%Entries(i)%subtype == subtype) &
                .and. (CodeMappings%Entries(i)%obstype == obstype) &
                .and. (CodeMappings%Entries(i)%codetype == codetype)) then
                
                groupid = CodeMappings%Entries(i)%groupid
                reportype = CodeMappings%Entries(i)%reportype
                satinst = CodeMappings%Entries(i)%satinst

                n_matches = n_matches + 1
            end if
      end if
    end do EntryLoop

    ! Exit with error if no matches, or if more than one match
    if (n_matches /= 1) then

        if (n_matches == 0) then
            write(reason, "(a)") 'Error: No reportype mapping: '
        else if (n_matches > 1) then
            write(reason ,"(a)") 'Error: Non-unique reportype mapping: '
        end if

        write(0, "(a,10(a,i0))") trim(reason), " reportype=", reportype, &
            & ", groupid=", groupid, ", bufrtype=", bufrtype, ", subtype=", subtype, &
            & ", obstype=", obstype, ", codetype=", codetype, ", sensor=", sensor, &
            & ", satinst=", satinst, ", satid=", satid, ", accumulation_length=", &
            & accumulation_length

        call abor1("find_reportype")
    end if 

    if (lhook) call dr_hook(hook_label, 1, hook_handle)

    return

end subroutine find_reportype

function count_lines_in_file(file_handle) result(n_lines)
    ! Counts the number of lines in the file

    implicit none

    integer(kind=4), intent(in) :: file_handle
    integer(kind=4) :: RetCode
    integer(kind=4) :: n_lines

    n_lines = 0
    LineCount: do 
        read(file_handle,*, iostat = RetCode)
        if (RetCode < 0) exit
        n_lines = n_lines + 1
    end do LineCount

    rewind(file_handle)

end function count_lines_in_file

subroutine load_report_type_mappings_from_config(config_filename)
    ! Loads reportype mapping data from ODB-Gov config file
    implicit none

    character(len=*), intent(in) :: config_filename

    integer(kind=4) :: i
    integer(kind=4) :: file_handle
    integer(kind=4) :: n_mappings
    integer(kind=4) :: n_lines
    integer(kind=4) :: sensor
    integer(kind=4) :: satid
    integer(kind=4) :: bufrtype
    integer(kind=4) :: subtype
    integer(kind=4) :: obstype
    integer(kind=4) :: codetype
    integer(kind=4) :: accumulation_length
    integer(kind=4) :: groupid
    integer(kind=4) :: reportype
    integer(kind=4) :: satinst
    integer(kind=4) :: RetCode
    integer(kind=4) :: default_value
    real(kind=8) :: hook_handle
    character(len=128), parameter :: hook_label = &
        & "odbmap_reportype:load_repor_type_mappings_from_config"

    if (lhook) call dr_hook(hook_label, 0, hook_handle)

    default_value = CodeMappings%mdi

    file_handle = 10
    open(unit=file_handle,file=config_filename,status='old',action='read',iostat=RetCode)
    if (RetCode /= 0)then
      write(0,*) 'Error opening ODB reportype mapping config file : filename, retcode ',trim(config_filename),RetCode
      call abor1("Error opening ODB reportype mapping config file")
    end if

    i = 1

    ! Count the number of lines in the file
    n_lines = count_lines_in_file(file_handle)
    n_mappings = n_lines - 1 ! First line in config file is header

    allocate(CodeMappings%Entries(n_mappings))

    ReadLoop: do i = 0,n_mappings
        if (i==0) then
            ! Ignore the first line - contains column headings, no data.
            read(file_handle,*)
        else

            ! Read columns from file
            read(file_handle,*,iostat=RetCode) reportype, groupid, bufrtype, subtype, obstype,&
                     & codetype, sensor, satinst, satid, accumulation_length

            if ( RetCode == -1) exit ReadLoop ! End of file

            if ( RetCode == 0 ) then
                CodeMappings%Entries(i)%sensor = sensor
                CodeMappings%Entries(i)%satid = satid
                CodeMappings%Entries(i)%bufrtype = bufrtype
                CodeMappings%Entries(i)%subtype = subtype
                CodeMappings%Entries(i)%obstype = obstype
                CodeMappings%Entries(i)%codetype = codetype
                CodeMappings%Entries(i)%accumulation_length = accumulation_length
                CodeMappings%Entries(i)%groupid = groupid
                CodeMappings%Entries(i)%reportype = reportype
                CodeMappings%Entries(i)%satinst = satinst
            end if
        end if
    end do ReadLoop
  
    close(file_handle)

    if (lhook) call dr_hook(hook_label, 1, hook_handle)

end subroutine load_report_type_mappings_from_config

end module odbmap_reportype
