list(APPEND module_files

    module/odb.F90
    module/odb_module.F90
    module/odb_module8.F90
    module/odbgetput.F90
    module/odbio_msgpass.F90
    module/odbiomap.F90
    module/odbmap_reportype.F90
    module/odbmp.F90
    #    module/odbnetcdf.F90
    module/odbprint.F90
    module/odbshared.F90
    module/odbsort.F90
    module/odbstat.F90
    module/odbutil.F90
    module/stackdb.F90
    module/str.F90
)

set(odb_module_files ${module_files} PARENT_SCOPE)
