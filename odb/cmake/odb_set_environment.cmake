macro(odb_set_environment)

    set(options)
    set(single_value_args ROOT)
    set(multi_value_args)

    set(_ARGN ${ARGN}) # convert ARGN into a list
    list(GET _ARGN 0 _output_variable) # and get the first argument

    cmake_parse_arguments(_PAR "${options}" "${single_value_args}" "${multi_value_args}" ${ARGN})

    if(NOT _PAR_ROOT)
        message(FATAL_ERROR "odb_set_environment: ROOT argument not specified")
    endif()

    set(_environment

        ODB_ROOT=${_PAR_ROOT}
        ODB_SYSPATH=${_PAR_ROOT}/include
        ODB_BINPATH=${_PAR_ROOT}/bin
        ODB_BEBINPATH=${_PAR_ROOT}/bin
        ODB_FEBINPATH=${_PAR_ROOT}/bin
        ODB_LIBPATH=${_PAR_ROOT}/lib
        ODB_RTABLE_PATH=${_PAR_ROOT}/share/odb
        ODB_SYSDBPATH=${_PAR_ROOT}/share/odb

        # Run-time compilation and linking

        ODB_CC=${CMAKE_C_COMPILER}\ ${CMAKE_C_FLAGS}\ -I${_PAR_ROOT}/include
        ODB_F90=${CMAKE_Fortran_COMPILER}\ ${CMAKE_Fortran_FLAGS}\ -I${_PAR_ROOT}/include\ -I${_PAR_ROOT}/module\ -I${_PAR_ROOT}/odb/module
        ODB_COMPILER=${_PAR_ROOT}/bin/odb98.x\ -V\ -O3
        ODB_COMPILER_FLAGS=${_PAR_ROOT}/share/odb/odb98.flags

        ODB_STATIC_LINKING=1
        ODB_LD_SHARED=none
        ODB_LD_SHARED_SFX=${CMAKE_SHARED_LIBRARY_SUFFIX}

        ODB_IOASSIGN_MAXPROC=32
        ODB_IOASSIGN_PARAMS=-r\ 1m\ -w\ 1m

        ODB_AR=${ODB_AR}
        ODB_GZIP=${ODB_GZIP}
        ODB_GUNZIP=${ODB_GUNZIP}

        ODB_SETUP_SHELL=/bin/sh
    )

    set(${_output_variable} ${_environment} CACHE INTERNAL "Environment variables used by ODB at run-time")

endmacro()
