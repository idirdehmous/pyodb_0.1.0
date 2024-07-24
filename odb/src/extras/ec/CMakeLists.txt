list(APPEND _odb_ec_sources

    eclib.h
    eclib_c.c
    error.c
    julian.h
    julian_lib.c
    myhdr.h
)

if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    add_definitions(-D_ABI64)
endif()

ecbuild_add_library(TARGET odbec SOURCES ${_odb_ec_sources})
