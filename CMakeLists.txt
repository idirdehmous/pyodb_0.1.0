cmake_minimum_required(VERSION 3.15)
cmake_policy(SET CMP0002 NEW)
project( pyodb )

find_package(PythonInterp   REQUIRED)
find_package(PythonLibs     REQUIRED)



# This comes to hand if we also need to use the NumPy C API
exec_program(${PYTHON_EXECUTABLE}
             ARGS "-c \"import numpy; print(numpy.get_include())\""
             OUTPUT_VARIABLE NUMPY_INCLUDE_DIR
             RETURN_VALUE NUMPY_NOT_FOUND
            )
if(NUMPY_NOT_FOUND)
    message(FATAL_ERROR "NumPy headers not found")
endif()


set ( MACROS_DIR     ${CMAKE_SOURCE_DIR}/cmake   )
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
list( APPEND ${CMAKE_MODULE_PATH}    ${MACROS_DIR}      )

include(ecbuild_system)
include(odb_add_schema)
include(odb_link_schemas)
include(odb_set_environment)

# COMPILE ODB 
add_subdirectory (odb)

# DEFINE INSTALLATION DIR FOR setup.py 
file( WRITE  ${CMAKE_CURRENT_SOURCE_DIR}/odb_install_dir  ${CMAKE_INSTALL_PREFIX})


