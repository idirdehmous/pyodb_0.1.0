cmake_minimum_required(VERSION 3.15)
cmake_policy(SET CMP0002 NEW)
project( pyodb )

# DEPRECATED cmake >=3.20
#find_package(PythonInterp   REQUIRED)
#find_package(PythonLibs     REQUIRED)
find_package (Python3 COMPONENTS Interpreter Development)


# This comes to hand if we also need to use the NumPy C API
exec_program(${PYTHON_EXECUTABLE}
             ARGS "-c \"import numpy; print(numpy.get_include())\""
             OUTPUT_VARIABLE NUMPY_INCLUDE_DIR
             RETURN_VALUE NUMPY_NOT_FOUND
            )


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

set (CMD   "   cd ${CMAKE_CURRENT_SOURCE_DIR} \n   python setup.py build\n   sudo python  setup.py  install" )
message ("----------------------------------------------------------" )
message ("   ")
message ("      *The variable CMAKE_INSTALL_PREFIX=" ${CMAKE_INSTALL_PREFIX} "\n      *will be written into the file : ${CMAKE_CURRENT_SOURCE_DIR}/odb_install_dir" )
message ("      *THE CONFIGURATION SUCCEDED  ? \n        make -j Ncpu && make install \n")
message ("      *BUILD pyodb MODULE WITH  :    \n${CMD}" )
message ("----------------------------------------------------------")
message ("   ")
file( WRITE  ${CMAKE_CURRENT_SOURCE_DIR}/odb_install_dir  ${CMAKE_INSTALL_PREFIX})
