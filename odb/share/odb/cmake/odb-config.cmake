# Config file for the odb package
# Defines the following variables:
#
#  ODB_INCLUDE_DIRS   - include directories
#  ODB_DEFINITIONS    - preprocessor definitions
#  ODB_LIBRARIES      - libraries to link against
#  ODB_FEATURES       - list of enabled features
#  ODB_VERSION        - version of the package
#  ODB_GIT_SHA1       - Git revision of the package
#  ODB_GIT_SHA1_SHORT - short Git revision of the package
#
# Also defines odb third-party library dependencies:
#  ODB_TPLS             - package names of  third-party library dependencies
#  ODB_TPL_INCLUDE_DIRS - include directories
#  ODB_TPL_DEFINITIONS  - preprocessor definitions
#  ODB_TPL_LIBRARIES    - libraries to link against

### compute paths

get_filename_component(ODB_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)

set( ODB_SELF_INCLUDE_DIRS "${ODB_CMAKE_DIR}/../../../include" )
set( ODB_SELF_DEFINITIONS  "LITTLE;LINUX" )
set( ODB_SELF_LIBRARIES    "odb_lib" )

set( ODB_TPLS              "Dl" )
set( ODB_TPL_INCLUDE_DIRS  "" )
set( ODB_TPL_DEFINITIONS   "" )
set( ODB_TPL_LIBRARIES     "/usr/lib64/libdl.so" )

set( ODB_VERSION           "1.0.10" )
set( ODB_GIT_SHA1          "" )
set( ODB_GIT_SHA1_SHORT    "" )

### export include paths as absolute paths

set( ODB_INCLUDE_DIRS "" )
foreach( path ${ODB_SELF_INCLUDE_DIRS} )
  get_filename_component( abspath ${path} ABSOLUTE )
  list( APPEND ODB_INCLUDE_DIRS ${abspath} )
endforeach()
list( APPEND ODB_INCLUDE_DIRS ${ODB_TPL_INCLUDE_DIRS} )

### export definitions

set( ODB_DEFINITIONS      ${ODB_SELF_DEFINITIONS} ${ODB_TPL_DEFINITIONS} )

### export list of all libraries

set( ODB_LIBRARIES        ${ODB_SELF_LIBRARIES}   ${ODB_TPL_LIBRARIES}   )

### export the features provided by the package

set( ODB_FEATURES    "TESTS;ODB_SHARED_LIBS" )
foreach( _f ${ODB_FEATURES} )
  set( ODB_HAVE_${_f} 1 )
endforeach()

# Has this configuration been exported from a build tree?
set( ODB_IS_BUILD_DIR_EXPORT OFF )

if( EXISTS ${ODB_CMAKE_DIR}/odb-import.cmake )
  set( ODB_IMPORT_FILE "${ODB_CMAKE_DIR}/odb-import.cmake" )
  include( ${ODB_IMPORT_FILE} )
endif()

# here goes the imports of the TPL's

include( ${CMAKE_CURRENT_LIST_FILE}.tpls OPTIONAL )

# insert definitions for IMPORTED targets

if( NOT odb_BINARY_DIR )

  if( ODB_IS_BUILD_DIR_EXPORT )
    include( "/hpcperm/cvah/odb/pyodb_0.1.0/build_odb/pyodb-targets.cmake" OPTIONAL )
  else()
    include( "${ODB_CMAKE_DIR}/odb-targets.cmake" OPTIONAL )
  endif()

endif()

# publish this file as imported

set( ODB_IMPORT_FILE ${CMAKE_CURRENT_LIST_FILE} )
mark_as_advanced( ODB_IMPORT_FILE )

# set odb_BASE_DIR for final installations or build directories

if( NOT odb )
  if( ODB_IS_BUILD_DIR_EXPORT )
    set( odb_BASE_DIR /hpcperm/cvah/odb/pyodb_0.1.0/build_odb )
  else()
    get_filename_component( abspath ${CMAKE_CURRENT_LIST_DIR}/../../.. ABSOLUTE )
    set( odb_BASE_DIR ${abspath} )
  endif()
endif()
