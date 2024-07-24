#----------------------------------------------------------------
# Generated CMake target import file for configuration "RelWithDebInfo".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "odbsqlcompiler" for configuration "RelWithDebInfo"
set_property(TARGET odbsqlcompiler APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(odbsqlcompiler PROPERTIES
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libodbsqlcompiler.so"
  IMPORTED_SONAME_RELWITHDEBINFO "libodbsqlcompiler.so"
  )

list(APPEND _IMPORT_CHECK_TARGETS odbsqlcompiler )
list(APPEND _IMPORT_CHECK_FILES_FOR_odbsqlcompiler "${_IMPORT_PREFIX}/lib/libodbsqlcompiler.so" )

# Import target "odb98.x" for configuration "RelWithDebInfo"
set_property(TARGET odb98.x APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(odb98.x PROPERTIES
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/bin/odb98.x"
  )

list(APPEND _IMPORT_CHECK_TARGETS odb98.x )
list(APPEND _IMPORT_CHECK_FILES_FOR_odb98.x "${_IMPORT_PREFIX}/bin/odb98.x" )

# Import target "odbec" for configuration "RelWithDebInfo"
set_property(TARGET odbec APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(odbec PROPERTIES
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libodbec.so"
  IMPORTED_SONAME_RELWITHDEBINFO "libodbec.so"
  )

list(APPEND _IMPORT_CHECK_TARGETS odbec )
list(APPEND _IMPORT_CHECK_FILES_FOR_odbec "${_IMPORT_PREFIX}/lib/libodbec.so" )

# Import target "odbmpiserial" for configuration "RelWithDebInfo"
set_property(TARGET odbmpiserial APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(odbmpiserial PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELWITHDEBINFO "C;Fortran"
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libodbmpiserial.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS odbmpiserial )
list(APPEND _IMPORT_CHECK_FILES_FOR_odbmpiserial "${_IMPORT_PREFIX}/lib/libodbmpiserial.a" )

# Import target "odbemos" for configuration "RelWithDebInfo"
set_property(TARGET odbemos APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(odbemos PROPERTIES
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libodbemos.so"
  IMPORTED_SONAME_RELWITHDEBINFO "libodbemos.so"
  )

list(APPEND _IMPORT_CHECK_TARGETS odbemos )
list(APPEND _IMPORT_CHECK_FILES_FOR_odbemos "${_IMPORT_PREFIX}/lib/libodbemos.so" )

# Import target "odbifsaux" for configuration "RelWithDebInfo"
set_property(TARGET odbifsaux APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(odbifsaux PROPERTIES
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libodbifsaux.so"
  IMPORTED_SONAME_RELWITHDEBINFO "libodbifsaux.so"
  )

list(APPEND _IMPORT_CHECK_TARGETS odbifsaux )
list(APPEND _IMPORT_CHECK_FILES_FOR_odbifsaux "${_IMPORT_PREFIX}/lib/libodbifsaux.so" )

# Import target "odbdummy" for configuration "RelWithDebInfo"
set_property(TARGET odbdummy APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(odbdummy PROPERTIES
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libodbdummy.so"
  IMPORTED_SONAME_RELWITHDEBINFO "libodbdummy.so"
  )

list(APPEND _IMPORT_CHECK_TARGETS odbdummy )
list(APPEND _IMPORT_CHECK_FILES_FOR_odbdummy "${_IMPORT_PREFIX}/lib/libodbdummy.so" )

# Import target "dcagen.x" for configuration "RelWithDebInfo"
set_property(TARGET dcagen.x APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(dcagen.x PROPERTIES
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/bin/dcagen.x"
  )

list(APPEND _IMPORT_CHECK_TARGETS dcagen.x )
list(APPEND _IMPORT_CHECK_FILES_FOR_dcagen.x "${_IMPORT_PREFIX}/bin/dcagen.x" )

# Import target "odb_lib" for configuration "RelWithDebInfo"
set_property(TARGET odb_lib APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(odb_lib PROPERTIES
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libodb.so"
  IMPORTED_SONAME_RELWITHDEBINFO "libodb.so"
  )

list(APPEND _IMPORT_CHECK_TARGETS odb_lib )
list(APPEND _IMPORT_CHECK_FILES_FOR_odb_lib "${_IMPORT_PREFIX}/lib/libodb.so" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
