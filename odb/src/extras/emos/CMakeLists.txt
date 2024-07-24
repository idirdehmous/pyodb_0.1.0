list(APPEND _odb_emos_sources

  bufrgrib.h
  fileRead.h
  fort2c.h
  pbio.c
  readprod.c
  sizeRoutines.h
)

ecbuild_add_library(TARGET odbemos
  DEFINITIONS INTEGER_IS_INT
  PRIVATE_INCLUDES ${ODB_INCLUDE_DIRS}
  SOURCES ${_odb_emos_sources}
  CONDITION NOT HAVE_ODB_EMOS)

if(NOT HAVE_ODB_EMOS)
  set(_ODB_EMOS_LIBS odbemos PARENT_SCOPE)
else()
  set(_ODB_EMOS_LIBS ${LIBEMOS_LIBRARIES} PARENT_SCOPE)
endif()
