

set(libs ${ODB_LIBRARIES} -Wl,--whole-archive odbmpiserial -Wl,--no-whole-archive)
include_directories(${ODB_INCLUDE_DIRS})
ecbuild_add_executable(TARGET dcagen.x  SOURCES dcagen.c     LIBS ${libs})
ecbuild_add_executable(TARGET ioassign  SOURCES ioassign.c   LIBS ${libs})

