
add_subdirectory(aux)
add_subdirectory(compiler)
add_subdirectory(extras)
add_subdirectory(include)
add_subdirectory(lib)
add_subdirectory(module)
add_subdirectory(scripts)
add_subdirectory(tools)

if(CMAKE_SIZEOF_VOID_P EQUAL 8)
  set_property(SOURCE lib/twindow.c PROPERTY COMPILE_DEFINITIONS _ABI64)
endif()

#set_property(SOURCE aux/codb_netcdf.c aux/result.c aux/dtnum.c
#  PROPERTY COMPILE_DEFINITIONS HAS_NETCDF=0) # needed by include/codb_netcdf.h

ecbuild_add_library(TARGET odb_lib OUTPUT_NAME odb
  SOURCES ${odb_aux_files} 
          ${odb_lib_files} 
	  ${odb_module_files}
  PRIVATE_INCLUDES ${ODB_INCLUDE_DIRS}
                   ${CMAKE_CURRENT_SOURCE_DIR}/interface
                   ${CMAKE_CURRENT_SOURCE_DIR}/extras/ifsaux/include
  LIBS odbifsaux odbec odbdummy ${DL_LIBRARIES})

# Install Fortran modules

install(CODE "EXECUTE_PROCESS (COMMAND \"${CMAKE_COMMAND}\" -E copy_directory \"${CMAKE_Fortran_MODULE_DIRECTORY}/\${CMAKE_CFG_INTDIR}\" \"${CMAKE_INSTALL_PREFIX}/module\")" )
