install(DIRECTORY odb DESTINATION ${INSTALL_DATA_DIR}/..)
file(GLOB_RECURSE odb_support_files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "odb/*")
ecbuild_add_resources(TARGET ${PROJECT_NAME}_odb_support_files SOURCES_PACK ${odb_support_files})
