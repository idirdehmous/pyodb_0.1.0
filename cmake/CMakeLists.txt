file(GLOB cmake_files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cmake" "*.py")

# Copy files to build tree so they're added to the CMAKE_MODULE_PATH by
# odb-import.cmake when odb is picked up from a build tree
file(COPY ${cmake_files} DESTINATION ${PROJECT_BINARY_DIR})
install(FILES ${cmake_files} DESTINATION ${INSTALL_DATA_DIR}/cmake)

ecbuild_add_resources(TARGET odb_cmake_files SOURCES_PACK ${cmake_files})
