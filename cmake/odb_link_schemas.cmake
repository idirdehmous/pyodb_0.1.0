# Link a target to given ODB schemas
#
# Usage: odb_link_schemas(<target> <schema1> [<schema2> ...])
#
# This function generates a special glue code that needs to be linked to a
# program in order to register the listed schemas with ODB library (see ODB
# User Guide 2.2.1 for more details).
#
# Note that this function must be called from target's source directory.
#
# TODO: Add the possibility to invoke this function  multiple times for the
# same target. This would allow to add new schemas iteratively in a similar
# fashion target_link_libraries allows to add libraries to the same target.
# Currently, this function can be invoked only once per target.

function(odb_link_schemas target)

    if(NOT target)
        message(FATAL_ERROR "odb_link_schemas: <target> not specified")
    endif()

    set(schemas ${ARGN})

    if(NOT schemas)
        return()
    endif()

    list(REMOVE_DUPLICATES schemas)

    # Inspect target's condition file, if present

    set(target_condition_file "${CMAKE_CURRENT_BINARY_DIR}/set_${target}_condition.cmake")

    if(EXISTS ${target_condition_file})
        include(${target_condition_file})
        if(NOT _${target}_condition)
            return()
        endif()
    endif()

    # Generate a glue source file

    set(glue_source ${target}_glue.c)

    add_custom_command(OUTPUT ${glue_source}
        COMMAND ${CMAKE_COMMAND} -D OUTPUT=${glue_source} -D SCHEMAS="${schemas}"
            -P ${ODB_CMAKE_DIR}/odb_create_glue.cmake)

    # This compiles the glue source without creating an actual library

    add_library(${target}_glue OBJECT ${glue_source})

    # Get hold of the compiled glue object file

    set(glue_object ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/${target}_glue.dir/${glue_source}${CMAKE_C_OUTPUT_EXTENSION})

    # Add glue object to the list of target's link libraries. Note that glue
    # object must precede ODB libraries in order to override the default
    # implementation of codb_set_entrypoint_ from odb/lib/static.c.

    get_target_property(libs ${target} LINK_LIBRARIES)

    list(INSERT libs 0 -Wl,-z,muldefs ${glue_object})

    set_property(TARGET ${target} PROPERTY LINK_LIBRARIES ${libs})

    # Finaly, link the schema libraries

    add_dependencies(${target} ${target}_glue)
    target_link_libraries(${target} ${schemas})

endfunction()
