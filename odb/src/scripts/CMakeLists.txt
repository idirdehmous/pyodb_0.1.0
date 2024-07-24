#configure_file(use_odb.csh.in ${CMAKE_BINARY_DIR}/bin/use_odb.csh @ONLY)
#configure_file(use_odb.sh.in ${CMAKE_BINARY_DIR}/bin/use_odb.sh @ONLY)

#install(PROGRAMS ${CMAKE_BINARY_DIR}/bin/use_odb.csh DESTINATION bin)
#install(PROGRAMS ${CMAKE_BINARY_DIR}/bin/use_odb.sh DESTINATION bin)

# WE NEED ONLY dcgan SCRIPT 
list(APPEND _odb_scripts

    create_ioassign
    dcafix
    dcagen
    dcaquick
    odb_prune
    odbprune
    # DEPRECATED: wrapper scripts for odb-api tools

    #    .odb_deprecated.sh
    # odb_count
    #odb_dump
    #odb_header
    #odb_set
    #odb_split
    #odb_to_request
    #odbcount
    #odbdump
    #odbheader
    #odbset
)


install(PROGRAMS ${_odb_scripts} DESTINATION bin)

foreach(script ${_odb_scripts})
    configure_file(${script} ${CMAKE_BINARY_DIR}/bin COPYONLY)
endforeach()
