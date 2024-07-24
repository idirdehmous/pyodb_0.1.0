list(APPEND include_files

    alloc.h
    bits.h
    cdrhook.h
    cma_read.h
    cma_seek.h
    cma_write.h
    cmaio.h
    codb.h
    codb_netcdf.h
    dca.h
    defs.h
    ecstdlib.h
    evaluate.h
    fodb.h
    fodbmp.h
    fodbmp1.h
    fodbmp2.h
    fodbutil.h
    fortint.h
    funcs.h
    history.h
    idx.h
    info.h
    ioassign.h
    iostuff.h
    lzw.h
    magicwords.h
    memmap.h
    mr2d_hdr.h
    mysql.h
    newio.h
    node.h
    odb.h
    odb_ioprof.h
    odb_macros.h
    odbcrc.h
    odbcs.h
    odbcsdefs.h
    odbcstags.h
    odbdump.h
    odbi.h
    odbi_cs.h
    odbi_direct.h
    odbi_struct.h
    odbmd5.h
    pcma.h
    pcma_extern.h
    privpub.h
    qtar.h
    regcache.h
    result.h
    setodbcs.h
    static.h
    swapbytes.h
    symtab.h
    vparam.h
)

foreach(file ${include_files})
    configure_file(${file} ${CMAKE_BINARY_DIR}/include/${file} COPYONLY)
endforeach()

install(FILES ${include_files} DESTINATION include)
