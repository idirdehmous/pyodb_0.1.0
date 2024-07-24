list(APPEND lib_files

    lib/ae_dump.c
    lib/aggr.c
    lib/alist.c
    lib/append_num.F90
    lib/apply_poolmasking.F90
    lib/bsslzr_odb.F90
    lib/ckeysort.F90
    lib/cmdbkeys.c
    lib/codb.c
    lib/codb_distribute.F90
    lib/codb_truename.c
    lib/cread_iomap.c
    lib/create_iomap.F90
    lib/create_statid.F90
    lib/ctransfer.c
    lib/ctx.c
    lib/datastream.F90
    lib/dynlink.c
    lib/eq_regions.c
    lib/errtrap.c
    lib/evaluate.c
    lib/fodb_checkviewreg.F90
    lib/fodb_propagate_env.F90
    lib/forfunc.c
    lib/funcs.c
    lib/fwrite_iomap.F90
    lib/gauaw_odb.F90
    lib/hashing.c
    lib/infile.c
    lib/init_region.c
    lib/inside.c
    lib/iolockdb.F90
    lib/lnkdb.F90
    lib/lnkdb2.F90
    lib/msgpass_loaddata.F90
    lib/msgpass_loadobs.F90
    lib/msgpass_storedata.F90
    lib/msgpass_storeobs.F90
    lib/odb_array_dump.F90
    lib/orlist.c
    lib/peinfo.c
    lib/poolmasking.c
    lib/poolreg.c
    lib/prt.c
    lib/random_odb.c
    lib/rgg_regions.c
    lib/rlnkdb.F90
    lib/rlnkdb2.F90
    lib/rot.c
    lib/rsort32_odb.c
    lib/savelist.c
    lib/setup_sort.c
    lib/solar.c
    lib/stack.c
    lib/static.c
    lib/symtab.c
    lib/tracing.c
    lib/twindow.c
    lib/uniquenumdb.F90
    lib/var.c
    lib/vecloops.c
    lib/version.c
    lib/wildcard.c
    lib/write_ddl.c
    lib/pydump.c  
)

set(odb_lib_files ${lib_files} PARENT_SCOPE)

#set(ODB_INCLUDE_DIRS /home/micro/Bureau/pkg/src/odb_api/odb/src/include;${CMAKE_CURRENT_SOURCE_DIR}/src/include;${CMAKE_Fortran_MODULE_DIRECTORY})
	
list(APPEND _odb_dummy_sources
    Dummies.c
    Codb_set_entrypoint.c
    Ctxprint.F90
    Dummyprofile_newio.c
)

ecbuild_add_library(TARGET odbdummy
    PRIVATE_INCLUDES ${ODB_INCLUDE_DIRS}
    SOURCES ${_odb_dummy_sources}
    LIBS odbifsaux)
