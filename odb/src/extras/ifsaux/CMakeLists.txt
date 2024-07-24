
list(APPEND _odb_ifsaux_includes

    include/cargs.h
    include/crc.h
    include/dr_hook_util.h
    include/dr_hook_util_multi.h
    include/drhook.h
    include/ecsort_shared.h
    include/getstatm.h
    include/intercept_alloc.h
    include/raise.h
    include/user_clock.h
)

list(APPEND _odb_ifsaux_sources

    module/parkind1.F90
    module/parkind2.F90
    module/dr_hook_watch_mod.F90
    module/ecsort_mix.F90
    module/f90_unix_env.F90
    module/f90_unix_io.F90
    module/f90_unix_proc.F90
    module/local_trafos.F90
    module/mpi4to8.F90
    module/mpi4to8_m.F90
    module/mpi4to8_s.F90
    module/mpl_abort_mod.F90
    module/mpl_allgather_mod.F90
    module/mpl_allgatherv_mod.F90
    module/mpl_allreduce_mod.F90
    module/mpl_alltoallv_mod.F90
    module/mpl_arg_mod.F90
    module/mpl_barrier_mod.F90
    module/mpl_broadcast_mod.F90
    module/mpl_buffer_method_mod.F90
    module/mpl_close_mod.F90
    module/mpl_comm_create_mod.F90
    module/mpl_data_module.F90
    module/mpl_end_mod.F90
    module/mpl_gatherv_mod.F90
    module/mpl_groups.F90
    module/mpl_init_mod.F90
    module/mpl_ioinit_mod.F90
    module/mpl_locomm_create_mod.F90
    module/mpl_message_mod.F90
    module/mpl_module.F90
    module/mpl_mpif.F90
    module/mpl_mygatherv_mod.F90
    module/mpl_myrank_mod.F90
    module/mpl_nproc_mod.F90
    module/mpl_open_mod.F90
    module/mpl_probe_mod.F90
    module/mpl_read_mod.F90
    module/mpl_recv_mod.F90
    module/mpl_scatterv_mod.F90
    module/mpl_send_mod.F90
    module/mpl_setdflt_comm_mod.F90
    module/mpl_tour_table_mod.F90
    module/mpl_wait_mod.F90
    module/mpl_write_mod.F90
    module/oml_mod.F90
    module/sdl_mod.F90
    module/strhandler_mod.F90
    module/yomabrt.F90
    module/yomerrtrap.F90
    module/yomgstats.F90
    module/yomhook.F90
    module/yomhookstack.F90
    module/yomlun_ifsaux.F90
    module/yommpi.F90
    parallel/cmpl_binding.F90
    parallel/coml_binding.F90
    support/abor1.F90
    support/cargs.c
    support/cdrhookinit.F90
    support/clock.F
    support/convin.F90
    support/convout.F90
    support/cptime.F
    support/crc.c
    support/dr_hook_procinfo.F90
    support/dr_hook_prt.F90
    support/dr_hook_util.F90
    support/dr_hook_util_multi.F90
    support/drhook.c
    support/endian.c
    support/env.c
    support/gstats.F90
    support/mpe_locking.c
    support/my_sync.c
    support/timef.F
    support/user_clock.F90
    utilities/addrdiff.c
    utilities/countingsort.c
    utilities/ecqsort.c
    utilities/fnecsx.c
    utilities/gentrbk.F90
    utilities/get_max_threads.F90
    utilities/get_num_threads.F90
    utilities/get_thread_id.F90
    utilities/getcurheap.c
    utilities/getheapstat.F90
    utilities/gethwm.c
    utilities/getmemstat.F90
    utilities/getmemvals.F90
    utilities/getpag.c
    utilities/getrss.c
    utilities/getstackusage.c
    utilities/getstatm.c
    utilities/getstk.c
    utilities/gnomesort.c
    utilities/iusrcl.F
    utilities/jsort.F
    utilities/linuxtrbk.c
    utilities/rsort32.c
    utilities/rsort64.c
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
#add_library( TARGET odbifsaux  SOURCES ${_odb_ifsaux_sources}  )

ecbuild_add_library(TARGET odbifsaux
    SOURCES ${_odb_ifsaux_sources}
    PRIVATE_INCLUDES ${_ODB_MPI_INCLUDE_DIRS}
    LIBS ${_ODB_MPI_LIBS} ${_ODB_EMOS_LIBS})

#install(FILES ${_odb_ifsaux_includes} DESTINATION include)
