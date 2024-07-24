list(APPEND _odb_mpi_serial_sources

    mpif.h
    mpif.h.necsx.BoM
    mpif.h.necsx5.CSCS

    mpi_abort.F
    mpi_allgatherv.F
    mpi_allreduce.F
    mpi_alltoallv.F
    mpi_barrier.F
    mpi_bcast.F
    mpi_bsend.F
    mpi_buffer_attach.F
    mpi_buffer_detach.F
    mpi_cart_coords.F
    mpi_cart_create.F
    mpi_cart_rank.F
    mpi_cart_sub.F
    mpi_comm_create.F
    mpi_comm_group.F
    mpi_comm_rank.F
    mpi_comm_size.F
    mpi_comm_split.F
    mpi_end.F
    mpi_error_string.F
    mpi_finalize.F
    mpi_finalized.F
    mpi_gather.F
    mpi_gatherv.F
    mpi_get_count.F
    mpi_group_incl.F
    mpi_ibsend.F
    mpi_init.F
    mpi_initialized.F
    mpi_iprobe.F
    mpi_irecv.F
    mpi_isend.F
    mpi_probe.F
    mpi_recv.F
    mpi_scatterv.F
    mpi_send.F
    mpi_ssend.F
    mpi_wait.F
    mpi_waitall.F
    tracecalls.c
    vpp_abort.F
    vpp_barrier.F
    vpp_sempost.c
    vpp_semwait.c
)

ecbuild_add_library(TARGET odbmpiserial
  SOURCES ${_odb_mpi_serial_sources}
  TYPE STATIC)

if(NOT HAVE_ODB_MPI)
  set(_ODB_MPI_LIBS odbmpiserial PARENT_SCOPE)
  set(_ODB_MPI_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR} PARENT_SCOPE)
else()
  set(_ODB_MPI_LIBS ${MPI_Fortran_LIBRARIES} PARENT_SCOPE)
  set(_ODB_MPI_INCLUDE_DIRS ${MPI_Fortran_INCLUDE_PATH} PARENT_SCOPE)
endif()
