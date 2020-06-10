# Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

find_package(MPI REQUIRED)
if (MPI_FOUND)
	MESSAGE(STATUS "MPI           = ${MPI_LIBRARY}")
	MESSAGE(STATUS "MPI           = ${MPI_INCLUDE_DIR}")

	SET(CMAKE_C_COMPILER mpicc)
	SET(CMAKE_CXX_COMPILER mpicxx)

	include_directories(SYSTEM ${MPI_INCLUDE_DIR})
	add_definitions(-DOMPI_SKIP_MPICXX)

	include(FindPackageHandleStandardArgs)
	find_package_handle_standard_args(MPI REQUIRED_VARS MPI_LIBRARY MPI_INCLUDE_DIR)
else (MPI_FOUND)
    message(SEND_ERROR "This application cannot compile without MPI")
endif (MPI_FOUND)