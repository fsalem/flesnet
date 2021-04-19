# Copyright 2016 Thorsten Schuett <schuett@zib.de> Updated by Farouk Salem <salem@zib.de>

file(GLOB LIB_SOURCES *.cpp providers/*.cpp dfs/*.cpp dfs/controller/*.cpp dfs/controller/fault_tolerance/*.cpp dfs/controller/interval_manager/*.cpp dfs/controller/load_balancer/*.cpp dfs/model/*.cpp dfs/model/fault_tolerance/*.cpp dfs/model/interval_manager/*.cpp dfs/model/load_balancer/*.cpp dfs/logger/*.cpp)
file(GLOB LIB_HEADERS *.hpp providers/*.hpp dfs/*.hpp dfs/controller/*.hpp dfs/controller/fault_tolerance/*.hpp dfs/controller/interval_manager/*.hpp dfs/controller/load_balancer/*.hpp dfs/model/*.hpp dfs/model/fault_tolerance/*.hpp dfs/model/interval_manager/*.hpp dfs/model/load_balancer/*.hpp dfs/logger/*.hpp)

add_library(fles_libfabric ${LIB_SOURCES} ${LIB_HEADERS})

target_compile_options(fles_libfabric
  PRIVATE -Wno-unknown-warning-option
  PRIVATE -Wno-unused-but-set-variable
  PRIVATE -Wno-unused-parameter
  PRIVATE -Wno-unused-variable
)

target_include_directories(fles_libfabric PUBLIC . dfs/ dfs/controller/ dfs/controller/fault_tolerance/ dfs/controller/interval_manager/ dfs/controller/load_balancer/ dfs/model/ dfs/model/fault_tolerance/ dfs/model/interval_manager/ dfs/model/load_balancer/ dfs/logger/)

target_include_directories(fles_libfabric SYSTEM
  PUBLIC ${LIBFABRIC_INCLUDE_DIR}
  PUBLIC ${Boost_INCLUDE_DIRS}
)

target_link_libraries(fles_libfabric
  PUBLIC fles_ipc
  PUBLIC fles_core
  PUBLIC logging
  PUBLIC ${LIBFABRIC_LIBRARY}
)
