configure_file(version.cpp.in "${CMAKE_CURRENT_BINARY_DIR}/version.cpp" @ONLY)

find_package(Threads REQUIRED)

add_library(pushseg
  "${CMAKE_CURRENT_BINARY_DIR}/version.cpp"
  core/status.cpp
  core/config.cpp
  core/registry.cpp
  core/wire.cpp
  core/context.cpp
  core/stream_inbound.cpp
  core/stream_conn.cpp
  core/mapped_conn.cpp
  backends/tcp.cpp
  backends/shm.cpp
  backends/ntb.cpp
  backends/loopmem.cpp
)
add_library(pushseg::pushseg ALIAS pushseg)

target_compile_features(pushseg PUBLIC cxx_std_17)

target_include_directories(pushseg
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# rt: POSIX message queues and shared memory (a stub on glibc >= 2.34, needed on older ones).
target_link_libraries(pushseg PUBLIC Threads::Threads rt)

pushseg_set_warnings(pushseg)
pushseg_enable_sanitizers(pushseg)
pushseg_enable_coverage(pushseg)
pushseg_enable_clang_tidy(pushseg)
pushseg_enable_cppcheck(pushseg)
