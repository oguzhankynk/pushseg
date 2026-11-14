include(GoogleTest)

add_executable(pushseg_unit_tests
  unit/test_version.cpp
  unit/test_config.cpp
  unit/test_wire.cpp
  unit/test_transfer.cpp
  unit/test_api.cpp
)

target_link_libraries(pushseg_unit_tests PRIVATE pushseg::pushseg GTest::gtest GTest::gtest_main)
# Tests also exercise internal pieces (wire decoder).
target_include_directories(pushseg_unit_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)

pushseg_set_warnings(pushseg_unit_tests)
pushseg_enable_sanitizers(pushseg_unit_tests)
pushseg_enable_coverage(pushseg_unit_tests)
pushseg_apply_sanitizer_test_wrapper(pushseg_unit_tests)

gtest_discover_tests(pushseg_unit_tests)

pushseg_add_coverage_target()
if(TARGET coverage)
  add_dependencies(coverage pushseg_unit_tests)
endif()
