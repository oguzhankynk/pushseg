include_guard(GLOBAL)

## pushseg_enable_clang_tidy(<target>)
#
# Sets CXX_CLANG_TIDY on <target> when PUSHSEG_ENABLE_CLANG_TIDY is ON, so
# clang-tidy runs as part of the normal build for that target.
function(pushseg_enable_clang_tidy target)
  if(NOT PUSHSEG_ENABLE_CLANG_TIDY)
    return()
  endif()

  find_program(CLANG_TIDY_EXECUTABLE NAMES clang-tidy clang-tidy-14)
  if(NOT CLANG_TIDY_EXECUTABLE)
    message(WARNING "PUSHSEG_ENABLE_CLANG_TIDY is ON but clang-tidy was not found; skipping ${target}.")
    return()
  endif()

  set_target_properties(${target} PROPERTIES
    CXX_CLANG_TIDY "${CLANG_TIDY_EXECUTABLE}")
endfunction()
