cmake_minimum_required(VERSION 3.20)
project(ofi_microprice_queue_hft LANGUAGES CXX)

# -------- Defaults / flags --------
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(-O3 -Wall -Wextra -Wpedantic)

# -------- Databento C++ SDK via FetchContent --------
include(FetchContent)
FetchContent_Declare(
  databento_cpp
  GIT_REPOSITORY https://github.com/databento/databento-cpp.git
  GIT_TAG        main
)
FetchContent_MakeAvailable(databento_cpp)

# Some versions export 'databento::databento', others 'databento'
if(TARGET databento::databento)
  set(DBN_TARGET databento::databento)
else()
  set(DBN_TARGET databento)
endif()

# -------- Include path for our headers --------
set(PROJ_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/include)

# ===== Executable 1: smoke / reader sanity =====
add_executable(ofi_mp_queue
  src/smoke.cpp
  src/dbn_reader.cpp
)
target_include_directories(ofi_mp_queue PRIVATE ${PROJ_INCLUDE_DIR})
target_link_libraries(ofi_mp_queue PRIVATE ${DBN_TARGET})
target_compile_features(ofi_mp_queue PRIVATE cxx_std_20)

# ===== Executable 2: single-day backtest =====
add_executable(backtest_ofi
  src/backtest_ofi.cpp
  src/strategy/QueueOfi.cpp
  src/dbn_reader.cpp
)
target_include_directories(backtest_ofi PRIVATE ${PROJ_INCLUDE_DIR})
target_link_libraries(backtest_ofi PRIVATE ${DBN_TARGET})
target_compile_features(backtest_ofi PRIVATE cxx_std_20)

# ===== Executable 3: optimizer (Oct 1–15 train, 16–30 validate) =====
add_executable(optimize_ofi
  src/optimize_ofi.cpp
  src/strategy/QueueOfi.cpp
  src/dbn_reader.cpp
)
target_include_directories(optimize_ofi PRIVATE ${PROJ_INCLUDE_DIR})
target_link_libraries(optimize_ofi PRIVATE ${DBN_TARGET})
target_compile_features(optimize_ofi PRIVATE cxx_std_20)

# --- Tool: id_counts (inspect instrument_id distribution per day) ---
add_executable(id_counts
  src/tools/id_counts.cpp
)
target_include_directories(id_counts PRIVATE ${PROJ_INCLUDE_DIR})
target_link_libraries(id_counts PRIVATE ${DBN_TARGET})
target_compile_features(id_counts PRIVATE cxx_std_20)
