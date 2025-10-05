cmake_minimum_required(VERSION 3.20)
project(ofi_microprice_queue_hft LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Optional but helpful warnings/opts
add_compile_options(-O3 -Wall -Wextra -Wpedantic)

# ---- Databento C++ SDK via FetchContent ----
include(FetchContent)
FetchContent_Declare(
  databento_cpp
  GIT_REPOSITORY https://github.com/databento/databento-cpp.git
  GIT_TAG        main
)
FetchContent_MakeAvailable(databento_cpp)
# This provides a target named: databento

# ---- Your sources ----
file(GLOB_RECURSE SRC "${CMAKE_SOURCE_DIR}/src/*.cpp")

add_executable(ofi_mp_queue ${SRC})
target_include_directories(ofi_mp_queue PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(ofi_mp_queue PRIVATE databento)   # <-- important
target_compile_features(ofi_mp_queue PRIVATE cxx_std_20)
