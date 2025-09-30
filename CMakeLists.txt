cmake_minimum_required(VERSION 3.20)
project(ofi_microprice_queue_hft CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

file(GLOB_RECURSE SRC "src/*.cpp")
add_executable(ofi_mp_queue ${SRC})
target_include_directories(ofi_mp_queue PRIVATE include)
