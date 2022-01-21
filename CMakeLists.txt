cmake_minimum_required(VERSION 3.16)
project(sqlite_performance)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

FetchContent_Declare(
        txbench
        GIT_REPOSITORY https://github.com/UWHustle/txbench.git
        GIT_TAG master
)
FetchContent_MakeAvailable(txbench)

add_subdirectory(systems)
add_subdirectory(benchmarks)
