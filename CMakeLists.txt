cmake_minimum_required(VERSION 3.16)
project(sqlite_performance)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

include(FetchContent)

FetchContent_Declare(
        cxxopts
        GIT_REPOSITORY https://github.com/jarro2783/cxxopts.git
        GIT_TAG v2.2.1
)
set(CXXOPTS_BUILD_EXAMPLES OFF)
set(CXXOPTS_BUILD_TESTS OFF)
set(CXXOPTS_ENABLE_INSTALL OFF)
set(CXXOPTS_ENABLE_WARNINGS OFF)
FetchContent_MakeAvailable(cxxopts)

FetchContent_Declare(
        dbbench
        GIT_REPOSITORY https://github.com/kpgaffney/dbbench.git
        GIT_TAG main
)
set(DBBENCH_BUILD_EXAMPLES OFF)
FetchContent_MakeAvailable(dbbench)

add_library(
        sqlite3
        src/systems/sqlite/sqlite3.c
        src/systems/sqlite/sqlite3.h
        src/systems/sqlite/sqlite3.hpp
)
target_compile_options(
        sqlite3
        PRIVATE
        -DSQLITE_DQS=0
        -DSQLITE_THREADSAFE=0
        -DSQLITE_DEFAULT_MEMSTATUS=0
        -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1
        -DSQLITE_LIKE_DOESNT_MATCH_BLOBS
        -DSQLITE_MAX_EXPR_DEPTH=0
        -DSQLITE_OMIT_DECLTYPE
        -DSQLITE_OMIT_DEPRECATED
        -DSQLITE_OMIT_PROGRESS_CALLBACK
        -DSQLITE_OMIT_SHARED_CACHE
        -DSQLITE_USE_ALLOCA
        -DSQLITE_OMIT_AUTOINIT
)

add_executable(
        sqlite3_shell
        src/systems/sqlite/shell.c
        src/systems/sqlite/sqlite3.c
        src/systems/sqlite/sqlite3.h
)

add_library(
        duckdb
        src/systems/duckdb/duckdb.cpp
        src/systems/duckdb/duckdb.hpp
)

# SSB executables.

add_subdirectory(src/benchmarks/ssb/dbgen)

add_executable(ssb_sqlite3 src/benchmarks/ssb/ssb_sqlite3.cpp)
target_include_directories(ssb_sqlite3 PRIVATE src)
target_link_libraries(ssb_sqlite3 cxxopts sqlite3 ${CMAKE_DL_LIBS})
set_target_properties(
        ssb_sqlite3
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/ssb
)

add_executable(ssb_duckdb src/benchmarks/ssb/ssb_duckdb.cpp)
target_include_directories(ssb_duckdb PRIVATE src)
target_link_libraries(ssb_duckdb cxxopts duckdb ${CMAKE_DL_LIBS})
set_target_properties(
        ssb_duckdb
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/ssb
)

file(COPY src/benchmarks/ssb/sql DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/ssb)

# TATP executables.

add_executable(tatp_sqlite3 src/benchmarks/tatp/tatp_sqlite3.cpp)
target_include_directories(tatp_sqlite3 PRIVATE src)
target_link_libraries(tatp_sqlite3 cxxopts dbbench_tatp sqlite3 ${CMAKE_DL_LIBS})
set_target_properties(
        tatp_sqlite3
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tatp
)

add_executable(tatp_duckdb src/benchmarks/tatp/tatp_duckdb.cpp)
target_include_directories(tatp_duckdb PRIVATE src)
target_link_libraries(tatp_duckdb cxxopts dbbench_tatp duckdb ${CMAKE_DL_LIBS})
set_target_properties(
        tatp_duckdb
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tatp
)

# Blob executables.

add_executable(blob_sqlite3 src/benchmarks/blob/blob_sqlite3.cpp)
target_include_directories(blob_sqlite3 PRIVATE src)
target_link_libraries(blob_sqlite3 cxxopts dbbench_core sqlite3 ${CMAKE_DL_LIBS})
set_target_properties(
        blob_sqlite3
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/blob
)

add_executable(blob_duckdb src/benchmarks/blob/blob_duckdb.cpp)
target_include_directories(blob_duckdb PRIVATE src)
target_link_libraries(blob_duckdb cxxopts dbbench_core duckdb ${CMAKE_DL_LIBS})
set_target_properties(
        blob_duckdb
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/blob
)

# Scripts.
configure_file(scripts/benchmarks/ssb.sh ${CMAKE_CURRENT_BINARY_DIR}/ssb/ssb.sh COPYONLY)
configure_file(scripts/benchmarks/tatp.sh ${CMAKE_CURRENT_BINARY_DIR}/tatp/tatp.sh COPYONLY)
configure_file(scripts/benchmarks/blob.sh ${CMAKE_CURRENT_BINARY_DIR}/blob/blob.sh COPYONLY)
configure_file(scripts/all.sh ${CMAKE_CURRENT_BINARY_DIR}/all.sh COPYONLY)
