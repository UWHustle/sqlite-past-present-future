cmake_minimum_required(VERSION 3.16)
project(sqlite_performance)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

FetchContent_Declare(
        txbench
        GIT_REPOSITORY https://github.com/UWHustle/txbench.git
        GIT_TAG tatp-refinements
)
FetchContent_MakeAvailable(txbench)

FetchContent_Declare(
        sqlite3
        URL https://www.sqlite.org/2022/sqlite-amalgamation-3370200.zip
        URL_HASH SHA3_256=7f535314ac30f1c7847df2a66a9e16a322f55dae6e83b264178cf02114cd0617
)
FetchContent_MakeAvailable(sqlite3)

FetchContent_Declare(
        duckdb
        URL https://github.com/duckdb/duckdb/releases/download/v0.3.1/libduckdb-src.zip
)
FetchContent_MakeAvailable(duckdb)

add_library(sqlite3 ${sqlite3_SOURCE_DIR}/sqlite3.c)
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
        -DSQLITE_DEFAULT_CACHE_SIZE=-8388608
)

add_library(duckdb ${duckdb_SOURCE_DIR}/duckdb.cpp)

file(COPY src/tatp/sql DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/tatp)

add_executable(tatp_sqlite3 src/tatp/tatp_sqlite3.cpp)
target_include_directories(
        tatp_sqlite3
        BEFORE PRIVATE
        ${sqlite3_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(tatp_sqlite3 txbench::tatp sqlite3)
set_target_properties(
        tatp_sqlite3
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tatp
)

add_executable(tatp_duckdb src/tatp/tatp_duckdb.cpp)
target_include_directories(
        tatp_duckdb
        BEFORE PRIVATE
        ${duckdb_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(tatp_duckdb txbench::tatp duckdb)
set_target_properties(
        tatp_duckdb
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tatp
)
