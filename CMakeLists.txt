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

add_library(sqlite3 systems/sqlite-3370200/sqlite3.c)
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
        -DSQLITE_DEFAULT_CACHE_SIZE=-1048576
)

add_executable(tatp_sqlite3 benchmarks/tatp_sqlite3.cpp)
target_include_directories(tatp_sqlite3 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(tatp_sqlite3 txbench::tatp sqlite3)
