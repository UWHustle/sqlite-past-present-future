#include "cxxopts.hpp"
#include "duckdb.hpp"

#include <atomic>
#include <iostream>
#include <random>
#include <thread>

void assert_success(const std::unique_ptr<duckdb::QueryResult> &result) {
  if (!result->success) {
    throw std::runtime_error(result->error);
  }
}

int main(int argc, char **argv) {
  cxxopts::Options options("blob_duckdb", "Blob benchmark on DuckDB");

  cxxopts::OptionAdder option_adder = options.add_options();
  option_adder("size", "Size of the blob in bytes", cxxopts::value<size_t>());
  option_adder("mix", "Read transaction fraction", cxxopts::value<float>());
  option_adder("help", "(optional) Print help");

  auto parse_result = options.parse(argc, argv);

  if (parse_result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  auto size = parse_result["size"].as<size_t>();
  auto mix = parse_result["mix"].as<float>();

  void *blob = malloc(size);

  duckdb_database db;
  duckdb_connection conn;

  if (duckdb_open("blob.duckdb", &db) == DuckDBError) {
    return 1;
  }

  if (duckdb_connect(db, &conn) == DuckDBError) {
    return 1;
  }

  duckdb_state state;
  duckdb_prepared_statement insert_stmt;
  duckdb_prepared_statement select_stmt;
  duckdb_prepared_statement update_stmt;

  state = duckdb_query(conn, "DROP TABLE IF EXISTS t", nullptr);
  if (state == DuckDBError) {
    return 1;
  }

  state = duckdb_query(conn, "CREATE TABLE t (a BLOB)", nullptr);
  if (state == DuckDBError) {
    return 1;
  }

  state = duckdb_prepare(conn, "INSERT INTO t VALUES (?)", &insert_stmt);
  if (state == DuckDBError) {
    return 1;
  }

  state = duckdb_prepare(conn, "SELECT a FROM t", &select_stmt);
  if (state == DuckDBError) {
    return 1;
  }

  state = duckdb_prepare(conn, "UPDATE t SET a = ?", &update_stmt);
  if (state == DuckDBError) {
    return 1;
  }

  state = duckdb_bind_blob(insert_stmt, 1, blob, size);
  if (state == DuckDBError) {
    return 1;
  }

  state = duckdb_execute_prepared(insert_stmt, nullptr);
  if (state == DuckDBError) {
    return 1;
  }

  size_t count = 0;

  std::atomic_bool terminate = false;

  std::thread t([&] {
    std::random_device rd;
    std::minstd_rand gen(rd());
    std::discrete_distribution<int> dis({mix, 1.0 - mix});

    while (!terminate) {
      int type = dis(gen);

      if (type == 0) {
        state = duckdb_execute_prepared(select_stmt, nullptr);
        if (state == DuckDBError) {
          throw;
        }
      } else {
        state = duckdb_bind_blob(update_stmt, 1, blob, size);
        if (state == DuckDBError) {
          throw;
        }
        state = duckdb_execute_prepared(update_stmt, nullptr);
        if (state == DuckDBError) {
          throw;
        }
      }

      ++count;
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(5));

  terminate = true;
  t.join();

  duckdb_destroy_prepare(&insert_stmt);
  duckdb_destroy_prepare(&select_stmt);
  duckdb_destroy_prepare(&update_stmt);
  duckdb_disconnect(&conn);
  duckdb_close(&db);

  free(blob);

  std::cout << (double)count / 5.0 << std::endl;

  return 0;
}
