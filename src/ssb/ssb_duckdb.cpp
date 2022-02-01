#include "cxxopts.hpp"
#include "duckdb.hpp"
#include "readstr.hpp"

#include <array>
#include <chrono>
#include <iostream>
#include <stdexcept>

void assert_success(const std::unique_ptr<duckdb::QueryResult> &result) {
  if (!result->success) {
    throw std::runtime_error(result->error);
  }
}

void assert_success(
    const std::unique_ptr<duckdb::MaterializedQueryResult> &result) {
  if (!result->success) {
    throw std::runtime_error(result->error);
  }
}

void load(duckdb::Connection &conn, size_t sf) {
  std::string sql = readstr("sql/init/duckdb.sql");
  assert_success(conn.Query(sql));

  assert_success(conn.Query("COPY part FROM 'part.tbl' (AUTO_DETECT TRUE)"));
  assert_success(
      conn.Query("COPY supplier FROM 'supplier.tbl' (AUTO_DETECT TRUE)"));
  assert_success(
      conn.Query("COPY customer FROM 'customer.tbl' (AUTO_DETECT TRUE)"));
  assert_success(conn.Query("COPY date FROM 'date.tbl' (AUTO_DETECT TRUE)"));
  assert_success(
      conn.Query("COPY lineorder FROM 'lineorder.tbl' (AUTO_DETECT TRUE)"));
}

double run(duckdb::Connection &conn, const std::string &q_id) {
  std::string path = "sql/" + q_id + ".sql";
  std::string sql = readstr(path);

  auto t0 = std::chrono::high_resolution_clock::now();

  std::unique_ptr<duckdb::MaterializedQueryResult> result = conn.Query(sql);
  if (!result->success) {
    throw std::runtime_error(result->error);
  }

  auto t1 = std::chrono::high_resolution_clock::now();

  return std::chrono::duration<double>(t1 - t0).count();
}

int main(int argc, char **argv) {
  cxxopts::Options options("ssb_duckdb", "Star schema benchmark on DuckDB");

  cxxopts::OptionAdder option_adder = options.add_options();
  option_adder("load", "(optional) Load scale factor",
               cxxopts::value<size_t>());
  option_adder("help", "(optional) Print help");

  auto result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  duckdb::DuckDB db("ssb.duckdb");
  duckdb::Connection conn(db);

  if (result.count("load")) {
    load(conn, result["load"].as<size_t>());
  }

  std::cerr << "Running SSB on DuckDB." << std::endl;

  const char *q_ids[] = {"q1.1", "q1.2", "q1.3", "q2.1", "q2.2", "q2.3", "q3.1",
                         "q3.2", "q3.3", "q3.4", "q4.1", "q4.2", "q4.3"};

  for (const char *q_id : q_ids) {
    std::cout << q_id << ": " << run(conn, q_id) << std::endl;
  }

  return 0;
}
