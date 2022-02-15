#include "cxxopts.hpp"
#include "dbbench/runner.hpp"
#include "helpers.hpp"
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

class Worker {
public:
  Worker(duckdb::Connection conn, size_t size, float mix)
      : conn_(std::move(conn)), size_(size),
        select_stmt_(conn_.Prepare("SELECT a FROM t")),
        update_stmt_(conn_.Prepare("UPDATE t SET a = ?")), blob_(malloc(size)),
        dis_({mix, 1.0 - mix}), gen_(std::random_device()()) {}

  bool operator()() {
    int type = dis_(gen_);

    if (type == 0) {
      assert_success(select_stmt_->Execute());
    } else {
      assert_success(update_stmt_->Execute(
          duckdb::Value::BLOB((const unsigned char *)blob_, size_)));
    }

    return true;
  }

private:
  duckdb::Connection conn_;
  std::unique_ptr<duckdb::PreparedStatement> select_stmt_;
  std::unique_ptr<duckdb::PreparedStatement> update_stmt_;
  size_t size_;
  void *blob_;
  std::discrete_distribution<int> dis_;
  std::minstd_rand gen_;
};

int main(int argc, char **argv) {
  cxxopts::Options options =
      blob_options("blob_duckdb", "Blob benchmark on DuckDB");

  auto result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  auto size = result["size"].as<size_t>();
  auto mix = result["mix"].as<float>();

  duckdb::DuckDB db("blob.duckdb");

  if (result.count("load")) {
    duckdb::Connection conn(db);

    assert_success(conn.Query("DROP TABLE IF EXISTS t"));
    assert_success(conn.Query("CREATE TABLE t (a BLOB)"));

    void *blob = malloc(size);

    assert_success(
        conn.Query("INSERT INTO t VALUES (?)",
                   duckdb::Value::BLOB((const unsigned char *)blob, size)));

    free(blob);
  }

  if (result.count("run")) {
    duckdb::Connection conn(db);
    assert_success(conn.Query("PRAGMA memory_limit='1GB'"));
    std::vector<Worker> workers;
    workers.emplace_back(conn, size, mix);

    double throughput = dbbench::run(workers, result["warmup"].as<size_t>(),
                                     result["measure"].as<size_t>());

    std::cout << throughput << std::endl;
  }

  return 0;
}
