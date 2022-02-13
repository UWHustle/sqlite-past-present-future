#include "cxxopts.hpp"
#include "dbbench/runner.hpp"
#include "helpers.hpp"
#include "systems/sqlite/sqlite3.hpp"

#include <atomic>
#include <iostream>
#include <random>
#include <thread>

class Worker {
public:
  Worker(sqlite::Connection conn, size_t size, float mix)
      : conn_(std::move(conn)), size_(size),
        select_stmt_(conn_.prepare("SELECT a FROM t")),
        update_stmt_(conn_.prepare("UPDATE t SET a = ?")), blob_(malloc(size)),
        dis_({mix, 1.0 - mix}), gen_(std::random_device()()) {}

  ~Worker() { free(blob_); }

  bool operator()() {
    int type = dis_(gen_);

    if (type == 0) {
      select_stmt_.step().value();
      select_stmt_.reset();
    } else {
      update_stmt_.bind(1, blob_, (int)size_);
      update_stmt_.step();
      update_stmt_.reset();
    }

    return true;
  }

private:
  sqlite::Connection conn_;
  sqlite::Statement select_stmt_;
  sqlite::Statement update_stmt_;
  size_t size_;
  void *blob_;
  std::discrete_distribution<int> dis_;
  std::minstd_rand gen_;
};

int main(int argc, char **argv) {
  cxxopts::Options options =
      blob_options("blob_sqlite3", "Blob benchmark on SQLite3");

  auto result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  auto size = result["size"].as<size_t>();
  auto mix = result["mix"].as<float>();

  sqlite::Database db("blob.sqlite");

  if (result.count("load")) {
    sqlite::Connection conn = db.connect();

    conn.execute("DROP TABLE IF EXISTS t");
    conn.execute("CREATE TABLE t (a BLOB)");

    void *blob = malloc(size);

    sqlite::Statement insert_stmt = conn.prepare("INSERT INTO t VALUES (?)");
    insert_stmt.bind(1, blob, (int)size);
    insert_stmt.step();

    free(blob);
  }

  if (result.count("run")) {
    sqlite::Connection conn = db.connect();
    conn.execute("PRAGMA cache_size=-1000000");
    std::vector<Worker> workers;
    workers.emplace_back(conn, size, mix);

    double throughput = dbbench::run(workers, result["warmup"].as<size_t>(),
                                     result["measure"].as<size_t>());

    std::cout << throughput << std::endl;
  }

  return 0;
}
