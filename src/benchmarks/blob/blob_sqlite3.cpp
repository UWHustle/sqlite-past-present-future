#include "cxxopts.hpp"
#include "dbbench/runner.hpp"
#include "helpers.hpp"
#include "sqlite3.hpp"

#include <atomic>
#include <iostream>
#include <random>
#include <thread>

class Worker {
public:
  Worker(sqlite::Connection conn, size_t size, float mix)
      : conn_(std::move(conn)), size_(size), blob_(malloc(size)),
        dis_({mix, 1.0 - mix}), gen_(std::random_device()()) {
    conn_.prepare(select_stmt_, "SELECT a FROM t").expect(SQLITE_OK);
    conn_.prepare(update_stmt_, "UPDATE t SET a = ?").expect(SQLITE_OK);
  }

  ~Worker() { free(blob_); }

  bool operator()() {
    int type = dis_(gen_);

    if (type == 0) {
      select_stmt_.execute().expect(SQLITE_OK);
    } else {
      update_stmt_.bind_blob(1, blob_, (int)size_).expect(SQLITE_OK);
      update_stmt_.execute().expect(SQLITE_OK);
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
    sqlite::Connection conn;
    db.connect(conn).expect(SQLITE_OK);

    conn.execute("DROP TABLE IF EXISTS t").expect(SQLITE_OK);
    conn.execute("CREATE TABLE t (a BLOB)").expect(SQLITE_OK);

    void *blob = malloc(size);

    sqlite::Statement insert_stmt;
    conn.prepare(insert_stmt, "INSERT INTO t VALUES (?)").expect(SQLITE_OK);
    insert_stmt.bind_blob(1, blob, (int)size).expect(SQLITE_OK);
    insert_stmt.execute().expect(SQLITE_OK);

    free(blob);
  }

  if (result.count("run")) {
    sqlite::Connection conn;
    db.connect(conn).expect(SQLITE_OK);
    conn.execute("PRAGMA cache_size=-1000000").expect(SQLITE_OK);
    std::vector<Worker> workers;
    workers.emplace_back(conn, size, mix);

    double throughput = dbbench::run(workers, result["warmup"].as<size_t>(),
                                     result["measure"].as<size_t>());

    std::cout << throughput << std::endl;
  }

  return 0;
}
