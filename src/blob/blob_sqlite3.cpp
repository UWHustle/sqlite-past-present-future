#include "cxxopts.hpp"
#include "sqlite3.hpp"

#include <atomic>
#include <iostream>
#include <random>
#include <thread>

int main(int argc, char **argv) {
  cxxopts::Options options("blob_sqlite3", "Blob benchmark on SQLite3");

  cxxopts::OptionAdder option_adder = options.add_options();
  option_adder("size", "Size of the blob in bytes", cxxopts::value<size_t>());
  option_adder("mix", "Read transaction fraction", cxxopts::value<float>());
  option_adder("help", "(optional) Print help");

  auto result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  auto size = result["size"].as<size_t>();
  auto mix = result["mix"].as<float>();

  void *blob = malloc(size);

  auto conn = std::make_shared<sqlite::Connection>("blob.sqlite");

  conn->execute("DROP TABLE IF EXISTS t");
  conn->execute("CREATE TABLE t (a BLOB)");

  auto insert_stmt =
      std::make_shared<sqlite::Statement>(conn, "INSERT INTO t VALUES (?)");
  insert_stmt->bind_blob(1, blob, (int)size);
  bool row = insert_stmt->step();
  assert(!row);

  size_t count = 0;

  std::atomic_bool terminate = false;

  std::thread t([&] {
    auto select_stmt =
        std::make_shared<sqlite::Statement>(conn, "SELECT a FROM t");

    auto update_stmt =
        std::make_shared<sqlite::Statement>(conn, "UPDATE t SET a = ?");

    std::random_device rd;
    std::minstd_rand gen(rd());
    std::discrete_distribution<int> dis({mix, 1.0 - mix});

    while (!terminate) {
      int type = dis(gen);

      if (type == 0) {
        bool row = select_stmt->step();
        assert(row);
        assert(!select_stmt->step());
        select_stmt->reset();
      } else {
        update_stmt->bind_blob(1, blob, (int)size);
        bool row = update_stmt->step();
        assert(!row);
        update_stmt->reset();
      }

      ++count;
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(5));

  terminate = true;
  t.join();

  free(blob);

  std::cout << (double)count / 5.0 << std::endl;

  return 0;
}
