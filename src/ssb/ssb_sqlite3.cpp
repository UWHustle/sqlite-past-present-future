#include "cxxopts.hpp"
#include "readstr.hpp"
#include "sqlite3.hpp"

#include <iostream>

double run(sqlite::Connection &conn, const std::string &q_id) {
  std::string path = "sql/" + q_id + ".sql";
  std::string sql = readstr(path);

  auto t0 = std::chrono::high_resolution_clock::now();

  conn.execute(sql);

  auto t1 = std::chrono::high_resolution_clock::now();

  return std::chrono::duration<double>(t1 - t0).count();
}

int main(int argc, char **argv) {
  cxxopts::Options options("ssb_sqlite3", "Star schema benchmark on SQLite3");

  cxxopts::OptionAdder option_adder = options.add_options();
  option_adder("help", "(optional) Print help");

  auto result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  auto conn = std::make_shared<sqlite::Connection>("ssb.sqlite");

  const char *q_ids[] = {"q1.1", "q1.2", "q1.3", "q2.1", "q2.2", "q2.3", "q3.1",
                         "q3.2", "q3.3", "q3.4", "q4.1", "q4.2", "q4.3"};

  for (const char *q_id : q_ids) {
    std::cout << q_id << ": " << run(*conn, q_id) << std::endl;
  }

  return 0;
}
