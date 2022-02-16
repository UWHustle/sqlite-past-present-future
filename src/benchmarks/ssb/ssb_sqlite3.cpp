#include "cxxopts.hpp"
#include "helpers.hpp"
#include "readfile.hpp"
#include "sqlite3.hpp"

int main(int argc, char **argv) {
  cxxopts::Options options = ssb_options("ssb_sqlite3", "SSB on SQLite3");

  cxxopts::OptionAdder adder = options.add_options("SQLite3");
  adder("bloom_filter", "Use Bloom filters",
        cxxopts::value<bool>()->default_value("false"));
  adder("cache_size", "Cache size",
        cxxopts::value<std::string>()->default_value("-1000000"));

  cxxopts::ParseResult result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  sqlite::Database db("ssb.sqlite");

  sqlite::Connection conn;
  db.connect(conn).expect(SQLITE_OK);

  uint64_t mask = result["bloom_filter"].as<bool>() ? 0 : 0x00080000;
  int rc = sqlite3_test_control(SQLITE_TESTCTRL_OPTIMIZATIONS, conn.ptr().get(),
                                mask);
  if (rc != SQLITE_OK) {
    throw std::runtime_error(sqlite3_errmsg(conn.ptr().get()));
  }

  conn.execute("PRAGMA cache_size=" + result["cache_size"].as<std::string>())
      .expect(SQLITE_OK);

  conn.execute("ANALYZE").expect(SQLITE_OK);

  conn.execute("SELECT * FROM lineorder").expect(SQLITE_OK);
  conn.execute("SELECT * FROM part").expect(SQLITE_OK);
  conn.execute("SELECT * FROM supplier").expect(SQLITE_OK);
  conn.execute("SELECT * FROM customer").expect(SQLITE_OK);
  conn.execute("SELECT * FROM date").expect(SQLITE_OK);

  for (const std::string &query :
       {"q1.1", "q1.2", "q1.3", "q2.1", "q2.2", "q2.3", "q3.1", "q3.2", "q3.3",
        "q3.4", "q4.1", "q4.2", "q4.3"}) {
    std::string sql = readfile("sql/" + query + ".sql");
    std::cout << time([&] { conn.execute(sql).expect(SQLITE_OK); });
    if (query != "q4.3") {
      std::cout << "," << std::flush;
    }
  }
  std::cout << std::endl;

  return 0;
}
