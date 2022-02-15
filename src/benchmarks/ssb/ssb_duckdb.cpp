#include "cxxopts.hpp"
#include "helpers.hpp"
#include "readfile.hpp"
#include "duckdb.hpp"

void assert_success(const std::unique_ptr<duckdb::QueryResult> &result) {
  if (!result->success) {
    throw std::runtime_error(result->error);
  }
}

int main(int argc, char **argv) {
  cxxopts::Options options = ssb_options("ssb_duckdb", "SSB on DuckDB");

  cxxopts::OptionAdder adder = options.add_options("DuckDB");
  adder("load", "Load the database");
  adder("run", "Run the benchmark");
  adder("memory_limit", "Memory limit",
        cxxopts::value<std::string>()->default_value("1GB"));
  adder("threads", "Number of threads",
        cxxopts::value<std::string>()->default_value("1"));

  cxxopts::ParseResult result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  auto memory_limit = result["memory_limit"].as<std::string>();
  auto threads = result["threads"].as<std::string>();

  duckdb::DuckDB db("ssb.duckdb");

  if (result.count("load")) {
    duckdb::Connection conn(db);

    std::string sql = readfile("sql/init/duckdb.sql");
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

  if (result.count("run")) {
    duckdb::Connection conn(db);

    assert_success(conn.Query("PRAGMA memory_limit='" + memory_limit + "'"));
    assert_success(conn.Query("PRAGMA threads=" + threads));

    assert_success(conn.Query("SELECT * FROM lineorder"));
    assert_success(conn.Query("SELECT * FROM part"));
    assert_success(conn.Query("SELECT * FROM supplier"));
    assert_success(conn.Query("SELECT * FROM customer"));
    assert_success(conn.Query("SELECT * FROM date"));

    for (const std::string &query :
         {"q1.1", "q1.2", "q1.3", "q2.1", "q2.2", "q2.3", "q3.1", "q3.2",
          "q3.3", "q3.4", "q4.1", "q4.2", "q4.3"}) {
      std::string sql = readfile("sql/" + query + ".sql");
      std::cout << time([&] { assert_success(conn.Query(sql)); });
      if (query != "q4.3") {
        std::cout << "," << std::flush;
      }
    }
    std::cout << std::endl;
  }

  return 0;
}
