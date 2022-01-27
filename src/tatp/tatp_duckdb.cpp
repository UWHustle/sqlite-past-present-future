#include "duckdb.hpp"
#include "readstr.hpp"
#include "txbench/benchmarks/tatp.hpp"

#include <array>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

void assert_success(const std::unique_ptr<duckdb::QueryResult> &result) {
  if (!result->success) {
    throw std::runtime_error(result->error);
  }
}

class DuckDBTATPLoaderConnection : public TATPLoaderConnection {
public:
  explicit DuckDBTATPLoaderConnection(duckdb::DuckDB &db) : conn_(db) {
    std::string sql = readstr("sql/init/duckdb.sql");
    assert_success(conn_.Query(sql));
  }

  void load_subscriber_batch(
      const std::vector<TATPSubscriberRecord> &batch) override {}

  void load_access_info_batch(
      const std::vector<TATPAccessInfoRecord> &batch) override {}

  void load_special_facility_batch(
      const std::vector<TATPSpecialFacilityRecord> &batch) override {}

  void load_call_forwarding_batch(
      const std::vector<TATPCallForwardingRecord> &batch) override {}

private:
  duckdb::Connection conn_;
};

class DuckDBTATPClientConnection : public TATPClientConnection {
public:
  explicit DuckDBTATPClientConnection(duckdb::DuckDB &db) : con_(db) {}

  ReturnCode get_subscriber_data(unsigned long long int s_id,
                                 std::string *sub_nbr,
                                 std::array<bool, 10> &bit,
                                 std::array<unsigned short, 10> &hex,
                                 std::array<unsigned short, 10> &byte2,
                                 unsigned long *msc_location,
                                 unsigned long *vlr_location) override {
    return TXBENCH_MISSING;
  }
  ReturnCode get_new_destination(unsigned long long int s_id,
                                 unsigned short sf_type,
                                 unsigned short start_time,
                                 unsigned short end_time,
                                 std::vector<std::string> *numberx) override {
    return TXBENCH_MISSING;
  }
  ReturnCode get_access_data(unsigned long long int s_id,
                             unsigned short ai_type, unsigned short *data1,
                             unsigned short *data2, std::string *data3,
                             std::string *data4) override {
    return TXBENCH_MISSING;
  }
  ReturnCode update_subscriber_data(unsigned long long int s_id, bool bit_1,
                                    unsigned short sf_type,
                                    unsigned short data_a) override {
    return TXBENCH_MISSING;
  }
  ReturnCode update_location(const std::string &sub_nbr,
                             unsigned long vlr_location) override {
    return TXBENCH_MISSING;
  }
  ReturnCode insert_call_forwarding(std::string sub_nbr, unsigned short sf_type,
                                    unsigned short start_time,
                                    unsigned short end_time,
                                    std::string numberx) override {
    return TXBENCH_MISSING;
  }
  ReturnCode delete_call_forwarding(const std::string &sub_nbr,
                                    unsigned short sf_type,
                                    unsigned short start_time) override {
    return TXBENCH_MISSING;
  }

private:
  duckdb::Connection con_;
};

class DuckDBTATPServer : public TATPServer {
public:
  explicit DuckDBTATPServer(const std::string &path) : db_(path) {}

  std::unique_ptr<TATPLoaderConnection> connect_loader() override {
    return std::make_unique<DuckDBTATPLoaderConnection>(db_);
  }

  std::unique_ptr<TATPClientConnection> connect_client() override {
    return std::make_unique<DuckDBTATPClientConnection>(db_);
  }

private:
  duckdb::DuckDB db_;
};

int main(int argc, char **argv) {
  TATPOptions options = TATPOptions::parse(argc, argv);
  auto server = std::make_unique<DuckDBTATPServer>("tatp.duckdb");

  TATPBenchmark benchmark(std::move(server), options);

  double tps = benchmark.run();

  std::cout << "Throughput (tps): " << tps << std::endl;

  return 0;
}
