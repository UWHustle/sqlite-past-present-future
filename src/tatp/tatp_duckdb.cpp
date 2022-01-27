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

  ReturnCode get_subscriber_data(int s_id, std::string *sub_nbr,
                                 std::array<bool, 10> &bit,
                                 std::array<int, 10> &hex,
                                 std::array<int, 10> &byte2, int *msc_location,
                                 int *vlr_location) override {
    return TXBENCH_FAILURE;
  }
  ReturnCode get_new_destination(int s_id, int sf_type, int start_time,
                                 int end_time,
                                 std::vector<std::string> *numberx) override {
    return TXBENCH_FAILURE;
  }
  ReturnCode get_access_data(int s_id, int ai_type, int *data1, int *data2,
                             std::string *data3, std::string *data4) override {
    return TXBENCH_FAILURE;
  }
  ReturnCode update_subscriber_data(int s_id, bool bit_1, int sf_type,
                                    int data_a) override {
    return TXBENCH_FAILURE;
  }
  ReturnCode update_location(const std::string &sub_nbr,
                             int vlr_location) override {
    return TXBENCH_FAILURE;
  }
  ReturnCode insert_call_forwarding(std::string sub_nbr, int sf_type,
                                    int start_time, int end_time,
                                    std::string numberx) override {
    return TXBENCH_FAILURE;
  }
  ReturnCode delete_call_forwarding(const std::string &sub_nbr, int sf_type,
                                    int start_time) override {
    return TXBENCH_FAILURE;
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
