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
      const std::vector<TATPSubscriberRecord> &batch) override {
    duckdb::Appender appender(conn_, "subscriber");
    for (const TATPSubscriberRecord &record : batch) {
      appender.BeginRow();
      appender.Append(record.s_id);
      appender.Append(duckdb::string_t(record.sub_nbr));
      for (bool bit : record.bit) {
        appender.Append(bit);
      }
      for (unsigned short hex : record.hex) {
        appender.Append(hex);
      }
      for (unsigned short byte2 : record.byte2) {
        appender.Append(byte2);
      }
      appender.Append<unsigned long long>(record.msc_location);
      appender.Append<unsigned long long>(record.vlr_location);
      appender.EndRow();
    }
  }

  void load_access_info_batch(
      const std::vector<TATPAccessInfoRecord> &batch) override {
    duckdb::Appender appender(conn_, "access_info");
    for (const TATPAccessInfoRecord &record : batch) {
      appender.AppendRow(record.s_id, record.ai_type, record.data1,
                         record.data2, duckdb::string_t(record.data3),
                         duckdb::string_t(record.data4));
    }
  }

  void load_special_facility_batch(
      const std::vector<TATPSpecialFacilityRecord> &batch) override {
    duckdb::Appender appender(conn_, "special_facility");
    for (const TATPSpecialFacilityRecord &record : batch) {
      appender.AppendRow(record.s_id, record.sf_type, record.is_active,
                         record.error_cntrl, record.data_a,
                         duckdb::string_t(record.data_b));
    }
  }

  void load_call_forwarding_batch(
      const std::vector<TATPCallForwardingRecord> &batch) override {
    duckdb::Appender appender(conn_, "call_forwarding");
    for (const TATPCallForwardingRecord &record : batch) {
      appender.AppendRow(record.s_id, record.sf_type, record.start_time,
                         record.end_time, duckdb::string_t(record.numberx));
    }
  }

private:
  duckdb::Connection conn_;
};

class DuckDBTATPClientConnection : public TATPClientConnection {
public:
  explicit DuckDBTATPClientConnection(duckdb::DuckDB &db) : conn_(db) {
    stmts_.reserve(10);

    for (int i = 0; i <= 9; ++i) {
      std::string sql = readstr("sql/stmt_" + std::to_string(i) + ".sql");
      stmts_.push_back(conn_.Prepare(sql));
    }
  }

  ReturnCode get_subscriber_data(unsigned long long s_id, std::string *sub_nbr,
                                 std::array<bool, 10> &bit,
                                 std::array<unsigned short, 10> &hex,
                                 std::array<unsigned short, 10> &byte2,
                                 unsigned long *msc_location,
                                 unsigned long *vlr_location) override {
    std::unique_ptr<duckdb::QueryResult> result = stmts_[0]->Execute(s_id);
    std::unique_ptr<duckdb::DataChunk> chunk = result->Fetch();
    assert(chunk->size() == 1);
    *sub_nbr = chunk->GetValue(1, 0).GetValue<std::string>();
    for (int i = 0; i < 10; ++i) {
      bit[i] = chunk->GetValue(i + 2, 0).GetValue<bool>();
    }
    for (int i = 0; i < 10; ++i) {
      hex[i] = chunk->GetValue(i + 12, 0).GetValue<unsigned short>();
    }
    for (int i = 0; i < 10; ++i) {
      byte2[i] = chunk->GetValue(i + 22, 0).GetValue<unsigned short>();
    }
    *msc_location = chunk->GetValue(32, 0).GetValue<long long>();
    *vlr_location = chunk->GetValue(32, 0).GetValue<long long>();
    assert(!result->Fetch());
    return TXBENCH_SUCCESS;
  }

  ReturnCode get_new_destination(unsigned long long s_id,
                                 unsigned short sf_type,
                                 unsigned short start_time,
                                 unsigned short end_time,
                                 std::vector<std::string> *numberx) override {
    ReturnCode rc = TXBENCH_SUCCESS;

    std::unique_ptr<duckdb::QueryResult> result =
        stmts_[1]->Execute(s_id, sf_type, start_time, end_time);
    if (std::unique_ptr<duckdb::DataChunk> chunk = result->Fetch()) {
      std::vector<std::string> out;
      while (chunk) {
        for (size_t i = 0; i < chunk->size(); ++i) {
          out.push_back(chunk->GetValue(0, i).GetValue<std::string>());
        }
        chunk = result->Fetch();
      }
    } else {
      rc = TXBENCH_MISSING;
    }

    return rc;
  }

  ReturnCode get_access_data(unsigned long long s_id, unsigned short ai_type,
                             unsigned short *data1, unsigned short *data2,
                             std::string *data3, std::string *data4) override {
    ReturnCode rc = TXBENCH_SUCCESS;

    std::unique_ptr<duckdb::QueryResult> result =
        stmts_[2]->Execute(s_id, ai_type);
    if (std::unique_ptr<duckdb::DataChunk> chunk = result->Fetch()) {
      assert(chunk->size() == 1);
      *data1 = chunk->GetValue(0, 0).GetValue<unsigned short>();
      *data2 = chunk->GetValue(1, 0).GetValue<unsigned short>();
      *data3 = chunk->GetValue(2, 0).GetValue<std::string>();
      *data4 = chunk->GetValue(3, 0).GetValue<std::string>();
      assert(!result->Fetch());
    } else {
      rc = TXBENCH_MISSING;
    }

    return rc;
  }

  ReturnCode update_subscriber_data(unsigned long long s_id, bool bit_1,
                                    unsigned short sf_type,
                                    unsigned short data_a) override {
    ReturnCode rc = TXBENCH_SUCCESS;
    std::unique_ptr<duckdb::QueryResult> result;
    int changes;

    conn_.BeginTransaction();

    // Update subscriber.
    result = stmts_[3]->Execute(bit_1, s_id);
    changes = result->Fetch()->GetValue(0, 0).GetValue<int>();
    assert(changes == 1);

    // Update special facility.
    result = stmts_[4]->Execute(data_a, s_id, sf_type);
    changes = result->Fetch()->GetValue(0, 0).GetValue<int>();
    assert(changes == 0 || changes == 1);
    if (changes == 0) {
      rc = TXBENCH_MISSING;
    }

    conn_.Commit();

    return rc;
  }

  ReturnCode update_location(const std::string &sub_nbr,
                             unsigned long vlr_location) override {
    std::unique_ptr<duckdb::QueryResult> result =
        stmts_[5]->Execute((long long)vlr_location, std::string(sub_nbr));
    assert(result->Fetch()->GetValue(0, 0).GetValue<int>() == 1);

    return TXBENCH_SUCCESS;
  }

  ReturnCode insert_call_forwarding(std::string sub_nbr, unsigned short sf_type,
                                    unsigned short start_time,
                                    unsigned short end_time,
                                    std::string numberx) override {
    ReturnCode rc = TXBENCH_SUCCESS;
    std::unique_ptr<duckdb::QueryResult> result;

    conn_.BeginTransaction();

    // Select subscriber.
    result = stmts_[6]->Execute(sub_nbr);
    long long s_id = result->Fetch()->GetValue(0, 0).GetValue<long long>();
    assert(!result->Fetch());

    // Select special_facility.
    result = stmts_[7]->Execute(s_id);
    assert(result->Fetch()->size() > 0);
    assert(!result->Fetch());

    // Insert call_forwarding.
    result = stmts_[8]->Execute(s_id, sf_type, start_time, end_time, numberx);
    if (!result->success) {
      rc = TXBENCH_CONSTRAINT;
    }

    conn_.Commit();

    return rc;
  }

  ReturnCode delete_call_forwarding(const std::string &sub_nbr,
                                    unsigned short sf_type,
                                    unsigned short start_time) override {
    ReturnCode rc = TXBENCH_SUCCESS;
    std::unique_ptr<duckdb::QueryResult> result;

    conn_.BeginTransaction();

    // Select subscriber.
    result = stmts_[6]->Execute(sub_nbr);
    long long s_id = result->Fetch()->GetValue(0, 0).GetValue<long long>();
    assert(!result->Fetch());

    // Delete call_forwarding.
    result = stmts_[9]->Execute(s_id, sf_type, start_time);
    int changes = result->Fetch()->GetValue(0, 0).GetValue<int>();
    assert(changes == 0 || changes == 1);
    if (changes == 0) {
      rc = TXBENCH_MISSING;
    }

    conn_.Commit();

    return rc;
  }

private:
  duckdb::Connection conn_;
  std::vector<std::unique_ptr<duckdb::PreparedStatement>> stmts_;
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
