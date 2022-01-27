#include "readstr.hpp"
#include "sqlite3.h"
#include "sqlite3.hpp"
#include "txbench/benchmarks/tatp.hpp"

#include <array>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

class SQLite3TATPLoaderConnection : public TATPLoaderConnection {
public:
  explicit SQLite3TATPLoaderConnection(const std::string &filename)
      : conn_(std::make_shared<sqlite::Connection>(filename)) {
    std::string sql = readstr("sql/init/sqlite3.sql");
    conn_->execute(sql);
  }

  void load_subscriber_batch(
      const std::vector<TATPSubscriberRecord> &batch) override {
    load_batch("subscriber", batch,
               [](std::ostream &os, const TATPSubscriberRecord &record) {
                 os << record.s_id << ", '" << record.sub_nbr << "', ";
                 std::copy(record.bit.begin(), record.bit.end(),
                           std::ostream_iterator<bool>(os, ", "));
                 std::copy(record.hex.begin(), record.hex.end(),
                           std::ostream_iterator<int>(os, ", "));
                 std::copy(record.byte2.begin(), record.byte2.end(),
                           std::ostream_iterator<int>(os, ", "));
                 os << record.msc_location << ", " << record.vlr_location;
               });
  }

  void load_access_info_batch(
      const std::vector<TATPAccessInfoRecord> &batch) override {
    load_batch("access_info", batch,
               [](std::ostream &os, const TATPAccessInfoRecord &record) {
                 os << record.s_id << ", " << record.ai_type << ", "
                    << record.data1 << ", " << record.data2 << ", '"
                    << record.data3 << "', '" << record.data4 << "'";
               });
  }

  void load_special_facility_batch(
      const std::vector<TATPSpecialFacilityRecord> &batch) override {
    load_batch("special_facility", batch,
               [](std::ostream &os, const TATPSpecialFacilityRecord &record) {
                 os << record.s_id << ", " << record.sf_type << ", "
                    << record.is_active << ", " << record.error_cntrl << ", "
                    << record.data_a << ", '" << record.data_b << "'";
               });
  }

  void load_call_forwarding_batch(
      const std::vector<TATPCallForwardingRecord> &batch) override {
    load_batch("call_forwarding", batch,
               [](std::ostream &os, const TATPCallForwardingRecord &record) {
                 os << record.s_id << ", " << record.sf_type << ", "
                    << record.start_time << ", " << record.end_time << ", '"
                    << record.numberx << "'";
               });
  }

private:
  template <typename R, typename F>
  void load_batch(const std::string &table, const std::vector<R> &batch,
                  F &&values) {
    if (!batch.empty()) {
      std::ostringstream sql;
      sql << "INSERT INTO " << table << " VALUES ";

      for (auto it = batch.begin(); it != batch.end(); ++it) {
        sql << "(";
        values(sql, *it);
        sql << ")";

        if (it + 1 != batch.end()) {
          sql << ", ";
        }
      }

      conn_->execute(sql.str());
    }
  }

  std::shared_ptr<sqlite::Connection> conn_;
};

class SQLite3TATPClientConnection : public TATPClientConnection {
public:
  explicit SQLite3TATPClientConnection(const std::string &filename)
      : conn_(std::make_shared<sqlite::Connection>(filename)) {

    stmts_.reserve(10);

    for (int i = 0; i <= 9; ++i) {
      std::string sql = readstr("sql/stmt_" + std::to_string(i) + ".sql");
      stmts_.emplace_back(conn_, sql);
    }
  }

  ReturnCode get_subscriber_data(int s_id, std::string *sub_nbr,
                                 std::array<bool, 10> &bit,
                                 std::array<int, 10> &hex,
                                 std::array<int, 10> &byte2, int *msc_location,
                                 int *vlr_location) override {
    bool row;

    // Get subscriber data.
    stmts_[0].bind_int(0, s_id);
    row = stmts_[0].step();
    if (!row) {
      return TXBENCH_FAILURE;
    }
    *sub_nbr = stmts_[0].column_string(1);
    for (int i = 0; i < bit.size(); ++i) {
      bit[i] = stmts_[0].column_int(i + 2);
    }
    for (int i = 0; i < hex.size(); ++i) {
      hex[i] = stmts_[0].column_int(i + 12);
    }
    for (int i = 0; i < hex.size(); ++i) {
      byte2[i] = stmts_[0].column_int(i + 22);
    }
    *msc_location = stmts_[0].column_int(32);
    *vlr_location = stmts_[0].column_int(33);
    assert(!stmts_[0].step());
    stmts_[0].reset();

    return TXBENCH_SUCCESS;
  }

  ReturnCode get_new_destination(int s_id, int sf_type, int start_time,
                                 int end_time,
                                 std::vector<std::string> *numberx) override {
    ReturnCode rc = TXBENCH_SUCCESS;

    // Get new destination.
    stmts_[1].bind_int(0, s_id);
    stmts_[1].bind_int(1, sf_type);
    stmts_[1].bind_int(2, start_time);
    stmts_[1].bind_int(3, end_time);
    if (stmts_[1].step()) {
      std::vector<std::string> result;
      do {
        result.emplace_back(stmts_[1].column_string(0));
      } while (stmts_[1].step());
      *numberx = result;
    } else {
      rc = TXBENCH_MISSING;
    }
    stmts_[1].reset();

    return rc;
  }

  ReturnCode get_access_data(int s_id, int ai_type, int *data1, int *data2,
                             std::string *data3, std::string *data4) override {
    ReturnCode rc = TXBENCH_SUCCESS;

    // Get access data.
    stmts_[2].bind_int(0, s_id);
    stmts_[2].bind_int(1, ai_type);
    if (stmts_[2].step()) {
      *data1 = stmts_[2].column_int(0);
      *data2 = stmts_[2].column_int(1);
      *data3 = stmts_[2].column_string(2);
      *data4 = stmts_[2].column_string(3);
      assert(!stmts_[2].step());
    } else {
      rc = TXBENCH_MISSING;
    }
    stmts_[2].reset();

    return rc;
  }

  ReturnCode update_subscriber_data(int s_id, bool bit_1, int sf_type,
                                    int data_a) override {
    ReturnCode rc = TXBENCH_SUCCESS;

    conn_->begin();

    // Update subscriber.
    stmts_[3].bind_int(0, bit_1);
    stmts_[3].bind_int(1, s_id);
    stmts_[3].step();
    stmts_[3].reset();
    assert(conn_->changes() == 1);

    // Update special_facility.
    stmts_[4].bind_int(0, data_a);
    stmts_[4].bind_int(1, s_id);
    stmts_[4].bind_int(2, sf_type);
    stmts_[4].step();
    stmts_[4].reset();
    assert(conn_->changes() == 0 || conn_->changes() == 1);
    if (conn_->changes() == 0) {
      rc = TXBENCH_MISSING;
    }

    conn_->commit();

    return rc;
  }

  ReturnCode update_location(const std::string &sub_nbr,
                             int vlr_location) override {
    // Update location.
    stmts_[5].bind_int(0, vlr_location);
    stmts_[5].bind_string(1, sub_nbr);
    stmts_[5].step();
    assert(!stmts_[5].step());
    stmts_[5].reset();
    assert(conn_->changes() == 1);

    return TXBENCH_SUCCESS;
  }

  ReturnCode insert_call_forwarding(std::string sub_nbr, int sf_type,
                                    int start_time, int end_time,
                                    std::string numberx) override {
    bool row;
    ReturnCode rc = TXBENCH_SUCCESS;

    conn_->begin();

    // Select subscriber.
    stmts_[6].bind_string(0, sub_nbr);
    row = stmts_[6].step();
    if (!row) {
      return TXBENCH_FAILURE;
    }
    int s_id = stmts_[6].column_int(0);
    assert(!stmts_[6].step());
    stmts_[6].reset();

    // Select special_facility.
    stmts_[7].bind_int(0, s_id);
    row = stmts_[7].step();
    if (!row) {
      return TXBENCH_FAILURE;
    }
    do {
      row = stmts_[7].step();
    } while (row);
    stmts_[7].reset();

    // Insert call_forwarding.
    stmts_[8].bind_int(0, s_id);
    stmts_[8].bind_int(1, sf_type);
    stmts_[8].bind_int(2, start_time);
    stmts_[8].bind_int(3, end_time);
    stmts_[8].bind_string(4, numberx);
    try {
      stmts_[8].step();
      assert(conn_->changes() == 1);
    } catch (const sqlite::Exception &e) {
      // Primary key conflict is an acceptable error.
      if (e.code() != SQLITE_CONSTRAINT) {
        throw sqlite::Exception(e);
      }
      assert(conn_->changes() == 0);
      rc = TXBENCH_CONSTRAINT;
    }

    try {
      stmts_[8].reset();
    } catch (const sqlite::Exception &e) {
      if (e.code() != SQLITE_CONSTRAINT) {
        throw sqlite::Exception(e);
      }
    }

    conn_->commit();

    return rc;
  }

  ReturnCode delete_call_forwarding(const std::string &sub_nbr, int sf_type,
                                    int start_time) override {
    bool row;
    ReturnCode rc = TXBENCH_SUCCESS;

    conn_->begin();

    // Select subscriber.
    stmts_[6].bind_string(0, sub_nbr);
    row = stmts_[6].step();
    if (!row) {
      return TXBENCH_FAILURE;
    }
    int s_id = stmts_[6].column_int(0);
    assert(!stmts_[6].step());
    stmts_[6].reset();

    // Delete call_forwarding.
    stmts_[9].bind_int(0, s_id);
    stmts_[9].bind_int(1, sf_type);
    stmts_[9].bind_int(2, start_time);
    stmts_[9].step();
    assert(conn_->changes() == 0 || conn_->changes() == 1);
    if (conn_->changes() == 1) {
      rc = TXBENCH_SUCCESS;
    }
    stmts_[9].reset();

    conn_->commit();

    return rc;
  }

private:
  std::shared_ptr<sqlite::Connection> conn_;
  std::vector<sqlite::Statement> stmts_;
};

class SQLite3TATPServer : public TATPServer {
public:
  explicit SQLite3TATPServer(std::string &&filename) : filename_(filename) {}

  std::unique_ptr<TATPLoaderConnection> connect_loader() override {
    return std::make_unique<SQLite3TATPLoaderConnection>(filename_);
  }

  std::unique_ptr<TATPClientConnection> connect_client() override {
    return std::make_unique<SQLite3TATPClientConnection>(filename_);
  }

private:
  std::string filename_;
};

int main(int argc, char **argv) {
  TATPOptions options = TATPOptions::parse(argc, argv);
  auto server = std::make_unique<SQLite3TATPServer>("tatp.sqlite");

  TATPBenchmark benchmark(std::move(server), options);

  double tps = benchmark.run();

  std::cout << "Throughput (tps): " << tps << std::endl;

  return 0;
}
