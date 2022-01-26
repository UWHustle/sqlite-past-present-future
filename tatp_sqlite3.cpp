#include "sqlite3.h"
#include "txbench/benchmarks/tatp.h"

#include <array>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

class SQLite3TATPLoaderConnection : public TATPLoaderConnection {
public:
  explicit SQLite3TATPLoaderConnection(const std::string &filename) {
    int rc = sqlite3_initialize();
    assert(rc == SQLITE_OK);

    rc = sqlite3_open(filename.c_str(), &db_);
    assert(rc == SQLITE_OK);

    rc =
        sqlite3_exec(db_, "PRAGMA journal_mode=WAL", nullptr, nullptr, nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db_, "DROP TABLE IF EXISTS call_forwarding", nullptr,
                      nullptr, nullptr);
    assert(rc == SQLITE_OK);
    rc = sqlite3_exec(db_, "DROP TABLE IF EXISTS special_facility", nullptr,
                      nullptr, nullptr);
    assert(rc == SQLITE_OK);
    rc = sqlite3_exec(db_, "DROP TABLE IF EXISTS access_info", nullptr, nullptr,
                      nullptr);
    assert(rc == SQLITE_OK);
    rc = sqlite3_exec(db_, "DROP TABLE IF EXISTS subscriber", nullptr, nullptr,
                      nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(
        db_,
        "CREATE TABLE subscriber (s_id INTEGER PRIMARY KEY, "
        "  sub_nbr TEXT, "
        "  bit_1 INTEGER, bit_2 INTEGER, bit_3 INTEGER, bit_4 INTEGER, "
        "  bit_5 INTEGER, bit_6 INTEGER, bit_7 INTEGER, bit_8 INTEGER, "
        "  bit_9 INTEGER, bit_10 INTEGER, "
        "  hex_1 INTEGER, hex_2 INTEGER, hex_3 INTEGER, hex_4 INTEGER, "
        "  hex_5 INTEGER, hex_6 INTEGER, hex_7 INTEGER, hex_8 INTEGER, "
        "  hex_9 INTEGER, hex_10 INTEGER, "
        "  byte2_1 INTEGER, byte2_2 INTEGER, byte2_3 INTEGER, byte2_4 INTEGER, "
        "  byte2_5 INTEGER, byte2_6 INTEGER, byte2_7 INTEGER, byte2_8 INTEGER, "
        "  byte2_9 INTEGER, byte2_10 INTEGER, "
        "  msc_location INTEGER, vlr_location INTEGER)",
        nullptr, nullptr, nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db_,
                      "CREATE TABLE access_info (s_id INTEGER NOT NULL, "
                      "  ai_type INTEGER NOT NULL, "
                      "  data1 INTEGER, data2 INTEGER, data3 TEXT, data4 TEXT, "
                      "  PRIMARY KEY (s_id, ai_type), "
                      "  FOREIGN KEY (s_id) REFERENCES subscriber (s_id)"
                      ")",
                      nullptr, nullptr, nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db_,
                      "CREATE TABLE special_facility (s_id INTEGER NOT NULL, "
                      "  sf_type INTEGER NOT NULL, "
                      "  is_active INTEGER, error_cntrl INTEGER, "
                      "  data_a INTEGER, data_b TEXT, "
                      "  PRIMARY KEY (s_id, sf_type), "
                      "  FOREIGN KEY (s_id) REFERENCES subscriber (s_id))",
                      nullptr, nullptr, nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db_,
                      "CREATE TABLE call_forwarding (s_id INTEGER NOT NULL, "
                      "  sf_type INTEGER NOT NULL, "
                      "  start_time INTEGER NOT NULL, "
                      "  end_time INTEGER, numberx TEXT, "
                      "  PRIMARY KEY (s_id, sf_type, start_time), "
                      "  FOREIGN KEY (s_id, sf_type) "
                      "  REFERENCES special_facility(s_id, sf_type))",
                      nullptr, nullptr, nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db_,
                      "CREATE INDEX subscriber_sub_nbr_idx "
                      "ON subscriber (sub_nbr)",
                      nullptr, nullptr, nullptr);
    assert(rc == SQLITE_OK);
  }

  ~SQLite3TATPLoaderConnection() override { sqlite3_close(db_); }

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

      std::string sql_str = sql.str();
      int rc = sqlite3_exec(db_, sql_str.c_str(), nullptr, nullptr, nullptr);
      assert(rc == SQLITE_OK);
    }
  }

  sqlite3 *db_ = nullptr;
};

class SQLite3TATPClientConnection : public TATPClientConnection {
public:
  explicit SQLite3TATPClientConnection(const std::string &filename) {
    int rc;

    rc = sqlite3_open(filename.c_str(), &db_);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db_,
                            "SELECT * "
                            "FROM subscriber "
                            "WHERE s_id = ?",
                            -1, &stmts_[0], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_,
                         "SELECT cf.numberx "
                         "FROM special_facility AS sf, "
                         "  call_forwarding AS cf "
                         "WHERE sf.s_id = ? "
                         "  AND sf.sf_type = ? "
                         "  AND sf.is_active = 1 "
                         "  AND cf.s_id = sf.s_id "
                         "  AND cf.sf_type = sf.sf_type "
                         "  AND cf.start_time <= ? "
                         "  AND ? < cf.end_time",
                         -1, &stmts_[1], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_,
                         "SELECT data1, data2, data3, data4 "
                         "FROM access_info "
                         "WHERE s_id = ? "
                         "  AND ai_type = ?",
                         -1, &stmts_[2], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_,
                         "UPDATE subscriber "
                         "SET bit_1 = ? "
                         "WHERE s_id = ?",
                         -1, &stmts_[3], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_,
                         "UPDATE special_facility "
                         "SET data_a = ? "
                         "WHERE s_id = ? "
                         "  AND sf_type = ?",
                         -1, &stmts_[4], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_,
                         "UPDATE subscriber "
                         "SET vlr_location = ? "
                         "WHERE sub_nbr = ?",
                         -1, &stmts_[5], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_,
                         "SELECT s_id "
                         "FROM subscriber "
                         "WHERE sub_nbr = ?",
                         -1, &stmts_[6], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_,
                         "SELECT sf_type "
                         "FROM special_facility "
                         "WHERE s_id = ?",
                         -1, &stmts_[7], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_,
                         "INSERT INTO call_forwarding "
                         "VALUES (?, ?, ?, ?, ?)",
                         -1, &stmts_[8], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_,
                         "DELETE FROM call_forwarding "
                         "WHERE s_id = ?"
                         "  AND sf_type = ?"
                         "  AND start_time = ?",
                         -1, &stmts_[9], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_, "BEGIN", -1, &stmts_[10], nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare(db_, "COMMIT", -1, &stmts_[11], nullptr);
    assert(rc == SQLITE_OK);
  }

  ~SQLite3TATPClientConnection() override { sqlite3_close(db_); }

  ReturnCode get_subscriber_data(int s_id, std::string *sub_nbr,
                                 std::array<bool, 10> &bit,
                                 std::array<int, 10> &hex,
                                 std::array<int, 10> &byte2, int *msc_location,
                                 int *vlr_location) override {
    int rc;

    // Bind value to statement.
    rc = sqlite3_bind_int(stmts_[0], 1, s_id);
    assert(rc == SQLITE_OK);

    // Get subscriber data.
    rc = sqlite3_step(stmts_[0]);
    assert(rc == SQLITE_ROW);

    *sub_nbr = std::string((char *)sqlite3_column_text(stmts_[0], 1));

    for (int i = 0; i < bit.size(); ++i) {
      bit[i] = sqlite3_column_int(stmts_[0], i + 2);
    }

    for (int i = 0; i < hex.size(); ++i) {
      hex[i] = sqlite3_column_int(stmts_[0], i + 12);
    }

    for (int i = 0; i < hex.size(); ++i) {
      byte2[i] = sqlite3_column_int(stmts_[0], i + 22);
    }

    *msc_location = sqlite3_column_int(stmts_[0], 32);
    *vlr_location = sqlite3_column_int(stmts_[0], 33);

    rc = sqlite3_step(stmts_[0]);
    assert(rc == SQLITE_DONE);

    // Reset statement.
    rc = sqlite3_reset(stmts_[0]);
    assert(rc == SQLITE_OK);

    return TXBENCH_SUCCESS;
  }

  ReturnCode get_new_destination(int s_id, int sf_type, int start_time,
                                 int end_time,
                                 std::vector<std::string> *numberx) override {
    ReturnCode tx_rc = TXBENCH_MISSING;
    int rc;

    // Bind values to statement.
    rc = sqlite3_bind_int(stmts_[1], 1, s_id);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[1], 2, sf_type);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[1], 3, start_time);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[1], 4, end_time);
    assert(rc == SQLITE_OK);

    // Get new destination.
    rc = sqlite3_step(stmts_[1]);
    if (rc == SQLITE_ROW) {
      tx_rc = TXBENCH_SUCCESS;
      std::vector<std::string> result;
      do {
        result.emplace_back((char *)sqlite3_column_text(stmts_[1], 0));
      } while ((rc = sqlite3_step(stmts_[1])) == SQLITE_ROW);
      *numberx = result;
    }

    assert(rc == SQLITE_DONE);

    // Reset statement.
    rc = sqlite3_reset(stmts_[1]);
    assert(rc == SQLITE_OK);

    return tx_rc;
  }

  ReturnCode get_access_data(int s_id, int ai_type, int *data1, int *data2,
                             std::string *data3, std::string *data4) override {
    ReturnCode tx_rc = TXBENCH_MISSING;
    int rc;

    // Bind values to statement.
    rc = sqlite3_bind_int(stmts_[2], 1, s_id);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[2], 2, ai_type);
    assert(rc == SQLITE_OK);

    // Get access data.
    rc = sqlite3_step(stmts_[2]);
    if (rc == SQLITE_ROW) {
      tx_rc = TXBENCH_SUCCESS;

      *data1 = sqlite3_column_int(stmts_[2], 0);
      *data2 = sqlite3_column_int(stmts_[2], 1);
      *data3 = std::string((char *)sqlite3_column_text(stmts_[2], 3));
      *data4 = std::string((char *)sqlite3_column_text(stmts_[2], 3));

      rc = sqlite3_step(stmts_[2]);
    }

    assert(rc == SQLITE_DONE);

    // Reset statement.
    rc = sqlite3_reset(stmts_[2]);
    assert(rc == SQLITE_OK);

    return tx_rc;
  }

  ReturnCode update_subscriber_data(int s_id, bool bit_1, int sf_type,
                                    int data_a) override {
    int rc;

    // Bind values to update subscriber statement.
    rc = sqlite3_bind_int(stmts_[3], 1, bit_1);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[3], 2, s_id);
    assert(rc == SQLITE_OK);

    // Bind values to update special_facility statement.
    rc = sqlite3_bind_int(stmts_[4], 1, data_a);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[4], 2, s_id);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[4], 3, sf_type);
    assert(rc == SQLITE_OK);

    // Begin.
    rc = sqlite3_step(stmts_[10]);
    assert(rc == SQLITE_DONE);

    // Update subscriber.
    rc = sqlite3_step(stmts_[3]);
    assert(rc == SQLITE_DONE);
    assert(sqlite3_changes(db_) == 1);

    // Update special_facility.
    rc = sqlite3_step(stmts_[4]);
    assert(rc == SQLITE_DONE);
    assert(sqlite3_changes(db_) == 0 || sqlite3_changes(db_) == 1);

    // Commit.
    rc = sqlite3_step(stmts_[11]);
    assert(rc == SQLITE_DONE);

    // Reset statements.
    rc = sqlite3_reset(stmts_[10]);
    assert(rc == SQLITE_OK);
    rc = sqlite3_reset(stmts_[3]);
    assert(rc == SQLITE_OK);
    rc = sqlite3_reset(stmts_[4]);
    assert(rc == SQLITE_OK);
    rc = sqlite3_reset(stmts_[11]);
    assert(rc == SQLITE_OK);

    return sqlite3_changes(db_) == 0 ? TXBENCH_MISSING : TXBENCH_SUCCESS;
  }

  ReturnCode update_location(const std::string &sub_nbr,
                             int vlr_location) override {
    int rc;

    // Bind value to statement.
    rc = sqlite3_bind_int(stmts_[5], 1, vlr_location);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_text(stmts_[5], 2, sub_nbr.c_str(), -1, nullptr);
    assert(rc == SQLITE_OK);

    // Update location.
    rc = sqlite3_step(stmts_[5]);
    assert(rc == SQLITE_DONE);
    assert(sqlite3_changes(db_) == 1);

    // Reset statement.
    rc = sqlite3_reset(stmts_[5]);
    assert(rc == SQLITE_OK);

    return TXBENCH_SUCCESS;
  }

  ReturnCode insert_call_forwarding(std::string sub_nbr, int sf_type,
                                    int start_time, int end_time,
                                    std::string numberx) override {
    ReturnCode tx_rc = TXBENCH_SUCCESS;
    int rc;

    // Bind value to select subscriber statement.
    rc = sqlite3_bind_text(stmts_[6], 1, sub_nbr.c_str(), -1, nullptr);
    assert(rc == SQLITE_OK);

    // Bind values to select insert call_forwarding statement.
    rc = sqlite3_bind_int(stmts_[8], 2, sf_type);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[8], 3, start_time);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[8], 4, end_time);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_text(stmts_[8], 5, numberx.c_str(), -1, nullptr);
    assert(rc == SQLITE_OK);

    // Begin.
    rc = sqlite3_step(stmts_[10]);
    assert(rc == SQLITE_DONE);

    // Select subscriber.
    rc = sqlite3_step(stmts_[6]);
    assert(rc == SQLITE_ROW);
    int s_id = sqlite3_column_int(stmts_[6], 0);
    rc = sqlite3_step(stmts_[6]);
    assert(rc == SQLITE_DONE);

    // Bind value to select special_facility statement.
    rc = sqlite3_bind_int(stmts_[7], 1, s_id);
    assert(rc == SQLITE_OK);

    // Select special_facility.
    rc = sqlite3_step(stmts_[7]);
    assert(rc == SQLITE_ROW);
    while (rc == SQLITE_ROW) {
      rc = sqlite3_step(stmts_[7]);
    }
    assert(rc == SQLITE_DONE);

    // Bind value to insert call_forwarding statement.
    rc = sqlite3_bind_int(stmts_[8], 1, s_id);
    assert(rc == SQLITE_OK);

    // Insert call_forwarding.
    rc = sqlite3_step(stmts_[8]);
    std::cout << sqlite3_errmsg(db_) << std::endl;
    assert(rc == SQLITE_DONE && sqlite3_changes(db_) == 1 ||
           rc == SQLITE_CONSTRAINT && sqlite3_changes(db_) == 0);

    // Commit.
    rc = sqlite3_step(stmts_[11]);
    assert(rc == SQLITE_DONE);

    // Reset statements.
    rc = sqlite3_reset(stmts_[10]);
    assert(rc == SQLITE_OK);
    rc = sqlite3_reset(stmts_[6]);
    assert(rc == SQLITE_OK);
    rc = sqlite3_reset(stmts_[7]);
    assert(rc == SQLITE_OK);

    rc = sqlite3_reset(stmts_[8]);
    assert(rc == SQLITE_OK || rc == SQLITE_CONSTRAINT);
    if (rc == SQLITE_CONSTRAINT) {
      // Primary key conflict (acceptable error).
      tx_rc = TXBENCH_CONSTRAINT;
    }

    rc = sqlite3_reset(stmts_[11]);
    assert(rc == SQLITE_OK);

    return tx_rc;
  }

  ReturnCode delete_call_forwarding(const std::string &sub_nbr, int sf_type,
                                    int start_time) override {
    ReturnCode tx_rc = TXBENCH_MISSING;
    int rc;

    // Bind value to select subscriber statement.
    rc = sqlite3_bind_text(stmts_[6], 1, sub_nbr.c_str(), -1, nullptr);
    assert(rc == SQLITE_OK);

    // Begin.
    rc = sqlite3_step(stmts_[10]);
    assert(rc == SQLITE_DONE);

    // Select subscriber.
    rc = sqlite3_step(stmts_[6]);
    assert(rc == SQLITE_ROW);
    int s_id = sqlite3_column_int(stmts_[6], 0);
    rc = sqlite3_step(stmts_[6]);
    assert(rc == SQLITE_DONE);

    // Bind values to delete call_forwarding statement.
    rc = sqlite3_bind_int(stmts_[9], 1, s_id);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[9], 2, sf_type);
    assert(rc == SQLITE_OK);
    rc = sqlite3_bind_int(stmts_[9], 3, start_time);
    assert(rc == SQLITE_OK);

    // Delete call_forwarding.
    rc = sqlite3_step(stmts_[9]);
    assert(rc == SQLITE_DONE);
    assert(sqlite3_changes(db_) == 0 || sqlite3_changes(db_) == 1);
    if (sqlite3_changes(db_) == 1) {
      tx_rc = TXBENCH_SUCCESS;
    }

    // Commit.
    rc = sqlite3_step(stmts_[11]);
    assert(rc == SQLITE_DONE);

    // Reset statements.
    rc = sqlite3_reset(stmts_[10]);
    assert(rc == SQLITE_OK);
    rc = sqlite3_reset(stmts_[6]);
    assert(rc == SQLITE_OK);
    rc = sqlite3_reset(stmts_[9]);
    assert(rc == SQLITE_OK);
    rc = sqlite3_reset(stmts_[11]);
    assert(rc == SQLITE_OK);

    return tx_rc;
  }

private:
  sqlite3 *db_ = nullptr;
  std::array<sqlite3_stmt *, 12> stmts_{};
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
