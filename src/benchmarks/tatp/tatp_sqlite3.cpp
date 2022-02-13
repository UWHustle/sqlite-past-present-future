#include "cxxopts.hpp"
#include "dbbench/benchmarks/tatp.hpp"
#include "dbbench/runner.hpp"
#include "helpers.hpp"
#include "systems/sqlite/sqlite3.hpp"

#include <utility>

template <class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

void load(sqlite::Database &db, uint64_t n_subscriber_records) {
  sqlite::Connection conn = db.connect();
  for (const std::string &sql : tatp_create_sql("INTEGER", "INTEGER", "INTEGER",
                                                "INTEGER", "TEXT", true)) {
    conn.execute(sql);
  }

  sqlite::Statement insert_subscriber =
      conn.prepare("INSERT INTO subscriber VALUES ("
                   "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                   "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
  sqlite::Statement insert_access_info =
      conn.prepare("INSERT INTO access_info VALUES (?,?,?,?,?,?)");
  sqlite::Statement insert_special_facility =
      conn.prepare("INSERT INTO special_facility VALUES (?,?,?,?,?,?)");
  sqlite::Statement insert_call_forwarding =
      conn.prepare("INSERT INTO call_forwarding VALUES (?,?,?,?,?)");

  conn.begin();

  dbbench::tatp::RecordGenerator record_generator(n_subscriber_records);
  while (auto record = record_generator.next()) {
    std::visit(overloaded{
                   [&](const dbbench::tatp::SubscriberRecord &r) {
                     insert_subscriber.bind(1, (sqlite3_int64)r.s_id);
                     insert_subscriber.bind(2, r.sub_nbr);
                     for (int i = 0; i < 10; ++i) {
                       insert_subscriber.bind(i + 3, (int)r.bit[i]);
                     }
                     for (int i = 0; i < 10; ++i) {
                       insert_subscriber.bind(i + 13, (int)r.hex[i]);
                     }
                     for (int i = 0; i < 10; ++i) {
                       insert_subscriber.bind(i + 23, (int)r.byte2[i]);
                     }
                     insert_subscriber.bind(33, (sqlite3_int64)r.msc_location);
                     insert_subscriber.bind(34, (sqlite3_int64)r.vlr_location);

                     insert_subscriber.step();
                     insert_subscriber.reset();
                   },

                   [&](const dbbench::tatp::AccessInfoRecord &r) {
                     insert_access_info.bind(1, (sqlite3_int64)r.s_id);
                     insert_access_info.bind(2, (int)r.ai_type);
                     insert_access_info.bind(3, (int)r.data1);
                     insert_access_info.bind(4, (int)r.data2);
                     insert_access_info.bind(5, r.data3);
                     insert_access_info.bind(6, r.data4);

                     insert_access_info.step();
                     insert_access_info.reset();
                   },

                   [&](const dbbench::tatp::SpecialFacilityRecord &r) {
                     insert_special_facility.bind(1, (sqlite3_int64)r.s_id);
                     insert_special_facility.bind(2, (int)r.sf_type);
                     insert_special_facility.bind(3, (int)r.is_active);
                     insert_special_facility.bind(4, (int)r.error_cntrl);
                     insert_special_facility.bind(5, (int)r.data_a);
                     insert_special_facility.bind(6, r.data_b);

                     insert_special_facility.step();
                     insert_special_facility.reset();
                   },

                   [&](const dbbench::tatp::CallForwardingRecord &r) {
                     insert_call_forwarding.bind(1, (sqlite3_int64)r.s_id);
                     insert_call_forwarding.bind(2, (int)r.sf_type);
                     insert_call_forwarding.bind(3, (int)r.start_time);
                     insert_call_forwarding.bind(4, (int)r.end_time);
                     insert_call_forwarding.bind(5, r.numberx);

                     insert_call_forwarding.step();
                     insert_call_forwarding.reset();
                   },
               },
               *record);
  }

  conn.commit();
}

class Worker {
public:
  Worker(sqlite::Connection conn, uint64_t n_subscriber_records)
      : conn_(std::move(conn)), procedure_generator_(n_subscriber_records) {
    for (const std::string &sql : tatp_statement_sql()) {
      stmts_.push_back(conn_.prepare(sql));
    }
  }

  bool operator()() {
    return std::visit(overloaded{
                          [&](const dbbench::tatp::GetSubscriberData &p) {
                            stmts_[0].bind(1, (sqlite3_int64)p.s_id);
                            stmts_[0].step().value();
                            stmts_[0].reset();
                            return true;
                          },

                          [&](const dbbench::tatp::GetNewDestination &p) {
                            stmts_[1].bind(1, (sqlite3_int64)p.s_id);
                            stmts_[1].bind(2, (int)p.sf_type);
                            stmts_[1].bind(3, (int)p.start_time);
                            stmts_[1].bind(4, (int)p.end_time);
                            size_t count = 0;
                            while (stmts_[1].step().has_value()) {
                              ++count;
                            }
                            stmts_[1].reset();
                            return count > 0;
                          },

                          [&](const dbbench::tatp::GetAccessData &p) {
                            stmts_[2].bind(1, (sqlite3_int64)p.s_id);
                            stmts_[2].bind(2, (int)p.ai_type);
                            size_t count = 0;
                            while (stmts_[2].step().has_value()) {
                              ++count;
                            }
                            stmts_[2].reset();
                            return count > 0;
                          },

                          [&](const dbbench::tatp::UpdateSubscriberData &p) {
                            conn_.begin();

                            stmts_[3].bind(1, (int)p.bit_1);
                            stmts_[3].bind(2, (sqlite3_int64)p.s_id);
                            stmts_[3].step();
                            stmts_[3].reset();

                            stmts_[4].bind(1, (int)p.data_a);
                            stmts_[4].bind(2, (sqlite3_int64)p.s_id);
                            stmts_[4].bind(3, (int)p.sf_type);
                            stmts_[4].step();
                            stmts_[4].reset();

                            conn_.commit();

                            return conn_.changes() > 0;
                          },

                          [&](const dbbench::tatp::UpdateLocation &p) {
                            stmts_[5].bind(1, (sqlite3_int64)p.vlr_location);
                            stmts_[5].bind(2, p.sub_nbr);
                            stmts_[5].step();
                            stmts_[5].reset();
                            return true;
                          },

                          [&](const dbbench::tatp::InsertCallForwarding &p) {
                            conn_.begin();

                            stmts_[6].bind(1, p.sub_nbr);
                            uint64_t s_id = stmts_[6].step()->column_int64(0);
                            stmts_[6].reset();

                            stmts_[7].bind(1, (sqlite3_int64)s_id);
                            while (stmts_[7].step())
                              ;
                            stmts_[7].reset();

                            stmts_[8].bind(1, (sqlite3_int64)s_id);
                            stmts_[8].bind(2, (int)p.sf_type);
                            stmts_[8].bind(3, (int)p.start_time);
                            stmts_[8].bind(4, (int)p.end_time);
                            stmts_[8].bind(5, p.numberx);
                            bool success = true;
                            try {
                              stmts_[8].step();
                            } catch (const sqlite::ConstraintException &e) {
                              // Constraint violation is an acceptable error.
                              success = false;
                            }
                            try {
                              stmts_[8].reset();
                            } catch (const sqlite::ConstraintException &) {
                              // Constraint violation is an acceptable error.
                            }

                            conn_.commit();

                            return success;
                          },

                          [&](const dbbench::tatp::DeleteCallForwarding &p) {
                            conn_.begin();

                            stmts_[6].bind(1, p.sub_nbr);
                            uint64_t s_id = stmts_[6].step()->column_int64(0);
                            stmts_[6].reset();

                            stmts_[9].bind(1, (sqlite3_int64)s_id);
                            stmts_[9].bind(2, (int)p.sf_type);
                            stmts_[9].bind(3, (int)p.start_time);
                            stmts_[9].step();
                            stmts_[9].reset();

                            conn_.commit();

                            return conn_.changes() > 0;
                          },
                      },
                      procedure_generator_.next());
  }

private:
  sqlite::Connection conn_;
  std::vector<sqlite::Statement> stmts_;
  dbbench::tatp::ProcedureGenerator procedure_generator_;
};

int main(int argc, char **argv) {
  cxxopts::Options options = tatp_options("tatp_sqlite3", "TATP on SQLite3");

  cxxopts::OptionAdder adder = options.add_options("SQLite3");
  adder("journal_mode", "Journal mode",
        cxxopts::value<std::string>()->default_value("DELETE"));
  adder("cache_size", "Cache size",
        cxxopts::value<std::string>()->default_value("-1000000"));

  cxxopts::ParseResult result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  auto n_subscriber_records = result["records"].as<uint64_t>();
  auto journal_mode = result["journal_mode"].as<std::string>();
  auto cache_size = result["cache_size"].as<std::string>();

  sqlite::Database db("tatp.sqlite");

  if (result.count("load")) {
    load(db, n_subscriber_records);
  }

  if (result.count("run")) {
    std::vector<Worker> workers;
    for (size_t i = 0; i < result["clients"].as<size_t>(); ++i) {
      sqlite::Connection conn = db.connect();
      conn.execute("PRAGMA journal_mode=" + journal_mode);
      conn.execute("PRAGMA cache_size=" + cache_size);
      workers.emplace_back(std::move(conn), n_subscriber_records);
    }

    double throughput = dbbench::run(workers, result["warmup"].as<size_t>(),
                                     result["measure"].as<size_t>());

    std::cout << throughput << std::endl;
  }

  return 0;
}
