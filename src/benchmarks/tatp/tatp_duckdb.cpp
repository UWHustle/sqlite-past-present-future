#include "cxxopts.hpp"
#include "dbbench/benchmarks/tatp.hpp"
#include "dbbench/runner.hpp"
#include "systems/duckdb/duckdb.hpp"
#include "helpers.hpp"

template <class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

void assert_success(const std::unique_ptr<duckdb::QueryResult> &result) {
  if (!result->success || result->HasError()) {
    throw std::runtime_error(result->error);
  }
}

void load(duckdb::DuckDB &db, uint64_t n_subscriber_records) {
  duckdb::Connection conn(db);
  for (const std::string &sql : tatp_create_sql(
           "BOOLEAN", "UTINYINT", "UINTEGER", "UBIGINT", "VARCHAR", false)) {
    assert_success(conn.Query(sql));
  }

  duckdb::Appender subscriber(conn, "subscriber");
  duckdb::Appender access_info(conn, "access_info");
  duckdb::Appender special_facility(conn, "special_facility");
  duckdb::Appender call_forwarding(conn, "call_forwarding");

  dbbench::tatp::RecordGenerator record_generator(n_subscriber_records);
  while (auto record = record_generator.next()) {
    std::visit(overloaded{
                   [&](const dbbench::tatp::SubscriberRecord &r) {
                     subscriber.BeginRow();
                     subscriber.Append(r.s_id);
                     subscriber.Append(duckdb::string_t(r.sub_nbr));
                     for (bool bit : r.bit) {
                       subscriber.Append(bit);
                     }
                     for (uint8_t hex : r.hex) {
                       subscriber.Append(hex);
                     }
                     for (uint8_t byte2 : r.byte2) {
                       subscriber.Append(byte2);
                     }
                     subscriber.Append(r.msc_location);
                     subscriber.Append(r.vlr_location);
                     subscriber.EndRow();
                   },

                   [&](const dbbench::tatp::AccessInfoRecord &r) {
                     access_info.AppendRow(r.s_id, r.ai_type, r.data1, r.data2,
                                           duckdb::string_t(r.data3),
                                           duckdb::string_t(r.data4));
                   },

                   [&](const dbbench::tatp::SpecialFacilityRecord &r) {
                     special_facility.AppendRow(r.s_id, r.sf_type, r.is_active,
                                                r.error_cntrl, r.data_a,
                                                duckdb::string_t(r.data_b));
                   },

                   [&](const dbbench::tatp::CallForwardingRecord &r) {
                     call_forwarding.AppendRow(r.s_id, r.sf_type, r.start_time,
                                               r.end_time,
                                               duckdb::string_t(r.numberx));
                   },
               },
               *record);
  }
}

class Worker {
public:
  Worker(duckdb::Connection conn, uint64_t n_subscriber_records)
      : conn_(std::move(conn)), procedure_generator_(n_subscriber_records) {
    for (const std::string &sql : tatp_statement_sql()) {
      stmts_.push_back(conn_.Prepare(sql));
    }
  }

  bool operator()() {
    return std::visit(
        overloaded{
            [&](const dbbench::tatp::GetSubscriberData &p) {
              assert_success(stmts_[0]->Execute(p.s_id));
              return true;
            },

            [&](const dbbench::tatp::GetNewDestination &p) {
              auto result = stmts_[1]->Execute(p.s_id, p.sf_type, p.start_time,
                                               p.end_time);
              assert_success(result);
              size_t count = 0;
              for (auto &row : *result) {
                ++count;
              }
              return count > 0;
            },

            [&](const dbbench::tatp::GetAccessData &p) {
              auto result = stmts_[2]->Execute(p.s_id, p.ai_type);
              assert_success(result);
              size_t count = 0;
              for (auto &row : *result) {
                ++count;
              }
              return count > 0;
            },

            [&](const dbbench::tatp::UpdateSubscriberData &p) {
              conn_.BeginTransaction();

              assert_success(stmts_[3]->Execute(p.bit_1, p.s_id));

              auto result = stmts_[4]->Execute(p.data_a, p.s_id, p.sf_type);
              assert_success(result);
              auto changes = result->Fetch()->GetValue(0, 0).GetValue<int>();

              conn_.Commit();

              return changes > 0;
            },

            [&](const dbbench::tatp::UpdateLocation &p) {
              assert_success(stmts_[5]->Execute(p.vlr_location, p.sub_nbr));
              return true;
            },

            [&](const dbbench::tatp::InsertCallForwarding &p) {
              conn_.BeginTransaction();

              auto result = stmts_[6]->Execute(p.sub_nbr);
              assert_success(result);
              auto s_id = result->Fetch()->GetValue(0, 0).GetValue<uint64_t>();

              assert_success(stmts_[7]->Execute(s_id));

              result = stmts_[8]->Execute(s_id, p.sf_type, p.start_time,
                                          p.end_time, p.numberx);
              // Constraint violation is an acceptable error.
              if (!result->success &&
                  result->error.rfind("Constraint", 0) != 0) {
                assert_success(result);
              }

              conn_.Commit();

              return false;
            },

            [&](const dbbench::tatp::DeleteCallForwarding &p) {
              conn_.BeginTransaction();

              auto result = stmts_[6]->Execute(p.sub_nbr);
              assert_success(result);
              auto s_id = result->Fetch()->GetValue(0, 0).GetValue<uint64_t>();

              result = stmts_[9]->Execute(s_id, p.sf_type, p.start_time);
              assert_success(result);
              auto changes = result->Fetch()->GetValue(0, 0).GetValue<int>();

              conn_.Commit();

              return changes > 0;
            },
        },
        procedure_generator_.next());
  }

private:
  duckdb::Connection conn_;
  std::vector<std::unique_ptr<duckdb::PreparedStatement>> stmts_;
  dbbench::tatp::ProcedureGenerator procedure_generator_;
};

int main(int argc, char **argv) {
  cxxopts::Options options = tatp_options("tatp_duckdb", "TATP on DuckDB");

  cxxopts::OptionAdder adder = options.add_options("DuckDB");
  adder("memory_limit", "Memory limit",
        cxxopts::value<std::string>()->default_value("1GB"));
  adder("threads", "Number of threads",
        cxxopts::value<std::string>()->default_value("1"));

  cxxopts::ParseResult result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  auto n_subscriber_records = result["records"].as<uint64_t>();
  auto memory_limit = result["memory_limit"].as<std::string>();
  auto threads = result["threads"].as<std::string>();

  duckdb::DuckDB db("tatp.duckdb");

  if (result.count("load")) {
    load(db, n_subscriber_records);
  }

  if (result.count("run")) {
    std::vector<Worker> workers;
    for (size_t i = 0; i < result["clients"].as<size_t>(); ++i) {
      duckdb::Connection conn(db);
      assert_success(conn.Query("PRAGMA memory_limit='" + memory_limit + "'"));
      assert_success(conn.Query("PRAGMA threads=" + threads));
      workers.emplace_back(std::move(conn), n_subscriber_records);
    }

    double throughput = dbbench::run(workers, result["warmup"].as<size_t>(),
                                     result["measure"].as<size_t>());

    std::cout << throughput << std::endl;
  }
}
