#ifndef SQLITE_PERFORMANCE_TATP_HELPERS_HPP
#define SQLITE_PERFORMANCE_TATP_HELPERS_HPP

#include "cxxopts.hpp"

#include <sstream>

cxxopts::Options tatp_options(const std::string &program,
                              const std::string &help_string = "") {
  cxxopts::Options options(program, help_string);
  cxxopts::OptionAdder adder = options.add_options();
  adder("load", "Load the database");
  adder("run", "Run the benchmark");
  adder("records", "Number of subscriber records",
        cxxopts::value<uint64_t>()->default_value("1000"));
  adder("clients", "Number of clients",
        cxxopts::value<size_t>()->default_value("1"));
  adder("warmup", "Warmup duration in seconds",
        cxxopts::value<size_t>()->default_value("10"));
  adder("measure", "Measure duration in seconds",
        cxxopts::value<size_t>()->default_value("60"));
  adder("help", "Print help");
  return options;
}

std::vector<std::string>
tatp_create_sql(const std::string &bool_type, const std::string &uint8_type,
                const std::string &uint32_type, const std::string &uint64_type,
                const std::string &string_type, bool enable_foreign_keys) {
  std::vector<std::string> sql = {"DROP TABLE IF EXISTS call_forwarding",
                                  "DROP TABLE IF EXISTS special_facility",
                                  "DROP TABLE IF EXISTS access_info",
                                  "DROP TABLE IF EXISTS subscriber"};

  std::ostringstream subscriber;
  subscriber << "CREATE TABLE subscriber ("
             << "s_id " << uint64_type << ", "
             << "sub_nbr " << string_type << " UNIQUE, ";
  for (int i = 1; i <= 10; ++i) {
    subscriber << "bit_" << i << " " << bool_type << ", ";
  }
  for (int i = 1; i <= 10; ++i) {
    subscriber << "hex_" << i << " " << uint8_type << ", ";
  }
  for (int i = 1; i <= 10; ++i) {
    subscriber << "byte2_" << i << " " << uint8_type << ", ";
  }
  subscriber << "msc_location " << uint32_type << ", "
             << "vlr_location " << uint32_type << ", "
             << "PRIMARY KEY (s_id))" << std::endl;
  sql.push_back(subscriber.str());

  std::ostringstream access_info;
  access_info << "CREATE TABLE access_info ("
              << "s_id " << uint64_type << ", "
              << "ai_type " << uint8_type << ", "
              << "data1 " << uint8_type << ", "
              << "data2 " << uint8_type << ", "
              << "data3 " << string_type << ", "
              << "data4 " << string_type << ", "
              << "PRIMARY KEY (s_id, ai_type)";
  if (enable_foreign_keys) {
    access_info << ", FOREIGN KEY (s_id) REFERENCES subscriber (s_id)";
  }
  access_info << ")" << std::endl;
  sql.push_back(access_info.str());

  std::ostringstream special_facility;
  special_facility << "CREATE TABLE special_facility ("
                   << "s_id " << uint64_type << ", "
                   << "sf_type " << uint8_type << ", "
                   << "is_active " << bool_type << ", "
                   << "error_cntrl " << uint8_type << ", "
                   << "data_a " << uint8_type << ", "
                   << "data_b " << string_type << ", "
                   << "PRIMARY KEY (s_id, sf_type)";
  if (enable_foreign_keys) {
    special_facility << ", FOREIGN KEY (s_id) REFERENCES subscriber (s_id)";
  }
  special_facility << ")" << std::endl;
  sql.push_back(special_facility.str());

  std::ostringstream call_forwarding;
  call_forwarding << "CREATE TABLE call_forwarding ("
                  << "s_id " << uint64_type << ", "
                  << "sf_type " << uint8_type << ", "
                  << "start_time " << uint8_type << ", "
                  << "end_time " << uint8_type << ", "
                  << "numberx " << string_type << ", "
                  << "PRIMARY KEY (s_id, sf_type, start_time)";
  if (enable_foreign_keys) {
    call_forwarding << ", FOREIGN KEY (s_id, sf_type) "
                    << "REFERENCES special_facility (s_id, sf_type)";
  }
  call_forwarding << ")" << std::endl;
  sql.push_back(call_forwarding.str());

  return sql;
}

std::array<std::string, 10> tatp_statement_sql() {
  return {"SELECT * "
          "FROM subscriber "
          "WHERE s_id = ?;",

          "SELECT cf.numberx "
          "FROM special_facility AS sf, call_forwarding AS cf "
          "WHERE sf.s_id = ? AND sf.sf_type = ? AND sf.is_active = 1 "
          "  AND cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type "
          "  AND cf.start_time <= ? AND ? < cf.end_time;",

          "SELECT data1, data2, data3, data4 "
          "FROM access_info "
          "WHERE s_id = ? AND ai_type = ?;",

          "UPDATE subscriber "
          "SET bit_1 = ? "
          "WHERE s_id = ?;",

          "UPDATE special_facility "
          "SET data_a = ? "
          "WHERE s_id = ? AND sf_type = ?;",

          "UPDATE subscriber "
          "SET vlr_location = ? "
          "WHERE sub_nbr = ?;",

          "SELECT s_id "
          "FROM subscriber "
          "WHERE sub_nbr = ?;",

          "SELECT sf_type "
          "FROM special_facility "
          "WHERE s_id = ?;",

          "INSERT INTO call_forwarding "
          "VALUES (?, ?, ?, ?, ?);",

          "DELETE FROM call_forwarding "
          "WHERE s_id = ? AND sf_type = ? AND start_time = ?;"};
}

#endif // SQLITE_PERFORMANCE_TATP_HELPERS_HPP
