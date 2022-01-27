#ifndef SQLITE_PERFORMANCE_SQLITE3_HPP
#define SQLITE_PERFORMANCE_SQLITE3_HPP

#include "sqlite3.h"

#include <array>
#include <exception>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace sqlite {

class Exception : public std::exception {
public:
  explicit Exception(int code, const char *explanation = nullptr)
      : code_(code) {
    std::ostringstream stream;
    stream << "SQLite3 error " << code_ << " (" << sqlite3_errstr(code_) << ")";
    if (explanation) {
      stream << ": " << explanation;
    }
    what_ = stream.str();
  }

  [[nodiscard]] const char *what() const noexcept override {
    return what_.c_str();
  }

  [[nodiscard]] int code() const { return code_; }

private:
  int code_;
  std::string what_;
};

void assert_sqlite_ok(int rc) {
  if (rc != SQLITE_OK) {
    throw Exception(rc);
  }
}

class Statement;

class Connection {
public:
  explicit Connection(const std::string &path) {
    int rc;

    rc = sqlite3_initialize();
    assert_sqlite_ok(rc);

    rc = sqlite3_open(path.c_str(), &db_);
    assert_sqlite_ok(rc);

    std::array<const char *, 3> tx_sql = {"BEGIN", "COMMIT", "ROLLBACK"};
    for (int i = 0; i < 3; ++i) {
      rc = sqlite3_prepare_v2(db_, tx_sql[i], -1, &tx_stmts_[i], nullptr);
      assert_sqlite_ok(rc);
    }
  }

  ~Connection() {
    for (int i = 0; i < 3; ++i) {
      sqlite3_finalize(tx_stmts_[i]);
    }
    sqlite3_close(db_);
  }

  void execute(const std::string &sql) {
    int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, nullptr);
    assert_sqlite_ok(rc);
  }

  int changes() { return sqlite3_changes(db_); }

  void begin() { tx_stmt(0); }

  void commit() { tx_stmt(1); }

  void rollback() { tx_stmt(2); }

  sqlite3 *ptr() { return db_; }

private:
  void tx_stmt(int i) {
    sqlite3_step(tx_stmts_[i]);
    int rc = sqlite3_reset(tx_stmts_[i]);
    assert_sqlite_ok(rc);
  }

  sqlite3 *db_{};
  std::array<sqlite3_stmt *, 3> tx_stmts_{};
};

class Statement {
public:
  Statement(std::shared_ptr<Connection> connection, const std::string &sql)
      : conn_(std::move(connection)), stmt_(nullptr) {
    int rc = sqlite3_prepare_v2(conn_->ptr(), sql.c_str(), -1, &stmt_, nullptr);
    assert_sqlite_ok(rc);
  }

  ~Statement() { sqlite3_finalize(stmt_); }

  void bind_int(int i, int arg) {
    int rc = sqlite3_bind_int(stmt_, i + 1, arg);
    assert_sqlite_ok(rc);
  }

  void bind_text(int i, const char *arg) {
    int rc = sqlite3_bind_text(stmt_, i + 1, arg, -1, nullptr);
    assert_sqlite_ok(rc);
  }

  void bind_string(int i, const std::string &arg) { bind_text(i, arg.c_str()); }

  bool step() {
    int rc = sqlite3_step(stmt_);
    switch (rc) {
    case SQLITE_ROW:
      return true;
    case SQLITE_DONE:
      return false;
    default:
      throw Exception(rc);
    }
  }

  int column_int(int i) { return sqlite3_column_int(stmt_, i); }

  const unsigned char *column_text(int i) {
    return sqlite3_column_text(stmt_, i);
  }

  std::string column_string(int i) { return {(char *)column_text(i)}; }

  void reset() {
    int rc = sqlite3_reset(stmt_);
    assert_sqlite_ok(rc);
  }

  sqlite3_stmt *ptr() { return stmt_; }

private:
  std::shared_ptr<Connection> conn_;
  sqlite3_stmt *stmt_;
};

} // namespace sqlite

#endif // SQLITE_PERFORMANCE_SQLITE3_HPP
