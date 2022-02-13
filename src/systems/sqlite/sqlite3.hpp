#ifndef SQLITE_PERFORMANCE_SQLITE_HPP
#define SQLITE_PERFORMANCE_SQLITE_HPP

#include "sqlite3.h"

#include <array>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <variant>

namespace sqlite {

class Exception : public std::exception {
public:
  explicit Exception(int code, const std::string &message = "") : code_(code) {
    std::ostringstream stream;
    stream << "SQLite3 code " << code << " (" << sqlite3_errstr(code) << ")";
    if (!message.empty()) {
      stream << ": " << message;
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

class ConstraintException : public Exception {
public:
  explicit ConstraintException(const std::string &message = "")
      : Exception(SQLITE_CONSTRAINT, message) {}
};

class ConnectionContext {
public:
  explicit ConnectionContext(sqlite3 *db) : db_(db) {}

  ~ConnectionContext() {
    int rc = sqlite3_close(db_);
    if (rc != SQLITE_OK) {
      std::cerr << "sqlite3_close: " << sqlite3_errstr(rc) << std::endl;
    }
  }

  [[nodiscard]] sqlite3 *raw() const { return db_; }

private:
  sqlite3 *db_;
};

class StatementContext {
public:
  explicit StatementContext(sqlite3_stmt *stmt) : stmt_(stmt) {}

  ~StatementContext() {
    int rc = sqlite3_finalize(stmt_);
    if (rc != SQLITE_OK) {
      std::cerr << "sqlite3_finalize: " << sqlite3_errstr(rc) << std::endl;
    }
  }

  [[nodiscard]] sqlite3_stmt *raw() const { return stmt_; }

private:
  sqlite3_stmt *stmt_;
};

class Row {
public:
  explicit Row(std::shared_ptr<StatementContext> stmt_ctx)
      : stmt_ctx_(std::move(stmt_ctx)) {}

  int column_int(int i) { return sqlite3_column_int(stmt_ctx_->raw(), i); }

  sqlite3_int64 column_int64(int i) {
    return sqlite3_column_int64(stmt_ctx_->raw(), i);
  }

private:
  std::shared_ptr<StatementContext> stmt_ctx_;
};

class Statement {
  friend class Connection;

public:
  void bind(int i, int v) {
    int rc = sqlite3_bind_int(stmt_ctx_->raw(), i, v);
    if (rc != SQLITE_OK) {
      throw Exception(rc, sqlite3_errmsg(conn_ctx_->raw()));
    }
  }

  void bind(int i, sqlite3_int64 v) {
    int rc = sqlite3_bind_int64(stmt_ctx_->raw(), i, v);
    if (rc != SQLITE_OK) {
      throw Exception(rc, sqlite3_errmsg(conn_ctx_->raw()));
    }
  }

  void bind(int i, const std::string &v) {
    int rc =
        sqlite3_bind_text(stmt_ctx_->raw(), i, v.c_str(), -1, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
      throw Exception(rc, sqlite3_errmsg(conn_ctx_->raw()));
    }
  }

  void bind(int i, const void *v, int n) {
    int rc = sqlite3_bind_blob(stmt_ctx_->raw(), i, v, n, nullptr);
    if (rc != SQLITE_OK) {
      throw Exception(rc, sqlite3_errmsg(conn_ctx_->raw()));
    }
  }

  template <typename T> void bind(int i, T v) = delete;

  std::optional<Row> step() {
    int rc = sqlite3_step(stmt_ctx_->raw());
    switch (rc) {
    case SQLITE_ROW:
      return Row(stmt_ctx_);
    case SQLITE_DONE:
      return {};
    case SQLITE_CONSTRAINT:
      throw ConstraintException(sqlite3_errmsg(conn_ctx_->raw()));
    default:
      throw Exception(rc, sqlite3_errmsg(conn_ctx_->raw()));
    }
  }

  void reset() {
    int rc = sqlite3_reset(stmt_ctx_->raw());
    switch (rc) {
    case SQLITE_OK:
      break;
    case SQLITE_CONSTRAINT:
      throw ConstraintException(sqlite3_errmsg(conn_ctx_->raw()));
    default:
      throw Exception(rc, sqlite3_errmsg(conn_ctx_->raw()));
    }
  }

private:
  explicit Statement(std::shared_ptr<ConnectionContext> conn_ctx,
                     std::shared_ptr<StatementContext> stmt_ctx)
      : conn_ctx_(std::move(conn_ctx)), stmt_ctx_(std::move(stmt_ctx)) {}

  std::shared_ptr<ConnectionContext> conn_ctx_;
  std::shared_ptr<StatementContext> stmt_ctx_;
};

class Connection {
  friend class Database;

public:
  [[nodiscard]] Statement prepare(const std::string &sql) {
    sqlite3_stmt *stmt;
    int rc =
        sqlite3_prepare_v2(conn_ctx_->raw(), sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
      throw Exception(rc, sqlite3_errmsg(conn_ctx_->raw()));
    }
    return Statement(conn_ctx_, std::make_shared<StatementContext>(stmt));
  }

  void execute(const std::string &sql) {
    int rc =
        sqlite3_exec(conn_ctx_->raw(), sql.c_str(), nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) {
      throw Exception(rc, sqlite3_errmsg(conn_ctx_->raw()));
    }
  }

  void begin() {
    begin_.step();
    begin_.reset();
  }

  void commit() {
    commit_.step();
    commit_.reset();
  }

  int changes() { return sqlite3_changes(conn_ctx_->raw()); }

  [[nodiscard]] const std::shared_ptr<ConnectionContext> &context() const {
    return conn_ctx_;
  }

private:
  explicit Connection(std::shared_ptr<ConnectionContext> conn_ctx)
      : conn_ctx_(std::move(conn_ctx)), begin_(prepare("BEGIN")),
        commit_(prepare("COMMIT")) {}

  std::shared_ptr<ConnectionContext> conn_ctx_;
  Statement begin_;
  Statement commit_;
};

class Database {
public:
  explicit Database(std::string filename) : filename_(std::move(filename)) {
    int rc = sqlite3_initialize();
    if (rc != SQLITE_OK) {
      throw Exception(rc, "could not initialize SQLite3");
    }
  }

  Connection connect() {
    sqlite3 *db;
    int rc = sqlite3_open(filename_.c_str(), &db);
    if (rc != SQLITE_OK) {
      throw Exception(rc, sqlite3_errmsg(db));
    }
    return Connection(std::make_shared<ConnectionContext>(db));
  }

private:
  std::string filename_;
};

} // namespace sqlite

#endif // SQLITE_PERFORMANCE_SQLITE_HPP
