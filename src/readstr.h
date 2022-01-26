#ifndef SQLITE_PERFORMANCE_READSTR_H
#define SQLITE_PERFORMANCE_READSTR_H

#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>

std::string readstr(const std::string &path) {
  std::ifstream file(path);
  if (!file) {
    throw std::runtime_error("could not open file " + path);
  }
  std::ostringstream stream;
  stream << file.rdbuf();
  return stream.str();
}

#endif // SQLITE_PERFORMANCE_READSTR_H
