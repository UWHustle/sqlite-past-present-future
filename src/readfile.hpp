#ifndef SQLITE_PERFORMANCE_READFILE_HPP
#define SQLITE_PERFORMANCE_READFILE_HPP

#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>

std::string readfile(const std::string &filename) {
  std::ifstream f(filename);
  if (!f.is_open()) {
    throw std::runtime_error("could not open file " + filename);
  }
  std::stringstream s;
  s << f.rdbuf();
  return s.str();
}

#endif // SQLITE_PERFORMANCE_READFILE_HPP
