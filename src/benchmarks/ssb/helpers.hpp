#ifndef SQLITE_PERFORMANCE_SSB_HELPERS_HPP
#define SQLITE_PERFORMANCE_SSB_HELPERS_HPP

#include <array>
#include <chrono>
#include <cxxopts.hpp>
#include <string>

template <typename F> double time(F &&f) {
  auto t0 = std::chrono::high_resolution_clock::now();
  f();
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double>(t1 - t0).count();
}

cxxopts::Options ssb_options(const std::string &program,
                             const std::string &help_string = "") {
  cxxopts::Options options(program, help_string);
  cxxopts::OptionAdder adder = options.add_options();
  adder("help", "Print help");
  return options;
}

#endif // SQLITE_PERFORMANCE_SSB_HELPERS_HPP
