#include "utils/benchmark.hpp"

#include <chrono>
#include <string>
#include <iostream>

namespace duckdb {

ScopedTimer::ScopedTimer(const std::string &name) : name(name) {
	start = std::chrono::high_resolution_clock::now();
	last = start;
}
void ScopedTimer::print(const std::string msg, bool all) {
	auto end = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - (all ? start : last)).count();
	std::cout << name;
	if (!msg.empty()) {
		std::cout << " - " << msg;
	}
	std::cout << ": " << duration << "us" << std::endl;
	last = end;
}
}