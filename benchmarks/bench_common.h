// Helpers shared by the storage benchmarks (storage_read_bench.cpp, storage_write_bench.cpp).
// Header-only; not part of the library.
#pragma once

#include <sys/utsname.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#include "storage/kv_engine.h"

namespace raftkv {
namespace bench {

// The data set: keys are "k" + 15 digits (16 bytes), values are `size` bytes derived from the key.
inline std::string Key(uint64_t n) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "k%015llu", static_cast<unsigned long long>(n));
  return buf;
}

inline std::string Value(uint64_t n, size_t size = 100) {
  std::string v = Key(n);
  v.resize(size, static_cast<char>('a' + n % 26));
  return v;
}

[[noreturn]] inline void Die(const char* program, const char* what) {
  std::fprintf(stderr, "%s: %s\n", program, what);
  std::exit(1);
}

// Median, minimum and maximum of a set of measurements.
struct Spread {
  double median = 0, min = 0, max = 0;
};

inline Spread Summarize(std::vector<double> v) {
  std::sort(v.begin(), v.end());
  Spread s;
  s.min = v.front();
  s.max = v.back();
  const size_t n = v.size();
  s.median = n % 2 == 1 ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2;
  return s;
}

// Polls until every frozen memtable is flushed and no compaction is waiting to run (fewer than
// `compaction_trigger` live SSTables; trigger 0 or 1 means compaction is off). Returns false if the
// engine reports a failing flush.
inline bool WaitUntilIdle(KVEngine& engine, size_t compaction_trigger) {
  for (;;) {
    const KVEngine::Stats s = engine.GetStats();
    if (s.flush_failing || s.fail_stopped) return false;
    const bool compaction_pending = compaction_trigger >= 2 && s.sstables >= compaction_trigger;
    if (s.frozen_memtables == 0 && !compaction_pending) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

inline std::string CpuModel() {
#ifdef __APPLE__
  char buf[256];
  size_t len = sizeof(buf);
  if (sysctlbyname("machdep.cpu.brand_string", buf, &len, nullptr, 0) == 0) return buf;
#else
  std::ifstream in("/proc/cpuinfo");
  std::string line;
  while (std::getline(in, line)) {
    if (line.rfind("model name", 0) == 0) {
      const size_t colon = line.find(':');
      if (colon != std::string::npos && colon + 2 <= line.size()) return line.substr(colon + 2);
    }
  }
#endif
  return "unknown";
}

// One block of lines describing the environment, so that every result carries its own context.
// RAFTKV_BENCH_BUILD_TYPE is set by CMake to the build configuration.
inline void PrintEnvironment(const char* program) {
  struct utsname u;
  const bool have_uname = ::uname(&u) == 0;
#ifdef RAFTKV_BENCH_BUILD_TYPE
  const char* build_type = RAFTKV_BENCH_BUILD_TYPE;
#else
  const char* build_type = "";
#endif
#ifdef NDEBUG
  const bool optimized = true;
#else
  const bool optimized = false;
#endif
  std::printf("# %s\n", program);
  std::printf("# cpu: %s, %u hardware threads\n", CpuModel().c_str(), std::thread::hardware_concurrency());
  std::printf("# os: %s %s\n", have_uname ? u.sysname : "unknown", have_uname ? u.release : "");
#ifdef __VERSION__
  std::printf("# compiler: %s\n", __VERSION__);
#endif
  std::printf("# build type: %s (%s)\n", build_type[0] != '\0' ? build_type : "unspecified",
              optimized ? "NDEBUG defined" : "NDEBUG NOT defined: not an optimized build, do not compare");
}

}  // namespace bench
}  // namespace raftkv
