// raftkv_cli: command-line client for a RaftKV cluster.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cli/kv_client.h"

namespace {

using raftkv::KvClient;
using raftkv::KvResult;
using raftkv::KvStatus;

void Usage(FILE* out) {
  std::fputs(
      "usage: raftkv_cli --servers 1=HOST:PORT,2=HOST:PORT,... [options] COMMAND [ARGS]\n"
      "\n"
      "  --servers LIST     the CLIENT address of each node (required)\n"
      "  --timeout-ms N     how long one operation keeps retrying (default 10000)\n"
      "  --client-id N      fix the client id (default: random)\n"
      "  --hex              KEY and VALUE arguments (and Get output) are hex-encoded bytes\n"
      "\n"
      "commands:\n"
      "  put KEY VALUE      store a value                          prints OK\n"
      "  get KEY            read a value (linearizable)            prints the value\n"
      "  delete KEY         remove a key                           prints OK\n"
      "  status             role, term and log positions of every node\n"
      "  bench put|get COUNT [VALUE_BYTES] [THREADS]\n"
      "                     measure throughput and latency\n"
      "  repl               read commands from standard input (put/get/delete/status/quit)\n"
      "\n"
      "exit status: 0 success, 1 failed or unavailable, 2 usage error, 3 key not found\n",
      out);
}

bool ParseServers(const std::string& list, std::map<raftkv::NodeId, std::string>* out) {
  std::stringstream ss(list);
  std::string item;
  while (std::getline(ss, item, ',')) {
    const size_t eq = item.find('=');
    if (eq == std::string::npos || eq == 0 || eq + 1 == item.size()) return false;
    char* end = nullptr;
    const unsigned long id = std::strtoul(item.substr(0, eq).c_str(), &end, 10);
    if (*end != '\0' || id == 0) return false;
    (*out)[static_cast<raftkv::NodeId>(id)] = item.substr(eq + 1);
  }
  return !out->empty();
}

struct Context {
  KvClient* client;
  bool hex;
  bool Bytes(const std::string& arg, std::string* out) const {
    if (!hex) {
      *out = arg;
      return true;
    }
    return raftkv::HexDecode(arg, out);
  }
};

int PrintFailure(const KvResult& r, const char* what) {
  switch (r.status) {
    case KvStatus::kNotFound:
      std::fputs("(not found)\n", stderr);
      return 3;
    case KvStatus::kInvalidArgument:
      std::fprintf(stderr, "%s rejected: %s\n", what, r.message.c_str());
      return 1;
    case KvStatus::kStaleRequest:
      std::fprintf(stderr, "%s: %s\n", what, r.message.c_str());
      return 1;
    default:
      std::fprintf(stderr, "%s failed: %s (after %d attempts)\n", what, r.message.c_str(), r.attempts);
      return 1;
  }
}

int PrintStatus(KvClient* client) {
  int unreachable = 0;
  for (raftkv::NodeId id : client->node_ids()) {
    raftkv::network::proto::StatusResponse s;
    std::string error;
    if (!client->NodeStatus(id, &s, &error)) {
      std::printf("node %u  unreachable (%s)\n", static_cast<unsigned>(id), error.c_str());
      ++unreachable;
      continue;
    }
    std::printf("node %u  %-9s term %llu  leader %u  commit %llu  applied %llu  log %llu  snapshot %llu\n",
                s.node_id(), s.role().c_str(), static_cast<unsigned long long>(s.term()), s.leader_id_hint(),
                static_cast<unsigned long long>(s.commit_index()), static_cast<unsigned long long>(s.last_applied()),
                static_cast<unsigned long long>(s.log_length()), static_cast<unsigned long long>(s.snapshot_index()));
  }
  return unreachable == static_cast<int>(client->node_ids().size()) ? 1 : 0;
}

// Runs one command given as words; returns the exit status.
int Run(const Context& ctx, const std::vector<std::string>& words) {
  const std::string& cmd = words[0];
  if (cmd == "put" && words.size() == 3) {
    std::string k, v;
    if (!ctx.Bytes(words[1], &k) || !ctx.Bytes(words[2], &v)) {
      std::fputs("invalid hex\n", stderr);
      return 2;
    }
    KvResult r = ctx.client->Put(k, v);
    if (r.status != KvStatus::kOk) return PrintFailure(r, "put");
    std::puts("OK");
    return 0;
  }
  if (cmd == "get" && words.size() == 2) {
    std::string k;
    if (!ctx.Bytes(words[1], &k)) {
      std::fputs("invalid hex\n", stderr);
      return 2;
    }
    KvResult r = ctx.client->Get(k);
    if (r.status != KvStatus::kOk) return PrintFailure(r, "get");
    if (ctx.hex) {
      std::puts(raftkv::HexEncode(r.value).c_str());
    } else {
      std::fwrite(r.value.data(), 1, r.value.size(), stdout);
      std::fputc('\n', stdout);
    }
    return 0;
  }
  if (cmd == "delete" && words.size() == 2) {
    std::string k;
    if (!ctx.Bytes(words[1], &k)) {
      std::fputs("invalid hex\n", stderr);
      return 2;
    }
    KvResult r = ctx.client->Delete(k);
    if (r.status != KvStatus::kOk) return PrintFailure(r, "delete");
    std::puts("OK");
    return 0;
  }
  if (cmd == "status" && words.size() == 1) return PrintStatus(ctx.client);
  std::fprintf(stderr, "bad command or arguments: %s (try --help)\n", cmd.c_str());
  return 2;
}

// Splits a line into words; double quotes group words that contain spaces.
std::vector<std::string> Split(const std::string& line) {
  std::vector<std::string> words;
  std::string cur;
  bool in_quotes = false, have = false;
  for (char c : line) {
    if (c == '"') {
      in_quotes = !in_quotes;
      have = true;
    } else if (!in_quotes && (c == ' ' || c == '\t')) {
      if (have) words.push_back(cur);
      cur.clear();
      have = false;
    } else {
      cur.push_back(c);
      have = true;
    }
  }
  if (have) words.push_back(cur);
  return words;
}

int Repl(const Context& ctx) {
  std::string line;
  std::fputs("raftkv> ", stdout);
  std::fflush(stdout);
  while (std::getline(std::cin, line)) {
    std::vector<std::string> words = Split(line);
    if (!words.empty()) {
      if (words[0] == "quit" || words[0] == "exit") break;
      if (words[0] == "help") {
        std::puts("put KEY VALUE | get KEY | delete KEY | status | quit");
      } else {
        Run(ctx, words);
      }
    }
    std::fputs("raftkv> ", stdout);
    std::fflush(stdout);
  }
  return 0;
}

int Bench(const raftkv::KvClientOptions& base, const std::vector<std::string>& words) {
  if (words.size() < 3 || (words[1] != "put" && words[1] != "get")) {
    std::fputs("usage: bench put|get COUNT [VALUE_BYTES] [THREADS]\n", stderr);
    return 2;
  }
  const bool put = words[1] == "put";
  const long total = std::atol(words[2].c_str());
  const size_t value_bytes = words.size() > 3 ? static_cast<size_t>(std::atol(words[3].c_str())) : 100;
  const int threads = words.size() > 4 ? std::max(1, std::atoi(words[4].c_str())) : 1;
  if (total <= 0) {
    std::fputs("COUNT must be positive\n", stderr);
    return 2;
  }
  const std::string value(value_bytes, 'x');
  std::mutex mu;
  std::vector<double> latencies_ms;
  std::atomic<long> failed{0};
  std::atomic<long> next{0};
  auto worker = [&](int t) {
    raftkv::KvClientOptions options = base;
    options.client_id = 0;  // every thread is its own client
    KvClient client(options);
    std::vector<double> mine;
    if (!put) client.Put("bench-" + std::to_string(t), value);  // make sure there is something to read
    for (;;) {
      const long i = next.fetch_add(1);
      if (i >= total) break;
      const auto t0 = std::chrono::steady_clock::now();
      KvResult r = put ? client.Put("bench-" + std::to_string(t) + "-" + std::to_string(i), value)
                       : client.Get("bench-" + std::to_string(t));
      const double ms =
          std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
      if (r.status == KvStatus::kOk) {
        mine.push_back(ms);
      } else {
        ++failed;
      }
    }
    std::lock_guard<std::mutex> lk(mu);
    latencies_ms.insert(latencies_ms.end(), mine.begin(), mine.end());
  };
  const auto start = std::chrono::steady_clock::now();
  std::vector<std::thread> pool;
  for (int t = 0; t < threads; ++t) pool.emplace_back(worker, t);
  for (auto& th : pool) th.join();
  const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  std::sort(latencies_ms.begin(), latencies_ms.end());
  auto pct = [&](double p) {
    return latencies_ms.empty() ? 0.0 : latencies_ms[std::min(latencies_ms.size() - 1, static_cast<size_t>(p * latencies_ms.size()))];
  };
  std::printf("%s: %zu ok, %ld failed, %d thread(s), %zu-byte values\n", put ? "put" : "get", latencies_ms.size(),
              failed.load(), threads, value_bytes);
  std::printf("throughput %.1f ops/s   latency ms: p50 %.2f  p95 %.2f  p99 %.2f  max %.2f\n",
              latencies_ms.size() / seconds, pct(0.50), pct(0.95), pct(0.99),
              latencies_ms.empty() ? 0.0 : latencies_ms.back());
  return failed.load() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  raftkv::KvClientOptions options;
  bool hex = false;
  std::vector<std::string> rest;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto value = [&]() -> const char* {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "raftkv_cli: %s needs a value\n", arg.c_str());
        std::exit(2);
      }
      return argv[++i];
    };
    if (!rest.empty()) {  // everything after the command belongs to the command
      rest.push_back(arg);
    } else if (arg == "--help" || arg == "-h") {
      Usage(stdout);
      return 0;
    } else if (arg == "--servers") {
      if (!ParseServers(value(), &options.servers)) {
        std::fputs("raftkv_cli: --servers must look like 1=host:port,2=host:port\n", stderr);
        return 2;
      }
    } else if (arg == "--timeout-ms") {
      options.op_timeout = std::chrono::milliseconds(std::atol(value()));
    } else if (arg == "--client-id") {
      options.client_id = std::strtoull(value(), nullptr, 10);
    } else if (arg == "--hex") {
      hex = true;
    } else if (arg.rfind("--", 0) == 0) {
      std::fprintf(stderr, "raftkv_cli: unknown option %s\n", arg.c_str());
      Usage(stderr);
      return 2;
    } else {
      rest.push_back(arg);
    }
  }
  if (options.servers.empty() || rest.empty()) {
    Usage(stderr);
    return 2;
  }
  try {
    if (rest[0] == "bench") return Bench(options, rest);
    KvClient client(options);
    Context ctx{&client, hex};
    if (rest[0] == "repl") return Repl(ctx);
    return Run(ctx, rest);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "raftkv_cli: %s\n", e.what());
    return 1;
  }
}
