// raftkv_node: runs one node of a RaftKV cluster until it receives SIGINT or SIGTERM.
#include <pthread.h>
#include <signal.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "node/node_runtime.h"

namespace {

void Usage(FILE* out) {
  std::fputs(
      "usage: raftkv_node --id N --peers 1=HOST:PORT,2=HOST:PORT,... --client-addr HOST:PORT\n"
      "                   --data-dir DIR [options]\n"
      "\n"
      "  --id N               this node's id (>= 1); it must appear in --peers\n"
      "  --peers LIST         the Raft (node-to-node) address of EVERY node, this one included;\n"
      "                       this node listens on its own entry\n"
      "  --raft-addr ADDR     listen address for other nodes (default: this node's --peers entry)\n"
      "  --client-addr ADDR   listen address for clients (required)\n"
      "  --data-dir DIR       where the Raft log and the storage engine live (required)\n"
      "  --join               start as a NEW node that is not yet a member; it waits until the\n"
      "                       leader adds it (raftkv_cli add-node). --peers lists the current\n"
      "                       members and this node\n"
      "  --no-sync            do not fsync (fast, NOT crash safe: testing and benchmarks only)\n"
      "  --sync-engine-wal    also fsync the engine's own WAL on every write (slower; the Raft log\n"
      "                       is already durable, so the engine can be rebuilt from it)\n"
      "  --no-sstables        use the old WAL + memtable engine instead of the LSM engine\n"
      "  --memtable-bytes N   flush a memtable to an SSTable at this size (default 4194304)\n"
      "  --compaction-trigger N  merge SSTables when N are live; 0 = never (default 4)\n"
      "  --snapshot-threshold N  snapshot the data and drop the Raft log every N applied entries;\n"
      "                       0 = never (default 10000)\n"
      "  --snapshot-trailing N  Raft log entries kept below each snapshot (default 100)\n"
      "  --no-prevote         disable PreVote\n"
      "  --heartbeat-ms N     leader heartbeat interval (default 50)\n"
      "  --election-ms MIN,MAX  election timeout range (default 150,300)\n"
      "  --help\n",
      out);
}

bool ParsePeers(const std::string& list, std::map<raftkv::NodeId, std::string>* out) {
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

}  // namespace

int main(int argc, char** argv) {
  raftkv::NodeOptions options;
  std::map<raftkv::NodeId, std::string> peers;
  std::string raft_addr;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto value = [&](const char* name) -> const char* {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "raftkv_node: %s needs a value\n", name);
        std::exit(2);
      }
      return argv[++i];
    };
    if (arg == "--help" || arg == "-h") {
      Usage(stdout);
      return 0;
    } else if (arg == "--id") {
      options.id = static_cast<raftkv::NodeId>(std::strtoul(value("--id"), nullptr, 10));
    } else if (arg == "--peers") {
      if (!ParsePeers(value("--peers"), &peers)) {
        std::fprintf(stderr, "raftkv_node: --peers must look like 1=host:port,2=host:port\n");
        return 2;
      }
    } else if (arg == "--raft-addr") {
      raft_addr = value("--raft-addr");
    } else if (arg == "--client-addr") {
      options.client_listen = value("--client-addr");
    } else if (arg == "--data-dir") {
      options.data_dir = value("--data-dir");
    } else if (arg == "--join") {
      options.join = true;
    } else if (arg == "--no-sync") {
      options.sync = false;
    } else if (arg == "--memtable-bytes") {
      options.memtable_bytes = static_cast<size_t>(std::strtoull(value("--memtable-bytes"), nullptr, 10));
    } else if (arg == "--compaction-trigger") {
      options.compaction_trigger = static_cast<size_t>(std::strtoull(value("--compaction-trigger"), nullptr, 10));
    } else if (arg == "--snapshot-threshold") {
      options.snapshot_threshold_entries = static_cast<size_t>(std::strtoull(value("--snapshot-threshold"), nullptr, 10));
    } else if (arg == "--snapshot-trailing") {
      options.snapshot_trailing_entries = static_cast<size_t>(std::strtoull(value("--snapshot-trailing"), nullptr, 10));
    } else if (arg == "--sync-engine-wal") {
      options.sync_engine_wal = true;
    } else if (arg == "--no-sstables") {
      options.use_sstables = false;
    } else if (arg == "--no-prevote") {
      options.raft.enable_pre_vote = false;
    } else if (arg == "--heartbeat-ms") {
      options.raft.heartbeat_interval_ms = std::atoi(value("--heartbeat-ms"));
    } else if (arg == "--election-ms") {
      int lo = 0, hi = 0;
      if (std::sscanf(value("--election-ms"), "%d,%d", &lo, &hi) != 2 || lo <= 0 || hi < lo) {
        std::fprintf(stderr, "raftkv_node: --election-ms must look like 150,300\n");
        return 2;
      }
      options.raft.election_timeout_min_ms = lo;
      options.raft.election_timeout_max_ms = hi;
    } else {
      std::fprintf(stderr, "raftkv_node: unknown argument %s\n", arg.c_str());
      Usage(stderr);
      return 2;
    }
  }
  if (options.id == 0 || peers.empty() || options.client_listen == "127.0.0.1:0" ||
      options.data_dir.empty() || peers.count(options.id) == 0) {
    std::fputs("raftkv_node: --id, --peers (containing this id), --client-addr and --data-dir are required\n",
               stderr);
    Usage(stderr);
    return 2;
  }
  for (const auto& p : peers) options.cluster.push_back(p.first);
  options.raft_listen = raft_addr.empty() ? peers[options.id] : raft_addr;
  options.peers = peers;
  options.peers.erase(options.id);

  // Block the shutdown signals before any thread exists so that all threads inherit the mask and
  // only this thread's sigwait() receives them.
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  try {
    raftkv::NodeRuntime node(options);
    node.Start();
    std::printf("raftkv_node id=%u raft=%s client=%s data=%s READY\n",
                static_cast<unsigned>(options.id), options.raft_listen.c_str(),
                options.client_listen.c_str(), options.data_dir.c_str());
    std::fflush(stdout);
    int sig = 0;
    sigwait(&signals, &sig);
    std::printf("raftkv_node id=%u stopping (signal %d)\n", static_cast<unsigned>(options.id), sig);
    std::fflush(stdout);
    node.Stop();
  } catch (const std::exception& e) {
    std::fprintf(stderr, "raftkv_node: %s\n", e.what());
    return 1;
  }
  return 0;
}
