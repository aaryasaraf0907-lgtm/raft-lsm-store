#pragma once
// One RaftKV node: durable Raft log, storage engine, state machine, the Raft node itself, the
// node-to-node gRPC server and the client-facing gRPC server, wired together. raftkv_node is a thin
// command-line wrapper around this; tests start several in one process.
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "network/grpc_transport.h"
#include "raft/file_raft_storage.h"
#include "raft/raft_node.h"
#include "server/client_service.h"
#include "server/kv_state_machine.h"
#include "storage/kv_engine.h"

namespace raftkv {

struct NodeOptions {
  NodeId id = 0;
  std::vector<NodeId> cluster;  // every node id of the cluster, including this one
  std::string raft_listen = "127.0.0.1:0";    // for the other nodes (port 0 picks a free port)
  std::string client_listen = "127.0.0.1:0";  // for clients
  // Raft addresses of the other nodes. May be incomplete here and filled in with SetPeerAddress.
  std::map<NodeId, std::string> peers;
  std::string data_dir;  // holds "raft/" (log, snapshots) and "kv/" (storage engine)
  bool sync = true;      // fsync every write (turn off only for tests and benchmarks)
  // Start as a new node that is not yet a member: it waits until the leader adds it (AddServer).
  // `cluster`/`peers` should still list the current members' Raft addresses and this node's own.
  bool join = false;
  // Also fsync the storage engine's own WAL on every write. Off by default: the Raft log is already
  // durable, so the engine is rebuilt from it (or from a Raft snapshot) after a crash, and skipping
  // this fsync removes one of the three that every write used to wait for. A damaged engine WAL is
  // then truncated rather than refusing to start.
  bool sync_engine_wal = false;
  // Use the real LSM engine (manifest, SSTables, compaction). Off = the old WAL + memtable engine.
  bool use_sstables = true;
  size_t memtable_bytes = 4 * 1024 * 1024;  // the engine freezes and flushes a memtable at this size
  size_t compaction_trigger = 4;            // merge SSTables when this many are live (0 = never)
  // Raft snapshots (LSM engine only): after this many applied entries the node snapshots its data
  // and drops the Raft log, keeping `snapshot_trailing_entries` of it for followers that are only
  // a little behind. 0 = never snapshot. The node can always RECEIVE a snapshot from the leader.
  size_t snapshot_threshold_entries = 10000;
  size_t snapshot_trailing_entries = 100;
  RaftConfig raft;
};

class NodeRuntime {
 public:
  explicit NodeRuntime(NodeOptions options);
  ~NodeRuntime();
  NodeRuntime(const NodeRuntime&) = delete;
  NodeRuntime& operator=(const NodeRuntime&) = delete;

  // Opens the on-disk state (recovering from a crash if needed), starts both gRPC servers and the
  // Raft node. Throws std::invalid_argument for bad options and std::runtime_error if the storage
  // cannot be opened or a port cannot be bound.
  void Start();
  // Stops serving and shuts the node down; the on-disk state stays for the next node started with
  // the same data_dir. Idempotent.
  void Stop();

  int raft_port() const { return raft_port_; }
  int client_port() const { return client_port_; }
  void SetPeerAddress(NodeId peer, const std::string& address);

  RaftNodeImpl* node() { return node_.get(); }
  KVStateMachine* state_machine() { return machine_.get(); }
  KVStore* store() { return engine_.get(); }

 private:
  NodeOptions options_;
  bool started_ = false;
  int raft_port_ = 0;
  int client_port_ = 0;
  // Declaration order is destruction order in reverse: the Raft node uses the transport and the
  // storage, the client service uses the node, the engine and the state machine.
  std::unique_ptr<FileRaftStorage> storage_;
  std::unique_ptr<KVEngine> engine_;
  std::unique_ptr<KVStateMachine> machine_;
  std::unique_ptr<GrpcTransport> transport_;
  std::unique_ptr<RaftNodeImpl> node_;
  std::unique_ptr<ClientService> service_;
  std::unique_ptr<grpc::Server> client_server_;
};

}  // namespace raftkv
