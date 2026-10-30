#pragma once
// A client for a RaftKV cluster. It finds the leader, follows redirects, and retries safely:
// every write gets one request number (client_id, seq) and ALL its retries reuse it, so a write
// that timed out but did take effect is applied exactly once. One KvClient must be used by one
// thread at a time (give each thread its own client; client ids are random and distinct).
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "client_rpc.grpc.pb.h"
#include "raft/raft_interface.h"

namespace raftkv {

struct KvClientOptions {
  std::map<NodeId, std::string> servers;  // the CLIENT address of each node
  uint64_t client_id = 0;                 // 0 = pick a random non-zero id
  std::chrono::milliseconds op_timeout{10000};  // how long one Put/Get/Delete keeps trying
  std::chrono::milliseconds rpc_timeout{2000};  // deadline of each individual attempt
};

enum class KvStatus {
  kOk,
  kNotFound,         // Get: the key does not exist
  kInvalidArgument,  // rejected (empty or oversize key, oversize value); retrying cannot help
  kStaleRequest,     // a newer request from this client was already applied
  kUnavailable,      // no node answered usefully before op_timeout
  kOutcomeUnknown,   // the session expired after an attempt that may have been applied: the write
                     // may or may not have happened (later requests use a new session)
};

struct KvResult {
  KvStatus status = KvStatus::kUnavailable;
  std::string value;    // Get
  std::string message;  // the last error, if any
  int attempts = 0;
};

class KvClient {
 public:
  explicit KvClient(KvClientOptions options);

  KvResult Put(const std::string& key, const std::string& value);
  KvResult Delete(const std::string& key);
  KvResult Get(const std::string& key);  // linearizable (the leader confirms with a majority)

  // Asks one specific node about itself. False (and *error set) if it cannot be reached.
  bool NodeStatus(NodeId id, network::proto::StatusResponse* out, std::string* error);

  // Membership changes, sent to the leader (leader hints are followed). Each keeps trying for
  // `timeout`, which must cover the new node's catch-up for AddServer. kOk: committed.
  // kOutcomeUnknown: not committed in time, it may still take effect. kInvalidArgument: bad id or
  // address, or removing the last member. kUnavailable: see the message (for AddServer often: the
  // new node is not running with --join, or not reachable at that address).
  KvResult AddServer(NodeId id, const std::string& raft_address, std::chrono::milliseconds timeout);
  KvResult RemoveServer(NodeId id, std::chrono::milliseconds timeout);

  // Points `id` at a new address (a node restarted on another port).
  void SetServer(NodeId id, const std::string& address);

  uint64_t client_id() const { return client_id_; }
  const std::vector<NodeId>& node_ids() const { return ids_; }

 private:
  using Stub = network::proto::ClientRpc::Stub;
  KvResult Write(bool is_put, const std::string& key, const std::string& value);
  KvResult ChangeMembership(bool add, NodeId id, const std::string& raft_address, std::chrono::milliseconds timeout);
  void Rotate();
  void Prefer(NodeId hint);
  NodeId Current() const { return ids_[current_]; }

  KvClientOptions options_;
  // Starts a new session: a fresh random client id, numbering requests from 1 again.
  void RenewSession();
  uint64_t client_id_;
  uint64_t seq_ = 0;
  std::vector<NodeId> ids_;  // sorted
  std::map<NodeId, std::shared_ptr<Stub>> stubs_;
  size_t current_ = 0;
};

// Hex helpers for binary keys and values on the command line.
std::string HexEncode(const std::string& bytes);
bool HexDecode(const std::string& hex, std::string* out);

}  // namespace raftkv
