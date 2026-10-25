#pragma once
#include <functional>
#include "raft/raft_interface.h"
#include "raft/rpc_messages.h"

namespace raftkv {

// Implemented by the Raft node. The transport calls these when a request
// arrives from another node. May be called from any thread.
class RpcHandler {
 public:
  virtual ~RpcHandler() = default;
  virtual RequestVoteReply HandleRequestVote(const RequestVoteRequest& req) = 0;
  virtual AppendEntriesReply HandleAppendEntries(const AppendEntriesRequest& req) = 0;
  virtual InstallSnapshotReply HandleInstallSnapshot(const InstallSnapshotRequest& req) = 0;
};

// Contract for node-to-node messaging.
// Two implementations: FakeTransport (in-process, deterministic, used for Raft tests,
// owned by Aarya) and GrpcTransport (real network, owned by Aditya K.).
// Sends are asynchronous. on_reply is called at most once, possibly from another thread.
// If the target is down, unreachable or times out, on_reply is NEVER called
// (Raft treats silence as failure and retries on its own timers).
class Transport {
 public:
  virtual ~Transport() = default;

  virtual void SendRequestVote(
      NodeId to, const RequestVoteRequest& req,
      std::function<void(const RequestVoteReply&)> on_reply) = 0;

  virtual void SendAppendEntries(
      NodeId to, const AppendEntriesRequest& req,
      std::function<void(const AppendEntriesReply&)> on_reply) = 0;

  // Sends one chunk of a snapshot. The default drops it (no reply), which Raft treats as a
  // failed send; a transport that supports snapshots overrides it.
  virtual void SendInstallSnapshot(NodeId, const InstallSnapshotRequest&,
                                   std::function<void(const InstallSnapshotReply&)>) {}

  // Register the node that handles incoming requests. Call before Start().
  virtual void SetHandler(RpcHandler* handler) = 0;
  virtual void Start() = 0;
  virtual void Stop() = 0;
};

}  // namespace raftkv
