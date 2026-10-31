#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "raft/transport.h"

namespace raftkv {

class GrpcTransport final : public Transport {
 public:
  // `rpc_deadline` bounds every AppendEntries, RequestVote and PreVote call (snapshot chunks have
  // their own, longer deadline). A call that misses it is dropped without a callback; Raft simply
  // resends. Tests that are about something else than deadlines pass a generous one, so that a
  // slow machine cannot make them lose their only call.
  static constexpr std::chrono::milliseconds kDefaultRpcDeadline{150};
  GrpcTransport(std::string listen_address,
                std::map<NodeId, std::string> peer_addresses,
                std::chrono::milliseconds rpc_deadline = kDefaultRpcDeadline);
  ~GrpcTransport() override;

  GrpcTransport(const GrpcTransport&) = delete;
  GrpcTransport& operator=(const GrpcTransport&) = delete;

  void SetPeerAddress(NodeId peer, std::string address);
  int ListeningPort() const;

  void SendRequestVote(
      NodeId to, const RequestVoteRequest& req,
      std::function<void(const RequestVoteReply&)> on_reply) override;
  void SendAppendEntries(
      NodeId to, const AppendEntriesRequest& req,
      std::function<void(const AppendEntriesReply&)> on_reply) override;
  // PreVote is carried over gRPC, so nodes run PreVote rounds before real elections.
  bool SupportsPreVote() const override { return true; }
  void SendPreVote(
      NodeId to, const PreVoteRequest& req,
      std::function<void(const PreVoteReply&)> on_reply) override;
  // Sends one snapshot chunk. Raft resends the current chunk on every heartbeat until it is
  // acknowledged, so at most ONE snapshot RPC per peer is in flight: while one is outstanding,
  // further sends to that peer are dropped (Raft treats silence as a failed send and retries).
  // Without this, a peer that never answers would fill the worker pool with duplicate snapshot
  // RPCs and starve heartbeats to the healthy peers.
  void SendInstallSnapshot(
      NodeId to, const InstallSnapshotRequest& req,
      std::function<void(const InstallSnapshotReply&)> on_reply) override;
  // Non-owning: the handler must remain alive until Stop() has completed.
  // Replacing or clearing it while running must be synchronized with requests.
  void SetHandler(RpcHandler* handler) override;
  void Start() override;
  // A callback may call Stop(). That worker cannot join itself; its internal
  // state remains alive until it exits. Call Stop() off-worker to wait for all
  // workers to drain.
  void Stop() override;

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace raftkv
