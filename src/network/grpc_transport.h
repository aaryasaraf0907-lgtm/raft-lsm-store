#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>

#include "raft/transport.h"

namespace raftkv {

class GrpcTransport final : public Transport {
 public:
  GrpcTransport(std::string listen_address,
                std::map<NodeId, std::string> peer_addresses);
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
