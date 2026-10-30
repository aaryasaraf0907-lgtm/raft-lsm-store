#pragma once

#include <functional>

#include "raft/raft_interface.h"
#include "server/kv_state_machine.h"
#include "storage/kv_store.h"
#include "client_rpc.grpc.pb.h"

namespace raftkv {

// Client API handlers; the Raft node, store, and state machine are non-owning and must outlive
// this service.
class ClientService final : public network::proto::ClientRpc::Service {
 public:
  ClientService(RaftNode* node, KVStore* store, KVStateMachine* state_machine);

  grpc::Status Put(grpc::ServerContext* context,
                   const network::proto::PutRequest* request,
                   network::proto::WriteResponse* response) override;
  grpc::Status Get(grpc::ServerContext* context,
                   const network::proto::GetRequest* request,
                   network::proto::GetResponse* response) override;
  grpc::Status Delete(grpc::ServerContext* context,
                      const network::proto::DeleteRequest* request,
                      network::proto::WriteResponse* response) override;
  grpc::Status NodeStatus(grpc::ServerContext* context,
                          const network::proto::StatusRequest* request,
                          network::proto::StatusResponse* response) override;

  // Fills in the node-specific part of a Status reply. Without a provider, NodeStatus returns
  // UNIMPLEMENTED. The provider may be called from any gRPC thread.
  using StatusProvider = std::function<void(network::proto::StatusResponse*)>;
  void SetStatusProvider(StatusProvider provider) { status_provider_ = std::move(provider); }

  // Membership changes are handled by whoever owns the Raft node (NodeRuntime); without handlers
  // the RPCs answer MEMBERSHIP_UNSUPPORTED.
  using AddServerHandler = std::function<void(const network::proto::AddServerRequest&, network::proto::MembershipReply*)>;
  using RemoveServerHandler =
      std::function<void(const network::proto::RemoveServerRequest&, network::proto::MembershipReply*)>;
  void SetMembershipHandlers(AddServerHandler add, RemoveServerHandler remove) {
    add_server_ = std::move(add);
    remove_server_ = std::move(remove);
  }
  grpc::Status AddServer(grpc::ServerContext* /*context*/, const network::proto::AddServerRequest* request,
                         network::proto::MembershipReply* reply) override {
    if (!add_server_) return Unsupported(reply);
    add_server_(*request, reply);
    return grpc::Status::OK;
  }
  grpc::Status RemoveServer(grpc::ServerContext* /*context*/, const network::proto::RemoveServerRequest* request,
                            network::proto::MembershipReply* reply) override {
    if (!remove_server_) return Unsupported(reply);
    remove_server_(*request, reply);
    return grpc::Status::OK;
  }

 private:
 public:
  // Request size limits. They are enforced BEFORE anything is proposed: a committed entry that the
  // storage engine cannot apply would abort every replica, and entries must also fit in one
  // gRPC message (4 MiB by default) when replicated. See the static_assert in client_service.cpp.
  static constexpr size_t kMaxKeyBytes = 4096;
  static constexpr size_t kMaxValueBytes = 1024 * 1024;

 private:
  static constexpr uint32_t kMaxRequestTimeoutMs = 30000;

  static bool IsValidKey(const std::string& key);
  static bool IsValidValue(const std::string& value);
  static bool IsValidTimeout(uint32_t timeout_ms);
  void SetCommitResponse(const CommitResponse& commit, uint64_t client_id, uint64_t seq,
                         network::proto::WriteResponse* response);
  static void SetCommitResponse(const CommitResponse& commit,
                                network::proto::GetResponse* response);

  RaftNode* node_;
  KVStore* store_;
  KVStateMachine* state_machine_;
  StatusProvider status_provider_;
  AddServerHandler add_server_;
  RemoveServerHandler remove_server_;
  static grpc::Status Unsupported(network::proto::MembershipReply* reply) {
    reply->set_status(network::proto::MEMBERSHIP_UNSUPPORTED);
    reply->set_error_message("membership changes are not available on this server");
    return grpc::Status::OK;
  }
};

}  // namespace raftkv
