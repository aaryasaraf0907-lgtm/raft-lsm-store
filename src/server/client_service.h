#pragma once

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

 private:
  static constexpr uint32_t kMaxRequestTimeoutMs = 30000;

  static bool IsValidKey(const std::string& key);
  static bool IsValidTimeout(uint32_t timeout_ms);
  void SetCommitResponse(const CommitResponse& commit,
                         network::proto::WriteResponse* response);
  static void SetCommitResponse(const CommitResponse& commit,
                                network::proto::GetResponse* response);

  RaftNode* node_;
  KVStore* store_;
  KVStateMachine* state_machine_;
};

}  // namespace raftkv
