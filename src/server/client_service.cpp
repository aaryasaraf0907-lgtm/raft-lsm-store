#include "server/client_service.h"

#include <chrono>
#include <stdexcept>
#include <string>

namespace raftkv {
namespace {

using network::proto::ClientStatus;

ClientStatus ToClientStatus(CommitResult result) {
  switch (result) {
    case CommitResult::kApplied:
      return network::proto::CLIENT_STATUS_OK;
    case CommitResult::kNotLeader:
      return network::proto::CLIENT_STATUS_NOT_LEADER;
    case CommitResult::kLost:
      return network::proto::CLIENT_STATUS_NOT_COMMITTED;
    case CommitResult::kTimeout:
      return network::proto::CLIENT_STATUS_OUTCOME_UNKNOWN;
    case CommitResult::kStopped:
      return network::proto::CLIENT_STATUS_STOPPED;
  }
  return network::proto::CLIENT_STATUS_INTERNAL_ERROR;
}

const char* CommitError(CommitResult result) {
  switch (result) {
    case CommitResult::kApplied:
      return "";
    case CommitResult::kNotLeader:
      return "this node is not the leader";
    case CommitResult::kLost:
      return "the proposal was overwritten and was not committed";
    case CommitResult::kTimeout:
      return "the request timed out; its outcome is unknown";
    case CommitResult::kStopped:
      return "the Raft node stopped before the outcome could be confirmed";
  }
  return "unknown Raft commit result";
}

}  // namespace

ClientService::ClientService(RaftNode* node, KVStore* store,
                             KVStateMachine* state_machine)
    : node_(node), store_(store), state_machine_(state_machine) {
  if (node_ == nullptr || store_ == nullptr || state_machine_ == nullptr) {
    throw std::invalid_argument(
        "ClientService requires a Raft node, KV store, and state machine");
  }
}

bool ClientService::IsValidKey(const std::string& key) {
  return !key.empty();
}

bool ClientService::IsValidTimeout(uint32_t timeout_ms) {
  return timeout_ms > 0 && timeout_ms <= kMaxRequestTimeoutMs;
}

void ClientService::SetCommitResponse(
    const CommitResponse& commit, network::proto::WriteResponse* response) {
  response->set_status(ToClientStatus(commit.result));
  response->set_error_message(CommitError(commit.result));
  response->set_leader_id_hint(commit.leader_hint);
  if (commit.result != CommitResult::kApplied) return;

  switch (state_machine_->LookupRequestOutcome(commit.index)) {
    case KVStateMachine::RequestOutcome::kApplied:
      return;
    case KVStateMachine::RequestOutcome::kDuplicate:
      response->set_status(
          network::proto::CLIENT_STATUS_DUPLICATE_OUTCOME_UNKNOWN);
      response->set_error_message(
          "request sequence is duplicate or stale; its original result is unknown");
      return;
    case KVStateMachine::RequestOutcome::kUnknown:
      response->set_status(network::proto::CLIENT_STATUS_OUTCOME_UNKNOWN);
      response->set_error_message(
          "proposal applied, but this request's state-machine outcome is unavailable");
      return;
  }
}

void ClientService::SetCommitResponse(
    const CommitResponse& commit, network::proto::GetResponse* response) {
  response->set_status(ToClientStatus(commit.result));
  response->set_error_message(CommitError(commit.result));
  response->set_leader_id_hint(commit.leader_hint);
}

grpc::Status ClientService::Put(grpc::ServerContext*,
                                const network::proto::PutRequest* request,
                                network::proto::WriteResponse* response) {
  if (request->client_id() == 0 || request->seq() == 0) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("client_id and seq must both be nonzero");
    return grpc::Status::OK;
  }
  if (!IsValidKey(request->key())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("key must not be empty");
    return grpc::Status::OK;
  }
  if (!IsValidTimeout(request->timeout_ms())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("timeout_ms must be between 1 and 30000");
    return grpc::Status::OK;
  }

  Command command{CommandType::kPut, request->key(), request->value()};
  command.client_id = request->client_id();
  command.seq = request->seq();
  const CommitResponse commit = node_->ProposeAndWait(
      command, std::chrono::milliseconds(request->timeout_ms()));
  SetCommitResponse(commit, response);
  return grpc::Status::OK;
}

grpc::Status ClientService::Delete(
    grpc::ServerContext*, const network::proto::DeleteRequest* request,
    network::proto::WriteResponse* response) {
  if (request->client_id() == 0 || request->seq() == 0) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("client_id and seq must both be nonzero");
    return grpc::Status::OK;
  }
  if (!IsValidKey(request->key())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("key must not be empty");
    return grpc::Status::OK;
  }
  if (!IsValidTimeout(request->timeout_ms())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("timeout_ms must be between 1 and 30000");
    return grpc::Status::OK;
  }

  Command command{CommandType::kDelete, request->key(), ""};
  command.client_id = request->client_id();
  command.seq = request->seq();
  const CommitResponse commit = node_->ProposeAndWait(
      command, std::chrono::milliseconds(request->timeout_ms()));
  SetCommitResponse(commit, response);
  return grpc::Status::OK;
}

grpc::Status ClientService::Get(grpc::ServerContext*,
                                const network::proto::GetRequest* request,
                                network::proto::GetResponse* response) {
  if (!IsValidKey(request->key())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("key must not be empty");
    return grpc::Status::OK;
  }
  if (!IsValidTimeout(request->timeout_ms())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("timeout_ms must be between 1 and 30000");
    return grpc::Status::OK;
  }

  // A fresh current-term no-op must commit on a majority and be applied before reading.
  // IsLeader() alone is insufficient: it gives no quorum confirmation and may be stale.
  const Command barrier{CommandType::kNoop, "", ""};
  const CommitResponse commit = node_->ProposeAndWait(
      barrier, std::chrono::milliseconds(request->timeout_ms()));
  SetCommitResponse(commit, response);
  if (commit.result != CommitResult::kApplied) return grpc::Status::OK;

  std::string value;
  switch (store_->Get(request->key(), &value)) {
    case Status::kOk:
      response->set_value(std::move(value));
      response->set_status(network::proto::CLIENT_STATUS_OK);
      response->clear_error_message();
      break;
    case Status::kNotFound:
      response->set_status(network::proto::CLIENT_STATUS_NOT_FOUND);
      response->set_error_message("key was not found");
      break;
    case Status::kIOError:
    case Status::kCorruption:
      response->set_status(network::proto::CLIENT_STATUS_INTERNAL_ERROR);
      response->set_error_message("local storage read failed");
      break;
  }
  return grpc::Status::OK;
}

}  // namespace raftkv
