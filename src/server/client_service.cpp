#include "server/client_service.h"

#include <chrono>
#include <stdexcept>
#include <string>

#include "storage/wal.h"

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

// The largest request must fit the WAL record (type + key length + key + value) with a lot of room.
static_assert(1 + 4 + ClientService::kMaxKeyBytes + ClientService::kMaxValueBytes + 1024 <
                  WAL::kMaxPayloadBytes,
              "client size limits must stay well below the WAL record limit");

ClientService::ClientService(RaftNode* node, KVStore* store,
                             KVStateMachine* state_machine)
    : node_(node), store_(store), state_machine_(state_machine) {
  if (node_ == nullptr || store_ == nullptr || state_machine_ == nullptr) {
    throw std::invalid_argument(
        "ClientService requires a Raft node, KV store, and state machine");
  }
}

bool ClientService::IsValidKey(const std::string& key) {
  return !key.empty() && key.size() <= kMaxKeyBytes && !HasReservedKeyPrefix(key);
}

bool ClientService::IsValidValue(const std::string& value) {
  return value.size() <= kMaxValueBytes;
}

bool ClientService::IsValidTimeout(uint32_t timeout_ms) {
  return timeout_ms > 0 && timeout_ms <= kMaxRequestTimeoutMs;
}

void ClientService::SetCommitResponse(const CommitResponse& commit, uint64_t client_id,
                                      uint64_t seq, network::proto::WriteResponse* response) {
  response->set_status(ToClientStatus(commit.result));
  response->set_error_message(CommitError(commit.result));
  response->set_leader_id_hint(commit.leader_hint);
  if (commit.result != CommitResult::kApplied) return;

  switch (state_machine_->LookupRequestOutcome(commit.index)) {
    case KVStateMachine::RequestOutcome::kApplied:
      return;
    case KVStateMachine::RequestOutcome::kDuplicate:
      // A client has one request in flight and numbers them upwards, so a repeat of the request
      // with the CLIENT'S LATEST applied number is a retry whose original already took effect
      // (Put and Delete cannot fail once committed): report success, exactly once. A number
      // BELOW the latest is a stale, delayed duplicate of a request the client has long moved on
      // from; we cannot tell what its result was, so it is not reported as a success.
      if (seq == state_machine_->Sessions().LastSeq(client_id)) return;
      response->set_status(network::proto::CLIENT_STATUS_DUPLICATE_OUTCOME_UNKNOWN);
      response->set_error_message("stale request sequence: a newer request from this client was applied");
      return;
    case KVStateMachine::RequestOutcome::kSessionExpired:
      response->set_status(network::proto::CLIENT_STATUS_SESSION_EXPIRED);
      response->set_error_message(
          "client session expired: this request was not applied; start a new session (new client id)");
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

void ClientService::SetCommitResponse(
    const CommitResponse& commit, network::proto::ScanResponse* response) {
  response->set_status(ToClientStatus(commit.result));
  response->set_error_message(CommitError(commit.result));
  response->set_leader_id_hint(commit.leader_hint);
}

grpc::Status ClientService::Scan(grpc::ServerContext*, const network::proto::ScanRequest* request,
                                 network::proto::ScanResponse* response) {
  if (!scan_provider_) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("range scans are not available on this server");
    return grpc::Status::OK;
  }
  if (!IsValidTimeout(request->timeout_ms())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("timeout_ms must be between 1 and 30000");
    return grpc::Status::OK;
  }
  if (request->start().size() > 4096 || request->end().size() > 4096) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("start and end must be at most 4096 bytes");
    return grpc::Status::OK;
  }
  // Linearizable, exactly like Get: the leader confirms with a majority that it still leads and
  // waits until it has applied everything committed so far, then reads its own engine.
  const CommitResponse commit = node_->ReadIndex(std::chrono::milliseconds(request->timeout_ms()));
  SetCommitResponse(commit, response);
  if (commit.result != CommitResult::kApplied) return grpc::Status::OK;
  const size_t limit = (request->limit() == 0 || request->limit() > kMaxScanKeys) ? kMaxScanKeys : request->limit();
  std::vector<std::pair<std::string, std::string>> found;
  const Status s = scan_provider_(request->start(), request->end(), limit + 1, &found);  // +1: is there more?
  if (s != Status::kOk) {
    response->set_status(network::proto::CLIENT_STATUS_INTERNAL_ERROR);
    response->set_error_message("local storage scan failed");
    return grpc::Status::OK;
  }
  size_t bytes = 0;
  for (size_t i = 0; i < found.size() && i < limit; ++i) {
    if (i > 0 && bytes + found[i].first.size() + found[i].second.size() > kMaxScanBytes) break;  // always one
    bytes += found[i].first.size() + found[i].second.size();
    network::proto::KeyValue* kv = response->add_items();
    kv->set_key(std::move(found[i].first));
    kv->set_value(std::move(found[i].second));
  }
  response->set_more(found.size() > static_cast<size_t>(response->items_size()));
  response->set_status(network::proto::CLIENT_STATUS_OK);
  response->clear_error_message();
  return grpc::Status::OK;
}

grpc::Status ClientService::NodeStatus(grpc::ServerContext*, const network::proto::StatusRequest*,
                                       network::proto::StatusResponse* response) {
  if (!status_provider_) {
    return {grpc::StatusCode::UNIMPLEMENTED, "this node has no status provider"};
  }
  status_provider_(response);
  return grpc::Status::OK;
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
    response->set_error_message("key must be 1 to 4096 bytes and must not start with the engine's reserved prefix");
    return grpc::Status::OK;
  }
  if (!IsValidValue(request->value())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("value must be at most 1 MiB (1048576 bytes)");
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
  SetCommitResponse(commit, command.client_id, command.seq, response);
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
    response->set_error_message("key must be 1 to 4096 bytes and must not start with the engine's reserved prefix");
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
  SetCommitResponse(commit, command.client_id, command.seq, response);
  return grpc::Status::OK;
}

grpc::Status ClientService::Get(grpc::ServerContext*,
                                const network::proto::GetRequest* request,
                                network::proto::GetResponse* response) {
  if (!IsValidKey(request->key())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("key must be 1 to 4096 bytes and must not start with the engine's reserved prefix");
    return grpc::Status::OK;
  }
  if (!IsValidTimeout(request->timeout_ms())) {
    response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
    response->set_error_message("timeout_ms must be between 1 and 30000");
    return grpc::Status::OK;
  }

  // Linearizable read without writing to the Raft log: the leader confirms with a majority that it
  // is still the leader and waits until its state machine has applied everything committed so far.
  // IsLeader() alone is insufficient: it gives no quorum confirmation and may be stale.
  const CommitResponse commit =
      node_->ReadIndex(std::chrono::milliseconds(request->timeout_ms()));
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
    case Status::kInvalidArgument:
      response->set_status(network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
      response->set_error_message("the storage engine rejected the key");
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
