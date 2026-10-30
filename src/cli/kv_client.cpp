#include "cli/kv_client.h"

#include <algorithm>
#include <random>
#include <stdexcept>
#include <thread>

#include <grpcpp/grpcpp.h>

namespace raftkv {
namespace {

using network::proto::ClientStatus;
using Clock = std::chrono::steady_clock;

std::shared_ptr<network::proto::ClientRpc::Stub> MakeStub(const std::string& address) {
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(8 * 1024 * 1024);
  args.SetMaxSendMessageSize(8 * 1024 * 1024);
  auto channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
  return std::shared_ptr<network::proto::ClientRpc::Stub>(network::proto::ClientRpc::NewStub(channel));
}

uint32_t ServerTimeoutMs(std::chrono::milliseconds rpc_timeout) {
  // The node's own wait ends a little before the gRPC deadline, so we get an answer instead of a
  // transport error whenever possible.
  const auto ms = rpc_timeout.count() - 100;
  return static_cast<uint32_t>(std::max<long long>(50, ms));
}

uint64_t RandomClientId() {
  std::random_device rd;
  std::mt19937_64 rng((static_cast<uint64_t>(rd()) << 32) ^ rd() ^
                      static_cast<uint64_t>(Clock::now().time_since_epoch().count()));
  uint64_t id = 0;
  do {
    id = rng();
  } while (id == 0);
  return id;
}

}  // namespace

void KvClient::RenewSession() {
  uint64_t id = RandomClientId();
  while (id == client_id_) id = RandomClientId();
  client_id_ = id;
  seq_ = 0;
}

KvClient::KvClient(KvClientOptions options) : options_(std::move(options)) {
  if (options_.servers.empty()) throw std::invalid_argument("KvClient needs at least one server");
  client_id_ = options_.client_id;
  if (client_id_ == 0) client_id_ = RandomClientId();
  for (const auto& s : options_.servers) {
    ids_.push_back(s.first);
    stubs_[s.first] = MakeStub(s.second);
  }
  std::sort(ids_.begin(), ids_.end());
}

void KvClient::SetServer(NodeId id, const std::string& address) {
  if (stubs_.find(id) == stubs_.end()) {
    ids_.push_back(id);
    std::sort(ids_.begin(), ids_.end());
  }
  stubs_[id] = MakeStub(address);
}

void KvClient::Rotate() { current_ = (current_ + 1) % ids_.size(); }

void KvClient::Prefer(NodeId hint) {
  const auto it = std::find(ids_.begin(), ids_.end(), hint);
  if (it != ids_.end()) {
    current_ = static_cast<size_t>(it - ids_.begin());
  } else {
    Rotate();
  }
}

KvResult KvClient::Put(const std::string& key, const std::string& value) { return Write(true, key, value); }
KvResult KvClient::Delete(const std::string& key) { return Write(false, key, ""); }

KvResult KvClient::Write(bool is_put, const std::string& key, const std::string& value) {
  uint64_t seq = ++seq_;  // ONE number for the request and every retry of it
  // Whether an earlier attempt of THIS request might have been applied (it reached a server and we
  // did not get a definite "not applied"). Decides what to do if the session turns out to be expired.
  bool may_have_applied = false;
  const auto deadline = Clock::now() + options_.op_timeout;
  KvResult result;
  result.message = "no response before the deadline";
  size_t quiet_failures = 0;
  while (Clock::now() < deadline) {
    ++result.attempts;
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    const auto rpc_timeout = std::min(remaining, options_.rpc_timeout);
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + rpc_timeout);
    network::proto::WriteResponse response;
    grpc::Status status;
    const auto stub = stubs_[Current()];
    if (is_put) {
      network::proto::PutRequest request;
      request.set_client_id(client_id_);
      request.set_seq(seq);
      request.set_key(key);
      request.set_value(value);
      request.set_timeout_ms(ServerTimeoutMs(rpc_timeout));
      status = stub->Put(&context, request, &response);
    } else {
      network::proto::DeleteRequest request;
      request.set_client_id(client_id_);
      request.set_seq(seq);
      request.set_key(key);
      request.set_timeout_ms(ServerTimeoutMs(rpc_timeout));
      status = stub->Delete(&context, request, &response);
    }
    if (!status.ok()) {  // node down or unreachable: try the next one
      result.message = status.error_message().empty() ? "rpc failed" : status.error_message();
      may_have_applied = true;  // the request may have arrived before the call failed
      Rotate();
      if (++quiet_failures % ids_.size() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(50));
      continue;
    }
    quiet_failures = 0;
    switch (response.status()) {
      case ClientStatus::CLIENT_STATUS_OK:
        result.status = KvStatus::kOk;
        result.message.clear();
        return result;
      case ClientStatus::CLIENT_STATUS_SESSION_EXPIRED:
        // This attempt was refused, not applied. If no earlier attempt can have been applied
        // either, the request simply never happened: retry it as request 1 of a new session.
        // Otherwise an earlier attempt may have taken effect, and retrying could apply it twice.
        RenewSession();
        if (!may_have_applied) {
          seq = ++seq_;
          result.message = "session expired; retried under a new session";
          break;
        }
        result.status = KvStatus::kOutcomeUnknown;
        result.message = "session expired after an attempt whose outcome is unknown: the write may or may not have happened";
        return result;
      case ClientStatus::CLIENT_STATUS_INVALID_ARGUMENT:
        result.status = KvStatus::kInvalidArgument;
        result.message = response.error_message();
        return result;
      case ClientStatus::CLIENT_STATUS_DUPLICATE_OUTCOME_UNKNOWN:
        result.status = KvStatus::kStaleRequest;
        result.message = response.error_message();
        return result;
      case ClientStatus::CLIENT_STATUS_NOT_LEADER:
        result.message = "not the leader";
        if (response.leader_id_hint() != 0) {
          Prefer(response.leader_id_hint());
        } else {
          Rotate();
          std::this_thread::sleep_for(std::chrono::milliseconds(30));  // an election may be running
        }
        break;
      case ClientStatus::CLIENT_STATUS_STOPPED:
      case ClientStatus::CLIENT_STATUS_UNAVAILABLE:
        result.message = response.error_message();
        may_have_applied = true;  // the proposal may already be in the log
        Rotate();
        break;
      default:  // NOT_COMMITTED, OUTCOME_UNKNOWN, INTERNAL_ERROR: retry the SAME request number
        result.message = response.error_message();
        may_have_applied = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        break;
    }
  }
  result.status = KvStatus::kUnavailable;
  return result;
}

KvResult KvClient::Get(const std::string& key) {
  const auto deadline = Clock::now() + options_.op_timeout;
  KvResult result;
  result.message = "no response before the deadline";
  size_t quiet_failures = 0;
  while (Clock::now() < deadline) {
    ++result.attempts;
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    const auto rpc_timeout = std::min(remaining, options_.rpc_timeout);
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + rpc_timeout);
    network::proto::GetRequest request;
    request.set_key(key);
    request.set_timeout_ms(ServerTimeoutMs(rpc_timeout));
    network::proto::GetResponse response;
    const auto status = stubs_[Current()]->Get(&context, request, &response);
    if (!status.ok()) {
      result.message = status.error_message().empty() ? "rpc failed" : status.error_message();
      Rotate();
      if (++quiet_failures % ids_.size() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(50));
      continue;
    }
    quiet_failures = 0;
    switch (response.status()) {
      case ClientStatus::CLIENT_STATUS_OK:
        result.status = KvStatus::kOk;
        result.value = response.value();
        result.message.clear();
        return result;
      case ClientStatus::CLIENT_STATUS_NOT_FOUND:
        result.status = KvStatus::kNotFound;
        result.message.clear();
        return result;
      case ClientStatus::CLIENT_STATUS_INVALID_ARGUMENT:
        result.status = KvStatus::kInvalidArgument;
        result.message = response.error_message();
        return result;
      case ClientStatus::CLIENT_STATUS_NOT_LEADER:
        result.message = "not the leader";
        if (response.leader_id_hint() != 0) {
          Prefer(response.leader_id_hint());
        } else {
          Rotate();
          std::this_thread::sleep_for(std::chrono::milliseconds(30));
        }
        break;
      default:
        result.message = response.error_message();
        Rotate();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        break;
    }
  }
  result.status = KvStatus::kUnavailable;
  return result;
}

bool KvClient::NodeStatus(NodeId id, network::proto::StatusResponse* out, std::string* error) {
  const auto it = stubs_.find(id);
  if (it == stubs_.end()) {
    if (error) *error = "unknown node";
    return false;
  }
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
  network::proto::StatusRequest request;
  const auto status = it->second->NodeStatus(&context, request, out);
  if (!status.ok()) {
    if (error) *error = status.error_message().empty() ? "unreachable" : status.error_message();
    return false;
  }
  return true;
}

std::string HexEncode(const std::string& bytes) {
  static const char* kDigits = "0123456789abcdef";
  std::string out;
  out.reserve(bytes.size() * 2);
  for (unsigned char c : bytes) {
    out.push_back(kDigits[c >> 4]);
    out.push_back(kDigits[c & 0xF]);
  }
  return out;
}

bool HexDecode(const std::string& hex, std::string* out) {
  if (hex.size() % 2 != 0) return false;
  auto nibble = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  };
  std::string result;
  result.reserve(hex.size() / 2);
  for (size_t i = 0; i < hex.size(); i += 2) {
    const int hi = nibble(hex[i]), lo = nibble(hex[i + 1]);
    if (hi < 0 || lo < 0) return false;
    result.push_back(static_cast<char>(hi * 16 + lo));
  }
  *out = std::move(result);
  return true;
}

}  // namespace raftkv
