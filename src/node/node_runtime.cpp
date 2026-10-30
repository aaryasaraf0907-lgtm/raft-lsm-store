#include "node/node_runtime.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <utility>

namespace raftkv {
namespace {

constexpr int kMaxMessageBytes = 8 * 1024 * 1024;

const char* RoleName(Role role) {
  switch (role) {
    case Role::kLeader:
      return "leader";
    case Role::kCandidate:
      return "candidate";
    case Role::kFollower:
      return "follower";
  }
  return "unknown";
}

}  // namespace

NodeRuntime::NodeRuntime(NodeOptions options) : options_(std::move(options)) {}

namespace {
void FillMembershipReply(const MembershipResponse& r, network::proto::MembershipReply* reply) {
  using network::proto::MembershipStatus;
  MembershipStatus status = network::proto::MEMBERSHIP_STOPPED;
  const char* message = "";
  switch (r.result) {
    case MembershipResult::kOk: status = network::proto::MEMBERSHIP_OK; break;
    case MembershipResult::kNotLeader: status = network::proto::MEMBERSHIP_NOT_LEADER; message = "not the leader"; break;
    case MembershipResult::kInProgress:
      status = network::proto::MEMBERSHIP_IN_PROGRESS;
      message = "another membership change is in progress (or the leader is new): retry shortly";
      break;
    case MembershipResult::kInvalid:
      status = network::proto::MEMBERSHIP_INVALID;
      message = "invalid node id or address, or it would remove the last member";
      break;
    case MembershipResult::kCatchUpFailed:
      status = network::proto::MEMBERSHIP_CATCH_UP_FAILED;
      message = "the new node did not catch up with the log in time (is it running, started with --join, and reachable at that address?)";
      break;
    case MembershipResult::kTimeout:
      status = network::proto::MEMBERSHIP_TIMEOUT;
      message = "the configuration change was not committed in time; it may still take effect";
      break;
    case MembershipResult::kLost: status = network::proto::MEMBERSHIP_LOST; message = "a new leader replaced the change; nothing changed"; break;
    case MembershipResult::kStopped: status = network::proto::MEMBERSHIP_STOPPED; message = "the node is stopping"; break;
  }
  reply->set_status(status);
  reply->set_leader_id_hint(r.leader_hint);
  reply->set_error_message(message);
}

}  // namespace

NodeRuntime::~NodeRuntime() { Stop(); }

void NodeRuntime::Start() {
  if (started_) throw std::logic_error("node already started");
  if (options_.id == 0) throw std::invalid_argument("node id must be >= 1");
  if (std::find(options_.cluster.begin(), options_.cluster.end(), options_.id) ==
      options_.cluster.end()) {
    throw std::invalid_argument("the cluster list must contain this node's id");
  }
  if (options_.data_dir.empty()) throw std::invalid_argument("data_dir is required");

  std::vector<NodeId> peer_ids;
  for (NodeId id : options_.cluster) {
    if (id != options_.id) peer_ids.push_back(id);
  }

  const std::filesystem::path root(options_.data_dir);
  std::error_code ec;
  std::filesystem::create_directories(root / "raft", ec);
  if (ec) throw std::runtime_error("cannot create " + (root / "raft").string() + ": " + ec.message());

  storage_ = std::make_unique<FileRaftStorage>((root / "raft").string(), options_.sync);
  KVEngine::Options engine_options;
  engine_options.dir = (root / "kv").string();
  engine_options.sync_wal = options_.sync && options_.sync_engine_wal;
  engine_options.wal_truncate_on_corruption = !options_.sync_engine_wal;
  engine_options.use_sstables = options_.use_sstables;
  engine_options.memtable_bytes = options_.memtable_bytes;
  engine_options.compaction_trigger = options_.compaction_trigger;
  const Status opened = KVEngine::Open(engine_options, &engine_);
  if (opened != Status::kOk) {
    throw std::runtime_error("cannot open the storage engine in " + engine_options.dir +
                             " (status " + std::to_string(static_cast<int>(opened)) + ")");
  }
  machine_ = std::make_unique<KVStateMachine>(engine_.get());

  std::map<NodeId, std::string> peer_addresses;
  for (const auto& p : options_.peers) {
    if (p.first != options_.id && !p.second.empty()) peer_addresses.insert(p);
  }
  transport_ = std::make_unique<GrpcTransport>(options_.raft_listen, std::move(peer_addresses));
  RaftConfig raft_config = options_.raft;
  raft_config.join_existing_cluster = options_.join;
  if (options_.use_sstables) {  // only the LSM engine can checkpoint its data
    raft_config.snapshot_threshold_entries = options_.snapshot_threshold_entries;
    raft_config.snapshot_trailing_entries = options_.snapshot_trailing_entries;
  }
  node_ = std::make_unique<RaftNodeImpl>(options_.id, peer_ids, transport_.get(), raft_config,
                                         storage_.get());
  node_->SetApplyCallback(machine_->AsCallback());
  if (options_.use_sstables) {
    KVStateMachine* machine = machine_.get();
    node_->SetSnapshotCallbacks([machine] { return machine->TakeSnapshot(); },
                                [machine](const std::string& bytes) { machine->RestoreSnapshot(bytes); });
  }

  // New members' Raft addresses arrive inside configuration entries: hand them to the transport.
  // (Members of the initial configuration have no address there; the transport knows them from
  // the peer list.) Runs under the node's lock: SetPeerAddress only takes the transport's own lock.
  {
    GrpcTransport* transport = transport_.get();
    const NodeId self = options_.id;
    auto known = std::make_shared<std::map<NodeId, std::string>>();
    node_->SetConfigCallback([transport, self, known](const std::map<NodeId, std::string>& servers) {
      for (const auto& s : servers) {
        if (s.first == self || s.second.empty()) continue;
        auto it = known->find(s.first);
        if (it != known->end() && it->second == s.second) continue;  // unchanged: keep the channel
        (*known)[s.first] = s.second;
        transport->SetPeerAddress(s.first, s.second);
      }
    });
  }

  service_ = std::make_unique<ClientService>(node_.get(), engine_.get(), machine_.get());
  RaftNodeImpl* node = node_.get();
  auto deadline_of = [](uint32_t timeout_ms) {
    return std::chrono::milliseconds(timeout_ms == 0 ? 30000 : std::min<uint32_t>(timeout_ms, 600000));
  };
  service_->SetMembershipHandlers(
      [node, deadline_of](const network::proto::AddServerRequest& req, network::proto::MembershipReply* reply) {
        FillMembershipReply(node->AddServer(req.node_id(), req.raft_address(), deadline_of(req.timeout_ms())), reply);
      },
      [node, deadline_of](const network::proto::RemoveServerRequest& req, network::proto::MembershipReply* reply) {
        FillMembershipReply(node->RemoveServer(req.node_id(), deadline_of(req.timeout_ms())), reply);
      });
  service_->SetStatusProvider([node](network::proto::StatusResponse* out) {
    out->set_node_id(node->Id());
    out->set_role(RoleName(node->GetRole()));
    out->set_term(node->CurrentTerm());
    out->set_leader_id_hint(node->LeaderHint());
    out->set_commit_index(node->CommitIndex());
    out->set_last_applied(node->LastApplied());
    out->set_log_length(node->LogLength());
    out->set_snapshot_index(node->SnapshotIndex());
    for (const auto& m : node->Configuration()) out->add_members(m.first);
  });

  node_->Start();  // starts the node-to-node gRPC server
  raft_port_ = transport_->ListeningPort();

  grpc::ServerBuilder builder;
  builder.AddListeningPort(options_.client_listen, grpc::InsecureServerCredentials(), &client_port_);
  builder.SetMaxReceiveMessageSize(kMaxMessageBytes);
  builder.SetMaxSendMessageSize(kMaxMessageBytes);
  builder.RegisterService(service_.get());
  client_server_ = builder.BuildAndStart();
  if (!client_server_ || client_port_ == 0) {
    node_->Stop();
    throw std::runtime_error("cannot listen for clients on " + options_.client_listen);
  }
  started_ = true;
}

void NodeRuntime::Stop() {
  if (client_server_) {
    client_server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1));
    client_server_->Wait();
    client_server_.reset();
  }
  if (node_) node_->Stop();
  started_ = false;
}

void NodeRuntime::SetPeerAddress(NodeId peer, const std::string& address) {
  if (!transport_) throw std::logic_error("node not started");
  transport_->SetPeerAddress(peer, address);
}

}  // namespace raftkv
