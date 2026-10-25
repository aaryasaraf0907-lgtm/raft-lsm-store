#include "network/grpc_transport.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <exception>
#include <functional>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "network/proto_conversions.h"
#include "raft_rpc.grpc.pb.h"

namespace raftkv {
namespace {

constexpr auto kRpcDeadline = std::chrono::milliseconds(150);
// A snapshot chunk is up to ~1 MiB and the receiver may fsync a whole snapshot when the last one
// arrives, so it needs far longer than a heartbeat.
constexpr auto kSnapshotRpcDeadline = std::chrono::milliseconds(3000);
// Raft messages are bounded (an AppendEntries is about 1 MiB, a snapshot chunk is about 1 MiB),
// but gRPC's 4 MiB default would silently turn a larger message into a failed RPC; the limit is
// set explicitly so it is a decision, not an accident.
constexpr int kMaxMessageBytes = 8 * 1024 * 1024;

std::shared_ptr<grpc::Channel> MakeChannel(const std::string& address) {
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(kMaxMessageBytes);
  args.SetMaxSendMessageSize(kMaxMessageBytes);
  return grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
}
constexpr auto kServerShutdownDeadline = std::chrono::milliseconds(500);
constexpr std::size_t kWorkerCount = 4;
constexpr std::size_t kMaxQueuedTasks = 256;

}  // namespace

class GrpcTransport::Impl
    : public std::enable_shared_from_this<GrpcTransport::Impl> {
 public:
  Impl(std::string listen_address,
       std::map<NodeId, std::string> peer_addresses)
      : listen_address_(std::move(listen_address)) {
    for (const auto& peer : peer_addresses) {
      auto channel = MakeChannel(peer.second);
      peer_stubs_.emplace(
          peer.first,
          std::shared_ptr<network::proto::RaftRpc::Stub>(
              network::proto::RaftRpc::NewStub(std::move(channel))));
    }
  }

  void SetHandler(RpcHandler* handler) {
    handler_.store(handler, std::memory_order_release);
  }

  void SetPeerAddress(NodeId peer, std::string address) {
    if (address.empty()) {
      throw std::invalid_argument("peer address cannot be empty");
    }
    auto channel = MakeChannel(address);
    auto stub = std::shared_ptr<network::proto::RaftRpc::Stub>(
        network::proto::RaftRpc::NewStub(std::move(channel)));
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) {
      throw std::logic_error("cannot configure peer while stopping");
    }
    peer_stubs_[peer] = std::move(stub);
  }

  int ListeningPort() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return listening_port_;
  }

  void Start() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (ever_started_) {
      throw std::logic_error("GrpcTransport cannot be restarted");
    }
    if (handler_.load(std::memory_order_acquire) == nullptr) {
      throw std::logic_error("GrpcTransport requires an RPC handler before Start");
    }

    workers_.reserve(kWorkerCount);
    worker_ids_.reserve(kWorkerCount);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_address_,
                             grpc::InsecureServerCredentials(),
                             &listening_port_);
    builder.SetMaxReceiveMessageSize(kMaxMessageBytes);
    builder.SetMaxSendMessageSize(kMaxMessageBytes);
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    if (!server_) {
      throw std::runtime_error("failed to start gRPC server");
    }

    started_ = true;
    ever_started_ = true;
    const auto self = shared_from_this();
    try {
      for (std::size_t i = 0; i < kWorkerCount; ++i) {
        workers_.emplace_back([self] { self->WorkerLoop(); });
        ++active_workers_;
        worker_ids_.push_back(workers_.back().get_id());
      }
    } catch (...) {
      started_ = false;
      stopping_ = true;
      stop_in_progress_ = true;
      auto* server = server_.get();
      lock.unlock();
      cv_.notify_all();
      server->Shutdown();
      server->Wait();
      for (auto& worker : workers_) {
        if (worker.joinable()) {
          worker.join();
        }
      }
      lock.lock();
      workers_.clear();
      server_.reset();
      stop_in_progress_ = false;
      stop_cleanup_complete_ = true;
      if (active_workers_ == 0) {
        worker_ids_.clear();
      }
      lock.unlock();
      cv_.notify_all();
      throw;
    }
  }

  void Stop() {
    grpc::Server* server = nullptr;
    bool caller_is_worker = false;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (!started_ && !stopping_) {
        return;
      }
      caller_is_worker = IsWorkerThreadLocked();
      if (stop_in_progress_ || stop_cleanup_complete_) {
        if (caller_is_worker) {
          return;
        }
        cv_.wait(lock, [this] {
          return stop_cleanup_complete_ && active_workers_ == 0;
        });
        return;
      }

      started_ = false;
      stopping_ = true;
      stop_in_progress_ = true;
      tasks_.clear();
      server = server_.get();
    }
    cv_.notify_all();

    server->Shutdown(std::chrono::system_clock::now() +
                     kServerShutdownDeadline);
    server->Wait();

    for (auto& worker : workers_) {
      if (!worker.joinable()) {
        continue;
      }
      if (worker.get_id() == std::this_thread::get_id()) {
        // Worker threads retain shared ownership of Impl until WorkerLoop exits.
        worker.detach();
      } else {
        worker.join();
      }
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      workers_.clear();
      server_.reset();
      stop_in_progress_ = false;
      stop_cleanup_complete_ = true;
      if (active_workers_ == 0) {
        worker_ids_.clear();
      }
      cv_.notify_all();
    }

    if (!caller_is_worker) {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return active_workers_ == 0; });
    }
  }

  void SendRequestVote(
      NodeId to, const RequestVoteRequest& request,
      std::function<void(const RequestVoteReply&)> on_reply) {
    const auto deadline = std::chrono::system_clock::now() + kRpcDeadline;
    const auto stub = FindStub(to);
    if (!stub) {
      return;
    }
    auto wire_request = network::ToProto(request);
    Submit([this, stub, deadline, request = std::move(wire_request),
                on_reply = std::move(on_reply)]() mutable {
      network::proto::RequestVoteReply wire_reply;
      grpc::ClientContext context;
      context.set_deadline(deadline);
      const auto status =
          stub->RequestVote(&context, request, &wire_reply);
      if (!status.ok() || !IsRunning() || !on_reply) {
        return;
      }

      const auto reply = network::FromProto(wire_reply);
      on_reply(reply);
    });
  }

  void SendAppendEntries(
      NodeId to, const AppendEntriesRequest& request,
      std::function<void(const AppendEntriesReply&)> on_reply) {
    const auto deadline = std::chrono::system_clock::now() + kRpcDeadline;
    const auto stub = FindStub(to);
    if (!stub) {
      return;
    }
    auto wire_request = network::ToProto(request);
    Submit([this, stub, deadline, request = std::move(wire_request),
                on_reply = std::move(on_reply)]() mutable {
      network::proto::AppendEntriesReply wire_reply;
      grpc::ClientContext context;
      context.set_deadline(deadline);
      const auto status =
          stub->AppendEntries(&context, request, &wire_reply);
      if (!status.ok() || !IsRunning() || !on_reply) {
        return;
      }

      const auto reply = network::FromProto(wire_reply);
      on_reply(reply);
    });
  }

  void SendPreVote(
      NodeId to, const PreVoteRequest& request,
      std::function<void(const PreVoteReply&)> on_reply) {
    const auto deadline = std::chrono::system_clock::now() + kRpcDeadline;
    const auto stub = FindStub(to);
    if (!stub) {
      return;
    }
    auto wire_request = network::ToProto(request);
    Submit([this, stub, deadline, request = std::move(wire_request),
                on_reply = std::move(on_reply)]() mutable {
      network::proto::PreVoteReply wire_reply;
      grpc::ClientContext context;
      context.set_deadline(deadline);
      const auto status = stub->PreVote(&context, request, &wire_reply);
      if (!status.ok() || !IsRunning() || !on_reply) {
        return;
      }
      const auto reply = network::FromProto(wire_reply);
      on_reply(reply);
    });
  }

  void SendInstallSnapshot(
      NodeId to, const InstallSnapshotRequest& request,
      std::function<void(const InstallSnapshotReply&)> on_reply) {
    const auto stub = FindStub(to);
    if (!stub) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!started_ || stopping_) {
        return;
      }
      // One snapshot RPC per peer at a time: a repeat while one is outstanding is dropped.
      if (!snapshot_in_flight_.insert(to).second) {
        return;
      }
    }
    auto wire_request = network::ToProto(request);
    const bool queued = Submit([this, to, stub, request = std::move(wire_request),
                                    on_reply = std::move(on_reply)]() mutable {
      struct Release {  // frees the per-peer slot however the task ends
        Impl* self;
        NodeId peer;
        ~Release() {
          std::lock_guard<std::mutex> lock(self->mutex_);
          self->snapshot_in_flight_.erase(peer);
        }
      } release{this, to};
      network::proto::InstallSnapshotReply wire_reply;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + kSnapshotRpcDeadline);
      const auto status = stub->InstallSnapshot(&context, request, &wire_reply);
      if (!status.ok() || !IsRunning() || !on_reply) {
        return;
      }
      const auto reply = network::FromProto(wire_reply);
      on_reply(reply);
    });
    if (!queued) {  // the task never ran, so nothing will release the slot
      std::lock_guard<std::mutex> lock(mutex_);
      snapshot_in_flight_.erase(to);
    }
  }

 private:
  class Service final : public network::proto::RaftRpc::Service {
   public:
    explicit Service(Impl* owner) : owner_(owner) {}

    grpc::Status RequestVote(
        grpc::ServerContext*, const network::proto::RequestVoteRequest* request,
        network::proto::RequestVoteReply* response) override {
      auto* handler = owner_->handler_.load(std::memory_order_acquire);
      if (!handler) {
        return {grpc::StatusCode::UNAVAILABLE, "RPC handler is not set"};
      }
      try {
        const auto converted = network::FromProto(*request);
        *response = network::ToProto(handler->HandleRequestVote(converted));
        return grpc::Status::OK;
      } catch (const std::invalid_argument& error) {
        return {grpc::StatusCode::INVALID_ARGUMENT, error.what()};
      } catch (const std::exception& error) {
        return {grpc::StatusCode::INTERNAL, error.what()};
      } catch (...) {
        return {grpc::StatusCode::INTERNAL, "RPC handler threw an unknown exception"};
      }
    }

    grpc::Status AppendEntries(
        grpc::ServerContext*,
        const network::proto::AppendEntriesRequest* request,
        network::proto::AppendEntriesReply* response) override {
      auto* handler = owner_->handler_.load(std::memory_order_acquire);
      if (!handler) {
        return {grpc::StatusCode::UNAVAILABLE, "RPC handler is not set"};
      }
      try {
        const auto converted = network::FromProto(*request);
        *response =
            network::ToProto(handler->HandleAppendEntries(converted));
        return grpc::Status::OK;
      } catch (const std::invalid_argument& error) {
        return {grpc::StatusCode::INVALID_ARGUMENT, error.what()};
      } catch (const std::exception& error) {
        return {grpc::StatusCode::INTERNAL, error.what()};
      } catch (...) {
        return {grpc::StatusCode::INTERNAL, "RPC handler threw an unknown exception"};
      }
    }

    grpc::Status PreVote(
        grpc::ServerContext*, const network::proto::PreVoteRequest* request,
        network::proto::PreVoteReply* response) override {
      auto* handler = owner_->handler_.load(std::memory_order_acquire);
      if (!handler) {
        return {grpc::StatusCode::UNAVAILABLE, "RPC handler is not set"};
      }
      try {
        const auto converted = network::FromProto(*request);
        *response = network::ToProto(handler->HandlePreVote(converted));
        return grpc::Status::OK;
      } catch (const std::invalid_argument& error) {
        return {grpc::StatusCode::INVALID_ARGUMENT, error.what()};
      } catch (const std::exception& error) {
        return {grpc::StatusCode::INTERNAL, error.what()};
      } catch (...) {
        return {grpc::StatusCode::INTERNAL, "RPC handler threw an unknown exception"};
      }
    }

    grpc::Status InstallSnapshot(
        grpc::ServerContext*,
        const network::proto::InstallSnapshotRequest* request,
        network::proto::InstallSnapshotReply* response) override {
      auto* handler = owner_->handler_.load(std::memory_order_acquire);
      if (!handler) {
        return {grpc::StatusCode::UNAVAILABLE, "RPC handler is not set"};
      }
      try {
        const auto converted = network::FromProto(*request);
        *response = network::ToProto(handler->HandleInstallSnapshot(converted));
        return grpc::Status::OK;
      } catch (const std::invalid_argument& error) {
        return {grpc::StatusCode::INVALID_ARGUMENT, error.what()};
      } catch (const std::exception& error) {
        return {grpc::StatusCode::INTERNAL, error.what()};
      } catch (...) {
        return {grpc::StatusCode::INTERNAL, "RPC handler threw an unknown exception"};
      }
    }

   private:
    Impl* owner_;
  };

  std::shared_ptr<network::proto::RaftRpc::Stub> FindStub(NodeId to) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = peer_stubs_.find(to);
    return it == peer_stubs_.end() ? nullptr : it->second;
  }

  bool IsRunning() {
    std::lock_guard<std::mutex> lock(mutex_);
    return started_ && !stopping_;
  }

  bool IsWorkerThreadLocked() const {
    const auto current_id = std::this_thread::get_id();
    for (const auto worker_id : worker_ids_) {
      if (worker_id == current_id) {
        return true;
      }
    }
    return false;
  }

  // False if the task was not queued (not running, or the queue is full).
  bool Submit(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!started_ || stopping_ || tasks_.size() >= kMaxQueuedTasks) {
        return false;
      }
      tasks_.push_back(std::move(task));
    }
    cv_.notify_one();
    return true;
  }

  void WorkerLoop() {
    for (;;) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
        if (stopping_ && tasks_.empty()) {
          break;
        }
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      try {
        task();
      } catch (const std::exception& error) {
        std::fprintf(stderr, "GrpcTransport worker task threw: %s\n",
                     error.what());
      } catch (...) {
        std::fprintf(stderr,
                     "GrpcTransport worker task threw an unknown exception\n");
      }
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      --active_workers_;
      if (active_workers_ == 0 && stop_cleanup_complete_) {
        worker_ids_.clear();
      }
    }
    cv_.notify_all();
  }

  std::string listen_address_;
  std::map<NodeId, std::shared_ptr<network::proto::RaftRpc::Stub>>
      peer_stubs_;
  Service service_{this};
  std::atomic<RpcHandler*> handler_{nullptr};
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> tasks_;
  std::set<NodeId> snapshot_in_flight_;  // peers with a snapshot RPC outstanding (guarded by mutex_)
  std::vector<std::thread> workers_;
  std::vector<std::thread::id> worker_ids_;
  std::unique_ptr<grpc::Server> server_;
  int listening_port_ = 0;
  std::size_t active_workers_ = 0;
  bool started_ = false;
  bool stopping_ = false;
  bool ever_started_ = false;
  bool stop_in_progress_ = false;
  bool stop_cleanup_complete_ = false;
};

GrpcTransport::GrpcTransport(
    std::string listen_address,
    std::map<NodeId, std::string> peer_addresses)
    : impl_(std::make_shared<Impl>(std::move(listen_address),
                                   std::move(peer_addresses))) {}

GrpcTransport::~GrpcTransport() { Stop(); }

void GrpcTransport::SendRequestVote(
    NodeId to, const RequestVoteRequest& req,
    std::function<void(const RequestVoteReply&)> on_reply) {
  impl_->SendRequestVote(to, req, std::move(on_reply));
}

void GrpcTransport::SendAppendEntries(
    NodeId to, const AppendEntriesRequest& req,
    std::function<void(const AppendEntriesReply&)> on_reply) {
  impl_->SendAppendEntries(to, req, std::move(on_reply));
}

void GrpcTransport::SendPreVote(
    NodeId to, const PreVoteRequest& req,
    std::function<void(const PreVoteReply&)> on_reply) {
  impl_->SendPreVote(to, req, std::move(on_reply));
}

void GrpcTransport::SendInstallSnapshot(
    NodeId to, const InstallSnapshotRequest& req,
    std::function<void(const InstallSnapshotReply&)> on_reply) {
  impl_->SendInstallSnapshot(to, req, std::move(on_reply));
}

void GrpcTransport::SetHandler(RpcHandler* handler) {
  impl_->SetHandler(handler);
}

void GrpcTransport::SetPeerAddress(NodeId peer, std::string address) {
  impl_->SetPeerAddress(peer, std::move(address));
}

int GrpcTransport::ListeningPort() const { return impl_->ListeningPort(); }

void GrpcTransport::Start() { impl_->Start(); }

void GrpcTransport::Stop() { impl_->Stop(); }

}  // namespace raftkv
