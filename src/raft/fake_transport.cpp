#include "raft/fake_transport.h"

#include <utility>

namespace raftkv {

// ---- FakeTransport: thin wrappers that forward to the network ----

void FakeTransport::SendRequestVote(NodeId to, const RequestVoteRequest& req,
                                    std::function<void(const RequestVoteReply&)> on_reply) {
  net_->SendRequestVote(id_, to, req, std::move(on_reply));
}

void FakeTransport::SendAppendEntries(NodeId to, const AppendEntriesRequest& req,
                                      std::function<void(const AppendEntriesReply&)> on_reply) {
  net_->SendAppendEntries(id_, to, req, std::move(on_reply));
}

void FakeTransport::SetHandler(RpcHandler* handler) { net_->Register(id_, handler); }

void FakeTransport::Stop() { net_->Register(id_, nullptr); }

// ---- FakeNetwork ----

FakeNetwork::FakeNetwork() { worker_ = std::thread(&FakeNetwork::WorkerLoop, this); }

FakeNetwork::~FakeNetwork() { Shutdown(); }

std::unique_ptr<FakeTransport> FakeNetwork::MakeTransport(NodeId id) {
  return std::make_unique<FakeTransport>(id, this);
}

void FakeNetwork::Disconnect(NodeId id) {
  std::lock_guard<std::mutex> lk(mu_);
  disconnected_.insert(id);
}

void FakeNetwork::Reconnect(NodeId id) {
  std::lock_guard<std::mutex> lk(mu_);
  disconnected_.erase(id);
}

void FakeNetwork::Shutdown() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  if (worker_.joinable()) worker_.join();
}

void FakeNetwork::Register(NodeId id, RpcHandler* handler) {
  std::lock_guard<std::mutex> lk(mu_);
  if (handler == nullptr) {
    handlers_.erase(id);
  } else {
    handlers_[id] = handler;
  }
}

bool FakeNetwork::Reachable(NodeId a, NodeId b) {
  std::lock_guard<std::mutex> lk(mu_);
  return !stop_ && disconnected_.count(a) == 0 && disconnected_.count(b) == 0;
}

RpcHandler* FakeNetwork::Lookup(NodeId from, NodeId to) {
  std::lock_guard<std::mutex> lk(mu_);
  if (stop_ || disconnected_.count(from) != 0 || disconnected_.count(to) != 0) return nullptr;
  auto it = handlers_.find(to);
  return it == handlers_.end() ? nullptr : it->second;
}

void FakeNetwork::Post(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stop_) return;
    queue_.push_back(std::move(task));
  }
  cv_.notify_one();
}

void FakeNetwork::SendRequestVote(NodeId from, NodeId to, const RequestVoteRequest& req,
                                  std::function<void(const RequestVoteReply&)> on_reply) {
  Post([this, from, to, req, on_reply = std::move(on_reply)]() {
    RpcHandler* handler = Lookup(from, to);
    if (handler == nullptr) return;  // dropped: silence, like a real timeout
    RequestVoteReply reply = handler->HandleRequestVote(req);
    if (Reachable(to, from)) on_reply(reply);
  });
}

void FakeNetwork::SendAppendEntries(NodeId from, NodeId to, const AppendEntriesRequest& req,
                                    std::function<void(const AppendEntriesReply&)> on_reply) {
  Post([this, from, to, req, on_reply = std::move(on_reply)]() {
    RpcHandler* handler = Lookup(from, to);
    if (handler == nullptr) return;
    AppendEntriesReply reply = handler->HandleAppendEntries(req);
    if (Reachable(to, from)) on_reply(reply);
  });
}

void FakeNetwork::WorkerLoop() {
  for (;;) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
      if (stop_) return;
      task = std::move(queue_.front());
      queue_.pop_front();
    }
    task();  // run without holding the lock
  }
}

}  // namespace raftkv
