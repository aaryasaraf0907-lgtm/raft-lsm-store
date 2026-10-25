#include "raft/fake_transport.h"

#include <utility>

#include "raft/message_size.h"

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

void FakeTransport::SendInstallSnapshot(NodeId to, const InstallSnapshotRequest& req,
                                        std::function<void(const InstallSnapshotReply&)> on_reply) {
  net_->SendInstallSnapshot(id_, to, req, std::move(on_reply));
}

void FakeTransport::SendPreVote(NodeId to, const PreVoteRequest& req,
                                std::function<void(const PreVoteReply&)> on_reply) {
  net_->SendPreVote(id_, to, req, std::move(on_reply));
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

void FakeNetwork::BlockLink(NodeId from, NodeId to) {
  std::lock_guard<std::mutex> lk(mu_);
  blocked_links_.insert({from, to});
}

void FakeNetwork::UnblockLink(NodeId from, NodeId to) {
  std::lock_guard<std::mutex> lk(mu_);
  blocked_links_.erase({from, to});
}

void FakeNetwork::SetFaults(const FaultPlan& plan) {
  std::lock_guard<std::mutex> lk(mu_);
  faults_ = plan;
}

void FakeNetwork::SetFaultSeed(uint64_t seed) {
  std::lock_guard<std::mutex> lk(mu_);
  fault_rng_.seed(seed);
}

void FakeNetwork::HealAll() {
  std::lock_guard<std::mutex> lk(mu_);
  disconnected_.clear();
  blocked_links_.clear();
  faults_ = FaultPlan{};
}

void FakeNetwork::Shutdown() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  idle_cv_.notify_all();
  if (worker_.joinable()) worker_.join();
}

void FakeNetwork::Register(NodeId id, RpcHandler* handler) {
  std::unique_lock<std::mutex> lk(mu_);
  ++generation_[id];  // replies owed to the previous incarnation of this node are now void
  if (handler == nullptr) {
    handlers_.erase(id);
    // The node is about to be destroyed: wait until no delivery is touching it.
    idle_cv_.wait(lk, [this] { return !busy_ || stop_; });
  } else {
    handlers_[id] = handler;
  }
}

uint64_t FakeNetwork::GenerationOf(NodeId id) {
  std::lock_guard<std::mutex> lk(mu_);
  return generation_[id];
}

bool FakeNetwork::SenderStillCurrent(NodeId id, uint64_t generation) {
  std::lock_guard<std::mutex> lk(mu_);
  return generation_[id] == generation;
}

// True if a message travelling a -> b gets through.
bool FakeNetwork::Reachable(NodeId a, NodeId b) {
  std::lock_guard<std::mutex> lk(mu_);
  return !stop_ && disconnected_.count(a) == 0 && disconnected_.count(b) == 0 &&
         blocked_links_.count({a, b}) == 0;
}

RpcHandler* FakeNetwork::Lookup(NodeId from, NodeId to) {
  std::lock_guard<std::mutex> lk(mu_);
  if (stop_ || disconnected_.count(from) != 0 || disconnected_.count(to) != 0 ||
      blocked_links_.count({from, to}) != 0) {
    return nullptr;
  }
  auto it = handlers_.find(to);
  return it == handlers_.end() ? nullptr : it->second;
}

void FakeNetwork::PostAfter(int delay_ms, std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stop_) return;
    queue_.push(Item{Clock::now() + std::chrono::milliseconds(delay_ms), next_seq_++, std::move(task)});
  }
  cv_.notify_one();
}

FakeNetwork::Decision FakeNetwork::Decide(bool is_request) {
  std::lock_guard<std::mutex> lk(mu_);
  Decision d;
  const FaultPlan& f = faults_;
  auto roll = [&](double p) { return p > 0 && std::uniform_real_distribution<double>(0, 1)(fault_rng_) < p; };
  auto delay = [&] {
    if (f.max_delay_ms <= 0) return 0;
    return static_cast<int>(std::uniform_int_distribution<int>(f.min_delay_ms, f.max_delay_ms)(fault_rng_));
  };
  d.drop = roll(is_request ? f.drop_request : f.drop_reply);
  if (d.drop) return d;
  if (is_request) d.duplicate = roll(f.duplicate);
  d.delay_ms = delay();
  if (d.duplicate) d.duplicate_delay_ms = d.delay_ms + delay() + 1;
  return d;
}

// One request/reply exchange. The request may be lost, delayed or delivered twice; the reply may be
// lost or delayed. If the request is delivered twice the handler runs twice (as it would if a real
// network retransmitted it) but the sender still gets at most ONE reply, as the Transport contract
// promises.
template <typename Req, typename Reply, typename Handle>
void FakeNetwork::SendRpc(NodeId from, NodeId to, const Req& req,
                          std::function<void(const Reply&)> on_reply, Handle handle) {
  const uint64_t gen = GenerationOf(from);
  const Decision d = Decide(/*is_request=*/true);
  if (d.drop) {
    ++fault_dropped_;
    return;
  }
  auto replied = std::make_shared<std::atomic<bool>>(false);
  auto deliver = [this, from, to, gen, req, on_reply = std::move(on_reply), replied, handle]() {
    RpcHandler* handler = Lookup(from, to);
    if (handler == nullptr) return;  // dropped: silence, like a real timeout
    Reply reply = handle(handler, req);
    const Decision r = Decide(/*is_request=*/false);
    if (r.drop) {
      ++fault_dropped_;
      return;
    }
    auto send_reply = [this, from, to, gen, reply, on_reply, replied]() {
      if (!Reachable(to, from) || !SenderStillCurrent(from, gen)) return;
      if (replied->exchange(true)) return;  // a duplicate request: only the first reply counts
      on_reply(reply);
    };
    if (r.delay_ms > 0) {
      PostAfter(r.delay_ms, std::move(send_reply));
    } else {
      send_reply();
    }
  };
  if (d.duplicate) {
    ++fault_duplicated_;
    PostAfter(d.duplicate_delay_ms, deliver);
  }
  PostAfter(d.delay_ms, std::move(deliver));
}

void FakeNetwork::SendRequestVote(NodeId from, NodeId to, const RequestVoteRequest& req,
                                  std::function<void(const RequestVoteReply&)> on_reply) {
  SendRpc<RequestVoteRequest, RequestVoteReply>(
      from, to, req, std::move(on_reply),
      [](RpcHandler* h, const RequestVoteRequest& r) { return h->HandleRequestVote(r); });
}

void FakeNetwork::SendAppendEntries(NodeId from, NodeId to, const AppendEntriesRequest& req,
                                    std::function<void(const AppendEntriesReply&)> on_reply) {
  const size_t bytes = ApproxAppendBytes(req);
  NoteSize(bytes);
  if (TooBig(bytes)) return;  // too big for the "wire": never delivered, no reply
  SendRpc<AppendEntriesRequest, AppendEntriesReply>(
      from, to, req, std::move(on_reply),
      [](RpcHandler* h, const AppendEntriesRequest& r) { return h->HandleAppendEntries(r); });
}

void FakeNetwork::NoteSize(size_t bytes) {
  size_t seen = max_observed_bytes_.load();
  while (bytes > seen && !max_observed_bytes_.compare_exchange_weak(seen, bytes)) {
  }
}

bool FakeNetwork::TooBig(size_t bytes) {
  size_t limit = max_message_bytes_.load();
  if (limit != 0 && bytes > limit) {
    ++oversized_dropped_;
    return true;
  }
  return false;
}

void FakeNetwork::SendInstallSnapshot(NodeId from, NodeId to, const InstallSnapshotRequest& req,
                                      std::function<void(const InstallSnapshotReply&)> on_reply) {
  const size_t bytes = ApproxInstallBytes(req);
  NoteSize(bytes);
  if (TooBig(bytes)) return;
  SendRpc<InstallSnapshotRequest, InstallSnapshotReply>(
      from, to, req, std::move(on_reply),
      [](RpcHandler* h, const InstallSnapshotRequest& r) { return h->HandleInstallSnapshot(r); });
}

void FakeNetwork::SendPreVote(NodeId from, NodeId to, const PreVoteRequest& req,
                              std::function<void(const PreVoteReply&)> on_reply) {
  SendRpc<PreVoteRequest, PreVoteReply>(
      from, to, req, std::move(on_reply),
      [](RpcHandler* h, const PreVoteRequest& r) { return h->HandlePreVote(r); });
}

void FakeNetwork::WorkerLoop() {
  for (;;) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lk(mu_);
      for (;;) {
        if (stop_) return;
        if (queue_.empty()) {
          cv_.wait(lk);
          continue;
        }
        const auto due = queue_.top().due;
        if (due <= Clock::now()) break;
        cv_.wait_until(lk, due);  // woken early if something more urgent is posted
      }
      task = std::move(const_cast<Item&>(queue_.top()).task);
      queue_.pop();
      busy_ = true;
    }
    task();  // run without holding the lock
    {
      std::lock_guard<std::mutex> lk(mu_);
      busy_ = false;
    }
    idle_cv_.notify_all();
  }
}

}  // namespace raftkv
