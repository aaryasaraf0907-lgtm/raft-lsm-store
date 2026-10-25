#include "network/grpc_transport.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace raftkv {
namespace {

std::string Address(int port) {
  return "127.0.0.1:" + std::to_string(port);
}

class RecordingHandler final : public RpcHandler {
 public:
  RequestVoteReply HandleRequestVote(
      const RequestVoteRequest& request) override {
    std::lock_guard<std::mutex> lock(mutex_);
    vote_request_ = request;
    return {request.term + 1, true};
  }

  AppendEntriesReply HandleAppendEntries(
      const AppendEntriesRequest& request) override {
    std::lock_guard<std::mutex> lock(mutex_);
    append_request_ = request;
    return {request.term, true, request.prev_log_index + request.entries.size()};
  }

  RequestVoteRequest VoteRequest() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return vote_request_;
  }

  AppendEntriesRequest AppendRequest() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return append_request_;
  }

 private:
  mutable std::mutex mutex_;
  RequestVoteRequest vote_request_{};
  AppendEntriesRequest append_request_{};
};

class ThrowsOnceHandler final : public RpcHandler {
 public:
  RequestVoteReply HandleRequestVote(
      const RequestVoteRequest& request) override {
    ++vote_calls_;
    if (!thrown_.exchange(true)) {
      throw 42;
    }
    return {request.term + 1, true};
  }

  AppendEntriesReply HandleAppendEntries(
      const AppendEntriesRequest&) override {
    return {};
  }

  int VoteCalls() const { return vote_calls_.load(); }

 private:
  std::atomic<bool> thrown_{false};
  std::atomic<int> vote_calls_{0};
};

TEST(GrpcTransportTest, SendsBothRpcTypesBetweenLocalTransports) {
  RecordingHandler handler_a;
  RecordingHandler handler_b;
  GrpcTransport transport_a("127.0.0.1:0", {});
  GrpcTransport transport_b("127.0.0.1:0", {});
  transport_a.SetHandler(&handler_a);
  transport_b.SetHandler(&handler_b);
  transport_a.Start();
  transport_b.Start();
  ASSERT_NE(transport_a.ListeningPort(), 0);
  ASSERT_NE(transport_b.ListeningPort(), 0);
  transport_a.SetPeerAddress(2, Address(transport_b.ListeningPort()));
  transport_b.SetPeerAddress(1, Address(transport_a.ListeningPort()));

  std::mutex mutex;
  std::condition_variable cv;
  int replies_received = 0;
  RequestVoteReply vote_reply{};
  AppendEntriesReply append_reply{};

  transport_b.SendRequestVote(
      1, {8, 2, 17, 7}, [&](const RequestVoteReply& reply) {
        std::lock_guard<std::mutex> lock(mutex);
        vote_reply = reply;
        ++replies_received;
        cv.notify_one();
      });
  transport_a.SendAppendEntries(
      2, {9, 1, 17, 8, {{9, 18, {CommandType::kPut, "k", "v"}}}, 17},
      [&](const AppendEntriesReply& reply) {
        std::lock_guard<std::mutex> lock(mutex);
        append_reply = reply;
        ++replies_received;
        cv.notify_one();
      });

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3),
                            [&] { return replies_received == 2; }));
  }

  EXPECT_EQ(vote_reply.term, 9);
  EXPECT_TRUE(vote_reply.vote_granted);
  const auto received_vote = handler_a.VoteRequest();
  EXPECT_EQ(received_vote.candidate_id, 2);
  EXPECT_EQ(received_vote.last_log_index, 17);

  EXPECT_EQ(append_reply.term, 9);
  EXPECT_TRUE(append_reply.success);
  EXPECT_EQ(append_reply.match_index, 18);
  const auto received_append = handler_b.AppendRequest();
  ASSERT_EQ(received_append.entries.size(), 1U);
  EXPECT_EQ(received_append.entries.front().command.key, "k");
  EXPECT_EQ(received_append.entries.front().command.value, "v");

  transport_a.Stop();
  transport_b.Stop();
}

TEST(GrpcTransportTest, DoesNotCallBackForUnavailablePeer) {
  RecordingHandler local_handler;
  RecordingHandler remote_handler;
  GrpcTransport transport("127.0.0.1:0", {});
  GrpcTransport remote("127.0.0.1:0", {});
  transport.SetHandler(&local_handler);
  remote.SetHandler(&remote_handler);
  remote.Start();
  ASSERT_NE(remote.ListeningPort(), 0);
  const auto remote_address = Address(remote.ListeningPort());
  remote.Stop();

  transport.Start();
  transport.SetPeerAddress(2, remote_address);

  std::mutex mutex;
  std::condition_variable cv;
  bool callback_called = false;
  transport.SendRequestVote(2, {1, 1, 0, 0},
                            [&](const RequestVoteReply&) {
                              std::lock_guard<std::mutex> lock(mutex);
                              callback_called = true;
                              cv.notify_one();
                            });

  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(400),
                [&] { return callback_called; });
    EXPECT_FALSE(callback_called);
  }
  transport.Stop();
}

TEST(GrpcTransportTest, CallbackExceptionDoesNotStopWorkerPool) {
  RecordingHandler handler;
  GrpcTransport transport("127.0.0.1:0", {});
  transport.SetHandler(&handler);
  transport.Start();
  ASSERT_NE(transport.ListeningPort(), 0);
  transport.SetPeerAddress(1, Address(transport.ListeningPort()));

  std::mutex mutex;
  std::condition_variable cv;
  bool second_callback_called = false;
  transport.SendRequestVote(1, {1, 1, 0, 0},
                            [](const RequestVoteReply&) {
                              throw std::runtime_error("callback failure");
                            });
  transport.SendRequestVote(
      1, {2, 1, 0, 0}, [&](const RequestVoteReply&) {
        std::lock_guard<std::mutex> lock(mutex);
        second_callback_called = true;
        cv.notify_one();
      });

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3),
                            [&] { return second_callback_called; }));
  }

  transport.Stop();
}

TEST(GrpcTransportTest, StopCanBeCalledFromReplyCallback) {
  RecordingHandler handler;
  GrpcTransport transport("127.0.0.1:0", {});
  transport.SetHandler(&handler);
  transport.Start();
  ASSERT_NE(transport.ListeningPort(), 0);
  transport.SetPeerAddress(1, Address(transport.ListeningPort()));

  std::mutex mutex;
  std::condition_variable cv;
  bool callback_ready = false;
  bool allow_callback_stop = false;
  bool callback_finished = false;
  transport.SendRequestVote(1, {1, 1, 0, 0},
                            [&](const RequestVoteReply&) {
                              {
                                std::unique_lock<std::mutex> lock(mutex);
                                callback_ready = true;
                                cv.notify_one();
                                cv.wait(lock,
                                        [&] { return allow_callback_stop; });
                              }
                              transport.Stop();
                              std::lock_guard<std::mutex> lock(mutex);
                              callback_finished = true;
                              cv.notify_one();
                            });

  bool callback_ready_observed = false;
  {
    std::unique_lock<std::mutex> lock(mutex);
    callback_ready_observed =
        cv.wait_for(lock, std::chrono::seconds(3),
                    [&] { return callback_ready; });
    allow_callback_stop = true;
    cv.notify_one();
  }

  ASSERT_TRUE(callback_ready_observed);
  transport.Stop();
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3),
                            [&] { return callback_finished; }));
  }
  transport.Stop();
}

TEST(GrpcTransportTest, NonStandardHandlerExceptionDoesNotEscapeService) {
  ThrowsOnceHandler handler;
  GrpcTransport transport("127.0.0.1:0", {});
  transport.SetHandler(&handler);
  transport.Start();
  ASSERT_NE(transport.ListeningPort(), 0);
  transport.SetPeerAddress(1, Address(transport.ListeningPort()));

  std::mutex mutex;
  std::condition_variable cv;
  int replies_received = 0;
  transport.SendRequestVote(1, {1, 1, 0, 0},
                            [&](const RequestVoteReply&) {
                              std::lock_guard<std::mutex> lock(mutex);
                              ++replies_received;
                              cv.notify_one();
                            });
  transport.SendRequestVote(1, {2, 1, 0, 0},
                            [&](const RequestVoteReply&) {
                              std::lock_guard<std::mutex> lock(mutex);
                              ++replies_received;
                              cv.notify_one();
                            });

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3),
                            [&] { return replies_received == 1; }));
  }
  transport.Stop();

  EXPECT_EQ(handler.VoteCalls(), 2);
  EXPECT_EQ(replies_received, 1);
}

TEST(GrpcTransportTest, CallbackStopReturnsWhenExternalStopIsInProgress) {
  RecordingHandler handler;
  GrpcTransport transport("127.0.0.1:0", {});
  transport.SetHandler(&handler);
  transport.Start();
  ASSERT_NE(transport.ListeningPort(), 0);
  const auto address = Address(transport.ListeningPort());
  transport.SetPeerAddress(1, address);

  std::mutex mutex;
  std::condition_variable cv;
  bool callback_ready = false;
  bool allow_callback_stop = false;
  bool callback_finished = false;
  bool external_stop_finished = false;
  transport.SendRequestVote(1, {1, 1, 0, 0},
                            [&](const RequestVoteReply&) {
                              std::unique_lock<std::mutex> lock(mutex);
                              callback_ready = true;
                              cv.notify_all();
                              cv.wait(lock,
                                      [&] { return allow_callback_stop; });
                              lock.unlock();
                              transport.Stop();
                              lock.lock();
                              callback_finished = true;
                              cv.notify_all();
                            });

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3),
                            [&] { return callback_ready; }));
  }

  std::thread external_stopper([&] {
    transport.Stop();
    std::lock_guard<std::mutex> lock(mutex);
    external_stop_finished = true;
    cv.notify_all();
  });

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(3);
  bool shutdown_started = false;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      transport.SetPeerAddress(1, address);
      std::this_thread::yield();
    } catch (const std::logic_error&) {
      shutdown_started = true;
      break;
    }
  }

  {
    std::lock_guard<std::mutex> lock(mutex);
    allow_callback_stop = true;
    cv.notify_all();
  }
  external_stopper.join();

  EXPECT_TRUE(shutdown_started);
  EXPECT_TRUE(callback_finished);
  EXPECT_TRUE(external_stop_finished);
  transport.Stop();
}

class SnapshotHandler final : public RpcHandler {
 public:
  RequestVoteReply HandleRequestVote(const RequestVoteRequest&) override { return {}; }
  AppendEntriesReply HandleAppendEntries(const AppendEntriesRequest& request) override {
    return {request.term, true, request.prev_log_index};
  }
  PreVoteReply HandlePreVote(const PreVoteRequest& request) override {
    std::lock_guard<std::mutex> lock(mutex_);
    prevote_ = request;
    return {request.term - 1, true};
  }
  InstallSnapshotReply HandleInstallSnapshot(const InstallSnapshotRequest& request) override {
    ++snapshot_calls_;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      snapshot_ = request;
    }
    if (snapshot_delay_ms_ > 0) std::this_thread::sleep_for(std::chrono::milliseconds(snapshot_delay_ms_));
    if (block_first_ && snapshot_calls_.load() == 1) {
      std::unique_lock<std::mutex> lock(gate_mutex_);
      gate_cv_.wait_for(lock, std::chrono::seconds(5), [this] { return released_; });
    }
    return {request.term, request.offset + request.data.size(), request.done};
  }
  void Release() {
    {
      std::lock_guard<std::mutex> lock(gate_mutex_);
      released_ = true;
    }
    gate_cv_.notify_all();
  }
  PreVoteRequest PreVote() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return prevote_;
  }
  InstallSnapshotRequest Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_;
  }
  int SnapshotCalls() const { return snapshot_calls_.load(); }
  bool block_first_ = false;
  int snapshot_delay_ms_ = 0;  // every snapshot chunk takes this long to handle

 private:
  mutable std::mutex mutex_;
  PreVoteRequest prevote_{};
  InstallSnapshotRequest snapshot_{};
  std::atomic<int> snapshot_calls_{0};
  std::mutex gate_mutex_;
  std::condition_variable gate_cv_;
  bool released_ = false;
};

template <typename Pred>
bool WaitUntil(Pred pred, std::chrono::milliseconds timeout) {
  const auto end = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < end) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return pred();
}

TEST(GrpcTransportTest, CarriesPreVoteAndBinarySnapshotChunksAndReportsPreVoteSupport) {
  SnapshotHandler handler;
  GrpcTransport server("127.0.0.1:0", {});
  server.SetHandler(&handler);
  server.Start();
  GrpcTransport client("127.0.0.1:0", {{2, Address(server.ListeningPort())}});
  RecordingHandler unused;
  client.SetHandler(&unused);
  client.Start();
  EXPECT_TRUE(client.SupportsPreVote());

  std::atomic<bool> got_prevote{false};
  PreVoteReply prevote_reply{};
  client.SendPreVote(2, PreVoteRequest{6, 1, 50, 5}, [&](const PreVoteReply& r) {
    prevote_reply = r;
    got_prevote = true;
  });
  ASSERT_TRUE(WaitUntil([&] { return got_prevote.load(); }, std::chrono::seconds(3)));
  EXPECT_EQ(prevote_reply.term, 5u);
  EXPECT_TRUE(prevote_reply.granted);
  EXPECT_EQ(handler.PreVote().term, 6u);
  EXPECT_EQ(handler.PreVote().last_log_index, 50u);

  std::string data(1024 * 1024, '\0');  // a full-size chunk with NULs and non-UTF-8 bytes
  for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>(i * 37 + 1);
  data[10] = '\0';
  data[11] = '\xff';
  std::atomic<bool> got_snapshot{false};
  InstallSnapshotReply snapshot_reply{};
  client.SendInstallSnapshot(2, InstallSnapshotRequest{3, 1, 777, 2, 4096, data, true},
                             [&](const InstallSnapshotReply& r) {
                               snapshot_reply = r;
                               got_snapshot = true;
                             });
  ASSERT_TRUE(WaitUntil([&] { return got_snapshot.load(); }, std::chrono::seconds(5)));
  EXPECT_EQ(snapshot_reply.next_offset, 4096u + data.size());
  EXPECT_TRUE(snapshot_reply.installed);
  EXPECT_EQ(handler.Snapshot().data, data);
  EXPECT_EQ(handler.Snapshot().last_included_index, 777u);
  client.Stop();
  server.Stop();
}

// Raft resends the current snapshot chunk on every heartbeat. Only one snapshot RPC per peer may
// be outstanding, or a peer that never answers would flood the worker pool.
TEST(GrpcTransportTest, AtMostOneSnapshotRpcPerPeerIsInFlight) {
  SnapshotHandler handler;
  handler.block_first_ = true;
  GrpcTransport server("127.0.0.1:0", {});
  server.SetHandler(&handler);
  server.Start();
  GrpcTransport client("127.0.0.1:0", {{2, Address(server.ListeningPort())}});
  RecordingHandler unused;
  client.SetHandler(&unused);
  client.Start();

  std::atomic<int> replies{0};
  auto send = [&] {
    client.SendInstallSnapshot(2, InstallSnapshotRequest{3, 1, 10, 2, 0, "chunk", false},
                               [&](const InstallSnapshotReply&) { ++replies; });
  };
  send();
  ASSERT_TRUE(WaitUntil([&] { return handler.SnapshotCalls() == 1; }, std::chrono::seconds(3)));
  for (int i = 0; i < 20; ++i) send();  // heartbeat resends while the first is still outstanding
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(handler.SnapshotCalls(), 1) << "duplicates were sent while one was in flight";

  handler.Release();  // the first RPC completes ...
  ASSERT_TRUE(WaitUntil([&] { return replies.load() == 1; }, std::chrono::seconds(3)));
  send();             // ... and the slot is free again
  ASSERT_TRUE(WaitUntil([&] { return handler.SnapshotCalls() == 2; }, std::chrono::seconds(3)));
  client.Stop();
  server.Stop();
}

// A peer that accepts the connection but never answers must not keep heartbeats from reaching a
// healthy peer: snapshot sends to it may occupy at most one worker.
TEST(GrpcTransportTest, ASnapshotToAnUnresponsivePeerDoesNotBlockOtherPeers) {
  SnapshotHandler stuck;
  stuck.block_first_ = true;  // never released: stays stuck until the deadline
  GrpcTransport stuck_server("127.0.0.1:0", {});
  stuck_server.SetHandler(&stuck);
  stuck_server.Start();
  RecordingHandler healthy;
  GrpcTransport healthy_server("127.0.0.1:0", {});
  healthy_server.SetHandler(&healthy);
  healthy_server.Start();

  GrpcTransport client("127.0.0.1:0", {{2, Address(stuck_server.ListeningPort())},
                                      {3, Address(healthy_server.ListeningPort())}});
  RecordingHandler unused;
  client.SetHandler(&unused);
  client.Start();
  for (int i = 0; i < 50; ++i) {
    client.SendInstallSnapshot(2, InstallSnapshotRequest{3, 1, 10, 2, 0, "chunk", false},
                               [](const InstallSnapshotReply&) {});
  }
  std::atomic<int> acked{0};
  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 20; ++i) {
    client.SendAppendEntries(3, AppendEntriesRequest{3, 1, 0, 0, {}, 0},
                             [&](const AppendEntriesReply&) { ++acked; });
  }
  ASSERT_TRUE(WaitUntil([&] { return acked.load() == 20; }, std::chrono::seconds(2)))
      << "heartbeats to a healthy peer were starved";
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(2));
  stuck.Release();
  client.Stop();
  stuck_server.Stop();
  healthy_server.Stop();
}

// A snapshot chunk can take much longer than a heartbeat (the receiver may fsync a whole snapshot),
// so it gets a longer deadline than the 150 ms used for votes and AppendEntries.
TEST(GrpcTransportTest, ASlowSnapshotChunkStillGetsItsReply) {
  SnapshotHandler handler;
  handler.snapshot_delay_ms_ = 700;
  GrpcTransport server("127.0.0.1:0", {});
  server.SetHandler(&handler);
  server.Start();
  GrpcTransport client("127.0.0.1:0", {{2, Address(server.ListeningPort())}});
  RecordingHandler unused;
  client.SetHandler(&unused);
  client.Start();
  std::atomic<bool> got{false};
  client.SendInstallSnapshot(2, InstallSnapshotRequest{3, 1, 10, 2, 0, "chunk", true},
                             [&](const InstallSnapshotReply&) { got = true; });
  EXPECT_TRUE(WaitUntil([&] { return got.load(); }, std::chrono::seconds(5)))
      << "the snapshot RPC hit the short heartbeat deadline";
  client.Stop();
  server.Stop();
}

}  // namespace
}  // namespace raftkv
