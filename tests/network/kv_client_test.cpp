// KvClient against scripted fake servers, so the retry rules are tested deterministically (a real
// cluster cannot be made to lose exactly one reply on demand).
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "cli/kv_client.h"

namespace raftkv {
namespace {

using network::proto::ClientStatus;
using namespace std::chrono_literals;

struct Step {
  ClientStatus status = ClientStatus::CLIENT_STATUS_OK;
  uint32_t hint = 0;
  int delay_ms = 0;  // how long the server takes before replying
};

struct SeenWrite {
  uint64_t client_id;
  uint64_t seq;
  std::string key;
};

// Replies to each call with the next scripted step (OK once the script runs out).
class ScriptedServer final : public network::proto::ClientRpc::Service {
 public:
  explicit ScriptedServer(std::vector<Step> script = {}) : script_(script.begin(), script.end()) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port_);
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
  }
  ~ScriptedServer() override { server_->Shutdown(std::chrono::system_clock::now() + 100ms); }

  std::string Address() const { return "127.0.0.1:" + std::to_string(port_); }
  std::vector<SeenWrite> Writes() const {
    std::lock_guard<std::mutex> lk(mu_);
    return writes_;
  }
  int GetCalls() const {
    std::lock_guard<std::mutex> lk(mu_);
    return get_calls_;
  }

  grpc::Status Put(grpc::ServerContext*, const network::proto::PutRequest* req,
                   network::proto::WriteResponse* resp) override {
    return Write(req->client_id(), req->seq(), req->key(), resp);
  }
  grpc::Status Delete(grpc::ServerContext*, const network::proto::DeleteRequest* req,
                      network::proto::WriteResponse* resp) override {
    return Write(req->client_id(), req->seq(), req->key(), resp);
  }
  grpc::Status Get(grpc::ServerContext*, const network::proto::GetRequest*,
                   network::proto::GetResponse* resp) override {
    Step step = Next();
    {
      std::lock_guard<std::mutex> lk(mu_);
      ++get_calls_;
    }
    resp->set_status(step.status);
    resp->set_leader_id_hint(step.hint);
    if (step.status == ClientStatus::CLIENT_STATUS_OK) resp->set_value("the-value");
    return grpc::Status::OK;
  }

 private:
  Step Next() {
    Step step;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (!script_.empty()) {
        step = script_.front();
        script_.pop_front();
      }
    }
    if (step.delay_ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(step.delay_ms));
    return step;
  }
  grpc::Status Write(uint64_t client_id, uint64_t seq, const std::string& key,
                     network::proto::WriteResponse* resp) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      writes_.push_back({client_id, seq, key});
    }
    Step step = Next();
    resp->set_status(step.status);
    resp->set_leader_id_hint(step.hint);
    return grpc::Status::OK;
  }

  mutable std::mutex mu_;
  std::deque<Step> script_;
  std::vector<SeenWrite> writes_;
  int get_calls_ = 0;
  int port_ = 0;
  std::unique_ptr<grpc::Server> server_;
};

KvClientOptions Options(std::map<NodeId, std::string> servers) {
  KvClientOptions o;
  o.servers = std::move(servers);
  o.client_id = 77;
  o.rpc_timeout = 200ms;
  o.op_timeout = 5s;
  return o;
}

}  // namespace

// The heart of exactly-once: the reply to the first attempt is lost (it arrives after the client
// gave up on it), so the client retries. The retry MUST carry the same request number, or the
// state machine could not tell it is the same request and would apply the write twice.
TEST(KvClientTest, RetryAfterALostReplyReusesTheSameRequestNumber) {
  ScriptedServer server({Step{ClientStatus::CLIENT_STATUS_OK, 0, /*delay_ms=*/600}});  // slower than rpc_timeout
  KvClient client(Options({{1, server.Address()}}));
  KvResult r = client.Put("k", "v");
  EXPECT_EQ(r.status, KvStatus::kOk);
  EXPECT_GE(r.attempts, 2);
  auto writes = server.Writes();
  ASSERT_GE(writes.size(), 2u);
  for (const auto& w : writes) {
    EXPECT_EQ(w.client_id, 77u);
    EXPECT_EQ(w.seq, writes[0].seq) << "a retry used a different request number";
  }
}

TEST(KvClientTest, RetriesTheSameRequestNumberAfterUnknownOutcomeAndNotCommitted) {
  ScriptedServer server({Step{ClientStatus::CLIENT_STATUS_OUTCOME_UNKNOWN},
                         Step{ClientStatus::CLIENT_STATUS_NOT_COMMITTED}});
  KvClient client(Options({{1, server.Address()}}));
  KvResult r = client.Delete("gone");
  EXPECT_EQ(r.status, KvStatus::kOk);
  EXPECT_EQ(r.attempts, 3);
  auto writes = server.Writes();
  ASSERT_EQ(writes.size(), 3u);
  EXPECT_EQ(writes[1].seq, writes[0].seq);
  EXPECT_EQ(writes[2].seq, writes[0].seq);
}

TEST(KvClientTest, EachNewRequestGetsTheNextNumber) {
  ScriptedServer server;
  KvClient client(Options({{1, server.Address()}}));
  ASSERT_EQ(client.Put("a", "1").status, KvStatus::kOk);
  ASSERT_EQ(client.Put("b", "2").status, KvStatus::kOk);
  ASSERT_EQ(client.Delete("a").status, KvStatus::kOk);
  auto writes = server.Writes();
  ASSERT_EQ(writes.size(), 3u);
  EXPECT_EQ(writes[1].seq, writes[0].seq + 1);
  EXPECT_EQ(writes[2].seq, writes[1].seq + 1);
}

TEST(KvClientTest, FollowsTheLeaderHintAndKeepsTheRequestNumber) {
  ScriptedServer follower({Step{ClientStatus::CLIENT_STATUS_NOT_LEADER, /*hint=*/2}});
  ScriptedServer leader;
  KvClient client(Options({{1, follower.Address()}, {2, leader.Address()}}));
  KvResult r = client.Put("k", "v");
  EXPECT_EQ(r.status, KvStatus::kOk);
  ASSERT_EQ(follower.Writes().size(), 1u);
  ASSERT_EQ(leader.Writes().size(), 1u);
  EXPECT_EQ(leader.Writes()[0].seq, follower.Writes()[0].seq);
  // The next request goes straight to the leader.
  ASSERT_EQ(client.Put("k2", "v2").status, KvStatus::kOk);
  EXPECT_EQ(follower.Writes().size(), 1u);
  EXPECT_EQ(leader.Writes().size(), 2u);
}

// With three nodes, "try the next one" and "go where the hint says" differ: the hint points at node 3,
// and node 2 must never be bothered.
TEST(KvClientTest, FollowsTheHintInsteadOfJustTryingTheNextServer) {
  ScriptedServer follower({Step{ClientStatus::CLIENT_STATUS_NOT_LEADER, /*hint=*/3}});
  ScriptedServer bystander;  // node 2: would accept, but is not the leader
  ScriptedServer leader;
  KvClient client(Options({{1, follower.Address()}, {2, bystander.Address()}, {3, leader.Address()}}));
  EXPECT_EQ(client.Put("k", "v").status, KvStatus::kOk);
  EXPECT_EQ(bystander.Writes().size(), 0u) << "the client ignored the leader hint";
  EXPECT_EQ(leader.Writes().size(), 1u);
}

TEST(KvClientTest, MovesOnWhenANodeIsDown) {
  ScriptedServer live;
  KvClient client(Options({{1, "127.0.0.1:1"}, {2, live.Address()}}));  // node 1 refuses connections
  KvResult r = client.Put("k", "v");
  EXPECT_EQ(r.status, KvStatus::kOk);
  EXPECT_EQ(live.Writes().size(), 1u);
}

TEST(KvClientTest, DoesNotRetryWhatRetryingCannotFix) {
  ScriptedServer invalid({Step{ClientStatus::CLIENT_STATUS_INVALID_ARGUMENT}});
  KvClient a(Options({{1, invalid.Address()}}));
  KvResult r = a.Put("k", "v");
  EXPECT_EQ(r.status, KvStatus::kInvalidArgument);
  EXPECT_EQ(r.attempts, 1);

  ScriptedServer stale({Step{ClientStatus::CLIENT_STATUS_DUPLICATE_OUTCOME_UNKNOWN}});
  KvClient b(Options({{1, stale.Address()}}));
  r = b.Put("k", "v");
  EXPECT_EQ(r.status, KvStatus::kStaleRequest);
  EXPECT_EQ(r.attempts, 1);
}

TEST(KvClientTest, GivesUpAfterTheOperationTimeout) {
  KvClientOptions o = Options({{1, "127.0.0.1:1"}});
  o.op_timeout = 400ms;
  KvClient client(o);
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(client.Put("k", "v").status, KvStatus::kUnavailable);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 3s);
}

TEST(KvClientTest, GetMapsNotFoundRedirectsAndReturnsTheValue) {
  ScriptedServer follower({Step{ClientStatus::CLIENT_STATUS_NOT_LEADER, 2}});
  ScriptedServer leader({Step{ClientStatus::CLIENT_STATUS_NOT_FOUND}, Step{ClientStatus::CLIENT_STATUS_OK}});
  KvClient client(Options({{1, follower.Address()}, {2, leader.Address()}}));
  EXPECT_EQ(client.Get("missing").status, KvStatus::kNotFound);
  KvResult r = client.Get("present");
  EXPECT_EQ(r.status, KvStatus::kOk);
  EXPECT_EQ(r.value, "the-value");
}

TEST(KvClientTest, HexHelpersRoundTripBinaryData) {
  const std::string binary("\0\x01\xab\xff", 4);
  std::string back;
  ASSERT_TRUE(HexDecode(HexEncode(binary), &back));
  EXPECT_EQ(back, binary);
  EXPECT_EQ(HexEncode(binary), "0001abff");
  EXPECT_FALSE(HexDecode("abc", &back));   // odd length
  EXPECT_FALSE(HexDecode("zz", &back));    // not hex
  EXPECT_TRUE(HexDecode("ABCDEF", &back)); // upper case accepted
}

}  // namespace raftkv
