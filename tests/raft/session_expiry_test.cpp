// Client sessions are forgotten after a while (or the table grows forever), deterministically, and
// never in a way that lets a retried request be applied twice.
#include <gtest/gtest.h>

#include <map>
#include <random>
#include <string>
#include <vector>

#include "raft/client_sessions.h"
#include "server/kv_state_machine.h"

using namespace raftkv;

namespace {

Command Req(uint64_t client, uint64_t seq) {
  Command c{CommandType::kPut, "k", "v"};
  c.client_id = client;
  c.seq = seq;
  return c;
}

class MapStore : public KVStore {
 public:
  Status Put(const std::string& k, const std::string& v) override {
    data[k] = v;
    ++writes;
    return Status::kOk;
  }
  Status Get(const std::string& k, std::string* v) override {
    auto it = data.find(k);
    if (it == data.end()) return Status::kNotFound;
    *v = it->second;
    return Status::kOk;
  }
  Status Delete(const std::string& k) override {
    data.erase(k);
    ++writes;
    return Status::kOk;
  }
  std::map<std::string, std::string> data;
  int writes = 0;
};

}  // namespace

TEST(SessionExpiry, NothingIsForgottenWithinTheLimits) {
  ClientSessionTable t;  // the defaults
  for (uint64_t c = 1; c <= 1000; ++c) EXPECT_EQ(t.Decide(Req(c, 1), c), SessionDecision::kApply);
  EXPECT_EQ(t.ClientCount(), 1000u);
  EXPECT_FALSE(t.ForgottenAny());
  // Before anything has been forgotten, a new client may start at any seq, exactly as before.
  EXPECT_EQ(t.Decide(Req(5000, 7), 1001), SessionDecision::kApply);
}

TEST(SessionExpiry, AClientIdleForTooManyEntriesIsForgottenAndAnActiveOneIsNot) {
  ClientSessionTable t(SessionLimits{1000, 10});
  ASSERT_EQ(t.Decide(Req(1, 1), 1), SessionDecision::kApply);  // goes idle
  ASSERT_EQ(t.Decide(Req(2, 1), 2), SessionDecision::kApply);  // stays active
  for (LogIndex i = 3; i <= 11; ++i) ASSERT_EQ(t.Decide(Req(2, i), i), SessionDecision::kApply);
  EXPECT_EQ(t.LastSeq(1), 1u) << "10 entries idle is still within the limit";
  ASSERT_EQ(t.Decide(Req(2, 12), 12), SessionDecision::kApply);
  EXPECT_EQ(t.LastSeq(1), 0u) << "11 entries idle: forgotten";
  EXPECT_EQ(t.LastSeq(2), 12u);
  EXPECT_TRUE(t.ForgottenAny());
}

// A client that is only retrying (its request keeps timing out) is still active: forgetting it
// mid-retry would turn its next retry into a refused request.
TEST(SessionExpiry, RetriesKeepASessionAlive) {
  ClientSessionTable t(SessionLimits{1000, 10});
  ASSERT_EQ(t.Decide(Req(1, 1), 1), SessionDecision::kApply);
  for (LogIndex i = 5; i <= 41; i += 4) {
    ASSERT_EQ(t.Decide(Req(1, 1), i), SessionDecision::kDuplicate) << "the same request, retried";
    t.Decide(Req(2, i), i + 1);  // other traffic in between
  }
  EXPECT_EQ(t.LastSeq(1), 1u) << "a client whose retries keep arriving must not be forgotten";
}

TEST(SessionExpiry, AtCapacityTheLeastRecentlyActiveSessionIsForgotten) {
  ClientSessionTable t(SessionLimits{3, 1000000});
  t.Decide(Req(1, 1), 1);
  t.Decide(Req(2, 1), 2);
  t.Decide(Req(3, 1), 3);
  t.Decide(Req(1, 2), 4);  // client 1 is active again, so client 2 is now the oldest
  t.Decide(Req(4, 1), 5);
  EXPECT_EQ(t.ClientCount(), 3u);
  EXPECT_EQ(t.LastSeq(2), 0u) << "the least recently active session goes";
  EXPECT_EQ(t.LastSeq(1), 2u);
  EXPECT_EQ(t.LastSeq(3), 1u);
  EXPECT_EQ(t.LastSeq(4), 1u);
}

// The safety rule: after a forget, an unknown client's request is applied only if it is its first.
TEST(SessionExpiry, AfterAForgetARetryFromAForgottenClientIsRefusedNotApplied) {
  ClientSessionTable t(SessionLimits{2, 1000000});
  t.Decide(Req(1, 1), 1);
  t.Decide(Req(1, 2), 2);  // client 1's request 2 was applied...
  t.Decide(Req(2, 1), 3);
  t.Decide(Req(3, 1), 4);  // ...and then client 1's session was forgotten
  ASSERT_EQ(t.LastSeq(1), 0u);
  EXPECT_EQ(t.Decide(Req(1, 2), 5), SessionDecision::kSessionExpired) << "a retry of request 2 must not apply again";
  EXPECT_EQ(t.Decide(Req(1, 3), 6), SessionDecision::kSessionExpired) << "nor may a later request";
  EXPECT_EQ(t.LastSeq(1), 0u) << "a refused request does not create a session";
  EXPECT_EQ(t.Decide(Req(9, 1), 7), SessionDecision::kApply) << "a genuinely new client starts at 1";
  EXPECT_EQ(t.Decide(Req(9, 2), 8), SessionDecision::kApply);
}

// Replicas reach the same table either by applying everything or by restoring a snapshot taken
// part-way and applying the rest. Their decisions and their bytes must be identical.
TEST(SessionExpiry, DecisionsAreTheSameAfterASnapshotAsWithout) {
  std::mt19937_64 g(42);
  std::vector<Command> cmds;
  std::map<uint64_t, uint64_t> next_seq;
  for (int i = 0; i < 2000; ++i) {
    const uint64_t c = 1 + g() % 60;
    const uint64_t seq = (g() % 5 == 0 && next_seq[c] > 0) ? next_seq[c] : ++next_seq[c];  // some retries
    cmds.push_back(Req(c, seq));
  }
  const SessionLimits limits{20, 150};
  ClientSessionTable full(limits);
  std::vector<SessionDecision> expected;
  for (size_t i = 0; i < cmds.size(); ++i) expected.push_back(full.Decide(cmds[i], i + 1));

  ClientSessionTable first_half(limits);
  for (size_t i = 0; i < 1000; ++i) first_half.Decide(cmds[i], i + 1);
  ClientSessionTable restored(limits);
  ASSERT_TRUE(restored.Restore(first_half.Serialize()));
  for (size_t i = 1000; i < cmds.size(); ++i) {
    ASSERT_EQ(restored.Decide(cmds[i], i + 1), expected[i]) << "decision " << i << " differs after a snapshot";
  }
  EXPECT_EQ(restored.Serialize(), full.Serialize());
  EXPECT_TRUE(full.ForgottenAny()) << "the test must actually exercise forgetting";
}

TEST(SessionExpiry, TheSnapshotFormatRoundTripsAndTheOldFormatStillLoads) {
  ClientSessionTable a(SessionLimits{2, 1000000});
  a.Decide(Req(1, 4), 10);
  a.Decide(Req(2, 1), 11);
  a.Decide(Req(3, 1), 12);  // forgets client 1
  ClientSessionTable b;
  ASSERT_TRUE(b.Restore(a.Serialize()));
  EXPECT_EQ(b.Serialize(), a.Serialize());
  EXPECT_TRUE(b.ForgottenAny()) << "the 'something was forgotten' flag is replicated state";

  std::string old_format;  // [u64 count] then (client, last_seq) pairs, as written before
  auto put = [&](uint64_t v) { for (int i = 0; i < 8; ++i) old_format.push_back(static_cast<char>((v >> (8 * i)) & 0xFF)); };
  put(2); put(7); put(5); put(9); put(1);
  ClientSessionTable c;
  ASSERT_TRUE(c.Restore(old_format));
  EXPECT_EQ(c.LastSeq(7), 5u);
  EXPECT_EQ(c.LastSeq(9), 1u);
  EXPECT_FALSE(c.ForgottenAny());

  const std::string good = a.Serialize();
  for (size_t len = 0; len < good.size(); ++len) {
    ClientSessionTable d;
    EXPECT_FALSE(d.Restore(good.substr(0, len))) << "truncated to " << len;
  }
  std::string bad_flag = good;
  bad_flag[8] = 7;
  ClientSessionTable e;
  EXPECT_FALSE(e.Restore(bad_flag));
}

TEST(SessionExpiry, TheStateMachineRefusesARequestFromAForgottenSession) {
  MapStore store;
  KVStateMachine m(&store);
  m.SetSessionLimits(SessionLimits{2, 1000000});
  auto put = [&](LogIndex index, uint64_t client, uint64_t seq, const std::string& value) {
    Command c{CommandType::kPut, "k" + std::to_string(client), value};
    c.client_id = client;
    c.seq = seq;
    m.Apply(LogEntry{1, index, c});
  };
  put(1, 1, 1, "a");
  put(2, 1, 2, "b");
  put(3, 2, 1, "x");
  put(4, 3, 1, "y");  // client 1 forgotten
  const int writes_before = store.writes;
  put(5, 1, 2, "RETRY");  // client 1 retries request 2
  EXPECT_EQ(store.writes, writes_before) << "the retry must not reach the store";
  EXPECT_EQ(store.data["k1"], "b");
  EXPECT_EQ(m.LookupRequestOutcome(5), KVStateMachine::RequestOutcome::kSessionExpired);
  EXPECT_EQ(m.SessionsExpired(), 1u);
}
