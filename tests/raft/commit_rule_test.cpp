// The commit rule of Raft paper section 5.4.2 / Figure 8: a leader may only commit an entry from an
// EARLIER term by committing an entry from its OWN term on top of it. Counting replicas of the old
// entry is not enough: that old entry could still be overwritten by a later leader even though it is
// stored on a majority, which would lose a "committed" entry. Random fault injection almost never
// produces the exact interleaving, so this test scripts it.
#include <gtest/gtest.h>

#include <functional>
#include <mutex>
#include <vector>

#include "raft/file_raft_storage.h"
#include "raft/raft_node.h"
#include "test_cluster.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {

class ScriptedTransport : public Transport {
 public:
  struct Vote {
    NodeId to;
    RequestVoteRequest req;
    std::function<void(const RequestVoteReply&)> cb;
  };
  struct Append {
    NodeId to;
    AppendEntriesRequest req;
    std::function<void(const AppendEntriesReply&)> cb;
  };
  void SendRequestVote(NodeId to, const RequestVoteRequest& req,
                       std::function<void(const RequestVoteReply&)> cb) override {
    std::lock_guard<std::mutex> lk(mu_);
    votes_.push_back({to, req, std::move(cb)});
  }
  void SendAppendEntries(NodeId to, const AppendEntriesRequest& req,
                         std::function<void(const AppendEntriesReply&)> cb) override {
    std::lock_guard<std::mutex> lk(mu_);
    appends_.push_back({to, req, std::move(cb)});
  }
  void SetHandler(RpcHandler*) override {}
  void Start() override {}
  void Stop() override {}
  std::vector<Vote> TakeVotes() {
    std::lock_guard<std::mutex> lk(mu_);
    return std::exchange(votes_, {});
  }
  std::vector<Append> TakeAppends() {
    std::lock_guard<std::mutex> lk(mu_);
    return std::exchange(appends_, {});
  }

 private:
  std::mutex mu_;
  std::vector<Vote> votes_;
  std::vector<Append> appends_;
};

}  // namespace

// Five nodes, we are node 1. Our log holds entry 1 (term 1) and entry 2 (term 2, the "old" entry
// from a previous leader of ours); the current term on disk is 3. We win the election for term 4
// and append our own no-op as entry 3. Nodes 2 and 3 now acknowledge ONLY the old entry 2: it is
// stored on a majority (us, 2 and 3), but it must NOT be committed yet.
TEST(CommitRule, AnOldTermEntryOnAMajorityIsNotCommittedUntilAnEntryOfTheLeadersOwnTermIs) {
  TempDir dir;
  FileRaftStorage storage(dir.path.string(), false);
  {
    PersistentState ps;
    ASSERT_TRUE(storage.Load(&ps));
    ASSERT_TRUE(storage.AppendEntries({LogEntry{1, 1, Put("first")}, LogEntry{2, 2, Put("old")}}));
    ASSERT_TRUE(storage.SaveHardState(/*term=*/3, /*voted_for=*/0));
  }
  ScriptedTransport net;
  RaftConfig cfg;
  cfg.max_entries_per_append = 1;  // one entry per message, so an acknowledgment covers exactly one
  RaftNodeImpl node(1, {2, 3, 4, 5}, &net, cfg, &storage);
  node.SetApplyCallback([](const LogEntry&) {});
  node.Start();

  // Win the election for term 4 with the votes of nodes 2 and 3.
  ASSERT_TRUE(WaitFor(
      [&] {
        bool asked = false;
        for (auto& v : net.TakeVotes()) {
          asked = true;
          EXPECT_EQ(v.req.term, 4u);
          if (v.to == 2 || v.to == 3) v.cb(RequestVoteReply{4, true});
        }
        return asked;
      },
      5000ms));
  ASSERT_TRUE(WaitFor([&] { return node.GetRole() == Role::kLeader; }, 2000ms));

  // Answer everything addressed to nodes 2 and 3 by the script. Phase 1: make them acknowledge the
  // old entry (index 2) and hold back everything about our own entry (index 3).
  std::vector<ScriptedTransport::Append> held;  // messages about entry 3, answered in phase 2
  bool node2_acked = false, node3_acked = false;
  auto pump = [&](bool phase_two) {
    for (auto& a : net.TakeAppends()) {
      if (a.to != 2 && a.to != 3) continue;
      const auto& r = a.req;
      bool& acked_old = a.to == 2 ? node2_acked : node3_acked;
      if (r.prev_log_index == 3) {  // a heartbeat: they have nothing, tell us where they are (index 1)
        a.cb(AppendEntriesReply{4, false, 1});
      } else if (r.prev_log_index == 2 && r.entries.size() == 1 && r.entries[0].index == 3) {
        if (!acked_old) {
          a.cb(AppendEntriesReply{4, false, 1});  // "I only have entry 1": the leader backs up to entry 2
        } else if (phase_two) {
          a.cb(AppendEntriesReply{4, true, 3});
        } else {
          held.push_back(std::move(a));  // about OUR entry: not answered yet
        }
      } else if (r.prev_log_index == 1 && r.entries.size() == 1 && r.entries[0].index == 2) {
        a.cb(AppendEntriesReply{4, true, 2});  // they stored the old entry
        acked_old = true;
      }
    }
  };
  ASSERT_TRUE(WaitFor(
      [&] {
        pump(false);
        return node2_acked && node3_acked;
      },
      5000ms)) << "nodes 2 and 3 never got to acknowledge the old entry";
  std::this_thread::sleep_for(200ms);
  pump(false);

  EXPECT_LT(node.CommitIndex(), 2u)
      << "the old-term entry was committed just because a majority stores it (Raft paper Figure 8)";

  // Phase 2: they now acknowledge our OWN entry 3. Committing it commits entry 2 beneath it.
  for (auto& a : held) a.cb(AppendEntriesReply{4, true, 3});
  held.clear();
  ASSERT_TRUE(WaitFor(
      [&] {
        pump(true);
        return node.CommitIndex() == 3;
      },
      5000ms)) << "the leader's own entry never committed (commit index " << node.CommitIndex() << ")";
  node.Stop();
}
