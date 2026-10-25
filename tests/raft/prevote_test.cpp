// PreVote: a node that is cut off must not be able to disrupt the cluster when it comes back.
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "raft/file_raft_storage.h"
#include "raft/raft_node.h"
#include "test_cluster.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {

NodeId WaitForLeader(Cluster& c) {
  EXPECT_TRUE(WaitFor([&] { return c.FindLeader() != 0; }, 8000ms));
  return c.FindLeader();
}

struct IsolationResult {
  Term term_before = 0;
  Term isolated_term = 0;
  NodeId leader_before = 0;
  NodeId leader_after = 0;
  Term leader_term_after = 0;
};

// Cuts one follower off for ~10 election timeouts, then lets it back in.
IsolationResult RunIsolation(bool pre_vote) {
  IsolationResult r;
  Cluster c(3, false, [pre_vote](RaftConfig& cfg) { cfg.enable_pre_vote = pre_vote; });
  NodeId leader = WaitForLeader(c);
  NodeId follower = leader % 3 + 1;
  std::this_thread::sleep_for(300ms);  // let everyone settle on the leader's term
  r.leader_before = leader;
  r.term_before = c.Node(leader)->CurrentTerm();

  c.net.Disconnect(follower);
  std::this_thread::sleep_for(2000ms);
  r.isolated_term = c.Node(follower)->CurrentTerm();

  c.net.Reconnect(follower);
  std::this_thread::sleep_for(1500ms);
  r.leader_after = c.FindLeader();
  if (r.leader_after != 0) r.leader_term_after = c.Node(r.leader_after)->CurrentTerm();
  return r;
}

// A transport the test drives by hand. Whether it carries PreVote is configurable.
class ManualTransport : public Transport {
 public:
  explicit ManualTransport(bool supports_prevote) : supports_prevote_(supports_prevote) {}
  struct Vote {
    NodeId to;
    RequestVoteRequest req;
    std::function<void(const RequestVoteReply&)> cb;
  };
  struct PreVote {
    NodeId to;
    PreVoteRequest req;
    std::function<void(const PreVoteReply&)> cb;
  };
  void SendRequestVote(NodeId to, const RequestVoteRequest& req,
                       std::function<void(const RequestVoteReply&)> cb) override {
    std::lock_guard<std::mutex> lk(mu_);
    votes_.push_back({to, req, std::move(cb)});
  }
  void SendAppendEntries(NodeId, const AppendEntriesRequest&,
                         std::function<void(const AppendEntriesReply&)>) override {}
  bool SupportsPreVote() const override { return supports_prevote_; }
  void SendPreVote(NodeId to, const PreVoteRequest& req,
                   std::function<void(const PreVoteReply&)> cb) override {
    std::lock_guard<std::mutex> lk(mu_);
    prevotes_.push_back({to, req, std::move(cb)});
  }
  void SetHandler(RpcHandler*) override {}
  void Start() override {}
  void Stop() override {}
  std::vector<Vote> TakeVotes() {
    std::lock_guard<std::mutex> lk(mu_);
    return std::exchange(votes_, {});
  }
  std::vector<PreVote> TakePreVotes() {
    std::lock_guard<std::mutex> lk(mu_);
    return std::exchange(prevotes_, {});
  }

 private:
  bool supports_prevote_;
  std::mutex mu_;
  std::vector<Vote> votes_;
  std::vector<PreVote> prevotes_;
};

LogEntry Entry(LogIndex i, Term t) { return LogEntry{t, i, Put("k" + std::to_string(i))}; }

}  // namespace

// The scenario PreVote exists for. A follower is cut off for many election timeouts.
TEST(PreVote, IsolatedFollowerDoesNotInflateItsTermOrDisruptTheLeaderOnReturn) {
  IsolationResult r = RunIsolation(/*pre_vote=*/true);
  ASSERT_NE(r.leader_before, 0u);
  EXPECT_EQ(r.isolated_term, r.term_before) << "a cut-off node raised its term";
  EXPECT_EQ(r.leader_after, r.leader_before) << "the returning node deposed the leader";
  EXPECT_EQ(r.leader_term_after, r.term_before) << "the returning node forced a new election";
}

// Control: the same scenario without PreVote DOES disrupt the cluster, which shows that the test
// above can actually see the problem.
TEST(PreVote, WithoutPreVoteTheSameScenarioDisruptsTheCluster) {
  IsolationResult r = RunIsolation(/*pre_vote=*/false);
  ASSERT_NE(r.leader_before, 0u);
  EXPECT_GT(r.isolated_term, r.term_before) << "control: the cut-off node should have inflated its term";
  EXPECT_GT(r.leader_term_after, r.term_before) << "control: its return should have forced a new election";
}

TEST(PreVote, ElectionsStillWorkAfterALeaderCrash) {
  for (int n : {3, 5}) {
    SCOPED_TRACE("n=" + std::to_string(n));
    Cluster c(n);
    NodeId first = WaitForLeader(c);
    ASSERT_NE(first, 0u);
    ASSERT_EQ(c.Node(first)->ProposeAndWait(Put("a"), 5000ms).result, CommitResult::kApplied);
    c.Node(first)->Stop();
    ASSERT_TRUE(WaitFor([&] { return c.FindLeader({first}) != 0; }, 8000ms)) << "no new leader";
    NodeId second = c.FindLeader({first});
    ASSERT_EQ(c.Node(second)->ProposeAndWait(Put("b"), 5000ms).result, CommitResult::kApplied);
  }
}

// Answering a pre-vote must change nothing: no term, no recorded vote, nothing on disk.
TEST(PreVote, AnsweringAPreVoteChangesNoState) {
  TempDir dir;
  FileRaftStorage storage(dir.path.string(), false);
  ManualTransport net(true);
  RaftNodeImpl node(1, {2, 3}, &net, RaftConfig(), &storage);
  const auto size_before = std::filesystem::file_size(dir.path / "raft.log");

  PreVoteReply r = node.HandlePreVote(PreVoteRequest{5, 2, 0, 0});
  EXPECT_TRUE(r.granted);
  EXPECT_EQ(r.term, 0u);
  EXPECT_EQ(node.CurrentTerm(), 0u) << "a pre-vote raised the term";
  EXPECT_EQ(std::filesystem::file_size(dir.path / "raft.log"), size_before) << "a pre-vote wrote to disk";

  // It also recorded no vote: at that very term we are still free to vote for someone else.
  RequestVoteReply v = node.HandleRequestVote(RequestVoteRequest{5, 3, 0, 0});
  EXPECT_TRUE(v.vote_granted) << "a pre-vote cost us our real vote";
  EXPECT_EQ(node.CurrentTerm(), 5u);
}

// A node that heard from a leader within the minimum election timeout refuses to pre-vote.
TEST(PreVote, RefusesWhileALeaderWasHeardRecently) {
  ManualTransport net(true);
  RaftNodeImpl node(1, {2, 3}, &net);
  ASSERT_TRUE(node.HandleAppendEntries(AppendEntriesRequest{1, 2, 0, 0, {}, 0}).success);
  EXPECT_FALSE(node.HandlePreVote(PreVoteRequest{2, 3, 0, 0}).granted) << "granted right after a heartbeat";
  std::this_thread::sleep_for(250ms);  // longer than the minimum election timeout
  EXPECT_TRUE(node.HandlePreVote(PreVoteRequest{2, 3, 0, 0}).granted);
}

TEST(PreVote, RefusesOldTermsAndStaleLogs) {
  ManualTransport net(true);
  RaftNodeImpl node(1, {2, 3}, &net);
  // Leader 2 gives us entries at terms 1,1,2 and we are at term 2; wait out the leader's lease.
  ASSERT_TRUE(node.HandleAppendEntries(
                      AppendEntriesRequest{2, 2, 0, 0, {Entry(1, 1), Entry(2, 1), Entry(3, 2)}, 0}).success);
  std::this_thread::sleep_for(250ms);

  PreVoteReply old_term = node.HandlePreVote(PreVoteRequest{2, 3, 3, 2});  // proposes term 2: not above ours
  EXPECT_FALSE(old_term.granted);
  EXPECT_EQ(old_term.term, 2u);  // the candidate learns our term from the reply

  EXPECT_FALSE(node.HandlePreVote(PreVoteRequest{3, 3, 2, 2}).granted) << "shorter log";
  EXPECT_FALSE(node.HandlePreVote(PreVoteRequest{3, 3, 9, 1}).granted) << "older last term";
  EXPECT_TRUE(node.HandlePreVote(PreVoteRequest{3, 3, 3, 2}).granted) << "equally up to date";
  EXPECT_TRUE(node.HandlePreVote(PreVoteRequest{3, 3, 1, 3}).granted) << "newer last term";
}

// An election starts only after a majority said yes, and the term is untouched until then.
TEST(PreVote, RealElectionStartsOnlyAfterAMajorityGrantsAPreVote) {
  ManualTransport net(true);
  RaftNodeImpl node(1, {2, 3}, &net);
  node.Start();
  ASSERT_TRUE(WaitFor([&] { return !net.TakePreVotes().empty(); }, 5000ms)) << "no pre-vote was sent";
  EXPECT_EQ(node.CurrentTerm(), 0u) << "asking for pre-votes must not raise the term";
  EXPECT_TRUE(net.TakeVotes().empty());

  std::vector<ManualTransport::PreVote> round;  // wait for the next round (election timer retries)
  ASSERT_TRUE(WaitFor(
      [&] {
        round = net.TakePreVotes();
        return !round.empty();
      },
      5000ms));
  EXPECT_EQ(round[0].req.term, 1u);  // proposed term = current + 1
  for (auto& p : round) {            // everyone says no: still no election
    p.cb(PreVoteReply{0, false});
  }
  std::this_thread::sleep_for(100ms);
  EXPECT_TRUE(net.TakeVotes().empty()) << "an election started without a majority of pre-votes";
  EXPECT_EQ(node.CurrentTerm(), 0u);

  ASSERT_TRUE(WaitFor(
      [&] {
        round = net.TakePreVotes();
        return !round.empty();
      },
      5000ms));
  round[0].cb(PreVoteReply{0, true});  // one grant + ourselves = a majority of 3
  ASSERT_TRUE(WaitFor([&] { return node.CurrentTerm() == 1u; }, 2000ms));
  auto votes = net.TakeVotes();
  ASSERT_FALSE(votes.empty()) << "no RequestVote after winning the pre-vote";
  EXPECT_EQ(votes[0].req.term, 1u);
  node.Stop();
}

TEST(PreVote, ALaggingNodeAdoptsTheTermItLearnsFromAPreVoteReply) {
  ManualTransport net(true);
  RaftNodeImpl node(1, {2, 3}, &net);
  node.Start();
  std::vector<ManualTransport::PreVote> round;
  ASSERT_TRUE(WaitFor(
      [&] {
        round = net.TakePreVotes();
        return !round.empty();
      },
      5000ms));
  round[0].cb(PreVoteReply{7, false});  // a peer is at term 7
  ASSERT_TRUE(WaitFor([&] { return node.CurrentTerm() == 7u; }, 2000ms));
  EXPECT_EQ(node.GetRole(), Role::kFollower);
  node.Stop();
}

// A transport that cannot carry PreVote (the gRPC one, until it implements it) must still elect
// leaders: the node falls back to plain elections instead of waiting for pre-votes that never come.
TEST(PreVote, FallsBackToPlainElectionsOnATransportWithoutPreVote) {
  ManualTransport net(/*supports_prevote=*/false);
  RaftNodeImpl node(1, {2, 3}, &net);
  node.Start();
  std::vector<ManualTransport::Vote> votes;
  ASSERT_TRUE(WaitFor(
      [&] {
        votes = net.TakeVotes();
        return !votes.empty();
      },
      5000ms)) << "no RequestVote: the node waited for pre-votes its transport cannot send";
  EXPECT_EQ(votes[0].req.term, 1u);
  EXPECT_TRUE(net.TakePreVotes().empty());
  node.Stop();
}
