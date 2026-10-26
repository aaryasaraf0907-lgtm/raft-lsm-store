// Fault injection for the Raft layer: (1) the fault knobs of FakeNetwork themselves, (2) the
// checkers, and (3) randomized runs in which a seeded nemesis breaks the network and the nodes while
// clients write, and the safety properties must still hold.
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "fault_injection.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {

// Counts what reaches it and what its callers get back.
class CountingHandler : public RpcHandler {
 public:
  RequestVoteReply HandleRequestVote(const RequestVoteRequest&) override {
    ++calls;
    return RequestVoteReply{1, true};
  }
  AppendEntriesReply HandleAppendEntries(const AppendEntriesRequest& r) override {
    ++calls;
    std::lock_guard<std::mutex> lk(mu);
    order.push_back(r.prev_log_index);
    return AppendEntriesReply{1, true, r.prev_log_index};
  }
  std::atomic<int> calls{0};
  std::mutex mu;
  std::vector<uint64_t> order;  // prev_log_index of each AppendEntries, in arrival order
};

// Two nodes (1 and 2) on a fake network; node 2 has a CountingHandler.
struct TwoNodes {
  FakeNetwork net;
  std::unique_ptr<FakeTransport> t1 = net.MakeTransport(1), t2 = net.MakeTransport(2);
  CountingHandler h1, h2;
  std::atomic<int> replies{0};
  TwoNodes() {
    t1->SetHandler(&h1);
    t2->SetHandler(&h2);
  }
  ~TwoNodes() {
    t1->Stop();
    t2->Stop();
  }
  void Vote(FakeTransport* from, NodeId to) {
    from->SendRequestVote(to, RequestVoteRequest{1, 1, 0, 0}, [this](const RequestVoteReply&) { ++replies; });
  }
  void Append(FakeTransport* from, NodeId to, uint64_t tag) {
    from->SendAppendEntries(to, AppendEntriesRequest{1, 1, tag, 0, {}, 0}, [this](const AppendEntriesReply&) { ++replies; });
  }
};

}  // namespace

// ---- the network's fault knobs ----

TEST(FakeNetworkFaults, NothingIsInjectedByDefaultAndMessagesStayInOrder) {
  TwoNodes n;
  for (uint64_t i = 0; i < 100; ++i) n.Append(n.t1.get(), 2, i);
  ASSERT_TRUE(WaitFor([&] { return n.replies == 100; }, 5000ms));
  std::lock_guard<std::mutex> lk(n.h2.mu);
  for (uint64_t i = 0; i < 100; ++i) EXPECT_EQ(n.h2.order[i], i) << "messages were reordered without any fault configured";
  EXPECT_EQ(n.net.DroppedByFaults(), 0u);
}

TEST(FakeNetworkFaults, LostRequestsNeverArrive) {
  TwoNodes n;
  FaultPlan plan;
  plan.drop_request = 1.0;
  n.net.SetFaults(plan);
  for (int i = 0; i < 50; ++i) n.Vote(n.t1.get(), 2);
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(n.h2.calls, 0);
  EXPECT_EQ(n.replies, 0);
  EXPECT_EQ(n.net.DroppedByFaults(), 50u);
}

TEST(FakeNetworkFaults, LostRepliesStillRunTheHandler) {
  TwoNodes n;
  FaultPlan plan;
  plan.drop_reply = 1.0;
  n.net.SetFaults(plan);
  for (int i = 0; i < 50; ++i) n.Vote(n.t1.get(), 2);
  ASSERT_TRUE(WaitFor([&] { return n.h2.calls == 50; }, 5000ms));
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(n.replies, 0) << "a reply got through although every reply was dropped";
}

TEST(FakeNetworkFaults, DuplicatedRequestsRunTheHandlerTwiceButTheSenderGetsOneReply) {
  TwoNodes n;
  FaultPlan plan;
  plan.duplicate = 1.0;
  n.net.SetFaults(plan);
  for (int i = 0; i < 20; ++i) n.Vote(n.t1.get(), 2);
  ASSERT_TRUE(WaitFor([&] { return n.h2.calls == 40; }, 5000ms));
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(n.replies, 20) << "the Transport contract: on_reply is called at most once";
  EXPECT_EQ(n.net.DuplicatedByFaults(), 20u);
}

TEST(FakeNetworkFaults, FixedDelayDelaysRequestAndReply) {
  TwoNodes n;
  FaultPlan plan;
  plan.min_delay_ms = plan.max_delay_ms = 60;
  n.net.SetFaults(plan);
  const auto start = std::chrono::steady_clock::now();
  n.Vote(n.t1.get(), 2);
  ASSERT_TRUE(WaitFor([&] { return n.replies == 1; }, 5000ms));
  EXPECT_GE(std::chrono::steady_clock::now() - start, 110ms) << "request and reply should each take about 60 ms";
}

TEST(FakeNetworkFaults, RandomDelayReordersMessages) {
  TwoNodes n;
  n.net.SetFaultSeed(5);
  FaultPlan plan;
  plan.max_delay_ms = 25;
  n.net.SetFaults(plan);
  for (uint64_t i = 0; i < 60; ++i) n.Append(n.t1.get(), 2, i);
  ASSERT_TRUE(WaitFor([&] { return n.replies == 60; }, 5000ms));
  std::lock_guard<std::mutex> lk(n.h2.mu);
  bool sorted = true;
  for (size_t i = 1; i < n.h2.order.size(); ++i) sorted = sorted && n.h2.order[i - 1] < n.h2.order[i];
  EXPECT_FALSE(sorted) << "60 messages with random delays arrived in perfect order";
}

TEST(FakeNetworkFaults, OneWayCutLosesOnlyOneDirection) {
  TwoNodes n;
  n.net.BlockLink(1, 2);
  for (int i = 0; i < 10; ++i) n.Vote(n.t1.get(), 2);  // 1 -> 2: the request is lost
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(n.h2.calls, 0);
  EXPECT_EQ(n.replies, 0);
  for (int i = 0; i < 10; ++i) n.Vote(n.t2.get(), 1);  // 2 -> 1 gets through ...
  ASSERT_TRUE(WaitFor([&] { return n.h1.calls == 10; }, 5000ms));
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(n.replies, 0) << "... but the replies travel 1 -> 2, which is cut";
  n.net.UnblockLink(1, 2);
  n.Vote(n.t1.get(), 2);
  EXPECT_TRUE(WaitFor([&] { return n.replies == 1; }, 5000ms));
}

TEST(FakeNetworkFaults, HealAllRestoresEverything) {
  TwoNodes n;
  FaultPlan plan;
  plan.drop_request = 1.0;
  n.net.SetFaults(plan);
  n.net.Disconnect(2);
  n.net.BlockLink(1, 2);
  n.Vote(n.t1.get(), 2);
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(n.replies, 0);
  n.net.HealAll();
  n.Vote(n.t1.get(), 2);
  EXPECT_TRUE(WaitFor([&] { return n.replies == 1; }, 5000ms));
}

TEST(FakeNetworkFaults, TheSameSeedMakesTheSameDecisions) {
  auto run = [](uint64_t seed) {
    TwoNodes n;
    n.net.SetFaultSeed(seed);
    FaultPlan plan;
    plan.drop_request = 0.5;  // only requests, sent from one thread, consume the generator
    n.net.SetFaults(plan);
    for (uint64_t i = 0; i < 60; ++i) n.Append(n.t1.get(), 2, i);
    EXPECT_TRUE(WaitFor([&] { return n.h2.calls + n.net.DroppedByFaults() == 60; }, 5000ms));
    std::lock_guard<std::mutex> lk(n.h2.mu);
    return n.h2.order;
  };
  EXPECT_EQ(run(11), run(11));
  EXPECT_NE(run(11), run(12));
}

// ---- the checkers must be able to fail ----

TEST(FaultInjectionCheckers, ElectionSafetyCheckerSeesTwoLeadersInOneTerm) {
  ElectionSafetyChecker c;
  c.Observe(1, 3, Role::kLeader);
  c.Observe(2, 3, Role::kFollower);
  c.Observe(2, 4, Role::kLeader);  // a different term: fine
  c.Observe(1, 3, Role::kLeader);  // the same leader again: fine
  EXPECT_FALSE(c.Violated());
  c.Observe(3, 3, Role::kLeader);
  EXPECT_TRUE(c.Violated());
  EXPECT_NE(c.Report().find("term 3"), std::string::npos);
}

TEST(FaultInjectionCheckers, PrefixAgreementAcceptsPrefixesAndRejectsDivergence) {
  auto entry = [](LogIndex i, Term t, const std::string& k) { return LogEntry{t, i, Put(k)}; };
  std::vector<LogEntry> a = {entry(2, 1, "a"), entry(3, 1, "b"), entry(5, 2, "c")};
  std::vector<LogEntry> shorter = {entry(2, 1, "a"), entry(3, 1, "b")};
  EXPECT_EQ(CheckPrefixAgreement({a, shorter, {}}), "");

  std::vector<LogEntry> other_key = {entry(2, 1, "a"), entry(3, 1, "X")};
  EXPECT_NE(CheckPrefixAgreement({a, other_key}), "");
  std::vector<LogEntry> other_term = {entry(2, 1, "a"), entry(3, 9, "b")};
  EXPECT_NE(CheckPrefixAgreement({a, other_term}), "");
  std::vector<LogEntry> backwards = {entry(3, 1, "b"), entry(2, 1, "a")};
  EXPECT_NE(CheckPrefixAgreement({backwards}), "");
}

// ---- randomized runs ----

namespace {

// Runs one seeded scenario: clients write continuously for `duration` while the nemesis breaks
// things, then everything is repaired and the properties are checked. Returns "" if all held, else
// what went wrong plus the nemesis schedule that provoked it.
std::string RunScenario(int nodes, bool persistent, NemesisOptions options, int threads,
                        std::chrono::milliseconds duration = 2000ms,
                        std::function<void(RaftConfig&)> tweak = nullptr) {
  Cluster c(nodes, persistent, std::move(tweak));
  Gate gate;
  ElectionSafetyChecker election;
  Monitor monitor(c, gate, election);
  Writers writers(c, gate, threads, /*per_thread=*/0);
  Nemesis nemesis(c, gate, options);
  monitor.Start();
  nemesis.Start();
  writers.Start();
  std::this_thread::sleep_for(duration);
  nemesis.StopAndHeal();
  std::this_thread::sleep_for(300ms);  // some writes in a healthy cluster, too
  writers.Stop();
  writers.Join();
  const std::vector<std::string> acked = writers.Acked();
  std::string problem;
  if (acked.size() < 5) {
    problem = "the cluster made no progress: only " + std::to_string(acked.size()) + " writes were acknowledged";
  } else {
    problem = CheckOutcome(c, acked, election);
  }
  monitor.Stop();
  if (std::getenv("RAFTKV_FI_VERBOSE") != nullptr) {
    const std::string schedule = nemesis.Schedule();
    std::printf("[fault-injection] seed %llu: %zu writes acknowledged, %zu messages lost, %zu duplicated, %zu nemesis actions\n",
                static_cast<unsigned long long>(options.seed), acked.size(), c.net.DroppedByFaults(),
                c.net.DuplicatedByFaults(), static_cast<size_t>(std::count(schedule.begin(), schedule.end(), '\n')));
  }
  if (problem.empty() && election.Violated()) problem = election.Report();
  if (problem.empty()) return "";
  return problem + "\nnemesis schedule:\n" + nemesis.Schedule();
}

}  // namespace

// A network that loses, duplicates, delays and reorders messages, with no node failures.
TEST(RaftFaultInjection, LossyDuplicatingReorderingNetworkKeepsEveryProperty) {
  for (uint64_t seed : {1, 2, 3}) {
    NemesisOptions o;
    o.seed = seed;
    o.partitions = false;
    o.one_way = false;
    std::string problem = RunScenario(3, false, o, 2);
    EXPECT_EQ(problem, "") << "seed " << seed;
  }
}

// Whole nodes and single directions of single links keep failing; five nodes.
TEST(RaftFaultInjection, PartitionsAndOneWayLinkCutsKeepEveryProperty) {
  for (uint64_t seed : {1, 2, 3}) {
    NemesisOptions o;
    o.seed = seed;
    std::string problem = RunScenario(5, false, o, 2);
    EXPECT_EQ(problem, "") << "seed " << seed;
  }
}

// Nodes crash and reboot from their disk while the network misbehaves.
TEST(RaftFaultInjection, CrashesAndRebootsFromDiskKeepEveryProperty) {
  for (uint64_t seed : {1, 2, 3}) {
    NemesisOptions o;
    o.seed = seed;
    o.crashes = true;
    std::string problem = RunScenario(3, true, o, 2);
    EXPECT_EQ(problem, "") << "seed " << seed;
  }
}

// Elections that keep colliding: very short, narrow election timeouts so that several nodes become
// candidates at about the same moment, no PreVote to calm things down, and a network that delays
// messages unevenly. This is where double voting and unsafe commit rules show up: terms change
// constantly and leaders keep losing their position.
TEST(RaftFaultInjection, ContestedElectionsKeepEveryProperty) {
  for (uint64_t seed : {1, 2, 3}) {
    NemesisOptions o;
    o.seed = seed;
    o.max_delay_ms = 12;
    o.max_drop = 0.3;
    o.crashes = true;
    std::string problem = RunScenario(
        5, true, o, 2, 2500ms, [](RaftConfig& cfg) {
          cfg.enable_pre_vote = false;
          cfg.heartbeat_interval_ms = 8;
          cfg.election_timeout_min_ms = 20;
          cfg.election_timeout_max_ms = 35;
        });
    EXPECT_EQ(problem, "") << "seed " << seed;
  }
}
