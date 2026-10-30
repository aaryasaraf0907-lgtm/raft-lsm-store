// The leader's fsync overlaps the followers' (Raft thesis 10.2.1), and the leader still counts
// itself toward a commit majority only once its own copy is on disk.
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "raft/file_raft_storage.h"
#include "test_cluster.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {

// Real files (unsynced, so fast), with every fsync replaced by a sleep of `sync_ms`: a slow disk
// whose speed the test controls, independent of the machine.
class SlowSyncStorage : public RaftStorage {
 public:
  SlowSyncStorage(const std::string& dir, int sync_ms) : inner_(dir, /*sync=*/false), sync_ms_(sync_ms) {}
  void SetSyncMs(int ms) { sync_ms_ = ms; }

  bool Load(PersistentState* out) override { return inner_.Load(out); }
  bool SaveHardState(Term term, NodeId voted_for) override { return inner_.SaveHardState(term, voted_for); }
  bool AppendEntries(const std::vector<LogEntry>& entries) override {  // write + fsync
    const bool ok = inner_.AppendEntries(entries);
    std::this_thread::sleep_for(std::chrono::milliseconds(sync_ms_.load()));
    return ok;
  }
  bool AppendEntriesNoSync(const std::vector<LogEntry>& entries) override { return inner_.AppendEntries(entries); }
  std::function<bool()> PrepareSync() override {
    const int ms = sync_ms_.load();
    return [ms] {
      std::this_thread::sleep_for(std::chrono::milliseconds(ms));
      return true;
    };
  }
  bool TruncateSuffix(LogIndex from_index) override { return inner_.TruncateSuffix(from_index); }
  using RaftStorage::SaveSnapshot;
  bool SaveSnapshot(LogIndex index, Term term, const std::string& data, LogIndex base_index, Term base_term,
                    const std::vector<LogEntry>& entries_after_base) override {
    return inner_.SaveSnapshot(index, term, data, base_index, base_term, entries_after_base);
  }

 private:
  FileRaftStorage inner_;
  std::atomic<int> sync_ms_;
};

struct SlowDisks {
  TempDir dir;
  std::vector<SlowSyncStorage*> by_id = std::vector<SlowSyncStorage*>(32, nullptr);
  std::function<std::unique_ptr<RaftStorage>(NodeId)> Factory(int sync_ms) {
    return [this, sync_ms](NodeId id) {
      auto s = std::make_unique<SlowSyncStorage>((dir.path / ("node" + std::to_string(id))).string(), sync_ms);
      by_id[id] = s.get();
      return std::unique_ptr<RaftStorage>(std::move(s));
    };
  }
};

NodeId WaitForLeader(Cluster& c) {
  NodeId leader = 0;
  WaitFor([&] { return (leader = c.FindLeader()) != 0; }, 10000ms);
  return leader;
}

}  // namespace

// Every fsync takes 150 ms. Two nodes, so a commit needs both copies on disk. Before, a write waited
// for the leader's fsync and THEN the follower's: about 300 ms. With the leader's fsync running while
// the follower writes its own, about 150 ms. (Two nodes, not three: the simulated network delivers
// on one thread, so with two followers their simulated disk writes would run one after the other,
// which a real network does not do.)
TEST(FsyncOverlap, AWriteWaitsForOneFsyncNotTwo) {
  SlowDisks disks;
  Cluster c(2, false, nullptr, disks.Factory(150));
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  ASSERT_EQ(c.Node(leader)->ProposeAndWait(Put("warm-up"), 5000ms).result, CommitResult::kApplied);
  std::vector<double> ms;
  for (int i = 0; i < 7; ++i) {
    const auto start = std::chrono::steady_clock::now();
    ASSERT_EQ(c.Node(leader)->ProposeAndWait(Put("k" + std::to_string(i)), 5000ms).result, CommitResult::kApplied);
    ms.push_back(std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count());
  }
  std::sort(ms.begin(), ms.end());
  std::printf("[ fsync overlap ] median write latency %.0f ms with 150 ms fsyncs (sequential would be ~300)\n", ms[3]);
  EXPECT_LT(ms[3], 230.0) << "the leader's fsync did not overlap the follower's";
}

// The safety rule. With one follower cut off, the leader and the other follower are exactly a
// majority. The follower's disk is fast, the leader's takes 800 ms: the entry must NOT commit
// before the leader's own fsync is done, and must commit after it.
TEST(FsyncOverlap, TheLeaderDoesNotCountItselfBeforeItsOwnFsync) {
  SlowDisks disks;
  Cluster c(3, false, nullptr, disks.Factory(0));
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  ASSERT_EQ(c.Node(leader)->ProposeAndWait(Put("warm-up"), 5000ms).result, CommitResult::kApplied);
  const NodeId cut = leader % 3 + 1;
  c.net.Disconnect(cut);
  disks.by_id[leader]->SetSyncMs(800);

  const LogIndex before = c.Node(leader)->CommitIndex();
  ProposeResponse p;
  std::thread proposer([&] { p = c.Node(leader)->Propose(Put("x")); });  // returns after the leader's fsync
  std::this_thread::sleep_for(300ms);  // the fast follower has long acknowledged it
  EXPECT_EQ(c.Node(leader)->CommitIndex(), before) << "committed before the leader's own copy was on disk";
  proposer.join();
  ASSERT_EQ(p.result, ProposeResult::kAccepted);
  EXPECT_TRUE(WaitFor([&] { return c.Node(leader)->CommitIndex() >= p.index; }, 3000ms))
      << "never committed after the leader's fsync finished";
}

// With the overlap, durability must survive crashes as before: every acknowledged write is still
// there after every node restarts (the restart re-loads the logs the storages kept).
TEST(FsyncOverlap, AcknowledgedWritesSurviveAFullRestart) {
  SlowDisks disks;
  Cluster c(3, false, nullptr, disks.Factory(20));
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  for (int i = 0; i < 20; ++i) {
    ASSERT_EQ(c.Node(leader)->ProposeAndWait(Put("k" + std::to_string(i)), 5000ms).result, CommitResult::kApplied);
  }
  c.RestartAll();
  const NodeId again = WaitForLeader(c);
  ASSERT_NE(again, 0u);
  ASSERT_EQ(c.Node(again)->ProposeAndWait(Put("after"), 5000ms).result, CommitResult::kApplied);
  for (NodeId id = 1; id <= 3; ++id) {
    EXPECT_TRUE(WaitFor([&] {
      const auto keys = c.Keys(id);
      return std::find(keys.begin(), keys.end(), "k19") != keys.end() &&
             std::find(keys.begin(), keys.end(), "after") != keys.end();
    }, 5000ms)) << "node " << id;
  }
}

// The real file storage syncs through a dup() of its descriptor, outside any lock. A compaction
// that replaces the log file between PrepareSync and the sync must not break it, and what was
// written must load back.
TEST(FsyncOverlap, TheFileStorageSyncsThroughADuplicateDescriptor) {
  TempDir dir;
  const std::string path = (dir.path / "node").string();
  {
    FileRaftStorage storage(path, /*sync=*/true);
    ASSERT_TRUE(storage.ok());
    std::vector<LogEntry> entries;
    for (LogIndex i = 1; i <= 5; ++i) entries.push_back(LogEntry{1, i, Put("k" + std::to_string(i))});
    ASSERT_TRUE(storage.AppendEntriesNoSync(entries));
    std::function<bool()> sync = storage.PrepareSync();
    ASSERT_TRUE(storage.SaveSnapshot(3, 1, "state", {entries[3], entries[4]}));  // replaces raft.log
    EXPECT_TRUE(sync()) << "a sync prepared before the file was replaced must still succeed";
    ASSERT_TRUE(storage.AppendEntriesNoSync({LogEntry{1, 6, Put("k6")}}));
    EXPECT_TRUE(storage.PrepareSync()());
  }
  FileRaftStorage reopened(path, /*sync=*/true);
  PersistentState st;
  ASSERT_TRUE(reopened.Load(&st));
  ASSERT_EQ(st.log.size(), 3u);
  EXPECT_EQ(st.log.front().index, 4u);
  EXPECT_EQ(st.log.back().index, 6u);
  EXPECT_EQ(st.snapshot_index, 3u);
}
