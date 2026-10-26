#include <gtest/gtest.h>

#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "checker/linearizability.h"

using namespace raftkv::lin;

namespace {

Operation Put(int client, const std::string& key, const std::string& value, int64_t invoke, int64_t ret, bool unknown = false) {
  Operation o;
  o.client = client;
  o.type = OpType::kPut;
  o.key = key;
  o.value = value;
  o.invoke = invoke;
  o.ret = ret;
  o.unknown = unknown;
  return o;
}
Operation Del(int client, const std::string& key, int64_t invoke, int64_t ret) {
  Operation o;
  o.client = client;
  o.type = OpType::kDelete;
  o.key = key;
  o.invoke = invoke;
  o.ret = ret;
  return o;
}
Operation Get(int client, const std::string& key, bool found, const std::string& value, int64_t invoke, int64_t ret) {
  Operation o;
  o.client = client;
  o.type = OpType::kGet;
  o.key = key;
  o.found = found;
  o.value = value;
  o.invoke = invoke;
  o.ret = ret;
  return o;
}

}  // namespace

TEST(Linearizability, ASequentialHistoryIsLinearizableIfEachReadSeesTheLatestWrite) {
  EXPECT_TRUE(Check({Get(1, "k", false, "", 0, 1), Put(1, "k", "a", 2, 3), Get(1, "k", true, "a", 4, 5), Put(1, "k", "b", 6, 7),
                     Get(1, "k", true, "b", 8, 9), Del(1, "k", 10, 11), Get(1, "k", false, "", 12, 13)}).ok);
  EXPECT_TRUE(Check({}).ok);
}

TEST(Linearizability, AStaleReadAfterANewerWriteFinishedIsRejected) {
  CheckResult r = Check({Put(1, "k", "a", 0, 1), Put(2, "k", "b", 2, 3), Get(3, "k", true, "a", 4, 5)});
  EXPECT_FALSE(r.ok);
  EXPECT_EQ(r.key, "k");
  EXPECT_NE(r.explanation.find("not linearizable"), std::string::npos);
}

TEST(Linearizability, ReadingADeletedValueAfterTheDeleteFinishedIsRejected) {
  EXPECT_FALSE(Check({Put(1, "k", "a", 0, 1), Del(1, "k", 2, 3), Get(2, "k", true, "a", 4, 5)}).ok);
  EXPECT_FALSE(Check({Put(1, "k", "a", 0, 1), Get(2, "k", false, "", 2, 3)}).ok) << "a finished write must be visible";
}

TEST(Linearizability, ReadingAValueThatWasNeverWrittenIsRejected) {
  EXPECT_FALSE(Check({Put(1, "k", "a", 0, 1), Get(2, "k", true, "ghost", 2, 3)}).ok);
}

TEST(Linearizability, OverlappingOperationsMayTakeEffectInEitherOrder) {
  // The read overlaps the write: seeing the old value or the new one are both fine.
  EXPECT_TRUE(Check({Put(1, "k", "a", 0, 1), Put(2, "k", "b", 10, 30), Get(3, "k", true, "a", 15, 20)}).ok);
  EXPECT_TRUE(Check({Put(1, "k", "a", 0, 1), Put(2, "k", "b", 10, 30), Get(3, "k", true, "b", 15, 20)}).ok);
  // ...but two reads that do not overlap each other cannot see the new value and then the old one.
  EXPECT_FALSE(Check({Put(1, "k", "a", 0, 1), Put(2, "k", "b", 10, 50), Get(3, "k", true, "b", 15, 20), Get(3, "k", true, "a", 25, 30)}).ok);
}

TEST(Linearizability, AWriteWhoseOutcomeIsUnknownMayOrMayNotHaveHappened) {
  const Operation maybe = Put(2, "k", "b", 10, 0, /*unknown=*/true);
  EXPECT_TRUE(Check({Put(1, "k", "a", 0, 1), maybe, Get(3, "k", true, "a", 40, 41)}).ok) << "it did not take effect";
  EXPECT_TRUE(Check({Put(1, "k", "a", 0, 1), maybe, Get(3, "k", true, "b", 40, 41)}).ok) << "it took effect";
  EXPECT_TRUE(Check({Put(1, "k", "a", 0, 1), maybe, Get(3, "k", true, "a", 40, 41), Get(3, "k", true, "b", 50, 51)}).ok)
      << "it took effect between the two reads";
  // It cannot take effect, then un-happen: b, then a again.
  EXPECT_FALSE(Check({Put(1, "k", "a", 0, 1), maybe, Get(3, "k", true, "b", 40, 41), Get(3, "k", true, "a", 50, 51)}).ok);
  // And it cannot take effect BEFORE it started.
  EXPECT_FALSE(Check({Put(1, "k", "a", 0, 1), Get(3, "k", true, "b", 2, 3), Put(2, "k", "b", 10, 0, true)}).ok);
}

TEST(Linearizability, KeysAreIndependentAndTheFailingKeyIsNamed) {
  CheckResult r = Check({Put(1, "good", "x", 0, 1), Get(1, "good", true, "x", 2, 3), Put(1, "bad", "a", 4, 5),
                         Put(1, "bad", "b", 6, 7), Get(2, "bad", true, "a", 8, 9)});
  EXPECT_FALSE(r.ok);
  EXPECT_EQ(r.key, "bad");
}

TEST(Linearizability, AnEmptyValueIsNotTheSameAsAbsent) {
  EXPECT_TRUE(Check({Put(1, "k", "", 0, 1), Get(1, "k", true, "", 2, 3)}).ok);
  EXPECT_FALSE(Check({Put(1, "k", "", 0, 1), Get(1, "k", false, "", 2, 3)}).ok);
  EXPECT_FALSE(Check({Get(1, "k", true, "", 0, 1)}).ok);
}

TEST(Linearizability, HistoriesRoundTripThroughJsonLines) {
  std::vector<Operation> h = {Put(1, std::string("k\0y", 3), std::string("\xff\x01", 2), 5, 9), Get(2, "k", true, "v", 10, 12),
                              Del(3, "k", 13, 14), Put(4, "k", "z", 15, 0, true)};
  std::vector<Operation> back;
  ASSERT_TRUE(FromJsonLines(ToJsonLines(h), &back));
  ASSERT_EQ(back.size(), h.size());
  for (size_t i = 0; i < h.size(); ++i) {
    EXPECT_EQ(back[i].client, h[i].client);
    EXPECT_EQ(back[i].type, h[i].type);
    EXPECT_EQ(back[i].key, h[i].key);
    EXPECT_EQ(back[i].value, h[i].value);
    EXPECT_EQ(back[i].found, h[i].found);
    EXPECT_EQ(back[i].invoke, h[i].invoke);
    EXPECT_EQ(back[i].unknown, h[i].unknown);
    if (!h[i].unknown) {
      EXPECT_EQ(back[i].ret, h[i].ret);
    }
  }
  EXPECT_FALSE(FromJsonLines("{\"client\":1}\n", &back));
  EXPECT_FALSE(FromJsonLines("{\"client\":1,\"op\":\"put\",\"key\":\"zz\",\"value\":\"\",\"found\":false,\"invoke\":1,\"ret\":2,\"unknown\":false}\n", &back));
}

TEST(Linearizability, TheRecorderTurnsUnfinishedWritesIntoUnknownAndDropsUnfinishedReads) {
  HistoryRecorder rec;
  size_t a = rec.Begin(1, OpType::kPut, "k", "v1");
  rec.EndWriteOk(a);
  size_t b = rec.Begin(2, OpType::kPut, "k", "v2");  // never ends: unknown
  size_t c = rec.Begin(3, OpType::kGet, "k");         // never ends: no information
  size_t d = rec.Begin(4, OpType::kGet, "k");
  rec.EndGet(d, true, "v2");
  size_t e = rec.Begin(5, OpType::kPut, "k", "bad");
  rec.Drop(e);
  (void)b; (void)c;
  std::vector<Operation> h = rec.Snapshot();
  ASSERT_EQ(h.size(), 3u);
  EXPECT_TRUE(h[1].unknown);
  EXPECT_TRUE(Check(h).ok) << "v2 may have taken effect";
}

// Histories that are linearizable BY CONSTRUCTION must always be accepted. Each operation takes
// effect at its own instant p_i (in order), and its interval is a random window around that instant.
TEST(Linearizability, RandomHistoriesBuiltToBeLinearizableAreAlwaysAccepted) {
  std::mt19937_64 g(2026);
  for (int trial = 0; trial < 300; ++trial) {
    std::map<std::string, std::pair<bool, std::string>> reg;  // the real register state, key -> (present, value)
    std::vector<Operation> h;
    const int n = 20 + static_cast<int>(g() % 50);
    for (int i = 0; i < n; ++i) {
      const std::string key = "k" + std::to_string(g() % 3);
      const int64_t p = static_cast<int64_t>(i) * 10;
      Operation o;
      o.client = static_cast<int>(g() % 5);
      o.key = key;
      o.invoke = p - static_cast<int64_t>(g() % 60);
      o.ret = p + 1 + static_cast<int64_t>(g() % 60);
      switch (g() % 3) {
        case 0: {
          o.type = OpType::kPut;
          o.value = "v" + std::to_string(i);
          const bool takes_effect = g() % 8 != 0;  // some "unknown" writes never took effect at all
          o.unknown = g() % 6 == 0;
          if (takes_effect || !o.unknown) reg[key] = {true, o.value};
          break;
        }
        case 1:
          o.type = OpType::kDelete;
          reg[key] = {false, ""};
          break;
        default:
          o.type = OpType::kGet;
          o.found = reg[key].first;
          o.value = reg[key].second;
          break;
      }
      h.push_back(o);
    }
    CheckResult r = Check(h);
    ASSERT_TRUE(r.ok) << "trial " << trial << " was linearizable by construction:\n" << r.explanation;
  }
}

// A linearizable history with a violation added at the end must always be rejected: a final write
// finishes, and afterwards a read still returns an older value.
TEST(Linearizability, ACorruptedHistoryIsAlwaysRejected) {
  std::mt19937_64 g(99);
  for (int trial = 0; trial < 200; ++trial) {
    std::vector<Operation> h;
    std::vector<std::string> written;
    for (int i = 0; i < 25; ++i) {
      const int64_t p = static_cast<int64_t>(i) * 10;
      Operation o = Put(i % 4, "k", "v" + std::to_string(i), p - static_cast<int64_t>(g() % 40), p + 1 + static_cast<int64_t>(g() % 40));
      written.push_back(o.value);
      h.push_back(o);
    }
    h.push_back(Put(1, "k", "FINAL", 1000, 1001));
    const std::string stale = written[g() % written.size()];
    h.push_back(Get(2, "k", true, stale, 1002, 1003));
    EXPECT_FALSE(Check(h).ok) << "trial " << trial << ": a stale read after the final write slipped through";

    h.pop_back();  // and the same history with the correct read is fine
    h.push_back(Get(2, "k", true, "FINAL", 1002, 1003));
    EXPECT_TRUE(Check(h).ok) << "trial " << trial;
  }
}

TEST(Linearizability, ManyOperationsWithClientSizedConcurrencyAreCheckedQuickly) {
  // 8 clients, 40 operations each on one key. Each client runs one operation at a time, so at most 8
  // operations overlap: windows around each operation's instant are kept to about that width.
  std::mt19937_64 g(5);
  std::vector<Operation> h;
  std::string current;
  bool present = false;
  for (int i = 0; i < 320; ++i) {
    const int64_t p = static_cast<int64_t>(i) * 10;
    Operation o;
    o.client = i % 8;
    o.key = "hot";
    o.invoke = p - static_cast<int64_t>(g() % 20);
    o.ret = p + 1 + static_cast<int64_t>(g() % 20);
    if (g() % 2) {
      o.type = OpType::kPut;
      o.value = "v" + std::to_string(i);
      current = o.value;
      present = true;
    } else {
      o.type = OpType::kGet;
      o.found = present;
      o.value = current;
    }
    h.push_back(o);
  }
  const auto start = std::chrono::steady_clock::now();
  CheckResult r = Check(h);
  EXPECT_TRUE(r.ok) << r.explanation;
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(5));
}

// A history with far more overlap than any client count could produce must come back as "undecided"
// within the budget, not hang, and must not be reported as a violation.
TEST(Linearizability, AnExplosivelyConcurrentHistoryGivesUpInsteadOfHanging) {
  std::vector<Operation> h;
  for (int i = 0; i < 60; ++i) {  // sixty writes and reads that all overlap each other
    Operation o = i % 2 ? Put(i, "k", "v" + std::to_string(i), 0, 1000) : Get(i, "k", true, "v" + std::to_string(i + 1), 0, 1000);
    h.push_back(o);
  }
  h.push_back(Get(99, "k", true, "ghost", 2000, 2001));  // makes the search exhaust every ordering
  const auto start = std::chrono::steady_clock::now();
  CheckResult r = Check(h, /*max_steps=*/200000);
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(20));
  EXPECT_FALSE(r.ok);
  if (r.gave_up) {
    EXPECT_NE(r.explanation.find("undecided"), std::string::npos);
  }
}
