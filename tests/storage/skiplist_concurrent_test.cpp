#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "storage/memtable.h"
#include "storage/skiplist.h"

using raftkv::MemTable;
using raftkv::SkipList;

namespace {

std::string Key(int i) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "k%08d", i);
  return buf;
}

// Starts all workers at (nearly) the same instant to maximise contention.
template <typename Fn>
void RunThreads(int n, Fn fn) {
  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  for (int t = 0; t < n; ++t) {
    threads.emplace_back([&, t] {
      while (!go.load()) {
      }
      fn(t);
    });
  }
  go = true;
  for (auto& th : threads) th.join();
}

void ExpectSortedAndComplete(const SkipList& l, size_t expected) {
  SkipList::Iterator it(&l);
  size_t n = 0;
  std::string prev;
  for (it.SeekToFirst(); it.Valid(); it.Next(), ++n) {
    if (n > 0) { ASSERT_LT(prev, it.entry().key); }
    prev = it.entry().key;
  }
  EXPECT_EQ(n, expected);
}

}  // namespace

TEST(SkipListConcurrentTest, DisjointInsertsLoseNothing) {
  SkipList l;
  constexpr int kThreads = 8, kPer = 4000;
  RunThreads(kThreads, [&](int t) {
    for (int i = 0; i < kPer; ++i) l.Upsert(Key(t * kPer + i), Key(t * kPer + i), false);
  });
  EXPECT_EQ(l.Count(), static_cast<size_t>(kThreads * kPer));
  ExpectSortedAndComplete(l, kThreads * kPer);
  for (int i = 0; i < kThreads * kPer; ++i) {
    const raftkv::Entry* e = l.Find(Key(i));
    ASSERT_NE(e, nullptr) << i;
    ASSERT_EQ(e->value, Key(i));
  }
}

TEST(SkipListConcurrentTest, InterleavedKeysFromManyThreads) {
  SkipList l;
  constexpr int kThreads = 8, kTotal = 32000;
  RunThreads(kThreads, [&](int t) {
    for (int i = t; i < kTotal; i += kThreads) l.Upsert(Key(i), "v", false);  // neighbours differ per thread
  });
  EXPECT_EQ(l.Count(), static_cast<size_t>(kTotal));
  ExpectSortedAndComplete(l, kTotal);
}

TEST(SkipListConcurrentTest, ExactlyOneThreadCreatesEachKey) {
  SkipList l;
  constexpr int kThreads = 8, kKeys = 3000;
  std::atomic<int> created{0};
  RunThreads(kThreads, [&](int t) {
    for (int i = 0; i < kKeys; ++i) {
      if (l.Upsert(Key(i), "t" + std::to_string(t), false)) ++created;
    }
  });
  EXPECT_EQ(created.load(), kKeys);
  EXPECT_EQ(l.Count(), static_cast<size_t>(kKeys));
  ExpectSortedAndComplete(l, kKeys);
  for (int i = 0; i < kKeys; ++i) {
    const raftkv::Entry* e = l.Find(Key(i));
    ASSERT_NE(e, nullptr);
    ASSERT_EQ(e->value[0], 't');  // final value is one that some thread really wrote
  }
}

TEST(SkipListConcurrentTest, HotKeyKeepsAWrittenValue) {
  SkipList l;
  constexpr int kThreads = 8, kWrites = 2000;
  RunThreads(kThreads, [&](int t) {
    for (int i = 0; i < kWrites; ++i) l.Upsert("hot", std::to_string(t) + ":" + std::to_string(i), false);
  });
  EXPECT_EQ(l.Count(), 1u);
  const raftkv::Entry* e = l.Find("hot");
  ASSERT_NE(e, nullptr);
  const size_t colon = e->value.find(':');
  ASSERT_NE(colon, std::string::npos);
  EXPECT_LT(std::stoi(e->value.substr(colon + 1)), kWrites);
  EXPECT_LT(std::stoi(e->value.substr(0, colon)), kThreads);
  EXPECT_GE(l.ApproximateBytes(), static_cast<size_t>(kThreads * kWrites) * (3 + SkipList::kVersionOverhead));
}

TEST(SkipListConcurrentTest, ReadersAlwaysSeeSortedConsistentEntries) {
  SkipList l;
  constexpr int kWriters = 4, kPer = 5000;
  std::atomic<int> writers_done{0};
  std::atomic<bool> failure{false};
  std::vector<std::thread> readers;
  for (int r = 0; r < 3; ++r) {
    readers.emplace_back([&] {
      size_t last_seen = 0;
      while (writers_done.load() < kWriters) {
        SkipList::Iterator it(&l);
        std::string prev;
        size_t n = 0;
        for (it.SeekToFirst(); it.Valid(); it.Next(), ++n) {
          const raftkv::Entry& e = it.entry();
          if ((n > 0 && !(prev < e.key)) || e.value != e.key) failure = true;  // unsorted or torn entry
          prev = e.key;
        }
        if (n < last_seen) failure = true;  // keys never disappear
        last_seen = n;
      }
    });
  }
  RunThreads(kWriters, [&](int t) {
    for (int i = 0; i < kPer; ++i) l.Upsert(Key(i * kWriters + t), Key(i * kWriters + t), false);
    ++writers_done;
  });
  for (auto& th : readers) th.join();
  EXPECT_FALSE(failure.load());
  EXPECT_EQ(l.Count(), static_cast<size_t>(kWriters * kPer));
}

TEST(SkipListConcurrentTest, CompletedInsertIsVisibleToOtherThreads) {
  SkipList l;
  constexpr int kN = 20000;
  std::atomic<int> published{-1};
  std::atomic<bool> failure{false};
  std::thread reader([&] {
    int checked = -1;
    while (checked < kN - 1) {
      int upto = published.load();  // everything <= upto finished before this load
      for (; checked < upto; ++checked) {
        if (l.Find(Key(checked + 1)) == nullptr) failure = true;
      }
    }
  });
  for (int i = 0; i < kN; ++i) {
    l.Upsert(Key(i), "v", false);
    published.store(i);
  }
  reader.join();
  EXPECT_FALSE(failure.load());
}

TEST(MemTableConcurrentTest, MixedPutDeleteGetFromManyThreads) {
  MemTable m;
  constexpr int kThreads = 6, kKeys = 3000;
  std::atomic<bool> stop{false};
  std::atomic<bool> failure{false};
  std::thread reader([&] {
    std::string v;
    int i = 0;
    while (!stop.load()) {
      if (m.Get(Key(i++ % kKeys), &v) == MemTable::GetResult::kFound && v != "v1" && v != "v2") failure = true;
    }
  });
  RunThreads(kThreads, [&](int t) {
    for (int i = t; i < kKeys; i += kThreads) {
      m.Put(Key(i), "v1");
      m.Put(Key(i), "v2");
      if (i % 3 == 0) m.Delete(Key(i));
    }
  });
  stop = true;
  reader.join();
  EXPECT_FALSE(failure.load());
  EXPECT_EQ(m.Count(), static_cast<size_t>(kKeys));
  for (int i = 0; i < kKeys; ++i) {
    std::string v;
    auto r = m.Get(Key(i), &v);
    if (i % 3 == 0) {
      ASSERT_EQ(r, MemTable::GetResult::kDeleted) << i;
    } else {
      ASSERT_EQ(r, MemTable::GetResult::kFound) << i;
      ASSERT_EQ(v, "v2");
    }
  }
}