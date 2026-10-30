#include "storage/block_cache.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "storage/sstable.h"

using raftkv::BlockCache;
using raftkv::Entry;
using raftkv::SSTable;
using raftkv::SSTableBuilder;
using raftkv::Status;
using R = raftkv::SSTable::GetResult;

namespace {

BlockCache::Block MakeBlock(const std::string& key, size_t value_bytes) {
  return std::make_shared<const std::vector<Entry>>(
      std::vector<Entry>{Entry{key, std::string(value_bytes, 'v'), false}});
}

// ---------------------------------------------------------------------------------------------
// The cache on its own

TEST(BlockCache, AMissThenAHitAfterInsert) {
  BlockCache cache(1 << 20);
  EXPECT_EQ(cache.Lookup(1, 0), nullptr);
  auto b = MakeBlock("k", 10);
  cache.Insert(1, 0, b);
  EXPECT_EQ(cache.Lookup(1, 0), b);
  EXPECT_EQ(cache.Lookup(1, 1), nullptr);
  EXPECT_EQ(cache.Lookup(2, 0), nullptr);
  EXPECT_EQ(cache.size(), 1u);
  EXPECT_EQ(cache.usage(), BlockCache::Charge(*b));
}

TEST(BlockCache, EvictsTheLeastRecentlyUsedBlockFirst) {
  auto a = MakeBlock("a", 100), b = MakeBlock("b", 100), c = MakeBlock("c", 100);
  const size_t charge = BlockCache::Charge(*a);
  BlockCache cache(2 * charge);  // room for exactly two
  cache.Insert(1, 0, a);
  cache.Insert(1, 1, b);
  ASSERT_EQ(cache.Lookup(1, 0), a);  // a is now more recent than b
  cache.Insert(1, 2, c);
  EXPECT_EQ(cache.Lookup(1, 1), nullptr) << "b was the least recently used";
  EXPECT_EQ(cache.Lookup(1, 0), a);
  EXPECT_EQ(cache.Lookup(1, 2), c);
  EXPECT_EQ(cache.usage(), 2 * charge);
}

TEST(BlockCache, UsageNeverExceedsTheCapacity) {
  BlockCache cache(10000);
  for (uint64_t i = 0; i < 1000; ++i) {
    cache.Insert(i % 7, i, MakeBlock("k" + std::to_string(i), i % 300));
    ASSERT_LE(cache.usage(), cache.capacity()) << i;
  }
  EXPECT_GT(cache.size(), 0u);
}

TEST(BlockCache, CapacityZeroCachesNothing) {
  BlockCache cache(0);
  cache.Insert(1, 0, MakeBlock("k", 1));
  EXPECT_EQ(cache.Lookup(1, 0), nullptr);
  EXPECT_EQ(cache.size(), 0u);
  EXPECT_EQ(cache.usage(), 0u);
}

TEST(BlockCache, ABlockLargerThanTheCapacityIsNotCachedAndEvictsNothing) {
  auto small = MakeBlock("s", 10);
  BlockCache cache(BlockCache::Charge(*small) + 100);
  cache.Insert(1, 0, small);
  cache.Insert(1, 1, MakeBlock("big", 1000));
  EXPECT_EQ(cache.Lookup(1, 1), nullptr);
  EXPECT_EQ(cache.Lookup(1, 0), small);
}

TEST(BlockCache, ASecondInsertOfTheSameBlockKeepsTheFirst) {
  BlockCache cache(1 << 20);
  auto first = MakeBlock("k", 10);
  cache.Insert(1, 0, first);
  cache.Insert(1, 0, MakeBlock("k", 10));
  EXPECT_EQ(cache.Lookup(1, 0), first);
  EXPECT_EQ(cache.usage(), BlockCache::Charge(*first));
}

TEST(BlockCache, AnEvictedBlockStaysValidForAReaderHoldingIt) {
  auto a = MakeBlock("a", 100);
  BlockCache cache(BlockCache::Charge(*a));
  cache.Insert(1, 0, a);
  BlockCache::Block held = cache.Lookup(1, 0);
  a.reset();
  cache.Insert(1, 1, MakeBlock("b", 100));  // evicts block 0
  ASSERT_EQ(cache.Lookup(1, 0), nullptr);
  ASSERT_NE(held, nullptr);
  EXPECT_EQ((*held)[0].key, "a");
  EXPECT_EQ((*held)[0].value, std::string(100, 'v'));
}

TEST(BlockCache, EraseTableDropsOnlyThatTable) {
  BlockCache cache(1 << 20);
  for (uint64_t b = 0; b < 4; ++b) {
    cache.Insert(1, b, MakeBlock("x", 10));
    cache.Insert(2, b, MakeBlock("y", 10));
  }
  cache.EraseTable(1, 4);
  for (uint64_t b = 0; b < 4; ++b) {
    EXPECT_EQ(cache.Lookup(1, b), nullptr);
    EXPECT_NE(cache.Lookup(2, b), nullptr);
  }
  EXPECT_EQ(cache.size(), 4u);
}

TEST(BlockCache, TableIdsAreNeverReused) {
  const uint64_t a = BlockCache::NewTableId();
  const uint64_t b = BlockCache::NewTableId();
  EXPECT_NE(a, b);
}

// ---------------------------------------------------------------------------------------------
// SSTables reading through a cache

class SSTableCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string tmpl = (std::filesystem::temp_directory_path() / "raftkv_cache_XXXXXX").string();
    ASSERT_NE(::mkdtemp(&tmpl[0]), nullptr);
    dir_ = tmpl;
  }
  void TearDown() override { std::filesystem::remove_all(dir_); }

  // n entries "key%06d" (even numbers), values distinguishable by `tag`; every 5th a tombstone.
  static std::vector<Entry> Entries(int n, const std::string& tag) {
    std::vector<Entry> v;
    char buf[32];
    for (int i = 0; i < n; ++i) {
      std::snprintf(buf, sizeof(buf), "key%06d", i * 2);
      v.push_back({buf, tag + std::to_string(i) + std::string(static_cast<size_t>(i % 40), 'x'), i % 5 == 0});
      if (v.back().tombstone) v.back().value.clear();
    }
    return v;
  }
  std::string Write(const std::string& subdir, const std::vector<Entry>& entries) {
    std::filesystem::create_directories(dir_ + "/" + subdir);
    const std::string path = dir_ + "/" + subdir + "/000001.sst";
    std::unique_ptr<SSTableBuilder> b;
    EXPECT_EQ(SSTableBuilder::Create(path, {1, 1, 1}, {}, &b), Status::kOk);
    for (const auto& e : entries) EXPECT_EQ(b->Add(e), Status::kOk);
    EXPECT_EQ(b->Finish(), Status::kOk);
    return path;
  }
  static std::unique_ptr<SSTable> Open(const std::string& path, std::shared_ptr<BlockCache> cache) {
    std::unique_ptr<SSTable> t;
    EXPECT_EQ(SSTable::Open(path, 1, &t, std::move(cache)), Status::kOk);
    return t;
  }
  // Flips one byte in the middle of the data blocks, in place (open readers see the change).
  static void DamageDataBlocks(const std::string& path, size_t at) {
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    f.seekg(static_cast<std::streamoff>(at));
    char c = 0;
    f.read(&c, 1);
    c = static_cast<char>(c ^ 1);
    f.seekp(static_cast<std::streamoff>(at));
    f.write(&c, 1);
  }
  static void ExpectSameAnswers(const SSTable& t, const std::vector<Entry>& entries) {
    for (size_t i = 0; i < entries.size(); ++i) {
      R r = R::kAbsent;
      std::string v;
      ASSERT_EQ(t.Get(entries[i].key, &r, &v), Status::kOk);
      if (entries[i].tombstone) {
        EXPECT_EQ(r, R::kDeleted) << entries[i].key;
      } else {
        EXPECT_EQ(r, R::kFound) << entries[i].key;
        EXPECT_EQ(v, entries[i].value) << entries[i].key;
      }
      char odd[32];
      std::snprintf(odd, sizeof(odd), "key%06d", static_cast<int>(i) * 2 + 1);
      ASSERT_EQ(t.Get(odd, &r, &v), Status::kOk);
      EXPECT_EQ(r, R::kAbsent) << odd;
    }
  }

  std::string dir_;
};

TEST_F(SSTableCacheTest, CachedReadsGiveTheSameAnswersAsUncachedOnes) {
  const auto entries = Entries(3000, "v");
  const std::string path = Write("t", entries);
  auto cache = std::make_shared<BlockCache>(1 << 20);
  auto t = Open(path, cache);
  ExpectSameAnswers(*t, entries);  // misses: every block read once
  EXPECT_GT(cache->size(), 0u);
  ExpectSameAnswers(*t, entries);  // hits
  auto small = std::make_shared<BlockCache>(8 * 1024);  // constant eviction
  auto t2 = Open(path, small);
  ExpectSameAnswers(*t2, entries);
  EXPECT_LE(small->usage(), small->capacity());
}

TEST_F(SSTableCacheTest, AHitServesTheValidatedBlockWithoutReadingTheDisk) {
  const auto entries = Entries(10, "v");  // one block
  const std::string path = Write("t", entries);
  auto cache = std::make_shared<BlockCache>(1 << 20);
  auto t = Open(path, cache);
  R r;
  std::string v;
  ASSERT_EQ(t->Get("key000002", &r, &v), Status::kOk);
  ASSERT_EQ(cache->size(), 1u);
  DamageDataBlocks(path, 20);  // the file changes underneath the cached block
  ASSERT_EQ(t->Get("key000002", &r, &v), Status::kOk) << "a hit does not read the disk";
  EXPECT_EQ(v, entries[1].value);
  // Read from disk again (no cache, or the block evicted): the damage is reported.
  auto uncached = Open(path, nullptr);
  EXPECT_EQ(uncached->Get("key000002", &r, &v), Status::kCorruption);
  auto other = Open(path, std::make_shared<BlockCache>(1 << 20));
  EXPECT_EQ(other->Get("key000002", &r, &v), Status::kCorruption);
}

TEST_F(SSTableCacheTest, ABlockThatFailedValidationIsNeverCached) {
  const auto entries = Entries(10, "v");
  const std::string path = Write("t", entries);
  DamageDataBlocks(path, 20);
  auto cache = std::make_shared<BlockCache>(1 << 20);
  auto t = Open(path, cache);
  R r;
  std::string v;
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(t->Get("key000002", &r, &v), Status::kCorruption) << i;
    auto it = t->NewIterator("key000002");
    EXPECT_FALSE(it->Valid());
    EXPECT_EQ(it->status(), Status::kCorruption) << i;
  }
  EXPECT_EQ(cache->size(), 0u);
}

TEST_F(SSTableCacheTest, TablesWithTheSameFileNumberNeverShareBlocks) {
  // Same file number, same keys, different values, one cache: identity is per SSTable object.
  const auto a = Entries(500, "a");
  const auto b = Entries(500, "b");
  auto cache = std::make_shared<BlockCache>(1 << 20);
  auto ta = Open(Write("a", a), cache);
  auto tb = Open(Write("b", b), cache);
  ExpectSameAnswers(*ta, a);
  ExpectSameAnswers(*tb, b);
  ExpectSameAnswers(*ta, a);
  // A reopened table is a new identity too: it never sees the old object's blocks.
  const size_t cached = cache->size();
  ta.reset();
  EXPECT_LT(cache->size(), cached) << "destroying a table erases its blocks";
  auto ta2 = Open(dir_ + "/a/000001.sst", cache);
  ExpectSameAnswers(*ta2, a);
}

TEST_F(SSTableCacheTest, DestroyingATableErasesItsBlocks) {
  const std::string path = Write("t", Entries(3000, "v"));
  auto cache = std::make_shared<BlockCache>(1 << 20);
  auto t = Open(path, cache);
  for (auto it = t->NewIterator(""); it->Valid(); it->Next()) {
  }
  EXPECT_EQ(cache->size(), t->block_count());
  t.reset();
  EXPECT_EQ(cache->size(), 0u);
  EXPECT_EQ(cache->usage(), 0u);
}

TEST_F(SSTableCacheTest, FullScansBypassTheCacheAndRangeReadsUseIt) {
  const auto entries = Entries(3000, "v");
  auto cache = std::make_shared<BlockCache>(1 << 20);
  auto t = Open(Write("t", entries), cache);
  size_t n = 0;
  auto full = t->NewIterator();
  for (; full->Valid(); full->Next()) ++n;
  EXPECT_EQ(full->status(), Status::kOk);
  EXPECT_EQ(n, entries.size());
  EXPECT_EQ(cache->size(), 0u) << "compaction and checkpoint scans must not fill the cache";
  auto range = t->NewIterator(entries[100].key);
  ASSERT_TRUE(range->Valid());
  EXPECT_EQ(range->Current().key, entries[100].key);
  EXPECT_EQ(cache->size(), 1u);
}

TEST_F(SSTableCacheTest, AnIteratorKeepsItsBlockWhileTheCacheEvictsIt) {
  const auto entries = Entries(3000, "v");
  auto cache = std::make_shared<BlockCache>(8 * 1024);  // about one block
  auto t = Open(Write("t", entries), cache);
  auto it = t->NewIterator("");
  ASSERT_TRUE(it->Valid());
  ExpectSameAnswers(*t, entries);  // reads every block: the iterator's block is evicted
  std::vector<std::string> keys;
  for (; it->Valid(); it->Next()) keys.push_back(it->Current().key);
  EXPECT_EQ(it->status(), Status::kOk);
  ASSERT_EQ(keys.size(), entries.size());
  for (size_t i = 0; i < keys.size(); ++i) EXPECT_EQ(keys[i], entries[i].key);
}

TEST_F(SSTableCacheTest, ConcurrentReadersShareOneCache) {
  const auto entries = Entries(3000, "v");
  auto cache = std::make_shared<BlockCache>(16 * 1024);  // small: lookups, inserts and evictions race
  auto t = Open(Write("t", entries), cache);
  std::vector<int> ok(4, 0);
  std::vector<std::thread> threads;
  for (int w = 0; w < 4; ++w) {
    threads.emplace_back([&, w] {
      bool good = true;
      for (int round = 0; round < 3 && good; ++round) {
        for (size_t i = static_cast<size_t>(w); i < entries.size() && good; i += 3) {
          R r = R::kAbsent;
          std::string v;
          good = t->Get(entries[i].key, &r, &v) == Status::kOk &&
                 r == (entries[i].tombstone ? R::kDeleted : R::kFound) &&
                 (entries[i].tombstone || v == entries[i].value);
        }
        size_t n = 0;
        auto it = t->NewIterator(entries[static_cast<size_t>(w) * 500].key);
        for (; it->Valid(); it->Next()) good = good && it->Current().key == entries[static_cast<size_t>(w) * 500 + n++].key;
        good = good && it->status() == Status::kOk && n == entries.size() - static_cast<size_t>(w) * 500;
      }
      ok[static_cast<size_t>(w)] = good;
    });
  }
  for (auto& th : threads) th.join();
  for (int v : ok) EXPECT_EQ(v, 1);
  EXPECT_LE(cache->usage(), cache->capacity());
}

}  // namespace
