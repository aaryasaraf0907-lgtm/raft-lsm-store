#include "storage/skiplist.h"

#include <gtest/gtest.h>

#include <map>
#include <random>
#include <string>
#include <vector>

using raftkv::SkipList;

TEST(SkipListTest, EmptyList) {
  SkipList l;
  EXPECT_EQ(l.Count(), 0u);
  EXPECT_EQ(l.ApproximateBytes(), 0u);
  EXPECT_EQ(l.Find("a"), nullptr);
  SkipList::Iterator it(&l);
  it.SeekToFirst();
  EXPECT_FALSE(it.Valid());
  it.Seek("x");
  EXPECT_FALSE(it.Valid());
}

TEST(SkipListTest, InsertAndFind) {
  SkipList l;
  EXPECT_TRUE(l.Upsert("b", "2", false));
  EXPECT_TRUE(l.Upsert("a", "1", false));
  ASSERT_NE(l.Find("a"), nullptr);
  EXPECT_EQ(l.Find("a")->value, "1");
  EXPECT_EQ(l.Find("b")->value, "2");
  EXPECT_EQ(l.Find("c"), nullptr);
  EXPECT_EQ(l.Count(), 2u);
}

TEST(SkipListTest, DuplicateKeyOverwritesInPlace) {
  SkipList l;
  EXPECT_TRUE(l.Upsert("k", "v1", false));
  EXPECT_FALSE(l.Upsert("k", "v2", false));
  EXPECT_EQ(l.Count(), 1u);
  EXPECT_EQ(l.Find("k")->value, "v2");
}

TEST(SkipListTest, EmptyKeyAndEmptyValue) {
  SkipList l;
  l.Upsert("", "", false);
  ASSERT_NE(l.Find(""), nullptr);
  EXPECT_EQ(l.Find("")->value, "");
}

TEST(SkipListTest, IteratesInSortedOrder) {
  SkipList l;
  std::vector<std::string> keys = {"m", "c", "z", "a", "q", "b", "y"};
  for (auto& k : keys) l.Upsert(k, k, false);
  SkipList::Iterator it(&l);
  std::string prev;
  size_t n = 0;
  for (it.SeekToFirst(); it.Valid(); it.Next(), ++n) {
    if (n > 0) { EXPECT_LT(prev, it.entry().key); }
    prev = it.entry().key;
  }
  EXPECT_EQ(n, keys.size());
}

TEST(SkipListTest, SeekFindsFirstKeyAtOrAfterTarget) {
  SkipList l;
  for (std::string k : {"b", "d", "f"}) l.Upsert(k, "", false);
  SkipList::Iterator it(&l);
  it.Seek("c");
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.entry().key, "d");
  it.Seek("d");
  EXPECT_EQ(it.entry().key, "d");
  it.Seek("a");
  EXPECT_EQ(it.entry().key, "b");
  it.Seek("g");
  EXPECT_FALSE(it.Valid());
}

TEST(SkipListTest, OverwriteAddsOneVersionToSize) {
  SkipList l;
  l.Upsert("k", std::string(100, 'x'), false);
  size_t before = l.ApproximateBytes();
  l.Upsert("k", std::string(10, 'x'), false);  // old version is kept, so size only grows
  EXPECT_EQ(l.ApproximateBytes(), before + 1 + 10 + SkipList::kVersionOverhead);
}

TEST(SkipListTest, MatchesStdMapOnRandomWorkload) {
  SkipList l(42);
  std::map<std::string, std::string> ref;
  std::mt19937 rng(7);
  for (int i = 0; i < 20000; ++i) {
    std::string k = "key" + std::to_string(rng() % 3000);
    std::string v = std::to_string(rng());
    l.Upsert(k, v, false);
    ref[k] = v;
  }
  EXPECT_EQ(l.Count(), ref.size());
  SkipList::Iterator it(&l);
  it.SeekToFirst();
  for (auto& [k, v] : ref) {
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(it.entry().key, k);
    EXPECT_EQ(it.entry().value, v);
    it.Next();
  }
  EXPECT_FALSE(it.Valid());
}