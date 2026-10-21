#include "storage/memtable.h"

#include <gtest/gtest.h>

#include <string>

using raftkv::MemTable;
using R = raftkv::MemTable::GetResult;

TEST(MemTableTest, EmptyStore) {
  MemTable m;
  std::string v;
  EXPECT_EQ(m.Get("x", &v), R::kNotFound);
  EXPECT_TRUE(m.Empty());
  EXPECT_FALSE(m.ShouldFlush());
  auto it = m.NewIterator();
  EXPECT_FALSE(it.Valid());
}

TEST(MemTableTest, PutThenGet) {
  MemTable m;
  m.Put("k", "v");
  std::string v;
  EXPECT_EQ(m.Get("k", &v), R::kFound);
  EXPECT_EQ(v, "v");
}

TEST(MemTableTest, PutOverwritesDuplicateKey) {
  MemTable m;
  m.Put("k", "v1");
  m.Put("k", "v2");
  std::string v;
  EXPECT_EQ(m.Get("k", &v), R::kFound);
  EXPECT_EQ(v, "v2");
  EXPECT_EQ(m.Count(), 1u);
}

TEST(MemTableTest, DeleteLeavesTombstone) {
  MemTable m;
  m.Put("k", "v");
  m.Delete("k");
  std::string v;
  EXPECT_EQ(m.Get("k", &v), R::kDeleted);
  EXPECT_EQ(m.Count(), 1u);  // key is never removed
  auto it = m.NewIterator();
  ASSERT_TRUE(it.Valid());
  EXPECT_TRUE(it.Current().tombstone);
}

TEST(MemTableTest, DeleteOfAbsentKeyStillWritesTombstone) {
  MemTable m;  // the key may live in an older SSTable, so the tombstone is required
  m.Delete("ghost");
  std::string v;
  EXPECT_EQ(m.Get("ghost", &v), R::kDeleted);
}

TEST(MemTableTest, PutAfterDeleteResurrectsKey) {
  MemTable m;
  m.Put("k", "a");
  m.Delete("k");
  m.Put("k", "b");
  std::string v;
  EXPECT_EQ(m.Get("k", &v), R::kFound);
  EXPECT_EQ(v, "b");
  auto it = m.NewIterator();
  EXPECT_FALSE(it.Current().tombstone);
}

TEST(MemTableTest, IteratorIsSortedAndIncludesTombstones) {
  MemTable m;
  m.Put("c", "3");
  m.Put("a", "1");
  m.Put("b", "2");
  m.Delete("b");
  auto it = m.NewIterator();
  std::string order;
  for (; it.Valid(); it.Next()) order += it.Current().key;
  EXPECT_EQ(order, "abc");
}

TEST(MemTableTest, ShouldFlushOnceThresholdReached) {
  MemTable m(1024);
  EXPECT_FALSE(m.ShouldFlush());
  for (int i = 0; i < 100 && !m.ShouldFlush(); ++i) m.Put("key" + std::to_string(i), std::string(64, 'x'));
  EXPECT_TRUE(m.ShouldFlush());
  EXPECT_GE(m.ApproximateBytes(), 1024u);
}

TEST(MemTableTest, BinaryKeysAndValuesWithNulBytes) {
  MemTable m;
  std::string k("a\0b", 3), val("\0\0x", 3);
  m.Put(k, val);
  std::string out;
  EXPECT_EQ(m.Get(k, &out), R::kFound);
  EXPECT_EQ(out, val);
}

TEST(MemTableTest, IteratorWorksThroughKVIteratorInterface) {
  MemTable m;
  m.Put("b", "2");
  m.Delete("a");
  m.Put("c", "3");
  auto mt_it = m.NewIterator();
  raftkv::KVIterator& it = mt_it;
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.Current().key, "a");
  EXPECT_TRUE(it.Current().tombstone);
  EXPECT_TRUE(it.Current().value.empty());
  it.Next();
  EXPECT_EQ(it.Current().value, "2");
  it.Next();
  it.Next();
  EXPECT_FALSE(it.Valid());
}

TEST(MemTableTest, IteratorCanStartAtKey) {
  MemTable m;
  for (std::string k : {"a", "c", "e"}) m.Put(k, k);
  auto it = m.NewIterator("b");
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.Current().key, "c");
}