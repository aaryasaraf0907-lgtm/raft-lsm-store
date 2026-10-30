#include "storage/merging_iterator.h"

#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "engine_test_util.h"
#include "storage/sstable.h"

using namespace raftkv;

namespace {

class VectorSource : public KVIterator {
 public:
  explicit VectorSource(std::vector<Entry> v) : v_(std::move(v)) {}
  bool Valid() const override { return i_ < v_.size(); }
  void Next() override { ++i_; }
  const Entry& Current() const override { return v_[i_]; }

 private:
  std::vector<Entry> v_;
  size_t i_ = 0;
};

std::vector<std::unique_ptr<KVIterator>> Sources(const std::vector<std::vector<Entry>>& lists) {
  std::vector<std::unique_ptr<KVIterator>> out;
  for (const auto& l : lists) out.push_back(std::make_unique<VectorSource>(l));
  return out;
}

// "key=value" for values, "key=<del>" for tombstones.
std::vector<std::string> Drain(MergingIterator* m) {
  std::vector<std::string> out;
  for (; m->Valid(); m->Next()) {
    out.push_back(m->Current().key + "=" + (m->Current().tombstone ? "<del>" : m->Current().value));
  }
  return out;
}

Entry Put(const std::string& k, const std::string& v) { return Entry{k, v, false}; }
Entry Del(const std::string& k) { return Entry{k, "", true}; }

TEST(MergingIterator, TheNewestSourceWinsAndOlderVersionsAreSkipped) {
  MergingIterator m(Sources({{Put("b", "new")},
                             {Put("a", "old"), Put("b", "old"), Put("c", "old")},
                             {Put("b", "oldest"), Put("d", "oldest")}}));
  EXPECT_EQ(Drain(&m), (std::vector<std::string>{"a=old", "b=new", "c=old", "d=oldest"}));
  EXPECT_EQ(m.status(), Status::kOk);
}

TEST(MergingIterator, ATombstoneIsYieldedAndHidesOlderValuesAndANewerValueHidesAnOlderTombstone) {
  MergingIterator m(Sources({{Del("a"), Put("b", "revived")}, {Put("a", "1"), Del("b")}}));
  EXPECT_EQ(Drain(&m), (std::vector<std::string>{"a=<del>", "b=revived"}));
}

TEST(MergingIterator, EmptySourcesAndNoSources) {
  MergingIterator none({});
  EXPECT_FALSE(none.Valid());
  EXPECT_EQ(none.status(), Status::kOk);
  MergingIterator empties(Sources({{}, {Put("", "empty key")}, {}}));
  EXPECT_EQ(Drain(&empties), (std::vector<std::string>{"=empty key"}));
  EXPECT_EQ(empties.status(), Status::kOk);
}

// One record per block (values over the 4096-byte target), so block i holds key i.
class MergingIteratorSSTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = dir_.path() + "/000001.sst";
    std::unique_ptr<SSTableBuilder> b;
    ASSERT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kOk);
    for (const char* k : {"a", "b", "c"}) ASSERT_EQ(b->Add(Put(k, std::string(5000, *k))), Status::kOk);
    ASSERT_EQ(b->Finish(), Status::kOk);
  }
  void DamageBlock(size_t block) {  // block i starts at i * (9 + 1 + 5000 + 4)
    std::fstream f(path_, std::ios::in | std::ios::out | std::ios::binary);
    const std::streamoff at = static_cast<std::streamoff>(block * 5014 + 100);
    f.seekp(at);
    f.put('!');
  }
  std::unique_ptr<SSTable> OpenTable() {
    std::unique_ptr<SSTable> t;
    EXPECT_EQ(SSTable::Open(path_, 1, &t), Status::kOk);
    return t;
  }

  testutil::TempDir dir_;
  std::string path_;
};

TEST_F(MergingIteratorSSTableTest, ASourceThatFailedBeforeTheMergeFailsItAtOnce) {
  DamageBlock(0);
  auto t = OpenTable();
  std::vector<std::unique_ptr<KVIterator>> sources = Sources({{Put("a", "mem")}});
  sources.push_back(t->NewIterator());
  MergingIterator m(std::move(sources));
  EXPECT_FALSE(m.Valid()) << "never a clean, shorter result";
  EXPECT_EQ(m.status(), Status::kCorruption);
}

TEST_F(MergingIteratorSSTableTest, AnErrorWhileSkippingAnOlderVersionEndsTheMerge) {
  // "a" comes from the newer source; skipping the table's own "a" moves the table into the
  // damaged block 1: the merge must stop with the error, not yield "a" and end cleanly.
  DamageBlock(1);
  auto t = OpenTable();
  std::vector<std::unique_ptr<KVIterator>> sources = Sources({{Put("a", "mem")}});
  sources.push_back(t->NewIterator());
  MergingIterator m(std::move(sources));
  EXPECT_FALSE(m.Valid());
  EXPECT_EQ(m.status(), Status::kCorruption);
  m.Next();  // sticky
  EXPECT_EQ(m.status(), Status::kCorruption);
}

TEST_F(MergingIteratorSSTableTest, EntriesBeforeTheDamageAreYieldedThenTheError) {
  DamageBlock(2);
  auto t = OpenTable();
  std::vector<std::unique_ptr<KVIterator>> sources;
  sources.push_back(t->NewIterator());
  MergingIterator m(std::move(sources));
  ASSERT_TRUE(m.Valid());
  EXPECT_EQ(m.Current().key, "a");
  m.Next();
  EXPECT_FALSE(m.Valid()) << "b is not yielded: advancing past it reached the damaged block";
  EXPECT_EQ(m.status(), Status::kCorruption);
}

}  // namespace
