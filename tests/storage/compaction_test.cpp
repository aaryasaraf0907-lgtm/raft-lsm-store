#include "storage/compaction.h"

#include <gtest/gtest.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "storage/sstable.h"
#include "storage/wal.h"  // Crc32, to reseal a deliberately edited footer

using raftkv::CompactionOptions;
using raftkv::CompactionResult;
using raftkv::Entry;
using raftkv::SSTable;
using raftkv::SSTableBuilder;
using raftkv::SSTableMeta;
using raftkv::Status;

namespace {

class CompactionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string tmpl = (std::filesystem::temp_directory_path() / "raftkv_compact_XXXXXX").string();
    ASSERT_NE(::mkdtemp(&tmpl[0]), nullptr);
    dir_ = tmpl;
  }
  void TearDown() override { std::filesystem::remove_all(dir_); }

  std::string PathFor(uint64_t file_number) const {
    return dir_ + "/" + std::to_string(file_number) + ".sst";
  }

  // Writes a table with the production builder and opens it. Entries must be sorted.
  const SSTable* Make(const SSTableMeta& meta, const std::vector<Entry>& entries) {
    std::unique_ptr<SSTableBuilder> b;
    EXPECT_EQ(SSTableBuilder::Create(PathFor(meta.file_number), meta, {}, &b), Status::kOk);
    for (const auto& e : entries) EXPECT_EQ(b->Add(e), Status::kOk);
    EXPECT_EQ(b->Finish(), Status::kOk);
    return OpenTable(meta.file_number);
  }
  const SSTable* OpenTable(uint64_t file_number) {
    std::unique_ptr<SSTable> t;
    EXPECT_EQ(SSTable::Open(PathFor(file_number), file_number, &t), Status::kOk);
    tables_.push_back(std::move(t));
    return tables_.back().get();
  }

  // Reads every entry of a published table; fails the test on any iterator error.
  std::vector<Entry> ReadAllEntries(uint64_t file_number, SSTableMeta* meta = nullptr) {
    std::unique_ptr<SSTable> t;
    EXPECT_EQ(SSTable::Open(PathFor(file_number), file_number, &t), Status::kOk);
    std::vector<Entry> out;
    if (t == nullptr) return out;
    if (meta != nullptr) *meta = t->meta();
    auto it = t->NewIterator();
    for (; it->Valid(); it->Next()) out.push_back(it->Current());
    EXPECT_EQ(it->status(), Status::kOk);
    return out;
  }

  std::string Bytes(const std::string& path) const {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
  }
  void WriteBytes(const std::string& path, const std::string& bytes) const {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  std::vector<std::string> DirEntries() const {
    std::vector<std::string> names;
    for (const auto& e : std::filesystem::directory_iterator(dir_)) {
      names.push_back(e.path().filename().string());
    }
    std::sort(names.begin(), names.end());
    return names;
  }

  Status Run(const std::vector<const SSTable*>& inputs, const std::vector<SSTableMeta>& live,
             uint64_t out_number, bool drop = false, CompactionResult* result = nullptr) {
    CompactionOptions opts;
    opts.drop_tombstones = drop;
    return raftkv::Compact(inputs, live, PathFor(out_number), out_number, opts, result);
  }

  std::string dir_;
  std::vector<std::unique_ptr<SSTable>> tables_;
};

bool Same(const std::vector<Entry>& a, const std::vector<Entry>& b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (a[i].key != b[i].key || a[i].value != b[i].value || a[i].tombstone != b[i].tombstone) {
      return false;
    }
  }
  return true;
}

Entry Put(const std::string& k, const std::string& v) { return Entry{k, v, false}; }
Entry Del(const std::string& k) { return Entry{k, "", true}; }

}  // namespace

// ---------------------------------------------------------------------------------------------
// ValidateCompaction: C1–C7 (§13.2) and the output range (§13.3)

TEST_F(CompactionTest, ValidateComputesOutputRange) {
  const SSTable* f1 = Make({1, 1, 2}, {Put("a", "1")});
  const SSTable* f2 = Make({2, 3, 5}, {Put("b", "2")});
  const SSTable* f3 = Make({3, 6, 6}, {Put("c", "3")});
  // live_files deliberately unsorted; inputs in reverse order.
  const std::vector<SSTableMeta> live = {{3, 6, 6}, {1, 1, 2}, {2, 3, 5}};
  SSTableMeta out;
  ASSERT_EQ(raftkv::ValidateCompaction({f3, f2}, live, 9, false, &out), Status::kOk);
  EXPECT_EQ(out.file_number, 9u);
  EXPECT_EQ(out.min_seq, 3u);  // S[a].min_seq
  EXPECT_EQ(out.max_seq, 6u);  // S[b].max_seq
  ASSERT_EQ(raftkv::ValidateCompaction({f2, f1, f3}, live, 9, true, &out), Status::kOk);
  EXPECT_EQ(out.min_seq, 1u);
  EXPECT_EQ(out.max_seq, 6u);
  ASSERT_EQ(raftkv::ValidateCompaction({f2}, live, 9, false, &out), Status::kOk);
  EXPECT_EQ(out.min_seq, 3u);
  EXPECT_EQ(out.max_seq, 5u);
}

TEST_F(CompactionTest, ValidateRejectsEveryRuleViolation) {
  const SSTable* f1 = Make({1, 1, 2}, {Put("a", "1")});
  const SSTable* f2 = Make({2, 3, 5}, {Put("b", "2")});
  const SSTable* f3 = Make({3, 6, 6}, {Put("c", "3")});
  const std::vector<SSTableMeta> live = {{1, 1, 2}, {2, 3, 5}, {3, 6, 6}};
  struct Case {
    const char* rule;
    std::vector<const SSTable*> inputs;
    std::vector<SSTableMeta> live;
    uint64_t out;
    bool drop;
  };
  const std::vector<Case> cases = {
      {"C1 duplicate file number", {f1}, {{1, 1, 2}, {1, 3, 5}}, 9, false},
      {"C1 min_seq > max_seq", {f1}, {{1, 1, 2}, {2, 5, 3}}, 9, false},
      {"C2 overlapping ranges", {f1}, {{1, 1, 2}, {2, 2, 5}}, 9, false},
      {"C2 equal min_seq", {f1}, {{1, 1, 2}, {2, 1, 1}}, 9, false},
      {"C3 zero inputs", {}, live, 9, false},
      {"C4 input not live", {f3}, {{1, 1, 2}, {2, 3, 5}}, 9, false},
      {"C4 min_seq mismatch", {f2}, {{1, 1, 2}, {2, 4, 5}, {3, 6, 6}}, 9, false},
      {"C4 max_seq mismatch", {f2}, {{1, 1, 2}, {2, 3, 4}, {3, 6, 6}}, 9, false},
      {"C4 input twice", {f2, f2}, live, 9, false},
      {"C4 null input", {f1, nullptr}, live, 9, false},
      {"C5 gap in run", {f1, f3}, live, 9, false},
      {"C6 output number is live", {f2}, live, 3, false},
      {"C6 output number is an input", {f2}, live, 2, false},
      {"C7 drop without the oldest file", {f2, f3}, live, 9, true},
  };
  for (const Case& c : cases) {
    SSTableMeta out{77, 77, 77};
    EXPECT_EQ(raftkv::ValidateCompaction(c.inputs, c.live, c.out, c.drop, &out),
              Status::kInvalidArgument)
        << c.rule;
    EXPECT_EQ(out.file_number, 77u) << c.rule << ": output untouched on error";
    const auto before = DirEntries();
    CompactionResult result{{55, 55, 55}, 55};
    EXPECT_EQ(Run(c.inputs, c.live, c.out, c.drop, &result), Status::kInvalidArgument) << c.rule;
    EXPECT_EQ(DirEntries(), before) << c.rule << ": no file may be created";
    EXPECT_EQ(result.entries_written, 55u) << c.rule << ": result untouched on error";
  }
  EXPECT_EQ(raftkv::ValidateCompaction({f1}, live, 9, false, nullptr), Status::kInvalidArgument);
}

TEST_F(CompactionTest, InvalidBuilderOptionsCreateNoFile) {
  const SSTable* f1 = Make({1, 1, 1}, {Put("a", "1")});
  CompactionOptions opts;
  opts.builder.bits_per_key = 0;
  const auto before = DirEntries();
  EXPECT_EQ(raftkv::Compact({f1}, {{1, 1, 1}}, PathFor(9), 9, opts, nullptr),
            Status::kInvalidArgument);
  EXPECT_EQ(DirEntries(), before);
}

// ---------------------------------------------------------------------------------------------
// Merge (§13.4)

TEST_F(CompactionTest, NewestVersionWinsByMaxSeq) {
  const SSTable* old_t = Make({1, 1, 1}, {Put("a", "old"), Put("b", "old"), Put("d", "only-old")});
  const SSTable* mid_t = Make({2, 2, 4}, {Put("a", "mid"), Put("c", "only-mid")});
  const SSTable* new_t = Make({3, 5, 5}, {Put("a", "new"), Put("b", "new")});
  CompactionResult r;
  ASSERT_EQ(Run({old_t, mid_t, new_t}, {{1, 1, 1}, {2, 2, 4}, {3, 5, 5}}, 9, false, &r),
            Status::kOk);
  EXPECT_TRUE(Same(ReadAllEntries(9), {Put("a", "new"), Put("b", "new"), Put("c", "only-mid"),
                                       Put("d", "only-old")}));
  EXPECT_EQ(r.entries_written, 4u);
}

TEST_F(CompactionTest, InputOrderDoesNotAffectOutput) {
  const SSTable* t1 = Make({1, 1, 1}, {Put("a", "1"), Put("k", "1"), Del("x")});
  const SSTable* t2 = Make({2, 2, 2}, {Put("a", "2"), Del("k"), Put("m", "2")});
  const SSTable* t3 = Make({3, 3, 3}, {Put("a", "3"), Put("x", "3"), Put("z", "")});
  const std::vector<SSTableMeta> live = {{1, 1, 1}, {2, 2, 2}, {3, 3, 3}};
  std::vector<const SSTable*> inputs = {t1, t2, t3};
  std::sort(inputs.begin(), inputs.end());
  std::string reference;
  uint64_t out = 10;
  do {
    ASSERT_EQ(Run(inputs, live, out), Status::kOk);
    const std::string bytes = Bytes(PathFor(out));
    if (reference.empty()) {
      reference = bytes;
      EXPECT_TRUE(Same(ReadAllEntries(out), {Put("a", "3"), Del("k"), Put("m", "2"),
                                             Put("x", "3"), Put("z", "")}));
    } else {
      // Same entries; the footer differs only in file_number, so compare everything before it.
      EXPECT_EQ(bytes.substr(0, bytes.size() - 100), reference.substr(0, reference.size() - 100));
    }
    ++out;
  } while (std::next_permutation(inputs.begin(), inputs.end()));
  EXPECT_EQ(out, 16u) << "all 6 orders were run";
}

TEST_F(CompactionTest, SingleInputIsCopiedUnchanged) {
  const std::vector<Entry> entries = {Put("", "empty-key"), Del("b"), Put("c", ""), Put("d", "4")};
  const SSTable* t = Make({4, 7, 9}, entries);
  CompactionResult r;
  ASSERT_EQ(Run({t}, {{4, 7, 9}}, 5, false, &r), Status::kOk);
  SSTableMeta meta;
  EXPECT_TRUE(Same(ReadAllEntries(5, &meta), entries));
  EXPECT_EQ(meta.min_seq, 7u);
  EXPECT_EQ(meta.max_seq, 9u);
}

TEST_F(CompactionTest, NewerTombstoneIsKeptAndHidesOlderValue) {
  const SSTable* older = Make({1, 1, 1}, {Put("k", "value"), Put("other", "1")});
  const SSTable* newer = Make({2, 2, 2}, {Del("k")});
  ASSERT_EQ(Run({older, newer}, {{1, 1, 1}, {2, 2, 2}}, 9), Status::kOk);
  EXPECT_TRUE(Same(ReadAllEntries(9), {Del("k"), Put("other", "1")}));
}

TEST_F(CompactionTest, TombstonesKeptWhenRunDoesNotStartAtOldestFile) {
  // f1 is older and outside the run: dropping k's tombstone would resurrect f1's value.
  Make({1, 1, 1}, {Put("k", "resurrect-me")});
  const SSTable* f2 = Make({2, 2, 2}, {Put("k", "v2")});
  const SSTable* f3 = Make({3, 3, 3}, {Del("k")});
  const std::vector<SSTableMeta> live = {{1, 1, 1}, {2, 2, 2}, {3, 3, 3}};
  EXPECT_EQ(Run({f2, f3}, live, 9, /*drop=*/true), Status::kInvalidArgument);  // C7
  ASSERT_EQ(Run({f2, f3}, live, 9, /*drop=*/false), Status::kOk);
  EXPECT_TRUE(Same(ReadAllEntries(9), {Del("k")}));
}

TEST_F(CompactionTest, DropTombstonesOmitsKeyAndAllOlderVersions) {
  const SSTable* f1 = Make({1, 1, 1}, {Put("gone", "old"), Put("keep", "1"), Del("x")});
  const SSTable* f2 = Make({2, 2, 2}, {Del("gone"), Put("x", "revived")});
  ASSERT_EQ(Run({f1, f2}, {{1, 1, 1}, {2, 2, 2}}, 9, /*drop=*/true), Status::kOk);
  EXPECT_TRUE(Same(ReadAllEntries(9), {Put("keep", "1"), Put("x", "revived")}));
}

TEST_F(CompactionTest, NewerValueOverridesOlderTombstone) {
  const SSTable* f1 = Make({1, 1, 1}, {Del("k")});
  const SSTable* f2 = Make({2, 2, 2}, {Put("k", "back")});
  ASSERT_EQ(Run({f1, f2}, {{1, 1, 1}, {2, 2, 2}}, 9), Status::kOk);
  EXPECT_TRUE(Same(ReadAllEntries(9), {Put("k", "back")}));
}

TEST_F(CompactionTest, EmptyValuesAreOrdinaryValues) {
  const SSTable* f1 = Make({1, 1, 1}, {Put("a", "full"), Put("b", "")});
  const SSTable* f2 = Make({2, 2, 2}, {Put("a", ""), Put("c", "")});
  for (bool drop : {false, true}) {
    const uint64_t out = drop ? 11 : 10;
    ASSERT_EQ(Run({f1, f2}, {{1, 1, 1}, {2, 2, 2}}, out, drop), Status::kOk);
    EXPECT_TRUE(Same(ReadAllEntries(out), {Put("a", ""), Put("b", ""), Put("c", "")})) << drop;
  }
}

TEST_F(CompactionTest, EmptyOutputIsStillWritten) {
  const SSTable* f1 = Make({1, 1, 1}, {Put("a", "1")});
  const SSTable* f2 = Make({2, 2, 3}, {Del("a"), Del("b")});
  CompactionResult r;
  ASSERT_EQ(Run({f1, f2}, {{1, 1, 1}, {2, 2, 3}}, 9, /*drop=*/true, &r), Status::kOk);
  EXPECT_EQ(Bytes(PathFor(9)).size(), 100u) << "an empty SSTable is the 100-byte footer (§9)";
  SSTableMeta meta;
  EXPECT_TRUE(ReadAllEntries(9, &meta).empty());
  EXPECT_EQ(meta.min_seq, 1u);
  EXPECT_EQ(meta.max_seq, 3u);
  EXPECT_EQ(r.entries_written, 0u);

  const SSTable* e1 = Make({20, 10, 10}, {});  // empty inputs
  const SSTable* e2 = Make({21, 11, 11}, {});
  ASSERT_EQ(Run({e1, e2}, {{20, 10, 10}, {21, 11, 11}}, 22), Status::kOk);
  EXPECT_TRUE(ReadAllEntries(22).empty());
}

TEST_F(CompactionTest, OutputMetadataAndResult) {
  const SSTable* f2 = Make({2, 4, 6}, {Put("a", "1")});
  const SSTable* f3 = Make({3, 7, 8}, {Put("b", "2")});
  CompactionResult r;
  ASSERT_EQ(Run({f3, f2}, {{1, 1, 3}, {2, 4, 6}, {3, 7, 8}, {4, 9, 9}}, 42, false, &r),
            Status::kOk);
  EXPECT_EQ(r.output.file_number, 42u);
  EXPECT_EQ(r.output.min_seq, 4u);
  EXPECT_EQ(r.output.max_seq, 8u);
  EXPECT_EQ(r.entries_written, 2u);
  std::unique_ptr<SSTable> out;
  ASSERT_EQ(SSTable::Open(PathFor(42), 42, &out), Status::kOk);
  EXPECT_EQ(out->meta().file_number, 42u);
  EXPECT_EQ(out->meta().min_seq, 4u);
  EXPECT_EQ(out->meta().max_seq, 8u);
  EXPECT_EQ(out->entry_count(), 2u);
}

TEST_F(CompactionTest, MatchesReferenceModelOnLargeMerge) {
  // 8 inputs, deterministic pseudo-random contents (fixed seed), checked against a std::map that
  // applies the inputs oldest to newest.
  std::mt19937 rng(12345);
  std::map<std::string, Entry> model;
  std::vector<const SSTable*> inputs;
  std::vector<SSTableMeta> live;
  for (uint64_t f = 1; f <= 8; ++f) {
    std::map<std::string, Entry> table;
    for (int i = 0; i < 6000; ++i) {
      const std::string key = "key" + std::to_string(rng() % 20000);
      const bool del = rng() % 5 == 0;
      const std::string value = std::to_string(f) + ":" + std::to_string(rng() % 1000);
      table[key] = del ? Del(key) : Put(key, value);
    }
    std::vector<Entry> sorted;
    for (auto& kv : table) {
      sorted.push_back(kv.second);
      model[kv.first] = kv.second;  // newer file overrides
    }
    const SSTableMeta meta{f, f * 10, f * 10 + 5};
    inputs.push_back(Make(meta, sorted));
    live.push_back(meta);
  }
  std::shuffle(inputs.begin(), inputs.end(), rng);
  for (bool drop : {false, true}) {
    const uint64_t out = drop ? 101 : 100;
    ASSERT_EQ(Run(inputs, live, out, drop), Status::kOk);
    std::vector<Entry> expected;
    for (auto& kv : model) {
      if (!(drop && kv.second.tombstone)) expected.push_back(kv.second);
    }
    EXPECT_TRUE(Same(ReadAllEntries(out), expected)) << "drop=" << drop;
  }
}

// ---------------------------------------------------------------------------------------------
// Errors (§13.4) and input preservation (§13.1)

TEST_F(CompactionTest, CorruptInputMidMergeAbandonsOutput) {
  std::vector<Entry> many;
  for (int i = 0; i < 2000; ++i) {
    many.push_back(Put("k" + std::to_string(100000 + i), std::string(40, 'v')));
  }
  Make({1, 1, 1}, many);
  const SSTable* f2 = Make({2, 2, 2}, {Put("k100500", "newer")});
  // Damage a byte in the middle of file 1's data region, then reopen it (Open validates only
  // the footer, index and filter; the damaged block is found during the merge).
  std::string bytes = Bytes(PathFor(1));
  bytes[bytes.size() / 3] ^= 0x01;
  WriteBytes(PathFor(1), bytes);
  const SSTable* f1 = OpenTable(1);
  const std::string f2_before = Bytes(PathFor(2));
  CompactionResult r{{55, 55, 55}, 55};
  EXPECT_EQ(Run({f1, f2}, {{1, 1, 1}, {2, 2, 2}}, 9, false, &r), Status::kCorruption);
  EXPECT_EQ(DirEntries(), (std::vector<std::string>{"1.sst", "2.sst"}))
      << "no output and no temp file";
  EXPECT_EQ(Bytes(PathFor(1)), bytes) << "inputs are never modified";
  EXPECT_EQ(Bytes(PathFor(2)), f2_before);
  EXPECT_EQ(r.entries_written, 55u);
}

TEST_F(CompactionTest, CorruptFirstBlockIsReportedBeforeAnyOutputExists) {
  Make({1, 1, 1}, {Put("a", "1"), Put("b", "2")});
  std::string bytes = Bytes(PathFor(1));
  bytes[10] ^= 0x01;  // value byte of "a" in the only block; detected by the block CRC
  WriteBytes(PathFor(1), bytes);
  const SSTable* f1 = OpenTable(1);
  EXPECT_EQ(Run({f1}, {{1, 1, 1}}, 9), Status::kCorruption);
  EXPECT_EQ(DirEntries(), std::vector<std::string>{"1.sst"});
}

TEST_F(CompactionTest, InputRecordCountMismatchIsCorruption) {
  // Footer claims one record fewer than the blocks hold: only a full scan can tell (§10.2).
  Make({1, 1, 1}, {Put("a", "1"), Put("b", "2"), Put("c", "3")});
  std::string bytes = Bytes(PathFor(1));
  const size_t footer = bytes.size() - 100;
  bytes[footer + 32] = 2;  // entry_count (u64, little-endian) 3 -> 2
  const uint32_t crc = raftkv::Crc32(bytes.data() + footer, 88);
  for (int i = 0; i < 4; ++i) bytes[footer + 88 + i] = static_cast<char>((crc >> (8 * i)) & 0xFF);
  WriteBytes(PathFor(1), bytes);
  const SSTable* f1 = OpenTable(1);
  EXPECT_EQ(Run({f1}, {{1, 1, 1}}, 9), Status::kCorruption);
  EXPECT_EQ(DirEntries(), std::vector<std::string>{"1.sst"});
}

TEST_F(CompactionTest, OutputWriteFailureIsIOErrorAndPublishesNothing) {
  std::vector<Entry> many;
  for (int i = 0; i < 400; ++i) {
    many.push_back(Put("k" + std::to_string(1000 + i), std::string(200, 'x')));
  }
  const SSTable* f1 = Make({1, 1, 1}, many);
  const std::string f1_before = Bytes(PathFor(1));
  std::signal(SIGXFSZ, SIG_IGN);  // exceeding the file-size limit then fails write() with EFBIG
  struct rlimit original;
  ASSERT_EQ(getrlimit(RLIMIT_FSIZE, &original), 0);
  struct rlimit tiny = {static_cast<rlim_t>(f1_before.size() / 4), original.rlim_max};
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &tiny), 0);
  const Status s = Run({f1}, {{1, 1, 1}}, 9);
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &original), 0);
  EXPECT_EQ(s, Status::kIOError);
  EXPECT_EQ(DirEntries(), std::vector<std::string>{"1.sst"}) << "no output and no temp file";
  EXPECT_EQ(Bytes(PathFor(1)), f1_before);
}

TEST_F(CompactionTest, ExistingOutputPathIsIOErrorAndUntouched) {
  const SSTable* f1 = Make({1, 1, 1}, {Put("a", "1")});
  WriteBytes(PathFor(9), "someone else's file");
  EXPECT_EQ(Run({f1}, {{1, 1, 1}}, 9), Status::kIOError);
  EXPECT_EQ(Bytes(PathFor(9)), "someone else's file");
  EXPECT_EQ(DirEntries(), (std::vector<std::string>{"1.sst", "9.sst"}));
}

TEST_F(CompactionTest, InputsAreNeverModifiedOrDeleted) {
  const SSTable* f1 = Make({1, 1, 1}, {Put("a", "1"), Del("b")});
  const SSTable* f2 = Make({2, 2, 2}, {Put("b", "2"), Put("c", "")});
  const std::string b1 = Bytes(PathFor(1)), b2 = Bytes(PathFor(2));
  ASSERT_EQ(Run({f1, f2}, {{1, 1, 1}, {2, 2, 2}}, 9), Status::kOk);
  ASSERT_EQ(Run({f1, f2}, {{1, 1, 1}, {2, 2, 2}}, 10, /*drop=*/true), Status::kOk);
  EXPECT_EQ(Bytes(PathFor(1)), b1);
  EXPECT_EQ(Bytes(PathFor(2)), b2);
  EXPECT_EQ(DirEntries(), (std::vector<std::string>{"1.sst", "10.sst", "2.sst", "9.sst"}));
}
