#include "storage/sstable.h"

#include <gtest/gtest.h>
#include <sys/resource.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "storage/memtable.h"
#include "storage/wal.h"  // Crc32, used by the independent test encoder below

using raftkv::BloomFilter;
using raftkv::Entry;
using raftkv::SSTable;
using raftkv::SSTableBuilder;
using raftkv::SSTableBuilderOptions;
using raftkv::SSTableMeta;
using raftkv::Status;
using R = raftkv::SSTable::GetResult;

namespace {

constexpr uint64_t kMaxKV = raftkv::SSTableFormat::kMaxKeyValueBytes;

void PutU32(std::string* s, uint32_t v) {
  for (int i = 0; i < 4; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void PutU64(std::string* s, uint64_t v) {
  for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void SetU32(std::string* s, size_t pos, uint32_t v) {
  for (int i = 0; i < 4; ++i) (*s)[pos + i] = static_cast<char>((v >> (8 * i)) & 0xFF);
}
void SetU64(std::string* s, size_t pos, uint64_t v) {
  for (int i = 0; i < 8; ++i) (*s)[pos + i] = static_cast<char>((v >> (8 * i)) & 0xFF);
}
uint64_t GetU64(const std::string& s, size_t pos) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<unsigned char>(s[pos + i])) << (8 * i);
  return v;
}
uint32_t Crc(const std::string& s) { return raftkv::Crc32(s.data(), s.size()); }

std::string FromHex(const std::string& hex) {
  std::string out;
  for (size_t i = 0; i + 1 < hex.size(); i += 2) {
    out.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
  }
  return out;
}

// --- Independent encoder (spec §3–§8), used to build valid and deliberately malformed files.
std::string Record(uint8_t flags, const std::string& key, const std::string& value) {
  std::string r(1, static_cast<char>(flags));
  PutU32(&r, static_cast<uint32_t>(key.size()));
  PutU32(&r, static_cast<uint32_t>(value.size()));
  return r + key + value;
}

struct RawBlock {
  std::string records;   // record bytes (no trailer)
  std::string last_key;  // stored in the index entry
};

std::string Encode(const std::vector<RawBlock>& blocks, const std::vector<std::string>& bloom_keys,
                   uint64_t entry_count, SSTableMeta meta = {1, 1, 1}) {
  std::string data, index;
  for (const RawBlock& b : blocks) {
    PutU64(&index, data.size());
    PutU32(&index, static_cast<uint32_t>(b.records.size()));
    PutU32(&index, static_cast<uint32_t>(b.last_key.size()));
    index += b.last_key;
    data += b.records;
    PutU32(&data, Crc(b.records));
  }
  std::string filter;
  if (entry_count > 0) {
    std::vector<uint64_t> h;
    for (const auto& k : bloom_keys) h.push_back(BloomFilter::Hash(k));
    BloomFilter f;
    EXPECT_EQ(BloomFilter::Build(h, 10, &f), Status::kOk);
    filter = f.Serialize();
  }
  std::string footer;
  PutU32(&footer, 1);
  PutU32(&footer, 0);
  PutU64(&footer, meta.file_number);
  PutU64(&footer, meta.min_seq);
  PutU64(&footer, meta.max_seq);
  PutU64(&footer, entry_count);
  PutU64(&footer, blocks.size());
  PutU64(&footer, data.size());
  PutU64(&footer, filter.size());
  PutU64(&footer, data.size() + filter.size());
  PutU64(&footer, index.size());
  PutU32(&footer, Crc(filter));
  PutU32(&footer, Crc(index));
  PutU32(&footer, Crc(footer));
  footer += "RKVSST01";
  return data + filter + index + footer;
}

// Footer field offsets (§8), relative to the start of the footer.
constexpr size_t kVersionOff = 0, kReservedOff = 4, kFileNumOff = 8, kMinSeqOff = 16,
                 kMaxSeqOff = 24, kEntryCountOff = 32, kBlockCountOff = 40, kFilterOffOff = 48,
                 kFilterLenOff = 56, kIndexOffOff = 64, kIndexLenOff = 72, kIndexCrcOff = 84;

size_t FooterStart(const std::string& file) { return file.size() - 100; }
void ResealFooter(std::string* file) {
  const size_t f = FooterStart(*file);
  SetU32(file, f + 88, raftkv::Crc32(file->data() + f, 88));
}
void PatchFooterU64(std::string* file, size_t field, uint64_t v) {
  SetU64(file, FooterStart(*file) + field, v);
  ResealFooter(file);
}

class SSTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string tmpl = (std::filesystem::temp_directory_path() / "raftkv_sst_XXXXXX").string();
    ASSERT_NE(::mkdtemp(&tmpl[0]), nullptr);
    dir_ = tmpl;
    path_ = dir_ + "/000001.sst";
  }
  void TearDown() override { std::filesystem::remove_all(dir_); }

  // Builds a table from `entries` with the production writer.
  Status Write(const std::vector<Entry>& entries, SSTableMeta meta = {1, 1, 1},
               const std::string& path = "") {
    std::unique_ptr<SSTableBuilder> b;
    Status s = SSTableBuilder::Create(path.empty() ? path_ : path, meta, {}, &b);
    if (s != Status::kOk) return s;
    for (const auto& e : entries) {
      s = b->Add(e);
      if (s != Status::kOk) return s;
    }
    return b->Finish();
  }
  std::unique_ptr<SSTable> OpenOk(uint64_t file_number = 1) {
    std::unique_ptr<SSTable> t;
    EXPECT_EQ(SSTable::Open(path_, file_number, &t), Status::kOk);
    return t;
  }
  Status OpenStatus(uint64_t file_number = 1) {
    std::unique_ptr<SSTable> t;
    return SSTable::Open(path_, file_number, &t);
  }
  std::string ReadAll() {
    std::ifstream in(path_, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
  }
  void WriteRaw(const std::string& bytes) {
    std::ofstream out(path_, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  std::vector<std::string> DirEntries() {
    std::vector<std::string> names;
    for (const auto& e : std::filesystem::directory_iterator(dir_)) names.push_back(e.path().filename().string());
    return names;
  }
  // Get with the out-parameters pre-filled, to check they are untouched on error.
  Status GetKey(const SSTable& t, const std::string& key, R* r, std::string* v) {
    *r = R::kFound;
    *v = "untouched";
    return t.Get(key, r, v);
  }

  std::string dir_, path_;
};

std::vector<Entry> ManyEntries(int n) {
  std::vector<Entry> v;
  char buf[32];
  for (int i = 0; i < n; ++i) {
    std::snprintf(buf, sizeof(buf), "key%06d", i * 2);  // even numbers: odd ones are absent
    v.push_back({buf, std::string(static_cast<size_t>(i % 50), 'v') + std::to_string(i), i % 7 == 0});
    if (v.back().tombstone) v.back().value.clear();
  }
  return v;
}

}  // namespace

// ---------------------------------------------------------------------------------------------
// Round trips and format conformance

TEST_F(SSTableTest, EmptyTableMatchesAppendixB2) {
  ASSERT_EQ(Write({}), Status::kOk);
  EXPECT_EQ(ReadAll(), FromHex(
                       "0100000000000000010000000000000001000000000000000100000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "00000000000000000000000000000000000000000000000040f69e91524b5653"
                       "53543031"));
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->entry_count(), 0u);
  EXPECT_EQ(t->block_count(), 0u);
  R r;
  std::string v = "untouched";
  ASSERT_EQ(t->Get("", &r, &v), Status::kOk);
  EXPECT_EQ(r, R::kAbsent);
  EXPECT_EQ(v, "untouched");
}

TEST_F(SSTableTest, SingleEntryBlockMatchesAppendixB3) {
  ASSERT_EQ(Write({{"a", "", false}}, {7, 3, 3}), Status::kOk);
  const std::string file = ReadAll();
  EXPECT_EQ(file.substr(0, 14), FromHex("00010000000000000061fb2d44ce"));
  EXPECT_EQ(file, Encode({{Record(0, "a", ""), "a"}}, {"a"}, 1, {7, 3, 3}))
      << "production writer and independent encoder must agree byte for byte";
  auto t = OpenOk(7);
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->meta().file_number, 7u);
  EXPECT_EQ(t->meta().min_seq, 3u);
  EXPECT_EQ(t->meta().max_seq, 3u);
}

TEST_F(SSTableTest, ManyEntriesRoundTripAcrossBlocks) {
  const auto entries = ManyEntries(10000);
  ASSERT_EQ(Write(entries), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->entry_count(), entries.size());
  EXPECT_GT(t->block_count(), 10u);
  for (const auto& e : entries) {
    R r;
    std::string v;
    ASSERT_EQ(t->Get(e.key, &r, &v), Status::kOk) << e.key;
    if (e.tombstone) {
      EXPECT_EQ(r, R::kDeleted) << e.key;
    } else {
      ASSERT_EQ(r, R::kFound) << e.key;
      EXPECT_EQ(v, e.value);
    }
  }
  for (std::string missing : {"", "a", "key000001", "key009999", "key0199999", "key019999", "zzz"}) {
    R r;
    std::string v = "untouched";
    ASSERT_EQ(t->Get(missing, &r, &v), Status::kOk) << missing;
    EXPECT_EQ(r, R::kAbsent) << missing;
    EXPECT_EQ(v, "untouched");
  }
}

TEST_F(SSTableTest, FileBytesIsTheSizeOfTheWholeFile) {
  // The compaction policy weighs tables by it: data blocks, filter, index and footer, all counted.
  struct Case {
    const char* what;
    std::vector<Entry> entries;
  };
  const std::vector<Case> cases = {
      {"an empty table", {}},
      {"one entry", {{"k", "v", false}}},
      {"one tombstone", {{"k", "", true}}},
      {"many entries over many blocks", ManyEntries(3000)},
      {"one record larger than a block", {{"big", std::string(200000, 'v'), false}}},
  };
  uint64_t previous = 0;
  for (const Case& c : cases) {
    std::filesystem::remove(path_);
    ASSERT_EQ(Write(c.entries), Status::kOk) << c.what;
    const uint64_t on_disk = std::filesystem::file_size(path_);
    {
      auto t = OpenOk();
      ASSERT_NE(t, nullptr) << c.what;
      EXPECT_EQ(t->file_bytes(), on_disk) << c.what;
      EXPECT_EQ(t->file_bytes(), ReadAll().size()) << c.what;
      EXPECT_GE(t->file_bytes(), raftkv::SSTableFormat::kFooterBytes) << c.what << ": at least the footer";
    }
    auto reopened = OpenOk();  // the value comes from the file, not from how it was written
    ASSERT_NE(reopened, nullptr) << c.what;
    EXPECT_EQ(reopened->file_bytes(), on_disk) << c.what;
    previous = on_disk;
  }
  EXPECT_GT(previous, 200000u);
  // More data, more bytes: a table of 3000 entries is larger than one of 10.
  std::filesystem::remove(path_);
  ASSERT_EQ(Write(ManyEntries(10)), Status::kOk);
  const uint64_t small = OpenOk()->file_bytes();
  std::filesystem::remove(path_);
  ASSERT_EQ(Write(ManyEntries(3000)), Status::kOk);
  EXPECT_GT(OpenOk()->file_bytes(), small);
}

TEST_F(SSTableTest, EmptyValueIsNotATombstone) {
  ASSERT_EQ(Write({{"deleted", "", true}, {"empty", "", false}, {"full", "x", false}}), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  R r;
  std::string v;
  ASSERT_EQ(GetKey(*t, "empty", &r, &v), Status::kOk);
  EXPECT_EQ(r, R::kFound);
  EXPECT_EQ(v, "");
  ASSERT_EQ(GetKey(*t, "deleted", &r, &v), Status::kOk);
  EXPECT_EQ(r, R::kDeleted);
  EXPECT_EQ(v, "untouched");
  ASSERT_EQ(GetKey(*t, "full", &r, &v), Status::kOk);
  EXPECT_EQ(v, "x");
  const std::string file = ReadAll();
  EXPECT_EQ(static_cast<uint8_t>(file[0]), 1u) << "first record carries the tombstone flag";
}

TEST_F(SSTableTest, EmptyKeyAndBinaryKeysAndValues) {
  const std::vector<Entry> entries = {{"", "empty-key", false},
                                      {std::string("\0", 1), std::string("\0\0", 2), false},
                                      {std::string("a\0b", 3), "nul", false},
                                      {std::string("\xff\xff", 2), std::string("\xff\x00", 2), false}};
  ASSERT_EQ(Write(entries), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  for (const auto& e : entries) {
    R r;
    std::string v;
    ASSERT_EQ(t->Get(e.key, &r, &v), Status::kOk);
    ASSERT_EQ(r, R::kFound);
    EXPECT_EQ(v, e.value);
  }
}

TEST_F(SSTableTest, BlockCuttingRule) {
  // Records of exactly 128 bytes (9 header + 4 key + 115 value), so 32 records fill a block to
  // exactly 4096 bytes. The block is closed before an append once it holds >= 4096 bytes (§5.1),
  // so record 33 starts a new block (a ">" rule would still append it).
  auto make = [](int n) {
    std::vector<Entry> v;
    char key[16];
    for (int i = 0; i < n; ++i) {
      std::snprintf(key, sizeof(key), "%04d", i);
      v.push_back({key, std::string(115, 'x'), false});
    }
    return v;
  };
  const std::vector<std::pair<int, uint64_t>> expected = {{32, 1}, {33, 2}, {64, 2}, {65, 3}};
  for (const auto& [n, blocks] : expected) {
    std::filesystem::remove(path_);
    ASSERT_EQ(Write(make(n)), Status::kOk);
    EXPECT_EQ(OpenOk()->block_count(), blocks) << n << " records";
  }
}

TEST_F(SSTableTest, LargeRecordFormsOneOversizedBlock) {
  const std::string big(100000, 'b');
  ASSERT_EQ(Write({{"a", "small", false}, {"b", big, false}, {"c", "after", false}}), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->block_count(), 2u);  // "a","b" share the first block; "c" starts a new one
  R r;
  std::string v;
  ASSERT_EQ(t->Get("b", &r, &v), Status::kOk);
  EXPECT_EQ(v, big);
}

TEST_F(SSTableTest, MaximumRecordSizeIsAccepted) {
  // key + value == 2^26 exactly (W3 boundary); writes a ~64 MiB file.
  const std::string value(kMaxKV - 1, 'm');
  ASSERT_EQ(Write({{"k", value, false}}), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  R r;
  std::string v;
  ASSERT_EQ(t->Get("k", &r, &v), Status::kOk);
  ASSERT_EQ(r, R::kFound);
  EXPECT_EQ(v.size(), value.size());
}

TEST_F(SSTableTest, BloomFilterSectionIsValidAndCoversAllKeys) {
  const auto entries = ManyEntries(2000);
  ASSERT_EQ(Write(entries), Status::kOk);
  const std::string file = ReadAll();
  const size_t f = FooterStart(file);
  const uint64_t off = GetU64(file, f + kFilterOffOff), len = GetU64(file, f + kFilterLenOff);
  BloomFilter filter;
  ASSERT_EQ(BloomFilter::Parse(file.data() + off, static_cast<size_t>(len), entries.size(), &filter),
            Status::kOk);
  EXPECT_EQ(filter.num_probes(), 6u);
  EXPECT_EQ(filter.num_bits(), 20000u);  // 2000 keys * 10 bits/key
  for (const auto& e : entries) EXPECT_TRUE(filter.MayContain(e.key)) << e.key;  // incl. tombstones
}

TEST_F(SSTableTest, WriteSSTableFromMemTable) {
  raftkv::MemTable m;
  m.Put("b", "2");
  m.Delete("a");
  m.Put("c", "");
  auto it = m.NewIterator();
  ASSERT_EQ(raftkv::WriteSSTable(path_, {1, 5, 5}, {}, &it), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->entry_count(), 3u);
  R r;
  std::string v;
  ASSERT_EQ(t->Get("a", &r, &v), Status::kOk);
  EXPECT_EQ(r, R::kDeleted);
  ASSERT_EQ(t->Get("c", &r, &v), Status::kOk);
  EXPECT_EQ(r, R::kFound);
  EXPECT_EQ(v, "");
  EXPECT_EQ(raftkv::WriteSSTable(dir_ + "/x.sst", {1, 1, 1}, {}, nullptr), Status::kInvalidArgument);
}

TEST_F(SSTableTest, ConcurrentGets) {
  const auto entries = ManyEntries(3000);
  ASSERT_EQ(Write(entries), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  std::vector<std::thread> threads;
  std::vector<int> errors(4, 0);
  for (int w = 0; w < 4; ++w) {
    threads.emplace_back([&, w] {
      for (const auto& e : entries) {
        R r;
        std::string v;
        if (t->Get(e.key, &r, &v) != Status::kOk || r == R::kAbsent) ++errors[static_cast<size_t>(w)];
      }
    });
  }
  for (auto& th : threads) th.join();
  for (int e : errors) EXPECT_EQ(e, 0);
}

// ---------------------------------------------------------------------------------------------
// Builder arguments and W1–W4 (§12.1)

TEST_F(SSTableTest, CreateRejectsInvalidArguments) {
  std::unique_ptr<SSTableBuilder> b;
  EXPECT_EQ(SSTableBuilder::Create("", {1, 1, 1}, {}, &b), Status::kInvalidArgument);
  EXPECT_EQ(SSTableBuilder::Create(path_, {1, 5, 4}, {}, &b), Status::kInvalidArgument);
  SSTableBuilderOptions bad;
  bad.bits_per_key = 0;
  EXPECT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, bad, &b), Status::kInvalidArgument);
  bad.bits_per_key = 65;
  EXPECT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, bad, &b), Status::kInvalidArgument);
  EXPECT_EQ(b, nullptr);
  EXPECT_TRUE(DirEntries().empty()) << "invalid arguments must not create any file";
}

TEST_F(SSTableTest, W1RejectsDuplicateAndDecreasingKeys) {
  for (const auto& second : {std::string("b"), std::string("a")}) {
    std::unique_ptr<SSTableBuilder> b;
    ASSERT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kOk);
    ASSERT_EQ(b->Add({"b", "1", false}), Status::kOk);
    EXPECT_EQ(b->Add({second, "2", false}), Status::kInvalidArgument) << second;
    EXPECT_EQ(b->Finish(), Status::kInvalidArgument) << "failed builder must not publish";
    EXPECT_TRUE(DirEntries().empty()) << "temp file removed on failure";
  }
  std::unique_ptr<SSTableBuilder> b;  // the empty key is valid once, but not twice
  ASSERT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kOk);
  ASSERT_EQ(b->Add({"", "1", false}), Status::kOk);
  EXPECT_EQ(b->Add({"", "2", false}), Status::kInvalidArgument);
}

TEST_F(SSTableTest, W2RejectsTombstoneWithValue) {
  std::unique_ptr<SSTableBuilder> b;
  ASSERT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kOk);
  EXPECT_EQ(b->Add({"k", "x", true}), Status::kInvalidArgument);
  EXPECT_EQ(b->Finish(), Status::kInvalidArgument);
  EXPECT_TRUE(DirEntries().empty());
}

TEST_F(SSTableTest, W3RejectsOversizedRecords) {
  for (int which = 0; which < 3; ++which) {
    std::unique_ptr<SSTableBuilder> b;
    ASSERT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kOk);
    Entry e;
    if (which == 0) e = {std::string(kMaxKV + 1, 'k'), "", false};          // key alone
    if (which == 1) e = {"k", std::string(kMaxKV, 'v'), false};              // 1 + 2^26
    if (which == 2) e = {std::string(kMaxKV / 2, 'k'), std::string(kMaxKV / 2 + 1, 'v'), false};
    EXPECT_EQ(b->Add(e), Status::kInvalidArgument) << which;
    EXPECT_EQ(b->Finish(), Status::kInvalidArgument);
  }
  EXPECT_TRUE(DirEntries().empty());
}

TEST_F(SSTableTest, W4RejectsCallsAfterFinishAbandonAndFailure) {
  {
    std::unique_ptr<SSTableBuilder> b;
    ASSERT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kOk);
    ASSERT_EQ(b->Add({"a", "1", false}), Status::kOk);
    ASSERT_EQ(b->Finish(), Status::kOk);
    EXPECT_EQ(b->Add({"b", "2", false}), Status::kInvalidArgument);
    EXPECT_EQ(b->Finish(), Status::kInvalidArgument);
    b->Abandon();  // no-op after Finish: the published file stays
  }
  EXPECT_EQ(OpenStatus(), Status::kOk);
  {
    std::unique_ptr<SSTableBuilder> b;
    ASSERT_EQ(SSTableBuilder::Create(dir_ + "/2.sst", {2, 1, 1}, {}, &b), Status::kOk);
    b->Abandon();
    b->Abandon();  // idempotent
    EXPECT_EQ(b->Add({"a", "1", false}), Status::kInvalidArgument);
    EXPECT_EQ(b->Finish(), Status::kInvalidArgument);
  }
  {
    std::unique_ptr<SSTableBuilder> b;
    ASSERT_EQ(SSTableBuilder::Create(dir_ + "/3.sst", {3, 1, 1}, {}, &b), Status::kOk);
    ASSERT_EQ(b->Add({"b", "1", false}), Status::kOk);
    ASSERT_EQ(b->Add({"a", "1", false}), Status::kInvalidArgument);  // W1 -> failed
    EXPECT_EQ(b->Add({"c", "1", false}), Status::kInvalidArgument);  // W4
  }
  EXPECT_EQ(DirEntries(), std::vector<std::string>{"000001.sst"});
}

TEST_F(SSTableTest, IOErrorDuringWriteThenW4) {
  std::signal(SIGXFSZ, SIG_IGN);  // exceeding the file-size limit then fails write() with EFBIG
  std::unique_ptr<SSTableBuilder> b;
  ASSERT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kOk);
  struct rlimit original;
  ASSERT_EQ(getrlimit(RLIMIT_FSIZE, &original), 0);
  struct rlimit tiny = {1000, original.rlim_max};
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &tiny), 0);
  Status failed = Status::kOk;
  for (int i = 0; i < 20 && failed == Status::kOk; ++i) {
    failed = b->Add({"key" + std::to_string(100 + i), std::string(600, 'x'), false});
  }
  if (failed == Status::kOk) failed = b->Finish();
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &original), 0);
  EXPECT_EQ(failed, Status::kIOError);
  EXPECT_EQ(b->Add({"zzz", "1", false}), Status::kInvalidArgument);  // W4 after an I/O failure
  EXPECT_EQ(b->Finish(), Status::kInvalidArgument);
  EXPECT_TRUE(DirEntries().empty()) << "no final file and no temp file after a failed write";
}

// ---------------------------------------------------------------------------------------------
// Publication (§12.2)

TEST_F(SSTableTest, SuccessfulPublishLeavesOnlyTheFinalFile) {
  ASSERT_EQ(Write(ManyEntries(100)), Status::kOk);
  EXPECT_EQ(DirEntries(), std::vector<std::string>{"000001.sst"});
}

TEST_F(SSTableTest, CreateFailsIfFinalPathExists) {
  WriteRaw("existing");
  std::unique_ptr<SSTableBuilder> b;
  EXPECT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kIOError);
  EXPECT_EQ(ReadAll(), "existing");
  EXPECT_EQ(DirEntries().size(), 1u);
}

TEST_F(SSTableTest, FinishNeverOverwritesAFileCreatedMeanwhile) {
  std::unique_ptr<SSTableBuilder> b;
  ASSERT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kOk);
  ASSERT_EQ(b->Add({"a", "1", false}), Status::kOk);
  WriteRaw("someone else's file");  // appears between P0 and P4
  EXPECT_EQ(b->Finish(), Status::kIOError);
  EXPECT_EQ(ReadAll(), "someone else's file") << "link() must not replace an existing file";
  EXPECT_EQ(DirEntries(), std::vector<std::string>{"000001.sst"}) << "temp file removed";
  EXPECT_EQ(b->Finish(), Status::kInvalidArgument);
}

TEST_F(SSTableTest, MissingDirectoryIsIOError) {
  std::unique_ptr<SSTableBuilder> b;
  EXPECT_EQ(SSTableBuilder::Create(dir_ + "/no/such/dir/1.sst", {1, 1, 1}, {}, &b), Status::kIOError);
}

TEST_F(SSTableTest, DestroyingAnUnfinishedBuilderLeavesNothing) {
  {
    std::unique_ptr<SSTableBuilder> b;
    ASSERT_EQ(SSTableBuilder::Create(path_, {1, 1, 1}, {}, &b), Status::kOk);
    ASSERT_EQ(b->Add({"a", std::string(10000, 'x'), false}), Status::kOk);
    ASSERT_EQ(b->Add({"b", "1", false}), Status::kOk);  // first block already written
    ASSERT_EQ(DirEntries().size(), 1u);                  // the temp file exists meanwhile
    EXPECT_NE(DirEntries()[0].find("000001.sst.tmp."), std::string::npos) << DirEntries()[0];
  }
  EXPECT_TRUE(DirEntries().empty());
  EXPECT_EQ(OpenStatus(), Status::kIOError);
}

// ---------------------------------------------------------------------------------------------
// Open-time validation (§10.1)

TEST_F(SSTableTest, OpenMissingFileIsIOError) { EXPECT_EQ(OpenStatus(), Status::kIOError); }

TEST_F(SSTableTest, OpenTooSmallIsCorruption) {
  WriteRaw("");
  EXPECT_EQ(OpenStatus(), Status::kCorruption);
  WriteRaw(std::string(99, '\0'));
  EXPECT_EQ(OpenStatus(), Status::kCorruption);
}

TEST_F(SSTableTest, OpenRejectsWrongExpectedFileNumber) {
  ASSERT_EQ(Write({{"a", "1", false}}, {5, 1, 1}), Status::kOk);
  EXPECT_EQ(OpenStatus(5), Status::kOk);
  EXPECT_EQ(OpenStatus(6), Status::kCorruption);  // §10.1 step 2
  std::string s = ReadAll();
  PatchFooterU64(&s, kFileNumOff, 9);  // the footer's file_number is what is compared
  WriteRaw(s);
  EXPECT_EQ(OpenStatus(9), Status::kOk);
  EXPECT_EQ(OpenStatus(5), Status::kCorruption);
}

TEST_F(SSTableTest, OpenRejectsBadFooterFields) {
  ASSERT_EQ(Write(ManyEntries(300)), Status::kOk);
  const std::string good = ReadAll();
  const size_t f = FooterStart(good);
  auto expect_corrupt = [&](std::string bad, const char* what) {
    WriteRaw(bad);
    EXPECT_EQ(OpenStatus(), Status::kCorruption) << what;
  };
  std::string s = good;
  s[s.size() - 1] = '2';
  expect_corrupt(s, "magic");
  s = good;
  s[f + kMinSeqOff] ^= 1;  // not resealed
  expect_corrupt(s, "footer crc");
  s = good;
  SetU32(&s, f + kVersionOff, 2);
  ResealFooter(&s);
  expect_corrupt(s, "version 2");
  s = good;
  SetU32(&s, f + kReservedOff, 1);
  ResealFooter(&s);
  expect_corrupt(s, "reserved");
  s = good;
  PatchFooterU64(&s, kMinSeqOff, 2);  // min 2 > max 1
  expect_corrupt(s, "min_seq > max_seq");
  s = good;
  PatchFooterU64(&s, kMaxSeqOff, 0);  // max 0 < min 1
  expect_corrupt(s, "max_seq < min_seq");
  s = good;
  PatchFooterU64(&s, kFilterLenOff, GetU64(good, f + kFilterLenOff) + 1);
  expect_corrupt(s, "L2");
  s = good;
  PatchFooterU64(&s, kIndexLenOff, GetU64(good, f + kIndexLenOff) - 1);
  expect_corrupt(s, "L3 (index_len)");
  // Isolated L3: one stray byte between the index and the footer; every section is intact.
  expect_corrupt(good.substr(0, f) + "X" + good.substr(f), "L3 (gap before footer)");
  // Isolated L2: one stray byte between the filter and the index, with index_offset moved.
  {
    const size_t index_off = static_cast<size_t>(GetU64(good, f + kIndexOffOff));
    std::string gap = good.substr(0, index_off) + "X" + good.substr(index_off);
    PatchFooterU64(&gap, kIndexOffOff, index_off + 1);
    expect_corrupt(gap, "L2 (gap before index)");
  }
  s = good;
  PatchFooterU64(&s, kBlockCountOff, 0);
  expect_corrupt(s, "non-empty table with block_count 0");
  s = good;
  PatchFooterU64(&s, kBlockCountOff, GetU64(good, f + kFilterOffOff) / 13 + 1);
  expect_corrupt(s, "block_count > filter_offset / 13");
  s = good;
  PatchFooterU64(&s, kEntryCountOff, GetU64(good, f + kFilterOffOff));
  expect_corrupt(s, "entry_count too large");
  s = good;
  PatchFooterU64(&s, kBlockCountOff, GetU64(good, f + kBlockCountOff) + 1);
  expect_corrupt(s, "block_count disagrees with index");
  WriteRaw(good);
  EXPECT_EQ(OpenStatus(), Status::kOk) << "unmodified file still opens";
}

TEST_F(SSTableTest, OpenRejectsBadEmptyTableFields) {
  ASSERT_EQ(Write({}), Status::kOk);
  const std::string good = ReadAll();
  for (size_t field : {kBlockCountOff, kFilterOffOff, kFilterLenOff, kIndexOffOff, kIndexLenOff}) {
    std::string s = good;
    PatchFooterU64(&s, field, 1);
    WriteRaw(s);
    EXPECT_EQ(OpenStatus(), Status::kCorruption) << "field " << field;
  }
  std::string s = good;
  PatchFooterU64(&s, kEntryCountOff, 1);  // claims an entry but has no blocks or filter
  WriteRaw(s);
  EXPECT_EQ(OpenStatus(), Status::kCorruption);
}

TEST_F(SSTableTest, OpenRejectsBadIndexAndFilterSections) {
  const RawBlock b1{Record(0, "a", "1") + Record(0, "b", "2"), "b"};
  const RawBlock b2{Record(0, "c", "3"), "c"};
  const std::vector<std::string> keys = {"a", "b", "c"};
  WriteRaw(Encode({b1, b2}, keys, 3));
  ASSERT_EQ(OpenStatus(), Status::kOk) << "encoder baseline must be valid";

  std::string s = Encode({b1, b2}, keys, 3);
  const size_t f = FooterStart(s);
  const size_t index_off = static_cast<size_t>(GetU64(s, f + kIndexOffOff));
  const size_t filter_off = static_cast<size_t>(GetU64(s, f + kFilterOffOff));
  auto with_index_patch = [&](size_t pos_in_index, uint32_t v) {
    std::string t = s;
    SetU32(&t, index_off + pos_in_index, v);
    const std::string index = t.substr(index_off, static_cast<size_t>(GetU64(t, f + kIndexLenOff)));
    SetU32(&t, f + kIndexCrcOff, Crc(index));
    ResealFooter(&t);
    return t;
  };
  std::string t = s;
  t[index_off + 33] ^= 4;  // block 1 last key "c" -> "g": structurally valid, only the CRC can tell
  WriteRaw(t);
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "index crc";
  WriteRaw(with_index_patch(0, 1));  // block 0 offset 1 instead of 0
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "offset mismatch";
  WriteRaw(with_index_patch(8, 8));  // block 0 size < 9
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "size < 9";
  WriteRaw(with_index_patch(8, 21));  // block 0 one byte shorter: block 1 offset no longer matches
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "offset chain";
  WriteRaw(with_index_patch(12, 1000));  // last_key_len runs past the index section
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "last_key_len";
  // Last block (index entry 1 starts at 16 + 1 = 17; its size at +8) one byte shorter: every
  // offset matches, but the chain ends before filter_offset (§10.1 step 5).
  WriteRaw(with_index_patch(25, 10));
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "offset chain ends before filter_offset";

  WriteRaw(Encode({{Record(0, "a", "1") + Record(0, "c", "2"), "c"}, {Record(0, "d", "3"), "b"}},
                  {"a", "c", "d"}, 3));
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "index last keys not increasing";

  t = s;
  t[filter_off + 15] ^= 1;  // inside the 8-byte bit array
  WriteRaw(t);
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "filter crc";
  t = s;
  t[filter_off] = 2;  // hash_kind 2, with the filter CRC resealed
  SetU32(&t, f + 80, Crc(t.substr(filter_off, static_cast<size_t>(GetU64(t, f + kFilterLenOff)))));
  ResealFooter(&t);
  WriteRaw(t);
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "filter hash_kind";
}

TEST_F(SSTableTest, TruncationAtEveryLengthIsCorruption) {
  ASSERT_EQ(Write(ManyEntries(200)), Status::kOk);
  const std::string good = ReadAll();
  for (size_t len = 0; len < good.size(); ++len) {
    WriteRaw(good.substr(0, len));
    ASSERT_EQ(OpenStatus(), Status::kCorruption) << "len=" << len;
  }
  WriteRaw(good + std::string(1, '\0'));
  EXPECT_EQ(OpenStatus(), Status::kCorruption) << "appended byte";
}

// ---------------------------------------------------------------------------------------------
// Block validation on Get (§10.2)

TEST_F(SSTableTest, GetDetectsBlockCrcMismatch) {
  ASSERT_EQ(Write({{"a", "1", false}, {"b", "2", false}}), Status::kOk);
  std::string s = ReadAll();
  s[10] ^= 1;  // the value byte of "a": still a well-formed record, only the CRC can tell
  WriteRaw(s);
  auto t = OpenOk();  // data blocks are not read at open time
  ASSERT_NE(t, nullptr);
  R r;
  std::string v;
  EXPECT_EQ(GetKey(*t, "a", &r, &v), Status::kCorruption);
  EXPECT_EQ(r, R::kFound);  // untouched
  EXPECT_EQ(v, "untouched");
}

TEST_F(SSTableTest, BloomNegativeSkipsTheBlockRead) {
  // §11.2: a definite Bloom negative answers kAbsent without reading the data block. Shown by
  // damaging the block: a key the filter excludes still returns kAbsent, a key it admits does not.
  ASSERT_EQ(Write({{"a", "1", false}, {"c", "3", false}}), Status::kOk);
  std::string s = ReadAll();
  const size_t f = FooterStart(s);
  BloomFilter filter;
  ASSERT_EQ(BloomFilter::Parse(s.data() + GetU64(s, f + kFilterOffOff),
                               static_cast<size_t>(GetU64(s, f + kFilterLenOff)), 2, &filter),
            Status::kOk);
  std::string excluded;
  for (int i = 0; i < 1000 && excluded.empty(); ++i) {
    const std::string k = "b" + std::to_string(i);  // sorts between "a" and "c": same block
    if (!filter.MayContain(k)) excluded = k;
  }
  ASSERT_FALSE(excluded.empty());
  s[10] ^= 1;  // damage the only data block
  WriteRaw(s);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  R r;
  std::string v;
  ASSERT_EQ(GetKey(*t, excluded, &r, &v), Status::kOk);
  EXPECT_EQ(r, R::kAbsent);
  EXPECT_EQ(GetKey(*t, "a", &r, &v), Status::kCorruption);
}

TEST_F(SSTableTest, GetDetectsMalformedRecords) {
  struct Case {
    const char* what;
    std::vector<RawBlock> blocks;
  };
  const std::string pad = Record(0, "z", "pad");
  const std::vector<Case> cases = {
      {"tombstone with value", {{Record(1, "k", "v"), "k"}}},
      {"unknown flag bit", {{Record(2, "k", "v"), "k"}}},
      {"keys out of order in block", {{Record(0, "m", "1") + Record(0, "k", "2"), "k"}}},
      {"duplicate key in block", {{Record(0, "k", "1") + Record(0, "k", "2"), "k"}}},
      {"last key differs from index", {{Record(0, "k", "1"), "kk"}}},
      {"record runs past block", {{Record(0, "k", "1").substr(0, 10), "k"}}},
      {"trailing bytes after last record", {{Record(0, "k", "1") + std::string(5, '\0'), "k"}}},
      {"key_len overflows block", {{Record(0, "k", "1").replace(1, 4, "\xff\xff\xff\x7f"), "k"}}},
  };
  for (const Case& c : cases) {
    WriteRaw(Encode(c.blocks, {"k"}, 1));
    std::unique_ptr<SSTable> t;
    ASSERT_EQ(SSTable::Open(path_, 1, &t), Status::kOk) << c.what << " (open validates structure only)";
    R r;
    std::string v;
    EXPECT_EQ(GetKey(*t, "k", &r, &v), Status::kCorruption) << c.what;
    EXPECT_EQ(v, "untouched") << c.what;
  }
}

TEST_F(SSTableTest, GetDetectsCrossBlockOrderViolation) {
  // Index last keys "b" < "c" are increasing, but block 1 starts with "a" <= "b".
  WriteRaw(Encode({{Record(0, "a", "1") + Record(0, "b", "2"), "b"},
                   {Record(0, "a", "3") + Record(0, "c", "4"), "c"}},
                  {"a", "b", "c"}, 4));
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  R r;
  std::string v;
  EXPECT_EQ(t->Get("b", &r, &v), Status::kOk);  // block 0 alone is valid
  EXPECT_EQ(GetKey(*t, "c", &r, &v), Status::kCorruption);
}

TEST_F(SSTableTest, GetValidatesTheWholeBlockEvenAfterFindingTheKey) {
  // The lookup searches the encoded records in place, but it must not stop validating at the
  // record it was looking for: damage AFTER the key in the same block is still kCorruption.
  struct Case {
    const char* what;
    RawBlock block;
  };
  const std::string a = Record(0, "a", "1");
  const std::vector<Case> cases = {
      {"key out of order after the target", {a + Record(0, "c", "3") + Record(0, "b", "2"), "b"}},
      {"tombstone with value after the target", {a + Record(1, "b", "2"), "b"}},
      {"unknown flag bit after the target", {a + Record(4, "b", "2"), "b"}},
      {"trailing bytes after the last record", {a + Record(0, "b", "2") + std::string(3, '\0'), "b"}},
      {"last key differs from the index", {a + Record(0, "b", "2"), "bb"}},
      {"record runs past the block", {a + Record(0, "b", "2").substr(0, 10), "b"}},
  };
  for (const Case& c : cases) {
    WriteRaw(Encode({c.block}, {"a", "b"}, 2));
    std::unique_ptr<SSTable> t;
    ASSERT_EQ(SSTable::Open(path_, 1, &t), Status::kOk) << c.what;
    R r;
    std::string v;
    EXPECT_EQ(GetKey(*t, "a", &r, &v), Status::kCorruption) << c.what;
    EXPECT_EQ(r, R::kFound) << c.what << " (untouched)";
    EXPECT_EQ(v, "untouched") << c.what;
  }
}

TEST_F(SSTableTest, GetAnswersEveryKeyOfAMultiBlockTable) {
  // Every present key (first, last and every block boundary included) is found or deleted, and
  // every key between, before or after them is absent; the value is copied exactly.
  const auto entries = ManyEntries(3000);
  ASSERT_EQ(Write(entries), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  ASSERT_GT(t->block_count(), 10u);
  for (int i = 0; i < 3000; ++i) {
    const Entry& e = entries[static_cast<size_t>(i)];
    R r;
    std::string v;
    ASSERT_EQ(GetKey(*t, e.key, &r, &v), Status::kOk) << e.key;
    if (e.tombstone) {
      EXPECT_EQ(r, R::kDeleted) << e.key;
      EXPECT_EQ(v, "untouched") << e.key;
    } else {
      EXPECT_EQ(r, R::kFound) << e.key;
      EXPECT_EQ(v, e.value) << e.key;
    }
    char odd[32];
    std::snprintf(odd, sizeof(odd), "key%06d", i * 2 + 1);
    ASSERT_EQ(GetKey(*t, odd, &r, &v), Status::kOk) << odd;
    EXPECT_EQ(r, R::kAbsent) << odd;
  }
  R r;
  std::string v;
  for (const std::string k : {"", "a", "key", "key0000000", "key999999", "zzz"}) {
    ASSERT_EQ(GetKey(*t, k, &r, &v), Status::kOk) << k;
    EXPECT_EQ(r, R::kAbsent) << k;
  }
}

// ---------------------------------------------------------------------------------------------
// Iterator (§11.3)

namespace {

// Scans the whole table; returns the iterator's final status and the entries it produced.
Status Scan(const SSTable& t, std::vector<Entry>* out) {
  out->clear();
  auto it = t.NewIterator();
  for (; it->Valid(); it->Next()) out->push_back(it->Current());
  EXPECT_FALSE(it->Valid());
  return it->status();
}

bool SameEntries(const std::vector<Entry>& a, const std::vector<Entry>& b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (a[i].key != b[i].key || a[i].value != b[i].value || a[i].tombstone != b[i].tombstone) {
      return false;
    }
  }
  return true;
}

}  // namespace

TEST_F(SSTableTest, IteratorOnEmptyTableEndsCleanly) {
  ASSERT_EQ(Write({}), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  auto it = t->NewIterator();
  EXPECT_FALSE(it->Valid());
  EXPECT_EQ(it->status(), Status::kOk);
  it->Next();  // no-op on an invalid iterator
  EXPECT_FALSE(it->Valid());
  EXPECT_EQ(it->status(), Status::kOk);
}

TEST_F(SSTableTest, IteratorReturnsEveryEntryInOrder) {
  auto entries = ManyEntries(10000);
  entries.insert(entries.begin(), Entry{"", "", false});  // empty key with an empty value
  ASSERT_EQ(Write(entries), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  ASSERT_GT(t->block_count(), 10u);
  std::vector<Entry> got;
  EXPECT_EQ(Scan(*t, &got), Status::kOk);
  EXPECT_TRUE(SameEntries(got, entries));
}

TEST_F(SSTableTest, IteratorWorksThroughKVIteratorInterface) {
  ASSERT_EQ(Write({{"a", "1", false}, {"b", "", true}, {"c", "", false}}), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  auto owned = t->NewIterator();
  raftkv::KVIterator& it = *owned;
  ASSERT_TRUE(it.Valid());
  const Entry& first = it.Current();
  EXPECT_EQ(first.key, "a");
  EXPECT_EQ(first.value, "1");
  it.Next();
  EXPECT_TRUE(it.Current().tombstone);
  EXPECT_TRUE(it.Current().value.empty());
  it.Next();
  EXPECT_FALSE(it.Current().tombstone) << "empty value, not a tombstone";
  it.Next();
  EXPECT_FALSE(it.Valid());
  EXPECT_EQ(owned->status(), Status::kOk);
}

TEST_F(SSTableTest, IteratorReportsCorruptionInsteadOfEndOfData) {
  const auto entries = ManyEntries(3000);
  ASSERT_EQ(Write(entries), Status::kOk);
  std::string s = ReadAll();
  const size_t f = FooterStart(s);
  const size_t filter_off = static_cast<size_t>(GetU64(s, f + kFilterOffOff));
  s[filter_off / 2] ^= 1;  // a byte in a middle data block
  WriteRaw(s);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  std::vector<Entry> got;
  EXPECT_EQ(Scan(*t, &got), Status::kCorruption);
  EXPECT_GT(got.size(), 0u) << "blocks before the damage are returned";
  EXPECT_LT(got.size(), entries.size()) << "the scan must stop at the damaged block";
  auto it = t->NewIterator();
  while (it->Valid()) it->Next();
  it->Next();  // sticky
  EXPECT_EQ(it->status(), Status::kCorruption);
}

TEST_F(SSTableTest, IteratorChecksRecordCountAgainstFooter) {
  const RawBlock b1{Record(0, "a", "1111111111") + Record(0, "b", "2222222222"), "b"};
  const RawBlock b2{Record(0, "c", "3333333333"), "c"};
  for (uint64_t claimed : {uint64_t{2}, uint64_t{4}}) {  // actual count is 3
    WriteRaw(Encode({b1, b2}, {"a", "b", "c"}, claimed));
    auto t = OpenOk();
    ASSERT_NE(t, nullptr) << claimed;
    R r;
    std::string v;
    EXPECT_EQ(t->Get("c", &r, &v), Status::kOk) << "point lookups cannot see the count";
    std::vector<Entry> got;
    EXPECT_EQ(Scan(*t, &got), Status::kCorruption) << "claimed=" << claimed;
    EXPECT_EQ(got.size(), 3u);
  }
}

TEST_F(SSTableTest, IteratorDetectsCrossBlockOrderViolation) {
  WriteRaw(Encode({{Record(0, "a", "1") + Record(0, "b", "2"), "b"},
                   {Record(0, "a", "3") + Record(0, "c", "4"), "c"}},
                  {"a", "b", "c"}, 4));
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  std::vector<Entry> got;
  EXPECT_EQ(Scan(*t, &got), Status::kCorruption);
  EXPECT_EQ(got.size(), 2u) << "only the valid first block is returned";
}

TEST_F(SSTableTest, IteratorSeekFindsTheFirstKeyAtOrAfterTheTarget) {
  // Every present key (so every block boundary), every key between two of them, before the first
  // and past the last; one iterator re-seeked forwards and backwards.
  auto entries = ManyEntries(3000);
  entries.insert(entries.begin(), Entry{"", "empty key", false});
  ASSERT_EQ(Write(entries), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  ASSERT_GT(t->block_count(), 10u);
  auto it = t->NewIterator();
  auto expect_at = [&](const std::string& target, size_t want) {
    it->Seek(target);
    ASSERT_EQ(it->status(), Status::kOk) << target;
    if (want == entries.size()) {
      EXPECT_FALSE(it->Valid()) << target;
      return;
    }
    ASSERT_TRUE(it->Valid()) << target;
    EXPECT_EQ(it->Current().key, entries[want].key) << target;
    EXPECT_EQ(it->Current().value, entries[want].value) << target;
    EXPECT_EQ(it->Current().tombstone, entries[want].tombstone) << target;
  };
  for (size_t i = entries.size(); i-- > 1;) {  // backwards over the non-empty keys
    expect_at(entries[i].key, i);
    expect_at(entries[i].key + std::string(1, '\0'), i + 1);  // just after the key
  }
  for (size_t i = 1; i < entries.size(); ++i) {
    char odd[32];
    std::snprintf(odd, sizeof(odd), "key%06d", static_cast<int>(i - 1) * 2 + 1);  // between i and i+1
    expect_at(odd, i + 1);
  }
  expect_at("", 0);  // the empty key itself
  expect_at("a", 1);
  expect_at("zzz", entries.size());
  auto fresh = t->NewIterator("key000004");
  ASSERT_TRUE(fresh->Valid());
  EXPECT_EQ(fresh->Current().key, "key000004");
}

TEST_F(SSTableTest, IteratorSeekThenNextVisitsTheRestInOrder) {
  const auto entries = ManyEntries(3000);
  ASSERT_EQ(Write(entries), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  for (size_t start : {size_t{0}, size_t{1}, size_t{777}, size_t{2998}, size_t{2999}}) {
    auto it = t->NewIterator(entries[start].key);
    std::vector<Entry> got;
    for (; it->Valid(); it->Next()) got.push_back(it->Current());
    EXPECT_EQ(it->status(), Status::kOk) << start;
    EXPECT_TRUE(SameEntries(got, std::vector<Entry>(entries.begin() + static_cast<long>(start), entries.end())))
        << start;
  }
}

TEST_F(SSTableTest, IteratorSeekOnAnEmptyTableEndsCleanly) {
  ASSERT_EQ(Write({}), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  for (const std::string target : {"", "k"}) {
    auto it = t->NewIterator(target);
    EXPECT_FALSE(it->Valid());
    EXPECT_EQ(it->status(), Status::kOk);
    it->Seek("");
    EXPECT_FALSE(it->Valid());
    EXPECT_EQ(it->status(), Status::kOk);
  }
}

TEST_F(SSTableTest, IteratorSeekReportsADamagedBlockAndTheErrorIsSticky) {
  const auto entries = ManyEntries(3000);
  ASSERT_EQ(Write(entries), Status::kOk);
  std::string s = ReadAll();
  const size_t f = FooterStart(s);
  const size_t filter_off = static_cast<size_t>(GetU64(s, f + kFilterOffOff));
  s[filter_off / 2] ^= 1;  // a byte in a middle data block
  WriteRaw(s);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);

  // Seeking into the damaged block fails; seeking elsewhere afterwards does not undo that.
  auto it = t->NewIterator(entries.front().key);
  ASSERT_TRUE(it->Valid());
  it->Seek(entries[1500].key);
  EXPECT_FALSE(it->Valid());
  EXPECT_EQ(it->status(), Status::kCorruption);
  it->Seek(entries.front().key);
  it->Next();
  EXPECT_FALSE(it->Valid());
  EXPECT_EQ(it->status(), Status::kCorruption);

  // Seeking before the damage and reading on reaches it as an error, never as a clean end.
  auto from_start = t->NewIterator(entries.front().key);
  size_t n = 0;
  for (; from_start->Valid(); from_start->Next()) ++n;
  EXPECT_EQ(from_start->status(), Status::kCorruption);
  EXPECT_GT(n, 0u);
  EXPECT_LT(n, entries.size());
}

TEST_F(SSTableTest, IteratorChecksTheRecordCountOnlyWhenNotSeeked) {
  // The count in the footer describes the whole table: a scan from the first record checks it
  // (IteratorChecksRecordCountAgainstFooter); a seeked scan never read the earlier blocks, so it
  // does not.
  const RawBlock b1{Record(0, "a", "1111111111") + Record(0, "b", "2222222222"), "b"};
  const RawBlock b2{Record(0, "c", "3333333333"), "c"};
  WriteRaw(Encode({b1, b2}, {"a", "b", "c"}, 4));  // actual count is 3
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  std::vector<Entry> got;
  EXPECT_EQ(Scan(*t, &got), Status::kCorruption);
  auto it = t->NewIterator("");
  size_t n = 0;
  for (; it->Valid(); it->Next()) ++n;
  EXPECT_EQ(it->status(), Status::kOk);
  EXPECT_EQ(n, 3u);
}

TEST_F(SSTableTest, ConcurrentIterators) {
  const auto entries = ManyEntries(3000);
  ASSERT_EQ(Write(entries), Status::kOk);
  auto t = OpenOk();
  ASSERT_NE(t, nullptr);
  std::vector<int> ok(4, 0);
  std::vector<std::thread> threads;
  for (int w = 0; w < 4; ++w) {
    threads.emplace_back([&, w] {
      std::vector<Entry> got;
      auto it = t->NewIterator();
      for (; it->Valid(); it->Next()) got.push_back(it->Current());
      ok[static_cast<size_t>(w)] = it->status() == Status::kOk && SameEntries(got, entries);
    });
  }
  for (auto& th : threads) th.join();
  for (int v : ok) EXPECT_EQ(v, 1);
}

TEST_F(SSTableTest, EverySingleBitFlipIsDetected) {
  // Every byte is covered by a block CRC, the filter or index CRC, the footer CRC or the magic,
  // so a flipped bit must surface as kCorruption at Open or during a full scan: never as a
  // clean scan.
  std::vector<Entry> entries;
  for (int i = 0; i < 60; ++i) {
    entries.push_back({"k" + std::to_string(1000 + i), std::string(100, static_cast<char>('a' + i % 26)),
                       i % 5 == 0});
    if (entries.back().tombstone) entries.back().value.clear();
  }
  ASSERT_EQ(Write(entries), Status::kOk);
  const std::string good = ReadAll();
  ASSERT_GT(OpenOk()->block_count(), 1u);
  for (size_t pos = 0; pos < good.size(); ++pos) {
    std::string s = good;
    s[pos] = static_cast<char>(s[pos] ^ (1 << (pos % 8)));
    WriteRaw(s);
    std::unique_ptr<SSTable> t;
    const Status open = SSTable::Open(path_, 1, &t);
    if (open != Status::kOk) {
      ASSERT_EQ(open, Status::kCorruption) << "pos=" << pos;
      continue;
    }
    std::vector<Entry> got;
    ASSERT_EQ(Scan(*t, &got), Status::kCorruption) << "undetected flip at pos=" << pos;
  }
}
