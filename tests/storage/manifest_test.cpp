#include <gtest/gtest.h>

#include <stdlib.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "storage/manifest.h"
#include "storage/wal.h"  // Crc32

using namespace raftkv;
namespace fs = std::filesystem;

namespace {

ManifestState Sample() {
  ManifestState s;
  s.next_file_number = 20;
  s.flushed_through = 17;
  s.files = {SSTableMeta{9, 1, 9}, SSTableMeta{14, 10, 14}, SSTableMeta{17, 15, 17}};
  return s;
}

// Rebuilds a manifest with extra bytes spliced in before the checksum, then re-seals it.
std::string Reseal(std::string body_without_crc) {
  std::string out = std::move(body_without_crc);
  uint32_t crc = Crc32(out.data(), out.size());
  for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((crc >> (8 * i)) & 0xFF));
  return out;
}
std::string WithoutCrc(const std::string& m) { return m.substr(0, m.size() - 4); }
std::string Record(uint16_t type, uint16_t flags, const std::string& payload) {
  std::string r;
  for (int i = 0; i < 2; ++i) r.push_back(static_cast<char>((type >> (8 * i)) & 0xFF));
  for (int i = 0; i < 2; ++i) r.push_back(static_cast<char>((flags >> (8 * i)) & 0xFF));
  uint32_t len = static_cast<uint32_t>(payload.size());
  for (int i = 0; i < 4; ++i) r.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
  return r + payload;
}

class ManifestTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char tmpl[] = "/tmp/raftkv_manifest_XXXXXX";
    dir_ = ::mkdtemp(tmpl);
  }
  void TearDown() override {
    std::error_code ec;
    fs::remove_all(dir_, ec);
  }
  std::string dir_;
};

}  // namespace

TEST_F(ManifestTest, RoundTripsTheState) {
  ManifestState out;
  ASSERT_EQ(DecodeManifest(EncodeManifest(Sample()), &out), Status::kOk);
  EXPECT_EQ(out.next_file_number, 20u);
  EXPECT_EQ(out.flushed_through, 17u);
  ASSERT_EQ(out.files.size(), 3u);
  EXPECT_EQ(out.files[1].file_number, 14u);
  EXPECT_EQ(out.files[1].min_seq, 10u);
  EXPECT_EQ(out.files[1].max_seq, 14u);
  ManifestState empty;
  ASSERT_EQ(DecodeManifest(EncodeManifest(empty), &out), Status::kOk);
  EXPECT_TRUE(out.files.empty());
}

TEST_F(ManifestTest, AMissingManifestIsAFreshDirectoryNotAnError) {
  ManifestState s;
  bool found = true;
  ASSERT_EQ(LoadManifest(dir_, &s, &found), Status::kOk);
  EXPECT_FALSE(found);
}

TEST_F(ManifestTest, EveryDamagedByteAndEveryTruncationIsRejected) {
  const std::string good = EncodeManifest(Sample());
  ManifestState out;
  for (size_t pos = 0; pos < good.size(); ++pos) {
    std::string bad = good;
    bad[pos] ^= 0x10;
    EXPECT_EQ(DecodeManifest(bad, &out), Status::kCorruption) << "byte " << pos;
  }
  for (size_t len = 0; len < good.size(); ++len) {
    EXPECT_EQ(DecodeManifest(good.substr(0, len), &out), Status::kCorruption) << "truncated to " << len;
  }
}

TEST_F(ManifestTest, ForwardCompatibilityRules) {
  const std::string base = WithoutCrc(EncodeManifest(Sample()));
  ManifestState out;
  // An unknown record that is not REQUIRED is skipped: a newer writer may add optional fields.
  ASSERT_EQ(DecodeManifest(Reseal(base + Record(99, 0, "some future field")), &out), Status::kOk);
  EXPECT_EQ(out.files.size(), 3u);
  // An unknown REQUIRED record means this reader cannot understand the file: refuse it.
  EXPECT_EQ(DecodeManifest(Reseal(base + Record(99, kManifestRequired, "x")), &out), Status::kCorruption);
  // A newer format version is refused outright.
  std::string newer = base;
  newer[8] = static_cast<char>(kManifestFormatVersion + 1);
  EXPECT_EQ(DecodeManifest(Reseal(newer), &out), Status::kCorruption);
  // Wrong magic.
  std::string wrong_magic = base;
  wrong_magic[0] ^= 1;
  EXPECT_EQ(DecodeManifest(Reseal(wrong_magic), &out), Status::kCorruption);
}

TEST_F(ManifestTest, StatesThatBreakTheInvariantsAreRejectedEvenWithAValidChecksum) {
  ManifestState out;
  auto reject = [&](ManifestState s, const char* why) {
    EXPECT_EQ(DecodeManifest(EncodeManifest(s), &out), Status::kCorruption) << why;
  };
  ManifestState s = Sample();
  s.files[1].min_seq = 5;  // overlaps the first file
  reject(s, "overlapping ranges");
  s = Sample();
  s.files[0].min_seq = 12;  // min > max
  reject(s, "min_seq > max_seq");
  s = Sample();
  s.files[2].file_number = 9;  // duplicate file number
  reject(s, "duplicate file number");
  s = Sample();
  s.next_file_number = 17;  // a live file number is not below the counter
  reject(s, "file number >= next_file_number");
  const std::string no_counter = Reseal(WithoutCrc(EncodeManifest(Sample())).substr(0, 16));  // header only
  EXPECT_EQ(DecodeManifest(no_counter, &out), Status::kCorruption) << "required records missing";
}

TEST_F(ManifestTest, StoreReplacesTheWholeStateAtomically) {
  ManifestState a = Sample(), b;
  b.next_file_number = 31;
  b.flushed_through = 30;
  b.files = {SSTableMeta{25, 1, 30}};  // a compaction: three files replaced by one, in ONE edit
  ASSERT_EQ(StoreManifest(dir_, a), Status::kOk);
  // A crash before the rename leaves a stray temp file; the committed manifest is still A.
  { std::ofstream(ManifestPath(dir_) + ".tmp", std::ios::binary) << "half-written garbage"; }
  ManifestState got;
  bool found = false;
  ASSERT_EQ(LoadManifest(dir_, &got, &found), Status::kOk);
  ASSERT_TRUE(found);
  EXPECT_EQ(got.files.size(), 3u);
  ASSERT_EQ(StoreManifest(dir_, b), Status::kOk);
  ASSERT_EQ(LoadManifest(dir_, &got, &found), Status::kOk);
  ASSERT_EQ(got.files.size(), 1u) << "the old three files and the new one must never be seen together";
  EXPECT_EQ(got.files[0].file_number, 25u);
  EXPECT_FALSE(fs::exists(ManifestPath(dir_) + ".tmp"));
}

TEST_F(ManifestTest, ADamagedManifestOnDiskIsCorruptionNotAnEmptyDatabase) {
  ASSERT_EQ(StoreManifest(dir_, Sample()), Status::kOk);
  std::string bytes;
  { std::ifstream in(ManifestPath(dir_), std::ios::binary); bytes.assign(std::istreambuf_iterator<char>(in), {}); }
  bytes[bytes.size() / 2] ^= 0x01;
  { std::ofstream out(ManifestPath(dir_), std::ios::binary | std::ios::trunc); out << bytes; }
  ManifestState s;
  bool found = false;
  EXPECT_EQ(LoadManifest(dir_, &s, &found), Status::kCorruption);
}

TEST_F(ManifestTest, StoreToAMissingDirectoryFailsCleanly) {
  EXPECT_EQ(StoreManifest(dir_ + "/does-not-exist", Sample()), Status::kIOError);
}
