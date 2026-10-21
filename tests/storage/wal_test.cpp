#include "storage/wal.h"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

using raftkv::Entry;
using raftkv::ReplayStats;
using raftkv::Status;
using raftkv::WAL;

class WalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string tmpl = (std::filesystem::temp_directory_path() / "raftkv_wal_XXXXXX").string();
    ASSERT_NE(::mkdtemp(&tmpl[0]), nullptr);
    dir_ = tmpl;
    path_ = dir_ + "/wal.log";
  }
  void TearDown() override { std::filesystem::remove_all(dir_); }

  std::unique_ptr<WAL> OpenWal() {
    std::unique_ptr<WAL> w;
    EXPECT_EQ(WAL::Open(path_, &w), Status::kOk);
    return w;
  }
  std::vector<Entry> Replay(ReplayStats* s = nullptr) {
    std::vector<Entry> out;
    EXPECT_EQ(WAL::Replay(path_, [&](const Entry& e) { out.push_back(e); }, s), Status::kOk);
    return out;
  }
  uint64_t FileSize() { return std::filesystem::file_size(path_); }

  std::string dir_, path_;
};

TEST(Crc32Test, KnownVector) { EXPECT_EQ(raftkv::Crc32("123456789", 9), 0xCBF43926u); }

TEST_F(WalTest, MissingFileIsEmptyLog) {
  EXPECT_TRUE(Replay().empty());
  EXPECT_FALSE(std::filesystem::exists(path_));
}

TEST_F(WalTest, EmptyFileIsEmptyLog) {
  OpenWal();
  ReplayStats s;
  EXPECT_TRUE(Replay(&s).empty());
  EXPECT_EQ(s.discarded_bytes, 0u);
}

TEST_F(WalTest, ReplaysPutsAndDeletesInOrder) {
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("a", "1"), Status::kOk);
    ASSERT_EQ(w->AppendPut("b", "2"), Status::kOk);
    ASSERT_EQ(w->AppendDelete("a"), Status::kOk);
    ASSERT_EQ(w->AppendPut("a", "3"), Status::kOk);
  }
  auto r = Replay();
  ASSERT_EQ(r.size(), 4u);
  EXPECT_EQ(r[0].key, "a");
  EXPECT_FALSE(r[0].tombstone);
  EXPECT_EQ(r[1].value, "2");
  EXPECT_EQ(r[2].key, "a");
  EXPECT_TRUE(r[2].tombstone);
  EXPECT_TRUE(r[2].value.empty());
  EXPECT_EQ(r[3].value, "3");
}

TEST_F(WalTest, EmptyKeyEmptyValueAndBinaryData) {
  std::string k("\0k\xff", 3), v("\0\0v", 3);
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("", "", true), Status::kOk);
    ASSERT_EQ(w->AppendPut(k, v), Status::kOk);
  }
  auto r = Replay();
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0].key, "");
  EXPECT_EQ(r[1].key, k);
  EXPECT_EQ(r[1].value, v);
}

TEST_F(WalTest, LargeValueRoundTrips) {
  std::string big(1 << 20, 'z');
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("big", big), Status::kOk);
  }
  auto r = Replay();
  ASSERT_EQ(r.size(), 1u);
  EXPECT_EQ(r[0].value, big);
}

TEST_F(WalTest, OversizedRecordIsRejected) {
  auto w = OpenWal();
  EXPECT_EQ(w->AppendPut("k", std::string(WAL::kMaxPayloadBytes, 'x')), Status::kIOError);
  EXPECT_EQ(FileSize(), 0u);
}

TEST_F(WalTest, UnsyncedAppendsThenSync) {
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("a", "1", false), Status::kOk);
    ASSERT_EQ(w->AppendPut("b", "2", false), Status::kOk);
    ASSERT_EQ(w->Sync(), Status::kOk);
  }
  EXPECT_EQ(Replay().size(), 2u);
}

// Crash simulation: cut the file at EVERY byte inside the last record.
TEST_F(WalTest, TornLastRecordAtEveryCutPoint) {
  uint64_t end_of_two = 0, full = 0;
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("k1", "v1"), Status::kOk);
    ASSERT_EQ(w->AppendPut("k2", "v2"), Status::kOk);
    end_of_two = FileSize();
    ASSERT_EQ(w->AppendPut("k3", "value-three"), Status::kOk);
    full = FileSize();
  }
  std::string original;
  {
    std::ifstream in(path_, std::ios::binary);
    original.assign(std::istreambuf_iterator<char>(in), {});
  }
  for (uint64_t cut = end_of_two; cut < full; ++cut) {
    {
      std::ofstream out(path_, std::ios::binary | std::ios::trunc);
      out.write(original.data(), static_cast<std::streamsize>(cut));
    }
    ReplayStats s;
    auto r = Replay(&s);
    ASSERT_EQ(r.size(), 2u) << "cut=" << cut;
    EXPECT_EQ(s.valid_bytes, end_of_two) << "cut=" << cut;
    EXPECT_EQ(s.discarded_bytes, cut - end_of_two) << "cut=" << cut;
    EXPECT_EQ(FileSize(), end_of_two) << "tail must be truncated, cut=" << cut;
  }
}

TEST_F(WalTest, CorruptByteInLastRecordDropsOnlyThatRecord) {
  uint64_t end_of_one = 0;
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("a", "1"), Status::kOk);
    end_of_one = FileSize();
    ASSERT_EQ(w->AppendPut("b", "2"), Status::kOk);
  }
  {
    std::fstream f(path_, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(static_cast<std::streamoff>(FileSize() - 1));
    f.put('\x7f');
  }
  auto r = Replay();
  ASSERT_EQ(r.size(), 1u);
  EXPECT_EQ(r[0].key, "a");
  EXPECT_EQ(FileSize(), end_of_one);
}

TEST_F(WalTest, CorruptionInMiddleStopsReplayThere) {
  uint64_t end_of_one = 0;
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("a", "1"), Status::kOk);
    end_of_one = FileSize();
    ASSERT_EQ(w->AppendPut("b", "2"), Status::kOk);
    ASSERT_EQ(w->AppendPut("c", "3"), Status::kOk);
  }
  {
    std::fstream f(path_, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(static_cast<std::streamoff>(end_of_one + 8 + 1));  // inside record 2's payload
    f.put('\x7f');
  }
  auto r = Replay();
  ASSERT_EQ(r.size(), 1u);  // records after the first bad one are not trusted
  EXPECT_EQ(r[0].key, "a");
}

TEST_F(WalTest, AbsurdLengthFieldIsTreatedAsCorrupt) {
  {
    std::ofstream out(path_, std::ios::binary);
    const unsigned char hdr[8] = {0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF};
    out.write(reinterpret_cast<const char*>(hdr), 8);
  }
  ReplayStats s;
  EXPECT_TRUE(Replay(&s).empty());
  EXPECT_EQ(s.discarded_bytes, 8u);
  EXPECT_EQ(FileSize(), 0u);
}

TEST_F(WalTest, GarbageFileYieldsNothing) {
  {
    std::ofstream out(path_, std::ios::binary);
    out << "this is not a wal file at all, just text";
  }
  EXPECT_TRUE(Replay().empty());
  EXPECT_EQ(FileSize(), 0u);
}

TEST_F(WalTest, AppendAfterRecoveryOfTornTail) {
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("a", "1"), Status::kOk);
    ASSERT_EQ(w->AppendPut("b", "2"), Status::kOk);
  }
  ASSERT_EQ(::truncate(path_.c_str(), static_cast<off_t>(FileSize() - 3)), 0);  // simulate crash
  ASSERT_EQ(Replay().size(), 1u);                                               // repairs file
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("c", "3"), Status::kOk);
  }
  auto r = Replay();
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0].key, "a");
  EXPECT_EQ(r[1].key, "c");
}

TEST_F(WalTest, ReopenContinuesAppending) {
  { ASSERT_EQ(OpenWal()->AppendPut("a", "1"), Status::kOk); }
  { ASSERT_EQ(OpenWal()->AppendPut("b", "2"), Status::kOk); }
  EXPECT_EQ(Replay().size(), 2u);
}

TEST_F(WalTest, ReplayIsRepeatable) {
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("a", "1"), Status::kOk);
    ASSERT_EQ(w->AppendDelete("a"), Status::kOk);
  }
  EXPECT_EQ(Replay().size(), 2u);
  EXPECT_EQ(Replay().size(), 2u);
}