#include "storage/wal.h"

#include <gtest/gtest.h>
#ifdef _WIN32
#include <io.h>
#else
#ifndef _WIN32
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#endif
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cerrno>
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
    static std::atomic<uint64_t> next_id{0};
    const auto timestamp = std::chrono::high_resolution_clock::now()
                              .time_since_epoch()
                              .count();
    dir_ = (std::filesystem::temp_directory_path() /
            ("raftkv_wal_" + std::to_string(timestamp) + "_" +
             std::to_string(next_id++))).string();
    std::error_code ec;
    ASSERT_TRUE(std::filesystem::create_directories(dir_, ec));
    ASSERT_FALSE(ec);
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
  std::string k("\0k\xff\x1a\n\r", 6), v("\0\0v\n\x1a\r", 6);
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

TEST_F(WalTest, CorruptionInMiddleIsReportedAndNothingIsDeleted) {
  uint64_t end_of_one = 0, full = 0;
  {
    auto w = OpenWal();
    ASSERT_EQ(w->AppendPut("a", "1"), Status::kOk);
    end_of_one = FileSize();
    ASSERT_EQ(w->AppendPut("b", "2"), Status::kOk);
    ASSERT_EQ(w->AppendPut("c", "3"), Status::kOk);
    full = FileSize();
  }
  {
    std::fstream f(path_, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(static_cast<std::streamoff>(end_of_one + 8 + 1));  // inside record 2's payload
    f.put('\x7f');
  }
  std::vector<Entry> out;
  EXPECT_EQ(WAL::Replay(path_, [&](const Entry& e) { out.push_back(e); }), Status::kCorruption);
  EXPECT_EQ(FileSize(), full) << "intact records after the damage must not be deleted";
}

TEST_F(WalTest, FailedWriteStopsTheLogInsteadOfHidingLaterRecords) {
#ifndef _WIN32
  std::signal(SIGXFSZ, SIG_IGN);  // exceeding the file-size limit then fails write() with EFBIG
  auto w = OpenWal();
  ASSERT_EQ(w->AppendPut("a", "1"), Status::kOk);
  uint64_t one_record = FileSize();

  struct rlimit original;
  ASSERT_EQ(getrlimit(RLIMIT_FSIZE, &original), 0);
  struct rlimit tiny = {static_cast<rlim_t>(one_record + 10), original.rlim_max};
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &tiny), 0);
  Status failed = w->AppendPut("b", "2");  // runs out of room halfway through the record
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &original), 0);  // the "disk" has space again
  EXPECT_EQ(failed, Status::kIOError);

  // The log may now end in a partial record, so it must refuse to acknowledge anything else.
  EXPECT_EQ(w->AppendPut("c", "3"), Status::kIOError);
  EXPECT_EQ(w->Sync(), Status::kIOError);
  w.reset();
  auto r = Replay();
  ASSERT_EQ(r.size(), 1u);
  EXPECT_EQ(r[0].key, "a");
#else
  GTEST_SKIP() << "POSIX file-size limit is unavailable on Windows; "
                  "the WAL-level injected partial-write test covers failure handling";
#endif
}

TEST_F(WalTest, InjectedPartialWriteFailurePoisonsWal) {
  int calls = 0;
  auto writer = [&calls](int fd, const char* data,
                         size_t size) -> std::ptrdiff_t {
    ++calls;
    if (calls == 3) {
      errno = EIO;
      return -1;
    }
#ifdef _WIN32
    const unsigned int bytes =
        static_cast<unsigned int>(calls == 2 ? std::min<size_t>(10, size) : size);
    return static_cast<std::ptrdiff_t>(::_write(fd, data, bytes));
#else
    return static_cast<std::ptrdiff_t>(
        ::write(fd, data, calls == 2 ? std::min<size_t>(10, size) : size));
#endif
  };
  std::unique_ptr<WAL> wal;
  ASSERT_EQ(WAL::OpenForTesting(path_, &wal, writer), Status::kOk);

  ASSERT_EQ(wal->AppendPut("first", "record"), Status::kOk);
  EXPECT_EQ(wal->AppendPut("partial", "record"), Status::kIOError);
  EXPECT_EQ(wal->AppendPut("later", "record"), Status::kIOError);
  EXPECT_EQ(wal->Sync(), Status::kIOError);
  wal.reset();

  const auto recovered = Replay();
  ASSERT_EQ(recovered.size(), 1u);
  EXPECT_EQ(recovered[0].key, "first");
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
  std::error_code ec;
  std::filesystem::resize_file(path_, FileSize() - 3, ec);  // simulate crash
  ASSERT_FALSE(ec);
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