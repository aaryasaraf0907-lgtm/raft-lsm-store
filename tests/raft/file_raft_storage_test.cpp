#include <gtest/gtest.h>

#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "raft/file_raft_storage.h"
#include "test_cluster.h"

using namespace raftkv;
using raftkv::testing_util::TempDir;

namespace {

LogEntry MakeEntry(LogIndex index, Term term, const std::string& key, const std::string& value,
                   CommandType type = CommandType::kPut) {
  return LogEntry{term, index, Command{type, key, value}};
}

PersistentState LoadFrom(const std::string& dir) {
  FileRaftStorage storage(dir);
  EXPECT_TRUE(storage.ok());
  PersistentState state;
  EXPECT_TRUE(storage.Load(&state));
  return state;
}

std::string ReadFile(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void WriteFile(const std::string& path, const std::string& bytes) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

}  // namespace

TEST(FileRaftStorage, EmptyDirectoryLoadsEmptyState) {
  TempDir dir;
  PersistentState state = LoadFrom(dir.path.string());
  EXPECT_EQ(state.term, 0u);
  EXPECT_EQ(state.voted_for, 0u);
  EXPECT_TRUE(state.log.empty());
}

TEST(FileRaftStorage, CreatesMissingDirectories) {
  TempDir dir;
  std::string nested = (dir.path / "a" / "b" / "node1").string();
  FileRaftStorage storage(nested);
  EXPECT_TRUE(storage.ok());
}

TEST(FileRaftStorage, HardStateLastWriteWins) {
  TempDir dir;
  {
    FileRaftStorage s(dir.path.string());
    PersistentState st;
    ASSERT_TRUE(s.Load(&st));
    ASSERT_TRUE(s.SaveHardState(3, 2));
    ASSERT_TRUE(s.SaveHardState(4, 0));
    ASSERT_TRUE(s.SaveHardState(4, 5));
  }
  PersistentState state = LoadFrom(dir.path.string());
  EXPECT_EQ(state.term, 4u);
  EXPECT_EQ(state.voted_for, 5u);
}

TEST(FileRaftStorage, EntriesRoundTrip) {
  TempDir dir;
  std::vector<LogEntry> entries = {
      MakeEntry(1, 1, "", "", CommandType::kNoop),
      MakeEntry(2, 1, "key", "value"),
      MakeEntry(3, 2, std::string("bin\0key", 7), std::string("\0\xff\x01", 3)),
      MakeEntry(4, 2, "gone", "", CommandType::kDelete),
      MakeEntry(5, 3, "big", std::string(100000, 'x')),
  };
  {
    FileRaftStorage s(dir.path.string());
    PersistentState st;
    ASSERT_TRUE(s.Load(&st));
    ASSERT_TRUE(s.AppendEntries({entries[0], entries[1]}));  // two batches
    ASSERT_TRUE(s.AppendEntries({entries[2], entries[3], entries[4]}));
  }
  PersistentState state = LoadFrom(dir.path.string());
  ASSERT_EQ(state.log.size(), entries.size());
  for (size_t i = 0; i < entries.size(); ++i) {
    EXPECT_EQ(state.log[i].index, entries[i].index);
    EXPECT_EQ(state.log[i].term, entries[i].term);
    EXPECT_EQ(state.log[i].command.type, entries[i].command.type);
    EXPECT_EQ(state.log[i].command.key, entries[i].command.key);
    EXPECT_EQ(state.log[i].command.value, entries[i].command.value);
  }
}

TEST(FileRaftStorage, TruncateThenAppendReplacesTheSuffix) {
  TempDir dir;
  {
    FileRaftStorage s(dir.path.string());
    PersistentState st;
    ASSERT_TRUE(s.Load(&st));
    ASSERT_TRUE(s.AppendEntries({MakeEntry(1, 1, "a", "1"), MakeEntry(2, 1, "b", "2"),
                                 MakeEntry(3, 1, "c", "3"), MakeEntry(4, 1, "d", "4")}));
    ASSERT_TRUE(s.TruncateSuffix(3));  // drop entries 3 and 4
    ASSERT_TRUE(s.AppendEntries({MakeEntry(3, 2, "c2", "33")}));
  }
  PersistentState state = LoadFrom(dir.path.string());
  ASSERT_EQ(state.log.size(), 3u);
  EXPECT_EQ(state.log[1].command.key, "b");
  EXPECT_EQ(state.log[2].command.key, "c2");
  EXPECT_EQ(state.log[2].term, 2u);
}

TEST(FileRaftStorage, TornTailIsDroppedAndFileIsRepaired) {
  TempDir dir;
  {
    FileRaftStorage s(dir.path.string());
    PersistentState st;
    ASSERT_TRUE(s.Load(&st));
    ASSERT_TRUE(s.SaveHardState(2, 1));
    ASSERT_TRUE(s.AppendEntries({MakeEntry(1, 2, "a", "1")}));
    ASSERT_TRUE(s.AppendEntries({MakeEntry(2, 2, "b", "2")}));
  }
  // Simulate a crash in the middle of writing the last record: cut off its final bytes.
  std::string path = (dir.path / "raft.log").string();
  std::string bytes = ReadFile(path);
  WriteFile(path, bytes.substr(0, bytes.size() - 5));

  {
    FileRaftStorage s(dir.path.string());
    PersistentState st;
    ASSERT_TRUE(s.Load(&st));
    ASSERT_EQ(st.log.size(), 1u) << "incomplete last record must be ignored";
    EXPECT_EQ(st.term, 2u);
    // The damaged tail was removed, so new writes line up and are readable afterwards.
    ASSERT_TRUE(s.AppendEntries({MakeEntry(2, 2, "b2", "22")}));
  }
  PersistentState state = LoadFrom(dir.path.string());
  ASSERT_EQ(state.log.size(), 2u);
  EXPECT_EQ(state.log[1].command.key, "b2");
}

TEST(FileRaftStorage, GarbageAppendedAfterCrashIsIgnored) {
  TempDir dir;
  {
    FileRaftStorage s(dir.path.string());
    PersistentState st;
    ASSERT_TRUE(s.Load(&st));
    ASSERT_TRUE(s.AppendEntries({MakeEntry(1, 1, "a", "1")}));
  }
  std::string path = (dir.path / "raft.log").string();
  WriteFile(path, ReadFile(path) + std::string("\x07\x00\x00\x00\xde\xad\xbe\xef\x01\x02", 10));
  PersistentState state = LoadFrom(dir.path.string());
  EXPECT_EQ(state.log.size(), 1u);
}

TEST(FileRaftStorage, FlippedBitInLastRecordIsDetected) {
  TempDir dir;
  {
    FileRaftStorage s(dir.path.string());
    PersistentState st;
    ASSERT_TRUE(s.Load(&st));
    ASSERT_TRUE(s.AppendEntries({MakeEntry(1, 1, "a", "1")}));
    ASSERT_TRUE(s.AppendEntries({MakeEntry(2, 1, "b", "2")}));
  }
  std::string path = (dir.path / "raft.log").string();
  std::string bytes = ReadFile(path);
  bytes[bytes.size() - 2] ^= 0x40;  // corrupt a byte inside the last record
  WriteFile(path, bytes);
  PersistentState state = LoadFrom(dir.path.string());
  EXPECT_EQ(state.log.size(), 1u) << "record with a bad checksum must not be trusted";
}
