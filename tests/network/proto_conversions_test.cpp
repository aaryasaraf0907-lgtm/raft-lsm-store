#include "network/proto_conversions.h"

#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

namespace raftkv::network {
namespace {

TEST(ProtoConversionsTest, RoundTripsRequestVoteMessages) {
  const RequestVoteRequest request{std::numeric_limits<Term>::max(),
                                   std::numeric_limits<NodeId>::max(),
                                   std::numeric_limits<LogIndex>::max() - 1,
                                   std::numeric_limits<Term>::max() - 2};
  const RequestVoteReply reply{request.term, true};

  const auto decoded_request = FromProto(ToProto(request));
  const auto decoded_reply = FromProto(ToProto(reply));

  EXPECT_EQ(decoded_request.term, request.term);
  EXPECT_EQ(decoded_request.candidate_id, request.candidate_id);
  EXPECT_EQ(decoded_request.last_log_index, request.last_log_index);
  EXPECT_EQ(decoded_request.last_log_term, request.last_log_term);
  EXPECT_EQ(decoded_reply.term, reply.term);
  EXPECT_EQ(decoded_reply.vote_granted, reply.vote_granted);
}

TEST(ProtoConversionsTest, RoundTripsAppendEntriesWithNestedEntries) {
  const AppendEntriesRequest request{
      12,
      7,
      10,
      9,
      {{9, 10,
        {CommandType::kPut, "key", "value", 0xABCDEF0123456789ull, 17}},
       {11, 11, {CommandType::kDelete, "old", ""}},
       {12, 12, {CommandType::kNoop, "", ""}}},
      10};
  const AppendEntriesReply reply{12, true, 10};

  const auto decoded_request = FromProto(ToProto(request));
  const auto decoded_reply = FromProto(ToProto(reply));

  ASSERT_EQ(decoded_request.entries.size(), request.entries.size());

  for (std::size_t i = 0; i < request.entries.size(); ++i) {
    EXPECT_EQ(decoded_request.entries[i].term, request.entries[i].term);
    EXPECT_EQ(decoded_request.entries[i].index, request.entries[i].index);
    EXPECT_EQ(decoded_request.entries[i].command.type,
              request.entries[i].command.type);
    EXPECT_EQ(decoded_request.entries[i].command.key,
              request.entries[i].command.key);
    EXPECT_EQ(decoded_request.entries[i].command.value,
              request.entries[i].command.value);
    EXPECT_EQ(decoded_request.entries[i].command.client_id,
              request.entries[i].command.client_id);
    EXPECT_EQ(decoded_request.entries[i].command.seq,
              request.entries[i].command.seq);
  }

  EXPECT_EQ(decoded_reply.term, reply.term);
  EXPECT_EQ(decoded_reply.success, reply.success);
  EXPECT_EQ(decoded_reply.match_index, reply.match_index);
}

TEST(ProtoConversionsTest, PreservesEmptyAppendEntriesHeartbeat) {
  const AppendEntriesRequest heartbeat{4, 2, 3, 3, {}, 3};

  const auto wire = ToProto(heartbeat);
  EXPECT_EQ(wire.entries_size(), 0);

  const auto decoded = FromProto(wire);
  EXPECT_TRUE(decoded.entries.empty());
  EXPECT_EQ(decoded.leader_commit, heartbeat.leader_commit);
}

TEST(ProtoConversionsTest, PreservesBinaryCommandDataOnWireRoundTrip) {
  const std::string key{"k\xff\xfe", 3};
  const std::string value{"v\0x\0", 4};
  const AppendEntriesRequest request{
      1, 1, 0, 0,
      {{1, 1, {CommandType::kPut, key, value, 0xFEDCBA9876543210ull, 29}}},
      1};

  std::string serialized;
  ASSERT_TRUE(ToProto(request).SerializeToString(&serialized));

  proto::AppendEntriesRequest parsed;
  ASSERT_TRUE(parsed.ParseFromString(serialized));
  const auto decoded = FromProto(parsed);

  ASSERT_EQ(decoded.entries.size(), 1);
  EXPECT_EQ(decoded.entries[0].command.key, key);
  EXPECT_EQ(decoded.entries[0].command.value, value);
  EXPECT_EQ(decoded.entries[0].command.client_id, 0xFEDCBA9876543210ull);
  EXPECT_EQ(decoded.entries[0].command.seq, 29u);
}

TEST(ProtoConversionsTest, PreservesZeroRequestIds) {
  const AppendEntriesRequest request{
      1, 1, 0, 0, {{1, 1, {CommandType::kPut, "key", "value"}}}, 1};

  const auto decoded = FromProto(ToProto(request));

  ASSERT_EQ(decoded.entries.size(), 1);
  EXPECT_EQ(decoded.entries[0].command.client_id, 0u);
  EXPECT_EQ(decoded.entries[0].command.seq, 0u);
}

TEST(ProtoConversionsTest, RejectsInvalidOrMissingCommand) {
  LogEntry invalid{1, 1, {static_cast<CommandType>(255), "", ""}};

  EXPECT_THROW(ToProto(AppendEntriesRequest{1, 1, 0, 0, {invalid}, 0}),
               std::invalid_argument);

  proto::AppendEntriesRequest unknown_command_type;
  unknown_command_type.add_entries()->mutable_command()->set_type(
      static_cast<proto::CommandType>(255));

  EXPECT_THROW(FromProto(unknown_command_type), std::invalid_argument);

  // Test missing commands through the public AppendEntries conversion.
  proto::AppendEntriesRequest missing_command;
  missing_command.add_entries();

  EXPECT_THROW(FromProto(missing_command), std::invalid_argument);
}

TEST(ProtoConversionsTest, RoundTripsPreVoteMessages) {
  const PreVoteRequest request{std::numeric_limits<uint64_t>::max(), 7, 123456789012ULL, 42};
  const auto wire_request = ToProto(request);
  EXPECT_EQ(wire_request.term(), request.term);
  const auto back = FromProto(wire_request);
  EXPECT_EQ(back.term, request.term);
  EXPECT_EQ(back.candidate_id, 7u);
  EXPECT_EQ(back.last_log_index, 123456789012ULL);
  EXPECT_EQ(back.last_log_term, 42u);

  for (bool granted : {true, false}) {
    const auto reply = FromProto(ToProto(PreVoteReply{9, granted}));
    EXPECT_EQ(reply.term, 9u);
    EXPECT_EQ(reply.granted, granted);
  }
}

TEST(ProtoConversionsTest, RoundTripsInstallSnapshotChunksWithBinaryData) {
  // Snapshot bytes are binary: NULs, 0xFF and invalid UTF-8 must survive (proto `bytes`, not
  // `string`), and a full-size chunk must round-trip unchanged.
  std::string data(1024 * 1024, '\0');
  for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>(i * 131 + 7);
  data[0] = '\0';
  data[1] = '\xff';
  data[2] = '\xc3';  // a truncated UTF-8 sequence
  const InstallSnapshotRequest request{5, 2, 9000, 4, 3 * 1024 * 1024, data, true};
  const auto back = FromProto(ToProto(request));
  EXPECT_EQ(back.term, 5u);
  EXPECT_EQ(back.leader_id, 2u);
  EXPECT_EQ(back.last_included_index, 9000u);
  EXPECT_EQ(back.last_included_term, 4u);
  EXPECT_EQ(back.offset, 3u * 1024 * 1024);
  EXPECT_EQ(back.data, data);
  EXPECT_TRUE(back.done);

  const auto reply = FromProto(ToProto(InstallSnapshotReply{6, 77, true}));
  EXPECT_EQ(reply.term, 6u);
  EXPECT_EQ(reply.next_offset, 77u);
  EXPECT_TRUE(reply.installed);

  const auto empty = FromProto(ToProto(InstallSnapshotRequest{1, 1, 1, 1, 0, "", false}));
  EXPECT_TRUE(empty.data.empty());
  EXPECT_FALSE(empty.done);
}

}  // namespace
}  // namespace raftkv::network
