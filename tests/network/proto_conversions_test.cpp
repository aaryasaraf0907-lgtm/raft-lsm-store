#include "network/proto_conversions.h"

#include <cstddef>
#include <limits>
#include <stdexcept>

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
      {{9, 10, {CommandType::kPut, "key", "value"}},
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

TEST(ProtoConversionsTest, RejectsInvalidOrMissingCommand) {
  LogEntry invalid{1, 1, {static_cast<CommandType>(255), "", ""}};

  EXPECT_THROW(ToProto(AppendEntriesRequest{1, 1, 0, 0, {invalid}, 0}),
               std::invalid_argument);

  // Test missing commands through the public AppendEntries conversion.
  proto::AppendEntriesRequest missing_command;
  missing_command.add_entries();

  EXPECT_THROW(FromProto(missing_command), std::invalid_argument);
}

}  // namespace
}  // namespace raftkv::network
