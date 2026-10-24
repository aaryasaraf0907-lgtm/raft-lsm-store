#include "network/proto_conversions.h"

#include <cstddef>
#include <stdexcept>

namespace raftkv::network {
namespace {

proto::CommandType ToProto(CommandType type) {
  switch (type) {
    case CommandType::kPut:
      return proto::COMMAND_TYPE_PUT;
    case CommandType::kDelete:
      return proto::COMMAND_TYPE_DELETE;
    case CommandType::kNoop:
      return proto::COMMAND_TYPE_NOOP;
  }
  throw std::invalid_argument("unsupported Raft command type");
}

CommandType FromProto(proto::CommandType type) {
  switch (type) {
    case proto::COMMAND_TYPE_PUT:
      return CommandType::kPut;
    case proto::COMMAND_TYPE_DELETE:
      return CommandType::kDelete;
    case proto::COMMAND_TYPE_NOOP:
      return CommandType::kNoop;
    default:
      throw std::invalid_argument("unsupported Protobuf command type");
  }
}

proto::LogEntry ToProto(const LogEntry& entry) {
  proto::LogEntry converted;
  converted.set_term(entry.term);
  converted.set_index(entry.index);
  auto* command = converted.mutable_command();
  command->set_type(ToProto(entry.command.type));
  command->set_key(entry.command.key);
  command->set_value(entry.command.value);
  command->set_client_id(entry.command.client_id);
  command->set_seq(entry.command.seq);
  return converted;
}

LogEntry FromProto(const proto::LogEntry& entry) {
  if (!entry.has_command()) {
    throw std::invalid_argument("Protobuf log entry is missing its command");
  }
  return {entry.term(),
          entry.index(),
          {FromProto(entry.command().type()),
           entry.command().key(),
           entry.command().value(),
           entry.command().client_id(),
           entry.command().seq()}};
}

}  // namespace

proto::RequestVoteRequest ToProto(const RequestVoteRequest& message) {
  proto::RequestVoteRequest converted;
  converted.set_term(message.term);
  converted.set_candidate_id(message.candidate_id);
  converted.set_last_log_index(message.last_log_index);
  converted.set_last_log_term(message.last_log_term);
  return converted;
}

proto::RequestVoteReply ToProto(const RequestVoteReply& message) {
  proto::RequestVoteReply converted;
  converted.set_term(message.term);
  converted.set_vote_granted(message.vote_granted);
  return converted;
}

proto::AppendEntriesRequest ToProto(const AppendEntriesRequest& message) {
  proto::AppendEntriesRequest converted;
  converted.set_term(message.term);
  converted.set_leader_id(message.leader_id);
  converted.set_prev_log_index(message.prev_log_index);
  converted.set_prev_log_term(message.prev_log_term);
  for (const auto& entry : message.entries) {
    *converted.add_entries() = ToProto(entry);
  }
  converted.set_leader_commit(message.leader_commit);
  return converted;
}

proto::AppendEntriesReply ToProto(const AppendEntriesReply& message) {
  proto::AppendEntriesReply converted;
  converted.set_term(message.term);
  converted.set_success(message.success);
  converted.set_match_index(message.match_index);
  return converted;
}

RequestVoteRequest FromProto(const proto::RequestVoteRequest& message) {
  return {message.term(), message.candidate_id(), message.last_log_index(),
          message.last_log_term()};
}

RequestVoteReply FromProto(const proto::RequestVoteReply& message) {
  return {message.term(), message.vote_granted()};
}

AppendEntriesRequest FromProto(const proto::AppendEntriesRequest& message) {
  AppendEntriesRequest converted{message.term(),
                                 message.leader_id(),
                                 message.prev_log_index(),
                                 message.prev_log_term(),
                                 {},
                                 message.leader_commit()};
  converted.entries.reserve(
      static_cast<std::size_t>(message.entries_size()));
  for (const auto& entry : message.entries()) {
    converted.entries.push_back(FromProto(entry));
  }
  return converted;
}

AppendEntriesReply FromProto(const proto::AppendEntriesReply& message) {
  return {message.term(), message.success(), message.match_index()};
}

}  // namespace raftkv::network
