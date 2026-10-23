#pragma once

#include "raft/rpc_messages.h"
#include "raft_rpc.pb.h"

namespace raftkv::network {

proto::RequestVoteRequest ToProto(const RequestVoteRequest& message);
proto::RequestVoteReply ToProto(const RequestVoteReply& message);
proto::AppendEntriesRequest ToProto(const AppendEntriesRequest& message);
proto::AppendEntriesReply ToProto(const AppendEntriesReply& message);

RequestVoteRequest FromProto(const proto::RequestVoteRequest& message);
RequestVoteReply FromProto(const proto::RequestVoteReply& message);
AppendEntriesRequest FromProto(const proto::AppendEntriesRequest& message);
AppendEntriesReply FromProto(const proto::AppendEntriesReply& message);

}  // namespace raftkv::network
