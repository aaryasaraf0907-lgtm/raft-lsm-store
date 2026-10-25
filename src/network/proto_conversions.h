#pragma once

#include "raft/rpc_messages.h"
#include "raft_rpc.pb.h"

namespace raftkv::network {

proto::RequestVoteRequest ToProto(const RequestVoteRequest& message);
proto::RequestVoteReply ToProto(const RequestVoteReply& message);
proto::AppendEntriesRequest ToProto(const AppendEntriesRequest& message);
proto::AppendEntriesReply ToProto(const AppendEntriesReply& message);
proto::PreVoteRequest ToProto(const PreVoteRequest& message);
proto::PreVoteReply ToProto(const PreVoteReply& message);
proto::InstallSnapshotRequest ToProto(const InstallSnapshotRequest& message);
proto::InstallSnapshotReply ToProto(const InstallSnapshotReply& message);

RequestVoteRequest FromProto(const proto::RequestVoteRequest& message);
RequestVoteReply FromProto(const proto::RequestVoteReply& message);
AppendEntriesRequest FromProto(const proto::AppendEntriesRequest& message);
AppendEntriesReply FromProto(const proto::AppendEntriesReply& message);
PreVoteRequest FromProto(const proto::PreVoteRequest& message);
PreVoteReply FromProto(const proto::PreVoteReply& message);
InstallSnapshotRequest FromProto(const proto::InstallSnapshotRequest& message);
InstallSnapshotReply FromProto(const proto::InstallSnapshotReply& message);

}  // namespace raftkv::network
