#include <gtest/gtest.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "server/client_service.h"
#include "server/kv_state_machine.h"

namespace raftkv {
namespace {

class TestStore final : public KVStore {
 public:
  Status Put(const std::string& key, const std::string& value) override {
    ++put_calls;
    values[key] = value;
    return Status::kOk;
  }

  Status Get(const std::string& key, std::string* value) override {
    ++get_calls;
    if (forced_get_status != Status::kOk) return forced_get_status;
    const auto found = values.find(key);
    if (found == values.end()) return Status::kNotFound;
    *value = found->second;
    return Status::kOk;
  }

  Status Delete(const std::string& key) override {
    ++delete_calls;
    return values.erase(key) == 0 ? Status::kNotFound : Status::kOk;
  }

  std::map<std::string, std::string> values;
  int put_calls = 0;
  int get_calls = 0;
  int delete_calls = 0;
  Status forced_get_status = Status::kOk;  // returned by Get when not kOk
};

class TestRaftNode final : public RaftNode {
 public:
  void Start() override {}
  void Stop() override {}
  ProposeResponse Propose(const Command&) override {
    return {ProposeResult::kNotLeader, 0, next_response.leader_hint};
  }
  CommitResponse ProposeAndWait(const Command& command,
                                std::chrono::milliseconds timeout) override {
    proposals.push_back(command);
    last_timeout = timeout;
    CommitResponse response = next_response;
    response.index = ++last_index;
    if (deferred_timeout_ && apply_callback) {
      apply_callback(LogEntry{1, deferred_timeout_index_, deferred_timeout_command_});
      deferred_timeout_ = false;
    }
    if (response.result == CommitResult::kTimeout &&
        defer_timeout_until_next_request) {
      deferred_timeout_command_ = command;
      deferred_timeout_index_ = response.index;
      deferred_timeout_ = true;
    } else if (response.result == CommitResult::kApplied &&
               command.type != CommandType::kNoop && apply_callback) {
      apply_callback(LogEntry{1, response.index, command});
    }
    return response;
  }
  // Reads must use ReadIndex, which writes nothing to the log.
  CommitResponse ReadIndex(std::chrono::milliseconds timeout) override {
    ++read_index_calls;
    last_timeout = timeout;
    return next_response;
  }
  void SetApplyCallback(ApplyCallback callback) override {
    apply_callback = std::move(callback);
  }
  bool IsLeader() const override { return is_leader; }
  Term CurrentTerm() const override { return 1; }

  CommitResponse next_response{CommitResult::kApplied, 0, 1};
  ApplyCallback apply_callback;
  std::vector<Command> proposals;
  int read_index_calls = 0;
  LogIndex last_index = 0;
  std::chrono::milliseconds last_timeout{0};
  bool is_leader = true;
  bool defer_timeout_until_next_request = false;

 private:
  Command deferred_timeout_command_{CommandType::kNoop, "", ""};
  LogIndex deferred_timeout_index_ = 0;
  bool deferred_timeout_ = false;
};

network::proto::PutRequest PutRequest(uint64_t client_id = 7,
                                     uint64_t seq = 1) {
  network::proto::PutRequest request;
  request.set_client_id(client_id);
  request.set_seq(seq);
  request.set_key("key");
  request.set_value("value");
  request.set_timeout_ms(100);
  return request;
}

network::proto::DeleteRequest DeleteRequest(uint64_t client_id = 7,
                                            uint64_t seq = 1) {
  network::proto::DeleteRequest request;
  request.set_client_id(client_id);
  request.set_seq(seq);
  request.set_key("key");
  request.set_timeout_ms(100);
  return request;
}

network::proto::GetRequest GetRequest() {
  network::proto::GetRequest request;
  request.set_key("key");
  request.set_timeout_ms(100);
  return request;
}

TEST(ClientServiceTest, RejectsInvalidWriteIdsKeysAndTimeouts) {
  TestStore store;
  TestRaftNode node;
  KVStateMachine machine(&store);
  ClientService service(&node, &store, &machine);
  network::proto::WriteResponse response;

  auto put = PutRequest(0, 1);
  EXPECT_TRUE(service.Put(nullptr, &put, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  put = PutRequest(1, 0);
  EXPECT_TRUE(service.Put(nullptr, &put, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  put = PutRequest();
  put.clear_key();
  EXPECT_TRUE(service.Put(nullptr, &put, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  put = PutRequest();
  put.set_timeout_ms(0);
  EXPECT_TRUE(service.Put(nullptr, &put, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  put.set_timeout_ms(30001);
  EXPECT_TRUE(service.Put(nullptr, &put, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);

  auto del = DeleteRequest(0, 1);
  EXPECT_TRUE(service.Delete(nullptr, &del, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  del = DeleteRequest();
  del.set_timeout_ms(0);
  EXPECT_TRUE(service.Delete(nullptr, &del, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  EXPECT_TRUE(node.proposals.empty());
}

TEST(ClientServiceTest, RejectsInvalidGetKeyAndTimeout) {
  TestStore store;
  TestRaftNode node;
  KVStateMachine machine(&store);
  ClientService service(&node, &store, &machine);
  network::proto::GetResponse response;

  auto get = GetRequest();
  get.clear_key();
  EXPECT_TRUE(service.Get(nullptr, &get, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  get = GetRequest();
  get.set_timeout_ms(30001);
  EXPECT_TRUE(service.Get(nullptr, &get, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  EXPECT_TRUE(node.proposals.empty());
  EXPECT_EQ(node.read_index_calls, 0);
  EXPECT_EQ(store.get_calls, 0);
}

TEST(ClientServiceTest, MapsEveryCommitResultAndPreservesLeaderHint) {
  TestStore store;
  KVStateMachine machine(&store);
  TestRaftNode node;
  node.SetApplyCallback(machine.AsCallback());
  ClientService service(&node, &store, &machine);
  const std::vector<std::pair<CommitResult, network::proto::ClientStatus>> cases{
      {CommitResult::kApplied, network::proto::CLIENT_STATUS_OK},
      {CommitResult::kNotLeader, network::proto::CLIENT_STATUS_NOT_LEADER},
      {CommitResult::kLost, network::proto::CLIENT_STATUS_NOT_COMMITTED},
      {CommitResult::kTimeout,
       network::proto::CLIENT_STATUS_OUTCOME_UNKNOWN},
      {CommitResult::kStopped, network::proto::CLIENT_STATUS_STOPPED}};

  for (const auto& test_case : cases) {
    node.next_response = {test_case.first, 0, 42};
    network::proto::WriteResponse response;
    const auto request = PutRequest();
    ASSERT_TRUE(service.Put(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status(), test_case.second);
    EXPECT_EQ(response.leader_id_hint(), 42u);
    if (test_case.first == CommitResult::kTimeout) {
      EXPECT_NE(response.error_message().find("outcome is unknown"),
                std::string::npos);
    }
  }
}

TEST(ClientServiceTest, NonLeaderReturnsHintWithoutReadingOrWriting) {
  TestStore store;
  KVStateMachine machine(&store);
  TestRaftNode node;
  node.next_response = {CommitResult::kNotLeader, 0, 9};
  ClientService service(&node, &store, &machine);

  network::proto::WriteResponse write_response;
  const auto put = PutRequest();
  ASSERT_TRUE(service.Put(nullptr, &put, &write_response).ok());
  EXPECT_EQ(write_response.status(), network::proto::CLIENT_STATUS_NOT_LEADER);
  EXPECT_EQ(write_response.leader_id_hint(), 9u);
  EXPECT_EQ(store.put_calls, 0);

  network::proto::GetResponse get_response;
  const auto get = GetRequest();
  ASSERT_TRUE(service.Get(nullptr, &get, &get_response).ok());
  EXPECT_EQ(get_response.status(), network::proto::CLIENT_STATUS_NOT_LEADER);
  EXPECT_EQ(get_response.leader_id_hint(), 9u);
  EXPECT_EQ(store.get_calls, 0);
}

TEST(ClientServiceTest, RetryOfAnAppliedRequestReportsSuccessWithoutApplyingTwice) {
  TestStore store;
  KVStateMachine machine(&store);
  TestRaftNode node;
  node.SetApplyCallback(machine.AsCallback());
  ClientService service(&node, &store, &machine);

  network::proto::WriteResponse response;
  auto request = PutRequest(7, 1);
  request.set_value("first");
  ASSERT_TRUE(service.Put(nullptr, &request, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OK);

  // The client never saw the first answer and retries with the same request id.
  request.set_value("same-sequence retry");
  ASSERT_TRUE(service.Put(nullptr, &request, &response).ok());

  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OK);
  EXPECT_EQ(store.values["key"], "first");  // applied exactly once, with the first value
  EXPECT_EQ(store.put_calls, 1);
  EXPECT_EQ(machine.DuplicatesSkipped(), 1u);
  ASSERT_EQ(node.proposals.size(), 2u);
  EXPECT_EQ(node.proposals[1].client_id, 7u);
  EXPECT_EQ(node.proposals[1].seq, 1u);
}

TEST(ClientServiceTest, StaleSequenceAfterNewerSequenceIsNotReportedSuccessful) {
  TestStore store;
  KVStateMachine machine(&store);
  TestRaftNode node;
  node.SetApplyCallback(machine.AsCallback());
  ClientService service(&node, &store, &machine);

  network::proto::WriteResponse response;
  auto request = PutRequest(7, 2);
  request.set_value("newer");
  ASSERT_TRUE(service.Put(nullptr, &request, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OK);

  request = PutRequest(7, 1);
  request.set_value("stale");
  ASSERT_TRUE(service.Put(nullptr, &request, &response).ok());

  EXPECT_EQ(response.status(),
            network::proto::CLIENT_STATUS_DUPLICATE_OUTCOME_UNKNOWN);
  EXPECT_EQ(store.values["key"], "newer");
  EXPECT_EQ(store.put_calls, 1);
  EXPECT_EQ(machine.DuplicatesSkipped(), 1u);
}

TEST(ClientServiceTest, RetryAfterATimeoutReportsSuccessIfTheTimedOutEntryApplied) {
  TestStore store;
  KVStateMachine machine(&store);
  TestRaftNode node;
  node.SetApplyCallback(machine.AsCallback());
  node.defer_timeout_until_next_request = true;
  node.next_response = {CommitResult::kTimeout, 0, 0};
  ClientService service(&node, &store, &machine);

  network::proto::WriteResponse response;
  const auto request = PutRequest(19, 8);
  ASSERT_TRUE(service.Put(nullptr, &request, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OUTCOME_UNKNOWN);
  EXPECT_EQ(store.put_calls, 0);

  node.next_response = {CommitResult::kApplied, 0, 1};
  ASSERT_TRUE(service.Put(nullptr, &request, &response).ok());

  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OK);
  EXPECT_EQ(store.values["key"], "value");
  EXPECT_EQ(store.put_calls, 1);
  EXPECT_EQ(machine.DuplicatesSkipped(), 1u);
  ASSERT_EQ(node.proposals.size(), 2u);
  EXPECT_EQ(node.proposals[0].client_id, node.proposals[1].client_id);
  EXPECT_EQ(node.proposals[0].seq, node.proposals[1].seq);
}

TEST(ClientServiceTest, DeleteIsProposedWithRequestIdAndTimeout) {
  TestStore store;
  store.values["key"] = "before";
  KVStateMachine machine(&store);
  TestRaftNode node;
  node.SetApplyCallback(machine.AsCallback());
  ClientService service(&node, &store, &machine);
  auto request = DeleteRequest(11, 4);
  request.set_timeout_ms(250);
  network::proto::WriteResponse response;

  ASSERT_TRUE(service.Delete(nullptr, &request, &response).ok());

  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OK);
  EXPECT_TRUE(store.values.empty());
  EXPECT_EQ(store.delete_calls, 1);
  ASSERT_EQ(node.proposals.size(), 1u);
  EXPECT_EQ(node.proposals[0].type, CommandType::kDelete);
  EXPECT_EQ(node.proposals[0].client_id, 11u);
  EXPECT_EQ(node.proposals[0].seq, 4u);
  EXPECT_EQ(node.last_timeout, std::chrono::milliseconds(250));
}

TEST(ClientServiceTest, GetUsesReadIndexAndWritesNothingToTheLog) {
  TestStore store;
  store.values["key"] = "value";
  KVStateMachine machine(&store);
  TestRaftNode node;
  ClientService service(&node, &store, &machine);

  node.next_response = {CommitResult::kTimeout, 0, 3};
  network::proto::GetResponse response;
  auto request = GetRequest();
  ASSERT_TRUE(service.Get(nullptr, &request, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OUTCOME_UNKNOWN);
  EXPECT_EQ(response.leader_id_hint(), 3u);
  EXPECT_EQ(store.get_calls, 0);

  node.next_response = {CommitResult::kApplied, 0, 2};
  ASSERT_TRUE(service.Get(nullptr, &request, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OK);
  EXPECT_EQ(response.value(), "value");
  EXPECT_EQ(response.leader_id_hint(), 2u);
  EXPECT_EQ(store.get_calls, 1);
  EXPECT_EQ(node.read_index_calls, 2);
  EXPECT_TRUE(node.proposals.empty()) << "a read must not append to the Raft log";
  EXPECT_EQ(node.last_timeout, std::chrono::milliseconds(100));
}

TEST(ClientServiceTest, GetReturnsNotFoundAfterBarrier) {
  TestStore store;
  KVStateMachine machine(&store);
  TestRaftNode node;
  ClientService service(&node, &store, &machine);
  network::proto::GetResponse response;
  const auto request = GetRequest();

  ASSERT_TRUE(service.Get(nullptr, &request, &response).ok());

  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_NOT_FOUND);
  EXPECT_FALSE(response.error_message().empty());
}

TEST(ClientServiceTest, EnforcesKeyAndValueSizeLimitsBeforeProposing) {
  TestStore store;
  KVStateMachine machine(&store);
  TestRaftNode node;
  node.SetApplyCallback(machine.AsCallback());
  ClientService service(&node, &store, &machine);
  network::proto::WriteResponse response;

  // One byte over the limits is rejected and never reaches Raft.
  auto put = PutRequest();
  put.set_key(std::string(ClientService::kMaxKeyBytes + 1, 'k'));
  ASSERT_TRUE(service.Put(nullptr, &put, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  put = PutRequest();
  put.set_value(std::string(ClientService::kMaxValueBytes + 1, 'v'));
  ASSERT_TRUE(service.Put(nullptr, &put, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  auto del = DeleteRequest();
  del.set_key(std::string(ClientService::kMaxKeyBytes + 1, 'k'));
  ASSERT_TRUE(service.Delete(nullptr, &del, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  EXPECT_TRUE(node.proposals.empty());
  EXPECT_EQ(store.put_calls, 0);

  // Exactly at the limits is accepted.
  put = PutRequest(7, 1);
  put.set_key(std::string(ClientService::kMaxKeyBytes, 'k'));
  put.set_value(std::string(ClientService::kMaxValueBytes, 'v'));
  ASSERT_TRUE(service.Put(nullptr, &put, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OK);
  del = DeleteRequest(7, 2);
  del.set_key(std::string(ClientService::kMaxKeyBytes, 'k'));
  ASSERT_TRUE(service.Delete(nullptr, &del, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_OK);
  EXPECT_EQ(node.proposals.size(), 2u);
}

TEST(ClientServiceTest, GetRejectsOversizeKeysAndMapsInvalidArgumentFromTheStore) {
  TestStore store;
  KVStateMachine machine(&store);
  TestRaftNode node;
  ClientService service(&node, &store, &machine);
  network::proto::GetResponse response;

  auto get = GetRequest();
  get.set_key(std::string(ClientService::kMaxKeyBytes + 1, 'k'));
  ASSERT_TRUE(service.Get(nullptr, &get, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  EXPECT_EQ(node.read_index_calls, 0);

  store.forced_get_status = Status::kInvalidArgument;
  get = GetRequest();
  ASSERT_TRUE(service.Get(nullptr, &get, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INVALID_ARGUMENT);
  store.forced_get_status = Status::kIOError;
  ASSERT_TRUE(service.Get(nullptr, &get, &response).ok());
  EXPECT_EQ(response.status(), network::proto::CLIENT_STATUS_INTERNAL_ERROR);
}

}  // namespace
}  // namespace raftkv
