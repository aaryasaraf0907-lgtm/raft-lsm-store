#include <gtest/gtest.h>
#include "raft/raft_interface.h"
#include "storage/kv_store.h"

TEST(Smoke, Works) { EXPECT_EQ(1 + 1, 2); }

TEST(Interfaces, HeadersCompile) {
  raftkv::Command c{raftkv::CommandType::kPut, "k", "v"};
  EXPECT_EQ(c.key, "k");
  EXPECT_EQ(raftkv::Status::kOk, raftkv::Status::kOk);
}
