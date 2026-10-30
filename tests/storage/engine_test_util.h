// Small helpers shared by the KVEngine Scan and model tests.
#pragma once

#include <gtest/gtest.h>
#include <stdlib.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "storage/kv_engine.h"
#include "storage/manifest.h"
#include "storage/sstable.h"

namespace raftkv {
namespace testutil {

// Polls `pred` until it holds or `timeout` passes; returns its final value.
inline bool WaitFor(const std::function<bool()>& pred,
                    std::chrono::milliseconds timeout = std::chrono::milliseconds(15000)) {
  const auto end = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < end) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return pred();
}

// A fresh temporary directory, removed at destruction.
class TempDir {
 public:
  TempDir() {
    std::string tmpl = (std::filesystem::temp_directory_path() / "raftkv_scan_XXXXXX").string();
    if (::mkdtemp(&tmpl[0]) != nullptr) path_ = tmpl;
  }
  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }
  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

// Writes `tables` (oldest first, each sorted) as SSTables 1..n plus a manifest naming them, so an
// engine opened on `dir` starts with exactly these persisted layers.
inline void BuildTables(const std::string& dir, const std::vector<std::vector<Entry>>& tables) {
  ManifestState m;
  for (size_t i = 0; i < tables.size(); ++i) {
    const uint64_t n = i + 1;
    const SSTableMeta meta{n, n, n};
    std::unique_ptr<SSTableBuilder> b;
    ASSERT_EQ(SSTableBuilder::Create(dir + "/" + KVEngine::SstFileName(n), meta, {}, &b), Status::kOk);
    for (const Entry& e : tables[i]) ASSERT_EQ(b->Add(e), Status::kOk);
    ASSERT_EQ(b->Finish(), Status::kOk);
    m.files.push_back(meta);
  }
  m.flushed_through = tables.size();
  m.next_file_number = tables.size() + 1;
  ASSERT_EQ(StoreManifest(dir, m), Status::kOk);
}

// Every frozen memtable flushed and no compaction pending (trigger 0: flushes only).
inline bool Settled(KVEngine& engine, size_t compaction_trigger) {
  const KVEngine::Stats s = engine.GetStats();
  return s.frozen_memtables == 0 && (compaction_trigger < 2 || s.sstables < compaction_trigger);
}

}  // namespace testutil
}  // namespace raftkv
