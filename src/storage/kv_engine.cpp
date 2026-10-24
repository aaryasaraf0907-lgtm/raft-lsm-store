#include "storage/kv_engine.h"

#include <filesystem>
#include <mutex>
#include <system_error>

namespace raftkv {

Status KVEngine::Open(const Options& options, std::unique_ptr<KVEngine>* out) {
  std::error_code ec;
  std::filesystem::create_directories(options.dir, ec);
  if (ec || !std::filesystem::is_directory(options.dir, ec)) return Status::kIOError;

  std::unique_ptr<KVEngine> engine(new KVEngine(options));
  const std::string wal_path = (std::filesystem::path(options.dir) / "wal.log").string();

  // Rebuild the memtable from the log BEFORE accepting any new writes.
  ReplayStats stats;
  Status s = WAL::Replay(
      wal_path,
      [&](const Entry& e) {
        if (e.tombstone) {
          engine->mem_.Delete(e.key);
        } else {
          engine->mem_.Put(e.key, e.value);
        }
      },
      &stats);
  if (s != Status::kOk) return s;
  engine->recovered_records_ = stats.records;

  s = WAL::Open(wal_path, &engine->wal_);
  if (s != Status::kOk) return s;

  *out = std::move(engine);
  return Status::kOk;
}

Status KVEngine::Put(const std::string& key, const std::string& value) {
  std::unique_lock<std::shared_mutex> lock(mu_);
  // Durable first, visible second: never expose a write that could be lost.
  if (wal_->AppendPut(key, value, options_.sync_wal) != Status::kOk) return Status::kIOError;
  mem_.Put(key, value);
  return Status::kOk;
}

Status KVEngine::Delete(const std::string& key) {
  std::unique_lock<std::shared_mutex> lock(mu_);
  if (wal_->AppendDelete(key, options_.sync_wal) != Status::kOk) return Status::kIOError;
  mem_.Delete(key);
  return Status::kOk;
}

Status KVEngine::Get(const std::string& key, std::string* value) {
  std::shared_lock<std::shared_mutex> lock(mu_);
  // A tombstone and "not here" both mean not-found today. Once SSTables exist,
  // only kNotFound may fall through to them; kDeleted must stop the search.
  return mem_.Get(key, value) == MemTable::GetResult::kFound ? Status::kOk : Status::kNotFound;
}

}  // namespace raftkv