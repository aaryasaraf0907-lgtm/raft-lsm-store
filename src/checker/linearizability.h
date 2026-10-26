// A linearizability checker for the key-value store.
//
// A history is a list of operations, each with the time the client started it and the time it got
// its answer. It is linearizable if the operations can be put in one total order that (1) respects
// real time (an operation that finished before another started comes first) and (2) is a legal
// sequence for a single-copy store (a Get sees the latest Put/Delete before it in that order).
//
// The store is a set of independent registers, one per key, and linearizability is compositional
// (a history is linearizable iff each key's sub-history is), so keys are checked separately. Each
// key is checked by the Wing & Gong search with Lowe's memoisation: try every operation that could
// come next, remember (set of operations placed, register value) states that led nowhere.
//
// A write whose outcome is not known (the client timed out) is `unknown`: it may have taken effect
// at any moment after it started, or never. It is never required to be placed, and it never forces
// anything to come before it. A read that got no answer carries no information and is left out.
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

namespace raftkv {
namespace lin {

enum class OpType { kPut, kGet, kDelete };

struct Operation {
  int client = 0;
  OpType type = OpType::kGet;
  std::string key;
  std::string value;   // Put: the value written. Get: the value observed (when found).
  bool found = false;  // Get: whether the key existed
  int64_t invoke = 0;  // when the client started the call (any monotonic unit)
  int64_t ret = 0;     // when it returned; ignored if `unknown`
  bool unknown = false;  // a write that may or may not have taken effect
};

struct CheckResult {
  bool ok = true;
  // The search ran out of its step budget before it could decide (a history with a huge number of
  // overlapping operations). `ok` is false but this is NOT a violation: the history is undecided.
  bool gave_up = false;
  std::string key;          // the key whose history is not linearizable
  std::string explanation;  // human readable: the offending sub-history
  size_t operations = 0;    // operations checked
};

namespace detail {

constexpr int64_t kInfinity = std::numeric_limits<int64_t>::max();

inline std::string Show(const std::string& s) {
  bool printable = true;
  for (unsigned char c : s) printable = printable && c >= 0x20 && c < 0x7f;
  if (printable && s.size() <= 40) return "\"" + s + "\"";
  static const char* hex = "0123456789abcdef";
  std::string out = "0x";
  for (size_t i = 0; i < s.size() && i < 16; ++i) {
    out.push_back(hex[static_cast<unsigned char>(s[i]) >> 4]);
    out.push_back(hex[static_cast<unsigned char>(s[i]) & 15]);
  }
  if (s.size() > 16) out += "..(" + std::to_string(s.size()) + " bytes)";
  return out;
}

inline std::string Describe(const Operation& o) {
  std::ostringstream out;
  out << "client " << o.client << ": ";
  switch (o.type) {
    case OpType::kPut: out << "put " << Show(o.value); break;
    case OpType::kDelete: out << "delete"; break;
    case OpType::kGet: out << "get -> " << (o.found ? Show(o.value) : std::string("not found")); break;
  }
  out << "  [" << o.invoke << ", ";
  if (o.unknown) out << "unknown";
  else out << o.ret;
  out << "]";
  return out.str();
}

// Checks one key. `ops` may be in any order.
class RegisterChecker {
 public:
  RegisterChecker(std::vector<Operation> ops, uint64_t max_steps) : max_steps_(max_steps), ops_(std::move(ops)) {
    std::stable_sort(ops_.begin(), ops_.end(), [](const Operation& a, const Operation& b) { return a.invoke < b.invoke; });
    n_ = ops_.size();
    done_.assign(n_, false);
    ret_.resize(n_);
    for (size_t i = 0; i < n_; ++i) {
      ret_[i] = ops_[i].unknown ? kInfinity : ops_[i].ret;
      if (!ops_[i].unknown) ++required_left_;
    }
    value_ids_[std::string()] = 0;  // id 0 is "absent"; an empty value gets its own id below
    absent_ = 0;
    next_id_ = 1;
    ids_.resize(n_);
    for (size_t i = 0; i < n_; ++i) {
      if (ops_[i].type == OpType::kPut || (ops_[i].type == OpType::kGet && ops_[i].found)) ids_[i] = Intern(ops_[i].value);
    }
  }

  bool Check() { return Search(absent_); }
  bool GaveUp() const { return gave_up_; }

  // After a failed Check(): the operations that could not be placed in the deepest attempt.
  std::string Explain() const {
    std::ostringstream out;
    out << "linearized " << deepest_ << " of " << n_ << " operations; could not place:\n";
    int shown = 0;
    for (size_t i = 0; i < n_; ++i) {
      if (!ops_[i].unknown && !deepest_mask_[i]) {
        if (shown++ < 8) out << "    " << Describe(ops_[i]) << "\n";
      }
    }
    out << "  full history of this key, by start time:\n";
    for (size_t i = 0; i < n_ && i < 60; ++i) out << "    " << Describe(ops_[i]) << "\n";
    if (n_ > 60) out << "    ... (" << n_ - 60 << " more)\n";
    return out.str();
  }

 private:
  // The register's value is an id: 0 = absent, otherwise an interned Put value.
  int Intern(const std::string& v) {
    auto it = value_ids_.find("v" + v);
    if (it != value_ids_.end()) return it->second;
    const int id = next_id_++;
    value_ids_["v" + v] = id;
    return id;
  }

  std::string MemoKey(int state) const {
    std::string k(reinterpret_cast<const char*>(&state), sizeof(state));
    for (size_t i = 0; i < n_; i += 8) {
      unsigned char b = 0;
      for (size_t j = 0; j < 8 && i + j < n_; ++j) b |= static_cast<unsigned char>(done_[i + j] ? 1 << j : 0);
      k.push_back(static_cast<char>(b));
    }
    return k;
  }

  bool Search(int state) {
    if (required_left_ == 0) return true;
    if (++steps_ > max_steps_) {
      gave_up_ = true;
      return false;
    }
    const std::string key = MemoKey(state);
    if (failed_.count(key) != 0) return false;

    // The two smallest finish times among operations still to be placed (writes of unknown outcome
    // never finish). An operation i may come next unless some OTHER pending operation finished
    // before i started.
    int64_t min1 = kInfinity, min2 = kInfinity;
    size_t arg1 = n_;
    for (size_t i = 0; i < n_; ++i) {
      if (done_[i] || ops_[i].unknown) continue;
      if (ret_[i] < min1) { min2 = min1; min1 = ret_[i]; arg1 = i; }
      else if (ret_[i] < min2) { min2 = ret_[i]; }
    }
    size_t placed = 0;
    for (size_t i = 0; i < n_; ++i) placed += done_[i] ? 1 : 0;
    if (placed >= deepest_) {
      deepest_ = placed;
      deepest_mask_ = done_;
    }

    for (size_t i = 0; i < n_; ++i) {
      if (done_[i]) continue;
      const int64_t blocker = (i == arg1) ? min2 : min1;
      if (blocker < ops_[i].invoke) continue;  // something pending must be placed before it
      int next = state;
      switch (ops_[i].type) {
        case OpType::kPut: next = ids_[i]; break;
        case OpType::kDelete: next = absent_; break;
        case OpType::kGet:
          if (ops_[i].found ? state != ids_[i] : state != absent_) continue;  // it would not have seen this
          break;
      }
      done_[i] = true;
      if (!ops_[i].unknown) --required_left_;
      const bool ok = Search(next);
      done_[i] = false;
      if (!ops_[i].unknown) ++required_left_;
      if (ok) return true;
      if (gave_up_) return false;  // unwinding: do not record a verdict for states we did not finish
    }
    failed_.insert(key);
    return false;
  }

  uint64_t max_steps_;
  uint64_t steps_ = 0;
  bool gave_up_ = false;
  std::vector<Operation> ops_;
  size_t n_ = 0;
  std::vector<bool> done_;
  std::vector<int64_t> ret_;
  std::vector<int> ids_;
  size_t required_left_ = 0;
  std::map<std::string, int> value_ids_;
  int absent_ = 0;
  int next_id_ = 1;
  std::unordered_set<std::string> failed_;
  size_t deepest_ = 0;
  std::vector<bool> deepest_mask_;
};

}  // namespace detail

// Checks a whole history. On failure, `key` and `explanation` describe the first key found that is
// not linearizable.
// `max_steps` bounds the search per key (each step is cheap; the default is generous for histories
// whose concurrency is bounded by a handful of clients, which is what a cluster test produces).
inline CheckResult Check(const std::vector<Operation>& history, uint64_t max_steps = 2000000) {
  std::map<std::string, std::vector<Operation>> by_key;
  for (const Operation& o : history) by_key[o.key].push_back(o);
  CheckResult result;
  result.operations = history.size();
  for (auto& kv : by_key) {
    detail::RegisterChecker checker(kv.second, max_steps);
    if (!checker.Check()) {
      result.ok = false;
      result.key = kv.first;
      if (checker.GaveUp()) {
        result.gave_up = true;
        result.explanation = "undecided: key " + detail::Show(kv.first) + " has too many overlapping operations for the step budget";
        return result;
      }
      result.explanation = "history of key " + detail::Show(kv.first) + " is not linearizable: " + checker.Explain();
      return result;
    }
  }
  return result;
}

// Collects a history from concurrently running clients.
class HistoryRecorder {
 public:
  static int64_t Now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
  }

  // Call immediately BEFORE sending the request. Returns a handle for the End* calls.
  size_t Begin(int client, OpType type, const std::string& key, const std::string& value = "") {
    std::lock_guard<std::mutex> lk(mu_);
    Entry e;
    e.op.client = client;
    e.op.type = type;
    e.op.key = key;
    e.op.value = value;
    e.op.invoke = Now();
    entries_.push_back(std::move(e));
    return entries_.size() - 1;
  }
  // Call immediately AFTER the answer arrives.
  void EndWriteOk(size_t id) { End(id, [](Operation&) {}); }
  void EndGet(size_t id, bool found, const std::string& value) {
    End(id, [&](Operation& o) { o.found = found; o.value = found ? value : std::string(); });
  }
  // A write that timed out or was rejected as stale: it may or may not have taken effect.
  void EndWriteUnknown(size_t id) {
    End(id, [](Operation& o) { o.unknown = true; });
  }
  // The operation certainly had no effect and learned nothing (a rejected request, a read that
  // got no answer).
  void Drop(size_t id) {
    std::lock_guard<std::mutex> lk(mu_);
    entries_[id].dropped = true;
  }

  // The history so far. An operation that never ended is an unknown write or a dropped read.
  std::vector<Operation> Snapshot() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<Operation> out;
    for (const Entry& e : entries_) {
      if (e.dropped) continue;
      Operation o = e.op;
      if (!e.ended) {
        if (o.type == OpType::kGet) continue;
        o.unknown = true;
      }
      out.push_back(std::move(o));
    }
    return out;
  }

 private:
  struct Entry {
    Operation op;
    bool ended = false;
    bool dropped = false;
  };
  template <typename F>
  void End(size_t id, F fill) {
    const int64_t t = Now();
    std::lock_guard<std::mutex> lk(mu_);
    Entry& e = entries_[id];
    e.op.ret = t;
    fill(e.op);
    e.ended = true;
  }
  mutable std::mutex mu_;
  std::vector<Entry> entries_;
};

// One JSON object per line, keys and values as hex, so a failing history can be saved and replayed.
inline std::string ToJsonLines(const std::vector<Operation>& history) {
  static const char* hex = "0123456789abcdef";
  auto enc = [](const std::string& s) {
    std::string out;
    for (unsigned char c : s) {
      out.push_back(hex[c >> 4]);
      out.push_back(hex[c & 15]);
    }
    return out;
  };
  std::ostringstream out;
  for (const Operation& o : history) {
    out << "{\"client\":" << o.client << ",\"op\":\"" << (o.type == OpType::kPut ? "put" : o.type == OpType::kGet ? "get" : "delete")
        << "\",\"key\":\"" << enc(o.key) << "\",\"value\":\"" << enc(o.value) << "\",\"found\":" << (o.found ? "true" : "false")
        << ",\"invoke\":" << o.invoke << ",\"ret\":" << (o.unknown ? std::string("null") : std::to_string(o.ret))
        << ",\"unknown\":" << (o.unknown ? "true" : "false") << "}\n";
  }
  return out.str();
}

inline bool FromJsonLines(const std::string& text, std::vector<Operation>* out) {
  auto field = [](const std::string& line, const std::string& name, std::string* value) {
    const std::string needle = "\"" + name + "\":";
    size_t p = line.find(needle);
    if (p == std::string::npos) return false;
    p += needle.size();
    if (p < line.size() && line[p] == '"') {
      const size_t e = line.find('"', p + 1);
      if (e == std::string::npos) return false;
      *value = line.substr(p + 1, e - p - 1);
    } else {
      size_t e = p;
      while (e < line.size() && line[e] != ',' && line[e] != '}') ++e;
      *value = line.substr(p, e - p);
    }
    return true;
  };
  auto dec = [](const std::string& h, std::string* s) {
    if (h.size() % 2 != 0) return false;
    s->clear();
    for (size_t i = 0; i < h.size(); i += 2) {
      auto nib = [](char c) { return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1; };
      const int hi = nib(h[i]), lo = nib(h[i + 1]);
      if (hi < 0 || lo < 0) return false;
      s->push_back(static_cast<char>(hi * 16 + lo));
    }
    return true;
  };
  out->clear();
  std::istringstream in(text);
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    std::string client, op, key, value, found, invoke, ret, unknown;
    if (!field(line, "client", &client) || !field(line, "op", &op) || !field(line, "key", &key) || !field(line, "value", &value) ||
        !field(line, "found", &found) || !field(line, "invoke", &invoke) || !field(line, "ret", &ret) || !field(line, "unknown", &unknown)) {
      return false;
    }
    Operation o;
    try {
      o.client = std::stoi(client);
      o.invoke = std::stoll(invoke);
      o.unknown = unknown == "true";
      o.ret = o.unknown ? 0 : std::stoll(ret);
    } catch (...) {
      return false;
    }
    o.type = op == "put" ? OpType::kPut : op == "get" ? OpType::kGet : OpType::kDelete;
    if (op != "put" && op != "get" && op != "delete") return false;
    o.found = found == "true";
    if (!dec(key, &o.key) || !dec(value, &o.value)) return false;
    out->push_back(std::move(o));
  }
  return true;
}

}  // namespace lin
}  // namespace raftkv
