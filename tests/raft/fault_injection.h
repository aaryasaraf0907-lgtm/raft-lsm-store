#pragma once
// A harness for testing Raft under failures: a "nemesis" that keeps breaking the network and the
// nodes on a seeded random schedule while clients write, plus checkers for the properties Raft must
// keep no matter what the nemesis does.
//
//   * Election safety:   in any one term there is at most one leader.
//   * State machine safety: no two nodes ever apply different entries at the same position.
//   * Durability:        every write that was acknowledged ends up applied on every node.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "test_cluster.h"

namespace raftkv {
namespace testing_util {

// ---- checkers ----

// Fed (node, term, role) observations from any thread; remembers the first violation.
class ElectionSafetyChecker {
 public:
  void Observe(NodeId id, Term term, Role role) {
    if (role != Role::kLeader) return;
    std::lock_guard<std::mutex> lk(mu_);
    std::set<NodeId>& leaders = leaders_[term];
    leaders.insert(id);
    if (leaders.size() > 1 && report_.empty()) {
      std::ostringstream os;
      os << "ELECTION SAFETY VIOLATED: term " << term << " has leaders";
      for (NodeId l : leaders) os << " " << l;
      report_ = os.str();
    }
  }
  bool Violated() const {
    std::lock_guard<std::mutex> lk(mu_);
    return !report_.empty();
  }
  std::string Report() const {
    std::lock_guard<std::mutex> lk(mu_);
    return report_;
  }

 private:
  mutable std::mutex mu_;
  std::map<Term, std::set<NodeId>> leaders_;
  std::string report_;
};

// "" if every node's applied sequence is strictly increasing in index and any two sequences agree on
// their common prefix; otherwise a description of the first disagreement.
inline std::string CheckPrefixAgreement(const std::vector<std::vector<LogEntry>>& applied) {
  std::ostringstream os;
  for (size_t n = 0; n < applied.size(); ++n) {
    for (size_t i = 1; i < applied[n].size(); ++i) {
      if (applied[n][i].index <= applied[n][i - 1].index) {
        os << "STATE MACHINE SAFETY VIOLATED: node " << n + 1 << " applied index " << applied[n][i].index
           << " after " << applied[n][i - 1].index;
        return os.str();
      }
    }
  }
  for (size_t a = 0; a < applied.size(); ++a) {
    for (size_t b = a + 1; b < applied.size(); ++b) {
      const size_t common = std::min(applied[a].size(), applied[b].size());
      for (size_t i = 0; i < common; ++i) {
        const LogEntry& x = applied[a][i];
        const LogEntry& y = applied[b][i];
        if (x.index != y.index || x.term != y.term || x.command.key != y.command.key) {
          os << "STATE MACHINE SAFETY VIOLATED: nodes " << a + 1 << " and " << b + 1 << " disagree at position "
             << i << ": (index " << x.index << ", term " << x.term << ", key " << x.command.key
             << ") vs (index " << y.index << ", term " << y.term << ", key " << y.command.key << ")";
          return os.str();
        }
      }
    }
  }
  return "";
}

// ---- keeping nodes alive while they are used ----

// The nemesis destroys and rebuilds nodes (a crash and a reboot). Anything that touches a node from
// another thread brackets that work with Enter()/Leave(); Pause() blocks new entries, waits for the
// ones in progress to leave, and so makes it safe to destroy a node.
class Gate {
 public:
  bool Enter() {
    std::lock_guard<std::mutex> lk(mu_);
    if (paused_) return false;
    ++active_;
    return true;
  }
  void Leave() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      --active_;
    }
    cv_.notify_all();
  }
  void Pause() {
    std::unique_lock<std::mutex> lk(mu_);
    paused_ = true;
    cv_.wait(lk, [this] { return active_ == 0; });
  }
  void Resume() {
    std::lock_guard<std::mutex> lk(mu_);
    paused_ = false;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool paused_ = false;
  int active_ = 0;
};

// Samples every node's (term, role) all the time and feeds the election safety checker.
class Monitor {
 public:
  Monitor(Cluster& c, Gate& gate, ElectionSafetyChecker& checker) : c_(c), gate_(gate), checker_(checker) {}
  ~Monitor() { Stop(); }
  void Start() { thread_ = std::thread([this] { Loop(); }); }
  void Stop() {
    stop_ = true;
    if (thread_.joinable()) thread_.join();
  }

 private:
  void Loop() {
    while (!stop_) {
      if (gate_.Enter()) {
        for (NodeId id = 1; id <= static_cast<NodeId>(c_.size); ++id) {
          RaftNodeImpl* node = c_.Node(id);
          const Term before = node->CurrentTerm();
          const Role role = node->GetRole();
          if (node->CurrentTerm() == before) checker_.Observe(id, before, role);  // a consistent pair
        }
        gate_.Leave();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  }
  Cluster& c_;
  Gate& gate_;
  ElectionSafetyChecker& checker_;
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

// ---- clients ----

// `threads` clients, each writing distinct keys one after the other, retrying each until it is
// acknowledged. Each stops after `per_thread` keys, or when Stop() is called if per_thread is 0.
class Writers {
 public:
  Writers(Cluster& c, Gate& gate, int threads, int per_thread)
      : c_(c), gate_(gate), threads_(threads), per_thread_(per_thread) {}
  ~Writers() { Join(); }
  void Start(std::chrono::seconds time_limit = std::chrono::seconds(90)) {
    deadline_ = std::chrono::steady_clock::now() + time_limit;
    for (int t = 0; t < threads_; ++t) pool_.emplace_back([this, t] { Run(t); });
  }
  void Stop() { stop_ = true; }
  void Join() {
    for (auto& th : pool_) {
      if (th.joinable()) th.join();
    }
  }
  std::vector<std::string> Acked() {
    std::lock_guard<std::mutex> lk(mu_);
    return acked_;
  }
  int Expected() const { return threads_ * per_thread_; }

 private:
  void Run(int t) {
    using namespace std::chrono_literals;
    for (int j = 0; (per_thread_ == 0 || j < per_thread_) && !stop_; ++j) {
      const std::string key = "w" + std::to_string(t) + "-" + std::to_string(j);
      while (std::chrono::steady_clock::now() < deadline_ && !stop_) {
        bool done = false;
        if (gate_.Enter()) {
          NodeId leader = c_.FindLeader();
          if (leader != 0) {
            done = c_.Node(leader)->ProposeAndWait(Put(key), 300ms).result == CommitResult::kApplied;
          }
          gate_.Leave();
        }
        if (done) {
          std::lock_guard<std::mutex> lk(mu_);
          acked_.push_back(key);
          break;
        }
        std::this_thread::sleep_for(3ms);
      }
    }
  }
  Cluster& c_;
  Gate& gate_;
  int threads_, per_thread_;
  std::chrono::steady_clock::time_point deadline_;
  std::atomic<bool> stop_{false};
  std::mutex mu_;
  std::vector<std::string> acked_;
  std::vector<std::thread> pool_;
};

// ---- the nemesis ----

struct NemesisOptions {
  uint64_t seed = 1;
  int min_gap_ms = 10;  // time between two actions
  int max_gap_ms = 60;
  double max_drop = 0.2;  // upper bounds for the random fault plans
  double max_duplicate = 0.1;
  int max_delay_ms = 10;
  bool faults = true;       // random message loss, duplication and delay
  bool partitions = true;   // isolate nodes
  bool one_way = true;      // cut single directions of single links
  bool crashes = false;     // stop nodes and later reboot them (needs a persistent cluster)
};

class Nemesis {
 public:
  Nemesis(Cluster& c, Gate& gate, NemesisOptions o) : c_(c), gate_(gate), o_(o), rng_(o.seed) {
    c_.net.SetFaultSeed(o.seed * 7919 + 13);
  }
  ~Nemesis() { StopAndHeal(); }
  void Start() { thread_ = std::thread([this] { Loop(); }); }

  // Stops the schedule, repairs the network and reboots every crashed node.
  void StopAndHeal() {
    stop_ = true;
    if (thread_.joinable()) thread_.join();
    c_.net.HealAll();
    for (NodeId id : down_) Reboot(id);
    down_.clear();
  }

  std::string Schedule() {
    std::lock_guard<std::mutex> lk(log_mu_);
    std::ostringstream os;
    for (const std::string& line : log_) os << "  " << line << "\n";
    return os.str();
  }

 private:
  void Note(const std::string& what) {
    std::lock_guard<std::mutex> lk(log_mu_);
    log_.push_back(std::to_string(log_.size()) + ": " + what);
  }
  int Uniform(int lo, int hi) { return std::uniform_int_distribution<int>(lo, hi)(rng_); }
  double Real(double hi) { return std::uniform_real_distribution<double>(0, hi)(rng_); }
  NodeId RandomNode() { return static_cast<NodeId>(Uniform(1, c_.size)); }

  void Reboot(NodeId id) {
    gate_.Pause();  // nobody may be using the node while it is destroyed and rebuilt
    c_.Restart(id);
    gate_.Resume();
  }

  void Loop() {
    while (!stop_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(Uniform(o_.min_gap_ms, o_.max_gap_ms)));
      if (stop_) break;
      Act();
    }
  }

  void Act() {
    const int kind = Uniform(0, 6);
    std::ostringstream os;
    if (kind == 0 && o_.faults) {
      FaultPlan plan;
      plan.drop_request = Real(o_.max_drop);
      plan.drop_reply = Real(o_.max_drop);
      plan.duplicate = Real(o_.max_duplicate);
      plan.max_delay_ms = Uniform(0, o_.max_delay_ms);
      c_.net.SetFaults(plan);
      os << "faults: drop " << plan.drop_request << "/" << plan.drop_reply << " dup " << plan.duplicate
         << " delay <= " << plan.max_delay_ms << " ms";
    } else if (kind == 1 && o_.partitions) {
      NodeId id = RandomNode();
      c_.net.Disconnect(id);
      os << "isolate node " << id;
    } else if (kind == 2 && o_.partitions) {
      for (NodeId id = 1; id <= static_cast<NodeId>(c_.size); ++id) c_.net.Reconnect(id);
      os << "reconnect all";
    } else if (kind == 3 && o_.one_way) {
      NodeId a = RandomNode(), b = RandomNode();
      if (a == b) return;
      c_.net.BlockLink(a, b);
      os << "cut link " << a << " -> " << b;
    } else if (kind == 4 && o_.one_way) {
      for (NodeId a = 1; a <= static_cast<NodeId>(c_.size); ++a)
        for (NodeId b = 1; b <= static_cast<NodeId>(c_.size); ++b) c_.net.UnblockLink(a, b);
      os << "restore all links";
    } else if (kind == 5 && o_.crashes) {
      const size_t max_down = static_cast<size_t>((c_.size - 1) / 2);  // keep a majority alive
      if (down_.size() >= max_down) return;
      NodeId id = RandomNode();
      if (down_.count(id) != 0) return;
      c_.Node(id)->Stop();  // the object stays valid; it just stops answering
      down_.insert(id);
      os << "crash node " << id;
    } else if (kind == 6 && o_.crashes) {
      if (down_.empty()) return;
      NodeId id = *down_.begin();
      down_.erase(down_.begin());
      Reboot(id);
      os << "reboot node " << id;
    } else {
      return;
    }
    Note(os.str());
  }

  Cluster& c_;
  Gate& gate_;
  NemesisOptions o_;
  std::mt19937_64 rng_;
  std::atomic<bool> stop_{false};
  std::thread thread_;
  std::set<NodeId> down_;  // touched only by the nemesis thread, then by StopAndHeal after joining it
  std::mutex log_mu_;
  std::vector<std::string> log_;
};

// After the nemesis has healed everything: wait until every acknowledged write is applied on every
// node and the nodes have stopped changing, then return "" or a description of what is wrong.
inline std::string CheckOutcome(Cluster& c, const std::vector<std::string>& acked,
                                ElectionSafetyChecker& election) {
  using namespace std::chrono_literals;
  if (election.Violated()) return election.Report();
  const bool converged = WaitFor(
      [&] {
        size_t length = c.Applied(1).size();
        for (NodeId id = 1; id <= static_cast<NodeId>(c.size); ++id) {
          if (c.Applied(id).size() != length) return false;
          std::vector<std::string> keys = c.Keys(id);
          for (const std::string& k : acked) {
            if (!Contains(keys, k)) return false;
          }
        }
        return true;
      },
      30000ms);
  if (election.Violated()) return election.Report();
  std::vector<std::vector<LogEntry>> applied;
  for (NodeId id = 1; id <= static_cast<NodeId>(c.size); ++id) applied.push_back(c.Applied(id));
  std::string prefix = CheckPrefixAgreement(applied);
  if (!prefix.empty()) return prefix;
  if (!converged) {
    for (NodeId id = 1; id <= static_cast<NodeId>(c.size); ++id) {
      std::vector<std::string> keys = c.Keys(id);
      for (const std::string& k : acked) {
        if (!Contains(keys, k)) {
          return "DURABILITY VIOLATED: write " + k + " was acknowledged but node " + std::to_string(id) +
                 " never applied it";
        }
      }
    }
    return "the nodes did not converge to the same state within 30 s";
  }
  return "";
}

}  // namespace testing_util
}  // namespace raftkv
