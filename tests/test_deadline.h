// Runs a call that might block forever (a lock that was supposed to be non-blocking, a process that
// should have exited) with a deadline, so that a regression fails the test in seconds instead of
// hanging the whole CI job.
#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

namespace raftkv {
namespace testutil {

// Calls fn() on a new thread and waits at most `limit` for it. Returns its result, or nullopt if it
// did not return in time. In that case the thread is left running (a blocked system call cannot be
// cancelled): it is detached and owns everything it uses (`fn` is copied into it and the result
// goes to shared state), so it cannot touch the caller's stack after the test has moved on. `fn`
// must therefore capture by value, not by reference.
template <typename R>
std::optional<R> RunWithDeadline(std::chrono::milliseconds limit, std::function<R()> fn) {
  struct State {
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    std::optional<R> value;
  };
  auto state = std::make_shared<State>();
  std::thread([state, fn = std::move(fn)] {
    R result = fn();
    std::lock_guard<std::mutex> lock(state->mu);
    state->value = std::move(result);
    state->done = true;
    state->cv.notify_all();
  }).detach();
  std::unique_lock<std::mutex> lock(state->mu);
  if (!state->cv.wait_for(lock, limit, [&] { return state->done; })) return std::nullopt;
  return std::move(state->value);
}

}  // namespace testutil
}  // namespace raftkv
