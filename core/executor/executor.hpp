#pragma once

#include <future>

#include "core/executor/abstract_executor.hpp"
#include "core/executor/thread_pool.hpp"

namespace xyz {

/*
 * A wrapper around ThreadPool.
 * Only accept void->void function.
 *
 * TODO: Not sure whether the GetNumPendingTask() and HasFreeThreads() functions are accurate.
 * Another way is to let the Engine design whether the Executor can accept more tasks.
 */
class Executor: public AbstractExecutor {
 public:
  Executor(size_t threads): thread_pool_(threads), num_threads_(threads) {}
  virtual std::future<void> Add(const std::function<void()>& func) override;
  // Return the number of tasks that are either running or waiting in the queue.
  int GetNumPendingTask() {
    std::lock_guard<std::mutex> lk(mu_);
    return num_finished_ - num_added_;
  }
  bool HasFreeThreads() {
    return GetNumPendingTask() < num_threads_;
  }
  int GetNumAdded() {
    std::lock_guard<std::mutex> lk(mu_);
    return num_added_;
  }
  int GetNumFinished() {
    std::lock_guard<std::mutex> lk(mu_);
    return num_finished_;
  }
 private:
  ThreadPool thread_pool_;
  int num_threads_;
  int num_added_ = 0;
  int num_finished_ = 0;
  std::mutex mu_;
};

}  // namespace xyz

