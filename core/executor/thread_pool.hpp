#pragma once

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <cassert>
#include <future>
#include <functional>


namespace xyz {

/*
 * Copy from: https://github.com/progschj/ThreadPool/blob/master/ThreadPool.h.
 */
class ThreadPool {
 public:
  ThreadPool(size_t);
  template<class F, class... Args>
  auto enqueue(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type>;
  ~ThreadPool();
  size_t size() {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return tasks_.size();
  }
 private:
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;

  std::mutex queue_mutex_;
  std::condition_variable cond_;
  bool stop_;
};

// add new work item to the pool
template <class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  using return_type = typename std::result_of<F(Args...)>::type;

  auto task = std::make_shared<std::packaged_task<return_type()>>(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  std::future<return_type> res = task->get_future();
  {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // don't allow enqueueing after stopping the pool
    assert(!stop_);

    tasks_.emplace([task]() { (*task)(); });
  }
  cond_.notify_one();
  return res;
}


}  // namespace xyz

