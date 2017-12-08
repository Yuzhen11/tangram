#include "core/thread_pool.hpp"

namespace xyz {

ThreadPool::ThreadPool(size_t threads) : stop_(false) {
  for (size_t i = 0; i < threads; ++i) {
    workers_.emplace_back([this] {
      for (;;) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(this->queue_mutex_);
          this->cond_.wait(
              lock, [this] { return this->stop_ || !this->tasks_.empty(); });
          if (this->stop_ && this->tasks_.empty())
            return;
          task = std::move(this->tasks_.front());
          this->tasks_.pop();
        }

        task();
      }
    });
  }
}

// the destructor joins all threads
ThreadPool::~ThreadPool() {
  {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    stop_ = true;
  }
  cond_.notify_all();
  for (std::thread &worker : workers_)
    worker.join();
}

} // namespace xyz
