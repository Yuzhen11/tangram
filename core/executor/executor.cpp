#include "core/executor/executor.hpp"

namespace xyz {

std::future<void> Executor::Add(const std::function<void()>& func) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    num_added_ += 1;
  }
  return thread_pool_.enqueue([this, func]() {
    func();
    std::lock_guard<std::mutex> lk(mu_);
    num_finished_ += 1;
  });
}

}  // namespace xyz

