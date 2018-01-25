#pragma once

#include <functional>
#include <future>

namespace xyz {

class AbstractExecutor {
 public:
  virtual std::future<void> Add(const std::function<void()>& func) = 0;
  ~AbstractExecutor() {}
};

}  // namespace xyz

