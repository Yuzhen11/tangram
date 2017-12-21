#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/executor/executor.hpp"

#include <chrono>

namespace xyz {
namespace {

class TestExecutor : public testing::Test {};

TEST_F(TestExecutor, Construct) {
  Executor executor(4);
}

TEST_F(TestExecutor, Add) {
  Executor executor(4);
  std::atomic<int> a(0);
  int size = 10;
  std::vector<std::future<void>> futures;
  for (int i = 0; i < size; ++ i) {
    futures.push_back(executor.Add([&a](){ a.fetch_add(1); }));
  }
  for (auto& f : futures) {
    f.get();
  }
  EXPECT_EQ(a, size);
  EXPECT_EQ(executor.GetNumPendingTask(), 0);
  EXPECT_EQ(executor.HasFreeThreads(), true);
  EXPECT_EQ(executor.GetNumAdded(), size);
  EXPECT_EQ(executor.GetNumFinished(), size);
}

}
}  // namespace xyz

