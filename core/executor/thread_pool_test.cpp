#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/executor/thread_pool.hpp"

#include <chrono>

namespace xyz {
namespace {

class TestThreadPool : public testing::Test {};

TEST_F(TestThreadPool, Construct) {
  ThreadPool pool(4);
}

TEST_F(TestThreadPool, EnqueueOne) {
  ThreadPool pool(4);
  pool.enqueue([]{
    return 10;
  });
}

TEST_F(TestThreadPool, EnqueueMultiple) {
  ThreadPool pool(4);
  for (int i = 0; i < 10; ++ i) {
    pool.enqueue([i]{
      VLOG(1) << "hello " << i;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      VLOG(1) << "world " << i;
    });
  }
}

TEST_F(TestThreadPool, EnqueueMultipleReturn) {
  ThreadPool pool(4);
  std::vector<std::future<int>> results;
  for (int i = 0; i < 10; ++ i) {
    results.emplace_back(
      pool.enqueue([i]{
        VLOG(1) << "hello " << i;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        VLOG(1) << "world " << i;
        return i*i;
      })
    );
  }
  std::vector<int> res(results.size());
  std::transform(results.begin(), results.end(), res.begin(), [](std::future<int>& f){ return f.get(); });
  std::sort(res.begin(), res.end());
  for (int i = 0; i < res.size(); ++ i) {
    VLOG(1) << res[i];
    EXPECT_EQ(res[i], i*i);
  }
}

}
}  // namespace xyz

