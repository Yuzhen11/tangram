#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/progress_tracker.hpp"

namespace xyz {
namespace {

class TestProgressTracker: public testing::Test {};

TEST_F(TestProgressTracker, Create) {
  ProgressTracker tracker;
}

}  // namespace
}  // namespace xyz

