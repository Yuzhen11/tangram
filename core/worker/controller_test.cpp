#include "glog/logging.h"
#include "gtest/gtest.h"

#include "core/worker/controller.hpp"


namespace xyz {
namespace {

class TestController : public testing::Test {};

TEST_F(TestController, Create) {
  int qid = 0;
  EngineElem elem;
  Controller(qid, elem);
}

} // namespace
} // namespace xyz
