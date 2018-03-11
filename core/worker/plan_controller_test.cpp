#include "glog/logging.h"
#include "gtest/gtest.h"

#include "core/worker/plan_controller.hpp"
#include "core/worker/controller.hpp"

namespace xyz {
namespace {

class TestPlanController : public testing::Test {};

TEST_F(TestPlanController, Create) {
  int qid = 0;
  EngineElem elem;
  Controller controller(qid, elem);
  PlanController plan_controller(&controller);
}

} // namespace
} // namespace xyz
