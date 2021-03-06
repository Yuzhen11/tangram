#include "glog/logging.h"
#include "gtest/gtest.h"

#include "core/worker/plan_controller.hpp"
#include "core/worker/controller.hpp"
#include "io/fake_reader.hpp"
#include "io/fake_writer.hpp"

namespace xyz {
namespace {

class TestPlanController : public testing::Test {};

TEST_F(TestPlanController, Create) {
  int qid = 0;
  EngineElem elem;
  elem.num_local_threads = 1;
  elem.num_update_threads = 1;
  auto io_wrapper = std::make_shared<IOWrapper>(
      []() { return std::make_shared<FakeReader>(); },
      []() { return std::make_shared<FakeWriter>(); });
  Controller controller(qid, elem, io_wrapper);
  PlanController plan_controller(&controller);
}

} // namespace
} // namespace xyz
