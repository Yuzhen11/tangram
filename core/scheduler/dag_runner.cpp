#include "core/scheduler/dag_runner.hpp"

#include <algorithm>

#include "glog/logging.h"

namespace xyz {

std::vector<int> SequentialDagRunner::GetRunnablePlans() {
  auto f = dag_visitor_.GetFront();
  CHECK(std::find(f.begin(), f.end(), plan_count_) != f.end());
  return {plan_count_};
}

void SequentialDagRunner::Finish(int plan_id) {
  CHECK_EQ(plan_id, plan_count_);
  dag_visitor_.Finish(plan_id);
  plan_count_ += 1;
}

int SequentialDagRunner::GetNumRemainingPlans() {
  return dag_visitor_.GetNumDagNodes();
}

// wide dag runner
std::vector<int> WideDagRunner::GetRunnablePlans() {
  auto f = dag_visitor_.GetFront();
  std::vector<int> ret;
  for (auto plan : f) {
    if (running_.find(plan) == running_.end()) {
      ret.push_back(plan);
      running_.insert(plan);
    }
  }
  return ret;
}

void WideDagRunner::Finish(int plan_id) {
  CHECK(running_.find(plan_id) != running_.end());
  running_.erase(plan_id);
  dag_visitor_.Finish(plan_id);
}

int WideDagRunner::GetNumRemainingPlans() {
  return dag_visitor_.GetNumDagNodes();
}

} // namespace xyz

