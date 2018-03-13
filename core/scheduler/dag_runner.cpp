#include "core/scheduler/dag_runner.hpp"

#include <algorithm>

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

} // namespace xyz

