#pragma once

#include "core/plan/plan_spec.hpp"
#include "core/plan/abstract_function_store.hpp"

namespace xyz {

struct PlanBase {
  virtual ~PlanBase() = default;
  virtual PlanSpec GetPlanSpec() = 0;
  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) = 0;
};

}  // namespace xyz

