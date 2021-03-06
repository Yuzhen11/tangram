#pragma once

#include "core/plan/spec_wrapper.hpp"
#include "core/plan/abstract_function_store.hpp"

namespace xyz {

struct PlanBase {
  PlanBase(int _plan_id) : plan_id(_plan_id) {}
  virtual ~PlanBase() = default;
  virtual SpecWrapper GetSpec() = 0;
  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) = 0;

  int plan_id;
  std::string name = "";
};

}  // namespace xyz

