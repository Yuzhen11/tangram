#pragma once

#include "core/plan/dag.hpp"

namespace xyz {

struct AbstractDagRunner {
  virtual std::vector<int> GetRunnablePlans() = 0;
  virtual void Finish(int) = 0;
  virtual int GetNumRemainingPlans() = 0;
};

/*
 * this dag runner runs plan one by one according to 
 * the definition order.
 */
class SequentialDagRunner : public AbstractDagRunner {
 public:
  SequentialDagRunner(const Dag& dag):
      dag_visitor_(dag) {
    num_plans_ = dag_visitor_.GetNumDagNodes();
    plan_count_ = 0;
  }
  virtual std::vector<int> GetRunnablePlans() override; 
  virtual void Finish(int) override;
  virtual int GetNumRemainingPlans() override;
 private:
  DagVistor dag_visitor_;
  int plan_count_ = 0;
  int num_plans_ = 0;
};

/*
 * run as many plans as possible
 */
class WideDagRunner : public AbstractDagRunner {
 public:
  WideDagRunner(const Dag& dag):
      dag_visitor_(dag) {
    num_plans_ = dag_visitor_.GetNumDagNodes();
    plan_count_ = 0;
  }
  virtual std::vector<int> GetRunnablePlans() override; 
  virtual void Finish(int) override;
  virtual int GetNumRemainingPlans() override;
 private:
  DagVistor dag_visitor_;
  int plan_count_ = 0;
  int num_plans_ = 0;

  std::set<int> running_;
};

} // namespace xyz

