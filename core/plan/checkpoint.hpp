#pragma once

#include "core/plan/plan_base.hpp"

namespace xyz {

struct Checkpoint : public PlanBase {
  Checkpoint(int _plan_id, int _cid, std::string _url)
      : PlanBase(_plan_id), cid(_cid), url(_url) {}

  virtual SpecWrapper GetSpec() override {
    SpecWrapper w;
    w.SetSpec<CheckpointSpec>(plan_id, SpecWrapper::Type::kCheckpoint, 
            cid, url);
    w.name = name;
    return w;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    
  }

  std::string url;
  int cid; // collection id
};

} // namespace xyz

