#pragma once

#include "core/plan/plan_base.hpp"
#include "glog/logging.h"

namespace xyz {

struct Checkpoint : public PlanBase {
  enum class Type : char {
    checkpoint, loadcheckpoint 
  };
  Checkpoint(int _plan_id, int _cid, std::string _url,
          Type _type)
      : PlanBase(_plan_id), cid(_cid), url(_url), type(_type) {
  }

  virtual SpecWrapper GetSpec() override {
    SpecWrapper w;
    SpecWrapper::Type t;
    if (type == Type::checkpoint) {
      t = SpecWrapper::Type::kCheckpoint;
    } else {
      t = SpecWrapper::Type::kLoadCheckpoint;
    }
    w.SetSpec<CheckpointSpec>(plan_id, t, cid, url);
    w.name = name;
    return w;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    
  }

  std::string url;
  int cid; // collection id
  Type type;
};

} // namespace xyz

