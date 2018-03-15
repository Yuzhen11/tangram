#pragma once

#include "base/actor.hpp"
#include "core/worker/abstract_plan_controller.hpp"
#include "core/engine_elem.hpp"
#include "io/io_wrapper.hpp"

namespace xyz {

class Controller : public Actor {
 public:
  Controller(int qid, EngineElem engine_elem, std::shared_ptr<IOWrapper> io_wrapper)
      : Actor(qid), engine_elem_(engine_elem), 
      io_wrapper_(io_wrapper) {
    Start();
  }

  virtual ~Controller() override {
    Stop();
  }

  virtual void Process(Message msg) override;

  void Setup(SArrayBinStream bin);

  std::shared_ptr<IOWrapper> io_wrapper_;
  EngineElem engine_elem_;
  std::map<int, std::shared_ptr<AbstractPlanController>> plan_controllers_;
};

}  // namespace xyz

