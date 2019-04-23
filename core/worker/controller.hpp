#pragma once

#include <chrono>

#include "base/actor.hpp"
#include "core/worker/abstract_plan_controller.hpp"
#include "core/engine_elem.hpp"
#include "io/io_wrapper.hpp"

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

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
  void TerminatePlan(int plan_id);
  void SendMsgToScheduler(SArrayBinStream bin);

  std::shared_ptr<IOWrapper> io_wrapper_;
  EngineElem engine_elem_;
  std::map<int, std::shared_ptr<AbstractPlanController>> plan_controllers_;
  std::map<int, bool> erased;
  boost::shared_mutex erase_mu_;

  struct Timer {
    std::chrono::microseconds plan_time{0};
    std::chrono::microseconds control_time{0};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
  };
  std::map<int, Timer> plan_timer_;
};

}  // namespace xyz

