#pragma once

#include "core/plan/spec_wrapper.hpp"
#include "base/message.hpp"

namespace xyz {

struct AbstractPlanController {
  virtual ~AbstractPlanController() = default;
  virtual void Setup(SpecWrapper spec) = 0;
  virtual void StartPlan() = 0;
  virtual void FinishMap(SArrayBinStream bin) = 0;
  virtual void FinishJoin(SArrayBinStream bin) = 0;
  virtual void UpdateVersion(SArrayBinStream bin) = 0;
  virtual void ReceiveJoin(Message msg) = 0;
  virtual void ReceiveFetchObjsRequest(Message msg) = 0;
  virtual void FinishRunObjsRequest(SArrayBinStream bin) = 0;
};

}  // namespace xyz

