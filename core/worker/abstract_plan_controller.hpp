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
  virtual void ReceiveFetchRequest(Message msg) = 0;
  virtual void FinishFetch(SArrayBinStream bin) = 0;
  virtual void FinishCheckpoint(SArrayBinStream bin) = 0;

  virtual void MigratePartition(Message msg) = 0;
  virtual void FinishLoadWith(SArrayBinStream bin) = 0;
  virtual void MigratePartitionDest(Message msg) = 0;

  virtual void DisplayTime() = 0;
};

}  // namespace xyz

