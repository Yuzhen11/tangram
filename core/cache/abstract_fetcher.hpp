#pragma once

#include <map>
#include <vector>
#include <memory>

#include "core/partition/abstract_partition.hpp"
#include "base/sarray_binstream.hpp"
#include "core/scheduler/control.hpp"

namespace xyz {

struct AbstractFetcher {
  virtual ~AbstractFetcher() = default;
  virtual void FetchObjs(int plan_id, int app_thread_id, int collection_id, 
        const std::map<int, SArrayBinStream>& part_to_keys,
        std::vector<SArrayBinStream>* const rets) = 0;
  virtual std::shared_ptr<AbstractPartition> FetchPart(FetchMeta meta) = 0;
};

}  // namespace xyz

