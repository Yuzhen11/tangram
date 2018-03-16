#pragma once

#include "core/map_output/partitioned_map_output.hpp"

namespace xyz {

/*
 * This function is used to merge and combine multiple map_outputs according to 
 * part_id and generate an SArrayBinStream
 */
template<typename KeyT, typename MsgT>
SArrayBinStream MergeCombineMultipleMapOutput(
        const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id) {
  CHECK_GE(map_outputs.size(), 0);
  std::vector<std::pair<KeyT, MsgT>> buffer;
  for (auto& map_output : map_outputs) {
    auto* p = static_cast<PartitionedMapOutput<KeyT, MsgT>*>(map_output.get());
    const auto& sub_buffer = p->GetBuffer(part_id);
    buffer.insert(buffer.end(), sub_buffer.begin(), sub_buffer.end());
  }
  const auto& combine_func = static_cast<PartitionedMapOutput<KeyT, MsgT>*>(map_outputs[0].get())->GetCombineFunc();
  std::sort(buffer.begin(), buffer.end(), 
    [](const std::pair<KeyT, MsgT>& p1, const std::pair<KeyT, MsgT>& p2) { return p1.first < p2.first; });
  PartitionedMapOutput<KeyT, MsgT>::CombineOneBuffer(buffer, combine_func);
  auto bin = PartitionedMapOutput<KeyT, MsgT>::SerializeOneBuffer(buffer);
  return bin;
}

}  // namespace xyz

