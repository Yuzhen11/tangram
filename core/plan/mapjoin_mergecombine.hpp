#pragma once

#include "core/plan/mapjoin.hpp"

#include "core/map_output/partitioned_map_output_helper.hpp"

namespace xyz {

// TODO: map join with merge combine
// this is not tested and used.
template<typename T1, typename T2, typename MsgT>
struct MapJoinMergeCombine : MapJoin<T1, T2, MsgT> {

  MapJoinMergeCombine(int _plan_id, 
          Collection<T1> _map_collection, 
          Collection<T2> _join_collection)
      : MapJoin<T1, T2, MsgT>(_plan_id, _map_collection, _join_collection) {}

  void RegisterMergeCombine(std::shared_ptr<AbstractFunctionStore> function_store) {
    auto map_part = this->GetMapPartFunc();
    // part -> mapoutput_manager
    function_store->AddPartToOutputManager(this->plan_id, [this, map_part](
                std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto map_output = map_part(partition, tracker);
      if (this->combine) {
        static_cast<TypedMapOutput<typename T2::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(this->combine);
      }
      return map_output;
    });
    // mapoutput_manager -> bin
    function_store->AddOutputsToBin(this->plan_id, [](const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id) {
      return MergeCombineMultipleMapOutput<typename T2::KeyT, MsgT>(map_outputs, part_id);
    });
  }

};

}  // namespace xyz

