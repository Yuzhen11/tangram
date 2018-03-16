#pragma once

#include "core/plan/mapjoin.hpp"

#include "core/map_output/partitioned_map_output_helper.hpp"

namespace xyz {

template<typename C1, typename C2, typename ObjT1, typename ObjT2, typename MsgT>
struct MapJoinMergeCombine;

template<typename MsgT, typename C1, typename C2>
MapJoinMergeCombine<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT> GetMapJoinMergeCombine(int plan_id, C1* c1, C2* c2) {
  MapJoinMergeCombine<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT> plan(plan_id, c1, c2);
  return plan;
}

// TODO: map join with merge combine
// this is not tested and used.
template<typename C1, typename C2, typename ObjT1, typename ObjT2, typename MsgT>
struct MapJoinMergeCombine : MapJoin<C1, C2, ObjT1, ObjT2, MsgT> {

  MapJoinMergeCombine(int _plan_id, 
          C1* _map_collection, 
          C2* _join_collection)
      : MapJoin<C1,C2,ObjT1,ObjT2,MsgT>(_plan_id, _map_collection, _join_collection) {}

  void RegisterMergeCombine(std::shared_ptr<AbstractFunctionStore> function_store) {
    auto map_part = this->GetMapPartFunc();
    // part -> mapoutput_manager
    function_store->AddMap(this->plan_id, [this, map_part](
                std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto map_output = map_part(partition, tracker);
      if (this->combine) {
        static_cast<PartitionedMapOutput<typename ObjT2::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(this->combine);
      }
      return map_output;
    });
    // mapoutput_manager -> bin
    function_store->AddMergeCombine(this->plan_id, [](const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id) {
      return MergeCombineMultipleMapOutput<typename ObjT2::KeyT, MsgT>(map_outputs, part_id);
    });
  }

};

}  // namespace xyz

