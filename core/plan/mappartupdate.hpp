#pragma once

#include <sstream>

#include "core/plan/plan_base.hpp"

#include "core/map_output/partitioned_map_output.hpp"
#include "core/index/abstract_key_to_part_mapper.hpp"

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/plan/update_helper.hpp"

namespace xyz {

template<typename C1, typename C2, typename ObjT1, typename ObjT2, typename MsgT>
struct MapPartJoin;

template<typename MsgT, typename C1, typename C2>
MapPartJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT> GetMapPartJoin(int plan_id, C1* c1, C2* c2) {
  MapPartJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT> plan(plan_id, c1, c2);
  return plan;
}

template<typename C1, typename C2, typename ObjT1, typename ObjT2, typename MsgT>
struct MapPartJoin : public PlanBase {
  using MapPartFuncT = std::function<void(
          TypedPartition<ObjT1>* p, Output<typename ObjT2::KeyT, MsgT>* )>;
  using JoinFuncT = std::function<void(ObjT2*, MsgT)>;
  using CombineFuncT = std::function<void(MsgT*, const MsgT&)>;

  // for internal use
  using MapFuncTempT = std::function<std::shared_ptr<AbstractMapOutput>(
          std::shared_ptr<AbstractPartition>)>;

  MapPartJoin(int _plan_id, C1* _map_collection, C2* _update_collection)
      : PlanBase(_plan_id), map_collection(_map_collection), update_collection(_update_collection) {
    std::stringstream ss;
    ss << "{";
    ss << "map collection: " << map_collection->Name();
    ss << ", update collection: " << update_collection->Name();
    ss << "}";
    description_ = ss.str();
  }

  MapPartJoin<C1, C2, ObjT1, ObjT2, MsgT>* SetStaleness(int s) {
    staleness = s;
    return this;
  }
  MapPartJoin<C1, C2, ObjT1, ObjT2, MsgT>* SetIter(int iter) {
    num_iter = iter;
    return this;
  }
  MapPartJoin<C1, C2, ObjT1, ObjT2, MsgT>* SetCheckpointInterval(int cp, std::string path = "/tmp/tmp") {
    checkpoint_interval = cp;
    checkpoint_path = path;
    return this;
  }

  // combine_timeout
  // see kMaxCombineTimeout in base/magic.hpp
  MapPartJoin<C1, C2, ObjT1, ObjT2, MsgT>* SetCombine(
          CombineFuncT combine_f, int timeout = 0) {
    combine_func = std::move(combine_f);
    combine_timeout = timeout;
    return this;
  }
  MapPartJoin<C1, C2, ObjT1, ObjT2, MsgT>* SetName(std::string n) {
    name = std::move(n);
    return this;
  }

  virtual SpecWrapper GetSpec() override {
    SpecWrapper w;
    w.SetSpec<MapJoinSpec>(plan_id, SpecWrapper::Type::kMapJoin,
            map_collection->Id(), update_collection->Id(), combine_timeout, num_iter, 
            staleness, checkpoint_interval, checkpoint_path, description_);
    w.name = name;
    return w;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    auto map_part = GetMapPartFunc();
    function_store->AddMap(plan_id, [this, map_part](
                std::shared_ptr<AbstractPartition> partition) {
      auto map_output = map_part(partition);
      if (combine_func) {
        static_cast<Output<typename ObjT2::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(combine_func);
      }
      return map_output;
    });

    CHECK_NOTNULL(update);
    function_store->AddJoin(plan_id, GetJoinPartFunc<ObjT2, MsgT>(update));
    function_store->AddJoin2(plan_id, GetJoinPartFunc2<ObjT2, MsgT>(update));
  }

  MapFuncTempT GetMapPartFunc() {
    CHECK_NOTNULL(mappart);
    return [this](std::shared_ptr<AbstractPartition> partition) {
      auto* p = static_cast<TypedPartition<ObjT1>*>(partition.get());
      CHECK_NOTNULL(update_collection->GetMapper());
      auto output = std::make_shared<Output<typename ObjT2::KeyT, MsgT>>(update_collection->GetMapper());
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      mappart(p, output.get());
      return output;
    };
  }

  C1* map_collection;
  C2* update_collection;

  MapPartFuncT mappart;
  JoinFuncT update;
  CombineFuncT combine_func;

  int num_iter = 1;
  int staleness = 0;
  std::string checkpoint_path;
  int checkpoint_interval = 0;
  int combine_timeout = -1;
  std::string description_;
};

}  // namespace xyz

