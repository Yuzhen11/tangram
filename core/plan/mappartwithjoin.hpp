#pragma once

#include "core/plan/plan_base.hpp"

#include "core/cache/typed_cache.hpp"

#include "core/plan/collection.hpp"

#include "core/partition/abstract_partition.hpp"
#include "core/map_output/partitioned_map_output.hpp"
#include "core/index/abstract_key_to_part_mapper.hpp"

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/plan/join_helper.hpp"
#include "glog/logging.h"

namespace xyz {

template<typename C1, typename C2, typename C3, typename ObjT1, typename ObjT2, typename ObjT3, typename MsgT>
struct MapPartWithJoin;

template<typename MsgT, typename C1, typename C2, typename C3>
MapPartWithJoin<C1, C2, C3, typename C1::ObjT, typename C2::ObjT, typename C3::ObjT, MsgT> GetMapPartWithJoin(int plan_id, C1* c1, C2* c2, C3* c3) {
  MapPartWithJoin<C1, C2, C3, typename C1::ObjT, typename C2::ObjT, typename C3::ObjT, MsgT> plan(plan_id, c1, c2, c3);
  return plan;
}

template<typename C1, typename C2, typename C3, typename ObjT1, typename ObjT2, typename ObjT3, typename MsgT>
struct MapPartWithJoin : public PlanBase {
  using MapPartWithFuncT = std::function<std::vector<std::pair<typename ObjT2::KeyT, MsgT>>(
          TypedPartition<ObjT1>* p, 
          TypedCache<ObjT2>* c, 
          AbstractMapProgressTracker* t)>;
  using JoinFuncT = std::function<void(ObjT2*, const MsgT&)>;
  using CombineFuncT = std::function<MsgT(const MsgT&, const MsgT&)>;

  // for internal use
  using MapPartWithTempFuncT= 
      std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>,
                                                       std::shared_ptr<AbstractFetcher>,
                                                       std::shared_ptr<AbstractMapProgressTracker>)>;

  MapPartWithJoin(int _plan_id, C1* _map_collection, 
          C2* _with_collection, C3* _join_collection)
      : PlanBase(_plan_id), map_collection(_map_collection), 
       with_collection(_with_collection), join_collection(_join_collection) {
  }

  // TODO: may make MapPartWithJoin a subclass of MapPartJoin
  MapPartWithJoin<C1, C2, C3, ObjT1, ObjT2, ObjT3, MsgT>* SetStaleness(int s) {
    staleness = s;
    return this;
  }
  MapPartWithJoin<C1, C2, C3, ObjT1, ObjT2, ObjT3, MsgT>* SetIter(int iter) {
    num_iter = iter;
    return this;
  }
  MapPartWithJoin<C1, C2, C3, ObjT1, ObjT2, ObjT3, MsgT>* SetCheckpointInterval(int cp) {
    checkpoint_interval = cp;
    return this;
  }

  virtual SpecWrapper GetSpec() override {
    // TODO the with collection
    SpecWrapper w;
    w.SetSpec<MapWithJoinSpec>(plan_id, SpecWrapper::Type::kMapWithJoin, 
            map_collection->Id(), join_collection->Id(), num_iter, 
            staleness, checkpoint_interval, with_collection->Id());
    return w;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    auto map_part_with = GetMapPartWithFunc();
    function_store->AddMapWith(this->plan_id, [this, map_part_with](
                std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractFetcher> fetcher,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto map_output = map_part_with(partition, fetcher, tracker);
      if (this->combine) {
        static_cast<TypedMapOutput<typename ObjT2::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(this->combine);
        map_output->Combine();
      }
      return map_output;
    });

    CHECK_NOTNULL(join);
    function_store->AddJoin(plan_id, GetJoinPartFunc<ObjT2, MsgT>(join));
  }

  MapPartWithTempFuncT GetMapPartWithFunc() {
    CHECK_NOTNULL(mappartwith);
    return [this](std::shared_ptr<AbstractPartition> partition, 
              std::shared_ptr<AbstractFetcher> fetcher,
              std::shared_ptr<AbstractMapProgressTracker> tracker) {
      // TODO: Fix the version
      int version = 0;
      TypedCache<ObjT2> typed_cache(plan_id, partition->id, with_collection->Id(), fetcher, this->with_collection->GetMapper());
      auto* p = static_cast<TypedPartition<ObjT1>*>(partition.get());
      CHECK_NOTNULL(this->join_collection->GetMapper());
      auto output = std::make_shared<PartitionedMapOutput<typename ObjT2::KeyT, MsgT>>(this->join_collection->GetMapper());
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      output->Add(mappartwith(p, &typed_cache, tracker.get()));
      return output;
    };
  }

  C1* map_collection;
  C2* with_collection;
  C3* join_collection;

  MapPartWithFuncT mappartwith;
  JoinFuncT join;
  CombineFuncT combine;

  int num_iter = 1;
  int staleness = 0;
  int checkpoint_interval = 0;
};

}  // namespace xyz

