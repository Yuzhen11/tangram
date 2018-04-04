#pragma once

#include <sstream>

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
  using JoinFuncT = std::function<void(ObjT3*, MsgT)>;
  using CombineFuncT = std::function<void(MsgT*, const MsgT&)>;

  // for internal use
  // using MapPartWithTempFuncT= 
  //     std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>,
  //                                                      std::shared_ptr<AbstractFetcher>,
  //                                                      std::shared_ptr<AbstractMapProgressTracker>)>;

  using MapPartWithTempFuncT = AbstractFunctionStore::MapWith;

  MapPartWithJoin(int _plan_id, C1* _map_collection, 
          C2* _with_collection, C3* _join_collection)
      : PlanBase(_plan_id), map_collection(_map_collection), 
       with_collection(_with_collection), join_collection(_join_collection) {
    std::stringstream ss;
    ss << "{";
    ss << "map collection: " << map_collection->Name();
    ss << ", with collection: " << with_collection->Name();
    ss << ", join collection: " << join_collection->Name();
    ss << "}";
    description_ = ss.str();
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
  MapPartWithJoin<C1, C2, C3, ObjT1, ObjT2, ObjT3, MsgT>* SetCheckpointInterval(int cp, std::string path = "/tmp/tmp") {
    checkpoint_interval = cp;
    checkpoint_path = path;
    return this;
  }
  MapPartWithJoin<C1, C2, C3, ObjT1, ObjT2, ObjT3, MsgT>* SetCombine(
          CombineFuncT combine_f, int timeout = 0) {
    combine_func = std::move(combine_f);
    combine_timeout = timeout;
    return this;
  }
  MapPartWithJoin<C1, C2, C3, ObjT1, ObjT2, ObjT3, MsgT>* SetName(std::string n) {
    name = std::move(n);
    return this;
  }

  virtual SpecWrapper GetSpec() override {
    // TODO the with collection
    SpecWrapper w;
    w.SetSpec<MapWithJoinSpec>(plan_id, SpecWrapper::Type::kMapWithJoin,
            map_collection->Id(), join_collection->Id(), combine_timeout, num_iter, 
            staleness, checkpoint_interval, checkpoint_path, 
            description_, with_collection->Id());
    w.name = name;
    return w;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    auto map_part_with = GetMapPartWithFunc();
    function_store->AddMapWith(this->plan_id, [this, map_part_with](
                int pid, int version,
                std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractFetcher> fetcher,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto map_output = map_part_with(pid, version, partition, fetcher, tracker);
      if (combine_func) {
        static_cast<PartitionedMapOutput<typename ObjT3::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(combine_func);
      }
      return map_output;
    });

    CHECK_NOTNULL(join);
    function_store->AddJoin(plan_id, GetJoinPartFunc<ObjT3, MsgT>(join));
    function_store->AddJoin2(plan_id, GetJoinPartFunc2<ObjT3, MsgT>(join));
  }

  MapPartWithTempFuncT GetMapPartWithFunc() {
    CHECK_NOTNULL(mappartwith);
    return [this](int pid, int version, std::shared_ptr<AbstractPartition> partition, 
              std::shared_ptr<AbstractFetcher> fetcher,
              std::shared_ptr<AbstractMapProgressTracker> tracker) {
      bool local_mode = true;
      TypedCache<ObjT2> typed_cache(pid, partition->id, version, 
              with_collection->Id(), fetcher, this->with_collection->GetMapper(), 
              staleness, local_mode);
      auto* p = static_cast<TypedPartition<ObjT1>*>(partition.get());
      CHECK_NOTNULL(this->join_collection->GetMapper());
      auto output = std::make_shared<PartitionedMapOutput<typename ObjT3::KeyT, MsgT>>(this->join_collection->GetMapper());
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
  CombineFuncT combine_func;

  int num_iter = 1;
  int staleness = 0;
  std::string checkpoint_path;
  int checkpoint_interval = 0;
  int combine_timeout = -1;
  std::string description_;
};

}  // namespace xyz

