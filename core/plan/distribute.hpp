#pragma once

#include "core/plan/plan_base.hpp"
#include "core/partition/seq_partition.hpp"
#include "core/partition/indexed_seq_partition.hpp"
#include "core/partition/range_indexed_seq_partition.hpp"

namespace xyz {

template<typename C, typename PartitionT = SeqPartition<C>>
struct Distribute : public PlanBase {
  Distribute(int _plan_id, int _collection_id, int _num_parts)
      : PlanBase(_plan_id), collection_id(_collection_id), num_parts(_num_parts) {}

  virtual SpecWrapper GetSpec() override {
    SArrayBinStream bin;
    bin << data;
    SpecWrapper w;
    w.SetSpec<DistributeSpec>(plan_id, SpecWrapper::Type::kDistribute, 
            collection_id, num_parts, bin);
    w.name = name;
    return w;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    // TODO: for placeholder collection, it may need to define the deserialization function. E.g., the Collector in Nomad.
    function_store->AddCreatePartFromBinFunc(collection_id, [](SArrayBinStream bin, int part_id, int num_part) {
      auto part = std::make_shared<PartitionT>();
      int i = 0;
      std::vector<C> vec;
      bin >> vec;
      for (auto elem : vec) {
        if (i % num_part == part_id) {
          part->Add(elem);
        }
        i += 1;
      }
      return part;
    });
  }

  int collection_id;
  int num_parts;
  std::vector<C> data;
};

template<typename C, typename KeyToPartMapper>
struct RangeDistribute : public Distribute<C, RangeIndexedSeqPartition<C>> {
  RangeDistribute(int _plan_id, int _collection_id, std::shared_ptr<KeyToPartMapper> mapper)
      : Distribute<C, RangeIndexedSeqPartition<C>>(_plan_id, _collection_id, mapper->GetNumRanges()),
        key_to_part_mapper(mapper) {}

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    // TODO: for placeholder collection, it may need to define the deserialization function. E.g., the Collector in Nomad.
    function_store->AddCreatePartFromBinFunc(this->collection_id, [this](SArrayBinStream bin, int part_id, int num_part) {
      auto part = std::make_shared<RangeIndexedSeqPartition<C>>(key_to_part_mapper->GetRange(part_id));
      int i = 0;
      std::vector<C> vec;
      bin >> vec;
      for (auto elem : vec) {
        if (i % num_part == part_id) {
          part->Add(elem);
        }
        i += 1;
      }
      return part;
    });
  }
  std::shared_ptr<KeyToPartMapper> key_to_part_mapper;
};

} // namespace xyz

