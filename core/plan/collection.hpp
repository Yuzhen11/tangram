#pragma once

#include <vector>

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/collection_spec.hpp"
#include "core/scheduler/collection_view.hpp"

#include "core/index/abstract_key_to_part_mapper.hpp"
#include "core/partition/indexed_seq_partition.hpp"

#include <memory>

namespace xyz {

template<typename T, typename PartitionT = IndexedSeqPartition<T>>
class Collection {
 public:
  using ObjT = T;
  int id;
  int num_partition;
  std::vector<T> data;
  std::shared_ptr<AbstractKeyToPartMapper> mapper;

  Collection(int _id): Collection(_id, 1) {}
  Collection(int _id, int _num_part): 
    id(_id), num_partition(_num_part) {
  }
  Collection(int _id, int _num_part, std::vector<T> _data)
      : id(_id), num_partition(_num_part), data(_data) {}

  CollectionSpec GetSpec() {
    CollectionSpec s;
    s.collection_id = id;
    s.num_partition = num_partition;
    s.data << data;
    return s;
  }

  void Register(std::shared_ptr<AbstractFunctionStore> function_store) {
    function_store->AddCreatePartitionFunc(id, [](SArrayBinStream bin, int part_id, int num_part) {
      auto part = std::make_shared<PartitionT>();
      int i = 0;
      std::vector<T> vec;
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

};

}  // namespace

