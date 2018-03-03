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
  CollectionSource source = CollectionSource::kOthers;
  // from distribute
  std::vector<T> data;
  // from hdfs file
  std::string load_url;

  std::shared_ptr<AbstractKeyToPartMapper> mapper;

  Collection(int _id): Collection(_id, 1) {}
  Collection(int _id, int _num_part): 
    id(_id), num_partition(_num_part) {
  }

  void Distribute(std::vector<T> _data) {
    source = CollectionSource::kDistribute;
    data = _data;
  }

  void Load(std::string url) {
    source = CollectionSource::kLoad;
    load_url = url;
  }

  CollectionSpec GetSpec() {
    CollectionSpec s;
    s.collection_id = id;
    s.num_partition = num_partition;
    s.data << data;
    s.source = source;
    s.load_url = load_url;
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

