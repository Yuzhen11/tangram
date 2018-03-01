#pragma once

#include <vector>

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/collection_builder_spec.hpp"
#include "core/collection.hpp"
#include "core/partition/seq_partition.hpp"

namespace xyz {

template<typename T>
class CollectionBuilder {
 public:
  CollectionBuilder(Collection<T> c, std::vector<T> data): c_(c), data_(data) {}

  CollectionBuilderSpec GetSpec() {
    CollectionBuilderSpec s;
    s.collection_id = c_.id;
    s.num_partition = c_.num_partition;
    s.data << data_;
    return s;
  }
  void Register(std::shared_ptr<AbstractFunctionStore> function_store) {
    function_store->AddCreatePartitionFunc(c_.id, [](SArrayBinStream bin, int part_id, int num_part) {
      auto part = std::make_shared<SeqPartition<T>>();
      int i = 0;
      while (bin.Size()) {
        T elem;
        bin >> elem;
        if (i % num_part == part_id) {
          part->Add(elem);
        }
      }
      return part;
    });
  }
 private:
  Collection<T> c_;
  std::vector<T> data_;
};

}  // namespace xyz

