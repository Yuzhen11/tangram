#pragma once

#include <cstdlib>

namespace xyz {

class AbstractKeyToPartMapper {
 public:
  AbstractKeyToPartMapper(size_t num_partition): num_partition_(num_partition)  {}
  ~AbstractKeyToPartMapper() {}
  size_t GetNumPart() const { return num_partition_; }
 private:
  size_t num_partition_;
};

template <typename KeyT>
class TypedKeyToPartMapper : public AbstractKeyToPartMapper {
 public:
  TypedKeyToPartMapper(size_t num_partition): AbstractKeyToPartMapper(num_partition)  {}
  virtual size_t Get(const KeyT& key) const = 0;
};

}  // namespace xyz

