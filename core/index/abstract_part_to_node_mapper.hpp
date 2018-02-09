#pragma once

#include "base/sarray_binstream.hpp"

namespace xyz {

class AbstractPartToNodeMapper {
 public:
  AbstractPartToNodeMapper() = default;
  ~AbstractPartToNodeMapper() = default;

  virtual int Get(int part_id) = 0;
  virtual void FromBin(SArrayBinStream& bin) = 0;
  virtual void ToBin(SArrayBinStream& bin) const = 0;
};

}  // namespace xyz

