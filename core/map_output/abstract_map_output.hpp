#pragma once

#include <utility>
#include <vector>
#include "base/sarray_binstream.hpp"

namespace xyz {

class AbstractMapOutput {
 public:
  virtual ~AbstractMapOutput() {}

  virtual std::vector<SArrayBinStream> Serialize() = 0;
  virtual void Combine() = 0;
};

}  // namespace xyz
