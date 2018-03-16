#pragma once

#include <utility>
#include <vector>
#include <memory>
#include "base/sarray_binstream.hpp"
#include "core/map_output/map_output_stream.hpp"

namespace xyz {

class AbstractMapOutput {
 public:
  virtual ~AbstractMapOutput() {}

  virtual std::vector<SArrayBinStream> Serialize() = 0;
  virtual void Combine() = 0;

  virtual int GetBufferSize() = 0;
  virtual std::shared_ptr<AbstractMapOutputStream> Get(int i) = 0;
};

}  // namespace xyz
