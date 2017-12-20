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

template<typename KeyT, typename MsgT>
class TypedMapOutput : public AbstractMapOutput {
 public:
  virtual void Add(std::pair<KeyT, MsgT> msg) = 0;
  virtual void Add(std::vector<std::pair<KeyT, MsgT>> msgs) = 0;

  void SetCombineFunc(std::function<MsgT(const MsgT&, const MsgT&)> combine_func) {
    combine_func_ = std::move(combine_func);
  }
 protected:
  std::function<MsgT(const MsgT&, const MsgT&)> combine_func_;  // optional
};

}  // namespace xyz
