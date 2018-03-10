#pragma once

#include "core/index/abstract_part_to_node_mapper.hpp"
#include "base/third_party/range.h"

#include <functional>
#include <algorithm>

namespace xyz {

template <typename KeyT>
class RangeKeyToPartMapper : public TypedKeyToPartMapper<KeyT> {
 public:
 RangeKeyToPartMapper(const std::vector<third_party::Range>& ranges): ranges_(ranges) {}
  
  virtual int Get(int part_id) const {
    for (int i = 0; i < ranges_.size(); i++) {
      if( part_id >= ranges_[i].begin() && part_id < ranges_[i].end() )
            return i;
    }
    return -1;
  }

  int GetNumKeys() const { return ranges_.size(); }
  void ResetRanges(const std::vector<third_party::Range>& ranges) { ranges_ = ranges; }

  virtual void FromBin(SArrayBinStream& bin) override {
    CHECK(false) << "Not implemented";
  }
  virtual void ToBin(SArrayBinStream& bin) const override {
    CHECK(false) << "Not implemented";
  }
 private:
  int num_nodes_;
  std::vector<third_party::Range> ranges_;
};

}  // namespace xyz

