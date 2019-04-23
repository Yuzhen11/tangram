#pragma once

#include "core/index/abstract_key_to_part_mapper.hpp"
#include "base/third_party/range.h"

#include <functional>
#include <algorithm>

#include "glog/logging.h"

namespace xyz {

template <typename KeyT>
class RangeKeyToPartMapper : public TypedKeyToPartMapper<KeyT> {
 public:
  RangeKeyToPartMapper(const std::vector<third_party::Range>& ranges)
    : TypedKeyToPartMapper<KeyT>(ranges.size()), ranges_(ranges) {
    CHECK_GT(ranges_.size(), 0);
    interval_ = ranges_[0].size();
    for (int i = 0; i < ranges_.size() - 1; ++ i) {
      CHECK_EQ(ranges_[i].size(), interval_) << "ranges should have equal size";
    }
  }
  
  virtual size_t Get(const KeyT& key) const override {
    size_t ret = key/interval_;
    CHECK_LT(ret, ranges_[ranges_.size()-1].end());
    return ret;
  }

  int GetNumRanges() const { return ranges_.size(); }
  // void ResetRanges(const std::vector<third_party::Range>& ranges) { ranges_ = ranges; }
  third_party::Range GetRange(int part_id) {
    CHECK_LT(part_id, ranges_.size());
    return ranges_[part_id];
  }

 private:
  int num_nodes_;
  std::vector<third_party::Range> ranges_;
  int interval_;
};

}  // namespace xyz

