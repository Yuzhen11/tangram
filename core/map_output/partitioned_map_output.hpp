#pragma once

#include "core/map_output/abstract_map_output.hpp"
#include "core/index/abstract_key_to_part_mapper.hpp"

#include <memory>
#include <vector>

#include "glog/logging.h"

namespace xyz {

/*
 * Not thread-safe
 */
template<typename KeyT, typename MsgT>
class PartitionedMapOutput : public TypedMapOutput<KeyT, MsgT> {
 public:
  PartitionedMapOutput(std::shared_ptr<AbstractKeyToPartMapper> mapper)
      :key_to_part_mapper_(mapper), buffer_(mapper->GetNumPart()) {}
  virtual ~PartitionedMapOutput() {}

  virtual void Add(std::pair<KeyT, MsgT> msg) override {
    auto* typed_mapper = static_cast<TypedKeyToPartMapper<KeyT>*>(key_to_part_mapper_.get());
    DCHECK(typed_mapper);
    auto part_id = typed_mapper->Get(msg.first);
    DCHECK_LT(part_id, key_to_part_mapper_->GetNumPart());
    buffer_[part_id].push_back(std::move(msg));
  }

  virtual void Add(std::vector<std::pair<KeyT, MsgT>> msgs) override {
    for (auto& msg : msgs) {
      Add(std::move(msg));
    }
  }

  virtual void Combine() override {
    if (!this->combine_func_) 
      return;
    for (auto& buffer : buffer_) {
      std::sort(buffer.begin(), buffer.end(), 
        [](const std::pair<KeyT, MsgT>& p1, const std::pair<KeyT, MsgT>& p2) { return p1.first < p2.first; });
      CombineOneBuffer(buffer, this->combine_func_);
    }
  }

  // Now we push into SArrayBinStream one by one.
  // May consider to use SArray as the underlying storage for SArrayMapOuput, so
  // that no need to serialize at all when the KeyT and MsgT are both trivially
  // copyable.
  virtual std::vector<SArrayBinStream> Serialize() override {
    std::vector<SArrayBinStream> rets;
    rets.reserve(buffer_.size());
    for (int i = 0; i < buffer_.size(); ++ i) {
      rets.push_back(SerializeOneBuffer(buffer_[i]));
    }
    return rets;
  }

  static SArrayBinStream SerializeOneBuffer(const std::vector<std::pair<KeyT, MsgT>>& buffer) {
    SArrayBinStream bin;
    for (auto& p : buffer) {
      bin << p.first << p.second;
    }
    return bin;
  }

  static void CombineOneBuffer(std::vector<std::pair<KeyT, MsgT>>& buffer, 
          const std::function<MsgT(const MsgT&, const MsgT&)>& combine) {
    int l = 0;
    for (int r = 1; r < buffer.size(); ++ r) {
      if (buffer[l].first == buffer[r].first) {
        buffer[l].second = combine(buffer[l].second, buffer[r].second);
      } else {
        l += 1;
        if (l != r) {
          buffer[l] = buffer[r];
        }
      }
    }
    if (!buffer.empty()) {
      buffer.resize(l+1);
    }
  }

  const std::vector<std::pair<KeyT, MsgT>>& GetBuffer(int part_id) const {
    CHECK_LE(part_id, buffer_.size());
    return buffer_[part_id];
  }

  // For test use only.
  std::vector<std::vector<std::pair<KeyT, MsgT>>> GetBuffer() { return buffer_; }
 private:
  std::vector<std::vector<std::pair<KeyT, MsgT>>> buffer_;
  std::shared_ptr<AbstractKeyToPartMapper> key_to_part_mapper_;
};

}  // namespace

