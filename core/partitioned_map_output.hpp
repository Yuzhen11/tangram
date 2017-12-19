#pragma once

#include "core/abstract_map_output.hpp"
#include "core/abstract_key_to_part_mapper.hpp"

#include <memory>
#include <vector>

#include "glog/logging.h"

namespace xyz {

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

  virtual SArrayBinStream Serialize() override {
    // TODO
  }

  // For test use only.
  std::vector<std::vector<std::pair<KeyT, MsgT>>> GetBuffer() { return buffer_; }
 private:
  std::vector<std::vector<std::pair<KeyT, MsgT>>> buffer_;
  std::shared_ptr<AbstractKeyToPartMapper> key_to_part_mapper_;
};

}  // namespace

