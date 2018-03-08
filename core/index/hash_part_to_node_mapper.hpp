#pragma once

#include "core/index/abstract_part_to_node_mapper.hpp"

#include <functional>

namespace xyz {

class HashPartToNodeMapper : public AbstractPartToNodeMapper {
 public:
  HashPartToNodeMapper(int num_nodes): num_nodes_(num_nodes) {}
  virtual int Get(int part_id) const {
    return std::hash<int>()(part_id) % num_nodes_;
  }

  int GetNumNodes() const { return num_nodes_; }
  void SetNumNodes(int num_nodes) { num_nodes_ = num_nodes; }

  virtual void FromBin(SArrayBinStream& bin) override {
    CHECK(false) << "Not implemented";
  }
  virtual void ToBin(SArrayBinStream& bin) const override {
    CHECK(false) << "Not implemented";
  }
 private:
  int num_nodes_;
};

}  // namespace xyz

