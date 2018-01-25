#pragma once

namespace xyz {

class AbstractPartToNodeMapper {
 public:
  AbstractPartToNodeMapper(int num_nodes): num_nodes_(num_nodes)  {}
  ~AbstractPartToNodeMapper() {}

  int GetNumNodes() const { return num_nodes_; }
  void SetNumNodes(int num_nodes) { num_nodes_ = num_nodes; }

  virtual int Get(int part_id) = 0;
 protected:
  int num_nodes_;
};

}  // namespace xyz

