#include "core/plan/dag.hpp"

namespace xyz {

std::string DagNode::DebugString() const {
  std::stringstream ss;
  ss << "id: " << id;
  ss << ", in:{";
  for (int i = 0; i < in.size(); ++ i) {
    ss << in[i];
    if (i != in.size() - 1) {
      ss << ", ";
    }
  }
  ss << "}, out:{";
  for (int i = 0; i < out.size(); ++ i) {
    ss << out[i];
    if (i != out.size() - 1) {
      ss << ", ";
    }
  }
  ss << "}";
  return ss.str();
}

void Dag::AddDagNode(int id, std::vector<int> read, std::vector<int> write) {
  DagNode* node = GetOrCreateDagNode(id);
  std::set<int> dependencies;
  for (auto c : read) {
    Col* col = GetOrCreateCol(c);
    int p = col->GetLastWrite();
    if (p != -1) {
      dependencies.insert(p);
    }
    col->AppendRead(id);
  }
  for (auto c : write) {
    Col* col = GetOrCreateCol(c);
    int p = col->GetLastWrite();
    if (p != -1) {
      dependencies.insert(p);
    }
    col->AppendWrite(id);
  }
  for (auto d : dependencies) {
    AddEdge(d, id);
  }
}

std::string Dag::DebugString() const {
  std::stringstream ss;
  ss << "DAG: \n";
  for (auto& node : nodes_) {
    ss << "Node: " << node.second.DebugString() << "\n";
  }
  return ss.str();
}

void Dag::AddEdge(int src, int dst) {
  DagNode* src_node = GetOrCreateDagNode(src);
  DagNode* dst_node = GetOrCreateDagNode(dst);
  src_node->AddOut(dst);
  dst_node->AddIn(src);
}

DagNode* Dag::GetOrCreateDagNode(int id) {
  if (nodes_.find(id) == nodes_.end()) {
    nodes_.insert({id, DagNode(id)});
  }
  return &nodes_[id];
}
Col* Dag::GetOrCreateCol(int id) {
  if (cols_.find(id) == cols_.end()) {
    cols_.insert({id, Col(id)});
  }
  return &cols_[id];
}

DagVistor::DagVistor(const Dag& dag) {
 nodes_ = dag.nodes_;
 cols_ = dag.cols_;
 for (auto& node: nodes_) {
   indegrees_[node.second.id] = node.second.in.size();
 }
}

// do not care about the complexity
std::vector<int> DagVistor::GetFront() {
  std::vector<int> ret;
  for (auto& node : nodes_) {
    if (indegrees_[node.second.id] == 0) {
      ret.push_back(node.second.id);
    }
  }
  return ret;
}

void DagVistor::Finish(int id) {
  CHECK_EQ(indegrees_[id], 0);
  DagNode* node = &nodes_[id];
  for (auto child : node->out) {
    indegrees_[child] -= 1;
    CHECK_GE(indegrees_[child], 0);
  }
  nodes_.erase(id);
  indegrees_.erase(id);
}

} // namespace xyz

