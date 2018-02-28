#pragma once

namespace xyz {
namespace {

// the number specifies the maximum number of queues in each node.
const int kMagic = 8;

int GetNodeId(int qid) {
  return qid / kMagic;
}

int GetWorkerQid(int nid) {
  return nid * kMagic;
}

int GetJoinActorQid(int nid) {
  return nid * kMagic + 1;
}

}
}  // namespace xyz

