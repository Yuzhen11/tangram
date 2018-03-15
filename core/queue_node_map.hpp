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

int GetFetcherQid(int nid) {
  return nid * kMagic + 2;
}
int GetControllerActorQid(int nid) {
  return nid * kMagic + 3;
}

}
}  // namespace xyz

