#include "core/plan/spec_wrapper.hpp"

namespace xyz {

constexpr const char* SpecWrapper::TypeName[];

MapJoinSpec* SpecWrapper::GetMapJoinSpec() {
  CHECK(type == Type::kMapJoin || type == Type::kMapWithJoin);
  return static_cast<MapJoinSpec*>(spec.get());
}

MapWithJoinSpec* SpecWrapper::GetMapWithJoinSpec() {
  CHECK(type == Type::kMapWithJoin);
  return static_cast<MapWithJoinSpec*>(spec.get());
}

DistributeSpec* SpecWrapper::GetDistributeSpec() {
  CHECK(type == Type::kDistribute);
  return static_cast<DistributeSpec*>(spec.get());
}

LoadSpec* SpecWrapper::GetLoadSpec() {
  CHECK(type == Type::kLoad);
  return static_cast<LoadSpec*>(spec.get());
}

CheckpointSpec* SpecWrapper::GetCheckpointSpec() {
  CHECK(type == Type::kCheckpoint);
  return static_cast<CheckpointSpec*>(spec.get());
}

LoadCheckpointSpec* SpecWrapper::GetLoadCheckpointSpec() {
  CHECK(type == Type::kLoadCheckpoint);
  return static_cast<LoadCheckpointSpec*>(spec.get());
}

WriteSpec* SpecWrapper::GetWriteSpec() {
  CHECK(type == Type::kWrite);
  return static_cast<WriteSpec*>(spec.get());
}

}  // namespace xyz

