#include "core/plan/function_store.hpp"

#include "glog/logging.h"
#include "core/queue_node_map.hpp"

namespace xyz {

const FunctionStore::MergeCombineFuncT& FunctionStore::GetMergeCombine(int id) {
  CHECK(merge_combines_.find(id) != merge_combines_.end());
  return merge_combines_[id];
}
const FunctionStore::MapFuncT& FunctionStore::GetMap(int id) {
  CHECK(maps_.find(id) != maps_.end());
  return maps_[id];
}
const FunctionStore::MapWith& FunctionStore::GetMapWith(int id) {
  CHECK(mapwiths_.find(id) != mapwiths_.end());
  return mapwiths_[id];
}
const FunctionStore::JoinFuncT& FunctionStore::GetJoin(int id) {
  CHECK(updates_.find(id) != updates_.end());
  return updates_[id];
}
const FunctionStore::JoinFunc2T& FunctionStore::GetJoin2(int id) {
  CHECK(updates2_.find(id) != updates2_.end());
  return updates2_[id];
}

const FunctionStore::CreatePartFromBinFuncT& FunctionStore::GetCreatePartFromBin(int id) {
  CHECK(create_part_from_bin_.find(id) != create_part_from_bin_.end()) << id;
  return create_part_from_bin_[id];
}

const FunctionStore::CreatePartFromBlockReaderFuncT& FunctionStore::GetCreatePartFromBlockReader(int id) {
  CHECK(create_part_from_block_reader_.find(id) != create_part_from_block_reader_.end()) << id;
  return create_part_from_block_reader_[id];
}

const FunctionStore::WritePartFuncT& FunctionStore::GetWritePartFunc(int id) {
  CHECK(write_part_.find(id) != write_part_.end()) << id;
  return write_part_[id];
}

const FunctionStore::GetterFuncT& FunctionStore::GetGetter(int id) {
  CHECK(getter_.find(id) != getter_.end()) << id;
  return getter_[id];
}

const FunctionStore::CreatePartFuncT& FunctionStore::GetCreatePart(int id) {
  CHECK(create_part_.find(id) != create_part_.end()) << id;
  return create_part_[id];
}

const FunctionStore::CreatePartFromStringFuncT& FunctionStore::GetCreatePartFromString(int id) {
  CHECK(create_part_from_string_.find(id) != create_part_from_string_.end()) << id;
  return create_part_from_string_[id];
}

void FunctionStore::AddMap(int id, MapFuncT map) {
  maps_.insert({id, map});
}

void FunctionStore::AddMergeCombine(int id, MergeCombineFuncT merge_combine) {
  // auto ret = [merge_combine, id](const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, 
  //               std::shared_ptr<AbstractIntermediateStore> intermediate_store,
  //               int part_id) {
  //   SArrayBinStream bin = merge_combine(map_outputs, part_id);
  //   // TODO Add msg header.
  //   Message msg = bin.ToMsg();
  //   intermediate_store->Add(msg);
  // };
  merge_combines_.insert({id, merge_combine});
}

void FunctionStore::AddJoin(int id, JoinFuncT func) {
  updates_.insert({id, func});
}
void FunctionStore::AddJoin2(int id, JoinFunc2T func) {
  updates2_.insert({id, func});
}

void FunctionStore::AddMapWith(int id, MapWith func) {
  mapwiths_.insert({id, func});
}

void FunctionStore::AddCreatePartFromBinFunc(int id, CreatePartFromBinFuncT func) {
  create_part_from_bin_.insert({id, func});
}

void FunctionStore::AddCreatePartFromBlockReaderFunc(int id, CreatePartFromBlockReaderFuncT func) {
  create_part_from_block_reader_.insert({id, func});
}

void FunctionStore::AddWritePart(int id, WritePartFuncT func) {
  write_part_.insert({id, func});
}

void FunctionStore::AddGetter(int id, GetterFuncT func) {
  getter_.insert({id, func});
}

void FunctionStore::AddCreatePartFunc(int id, CreatePartFuncT func) {
  create_part_.insert({id, func});
  CHECK(create_part_.find(id) != create_part_.end());
}

void FunctionStore::AddCreatePartFromStringFunc(int id, CreatePartFromStringFuncT func) {
  create_part_from_string_.insert({id, func});
  CHECK(create_part_from_string_.find(id) != create_part_from_string_.end());
}

}  // namespaca xyz

