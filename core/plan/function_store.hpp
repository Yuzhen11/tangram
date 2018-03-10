#pragma once

#include <map>

#include "core/plan/abstract_function_store.hpp"

#include "core/map_output/map_output_storage.hpp"
#include "core/shuffle_meta.hpp"

namespace xyz { 

/*
 * Store all the functions.
 */
class FunctionStore : public AbstractFunctionStore {
 public:
  FunctionStore() = default;

  using MapFuncT = AbstractFunctionStore::MapFuncT;
  using MergeCombineFuncT = AbstractFunctionStore::MergeCombineFuncT;
  using JoinFuncT = AbstractFunctionStore::JoinFuncT;
  using MapWith = AbstractFunctionStore::MapWith;
  using CreatePartFromBinFuncT = AbstractFunctionStore::CreatePartFromBinFuncT;
  using WritePartFuncT = AbstractFunctionStore::WritePartFuncT;
  using GetterFuncT = AbstractFunctionStore::GetterFuncT;

  // Used by engine.
  const MergeCombineFuncT& GetMergeCombine(int id);
  const MapFuncT& GetMap(int id);
  const MapWith& GetMapWith(int id);
  const JoinFuncT& GetJoin(int id);
  const CreatePartFromBinFuncT& GetCreatePartFromBin(int id);
  const CreatePartFromBlockReaderFuncT& GetCreatePartFromBlockReader(int id);
  const WritePartFuncT& GetWritePartFunc(int id);
  const std::map<int, GetterFuncT>& GetGetter();
  const CreatePartFuncT& GetCreatePart(int id);

  // Used by plan to register function.
  virtual void AddMap(int id, MapFuncT func) override;
  virtual void AddMergeCombine(int id, MergeCombineFuncT func) override;
  virtual void AddJoin(int id, JoinFuncT func) override;
  virtual void AddMapWith(int id, MapWith func) override;
  virtual void AddCreatePartFromBinFunc(int id, CreatePartFromBinFuncT func) override;
  virtual void AddCreatePartFromBlockReaderFunc(int id, CreatePartFromBlockReaderFuncT func) override;
  virtual void AddWritePart(int id, WritePartFuncT func) override;
  virtual void AddGetter(int id, GetterFuncT func) override;
  virtual void AddCreatePartFunc(int id, CreatePartFuncT func) override;

 private:
  std::map<int, MapFuncT> maps_;
  std::map<int, MergeCombineFuncT> merge_combines_;
  std::map<int, JoinFuncT> joins_;
  std::map<int, MapWith> mapwiths_;

  std::map<int, CreatePartFromBinFuncT> create_part_from_bin_;
  std::map<int, CreatePartFromBlockReaderFuncT> create_part_from_block_reader_;
  std::map<int, WritePartFuncT> write_part_;
  std::map<int, GetterFuncT> getter_;
  std::map<int, CreatePartFuncT> create_part_;
};

}  // namespaca xyz

