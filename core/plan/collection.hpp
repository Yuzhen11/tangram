#pragma once

#include <vector>
#include <memory>
#include <sstream>

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/collection_spec.hpp"
#include "core/scheduler/collection_view.hpp"

#include "core/index/abstract_key_to_part_mapper.hpp"
#include "core/partition/indexed_seq_partition.hpp"


namespace xyz {

template<typename T, typename PartitionT = IndexedSeqPartition<T>>
class Collection {
 public:
  using ObjT = T;
  Collection(int id): Collection(id, 1) {}
  Collection(int id, int num_part): 
    id_(id), num_partition_(num_part) {
  }
  
  int Id() const {
    return id_;
  }

  void Distribute(std::vector<T> data) {
    source_ = CollectionSource::kDistribute;
    data_ = data;
  }

  void Load(std::string url, std::function<T(std::string&)> parse_line) {
    source_ = CollectionSource::kLoad;
    load_url_ = url;
    parse_line_ = parse_line;
  }

  void SetWriteObj(std::function<void(ObjT& obj, std::stringstream& ss)> write_obj) {
    write_obj_ = write_obj;
  }
  
  void SetMapper(std::shared_ptr<AbstractKeyToPartMapper> mapper) {
    mapper_ = mapper;
  }
  std::shared_ptr<AbstractKeyToPartMapper> GetMapper() {
    return mapper_;
  }

  CollectionSpec GetSpec() {
    CollectionSpec s;
    s.collection_id = id_;
    s.num_partition = num_partition_;
    s.data << data_;
    s.source = source_;
    s.load_url = load_url_;
    return s;
  }

  void Register(std::shared_ptr<AbstractFunctionStore> function_store) {
    if (source_ == CollectionSource::kLoad) {
      RegisterCreatePartFromBlockReader(function_store);
    } else {  // kDistribute and kOthers
      RegisterCreatePartFromBin(function_store);
    }
    if (write_obj_) {
      RegisterWritePart(function_store);
    }
  }

  void RegisterCreatePartFromBlockReader(std::shared_ptr<AbstractFunctionStore> function_store) {
    function_store->AddCreatePartFromBlockReaderFunc(id_, [this](std::shared_ptr<AbstractBlockReader> reader) {
      auto part = std::make_shared<PartitionT>();
      while (reader->HasLine()) {
        auto s = reader->GetLine();
        part->Add(parse_line_(s));
      }
      return part;
    });
  }
  
  void RegisterCreatePartFromBin(std::shared_ptr<AbstractFunctionStore> function_store) {
    function_store->AddCreatePartFromBinFunc(id_, [](SArrayBinStream bin, int part_id, int num_part) {
      auto part = std::make_shared<PartitionT>();
      int i = 0;
      std::vector<T> vec;
      bin >> vec;
      for (auto elem : vec) {
        if (i % num_part == part_id) {
          part->Add(elem);
        }
        i += 1;
      }
      return part;
    });
  }

  void RegisterWritePart(std::shared_ptr<AbstractFunctionStore> function_store) {
    function_store->AddWritePart(id_, [this](std::shared_ptr<AbstractPartition> part, 
                    std::shared_ptr<AbstractWriter> writer, std::string url) {
      std::stringstream ss;
      auto* p = static_cast<TypedPartition<ObjT>*>(part.get());
      CHECK_NOTNULL(p);
      for (auto& elem : *p) {
        write_obj_(elem, ss);
      }
      ss.seekg(0, std::ios::end);
      auto size = ss.tellg();
      writer->Write(url, ss.rdbuf(), size);  // TODO: test these lines
    });
  }
 private:
  int id_;
  int num_partition_;
  CollectionSource source_ = CollectionSource::kOthers;
  // from distribute
  std::vector<T> data_;
  // from hdfs file
  std::string load_url_;
  std::function<T(std::string&)> parse_line_;
  std::function<void(ObjT&, std::stringstream& ss)> write_obj_;

  std::shared_ptr<AbstractKeyToPartMapper> mapper_;


};

}  // namespace

