#pragma once

#include "core/plan/plan_base.hpp"
#include "core/partition/seq_partition.hpp"

namespace xyz {

template<typename T>
struct Load : public PlanBase {
  Load(int _plan_id, int _collection_id, std::string _url, 
          std::function<T(std::string&)> f, int l)
      : PlanBase(_plan_id), collection_id(_collection_id), url(_url), 
        parse_line(f), max_line_per_part(l) {}

  virtual SpecWrapper GetSpec() override {
    SpecWrapper w;
    w.SetSpec<LoadSpec>(plan_id, SpecWrapper::Type::kLoad, 
            collection_id, url);
    w.name = name;
    return w;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    function_store->AddCreatePartFromBlockReaderFunc(collection_id, [this](std::shared_ptr<AbstractBlockReader> reader) {
      auto part = std::make_shared<SeqPartition<T>>();
      int count = 0;
      while (reader->HasLine()) {
        auto s = reader->GetLine();
        part->Add(parse_line(s));
        count += 1;
        if (count == max_line_per_part) {
          break;
        }
      }
      return part;
    });
  }

  std::function<T(std::string&)> parse_line;
  std::string url;
  int collection_id;

  int max_line_per_part;
};

} // namespace xyz

