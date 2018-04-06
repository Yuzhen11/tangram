#pragma once

#include "core/plan/plan_base.hpp"
#include "core/partition/seq_partition.hpp"

namespace xyz {

template<typename T>
struct Load : public PlanBase {
  Load(int _plan_id, int _collection_id, std::string _url, 
          std::function<T(std::string)> f, int l, bool _is_whole_file)
      : PlanBase(_plan_id), collection_id(_collection_id), url(_url), 
        parse_line(f), max_line_per_part(l), is_load_meta(false),
        is_whole_file(_is_whole_file) {}

  Load(int _plan_id, int _collection_id, std::string _url, bool _is_whole_file)
    : PlanBase(_plan_id), collection_id(_collection_id), url(_url), 
    is_load_meta(true), is_whole_file(_is_whole_file) {
  }

  virtual SpecWrapper GetSpec() override {
    SpecWrapper w;
    w.SetSpec<LoadSpec>(plan_id, SpecWrapper::Type::kLoad, 
            collection_id, url, is_load_meta, is_whole_file);
    w.name = name;
    return w;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    if (is_load_meta) {
      return;
    }
    CHECK(parse_line);
    function_store->AddCreatePartFromBlockReaderFunc(collection_id, [this](std::shared_ptr<AbstractBlockReader> reader) {
      auto part = std::make_shared<SeqPartition<T>>();
      int count = 0;
      while (reader->HasLine()) {
        auto s = reader->GetLine();
        part->Add(parse_line(std::move(s)));
        count += 1;
        if (count == max_line_per_part) {
          break;
        }
      }
      return part;
    });

    // TODO: enable this only for tfidf (parse file)
    function_store->AddCreatePartFromStringFunc(collection_id, [this](std::string str) { 
      auto part = std::make_shared<SeqPartition<T>>();
      std::string::size_type pos = 0;
      std::string::size_type prev = 0;
      std::string doc;
      while((pos = str.find('\n', prev)) != std::string::npos) {
        std::string line = str.substr(prev, pos - prev);
        if(line.compare("</doc>") == 0) {
          part->Add(parse_line(doc));
          doc.clear();
        }          
        else {
          doc += " " + line;
        }
        prev = pos + 1;
      }
      return part;
    });
  }

 private:
  std::function<T(std::string)> parse_line;
  std::string url;
  int collection_id;

  int max_line_per_part;
  bool is_load_meta;  // load meta only, instead of the real block.
  bool is_whole_file;
};

} // namespace xyz

