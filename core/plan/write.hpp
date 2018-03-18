#pragma once

#include "core/plan/plan_base.hpp"
#include "core/partition/abstract_partition.hpp"

namespace xyz {

template<typename T>
struct Write : public PlanBase {
  Write(int _plan_id, int _collection_id, std::string _url, std::function<void(const T& obj, std::stringstream& ss)> f)
      : PlanBase(_plan_id), collection_id(_collection_id), url(_url), write_obj(f) {}

  virtual SpecWrapper GetSpec() override {
    SpecWrapper w;
    w.SetSpec<WriteSpec>(plan_id, SpecWrapper::Type::kWrite, 
            collection_id, url);
    w.name = name;
    return w;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    function_store->AddWritePart(collection_id, [this](std::shared_ptr<AbstractPartition> part, 
                    std::shared_ptr<AbstractWriter> writer, std::string url) {
      std::stringstream ss;
      auto* p = static_cast<TypedPartition<T>*>(part.get());
      CHECK_NOTNULL(p);
      for (auto& elem : *p) {
        write_obj(elem, ss);
      }
      std::string s = ss.str();
      writer->Write(url, s.c_str(), s.size());
    });
  }

  std::function<void(T&, std::stringstream& ss)> write_obj;
  std::string url;
  int collection_id;
};

} // namespace xyz

