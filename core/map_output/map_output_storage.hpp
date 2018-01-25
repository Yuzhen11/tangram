#pragma once

#include <unordered_map>

#include "core/map_output/abstract_map_output.hpp"

namespace xyz {

class MapOutputManager {
 public:
  MapOutputManager() = default;
  void Add(int plan_id, std::shared_ptr<AbstractMapOutput> map_output);
  const std::vector<std::shared_ptr<AbstractMapOutput>>& Get(int plan_id);
 private:
  std::unordered_map<int, std::vector<std::shared_ptr<AbstractMapOutput>>> map_outputs_;
};

}  // namespace

