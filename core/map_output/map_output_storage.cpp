#include "core/map_output/map_output_storage.hpp"

namespace xyz {

void MapOutputManager::Add(int plan_id, std::shared_ptr<AbstractMapOutput> map_output) {
  map_outputs_[plan_id].push_back(std::move(map_output));
}

const std::vector<std::shared_ptr<AbstractMapOutput>>& MapOutputManager::Get(int plan_id) {
  return map_outputs_[plan_id];
}

}  // namespace

