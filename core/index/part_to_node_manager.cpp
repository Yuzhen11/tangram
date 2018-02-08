#include "core/index/part_to_node_manager.hpp"

namespace xyz {

void PartToNodeManager::Process(Message msg) {
}

void PartToNodeManager::Initialize(Message msg) {
  SArrayBinStream bin;
  bin.FromMsg(msg);
  map_.clear();
  while (bin.Size()) {
    int collection_id;
    std::shared_ptr<SimplePartToNodeMapper> map;
    bin >> collection_id;
    map->FromBin(bin);
    map_.insert({collection_id, std::move(map)});
  }
  Message reply_msg;
  // TODO: fill the meta
  sender_->Send(std::move(reply_msg));
}

}  // namespace xyz

