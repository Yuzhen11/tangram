#include "io/hdfs_manager.hpp"

#include "io/hdfs_assigner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace flexps {

HDFSManager::HDFSManager(Node node, const std::vector<Node>& nodes, const Config& config)
    : node_(node),
      nodes_(nodes),
      config_(config) {
  CHECK(!nodes.empty());
  CHECK(CheckValidNodeIds(nodes));
  CHECK(HasNode(nodes, 0));
  CHECK(config_.num_local_load_thread);
}
void HDFSManager::Start() {
  if (node_.id == 0) {
    hdfs_main_thread_ = std::thread([this] {
      HDFSBlockAssigner hdfs_block_assigner(config_.hdfs_namenode, config_.hdfs_namenode_port, 
                                            config_.master_port, app_thread_id);
      hdfs_block_assigner.Serve();
    });
  }
}

void HDFSManager::Run() {
  int num_threads = nodes_.size() * config_.num_local_load_thread;
  std::vector<std::thread> threads;
  for (int i = 0; i < config_.num_local_load_thread; ++i) {
    std::thread load_thread = std::thread([this, num_threads, i, func, node_.id] {
      InputFormat input_format(config_, num_threads, node_.id);
      while (input_format->HasRecord()) {
        auto record = input_format->GetNextRecord();
      }
      Message msg;
      msg.meta.sender = node_.id;
      msg.meta.recver = 0;
      SarrayBinStream finish_signal;
      finish_signal << config_.worker_host << node_.id * config_.num_local_load_thread + i << 300;
      msg.AddData(finish_signal.toSArray());
      sender->send(msg);
    });
    threads.push_back(std::move(load_thread));
  }
  for (int i = 0; i < config_.num_local_load_thread; ++i) {
    threads[i].join();
  }
  
}

void HDFSManager::Stop() {
  if (node_.id == 0) {  // join only for node 0
    hdfs_main_thread_.join();
  }
}

}  // namespace flexps
