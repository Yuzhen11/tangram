#pragma once

#include <mutex>
#include <condition_variable>

#include "comm/abstract_sender.hpp"
#include "base/actor.hpp"
#include "base/message.hpp"
#include "base/node.hpp"
#include "base/sarray_binstream.hpp"
#include "io/abstract_reader.hpp"
#include "io/meta.hpp"

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"

namespace xyz {

class HdfsLoader: public Actor {
 public:
  HdfsLoader(int qid, std::shared_ptr<AbstractSender> sender, 
          std::shared_ptr<AbstractReader> reader,
          std::shared_ptr<Executor> executor,
          std::shared_ptr<PartitionManager> partition_manager,
          std::string namenode, int port,
          Node node)
      : Actor(qid), sender_(sender), reader_(reader), executor_(executor),
        partition_manager_(partition_manager), namenode_(namenode), port_(port),
        node_(node) {
    Start();
  }

  virtual ~HdfsLoader() {
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this]() { return num_finished_ == num_added_; });
    Stop();
  }

  virtual void Process(Message msg) override;

  void Load(AssignedBlock block);
 private:
  std::shared_ptr<AbstractSender> sender_;
  std::shared_ptr<AbstractReader> reader_;
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<PartitionManager> partition_manager_;

  std::string namenode_;
  int port_;

  Node node_;

  int num_added_ = 0;
  int num_finished_ = 0;
  std::mutex mu_;
  std::condition_variable cond_;
};

}  // namespace xyz

