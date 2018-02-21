#pragma once

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
};

}  // namespace xyz

