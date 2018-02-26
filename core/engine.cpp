#include "core/engine.hpp"
#include "core/join_actor.hpp"

#include <chrono>

namespace xyz {

void Engine::Init(Engine::Config config) {
  engine_elem_.executor = std::make_shared<Executor>(config.num_threads);
  engine_elem_.partition_manager = std::make_shared<PartitionManager>();
  engine_elem_.function_store = std::make_shared<FunctionStore>();
  engine_elem_.intermediate_store = std::make_shared<SimpleIntermediateStore>();
  engine_elem_.partition_tracker = std::make_shared<PartitionTracker>(
          engine_elem_.partition_manager, engine_elem_.executor);
  engine_elem_.namenode = config.namenode;
  engine_elem_.port = config.port;
  config_ = config;
}

void Engine::Start() {
  // create mailbox
  Node scheduler_node{0, config_.scheduler, config_.scheduler_port, false};
  mailbox_ = std::make_shared<WorkerMailbox>(scheduler_node, config_.num_workers);
  mailbox_->Start();  // start the mailbox so we can get the node

  engine_elem_.sender = std::make_shared<Sender>(-1, mailbox_.get());
  engine_elem_.node = mailbox_->my_node();

  // create join actor
  const int join_actor_id = engine_elem_.node.id * 10 + 1;
  join_actor_ = std::make_shared<JoinActor>(join_actor_id, 
          engine_elem_.partition_manager, engine_elem_.executor, engine_elem_.function_store);
  mailbox_->RegisterQueue(join_actor_id, join_actor_->GetWorkQueue());

  // create worker actor
  const int worker_id = engine_elem_.node.id * 10;
  // set hdfs reader 
  auto reader = std::make_shared<HdfsReader>();
  worker_ = std::make_shared<Worker>(worker_id, engine_elem_, reader);
  worker_->SetProgram(program_);
  mailbox_->RegisterQueue(worker_id, worker_->GetWorkQueue());


  // make worker ready after register the queue.
  worker_->RegisterProgram();

  worker_->Wait();
}

void Engine::Stop() {
  mailbox_->Stop();
  worker_.reset();
  join_actor_.reset();
}


}  // namespace xyz

