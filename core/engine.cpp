#include "core/engine.hpp"
#include "core/join_actor.hpp"

#include <chrono>

namespace xyz {

void Engine::Start(Engine::Config config) {
  engine_elem_.executor = std::make_shared<Executor>(config.num_threads);
  engine_elem_.partition_manager = std::make_shared<PartitionManager>();
  engine_elem_.function_store = std::make_shared<FunctionStore>();
  engine_elem_.intermediate_store = std::make_shared<SimpleIntermediateStore>();
  engine_elem_.partition_tracker = std::make_shared<PartitionTracker>(
          engine_elem_.partition_manager, engine_elem_.executor);
  engine_elem_.namenode = config.namenode;
  engine_elem_.port = config.port;

  // start mailbox
  Node scheduler_node{0, config.scheduler, config.scheduler_port, false};
  mailbox_ = std::make_shared<Mailbox>(false, scheduler_node, config.num_workers);
  mailbox_->Start();
  engine_elem_.node = mailbox_->my_node();
  engine_elem_.sender = std::make_shared<Sender>(-1, mailbox_.get());

  // create all actors
  const int worker_id = engine_elem_.node.id * 10;
  const int join_actor_id = engine_elem_.node.id * 10 + 1;
  worker_ = std::make_shared<Worker>(worker_id, engine_elem_);
  join_actor_ = std::make_shared<JoinActor>(join_actor_id, 
          engine_elem_.partition_manager, engine_elem_.executor, engine_elem_.function_store);

  mailbox_->RegisterQueue(worker_id, worker_->GetWorkQueue());
  mailbox_->RegisterQueue(join_actor_id, join_actor_->GetWorkQueue());
  mailbox_->Barrier();
}

void Engine::Run() {
  // worker_->RegisterPlan(...);
  // worker_->Wait();
}

void Engine::Stop() {
  mailbox_->Stop();
}


}  // namespace xyz

