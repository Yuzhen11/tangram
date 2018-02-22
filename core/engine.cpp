#include "core/engine.hpp"
#include "core/join_actor.hpp"

#include <chrono>

namespace xyz {

Engine::Engine(int thread_pool_size)
    : executor_(new Executor(thread_pool_size)), thread_pool_size_(thread_pool_size),
      partition_manager_(new PartitionManager),
      function_store_(new FunctionStore),
      intermediate_store_(new SimpleIntermediateStore){}

Engine::~Engine() {
}

void Engine::Start(Engine::Config config) {
  executor_ = std::make_shared<Executor>(config.num_threads);
  partition_manager_ = std::make_shared<PartitionManager>();
  function_store_ = std::make_shared<FunctionStore>();
  intermediate_store_ = std::make_shared<SimpleIntermediateStore>();
  partition_tracker_ = std::make_shared<PartitionTracker>(partition_manager_, executor_);

  // start mailbox
  Node scheduler_node{0, config.scheduler_port, false};
  mailbox_ = std::make_shared<Mailbox>(false, scheduler_node, config.num_workers);
  mailbox_->Start();
  Node my_node = mailbox_->my_node();
  sender_ = std::make_shared<Sender>();  // TODO

  // create all actors
  worker_ = std::make_shared<Worker>(qid, sender_, 
          partition_tracker_, function_store_);// TODO
  join_actor_ = std::make_shared<JoinActor>(qid, 
          partition_manager_, executor_, function_store_);
}

void Engine::Run() {
  worker_->RegisterPlan(...);
  worker_->Wait();
}

void Engine::Stop() {
}


}  // namespace

