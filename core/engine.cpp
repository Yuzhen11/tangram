#include "core/engine.hpp"
#include "core/join_actor.hpp"
#include "core/queue_node_map.hpp"

#include <chrono>

namespace xyz {

void Engine::Init(Engine::Config config) {
  engine_elem_.executor = std::make_shared<Executor>(config.num_threads);
  engine_elem_.partition_manager = std::make_shared<PartitionManager>();
  engine_elem_.collection_map = std::make_shared<CollectionMap>();
  engine_elem_.function_store =
      std::make_shared<FunctionStore>(engine_elem_.collection_map);
  engine_elem_.namenode = config.namenode;
  engine_elem_.port = config.port;
  config_ = config;
}

void Engine::Start() {
  // create mailbox
  Node scheduler_node{0, config_.scheduler, config_.scheduler_port, false};
  mailbox_ =
      std::make_shared<WorkerMailbox>(scheduler_node);
  mailbox_->Start(); // start the mailbox so we can get the node

  engine_elem_.sender = std::make_shared<Sender>(-1, mailbox_.get());
  engine_elem_.node = mailbox_->my_node();

  engine_elem_.intermediate_store =
      std::make_shared<IntermediateStore>(engine_elem_.sender);
  engine_elem_.partition_tracker = std::make_shared<PartitionTracker>(
      engine_elem_.node.id, engine_elem_.partition_manager,
      engine_elem_.executor, engine_elem_.sender, engine_elem_.collection_map);

  // create join actor
  const int join_actor_id = GetJoinActorQid(engine_elem_.node.id);
  join_actor_ = std::make_shared<JoinActor>(
      join_actor_id, engine_elem_.partition_tracker, engine_elem_.executor,
      engine_elem_.function_store);
  mailbox_->RegisterQueue(join_actor_id, join_actor_->GetWorkQueue());

  // create worker actor
  const int worker_id = GetWorkerQid(engine_elem_.node.id);

  const std::string namenode = engine_elem_.namenode;
  const int port = engine_elem_.port;
  // set hdfs reader_wrapper
  auto reader_wrapper = std::make_shared<ReaderWrapper>(
      worker_id, engine_elem_.executor, engine_elem_.partition_manager,
      engine_elem_.node,
      [namenode, port]() { return std::make_shared<HdfsBlockReader>(namenode, port); });

  // set hdfs writer_wrapper
  auto writer_wrapper = std::make_shared<WriterWrapper>(
      worker_id, engine_elem_.executor, engine_elem_.partition_manager, [namenode, port]() {
        return std::make_shared<HdfsWriter>(namenode, port);
      });

  // create worker
  worker_ = std::make_shared<Worker>(worker_id, engine_elem_, reader_wrapper, writer_wrapper);
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

} // namespace xyz
