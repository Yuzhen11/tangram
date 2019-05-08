#include "core/engine.hpp"
#include "core/queue_node_map.hpp"

#include "io/hdfs_reader.hpp"
#include "io/hdfs_writer.hpp"
#include "io/hdfs_block_reader.hpp"

#include <chrono>

namespace xyz {

void Engine::Init(Engine::Config config) {
  engine_elem_.executor = std::make_shared<Executor>(config.num_local_threads);
  engine_elem_.partition_manager = std::make_shared<PartitionManager>();
  engine_elem_.collection_map = std::make_shared<CollectionMap>();
  engine_elem_.function_store = std::make_shared<FunctionStore>();
  engine_elem_.namenode = config.namenode;
  engine_elem_.port = config.port;
  engine_elem_.num_local_threads = config.num_local_threads;
  engine_elem_.num_update_threads = config.num_update_threads;
  engine_elem_.num_combine_threads = config.num_combine_threads;
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

  // create fetcher
  const int fetcher_id = GetFetcherQid(engine_elem_.node.id);
  fetcher_ = std::make_shared<Fetcher>(fetcher_id, 
          engine_elem_.function_store,
          engine_elem_.partition_manager,
          engine_elem_.collection_map, engine_elem_.sender);
  mailbox_->RegisterQueue(fetcher_id, fetcher_->GetWorkQueue());
  engine_elem_.fetcher = fetcher_;  // set it to engine_elem_ as worker needs it

  const std::string namenode = engine_elem_.namenode;
  const int port = engine_elem_.port;
  // set hdfs io_wrapper
  auto io_wrapper = std::make_shared<IOWrapper>(
      [namenode, port]() {
        return std::make_shared<HdfsReader>(namenode, port);
      },
      [namenode, port]() {
        return std::make_shared<HdfsWriter>(namenode, port);
      });

  // create controller
  const int controller_id = GetControllerActorQid(engine_elem_.node.id);
  controller_ = std::make_shared<Controller>(controller_id, engine_elem_, io_wrapper);
  mailbox_->RegisterQueue(controller_id, controller_->GetWorkQueue());

  // create worker actor
  const int worker_id = GetWorkerQid(engine_elem_.node.id);

  // create worker
  worker_ = std::make_shared<Worker>(worker_id, engine_elem_, 
          io_wrapper,
          [namenode, port]() { return std::make_shared<HdfsBlockReader>(namenode, port); });
  worker_->SetProgram(program_);
  mailbox_->RegisterQueue(worker_id, worker_->GetWorkQueue());

  // make worker ready after register the queue.
  worker_->RegisterProgram();

  worker_->Wait();
}

void Engine::Stop() {
  mailbox_->Stop();
  worker_.reset();
  fetcher_.reset();
  controller_.reset();
}

} // namespace xyz
