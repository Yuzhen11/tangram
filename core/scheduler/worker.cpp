#include "core/scheduler/worker.hpp"
#include "core/plan/collection_spec.hpp"
#include "core/queue_node_map.hpp"
#include "core/shuffle_meta.hpp"
#include "io/meta.hpp"

#include "core/plan/spec_wrapper.hpp"

#include "core/partition/block_partition.hpp"

namespace xyz {

void Worker::Wait() {
  std::future<void> f = exit_promise_.get_future();
  f.get();
}

void Worker::RegisterProgram() {
  LOG(INFO) << WorkerId() << "RegisterProgram";
  CHECK(is_program_set_);
  ready_ = true;
  
  SArrayBinStream bin;
  WorkerInfo info;
  info.num_local_threads = engine_elem_.num_local_threads;
  bin << info;
  bin << program_;
  SendMsgToScheduler(ScheduleFlag::kRegisterProgram, bin);
}

void Worker::Process(Message msg) {
  CHECK_EQ(msg.data.size(), 2); // cmd, content
  SArrayBinStream ctrl_bin;
  SArrayBinStream bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  ScheduleFlag flag;
  ctrl_bin >> flag;

  switch (flag) {
  // case ScheduleFlag::kInitWorkers: {
  //   InitWorkers(bin);
  //   break;
  // }
  case ScheduleFlag::kUpdateCollection: {
    UpdateCollection(bin);
    break;
  }
  case ScheduleFlag::kLoadBlock: {
    LoadBlock(bin);
    break;
  }
  case ScheduleFlag::kDummy: {
    RunDummy();
    break;
  }
  case ScheduleFlag::kExit: {
    Exit();
    break;
  }
  case ScheduleFlag::kDistribute: {
    Distribute(bin);
    break;
  }
  case ScheduleFlag::kCheckpoint: {
    CheckPoint(bin);
    break;
  }
  case ScheduleFlag::kLoadCheckpoint: {
    LoadCheckPoint(bin);
    break;
  }
  case ScheduleFlag::kWritePartition: {
    WritePartition(bin);
    break;
  }
  default:
    CHECK(false);
  }
}

void Worker::UpdateCollection(SArrayBinStream bin) {
  int collection_id;
  CollectionView cv;
  bin >> collection_id >> cv;
  // when we update a collection_view, no one is accessing it
  engine_elem_.collection_map->Insert(cv);

  SArrayBinStream reply_bin;
  reply_bin << collection_id << engine_elem_.node.id;
  SendMsgToScheduler(ScheduleFlag::kUpdateCollectionReply, reply_bin);
}

void Worker::RunDummy() { LOG(INFO) << WorkerId() << "RunDummy"; }

void Worker::LoadBlock(SArrayBinStream bin) {
  AssignedBlock block;
  bin >> block;
  LOG(INFO) << WorkerId() << "LoadBlock: " << block.DebugString();

  if (block.is_load_meta) {
    auto p = std::make_shared<BlockPartition>(block, block_reader_getter_);
    engine_elem_.partition_manager->Insert(block.collection_id, block.id, std::move(p));

    SArrayBinStream reply_bin;
    FinishedBlock b{block.id, engine_elem_.node.id, Qid(), engine_elem_.node.hostname,
                    block.collection_id};
    reply_bin << b;
    VLOG(1) << "Finish block: " << b.DebugString();
    SendMsgToScheduler(ScheduleFlag::kFinishBlock, reply_bin);
  } else {
    engine_elem_.executor->Add([this, block]() {
      // read
      CHECK(block_reader_getter_);
      auto block_reader = block_reader_getter_();
      block_reader->Init(block.url, block.offset);
      auto read_func = engine_elem_.function_store->GetCreatePartFromBlockReader(block.collection_id); 
      auto part = read_func(block_reader);
      engine_elem_.partition_manager->Insert(block.collection_id, block.id, std::move(part));

      // 2. reply
      SArrayBinStream reply_bin;
      FinishedBlock b{block.id, engine_elem_.node.id, Qid(), engine_elem_.node.hostname,
                      block.collection_id};
      reply_bin << b;
      VLOG(1) << "Finish block: " << b.DebugString();
      SendMsgToScheduler(ScheduleFlag::kFinishBlock, reply_bin);
    });
  }

  // TODO: enable this only for tfidf (parse file)
  /*
  engine_elem_.executor->Add([this, block]() {
    auto reader = io_wrapper_->GetReader();
    CHECK_EQ(block.offset, 0);
    reader->Init(block.url);
    size_t file_size = reader->GetFileSize();
    CHECK_GT(file_size, 0);

    // char *data = new char[file_size];
    // reader->Read(data, file_size);
    // std::string str(data);  // TODO: remove the copy

    std::string str;
    str.resize(file_size);
    reader->Read(&str[0], file_size);

    auto func = engine_elem_.function_store->GetCreatePartFromString(block.collection_id);
    auto part = func(std::move(str));
    engine_elem_.partition_manager->Insert(block.collection_id, block.id, std::move(part));
    // delete data;

    SArrayBinStream reply_bin;
    FinishedBlock b{block.id, engine_elem_.node.id, Qid(), engine_elem_.node.hostname,
                    block.collection_id};
    reply_bin << b;
    VLOG(1) << "Finish block: " << b.DebugString();
    SendMsgToScheduler(ScheduleFlag::kFinishBlock, reply_bin);
  });
  */
}

void Worker::Distribute(SArrayBinStream bin) {
  // LOG(INFO) << WorkerId() << "[Worker] Distribute";
  int part_id, plan_id;
  DistributeSpec spec;
  bin >> part_id >> plan_id;
  spec.FromBin(bin);
  auto func = engine_elem_.function_store->GetCreatePartFromBin(spec.collection_id);
  auto part = func(spec.data, part_id, spec.num_partition);
  engine_elem_.partition_manager->Insert(spec.collection_id, part_id, std::move(part));
  SArrayBinStream reply_bin;
  reply_bin << spec.collection_id << part_id << engine_elem_.node.id << plan_id;
  SendMsgToScheduler(ScheduleFlag::kFinishDistribute, reply_bin);
}

void Worker::CheckPoint(SArrayBinStream bin) {
  int collection_id, part_id;
  std::string dest_url;
  bin >> collection_id >> part_id >> dest_url;

  engine_elem_.executor->Add([this, collection_id, part_id, dest_url]() {
    // 1. write
    auto writer = io_wrapper_->GetWriter();
    CHECK(engine_elem_.partition_manager->Has(collection_id, part_id));
    auto part = engine_elem_.partition_manager->Get(collection_id, part_id);

    SArrayBinStream bin;
    part->ToBin(bin);
    bool rc = writer->Write(dest_url, bin.GetPtr(), bin.Size());
    CHECK_EQ(rc, 0);

    // 2. reply
    SArrayBinStream reply_bin;
    reply_bin << Qid() << collection_id;
    SendMsgToScheduler(ScheduleFlag::kFinishCheckpoint, reply_bin);
  });
}

void Worker::LoadCheckPoint(SArrayBinStream bin) {
  int collection_id, part_id;
  std::string dest_url;
  bin >> collection_id >> part_id >> dest_url;

  engine_elem_.executor->Add([this, collection_id, part_id, dest_url]() {
    // 1. read
    auto reader = io_wrapper_->GetReader();
    reader->Init(dest_url);
    size_t file_size = reader->GetFileSize();
    CHECK_NE(file_size, 0);
    // LOG(INFO) << "file_size: " << std::to_string(file_size);
    
    SArrayBinStream bin;
    bin.Resize(file_size);
    reader->Read(bin.GetBegin(), file_size);

    // 2. construct partition and insert
    auto get_func = engine_elem_.function_store->GetCreatePart(collection_id);
    auto p = get_func();
    p->FromBin(bin);
    engine_elem_.partition_manager->Insert(collection_id, part_id, std::move(p));

    // 3. reply
    SArrayBinStream reply_bin;
    reply_bin << Qid() << collection_id << part_id;
    SendMsgToScheduler(ScheduleFlag::kFinishLoadCheckpoint, reply_bin);
  });
}

void Worker::WritePartition(SArrayBinStream bin) {
  int collection_id, part_id;
  std::string dest_url;
  bin >> collection_id >> part_id >> dest_url;

  engine_elem_.executor->Add([this, collection_id, part_id, dest_url]() {
    // 1. write
    auto writer = io_wrapper_->GetWriter();
    CHECK(engine_elem_.partition_manager->Has(collection_id, part_id));
    auto part = engine_elem_.partition_manager->Get(collection_id, part_id);
    auto write_part_func = engine_elem_.function_store->GetWritePartFunc(collection_id);
    write_part_func(part, writer, dest_url);

    // 2. reply
    SArrayBinStream reply_bin;
    reply_bin << Qid() << collection_id;
    SendMsgToScheduler(ScheduleFlag::kFinishWritePartition, reply_bin);
  });
}

void Worker::Exit() { 
  LOG(INFO) << WorkerId() << "Exit";
  exit_promise_.set_value(); 
}

void Worker::SendMsgToScheduler(ScheduleFlag flag, SArrayBinStream bin) {
  Message msg;
  msg.meta.sender = GetWorkerQid(engine_elem_.node.id);
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  engine_elem_.sender->Send(std::move(msg));
}

} // namespace xyz
