#include "core/scheduler/worker.hpp"
#include "core/plan/plan_spec.hpp"

namespace xyz {

void Worker::RegisterProgram(ProgramContext program) {
  SArrayBinStream bin;
  bin << program;
  SendMsgToScheduler(ScheduleFlag::kRegisterProgram, bin);
}

void Worker::Wait() {
  std::future<void> f = exit_promise_.get_future();
  f.get();
}


void Worker::Process(Message msg) {
  CHECK_EQ(msg.data.size(), 2);  // cmd, content
  SArrayBinStream ctrl_bin;
  SArrayBinStream bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  ScheduleFlag flag;
  ctrl_bin >> flag;
  switch (flag) {
    case ScheduleFlag::kInitWorkers: {
      InitWorkers(bin);
      break;
    }
    case ScheduleFlag::kLoadBlock: {
      LoadBlock(bin);
      break;
    }
    case ScheduleFlag::kExit: {
      Exit();
      break;
    }
    default: CHECK(false);
  }
}

void Worker::InitWorkers(SArrayBinStream bin) {
  bin >> collection_map_;
  InitWorkersReply();
}

void Worker::InitWorkersReply() {
  SArrayBinStream bin;
  SendMsgToScheduler(ScheduleFlag::kInitWorkersReply, bin);
}

void Worker::LoadBlock(SArrayBinStream bin) {
  AssignedBlock block;
  bin >> block;
  loader_->Load(block);
}

void Worker::Exit() {
  exit_promise_.set_value();
}

void Worker::SendMsgToScheduler(ScheduleFlag flag, SArrayBinStream bin) {
  Message msg;
  // TODO: Fill the meta
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  engine_elem_.sender->Send(std::move(msg));
}

}  // namespace xyz


