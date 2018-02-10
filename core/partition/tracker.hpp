#pragma once

#include "core/partition/task_timer.hpp"

namespace xyz {

enum class TaskStatus {
  Pending, Running, Finished
};

struct JoinTaskTracker {
  JoinPartTracker(): status(TaskStatus::Pending) {}
  void Run() { 
    CHECK_EQ(status, TaskStatus::Pending);
    timer.Run(); 
    status = TaskStatus::Running;
  }
  void Finish() {
    CHECK_EQ(status, TaskStatus::Running);
    timer.Finish();
    status = TaskStatus::Finished;
  }
  TaskTimer timer;
  TaskStatus status;
};

struct JoinPartTracker {
  bool Has(int up_id) {
    return tracker.find(up_id) != tracker.end();
  }
  void Add(int up_id) {
    tracker.insert({up_id, JoinTaskTracker()});
  }
  void Run(int up_id) {
    CHECK(tracker.find(up_id) != tracker.end());
    tracker[up_id].Run();
    running.insert(up_id);
  }
  void Finish(int up_id) {
    CHECK(tracker.find(up_id) != tracker.end());
    tracker[up_id].Finish();
    running.erase(up_id);
    finished.insert(up_id);
  }
  std::map<int, JoinTaskTracker> tracker;  // upstream_part_id -> tracker
  std::set<int> running;
  std::set<int> finished;
};

struct JoinTracker {
  void Add(int part_id, int up_id) {
    join_tracker_[part_id].Add(up_id);
  }
  bool Has(int part_id, int up_id) {
    return join_tracker_[part_id].Has(up_id);
  }
  void Start(int part_id, int up_id) {
    join_tracker_[part_id].Start(up_id);
  }
  void Finish(int part_id, int up_id) {
    join_tracker_[part_id].Finish(up_id);
  }
  std::map<int, JoinPartTracker> join_tracker_;
};

}  // namespace

