#pragma once

#include <set>

#include "core/partition/task_timer.hpp"
#include "core/partition/abstract_map_progress_tracker.hpp"

namespace xyz {

enum class TaskStatus {
  Pending, Running, Finished
};

struct TaskTracker {
  TaskTracker(): status(TaskStatus::Pending) {}
  void Run() { 
    CHECK(status == TaskStatus::Pending);
    timer.Run(); 
    status = TaskStatus::Running;
  }
  void Finish() {
    CHECK(status == TaskStatus::Running);
    timer.Finish();
    status = TaskStatus::Finished;
  }
  TaskTimer timer;
  TaskStatus status;
};

// A partition should have multiple tasks (the number of map tasks)
struct JoinPartTracker {
  bool Has(int up_id) {
    return tracker.find(up_id) != tracker.end();
  }
  void Add(int up_id) {
    tracker.insert({up_id, TaskTracker()});
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
  std::map<int, TaskTracker> tracker;  // upstream_part_id -> tracker
  std::set<int> running;
  std::set<int> finished;
};

// A node will hold some partitions.
struct JoinTracker {
  void Add(int part_id, int up_id) {
    tracker[part_id].Add(up_id);
  }
  bool Has(int part_id, int up_id) {
    return tracker[part_id].Has(up_id);
  }
  void Run(int part_id, int up_id) {
    CHECK(tracker.find(part_id) != tracker.end());
    tracker[part_id].Run(up_id);
  }
  void Finish(int part_id, int up_id) {
    CHECK(tracker.find(part_id) != tracker.end());
    tracker[part_id].Finish(up_id);
  }
  std::map<int, JoinPartTracker> tracker;
};

// MapTaskTracker is to record the progress of a map task
// num_objs represents the workload in this map
// id represents the order of executing the map
struct MapTaskTracker : public TaskTracker, public AbstractMapProgressTracker {
  MapTaskTracker() = default;
  // Do not need to protect the constructor since one thread will init it.
  // Protect all the other functions.
  MapTaskTracker(int _id, int _num_objs)
      :TaskTracker(), num_objs(_num_objs), id(_id) {}

  // The map will report progress through this interface
  virtual void Report(int _num_finished) override {
    std::lock_guard<std::mutex> lk(mu);
    num_finished = _num_finished;
  }

  std::pair<int,int> GetProgress() {
    std::lock_guard<std::mutex> lk(mu);
    CHECK(status == TaskStatus::Pending);
    return {num_finished, num_objs};
  }

  void Run() { 
    std::lock_guard<std::mutex> lk(mu);
    TaskTracker::Run();
  }

  void Finish() {
    std::lock_guard<std::mutex> lk(mu);
    TaskTracker::Finish();
  }

  std::mutex mu;
  int num_finished;
  int num_objs;
  int id;
};

// A node will have some map partition
struct MapTracker {
  void Add(int part_id, int num_objs) {
    tracker.insert({part_id, std::make_shared<MapTaskTracker>(map_count, num_objs)});
    map_count += 1;
  }
  void Run(int part_id) {
    CHECK(tracker.find(part_id) != tracker.end());
    tracker[part_id]->Run();
  }
  void Finish(int part_id) {
    CHECK(tracker.find(part_id) != tracker.end());
    tracker[part_id]->Finish();
  }

  int map_count = 0;  // to record the creating order of the map tasks.
  std::map<int, std::shared_ptr<MapTaskTracker>> tracker;
};

}  // namespace xyz

