#pragma once

#include <set>
#include <sstream>

#include "core/partition/task_timer.hpp"
#include "core/partition/abstract_map_progress_tracker.hpp"

#include "glog/logging.h"

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

  std::string DebugString() const {
    std::stringstream ss;
    if (status == TaskStatus::Pending) ss << "{TaskStatus: Pending} ";
    if (status == TaskStatus::Running) ss << "{TaskStatus: Running} ";
    if (status == TaskStatus::Finished) ss << "{TaskStatus: Finished} ";
    return ss.str();
  }
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
    CHECK(tracker.find(up_id) != tracker.end()) << up_id;
    tracker[up_id].Run();
    running.insert(up_id);
  }
  void Finish(int up_id) {
    CHECK(tracker.find(up_id) != tracker.end());
    tracker[up_id].Finish();
    running.erase(up_id);
    finished.insert(up_id);
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ finished: " << finished.size();
    ss << ", running: " << running.size();
    ss << ", TaskStatus: ";
    //for (auto task_tracker: tracker) ss << "<" << task_tracker.second.DebugString() << ">"; 
    //ss << " >}";
    return ss.str();
  }
  std::map<int, TaskTracker> tracker;  // upstream_part_id -> tracker
  std::set<int> running;
  std::set<int> finished;
};

// A node will hold some partitions.
class JoinTracker {
 public:
  JoinTracker(int num_local_part, int num_upstream_part) {
    num_local_part_ = num_local_part;
    num_upstream_part_ = num_upstream_part;
  }
  void Add(int part_id, int up_id) {
    std::lock_guard<std::mutex> lk(mu);
    tracker[part_id].Add(up_id);
  }
  bool Has(int part_id, int up_id) {
    std::lock_guard<std::mutex> lk(mu);
    return tracker[part_id].Has(up_id);
  }
  void Run(int part_id, int up_id) {
    std::lock_guard<std::mutex> lk(mu);
    CHECK(tracker.find(part_id) != tracker.end());
    tracker[part_id].Run(up_id);
  }
  void Finish(int part_id, int up_id) {
    std::lock_guard<std::mutex> lk(mu);
    CHECK(tracker.find(part_id) != tracker.end());
    tracker[part_id].Finish(up_id);
  }
  void Clear() {
    std::lock_guard<std::mutex> lk(mu);
    tracker.clear();
  }
  bool FinishAll() {
    std::lock_guard<std::mutex> lk(mu);
    if (tracker.size() < num_local_part_) {
      return false;
    }
    for (auto kv: tracker) {
      if (kv.second.finished.size() < num_upstream_part_) {
        return false;
      }
    }
    return true;
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ num_local_part_:" << num_local_part_;
    ss << ", num_upstream_part_:" << num_upstream_part_;
    ss << ", tracker size:" << tracker.size();
    ss << ", tracker: ";
    for (auto kv : tracker) {
      ss << "<" << kv.first << ", " << kv.second.DebugString() << "> ";
    }
    ss << "}";
    return ss.str();
  }
 private:
  int num_local_part_;
  int num_upstream_part_;
  std::map<int, JoinPartTracker> tracker;
  std::mutex mu;
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
  int num_finished = 0;
  int num_objs;
  int id;
};

// A node will have some map partition
// TODO do we need to protect this structure?
class MapTracker {
 public:
  void Add(int part_id, int num_objs) {
    std::lock_guard<std::mutex> lk(mu_);
    tracker.insert({part_id, std::make_shared<MapTaskTracker>(map_count, num_objs)});
    map_count += 1;
  }
  void Run(int part_id) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(tracker.find(part_id) != tracker.end());
    tracker[part_id]->Run();
  }
  void Finish(int part_id) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(tracker.find(part_id) != tracker.end());
    tracker[part_id]->Finish();
  }
  std::shared_ptr<MapTaskTracker> GetTaskTracker(int part_id) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(tracker.find(part_id) != tracker.end());
    return tracker[part_id];
  }
  void Clear() {
    std::lock_guard<std::mutex> lk(mu_);
    LOG(INFO) << "[MapTracker] finish all map, clear tracker";
    tracker.clear();
    map_count = 0;
  }

 private:
  int map_count = 0;  // to record the creating order of the map tasks.
  std::map<int, std::shared_ptr<MapTaskTracker>> tracker;
  std::mutex mu_;
};

}  // namespace xyz

