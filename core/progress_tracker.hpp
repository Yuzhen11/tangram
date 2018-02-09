#pragma once

#include <chrono>
#include <map>
#include <mutex>

#include "partition/versioned_partition.hpp"

namespace xyz {

class PartProgress {
 public:
  PartProgress(int id, int num_objs) {
    id_ = id;
    num_objs_ = num_objs;
    Add();
  }
  void Add() {
    add_timepoint = std::chrono::steady_clock::now();
  }
  void Start() {
    start_timepoint = std::chrono::steady_clock::now();
  }
  void Finish() {
    end_timepoint = std::chrono::steady_clock::now();
  }

  int GetTimeFromAdd() {
    return GetTimeFrom(add_timepoint);
  }
  int GetTimeFromStart() {
    return GetTimeFrom(start_timepoint);
  }

 private:
  int GetTimeFrom(std::chrono::time_point<std::chrono::steady_clock> t) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t).count();
  }

  int num_objs_ = 0;
  int id_ = 0;

  std::chrono::time_point<std::chrono::steady_clock> add_timepoint;
  std::chrono::time_point<std::chrono::steady_clock> start_timepoint;
  std::chrono::time_point<std::chrono::steady_clock> end_timepoint;
};

class ProgressTracker {
 public:
  void AddMap(int part_id, int num_objs);
  void StartMap(int part_id);
  void FinishMap(int part_id);
 private:
  int num_finished_objs_ = 0;
  int num_total_objs_ = 0;

  std::map<int, int> map_;  // part_id -> id in vector
  std::vector<PartProgress> progresses_;
  std::mutex mu_;
};

}  // namespace xyz

