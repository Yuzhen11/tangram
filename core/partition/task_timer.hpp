#pragma once

#include <chrono>

namespace xyz {

class TaskTimer {
 public:
  TaskTimer() {
    Add();
  }
  void Add() {
    add_timepoint = std::chrono::steady_clock::now();
  }
  void Run() {
    run_timepoint = std::chrono::steady_clock::now();
  }
  void Finish() {
    end_timepoint = std::chrono::steady_clock::now();
  }

  int GetTimeFromAdd() {
    return GetTimeFrom(add_timepoint);
  }
  int GetTimeFromStart() {
    return GetTimeFrom(run_timepoint);
  }

 private:
  int GetTimeFrom(std::chrono::time_point<std::chrono::steady_clock> t) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t).count();
  }

 private:
  std::chrono::time_point<std::chrono::steady_clock> add_timepoint;
  std::chrono::time_point<std::chrono::steady_clock> run_timepoint;
  std::chrono::time_point<std::chrono::steady_clock> end_timepoint;
};

}  // namespace xyz

