#include "core/progress_tracker.hpp"

namespace xyz {

void ProgressTracker::AddMap(int part_id, int num_objs) {
  std::lock_guard<std::mutex> lk(mu_);
  num_total_objs_ += num_objs;
  PartProgress p(part_id, num_objs);
  progresses_.push_back(p);
  map_[part_id] = progresses_.size() - 1;
}

void ProgressTracker::StartMap(int part_id) {
  std::lock_guard<std::mutex> lk(mu_);
}

void ProgressTracker::FinishMap(int part_id) {
  std::lock_guard<std::mutex> lk(mu_);
}

}  // namespace xyz

