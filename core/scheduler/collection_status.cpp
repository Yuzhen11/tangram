#include "core/scheduler/scheduler.hpp"

namespace xyz {

// TODO: decide when to remove the cp. E.g., remove a checkpoint
// after a plan finishes. 

std::string CollectionStatus::GetLastCP(int collection_id) {
  CHECK(last_cp_.find(collection_id) != last_cp_.end());
  return last_cp_[collection_id];
}

void CollectionStatus::AddCP(int collection_id, std::string url) {
  last_cp_[collection_id] = url;
}

void CollectionStatus::AddPlan(int id, const ReadWriteVector& p) {
  CHECK(cur_plans_.find(id) == cur_plans_.end());
  cur_plans_.insert({id, p});
  for (auto r : p.first) {
    read_ids_[r] += 1;
  }
  for (auto w : p.second) {
    CHECK(write_ids_.find(w) == write_ids_.end());
    CHECK(read_ids_.find(w) == read_ids_.end());
    write_ids_[w] += 1;
    // last_cp_.erase(w);
  }
  plan_time_[id] = std::chrono::system_clock::now();
}

void CollectionStatus::FinishPlan(int plan_id) {
  CHECK(cur_plans_.find(plan_id) != cur_plans_.end());
  auto& p = cur_plans_[plan_id];
  for (auto r: p.first) {
    CHECK(read_ids_.find(r) != read_ids_.end());
    read_ids_[r] --;
    if (read_ids_[r] == 0) {
      read_ids_.erase(r);
    }
  }
  for (auto w: p.second) {
    CHECK_EQ(write_ids_.count(w), 1);
    write_ids_.erase(w);
  }
  cur_plans_.erase(plan_id);
  auto time = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(time-plan_time_[plan_id]);
  LOG(INFO) << "[CollectionStatus] plan: " << plan_id << " time: " << duration.count()*1.0/1000 << " s";
  plan_time_.erase(plan_id);
}

std::vector<int> CollectionStatus::GetCurrentPlans() {
  std::vector<int> ret;
  for (auto& p : cur_plans_) {
    ret.push_back(p.first);
  }
  return ret;
}

std::string CollectionStatus::DebugString() const {
  std::stringstream ss;
  ss << "cur_plans: ";
  for (auto& p: cur_plans_) {
    ss << p.first << ", ";
  }
  ss << "\n";
  ss << "read_ids: ";
  for (auto& c: read_ids_) {
    ss << c.first << ", ";
  }
  ss << "\n";
  ss << "write_ids: ";
  for (auto& c: write_ids_) {
    ss << c.first << ", ";
  }
  ss << "\n";
  ss << "last_cp: ";
  for (auto& kv: last_cp_) {
    ss << kv.first << ": " << kv.second << ", ";
  }
  return ss.str();
}

std::vector<int> CollectionStatus::GetReads() const {
  std::vector<int> ret;
  for (auto& r : read_ids_) {
    ret.push_back(r.first);
  }
  return ret;
}

std::vector<int> CollectionStatus::GetWrites() const {
  std::vector<int> ret;
  for (auto& w : write_ids_) {
    ret.push_back(w.first);
  }
  return ret;
}

} // namespace xyz

