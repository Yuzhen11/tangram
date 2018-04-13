#pragma once

#include <set>
#include <map>
#include <vector>
#include <sstream>
#include <chrono>

#include "glog/logging.h"

namespace xyz {

class CollectionStatus {
 public:
  using ReadWriteVector = std::pair<std::vector<int>, std::vector<int>>;

  std::string GetLastCP(int collection_id) const;

  void AddCP(int collection_id, std::string url);
  void AddPlan(int id, const ReadWriteVector& p);
  void FinishPlan(int plan_id);
  std::vector<int> GetCurrentPlans();
  std::string DebugString() const;
  std::vector<int> GetReads() const;
  std::vector<int> GetWrites() const;

  std::vector<std::pair<int, std::string>> GetReadsAndCP() const;
  std::vector<std::pair<int, std::string>> GetWritesAndCP() const;
 private:
  std::map<int, ReadWriteVector> cur_plans_;
  std::map<int, std::chrono::system_clock::time_point> plan_time_;
  std::map<int, int> read_ids_;
  std::map<int, int> write_ids_;

  std::map<int, std::string> last_cp_;
};

} // namespace xyz

