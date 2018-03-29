#pragma once

#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>

#include "core/worker/plan_controller.hpp"

#include "core/map_output/map_output_stream.hpp"
#include "core/map_output/partitioned_map_output.hpp"

namespace xyz {

class PlanController;
class DelayedCombiner {
 public:
  using StreamPair = std::pair<int, std::shared_ptr<AbstractMapOutputStream>>;

  DelayedCombiner(PlanController* plan_controller, int combine_timeout);

  ~DelayedCombiner() {
    finished_.store(true);
    if (detect_thread_.joinable()) {
      detect_thread_.join();
    }
  }

  void AddMapOutput(int upstream_part_id, int version, 
          std::shared_ptr<AbstractMapOutput> map_output);
  void AddStream(int upstream_part_id, int version, int part_id, 
          std::shared_ptr<AbstractMapOutputStream> stream);
  void PeriodicCombine();
  void Submit(int part_id, int version, std::vector<StreamPair> v);
  void CombineSerializeSend(int part_id, int version, std::vector<StreamPair> v);
  void PrepareMsgAndSend(int part_id, int version, 
        std::vector<int> upstream_part_ids, std::shared_ptr<AbstractMapOutputStream> stream);

  void Detect();
 private:
  std::thread detect_thread_;
  std::mutex mu_;
  // part_id -> version -> vector of <upstream_part_id, stream>
  std::vector<std::map<int, std::vector<StreamPair>>> store_;
  PlanController* plan_controller_;

  std::shared_ptr<Executor> executor_;
  // combine_timeout_:
  // <0 : send without combine 
  // 0: directly combine and send
  // 0-2000 : timeout in ms
  // >2000: shuffle combine
  const int combine_timeout_ = 0;
  std::atomic<bool> finished_{false};
};

}  // namespace xyz

