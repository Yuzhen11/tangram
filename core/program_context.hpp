#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

#include "scheduler/collection_view.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/plan/load_plan_spec.hpp"

namespace xyz {

struct ProgramContext {
  std::vector<LoadPlanSpec> load_plans;
  std::vector<PlanSpec> plans;
  std::vector<CollectionView> collections;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ # of plans: " << plans.size() 
       << ", # of collections: " << collections.size() 
       << " }\n";
    ss << "load_plans:\n";
    for (auto plan : load_plans) {
      ss << plan.DebugString() << "\n";
    }
    ss << "plans:\n";
    for (auto plan : plans) {
      ss << plan.DebugString() << "\n";
    }
    ss << "collections:\n";
    for (auto c : collections) {
      ss << c.DebugString() << "\n";
    }
    return ss.str();
  }

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const ProgramContext& c) {
    stream << c.load_plans << c.plans << c.collections;
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, ProgramContext& c) {
    stream >> c.load_plans >> c.plans >> c.collections;
  	return stream;
  }
};

}  // namespace xyz

