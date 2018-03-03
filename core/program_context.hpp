#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

#include "core/plan/plan_spec.hpp"
#include "core/plan/collection_spec.hpp"

namespace xyz {

struct ProgramContext {
  std::vector<PlanSpec> plans;
  std::vector<CollectionSpec> collections;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ # of plans: " << plans.size() 
       << ", # of collections: " << collections.size() 
       << " }\n";
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
    stream << c.plans << c.collections;
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, ProgramContext& c) {
    stream >> c.plans >> c.collections;
  	return stream;
  }
};

}  // namespace xyz

