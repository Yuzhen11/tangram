#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

#include "core/plan/spec_wrapper.hpp"
#include "core/plan/dag.hpp"

namespace xyz {

struct ProgramContext {
  std::vector<SpecWrapper> specs;
  Dag dag;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ # of specs: " << specs.size() 
       << " }\n";
    ss << "specs:\n";
    for (auto spec: specs) {
      ss << spec.DebugString() << "\n";
    }
    ss << dag.DebugString();
    return ss.str();
  }

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const ProgramContext& c) {
    stream << c.specs << c.dag;
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, ProgramContext& c) {
    stream >> c.specs >> c.dag;
  	return stream;
  }
};

}  // namespace xyz

