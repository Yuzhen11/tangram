#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

struct ProgramContext {
  std::vector<SpecWrapper> specs;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ # of specs: " << specs.size() 
       << " }\n";
    ss << "specs:\n";
    for (auto spec: specs) {
      ss << spec.DebugString() << "\n";
    }
    return ss.str();
  }

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const ProgramContext& c) {
    stream << c.specs;
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, ProgramContext& c) {
    stream >> c.specs;
  	return stream;
  }
};

}  // namespace xyz

