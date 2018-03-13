#pragma once

#include "core/plan/context.hpp"
#include "core/engine.hpp"
#include "core/program_context.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"


DECLARE_string(scheduler);
DECLARE_int32(scheduler_port);
DECLARE_string(hdfs_namenode);
DECLARE_int32(hdfs_port);
DECLARE_int32(num_local_threads);

namespace xyz {

class Runner {
 public:
  static void Init(int argc, char** argv);
  static void Run();
  static void PrintDag();
};

} // namespace xyz

