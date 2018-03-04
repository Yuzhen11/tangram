#pragma once

#include "glog/logging.h"
#include "hdfs/hdfs.h"

namespace xyz {
namespace {

hdfsFS GetFS(std::string hdfs_namenode, int hdfs_namenode_port) {
  hdfsFS fs;
  struct hdfsBuilder *builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, hdfs_namenode.c_str());
  hdfsBuilderSetNameNodePort(builder, hdfs_namenode_port);
  fs = hdfsBuilderConnect(builder);
  CHECK(fs);
  hdfsFreeBuilder(builder);
  return fs;
}

} // namespace
} // namespace xyz
